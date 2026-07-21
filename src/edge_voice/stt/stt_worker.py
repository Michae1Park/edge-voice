"""STTWorker: single thread consuming segment_queue and emitting TranscriptEvents.

Drop-in replacement for FakeSTTWorker(segment_queue, on_transcript).

Relationship to scratch/silero_moonshine.py
───────────────────────────────────────────
The scratch script fuses VAD and STT in one loop: it streams 32ms windows
into a Transcriber as Silero detects them, and manages session boundaries
(arm/close/soft-cut) from live VAD events. Here that work is already done --
VADWorker owns segmentation and hands us finalized SpeechSegments -- so all
the VAD state machine, score tracking, and soft/hard-cut logic is absent by
design. What carries over is the session lifecycle (start -> add_audio ->
stop), the feed-window batching, and the repetitive-output guard.

Per-channel Transcriber instances
─────────────────────────────────
Moonshine is a stateful streaming decoder, so sessions can't be shared
across channels (same rationale as the scratch script). Each channel_id
gets its own Transcriber, created lazily on first segment and tracked in
self._transcribers -- mirroring VADWorker's per-channel VADIterator
pattern. The OS typically memory-maps the same weight file for both.

Lazy construction is also what keeps `import edge_voice.stt.stt_worker` and
PipelineOrchestrator.build() working on machines without moonshine_voice
installed; the ImportError surfaces on the first real segment instead.
Inject `transcriber_factory` to bypass it entirely (tests, benchmarks).

Assumptions:
  - SpeechSegment.audio is raw PCM bytes, int16 mono, at config.sample_rate
    (what VADWorker emits -- it concatenates the AudioPacket.samples it was
    fed). Adjust _pcm_to_float32 if that ever changes.
  - Moonshine wants float32 in [-1, 1]; add_audio takes a plain list.
"""

from __future__ import annotations

import logging
import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

import numpy as np

from edge_voice.pipeline.models import SpeechSegment, TranscriptEvent

logger = logging.getLogger(__name__)

QUEUE_GET_TIMEOUT_S = 0.2


def _default_options() -> dict[str, str]:
    return {
        "max_tokens_per_second": "13.0",
        "identify_speakers": "false",
        "log_api_calls": "false",
        "save_input_wav_path": "",
        "return_audio_data": "false",
    }


@dataclass
class STTWorkerConfig:
    language: str = "ko"
    # Readable arch name ("tiny", "medium-streaming", ...), converted to
    # moonshine's ModelArch enum in _new_transcriber. See STTSettings for
    # which archs each language publishes.
    model_arch: str = "tiny"
    sample_rate: int = 16000
    # add_audio() is called every feed_windows * feed_window_samples samples,
    # matching the scratch script's batching (64 * 512 = 32768 = ~2.05s @ 16kHz).
    feed_windows: int = 64
    feed_window_samples: int = 512
    options: dict[str, str] = field(default_factory=_default_options)
    # Below this unique/total token ratio a line is treated as the model
    # looping on itself; see _is_repetitive.
    repetitive_ratio: float = 0.45

    @property
    def feed_chunk_samples(self) -> int:
        return self.feed_windows * self.feed_window_samples


def _is_repetitive(text: str, threshold: float) -> bool:
    """Detect the degenerate 'model loops on one phrase' failure mode.

    Carried over from the scratch script: short lines are always accepted
    (too few tokens to judge), longer ones are rejected when the ratio of
    unique tokens falls below `threshold`.
    """
    tokens = text.split()
    if len(tokens) < 4:
        return False
    return (len(set(tokens)) / len(tokens)) < threshold


_collector_cls: Any = None


def _collector_base() -> type:
    """moonshine_voice's listener base if installed, else plain object.

    Subclassing the real base matters in production (moonshine may check
    the type when registering a listener); falling back to `object` keeps
    the worker exercisable with an injected transcriber_factory on machines
    without moonshine_voice -- see the module docstring.
    """
    try:
        from moonshine_voice import TranscriptEventListener

        return TranscriptEventListener  # type: ignore[no-any-return]
    except ImportError:
        return object


def _make_collector(repetitive_ratio: float, segment_id: str) -> Any:
    """Build a listener that accumulates completed lines into one transcript.

    The class is defined lazily (and cached) so the base class above is only
    resolved on first use, keeping this module importable either way.
    """
    global _collector_cls

    if _collector_cls is None:

        class _Collector(_collector_base()):  # type: ignore[misc, valid-type]
            def __init__(self, ratio: float, seg_id: str) -> None:
                self.ratio = ratio
                self.seg_id = seg_id
                self.lines: list[str] = []
                self.best_partial = ""

            def on_line_text_changed(self, event: Any) -> None:
                text = event.line.text
                if not _is_repetitive(text, self.ratio):
                    self.best_partial = text

            def on_line_completed(self, event: Any) -> None:
                text = event.line.text
                if _is_repetitive(text, self.ratio):
                    logger.warning(
                        "STTWorker: segment=%s final line was repetitive, "
                        "falling back to best partial",
                        self.seg_id,
                    )
                    text = self.best_partial
                if text:
                    self.lines.append(text)

            def text(self) -> str:
                # A segment can decode into several lines; join them back up.
                return " ".join(self.lines).strip()

        _collector_cls = _Collector

    return _collector_cls(repetitive_ratio, segment_id)


class STTWorker(threading.Thread):
    """Drop-in replacement for FakeSTTWorker(segment_queue, on_transcript)."""

    def __init__(
        self,
        segment_queue: "queue.Queue[SpeechSegment]",
        on_transcript: Callable[[TranscriptEvent], None],
        config: STTWorkerConfig | None = None,
        transcriber_factory: Callable[[], Any] | None = None,
        name: str = "STTWorker",
    ) -> None:
        super().__init__(name=name, daemon=False)
        self._segment_queue = segment_queue
        self._on_transcript = on_transcript
        self.config = config or STTWorkerConfig()
        self._transcriber_factory = transcriber_factory
        self._transcribers: dict[str, Any] = {}
        # Resolved once and reused across channels -- get_model_for_language
        # re-checks/downloads assets and re-prints the license notice on
        # every call, so calling it per channel is wasteful and noisy.
        self._resolved_model: tuple[str, Any] | None = None
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def run(self) -> None:
        logger.info("STTWorker started")
        while not self._stop_event.is_set():
            try:
                segment = self._segment_queue.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue

            if segment is None:  # shutdown sentinel
                break

            try:
                self._handle_segment(segment)
            except Exception:
                logger.exception(
                    "STTWorker failed on segment=%s channel=%s",
                    segment.segment_id,
                    segment.channel_id,
                )
        logger.info("STTWorker stopped")

    # ── Per-segment handling ────────────────────────────────────

    def _handle_segment(self, segment: SpeechSegment) -> None:
        transcriber = self._transcriber_for(segment.channel_id)
        text = self._transcribe(transcriber, segment)

        if not text:
            logger.debug(
                "STTWorker: segment=%s produced no text (%.2fs)",
                segment.segment_id,
                segment.end - segment.start,
            )
            return

        self._on_transcript(
            TranscriptEvent(
                channel_id=segment.channel_id,
                segment_id=segment.segment_id,
                text=text,
                start=segment.start,
                end=segment.end,
            )
        )

    def _transcribe(self, transcriber: Any, segment: SpeechSegment) -> str:
        collector = _make_collector(self.config.repetitive_ratio, segment.segment_id)

        # remove_all_listeners() first: the transcriber is reused across
        # segments, so a stale collector would keep receiving events.
        transcriber.remove_all_listeners()
        transcriber.add_listener(collector)

        transcriber.start()
        try:
            for chunk in self._feed_chunks(segment.audio):
                transcriber.add_audio(chunk, self.config.sample_rate)
        finally:
            # stop() flushes the decoder; skipping it on error would leave
            # the session open and corrupt the next segment on this channel.
            transcriber.stop()

        return str(collector.text())

    def _feed_chunks(self, pcm_bytes: bytes) -> Iterator[list[float]]:
        """Yield float32 sample lists of feed_chunk_samples each (last may be short)."""
        samples = self._pcm_to_float32(pcm_bytes)
        step = self.config.feed_chunk_samples
        for i in range(0, len(samples), step):
            yield samples[i : i + step].tolist()

    # ── Helpers ──────────────────────────────────────────────────

    def _transcriber_for(self, channel_id: str) -> Any:
        transcriber = self._transcribers.get(channel_id)
        if transcriber is None:
            transcriber = self._new_transcriber()
            self._transcribers[channel_id] = transcriber
            logger.info("STTWorker: created Transcriber for channel=%s", channel_id)
        return transcriber

    def _new_transcriber(self) -> Any:
        if self._transcriber_factory is not None:
            return self._transcriber_factory()

        from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch

        if self._resolved_model is None:
            arch = string_to_model_arch(self.config.model_arch)
            self._resolved_model = get_model_for_language(self.config.language, arch)
            logger.info(
                "STTWorker: model=%s arch=%s", self._resolved_model[0], self._resolved_model[1]
            )
        model_path, model_arch = self._resolved_model

        return Transcriber(
            model_path=model_path,
            model_arch=model_arch,
            options=self.config.options,
        )

    def _pcm_to_float32(self, pcm_bytes: bytes) -> np.ndarray:
        # int16 mono PCM -> float32 normalized to [-1, 1]
        return np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32768.0
