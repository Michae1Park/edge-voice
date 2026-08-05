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
stop) and the repetitive-output guard.

One add_audio() call per segment, not windowed
────────────────────────────────────────────────
The scratch script fed Moonshine in small windows because it had no choice --
audio arrived live, as Silero produced it, packet by packet. STTWorker
already has the entire finalized segment before _transcribe runs, so that
constraint doesn't apply, and measuring it (scratch/demo_segment_cut_latency.py
et al.) showed windowed feeding was strictly worse on real audio: ~2.7x
slower (Moonshine's decoder redoes real work per add_audio() call, not just
call overhead) and prone to word/phrase duplication right at window
boundaries -- a duplication _is_repetitive can't catch, since it judges a
whole completed line's unique/total token ratio, not a short repeated phrase
inside an otherwise-fine line. moonshine_voice's own mic_transcriber.py
agrees: it coalesces queued audio into as few add_audio() calls as possible
specifically because that "lowers latency and avoids redundant work."

One shared Transcriber, not one per channel
────────────────────────────────────────────
The scratch script gives each channel its own Transcriber because it runs
two threads concurrently -- Moonshine's decoder is stateful, so concurrent
sessions can't share one instance. STTWorker has only one thread pulling
from a single mixed segment_queue, so segments are always handled one at a
time regardless of channel; there is no concurrency to isolate.

Sharing one Transcriber across channels is safe because start()/stop()
fully resets its decoder state -- verified empirically by alternating
channels on one shared instance and diffing the output against a fresh
Transcriber per segment; they matched byte-for-byte. It also fits this
application better: turn-taking dialogue, where the previous speaker's
context genuinely shouldn't bleed into the next line regardless of which
channel it's on, and halves the memory footprint (~175MB saved per
extra channel we're not holding open).

The shared Transcriber is resolved eagerly, in __init__, off the real-time
path -- not lazily on the first segment. `import edge_voice.stt.stt_worker`
stays safe either way (nothing above module level touches moonshine_voice),
but *constructing* an STTWorker now requires it to be importable, unless
`transcriber_factory` bypasses that entirely (tests, benchmarks). In
production this is never a real constraint: orchestrator.build() -- the only
caller that doesn't inject a factory -- always has moonshine_voice available.

Assumptions:
  - SpeechSegment.audio is raw PCM bytes, int16 mono, at config.sample_rate
    (what VADWorker emits -- it concatenates the AudioPacket.samples it was
    fed). Adjust _pcm_to_float32 if that ever changes.
  - Moonshine wants float32 in [-1, 1]; add_audio takes a plain list.
"""

from __future__ import annotations

import ctypes
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np

from edge_voice.observability.logging import get_stage_logger
from edge_voice.pipeline.models import SpeechSegment, TranscriptEvent

logger = get_stage_logger(__name__, stage="stt")

QUEUE_GET_TIMEOUT_S = 0.2


def _default_options() -> dict[str, str]:
    return {
        # 13.0 (the scratch script's value) truncates Korean mid-sentence;
        # see configs/default.yaml for the measurements behind 30.0.
        "max_tokens_per_second": "20.0",
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
    options: dict[str, str] = field(default_factory=_default_options)
    # Below this unique/total token ratio a line is treated as the model
    # looping on itself; see _is_repetitive.
    repetitive_ratio: float = 0.45
    # Segment-queue depth at or above which an in-progress partial is
    # skipped rather than transcribed (see _handle_partial). Only consulted
    # when vad.partial_interval_s has partials switched on at all.
    partial_max_queue_depth: int = 0


class _OrtApiBase(ctypes.Structure):
    # First two fields of ONNX Runtime's stable C ABI struct (the rest is a
    # large, version-sensitive function table we don't need):
    # struct OrtApiBase { const OrtApi*(*GetApi)(uint32_t); const char*(*GetVersionString)(void); };
    _fields_ = [("GetApi", ctypes.c_void_p), ("GetVersionString", ctypes.c_void_p)]


def _confirm_onnx_runtime_backend(model_path: str) -> None:
    """One-time startup proof the model is served by ONNX Runtime.

    moonshine_voice ships no torch/tf dependency, and its only inference
    path is libmoonshine.so -> onnxruntime's C API -- confirmed with gdb by
    breaking on libmoonshine.so's exported `ort_run` symbol and tracing a
    real transcription: moonshine_transcribe_stream ->
    Transcriber::transcribe_stream -> MoonshineModel::transcribe -> ort_run.

    Must run *after* Transcriber() is constructed: onnxruntime is dlopen'd
    lazily (verified absent from /proc/self/maps right after `import
    moonshine_voice`, present right after construction), so calling this any
    earlier would find nothing mapped.

    Reports the version via ONNX Runtime's own C API
    (OrtGetApiBase().GetVersionString()) against the exact .so mapped into
    this process, rather than parsing it out of a filename -- so it can't
    silently drift out of sync with what's actually loaded.
    """
    try:
        with open(f"/proc/{os.getpid()}/maps") as f:
            onnx_lib_paths = sorted({line.split()[-1] for line in f if "libonnxruntime" in line})
        if not onnx_lib_paths:
            logger.warning(
                "STTWorker: no onnxruntime shared library mapped in this process "
                "-- inference backend could not be confirmed"
            )
            return
        onnx_lib_path = onnx_lib_paths[0]

        lib = ctypes.CDLL(onnx_lib_path)
        lib.OrtGetApiBase.restype = ctypes.c_void_p
        api_base = ctypes.cast(lib.OrtGetApiBase(), ctypes.POINTER(_OrtApiBase)).contents
        get_version_string = ctypes.CFUNCTYPE(ctypes.c_char_p)(api_base.GetVersionString)
        onnx_version = get_version_string().decode()

        model_files = (
            sorted(f for f in os.listdir(model_path) if f.endswith((".onnx", ".ort")))
            if os.path.isdir(model_path)
            else []
        )

        logger.info(
            "STTWorker: model running on ONNX Runtime v%s (%s) -- model files=%s at %s",
            onnx_version,
            onnx_lib_path,
            model_files,
            model_path,
        )
    except Exception:
        logger.exception("STTWorker: failed to confirm ONNX Runtime backend")


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
                        extra={"segment_id": self.seg_id},
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
        # Resolved once -- get_model_for_language re-checks/downloads assets
        # and re-prints the license notice on every call, so this exists to
        # avoid doing that twice. Must be set before _new_transcriber() runs,
        # since that method reads it.
        self._resolved_model: tuple[str, Any] | None = None
        # Eager: the one shared Transcriber (see module docstring) is loaded
        # right here, at construction, off the real-time path -- not on
        # whichever thread's first segment happens to arrive. Requires
        # moonshine_voice to be importable unless transcriber_factory bypasses
        # it (tests/benchmarks); orchestrator.build() always has it in
        # production, so this only ever blocks startup, never a live segment.
        self._transcriber: Any = self._new_transcriber()
        self._stop_event = threading.Event()
        # Monotonic timestamp of the last segment handled, read by the
        # supervisor's stall check (docs/BUILDPLAN.md Milestone 6). A plain
        # float write/read is atomic under the GIL, so no lock is needed.
        self._last_activity = time.monotonic()
        # Wall-clock duration of the most recent _transcribe() call -- pure
        # inference time, not queue-to-transcript (queue_depths() already
        # answers "is there a backlog"; see Milestone 7 decision in
        # docs/BUILDPLAN.md). None until the first segment is transcribed.
        self._last_latency_s: float | None = None
        # Per-channel view of the same latency, for observability/metrics.py.
        # Needs its own lock (unlike the plain float above): a dict insert
        # (first segment on a channel) can resize the table, and
        # MetricsCollector's cross-thread dict(...) copy would race with that.
        self._channel_latency_lock = threading.Lock()
        self._channel_latency_s: dict[str, float] = {}
        # Partial-path counters, kept separate from every latency field above
        # so the two never mix (see _handle_partial). Written only by this
        # worker's own thread; readers tolerate a stale count.
        self._partials_transcribed = 0
        self._partials_dropped = 0

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    @property
    def last_activity(self) -> float:
        """Monotonic time of the last segment handled (for supervisor stall check)."""
        return self._last_activity

    @property
    def last_latency_s(self) -> float | None:
        """Inference time of the most recent _transcribe() call, or None if
        no segment has been transcribed yet (for observability/metrics.py)."""
        return self._last_latency_s

    def channel_latencies_s(self) -> dict[str, float]:
        """Per-channel view of the same latency last_latency_s reports in
        aggregate (for observability/metrics.py). Finals only -- partials
        never write here, so this stays a like-for-like series."""
        with self._channel_latency_lock:
            return dict(self._channel_latency_s)

    def partial_stats(self) -> dict[str, int]:
        """How many in-progress partials were transcribed vs skipped under
        backpressure. A high dropped:transcribed ratio means
        vad.partial_interval_s is asking for more than this device can do."""
        return {
            "transcribed": self._partials_transcribed,
            "dropped": self._partials_dropped,
        }

    def run(self) -> None:
        logger.info("STTWorker started")
        while not self._stop_event.is_set():
            try:
                segment = self._segment_queue.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue

            if segment is None:  # shutdown sentinel
                break

            self._last_activity = time.monotonic()

            try:
                self._handle_segment(segment)
            except Exception:
                logger.exception(
                    "STTWorker failed on segment=%s channel=%s",
                    segment.segment_id,
                    segment.channel_id,
                    extra={"segment_id": segment.segment_id, "channel_id": segment.channel_id},
                )
        logger.info("STTWorker stopped")

    # ── Per-segment handling ────────────────────────────────────

    def _handle_segment(self, segment: SpeechSegment) -> None:
        if segment.is_partial:
            self._handle_partial(segment)
            return

        # No lazy load here: __init__ already resolved the one shared
        # Transcriber, off the real-time path.
        transcriber = self._transcriber
        start = time.monotonic()
        text = self._transcribe(transcriber, segment)
        self._last_latency_s = time.monotonic() - start
        with self._channel_latency_lock:
            self._channel_latency_s[segment.channel_id] = self._last_latency_s

        if not text:
            logger.debug(
                "STTWorker: segment=%s produced no text (%.2fs)",
                segment.segment_id,
                segment.end - segment.start,
                extra={"segment_id": segment.segment_id, "channel_id": segment.channel_id},
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

    # ── Partial (in-progress prefix) handling ───────────────────

    def _handle_partial(self, segment: SpeechSegment) -> None:
        """Transcribe an in-progress prefix for display, then discard it.

        Deliberately does NOT touch _last_latency_s or _channel_latency_s.
        Those feed metrics.py and the TRANSCRIPT log line, and a partial
        times a *prefix* against a different model entry point -- blending
        the two would make "STT latency" mean nothing. Partials get their
        own counters instead; see partial_stats().
        """
        if self._segment_queue.qsize() > self.config.partial_max_queue_depth:
            # Throwaway work, and something real is already waiting. Skipping
            # is the whole backpressure story: finals never take this branch.
            self._partials_dropped += 1
            logger.debug(
                "STTWorker: dropping partial segment=%s (queue depth %d)",
                segment.segment_id,
                self._segment_queue.qsize(),
                extra={"segment_id": segment.segment_id, "channel_id": segment.channel_id},
            )
            return

        text = self._transcribe_partial(self._transcriber, segment)
        self._partials_transcribed += 1
        if not text:
            return

        self._on_transcript(
            TranscriptEvent(
                channel_id=segment.channel_id,
                segment_id=segment.segment_id,
                text=text,
                start=segment.start,
                end=segment.end,
                is_final=False,
            )
        )

    def _transcribe_partial(self, transcriber: Any, segment: SpeechSegment) -> str:
        """One stateless pass over the prefix.

        transcribe_without_streaming() works off the transcriber handle
        rather than the default stream, so it cannot perturb the state the
        final pass depends on -- the reason partials are safe to run on the
        same shared Transcriber instead of a second ~175MB copy. Safe
        because this worker is single-threaded and _transcribe brackets
        start()/stop() around one segment, so no stream is ever open here.
        """
        samples = self._pcm_to_float32(segment.audio).tolist()
        transcript = transcriber.transcribe_without_streaming(samples, self.config.sample_rate)
        lines = [line.text for line in getattr(transcript, "lines", [])]
        text = " ".join(t for t in lines if t).strip()
        # A looping partial is noise the user would watch get worse; the
        # final applies the same guard with a best_partial fallback, which
        # doesn't apply here because there's nothing to fall back to.
        return "" if _is_repetitive(text, self.config.repetitive_ratio) else text

    def _transcribe(self, transcriber: Any, segment: SpeechSegment) -> str:
        collector = _make_collector(self.config.repetitive_ratio, segment.segment_id)

        # remove_all_listeners() first: the transcriber is reused across
        # every segment (all channels), so a stale collector would keep
        # receiving events.
        transcriber.remove_all_listeners()
        transcriber.add_listener(collector)

        transcriber.start()
        try:
            samples = self._pcm_to_float32(segment.audio).tolist()
            transcriber.add_audio(samples, self.config.sample_rate)
        finally:
            # stop() flushes the decoder and resets its state (verified in
            # the module docstring); skipping it on error would leave the
            # session open and corrupt the next segment, on any channel.
            transcriber.stop()

        return str(collector.text())

    # ── Helpers ──────────────────────────────────────────────────

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

        transcriber = Transcriber(
            model_path=model_path,
            model_arch=model_arch,
            options=self.config.options,
        )
        # After construction, not before: onnxruntime is dlopen'd lazily by
        # libmoonshine.so, only once a Transcriber actually exists.
        _confirm_onnx_runtime_backend(model_path)
        return transcriber

    def _pcm_to_float32(self, pcm_bytes: bytes) -> np.ndarray:
        # int16 mono PCM -> float32 normalized to [-1, 1]
        return np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32768.0
