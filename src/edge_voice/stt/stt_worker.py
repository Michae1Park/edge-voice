from __future__ import annotations

import ctypes
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np

from edge_voice.observability.logging import get_stage_logger
from edge_voice.pipeline.models import SpeechSegment, TranscriptEvent
from edge_voice.stt.sensevoice.config import SenseVoiceConfig

logger = get_stage_logger(__name__, stage="stt")

QUEUE_GET_TIMEOUT_S = 0.2
INT16_FULL_SCALE = 32768.0

# Models published on moonshine's CDN but missing from its registry (its ko entry is mislabelled).
# Only ("ko", "base") has been verified here; the rest download but are untested.
# Re-run scratch/probe_moonshine_models.py after upgrading moonshine-voice.
_MODEL_REGISTRY_OVERRIDES: dict[tuple[str, str], str] = {
    ("ko", "base"): "https://download.moonshine.ai/model/base-ko/quantized/base-ko",
    ("ar", "tiny"): "https://download.moonshine.ai/model/tiny-ar/quantized/tiny-ar",
    ("uk", "tiny"): "https://download.moonshine.ai/model/tiny-uk/quantized/tiny-uk",
    ("vi", "tiny"): "https://download.moonshine.ai/model/tiny-vi/quantized/tiny-vi",
    ("zh", "tiny"): "https://download.moonshine.ai/model/tiny-zh/quantized/tiny-zh",
}


def _default_options() -> dict[str, str]:
    return {
        # 13.0 truncates Korean mid-sentence; see configs/default.yaml.
        "max_tokens_per_second": "20.0",
        "vad_threshold": "0",
        "identify_speakers": "false",
        "log_api_calls": "false",
        "save_input_wav_path": "",
        "return_audio_data": "false",
    }


@dataclass
class STTWorkerConfig:
    language: str = "ko"
    model_arch: str = "tiny"
    sample_rate: int = 16000
    options: dict[str, str] = field(default_factory=_default_options)
    repetitive_ratio: float = 0.45
    partial_max_queue_depth: int = 0
    model: str = "sensevoice"
    sensevoice: SenseVoiceConfig = field(default_factory=SenseVoiceConfig)


# First two fields of ONNX Runtime's stable C ABI OrtApiBase struct.
class _OrtApiBase(ctypes.Structure):
    _fields_ = [("GetApi", ctypes.c_void_p), ("GetVersionString", ctypes.c_void_p)]


# Must run after Transcriber() is constructed: onnxruntime is dlopen'd lazily by libmoonshine.so.
def _confirm_onnx_runtime_backend(model_path: str) -> None:
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
    tokens = text.split()
    if len(tokens) < 4:
        return False
    return (len(set(tokens)) / len(tokens)) < threshold


_collector_cls: Any = None


def _collector_base() -> type:
    try:
        from moonshine_voice import TranscriptEventListener

        return TranscriptEventListener  # type: ignore[no-any-return]
    except ImportError:
        return object


def _make_collector(repetitive_ratio: float, segment_id: str) -> Any:
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
                return " ".join(self.lines).strip()

        _collector_cls = _Collector

    return _collector_cls(repetitive_ratio, segment_id)


# One worker per channel: a single shared decoder can't sustain two real-time channels on the RPi5,
# and a second Transcriber costs only ~56MB. Each segment is fed in one add_audio() call: windowed
# feeding was ~2.7x slower and duplicated words at window boundaries.
class STTWorker(threading.Thread):
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
        # get_model_for_language re-checks downloads and re-prints the license on every call.
        self._resolved_model: tuple[str, Any] | None = None
        # Eager, so model loading never lands on the real-time path.
        self._transcriber: Any = self._new_transcriber()
        self._stop_event = threading.Event()
        self._last_activity = time.monotonic()
        self._last_latency_s: float | None = None
        # A dict insert can resize while MetricsCollector copies it from another thread.
        self._channel_latency_lock = threading.Lock()
        self._channel_latency_s: dict[str, float] = {}
        self._partials_transcribed = 0
        self._partials_dropped = 0

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    @property
    def last_activity(self) -> float:
        return self._last_activity

    @property
    def last_latency_s(self) -> float | None:
        return self._last_latency_s

    def channel_latencies_s(self) -> dict[str, float]:
        with self._channel_latency_lock:
            return dict(self._channel_latency_s)

    def partial_stats(self) -> dict[str, int]:
        return {
            "transcribed": self._partials_transcribed,
            "dropped": self._partials_dropped,
        }

    def run(self) -> None:
        logger.info("%s started", self.name)
        while not self._stop_event.is_set():
            try:
                segment = self._segment_queue.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue

            if segment is None:
                break

            self._last_activity = time.monotonic()

            try:
                self._handle_segment(segment)
            except Exception:
                logger.exception(
                    "%s failed on segment=%s channel=%s",
                    self.name,
                    segment.segment_id,
                    segment.channel_id,
                    extra={"segment_id": segment.segment_id, "channel_id": segment.channel_id},
                )
        logger.info("%s stopped", self.name)

    def _handle_segment(self, segment: SpeechSegment) -> None:
        if segment.is_partial:
            self._handle_partial(segment)
            return

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

    # Never touches the final-latency fields: a prefix timing would make "STT latency" meaningless.
    def _handle_partial(self, segment: SpeechSegment) -> None:
        if self._segment_queue.qsize() > self.config.partial_max_queue_depth:
            # Finals never take this branch; only throwaway partials are skipped under backpressure.
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

    # transcribe_without_streaming() leaves the default stream untouched, so partials can share the
    # final's Transcriber.
    def _transcribe_partial(self, transcriber: Any, segment: SpeechSegment) -> str:
        samples = self._pcm_to_float32(segment.audio).tolist()
        transcript = transcriber.transcribe_without_streaming(samples, self.config.sample_rate)
        lines = [line.text for line in getattr(transcript, "lines", [])]
        text = " ".join(t for t in lines if t).strip()
        return "" if _is_repetitive(text, self.config.repetitive_ratio) else text

    def _transcribe(self, transcriber: Any, segment: SpeechSegment) -> str:
        collector = _make_collector(self.config.repetitive_ratio, segment.segment_id)

        # The transcriber is reused across segments; a stale collector would keep receiving events.
        transcriber.remove_all_listeners()
        transcriber.add_listener(collector)

        transcriber.start()
        try:
            samples = self._pcm_to_float32(segment.audio).tolist()
            transcriber.add_audio(samples, self.config.sample_rate)
        finally:
            # Skipping stop() on error would leave the session open and corrupt the next segment.
            transcriber.stop()

        return str(collector.text())

    def _new_transcriber(self) -> Any:
        if self._transcriber_factory is not None:
            return self._transcriber_factory()

        if self.config.model == "sensevoice":
            return self._new_sensevoice_transcriber()
        return self._new_moonshine_transcriber()

    def _new_moonshine_transcriber(self) -> Any:
        from moonshine_voice import Transcriber

        if self._resolved_model is None:
            self._resolved_model = self._resolve_model()
            logger.info(
                "STTWorker: model=%s arch=%s", self._resolved_model[0], self._resolved_model[1]
            )
        model_path, model_arch = self._resolved_model

        transcriber = Transcriber(
            model_path=model_path,
            model_arch=model_arch,
            options=self.config.options,
        )
        _confirm_onnx_runtime_backend(model_path)
        return transcriber

    def _new_sensevoice_transcriber(self) -> Any:
        from edge_voice.stt.sensevoice import NoNpuDeviceError, build_sensevoice_transcriber

        config = self.config.sensevoice
        built = build_sensevoice_transcriber(config)
        if built.npu_fallback is not None:
            quiet = config.runtime == "auto" and isinstance(built.npu_fallback, NoNpuDeviceError)
            logger.log(
                logging.INFO if quiet else logging.WARNING,
                "%s: NPU unavailable, running SenseVoice on CPU: %s",
                self.name,
                built.npu_fallback,
            )
        logger.info(
            "%s: model=sensevoice runtime=%s%s model_dir=%s",
            self.name,
            built.runtime,
            f" npu_core={config.npu_core}" if built.runtime == "npu" else "",
            built.model_dir,
        )
        return built.transcriber

    # Overrides still go through download_model_from_info() so they share moonshine's cache and download logic.
    def _resolve_model(self) -> tuple[str, Any]:
        from moonshine_voice import (
            download_spelling_model_for_language,
            get_model_for_language,
            string_to_model_arch,
        )
        from moonshine_voice.download import download_model_from_info

        language, arch_name = self.config.language, self.config.model_arch
        override_url = _MODEL_REGISTRY_OVERRIDES.get((language, arch_name))
        arch = string_to_model_arch(arch_name)
        if override_url is None:
            return get_model_for_language(language, arch)

        logger.info(
            "STTWorker: %s/%s missing from moonshine's registry, using override %s",
            language,
            arch_name,
            override_url,
        )
        resolved = download_model_from_info(
            {
                "model_name": f"{arch_name}-{language}",
                "model_arch": arch,
                "language": language,
                "download_url": override_url,
            }
        )
        try:
            download_spelling_model_for_language(language)
        except Exception as exc:
            logger.warning("STTWorker: spelling-model prefetch failed for %s: %s", language, exc)
        return resolved

    def _pcm_to_float32(self, pcm_bytes: bytes) -> np.ndarray:
        return np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / INT16_FULL_SCALE
