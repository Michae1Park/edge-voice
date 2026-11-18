from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from edge_voice.stt.sensevoice import recognizer as sv_recognizer
from edge_voice.stt.sensevoice.config import SenseVoiceConfig
from edge_voice.stt.sensevoice.models import ModelUnavailableError, ensure_model_dir
from edge_voice.stt.utterance_transcriber import UtteranceTranscriber

_DRM_UEVENTS = "sys/class/drm/*/device/uevent"
_PROBE_STDERR_TAIL_CHARS = 500


class SenseVoiceUnavailableError(RuntimeError):
    pass


class NpuUnavailableError(RuntimeError):
    pass


class NoNpuDeviceError(NpuUnavailableError):
    pass


@dataclass
class BuiltTranscriber:
    transcriber: UtteranceTranscriber
    runtime: str
    model_dir: Path
    npu_fallback: NpuUnavailableError | None = None


def rknpu_present() -> bool:
    return any(
        "DRIVER=RKNPU" in path.read_text(errors="ignore") for path in Path("/").glob(_DRM_UEVENTS)
    )


def _require_sherpa() -> None:
    if importlib.util.find_spec("sherpa_onnx") is None:
        raise SenseVoiceUnavailableError("sherpa-onnx is not installed")


def _probe_npu(model_dir: Path, config: SenseVoiceConfig) -> None:
    cmd = [
        sys.executable,
        "-m",
        "edge_voice.stt.sensevoice.probe",
        str(model_dir),
        json.dumps(config.to_dict()),
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=config.probe_timeout_s, check=False
        )
    except subprocess.TimeoutExpired as exc:
        raise NpuUnavailableError(
            f"NPU model load probe timed out after {config.probe_timeout_s:.0f}s"
        ) from exc
    if result.returncode != 0:
        tail = result.stderr.strip()[-_PROBE_STDERR_TAIL_CHARS:]
        raise NpuUnavailableError(
            f"NPU model load probe exited with code {result.returncode}: {tail}"
        )


def _build_npu(config: SenseVoiceConfig) -> BuiltTranscriber:
    if not rknpu_present():
        raise NoNpuDeviceError("no RKNPU device found (no DRIVER=RKNPU under /sys/class/drm)")
    if config.npu_runtime_lib and not Path(config.npu_runtime_lib).expanduser().is_file():
        raise NpuUnavailableError(f"npu_runtime_lib {config.npu_runtime_lib} does not exist")
    try:
        sv_recognizer.preload_npu_runtime(config.npu_runtime_lib)
    except OSError as exc:
        raise NpuUnavailableError(f"cannot load librknnrt.so: {exc}") from exc
    sv_recognizer.core_mask(config.npu_core)
    try:
        model_dir = ensure_model_dir("npu", config)
    except ModelUnavailableError as exc:
        raise NpuUnavailableError(str(exc)) from exc
    _probe_npu(model_dir, config)
    recognizer = sv_recognizer.SenseVoiceRecognizer(model_dir, "npu", config)
    return BuiltTranscriber(
        UtteranceTranscriber(recognizer, window_s=config.npu_window_s), "npu", model_dir
    )


def _build_cpu(config: SenseVoiceConfig) -> BuiltTranscriber:
    try:
        model_dir = ensure_model_dir("cpu", config)
    except ModelUnavailableError as exc:
        raise SenseVoiceUnavailableError(str(exc)) from exc
    try:
        recognizer = sv_recognizer.SenseVoiceRecognizer(model_dir, "cpu", config)
    except (ImportError, OSError) as exc:
        raise SenseVoiceUnavailableError(
            f"cannot load sherpa-onnx: {exc}. The RKNN build of sherpa-onnx needs "
            "librknnrt.so even on CPU; set stt.sensevoice.npu_runtime_lib"
        ) from exc
    return BuiltTranscriber(UtteranceTranscriber(recognizer), "cpu", model_dir)


def prefetch_models(configs: list[SenseVoiceConfig]) -> None:
    npu_present = rknpu_present()
    runtimes = {"npu" if c.runtime != "cpu" and npu_present else "cpu": c for c in configs}
    for runtime, config in runtimes.items():
        ensure_model_dir(runtime, config)


def build_sensevoice_transcriber(config: SenseVoiceConfig) -> BuiltTranscriber:
    _require_sherpa()
    npu_fallback = None
    if config.runtime in ("npu", "auto"):
        try:
            return _build_npu(config)
        except NpuUnavailableError as exc:
            npu_fallback = exc
    built = _build_cpu(config)
    built.npu_fallback = npu_fallback
    return built
