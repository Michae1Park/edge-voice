"""SenseVoice via sherpa-onnx, on CPU or the RK3588 NPU (RKNN provider)."""

from __future__ import annotations

import ctypes
from pathlib import Path

import numpy as np

from edge_voice.stt.sensevoice.config import RK3588_NPU_CORES, SenseVoiceConfig
from edge_voice.stt.sensevoice.models import MODEL_FILES, TOKENS_FILE

# sherpa-onnx overloads num_threads as the RKNN core mask on the rknn provider
# (sherpa-onnx/csrc/rknn/utils.cc SetCoreMask): 1 = auto, 0/-1/-2 = core 0/1/2.
_AUTO_CORE_MASK = 1
_CORE_MASKS = {0: 0, 1: -1, 2: -2}


def core_mask(core: int | None) -> int:
    if core is None:
        return _AUTO_CORE_MASK
    if core not in _CORE_MASKS:
        raise ValueError(f"RK3588 NPU core must be 0..{RK3588_NPU_CORES - 1}, got {core}")
    return _CORE_MASKS[core]


def preload_npu_runtime(runtime_lib: str) -> None:
    """Load librknnrt.so by absolute path before sherpa-onnx is imported.

    sherpa-onnx's RKNN build links librknnrt.so by soname; once a library with
    that soname is already mapped, the dynamic linker reuses it, so a copy in
    a user directory works without sudo or LD_LIBRARY_PATH.
    """
    ctypes.CDLL(runtime_lib or "librknnrt.so", mode=ctypes.RTLD_GLOBAL)


class SenseVoiceRecognizer:
    def __init__(self, model_dir: Path, runtime: str, config: SenseVoiceConfig) -> None:
        if runtime == "npu":
            preload_npu_runtime(config.npu_runtime_lib)
            provider, threads = "rknn", core_mask(config.npu_core)
        else:
            provider, threads = "cpu", config.cpu_threads
            if config.npu_runtime_lib:
                # sherpa-onnx's RKNN build links librknnrt.so even when it
                # only ever runs on CPU.
                preload_npu_runtime(config.npu_runtime_lib)
        import sherpa_onnx

        self._recognizer = sherpa_onnx.OfflineRecognizer.from_sense_voice(
            model=str(model_dir / MODEL_FILES[runtime]),
            tokens=str(model_dir / TOKENS_FILE),
            num_threads=threads,
            provider=provider,
            language=config.language,
            use_itn=config.use_itn,
        )

    def transcribe(self, samples: np.ndarray, sample_rate: int) -> str:
        stream = self._recognizer.create_stream()
        stream.accept_waveform(sample_rate, samples)
        self._recognizer.decode_stream(stream)
        return str(stream.result.text)
