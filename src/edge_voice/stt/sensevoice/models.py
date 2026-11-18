"""Resolve, and on first use download, SenseVoice model files.

Prebuilt sherpa-onnx releases: an int8 ONNX model for CPU, and fixed-window
fp16 RKNN builds for the RK3588 NPU. Several STT worker processes start at
once, so downloads are serialized with a file lock and land atomically.
"""

from __future__ import annotations

import fcntl
import logging
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path

from edge_voice.stt.sensevoice.config import SenseVoiceConfig

logger = logging.getLogger(__name__)

_RELEASE_URL = "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/{name}.tar.bz2"
_CPU_RELEASE = "sherpa-onnx-sense-voice-zh-en-ja-ko-yue-int8-2024-07-17"
_NPU_RELEASE = "sherpa-onnx-rk3588-{window}-seconds-sense-voice-zh-en-ja-ko-yue-2024-07-17"
_DOWNLOAD_TIMEOUT_S = 60

MODEL_FILES = {"cpu": "model.int8.onnx", "npu": "model.rknn"}
TOKENS_FILE = "tokens.txt"


class ModelUnavailableError(RuntimeError):
    pass


def release_name(runtime: str, config: SenseVoiceConfig) -> str:
    if runtime == "npu":
        return _NPU_RELEASE.format(window=config.npu_window_s)
    return _CPU_RELEASE


def missing_files(model_dir: Path, runtime: str) -> list[str]:
    return [n for n in (MODEL_FILES[runtime], TOKENS_FILE) if not (model_dir / n).is_file()]


def ensure_model_dir(runtime: str, config: SenseVoiceConfig) -> Path:
    override = config.npu_model_dir if runtime == "npu" else config.cpu_model_dir
    if override:
        model_dir = Path(override).expanduser()
        missing = missing_files(model_dir, runtime)
        if missing:
            raise ModelUnavailableError(f"{model_dir} is missing {', '.join(missing)}")
        return model_dir

    name = release_name(runtime, config)
    cache = Path(config.cache_dir).expanduser()
    model_dir = cache / name
    if not missing_files(model_dir, runtime):
        return model_dir

    cache.mkdir(parents=True, exist_ok=True)
    with open(cache / f".{name}.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if missing_files(model_dir, runtime):
            _download(name, cache, model_dir)
    missing = missing_files(model_dir, runtime)
    if missing:
        raise ModelUnavailableError(f"{name} downloaded without {', '.join(missing)}")
    return model_dir


def _download(name: str, cache: Path, model_dir: Path) -> None:
    url = _RELEASE_URL.format(name=name)
    logger.info("Downloading SenseVoice model %s to %s", url, cache)
    staging = Path(tempfile.mkdtemp(prefix=f".{name}.", dir=cache))
    try:
        with urllib.request.urlopen(url, timeout=_DOWNLOAD_TIMEOUT_S) as response:
            with tarfile.open(fileobj=response, mode="r|bz2") as archive:
                archive.extractall(staging, filter="data")
        shutil.rmtree(model_dir, ignore_errors=True)
        (staging / name).rename(model_dir)
    except Exception as exc:
        raise ModelUnavailableError(f"could not download {url}: {exc}") from exc
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    logger.info("SenseVoice model ready at %s", model_dir)
