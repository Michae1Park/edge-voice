from __future__ import annotations

from dataclasses import asdict, dataclass

RK3588_NPU_CORES = 3
SENSEVOICE_LANGUAGES = ("auto", "zh", "en", "ja", "ko", "yue")
NPU_WINDOWS_S = (5, 10, 15, 20, 25, 30)
DEFAULT_CACHE_DIR = "~/.cache/edge_voice/sensevoice"


@dataclass
class SenseVoiceConfig:
    runtime: str = "auto"
    language: str = "ko"
    use_itn: bool = True
    cache_dir: str = DEFAULT_CACHE_DIR
    cpu_model_dir: str = ""
    cpu_threads: int = 1
    npu_model_dir: str = ""
    npu_window_s: int = 5
    npu_runtime_lib: str = ""
    npu_core: int | None = None
    probe_timeout_s: float = 60.0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
