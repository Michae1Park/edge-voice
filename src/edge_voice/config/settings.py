from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from edge_voice.stt.sensevoice.config import (
    DEFAULT_CACHE_DIR,
    RK3588_NPU_CORES,
    SENSEVOICE_LANGUAGES,
)


class MQTTChannels(BaseModel):
    topic: str
    channel_id: str


class MQTTSettings(BaseModel):
    broker_host: str = "localhost"
    broker_port: int = 1883
    channels: list[MQTTChannels] = Field(
        default_factory=lambda: [
            MQTTChannels(topic="stt/audio_chunks_rx", channel_id="rx"),
            MQTTChannels(topic="stt/audio_chunks_tx", channel_id="tx"),
        ]
    )


class AudioSettings(BaseModel):
    sample_rate: int = 16000
    chunk_samples: int = 320
    channels: int = 1
    format: Literal["int16", "int32", "float32"] = "int16"

    @field_validator("format", mode="before")
    @classmethod
    def normalize_format(cls, value: str):
        if isinstance(value, str):
            return value.lower()
        return value


class RepacketizerSettings(BaseModel):

    incoming_ms: float = Field(default=20.0, gt=0)
    outgoing_ms: float = Field(default=32.0, gt=0)
    bytes_per_sample: int = Field(default=2, ge=1)


class VADSettings(BaseModel):

    threshold: float = 0.5
    window_samples: int = 512
    rms_gate_enabled: bool = True
    silence_rms_floor: float = 0.01
    preroll_chunks: int = 3
    min_silence_duration_ms: int = 100
    speech_pad_ms: int = 30
    idle_flush_s: float = 2.0
    segment_limits_enabled: bool = False
    max_segment_s: float = 7.0
    soft_cut_enabled: bool = True
    soft_cut_s: float = 5.0
    soft_cut_lookahead_s: float = 1.0
    soft_cut_min_dip: float = 0.10
    partial_interval_s: float = Field(default=0.0, ge=0)
    partial_min_segment_s: float = Field(default=1.5, gt=0)

    @model_validator(mode="after")
    def _check_soft_cut_below_hard_cap(self) -> "VADSettings":
        if (
            self.segment_limits_enabled
            and self.soft_cut_enabled
            and self.soft_cut_s >= self.max_segment_s
        ):
            raise ValueError(
                f"vad.soft_cut_s ({self.soft_cut_s}s) must be less than "
                f"vad.max_segment_s ({self.max_segment_s}s), otherwise the hard "
                "cap preempts the soft-cut search entirely"
            )
        return self


class MoonshineSettings(BaseModel):

    model_config = ConfigDict(extra="forbid")

    model_arch: Literal[
        "tiny",
        "base",
        "tiny-streaming",
        "base-streaming",
        "small-streaming",
        "medium-streaming",
    ] = "tiny"
    max_tokens_per_second: str = "20.0"
    vad_threshold: str = "0"
    identify_speakers: bool = False
    log_api_calls: bool = False
    save_input_wav_path: str = ""
    return_audio_data: bool = False


class SenseVoiceSettings(BaseModel):

    model_config = ConfigDict(extra="forbid")

    use_itn: bool = True
    cache_dir: str = DEFAULT_CACHE_DIR
    cpu_model_dir: str = ""
    cpu_threads: int = Field(default=1, ge=1)
    npu_model_dir: str = ""
    npu_window_s: Literal[5, 10, 15, 20, 25, 30] = 5
    npu_runtime_lib: str = ""
    npu_max_channels: int = Field(default=RK3588_NPU_CORES, ge=1, le=RK3588_NPU_CORES)
    npu_probe_timeout_s: float = Field(default=60.0, gt=0)


class STTSettings(BaseModel):

    model_config = ConfigDict(extra="forbid")

    model: Literal["sensevoice", "moonshine"] = "sensevoice"
    runtime: Literal["auto", "npu", "cpu"] = "auto"
    language: str = "ko"
    moonshine: MoonshineSettings = MoonshineSettings()
    sensevoice: SenseVoiceSettings = SenseVoiceSettings()
    partial_max_queue_depth: int = Field(default=0, ge=0)
    use_processes: bool = True
    process_start_timeout_s: float = Field(default=120.0, gt=0)

    @model_validator(mode="after")
    def _check_model_runtime(self) -> "STTSettings":
        if self.model == "moonshine" and self.runtime == "npu":
            raise ValueError('stt.model="moonshine" runs on CPU only; use runtime "cpu" or "auto"')
        if self.model == "sensevoice" and self.language not in SENSEVOICE_LANGUAGES:
            raise ValueError(
                f"stt.language={self.language!r} is not supported by SenseVoice "
                f"(supported: {', '.join(SENSEVOICE_LANGUAGES)})"
            )
        return self


class LoggingSettings(BaseModel):
    enabled: bool = True
    console_enabled: bool = True
    file_enabled: bool = True
    level: str = "INFO"
    is_json: bool = Field(default=True, alias="json")
    output_dir: str = "./logs"
    max_bytes: int = Field(default=10_000_000, gt=0)
    backup_count: int = Field(default=5, ge=0)


class WebUISettings(BaseModel):
    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)
    transcript_backlog: int = Field(default=50, ge=0)


class HealthSettings(BaseModel):
    stale_segment_warning_s: float = 30.0


class MetricsSettings(BaseModel):

    enabled: bool = True
    emit_interval_s: float = Field(default=10.0, gt=0)
    latency_log_decimals: int = Field(default=3, ge=0)


class ReliabilitySettings(BaseModel):

    enabled: bool = True
    tick_interval_s: float = Field(default=2.0, gt=0)
    stall_timeout_s: float = Field(default=10.0, gt=0)
    max_restarts: int = Field(default=3, ge=0)
    restart_window_s: float = Field(default=60.0, gt=0)
    watchdog_enabled: bool = True


class DumpSettings(BaseModel):

    enabled: bool = False
    output_dir: str = "./dumped_audio"
    segment_secs: float = 10.0


class SegmentDumpSettings(BaseModel):

    enabled: bool = False
    output_dir: str = "./dumped_vad_segments"


class QueuesSettings(BaseModel):

    ingest: int = 256
    routed: int = 128
    segment: int = 64
    dump: int = 256


class Settings(BaseSettings):

    mqtt: MQTTSettings = MQTTSettings()
    audio: AudioSettings = AudioSettings()
    repacketizer: RepacketizerSettings = RepacketizerSettings()
    vad: VADSettings = VADSettings()
    stt: STTSettings = STTSettings()
    logging_: LoggingSettings = Field(default=LoggingSettings(), alias="logging")
    webui: WebUISettings = WebUISettings()
    health: HealthSettings = HealthSettings()
    reliability: ReliabilitySettings = ReliabilitySettings()
    metrics: MetricsSettings = MetricsSettings()
    dump: DumpSettings = DumpSettings()
    segment_dump: SegmentDumpSettings = SegmentDumpSettings()
    queues: QueuesSettings = QueuesSettings()

    model_config = SettingsConfigDict(
        env_prefix="EDGE_VOICE_",
        env_nested_delimiter="__",
        populate_by_name=True,
        extra="forbid",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            _YamlConfigSettingsSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )

    @model_validator(mode="after")
    def _check_repacketizer_matches_vad(self) -> "Settings":
        expected_samples = self.repacketizer.outgoing_ms * self.audio.sample_rate / 1000.0
        if round(expected_samples) != self.vad.window_samples:
            raise ValueError(
                f"repacketizer.outgoing_ms ({self.repacketizer.outgoing_ms}ms) must "
                f"produce vad.window_samples ({self.vad.window_samples}) at "
                f"audio.sample_rate ({self.audio.sample_rate}Hz), got "
                f"{expected_samples} samples instead"
            )
        return self

    @model_validator(mode="after")
    def _check_npu_channel_cap(self) -> "Settings":
        stt, n_channels = self.stt, len(self.mqtt.channels)
        cap = stt.sensevoice.npu_max_channels
        if stt.model == "sensevoice" and stt.runtime == "npu" and n_channels > cap:
            raise ValueError(
                f"stt.runtime='npu' supports at most stt.sensevoice.npu_max_channels={cap} "
                f"channels (one per NPU core), but mqtt.channels has {n_channels}; "
                'use runtime "auto" to run the extra channels on CPU'
            )
        return self

    @classmethod
    def load(cls) -> "Settings":
        return cls()


class _YamlConfigSettingsSource(PydanticBaseSettingsSource):

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        return _load_config_files()


def _load_config_files() -> dict[str, Any]:
    configs = []
    for path in ["configs/default.yaml", "configs/local.yaml"]:
        p = Path(path)
        if p.exists():
            with open(p) as f:
                configs.append(yaml.safe_load(f) or {})

    merged: dict[str, Any] = {}
    for cfg in configs:
        merged = _deep_merge(merged, cfg)
    return merged


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
