"""
Typed application configuration, loaded from YAML + environment overrides.

Design intent (see docs/design.md "Configuration"):
- One pydantic-settings model is the single source of truth for all tunable
  parameters (VAD thresholds, segment timing, MQTT topics, model paths, etc).
- Defaults live in code; overrides come from config/*.yaml, then env vars
  (EDGE_VOICE__SECTION__FIELD=value), so behavior is reproducible and
  diffable across deployments.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings


class MQTTChannels(BaseModel):
    topic: str
    channel_id: str


class MQTTSettings(BaseModel):
    broker_host: str = "localhost"
    broker_port: int = 1883
    channels: list[MQTTChannels] = Field(
        default_factory=lambda: [
            MQTTChannels(topic="stt/audio_chunks1", channel_id="ch1"),
            MQTTChannels(topic="stt/audio_chunks2", channel_id="ch2"),
        ]
    )


class AudioSettings(BaseModel):
    sample_rate: int = 16000
    channels: int = 1
    format: str = "int16"

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        valid = {"int16", "int32", "float32"}
        if v not in valid:
            raise ValueError(f"audio.format must be one of {valid}, got {v!r}")
        return v


class VADSettings(BaseModel):
    threshold: float = 0.5
    sampling_rate: int = 16000
    window_samples: int = 512
    max_segment_s: float = 7.0
    soft_cut_s: float = 5.0
    soft_cut_lookahead_s: float = 1.0
    soft_cut_min_dip: float = 0.10
    min_silence_ms: int = 300


class STTSettings(BaseModel):
    language: str = "ko"
    model: str = "tiny-ko"
    model_arch: int = 0
    feed_windows: int = 64
    max_tokens_per_second: str = "13.0"
    identify_speakers: bool = False
    log_api_calls: bool = False
    save_input_wav_path: str = ""
    return_audio_data: bool = False

    @field_validator("feed_windows", mode="before")
    @classmethod
    def validate_feed_windows(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = int(v)
        if v <= 0:
            raise ValueError("stt.feed_windows must be > 0")
        return v

    @field_validator(
        "identify_speakers",
        "log_api_calls",
        "return_audio_data",
        mode="before",
    )
    @classmethod
    def validate_bool_or_str(cls, v: Any) -> Any:
        if isinstance(v, str):
            if v.lower() == "true":
                return True
            if v.lower() == "false":
                return False
        return v


class SourceSettings(BaseModel):
    sample_rate: int = 16000
    default_audio: str = ""


class LoggingSettings(BaseModel):
    level: str = "INFO"
    is_json: bool = Field(default=True, alias="json")


class WebUISettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("webui.port must be 1-65535")
        return v


class HealthSettings(BaseModel):
    stale_segment_warning_s: float = 30.0


class Settings(BaseSettings):
    """Top-level application settings.

    Loading order (later overrides earlier):
    1. Code defaults (model field defaults)
    2. configs/default.yaml
    3. configs/local.yaml (gitignored)
    4. Environment variables (EDGE_VOICE__<SECTION>__<FIELD>)

    Usage:
        settings = Settings.load()
        sample_rate = settings.audio.sample_rate
    """

    mqtt: MQTTSettings = MQTTSettings()
    audio: AudioSettings = AudioSettings()
    vad: VADSettings = VADSettings()
    stt: STTSettings = STTSettings()
    source: SourceSettings = SourceSettings()
    logging_: LoggingSettings = Field(default=LoggingSettings(), alias="logging")
    webui: WebUISettings = WebUISettings()
    health: HealthSettings = HealthSettings()

    model_config = ConfigDict(
        env_prefix="EDGE_VOICE_",
        env_nested_delimiter="__",
        populate_by_name=True,
    )

    @classmethod
    def load(cls) -> "Settings":
        """Load settings with layered overrides."""
        base = cls()  # defaults

        # Load merged YAML overrides
        merged = _load_config_files()
        if merged:
            base = cls(**merged)

        return base


def _load_config_files() -> dict[str, Any]:
    """Load and merge all local YAML config files."""
    configs = []
    for path in ["configs/default.yaml", "configs/local.yaml"]:
        p = Path(path)
        if p.exists():
            with open(p) as f:
                configs.append(yaml.safe_load(f) or {})

    # Deep merge: later configs override earlier
    merged: dict[str, Any] = {}
    for cfg in configs:
        merged = _deep_merge(merged, cfg)
    return merged


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override into base."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
