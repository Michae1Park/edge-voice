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
from typing import Any, Literal

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    """Re-packetizes incoming audio frames to a fixed outgoing frame size
    before they hit the VAD/STT pipeline (e.g. 20ms in -> 32ms out).

    sample_rate is intentionally NOT a field here -- it always follows
    audio.sample_rate (see Settings._check_repacketizer_matches_vad), so
    there's exactly one place to change the pipeline's sample rate.
    """

    incoming_ms: float = Field(default=20.0, gt=0)
    outgoing_ms: float = Field(default=32.0, gt=0)
    bytes_per_sample: int = Field(default=2, ge=1)


class VADSettings(BaseModel):
    """VADWorker tuning. sample_rate is intentionally NOT a field here -- it
    always follows audio.sample_rate, same rationale as RepacketizerSettings.

    soft_cut_s/soft_cut_lookahead_s/soft_cut_min_dip/max_segment_s are the
    planned confidence-dip and hard-cut segment-length limits from
    docs/BUILDPLAN.md milestone 3; VADWorker doesn't implement them yet.
    """

    threshold: float = 0.5
    window_samples: int = 512
    rms_gate_enabled: bool = True
    silence_rms_floor: float = 0.01  # CALIBRATE: normalized float32 RMS, not raw int16
    preroll_chunks: int = 3
    min_silence_duration_ms: int = 100  # Silero VADIterator: silence needed before `end` fires
    speech_pad_ms: int = 30  # Silero VADIterator: padding appended around detected speech
    max_segment_s: float = 7.0
    soft_cut_s: float = 5.0
    soft_cut_lookahead_s: float = 1.0
    soft_cut_min_dip: float = 0.10
    min_silence_ms: int = 300


class STTSettings(BaseModel):
    language: str = "ko"
    model: str = "tiny-ko"
    model_arch: int = 0
    feed_windows: int = Field(default=64, gt=0)
    max_tokens_per_second: str = "13.0"
    identify_speakers: bool = False
    log_api_calls: bool = False
    save_input_wav_path: str = ""
    return_audio_data: bool = False


class LoggingSettings(BaseModel):
    level: str = "INFO"
    is_json: bool = Field(default=True, alias="json")


class WebUISettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = Field(default=8080, ge=1, le=65535)


class HealthSettings(BaseModel):
    stale_segment_warning_s: float = 30.0


class DumpSettings(BaseModel):
    """AudioDumpWorker configuration for debugging/verification."""

    enabled: bool = False
    output_dir: str = "./dumped_audio"
    segment_secs: float = 10.0


class SegmentDumpSettings(BaseModel):
    """SegmentAudioDumpWorker configuration for debugging VAD output."""

    enabled: bool = False
    output_dir: str = "./dumped_vad_segments"


class QueuesSettings(BaseModel):
    """Queue sizes for pipeline stages."""

    ingest: int = 256
    routed: int = 128
    segment: int = 64
    dump: int = 256


class Settings(BaseSettings):
    """Top-level application settings.

    Loading order (later overrides earlier):
    1. Code defaults (model field defaults)
    2. configs/default.yaml
    3. configs/local.yaml (gitignored)
    4. Environment variables (EDGE_VOICE__SECTION__<FIELD>)

    Usage:
        settings = Settings.load()
        sample_rate = settings.audio.sample_rate
    """

    mqtt: MQTTSettings = MQTTSettings()
    audio: AudioSettings = AudioSettings()
    repacketizer: RepacketizerSettings = RepacketizerSettings()
    vad: VADSettings = VADSettings()
    stt: STTSettings = STTSettings()
    logging_: LoggingSettings = Field(default=LoggingSettings(), alias="logging")
    webui: WebUISettings = WebUISettings()
    health: HealthSettings = HealthSettings()
    dump: DumpSettings = DumpSettings()
    segment_dump: SegmentDumpSettings = SegmentDumpSettings()
    queues: QueuesSettings = QueuesSettings()

    # For overriding configs. e.g. Docker/Kubernetes env vars
    model_config = SettingsConfigDict(
        env_prefix="EDGE_VOICE_",
        env_nested_delimiter="__",
        populate_by_name=True,
        extra="forbid",
    )

    @model_validator(mode="after")
    def _check_repacketizer_matches_vad(self) -> "Settings":
        """repacketizer.outgoing_ms must produce exactly vad.window_samples
        at audio.sample_rate, or VAD will receive mis-sized frames."""
        expected_samples = self.repacketizer.outgoing_ms * self.audio.sample_rate / 1000.0
        if round(expected_samples) != self.vad.window_samples:
            raise ValueError(
                f"repacketizer.outgoing_ms ({self.repacketizer.outgoing_ms}ms) must "
                f"produce vad.window_samples ({self.vad.window_samples}) at "
                f"audio.sample_rate ({self.audio.sample_rate}Hz), got "
                f"{expected_samples} samples instead"
            )
        return self

    @classmethod
    def load(cls) -> "Settings":
        """Load settings with layered overrides."""
        merged = _load_config_files()
        return cls.model_validate(merged)


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
