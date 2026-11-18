from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class AudioPacket:

    channel_id: str
    timestamp: float
    samples: bytes


@dataclass(slots=True)
class SpeechSegment:

    channel_id: str
    start: float
    end: float
    audio: bytes
    segment_id: str
    is_partial: bool = False


@dataclass(slots=True)
class TranscriptEvent:

    channel_id: str
    segment_id: str
    text: str
    start: float
    end: float
    created_at: float = field(default_factory=time.time)
    is_final: bool = True
