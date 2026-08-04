"""Shared dataclasses used across pipeline stages.

These live here (rather than inside audio_ingest/channel/vad/stt) so every
stage can import one shared vocabulary without depending on each other
directly. See BUILD_PLAN.md "Package map" for the rationale.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(slots=True)
class AudioPacket:
    """A chunk of raw audio tagged with its source channel.

    Produced by audio_ingest (real) or tools/audio_generation (test/dev).
    """

    channel_id: str
    timestamp: float
    samples: bytes


@dataclass(slots=True)
class SpeechSegment:
    """A finalized span of speech on one channel, ready for STT.

    Produced by vad.worker.
    """

    channel_id: str
    start: float
    end: float
    audio: bytes
    segment_id: str
    # True for an in-progress *prefix* of a segment, emitted mid-speech so the
    # UI can show text before the speaker stops. A partial carries the same
    # segment_id as the eventual final, is never dumped, and is droppable --
    # STT skips it under backlog. Finals are never partial, never dropped.
    is_partial: bool = False


@dataclass(slots=True)
class TranscriptEvent:
    """A transcribed segment, ready for logging / UI / persistence.

    Produced by stt.worker.
    """

    channel_id: str
    segment_id: str
    text: str
    start: float
    end: float
    created_at: float = field(default_factory=time.time)
    # False for the revisable transcript of an in-progress segment. Consumers
    # keyed on segment_id should replace a non-final event in place; exactly
    # one is_final=True event ever closes a given segment_id. Observability
    # (STT latency, the TRANSCRIPT log line) counts finals only -- a partial
    # transcribes a prefix, so mixing the two would make latency meaningless.
    is_final: bool = True
