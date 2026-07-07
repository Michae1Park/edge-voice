"""Bounded queues connecting pipeline stages.

Queue sizes come from ``Settings.queues`` (defaults in code), configurable
via ``configs/default.yaml`` under the ``queues`` key, or via
``EDGE_VOICE__QUEUE__INGEST`` etc.
"""

from __future__ import annotations

import queue

from edge_voice.config.settings import Settings
from edge_voice.pipeline.models import AudioPacket, SpeechSegment

_settings: Settings | None = None


def _get_settings() -> Settings:
    """Load Settings eagerly (first call) or return cached instance."""
    global _settings
    if _settings is None:
        _settings = Settings.load()
    return _settings


def _maxsize(key: str, maxsize: int | None = None) -> int:
    return maxsize or getattr(_get_settings().queues, key)


def make_ingest_queue(maxsize: int | None = None) -> "queue.Queue[AudioPacket]":
    """Queue for raw AudioPackets, between audio_ingest/audio_generation
    and channel routing (and, in Milestone 0, between the fake source and
    the fake router)."""
    return queue.Queue(maxsize=_maxsize("ingest", maxsize))


def make_routed_queue(maxsize: int | None = None) -> "queue.Queue[AudioPacket]":
    """Queue for packets between channel router and VAD."""
    return queue.Queue(maxsize=_maxsize("routed", maxsize))


def make_segment_queue(maxsize: int | None = None) -> "queue.Queue[SpeechSegment]":
    """Queue for finalized SpeechSegments, between vad and stt."""
    return queue.Queue(maxsize=_maxsize("segment", maxsize))


def make_dump_queue(maxsize: int | None = None) -> "queue.Queue[AudioPacket]":
    """Fan-out queue for dumping/persisting all routed packets separately."""
    return queue.Queue(maxsize=_maxsize("dump", maxsize))
