"""Bounded queues connecting pipeline stages.

Sizes are hardcoded for Milestone 0. Milestone 1 wires these up to
config.settings.Settings instead, so the queue-construction functions stay
the stable interface that callers (main.py, tests) depend on.
"""

from __future__ import annotations

import queue

from edge_voice.pipeline.models import AudioPacket, SpeechSegment

INGEST_QUEUE_MAXSIZE = 256
ROUTE_QUEUE_MAXSIZE = 128
SEGMENT_QUEUE_MAXSIZE = 64


def make_ingest_queue() -> "queue.Queue[AudioPacket]":
    """Queue for raw AudioPackets, between audio_ingest/audio_generation
    and channel routing (and, in Milestone 0, between the fake source and
    the fake router)."""
    return queue.Queue(maxsize=INGEST_QUEUE_MAXSIZE)


def make_routed_queue() -> "queue.Queue[AudioPacket]":
    """Queue for packets between channel router and VAD."""
    return queue.Queue(maxsize=ROUTE_QUEUE_MAXSIZE)


def make_segment_queue() -> "queue.Queue[SpeechSegment]":
    """Queue for finalized SpeechSegments, between vad and stt."""
    return queue.Queue(maxsize=SEGMENT_QUEUE_MAXSIZE)
