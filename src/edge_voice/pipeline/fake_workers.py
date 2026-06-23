"""Milestone 0 fake workers.

These exist purely to prove the queue/worker skeleton works end-to-end
before any real audio_ingest, channel routing, vad, or stt exists. Every
worker here ignores actual audio content and fabricates its output. Each
fake worker stands in for a specific future package:

    FakeRouter     -> channel.router        (Milestone 2)
    FakeVADWorker  -> vad.worker             (Milestone 3)
    FakeSTTWorker  -> stt.worker             (Milestone 4)

When the real implementation lands, the corresponding Fake* class is
deleted and main.py swaps the import — the queue contracts (what goes in,
what comes out) are designed to stay the same.
"""
from __future__ import annotations

import itertools
import logging
import queue
import threading
from typing import Callable

from pipeline.models import AudioPacket, SpeechSegment, TranscriptEvent

logger = logging.getLogger(__name__)

# How many packets to "collect" per channel before fabricating a segment.
PACKETS_PER_FAKE_SEGMENT = 3
FAKE_SEGMENT_DURATION_S = 1.5

# How long each queue.get() blocks before re-checking the stop event.
POLL_INTERVAL_S = 0.2


class StoppableWorker(threading.Thread):
    """Base class for the fake-worker threads.

    Centralizes the stop_event + queue.get(timeout=...) pattern so each
    worker's run() loop can notice shutdown promptly instead of blocking
    forever on an empty queue. Real workers (Milestones 2-4) should follow
    the same pattern.
    """

    def __init__(self, name: str) -> None:
        super().__init__(name=name, daemon=False)
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()


class FakeRouter(StoppableWorker):
    """Stand-in for `channel.router`.

    Real routing (Milestone 2) will validate channel_id against known
    channels and track per-channel freshness for health reporting. For now
    it just passes packets straight through untouched, so the rest of the
    skeleton has something to consume.
    """

    def __init__(
        self,
        ingest_queue: "queue.Queue[AudioPacket]",
        routed_queue: "queue.Queue[AudioPacket]",
    ) -> None:
        super().__init__(name="FakeRouter")
        self._ingest_queue = ingest_queue
        self._routed_queue = routed_queue

    def run(self) -> None:
        logger.info("FakeRouter started")
        while not self.stopping:
            try:
                packet = self._ingest_queue.get(timeout=POLL_INTERVAL_S)
            except queue.Empty:
                continue
            self._routed_queue.put(packet)
        logger.info("FakeRouter stopped")


class FakeVADWorker(StoppableWorker):
    """Stand-in for `vad.worker`.

    Emits a fixed-length fake SpeechSegment after collecting
    PACKETS_PER_FAKE_SEGMENT packets on a given channel. Maintains a
    per-channel packet buffer, mirroring the per-channel state pattern the
    real shared-VAD worker will need in Milestone 3.
    """

    def __init__(
        self,
        routed_queue: "queue.Queue[AudioPacket]",
        segment_queue: "queue.Queue[SpeechSegment]",
    ) -> None:
        super().__init__(name="FakeVADWorker")
        self._routed_queue = routed_queue
        self._segment_queue = segment_queue
        self._buffers: dict[str, list[AudioPacket]] = {}
        self._segment_counter = itertools.count(1)

    def run(self) -> None:
        logger.info("FakeVADWorker started")
        while not self.stopping:
            try:
                packet = self._routed_queue.get(timeout=POLL_INTERVAL_S)
            except queue.Empty:
                continue

            buf = self._buffers.setdefault(packet.channel_id, [])
            buf.append(packet)

            if len(buf) >= PACKETS_PER_FAKE_SEGMENT:
                start = buf[0].timestamp
                end = buf[-1].timestamp
                if end <= start:
                    end = start + FAKE_SEGMENT_DURATION_S
                segment = SpeechSegment(
                    channel_id=packet.channel_id,
                    start=start,
                    end=end,
                    audio=b"".join(p.samples for p in buf),
                    segment_id=f"fake-seg-{next(self._segment_counter)}",
                )
                self._buffers[packet.channel_id] = []
                self._segment_queue.put(segment)
        logger.info("FakeVADWorker stopped")


class FakeSTTWorker(StoppableWorker):
    """Stand-in for `stt.worker`.

    Emits a canned TranscriptEvent for every SpeechSegment it receives, via
    an on_transcript callback rather than another queue — Milestone 0 has
    nothing downstream of transcripts except logging.
    """

    def __init__(
        self,
        segment_queue: "queue.Queue[SpeechSegment]",
        on_transcript: Callable[[TranscriptEvent], None],
    ) -> None:
        super().__init__(name="FakeSTTWorker")
        self._segment_queue = segment_queue
        self._on_transcript = on_transcript

    def run(self) -> None:
        logger.info("FakeSTTWorker started")
        while not self.stopping:
            try:
                segment = self._segment_queue.get(timeout=POLL_INTERVAL_S)
            except queue.Empty:
                continue

            event = TranscriptEvent(
                channel_id=segment.channel_id,
                segment_id=segment.segment_id,
                text="[fake transcript]",
                start=segment.start,
                end=segment.end,
            )
            self._on_transcript(event)
        logger.info("FakeSTTWorker stopped")
