"""In-process pub/sub for TranscriptEvents, feeding the web UI's live feed.

Separate from fanout.py: that helper fans an item out to a fixed one-or-two
queues wired at build time, but the UI's subscriber set changes on every
browser connect/disconnect, so it needs registration instead of a fixed
destination. See docs/BUILDPLAN.md Milestone 5.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections import deque

from edge_voice.pipeline.models import TranscriptEvent

logger = logging.getLogger(__name__)

DEFAULT_BACKLOG = 50
DEFAULT_SUBSCRIBER_MAXSIZE = 200
PUT_TIMEOUT_S = 0.1


class TranscriptHub:
    """Fans out TranscriptEvents to every currently-registered subscriber.

    subscribe() pre-seeds the returned queue with the recent backlog, so a
    browser that just (re)connected isn't blank while waiting for the next
    segment -- a bare shared queue was considered and rejected for this: it
    either drops everything published while a client was detached, or hands
    a stale backlog to whichever client reconnects first.
    """

    def __init__(self, backlog: int = DEFAULT_BACKLOG) -> None:
        self._backlog: deque[TranscriptEvent] = deque(maxlen=backlog)
        self._subscribers: set[queue.Queue[TranscriptEvent]] = set()
        self._lock = threading.Lock()

    def publish(self, event: TranscriptEvent) -> None:
        with self._lock:
            self._backlog.append(event)
            subscribers = list(self._subscribers)
        for sub in subscribers:
            try:
                sub.put(event, timeout=PUT_TIMEOUT_S)
            except queue.Full:
                # A slow/disconnected browser tab shouldn't apply backpressure
                # to STTWorker; drop for that one subscriber and move on, same
                # drop-and-log philosophy as fanout_put.
                logger.warning("webui subscriber queue full -- dropping transcript")

    def subscribe(self) -> "queue.Queue[TranscriptEvent]":
        """Register a new subscriber, pre-seeded with the current backlog."""
        sub: "queue.Queue[TranscriptEvent]" = queue.Queue(maxsize=DEFAULT_SUBSCRIBER_MAXSIZE)
        with self._lock:
            for event in self._backlog:
                sub.put_nowait(event)
            self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: "queue.Queue[TranscriptEvent]") -> None:
        with self._lock:
            self._subscribers.discard(sub)
