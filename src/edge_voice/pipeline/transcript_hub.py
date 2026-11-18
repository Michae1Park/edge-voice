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

    def __init__(self, backlog: int = DEFAULT_BACKLOG) -> None:
        self._backlog: deque[TranscriptEvent] = deque(maxlen=backlog)
        self._subscribers: set[queue.Queue[TranscriptEvent]] = set()
        self._lock = threading.Lock()

    def publish(self, event: TranscriptEvent) -> None:
        with self._lock:
            if event.is_final:
                self._backlog.append(event)
            subscribers = list(self._subscribers)
        for sub in subscribers:
            try:
                sub.put(event, timeout=PUT_TIMEOUT_S)
            except queue.Full:
                logger.warning("webui subscriber queue full -- dropping transcript")

    def subscribe(self) -> "queue.Queue[TranscriptEvent]":
        sub: "queue.Queue[TranscriptEvent]" = queue.Queue(maxsize=DEFAULT_SUBSCRIBER_MAXSIZE)
        with self._lock:
            for event in sorted(self._backlog, key=lambda e: e.start):
                sub.put_nowait(event)
            self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: "queue.Queue[TranscriptEvent]") -> None:
        with self._lock:
            self._subscribers.discard(sub)

    def clear(self) -> None:
        with self._lock:
            self._backlog.clear()
