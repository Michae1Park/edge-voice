"""Generic queue copier: fans packets from one queue to two outputs.

Used for both raw audio (PacketCopier) and speech segments (SegmentCopier)
with configurable put timeouts.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import Callable, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)
QUEUE_GET_TIMEOUT_S = 0.2

_DEFAULT_PUT_TIMEOUT = 0.2


class QueueCopier(threading.Thread):
    """Read items from *src_queue* and push to both *dst1_queue* and *dst2_queue*.

    Args:
        track_callback: optional callable(item) for per-item side effects
                        (packet tracking, metrics, etc).
        put_timeout:    timeout in seconds for dst puts. 0 = strictly non-blocking.
    """

    def __init__(
        self,
        src_queue: queue.Queue[T],
        dst1_queue: queue.Queue[T],
        dst2_queue: queue.Queue[T],
        track_callback: Callable[[T], None] | None = None,
        put_timeout: float = _DEFAULT_PUT_TIMEOUT,
    ) -> None:
        super().__init__(name="QueueCopier", daemon=False)
        self._src = src_queue
        self._dst1 = dst1_queue
        self._dst2 = dst2_queue
        self._track = track_callback
        self._put_timeout = put_timeout
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info("QueueCopier started")
        while not self._stop_event.is_set():
            try:
                item = self._src.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue

            if self._track is not None:
                try:
                    self._track(item)
                except Exception:
                    pass  # don't break the pipeline if callback misbehaves

            # Always forward to the main pipeline
            try:
                self._dst1.put(item, timeout=self._put_timeout)
            except queue.Full:
                logger.warning("dst1 full -- dropping item")

            # Copy to dump queue separately (ignore drops)
            try:
                self._dst2.put(item, timeout=self._put_timeout)
            except queue.Full:
                logger.debug("Dump queue full -- dropping item")

        logger.info("QueueCopier stopped")

    def stop(self) -> None:
        self._stop_event.set()
