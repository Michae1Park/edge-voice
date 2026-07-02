"""Fan-out packet copier that duplicates packets from one queue to two outputs.

Optionally calls a tracking callback on every forwarded packet for centralized
per-channel state (packet count, last-seen, bytes, etc.).
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import Any, Callable

logger = logging.getLogger(__name__)
QUEUE_GET_TIMEOUT_S = 0.2


class PacketCopier(threading.Thread):
    """Read packets from src_queue and push to both dst1 and dst2 queues.

    Args:
        track_callback: optional callable(packet) for centralized per-channel state.
    """

    def __init__(
        self,
        src_queue: queue.Queue,
        dst1_queue: queue.Queue,
        dst2_queue: queue.Queue,
        track_callback: Callable[[Any], None] | None = None,
    ) -> None:
        super().__init__(name="PacketCopier", daemon=False)
        self._src = src_queue
        self._dst1 = dst1_queue
        self._dst2 = dst2_queue
        self._track = track_callback
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info("PacketCopier started")
        while not self._stop_event.is_set():
            try:
                packet = self._src.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue

            # Track the packet centrally (one place for all per-channel state)
            if self._track is not None:
                try:
                    self._track(packet)
                except Exception:
                    pass  # don't break the pipeline if tracker misbehaves

            # Always forward to the main pipeline
            try:
                self._dst1.put(packet, timeout=0.2)
            except queue.Full:
                logger.warning("dst1 full -- dropping packet")

            # Copy to dump queue separately (ignore drops)
            try:
                self._dst2.put(packet, timeout=0.2)
            except queue.Full:
                logger.debug("Dump queue full -- dropping packet")

        logger.info("PacketCopier stopped")

    def stop(self) -> None:
        self._stop_event.set()
