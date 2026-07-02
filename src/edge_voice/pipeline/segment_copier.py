"""Fan-out segment copier that duplicates SpeechSegments from one queue to two outputs.

Used between VAD and STT to send segment copies to a debug audio dump worker.
Mirrors the PacketCopier pattern used for routing raw audio packets.
"""

from __future__ import annotations

import logging
import queue
import threading

from edge_voice.pipeline.models import SpeechSegment

logger = logging.getLogger(__name__)
QUEUE_GET_TIMEOUT_S = 0.2


class SegmentCopier(threading.Thread):
    """Read SpeechSegments from src_queue and push to both dst1 and dst2 queues.

    Args:
        track_callback: optional callable(segment) for centralized per-segment state.
    """

    def __init__(
        self,
        src_queue: queue.Queue[SpeechSegment],
        dst1_queue: queue.Queue[SpeechSegment],
        dst2_queue: queue.Queue[SpeechSegment],
        track_callback=None,
    ) -> None:
        super().__init__(name="SegmentCopier", daemon=False)
        self._src = src_queue
        self._dst1 = dst1_queue
        self._dst2 = dst2_queue
        self._track = track_callback
        self._stop_event = threading.Event()

    def run(self) -> None:
        logger.info("SegmentCopier started")
        while not self._stop_event.is_set():
            try:
                seg = self._src.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue

            if self._track is not None:
                try:
                    self._track(seg)
                except Exception:
                    pass

            # Both pipes are non-blocking for debugging: if STT or dump can't keep up,
            # just drop instead of stalling the entire VAD thread.
            try:
                self._dst1.put(seg, timeout=0)
            except queue.Full:
                # STT couldn't consume quickly enough - just drop
                pass

            try:
                self._dst2.put(seg, timeout=0)
            except queue.Full:
                # Dump queue full - drop
                pass

        logger.info("SegmentCopier stopped")

    def stop(self) -> None:
        self._stop_event.set()
