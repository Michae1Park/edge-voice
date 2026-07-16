"""
Channel-aware audio router.

Consumes AudioPackets from the ingest queue, validates and tags each with its
channel_id, maintains per-channel bookkeeping (last-seen timestamp for
freshness checks), and forwards packets to the routed queue for downstream
VAD/STT consumption. Optionally copies packets to a dump queue for debugging.
"""

from __future__ import annotations

import logging
import queue
import threading
import time

from edge_voice.pipeline.fanout import fanout_put
from edge_voice.pipeline.models import AudioPacket

logger = logging.getLogger(__name__)

QUEUE_GET_TIMEOUT_S = 0.2
QUEUE_PUT_TIMEOUT_S = 0.2


class ChannelRouter(threading.Thread):
    """Validates and routes AudioPackets from ingest queue to the routed queue."""

    def __init__(
        self,
        ingest_queue: queue.Queue[AudioPacket],
        routed_queue: queue.Queue[AudioPacket],
        channel_ids: list[str],
        dump_queue: queue.Queue[AudioPacket] | None = None,
    ) -> None:
        super().__init__(name="ChannelRouter", daemon=False)
        self._ingest_queue = ingest_queue
        self._routed_queue = routed_queue
        self._dump_queue = dump_queue
        self._channel_ids = set(channel_ids)
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._channel_last_seen: dict[str, float] = {}

    def run(self) -> None:
        logger.info("ChannelRouter started for channels: %s", sorted(self._channel_ids))
        while not self._stop_event.is_set():
            try:
                packet = self._ingest_queue.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue

            if packet.channel_id not in self._channel_ids:
                logger.warning("Unknown channel_id %s -- dropping packet", packet.channel_id)
                continue

            self._mark_seen(packet.channel_id)
            fanout_put(
                packet, self._routed_queue, self._dump_queue, put_timeout=QUEUE_PUT_TIMEOUT_S
            )

        logger.info("ChannelRouter stopped")

    def stop(self) -> None:
        """Signal the router to stop."""
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def _mark_seen(self, channel_id: str) -> None:
        with self._lock:
            self._channel_last_seen[channel_id] = time.time()

    def get_freshness(self, channel_id: str) -> float | None:
        """Return seconds since last seen packet for a channel, or None."""
        with self._lock:
            last = self._channel_last_seen.get(channel_id)
        if last is None:
            return None
        return time.time() - last

    def get_channel_ids(self) -> list[str]:
        """Return the list of known valid channel IDs."""
        return sorted(self._channel_ids)
