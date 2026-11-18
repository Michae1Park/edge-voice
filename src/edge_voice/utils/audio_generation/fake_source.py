from __future__ import annotations

import logging
import queue
import threading
import time

from edge_voice.pipeline.models import AudioPacket

logger = logging.getLogger(__name__)

PACKET_INTERVAL_S = 0.02
FAKE_SAMPLE_PAYLOAD = b"\x00" * 640
PUT_TIMEOUT_S = 0.01


class FakeAudioSource(threading.Thread):
    def __init__(
        self,
        ingest_queue: "queue.Queue[AudioPacket]",
        channel_ids: list[str],
    ) -> None:
        super().__init__(name="FakeAudioSource", daemon=False)
        self._ingest_queue = ingest_queue
        self._channel_ids = channel_ids
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info("FakeAudioSource started for channels: %s", self._channel_ids)
        while not self._stop_event.is_set():
            for channel_id in self._channel_ids:
                if self._stop_event.is_set():
                    return
                packet = AudioPacket(
                    channel_id=channel_id,
                    timestamp=time.time(),
                    samples=FAKE_SAMPLE_PAYLOAD,
                )
                try:
                    self._ingest_queue.put(packet, timeout=PUT_TIMEOUT_S)
                except queue.Full:
                    logger.warning("ingest_queue full, dropping fake packet for %s", channel_id)
            self._stop_event.wait(PACKET_INTERVAL_S)
        logger.info("FakeAudioSource stopped")
