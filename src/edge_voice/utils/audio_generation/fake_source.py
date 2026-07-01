"""Milestone 0 fake audio source.

Stands in for the real audio_generation tools that arrive in Milestone 1:

    mic_source.py  -> captures from the system microphone
    wav_source.py  -> replays a .wav file at real-time pace

Both of those will publish over MQTT, tagged by channel_id, exactly like a
real call leg would. This fake version skips MQTT entirely and pushes
synthetic AudioPackets straight onto the ingest queue on a timer, just to
exercise the rest of the skeleton.
"""

from __future__ import annotations

import logging
import queue
import threading
import time

from edge_voice.pipeline.models import AudioPacket

logger = logging.getLogger(__name__)

PACKET_INTERVAL_S = 0.02
FAKE_SAMPLE_PAYLOAD = (
    b"\x00" * 640
)  # 16000 samples/sec × 0.020 sec = 320 samples; each sample is 16-bit: 320 samples × 2 bytes = 640 bytes
PUT_TIMEOUT_S = 0.01  # should be almost 0


class FakeAudioSource(threading.Thread):
    """Emits a fake AudioPacket for each configured channel every
    PACKET_INTERVAL_S seconds, round-robin, until stopped."""

    def __init__(
        self,
        ingest_queue: "queue.Queue[AudioPacket]",
        channel_ids: list[str],
    ) -> None:
        super().__init__(name="FakeAudioSource", daemon=False)  # non-daemon; critical
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
