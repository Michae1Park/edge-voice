"""Standalone microphone audio source for testing/dev.

Captures from the system microphone, converts to the format expected by
edge_voice pipeline, and publishes audio chunks to an ingest queue or
MQTT broker tagged with a channel_id.

Run standalone in its own terminal:
    python mic_source.py --output-queue  # push to in-memory queue
    python mic_source.py --mqtt           # push to MQTT broker

This is NOT imported by cli.py or pipeline/orchestrator.py. It's a separate
process, just like a real call leg would be.
"""

from __future__ import annotations

import argparse
from typing import Any
import logging
import queue
import threading
import time

logger = logging.getLogger(__name__)

# Audio format: 16-bit PCM, mono, 16kHz (matches default YAML config)
SAMPLE_RATE = 16000
CHUNK_SAMPLES = 320  # 20ms chunks (160 bytes @ 16bit)
PACKET_PUT_TIMEOUT = 0.01


class MicSource(threading.Thread):
    """Captures from the system microphone and pushes chunks onto a queue."""

    def __init__(
        self,
        ingest_queue: queue.Queue,
        channel_ids: list[str],
        device_index: int | None = None,
    ) -> None:
        super().__init__(name="MicSource", daemon=True)
        self._ingest_queue = ingest_queue
        self._channel_ids = channel_ids
        self._device_index = device_index
        self._stopped = threading.Event()

    def run(self) -> None:
        """Capture audio and push to ingest queue."""
        import sounddevice as sd

        logger.info("MicSource: initializing sounddevice...")

        if self._device_index is not None:
            device_count = len(sd.query_devices())
            if self._device_index >= device_count:
                raise ValueError(
                    f"Device index {self._device_index} out of range (0-{device_count - 1})"
                )
            logger.info("MicSource: using device %d", self._device_index)
        else:
            for i, info in enumerate(sd.query_devices()):
                logger.info(
                    "  Device %d: %s (%d in, %d out)",
                    i,
                    info["name"],
                    info["max_input_channels"],
                    info["max_output_channels"],
                )

        stream = sd.RawInputStream(
            samplerate=SAMPLE_RATE,
            channels=1,
            dtype="int16",
            blocksize=CHUNK_SAMPLES,
            device=self._device_index,
        )
        stream.start()
        logger.info("MicSource: capturing on %s", self._channel_ids)

        packet_num = 0
        try:
            while not self._stopped.is_set():
                raw, _overflowed = stream.read(CHUNK_SAMPLES)
                chunk = _raw_to_numpy(bytes(raw))

                for channel_id in self._channel_ids:
                    packet = _create_packet(channel_id, chunk)
                    try:
                        self._ingest_queue.put(packet, timeout=PACKET_PUT_TIMEOUT)
                        packet_num += 1
                    except queue.Full:
                        logger.warning("ingest_queue full, dropping packet for %s", channel_id)

                if packet_num % 50 == 0 and packet_num > 0:
                    logger.debug("MicSource: sent %d packets", packet_num)

        except Exception as e:
            logger.error("MicSource: error: %s", e)
        finally:
            stream.stop()
            stream.close()
            logger.info("MicSource: stopped after %d packets", packet_num)

    def stop(self) -> None:
        self._stopped.set()

    def is_alive(self) -> bool:
        return not self._stopped.is_set()


def _raw_to_numpy(raw: bytes) -> Any:
    """Convert raw audio bytes to numpy int16 array."""
    import numpy as np

    return np.frombuffer(raw, dtype=np.int16)


def _create_packet(
    channel_id: str,
    samples,  # type: ignore
    timestamp: float | None = None,
) -> Any:
    """Convert numpy array to AudioPacket."""
    from edge_voice.pipeline.models import AudioPacket

    return AudioPacket(
        channel_id=channel_id,
        timestamp=timestamp or time.time(),
        samples=samples.tobytes(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="MicSource standalone test")
    parser.add_argument(
        "-d", "--device", type=int, default=None, help="sounddevice device index (default: auto)"
    )
    parser.add_argument(
        "-c", "--channel", type=str, default="ch1", help="Channel ID to tag packets with"
    )
    parser.add_argument("-q", "--queue-type", choices=["memory", "mqtt"], default="memory")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    if args.queue_type == "memory":
        q: queue.Queue[Any] = queue.Queue(maxsize=256)
        source = MicSource(q, [args.channel], args.device)
        source.run()
    else:
        logger.info("MQTT mode not yet implemented")


if __name__ == "__main__":
    main()
