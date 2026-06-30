"""Standalone WAV file audio source for testing/dev.

Reads a .wav file, converts to the format expected by edge_voice pipeline,
and publishes audio chunks to an ingest queue or MQTT broker tagged with a
channel_id.

Run standalone in its own terminal:
    python wav_source.py --file test.wav --output-queue  # push to in-memory queue
    python wav_source.py --file test.wav --mqtt           # push to MQTT broker

This is NOT imported by cli.py or pipeline/orchestrator.py. It's a separate
process, just like a real call leg would be.
"""

from __future__ import annotations

import argparse
from typing import Any
import logging
import math
import queue
import struct
import threading
import time

import numpy as np

logger = logging.getLogger(__name__)

# Audio format: 16-bit PCM, mono, 16kHz (matches default YAML config)
TARGET_SAMPLE_RATE = 16000
CHUNK_SAMPLES = 320  # 20ms chunks (160 bytes @ 16bit)
PACKET_PUT_TIMEOUT = 0.01


def open_wav(path: str):
    """Open and parse a WAV file.

    Returns: (sample_rate, num_channels, block_align, num_frames, data)
    """
    with open(path, "rb") as f:
        # Read RIFF header
        riff = f.read(4)
        if riff != b"RIFF":
            raise ValueError(f"Invalid RIFF header: {riff!r}")

        struct.unpack("<I", f.read(4))
        wave = f.read(4)
        if wave != b"WAVE":
            raise ValueError(f"Invalid WAVE header: {wave!r}")

        # Read format chunk
        while True:
            header = f.read(8)
            if len(header) < 8:
                raise ValueError("Reached end of file before finding data chunk")
            chunk_id, chunk_len = struct.unpack("<4sI", header)
            if chunk_id == b"fmt ":
                fmt = struct.unpack("<H", f.read(2))[0]
                if fmt != 1:
                    raise ValueError(f"Unsupported WAV format: {fmt}")
                channels = struct.unpack("<H", f.read(2))[0]
                sample_rate = struct.unpack("<I", f.read(4))[0]
                _byte_rate = struct.unpack("<I", f.read(4))[0]
                block_align = struct.unpack("<H", f.read(2))[0]
                bits_per_sample = struct.unpack("<H", f.read(2))[0]
                if bits_per_sample != 16:
                    raise ValueError(f"Unsupported bit depth: {bits_per_sample}")
                # Keep looking for the data chunk
            elif chunk_id == b"data":
                break
            else:
                f.read(chunk_len)

        if chunk_id != b"data":
            raise ValueError(f"Expected 'data' chunk, got {chunk_id!r}")
        sample_data = bytearray(chunk_len)
        f.readinto(sample_data)

    data = np.frombuffer(sample_data, dtype=np.int16)
    return sample_rate, channels, block_align, len(data) // block_align, data


def resample(data: np.ndarray, src_rate: int, dst_rate: int) -> np.ndarray:
    """Simple linear interpolation resampling."""
    if src_rate == dst_rate:
        return data

    duration = len(data) / src_rate
    dst_samples = int(duration * dst_rate)
    indices = np.linspace(0, len(data) - 1, dst_samples)
    resampled = np.interp(indices, np.arange(len(data)), data.astype(float))
    return resampled.astype(np.int16)


class WavSource(threading.Thread):
    """Parses a .wav file and pushes 20ms audio packets onto a queue."""

    def __init__(
        self,
        ingest_queue: queue.Queue,
        channel_ids: list[str],
        wav_path: str,
    ) -> None:
        super().__init__(name="WavSource", daemon=True)
        self._ingest_queue = ingest_queue
        self._channel_ids = channel_ids
        self._wav_path = wav_path
        self._stopped = threading.Event()

    def run(self) -> None:
        """Read WAV file and push packets."""
        logger.info("WavSource: reading %s...", self._wav_path)
        sample_rate, channels, block_align, num_frames, data = open_wav(self._wav_path)

        # Resample if needed
        if sample_rate != TARGET_SAMPLE_RATE:
            logger.info("WavSource: resampling %d -> %d Hz", sample_rate, TARGET_SAMPLE_RATE)
            data = resample(data, sample_rate, TARGET_SAMPLE_RATE)
            sample_rate = TARGET_SAMPLE_RATE

        # Convert stereo to mono if needed
        if channels == 2:
            mono = (data[0::2] + data[1::2]) // 2
            # Handle odd length
            if len(data) % 2 == 1:
                mono = np.append(mono, data[-1] // 2)
            data = mono

        total_chunks = math.ceil(len(data) / CHUNK_SAMPLES)
        start_time = time.time()

        packet_num = 0
        for i in range(0, len(data), CHUNK_SAMPLES):
            if self._stopped.is_set():
                break
            chunk = data[i : i + CHUNK_SAMPLES]
            # Pad if last chunk is short
            if len(chunk) < CHUNK_SAMPLES:
                chunk = np.pad(chunk, (0, CHUNK_SAMPLES - len(chunk)), "constant")
            chunk_bytes = chunk.astype(np.int16).tobytes()

            for channel_id in self._channel_ids:
                from edge_voice.pipeline.models import AudioPacket

                packet = AudioPacket(
                    channel_id=channel_id,
                    timestamp=time.time(),
                    samples=chunk_bytes,
                )
                try:
                    self._ingest_queue.put(packet, timeout=PACKET_PUT_TIMEOUT)
                    packet_num += 1
                except queue.Full:
                    logger.warning("ingest_queue full, dropping packet for %s", channel_id)

            # Maintain real-time rate
            elapsed = time.time() - start_time
            expected = (packet_num * CHUNK_SAMPLES) / TARGET_SAMPLE_RATE
            delay = expected - elapsed
            if delay > 0:
                time.sleep(delay)

        logger.info(
            "WavSource: played %d packets (%.2fs duration)", packet_num, total_chunks * 0.02
        )

    def stop(self) -> None:
        self._stopped.set()

    def is_alive(self) -> bool:
        return not self._stopped.is_set()


def _create_packet(
    channel_id: str,
    samples: np.ndarray,
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
    parser = argparse.ArgumentParser(description="WavSource standalone test")
    parser.add_argument("file", help="Path to WAV file")
    parser.add_argument(
        "-c", "--channels", type=str, default="ch1,ch2", help="Channel IDs (comma-separated)"
    )
    parser.add_argument("-q", "--queue-type", choices=["memory", "mqtt"], default="memory")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    if args.queue_type == "memory":
        q: queue.Queue[Any] = queue.Queue(maxsize=256)
        channels = [c.strip() for c in args.channels.split(",")]
        source = WavSource(q, channels, args.file)
        source.run()
    else:
        logger.info("MQTT mode not yet implemented")


if __name__ == "__main__":
    main()
