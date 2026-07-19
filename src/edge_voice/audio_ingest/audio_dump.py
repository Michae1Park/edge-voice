"""Debug tool: collect AudioPackets from a queue and save as WAV files.

Usage:
    # From Python:
    from edge_voice.audio_ingest.audio_dump import AudioDumpWorker
    dump = AudioDumpWorker(dump_queue, output_dir="./dumped_audio")
    dump.start(); ...; dump.stop()

    # Or as a standalone process with a queue created via IPC.
"""

from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path

import numpy as np
import soundfile as sf

from edge_voice.pipeline.models import AudioPacket

logger = logging.getLogger(__name__)

BYTES_PER_SAMPLE = 2  # int16 PCM


class AudioDumpWorker(threading.Thread):
    """Consume packets from a dump queue and write per-channel rolling WAV files."""

    def __init__(
        self,
        dump_queue: queue.Queue[AudioPacket],
        output_dir: str = "./edge_voice/dumped_audio",
        channel_sample_rate: int = 16_000,
        segment_secs: float = 10.0,
    ) -> None:
        """
        Args:
            dump_queue: queue of AudioPackets to dump (e.g. ChannelRouter's dump_queue).
            output_dir: directory where WAV files will be written.
            channel_sample_rate: sample rate for the output WAV.
            segment_secs: number of seconds of audio per output file.
        """
        super().__init__(name="AudioDumpWorker", daemon=False)
        self._queue = dump_queue
        self._output_dir = Path(output_dir)
        self._sr = channel_sample_rate
        self._segment_secs = segment_secs
        self._threshold = int(segment_secs * self._sr * BYTES_PER_SAMPLE)
        self._stop_event = threading.Event()
        self._buffers: dict[str, bytearray] = {}
        self._segments: dict[str, int] = {}
        self._lock = threading.Lock()

    def _flush_full_segment(self, ch: str) -> tuple[np.ndarray, Path] | None:
        """Extract one full segment from *ch*'s buffer. Truncates in-place.

        Returns ``(segment_array, path)`` or ``None`` if buffer has fewer
        than threshold samples.
        """
        if len(self._buffers[ch]) < self._threshold:
            return None
        segment_id = self._segments[ch]
        segment = np.frombuffer(bytes(self._buffers[ch][: self._threshold]), dtype=np.int16)
        self._buffers[ch] = bytearray(self._buffers[ch][self._threshold :])
        self._segments[ch] += 1
        return segment, self._output_dir / f"{ch}_{segment_id:03d}.wav"

    def _flush_end(self, ch: str) -> None:
        """Write trailing partial buffer for *ch* after stop."""
        with self._lock:
            remaining = self._buffers.get(ch)
            if not remaining:
                return
            segment_id = self._segments[ch]
            path = self._output_dir / f"{ch}_{segment_id:03d}_end.wav"
            del self._buffers[ch]
        segment = np.frombuffer(bytes(remaining), dtype=np.int16)
        sf.write(str(path), segment, self._sr, subtype="PCM_16")
        total_s = len(segment) / self._sr
        logger.info("AudioDumpWorker: wrote %s (%d samples, %.2fs)", path, len(segment), total_s)

    def run(self) -> None:
        self._output_dir.mkdir(parents=True, exist_ok=True)

        while not self._stop_event.is_set():
            try:
                packet = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            written: list[tuple[np.ndarray, Path]] = []

            with self._lock:
                if packet.channel_id not in self._buffers:
                    self._buffers[packet.channel_id] = bytearray()
                    self._segments[packet.channel_id] = 0

                self._buffers[packet.channel_id].extend(packet.samples)

                while (result := self._flush_full_segment(packet.channel_id)) is not None:
                    written.append(result)

                if not written:
                    current_sec = len(self._buffers[packet.channel_id]) / (
                        self._sr * BYTES_PER_SAMPLE
                    )
                    logger.debug(
                        "AudioDumpWorker: %s buffer %.1f/%.1fs",
                        packet.channel_id,
                        current_sec,
                        self._segment_secs,
                    )

            for segment, out_path in written:
                sf.write(str(out_path), segment, self._sr, subtype="PCM_16")
                logger.info("AudioDumpWorker: wrote %s (%.2fs)", out_path, self._segment_secs)

        logger.info(
            "AudioDumpWorker: flushing trailing %d channel(s) to %s",
            len(self._buffers),
            self._output_dir,
        )

        for ch in list(self._buffers):
            self._flush_end(ch)

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()
