"""WAV file audio source that reads a file and pushes 20ms packets."""

from __future__ import annotations

import logging
import math
import queue
import threading
import time
from typing import Any

import numpy as np
import soundfile as sf
import torch
import torchaudio

logger = logging.getLogger(__name__)


class WavSource(threading.Thread):
    """Parses a .wav file and pushes 20ms audio packets onto a queue.

    All audio config is taken from Settings via *sample_rate* and
    *chunk_samples* rather than from module-level constants.
    """

    def __init__(
        self,
        ingest_queue: queue.Queue,
        channel_ids: list[str],
        wav_path: str,
        sample_rate: int = 16_000,
        chunk_samples: int = 320,  # 20 ms chunks (160 bytes @ 16bit mono)
    ) -> None:
        super().__init__(name="WavSource", daemon=True)
        self._ingest_queue = ingest_queue
        self._channel_ids = channel_ids
        self._wav_path = wav_path
        self._sample_rate = sample_rate
        self._chunk_samples = chunk_samples
        self._stopped = threading.Event()

    # ------------------------------------------------------------------
    # File helpers (moved from module scope into the class)
    # ------------------------------------------------------------------

    def _open_wav(self, path: str) -> tuple[int, int, np.ndarray]:
        """Open an entire WAV file via soundfile (int16)."""
        data, sr = sf.read(path, dtype="int16")
        nch = 1 if data.ndim == 1 else data.shape[1]
        return sr, nch, data

    def _resample(self, data: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
        """Resample with torchaudio (int16 → int16)."""
        if src_sr == dst_sr:
            return data
        tensor = torch.as_tensor(data, dtype=torch.float32).unsqueeze(0)
        resampler = torchaudio.transforms.Resample(src_sr, dst_sr, lowpass_filter_width=64)
        return resampler(tensor).squeeze(0).numpy().astype(np.int16)

    def _create_packet(
        self,
        channel_id: str,
        samples: np.ndarray,
        timestamp: float | None = None,
    ) -> Any:
        """Encode *samples* into an AudioPacket."""
        from edge_voice.pipeline.models import AudioPacket

        return AudioPacket(
            channel_id=channel_id,
            timestamp=timestamp or time.time(),
            samples=samples.tobytes(),
        )

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Read the WAV file, convert to 16 kHz mono int16, and push 20 ms chunks.

        Real-time pacing is maintained so playback speed matches wall-clock time.
        """
        logger.info("WavSource: reading %s...", self._wav_path)
        file_sr, nch, data = self._open_wav(self._wav_path)

        # --- Resample ---------------------------------------------------
        if file_sr != self._sample_rate:
            logger.info("WavSource: resampling %d → %d Hz", file_sr, self._sample_rate)
            data = self._resample(data, file_sr, self._sample_rate)

        # --- Stereo → mono if necessary ---------------------------------
        if nch == 2:
            mono = (data[:, 0].astype(np.int32) + data[:, 1].astype(np.int32)) // 2
            data = mono.astype(np.int16)

        total_chunks = math.ceil(len(data) / self._chunk_samples)
        start_time = time.time()
        packet_num = 0

        # --- Loop over 20 ms chunks -------------------------------------
        for i in range(0, len(data), self._chunk_samples):
            if self._stopped.is_set():
                break

            chunk = data[i : i + self._chunk_samples]
            if len(chunk) < self._chunk_samples:
                chunk = np.pad(chunk, (0, self._chunk_samples - len(chunk)), "constant")

            for channel_id in self._channel_ids:
                packet = self._create_packet(channel_id, chunk, time.time())
                try:
                    self._ingest_queue.put(packet, timeout=0.01)
                    packet_num += 1
                except queue.Full:
                    logger.warning("ingest_queue full; dropping packet for %s", channel_id)

            # Real-time pacing
            elapsed = time.time() - start_time
            expected = (packet_num * self._chunk_samples) / self._sample_rate
            delay = expected - elapsed
            if delay > 0:
                time.sleep(delay)

        logger.info(
            "WavSource: played %d packets (%.2fs duration)",
            packet_num,
            total_chunks * 0.02,
        )

    def stop(self) -> None:
        """Signal the worker to stop after the current packet."""
        self._stopped.set()

    def is_alive(self) -> bool:
        """Return *True* while the worker is not stopped — *not* the
        underlying ``threading.Thread.is_alive()``."""
        return not self._stopped.is_set()
