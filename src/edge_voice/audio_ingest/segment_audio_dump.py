from __future__ import annotations

import queue
import threading
from pathlib import Path

import numpy as np

from edge_voice.audio_ingest.atomic_write import atomic_sf_write
from edge_voice.observability.logging import get_stage_logger
from edge_voice.pipeline.models import SpeechSegment

logger = get_stage_logger(__name__, stage="audio_ingest")


class SegmentAudioDumpWorker(threading.Thread):

    def __init__(
        self,
        segment_queue: queue.Queue[SpeechSegment],
        output_dir: str = "./edge_voice/dumped_vad_segments",
        channel_sample_rate: int = 16_000,
    ) -> None:
        super().__init__(name="SegmentAudioDumpWorker", daemon=False)
        self._queue = segment_queue
        self._output_dir = Path(output_dir)
        self._sr = channel_sample_rate
        self._stop_event = threading.Event()
        self._counter = 0
        self._lock = threading.Lock()

    def run(self) -> None:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        written: list[tuple[np.ndarray, Path, SpeechSegment]] = []

        while not self._stop_event.is_set():
            try:
                seg = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            with self._lock:
                pcm = np.frombuffer(seg.audio, dtype=np.int16)
                self._counter += 1
                idx = self._counter
                written.append(
                    (
                        pcm,
                        self._output_dir
                        / f"{seg.channel_id}_{idx:03d}_{seg.start:.2f}-{seg.end:.2f}s.wav",
                        seg,
                    )
                )

            for pcm, path, seg in written:
                atomic_sf_write(str(path), pcm, self._sr, subtype="PCM_16")
                logger.info(
                    "SegmentAudioDumpWorker: wrote %s [%s] dur=%.2fs",
                    path,
                    seg.segment_id,
                    seg.end - seg.start,
                    extra={"channel_id": seg.channel_id, "segment_id": seg.segment_id},
                )

            written.clear()

        if written:
            for pcm, path, seg in written:
                atomic_sf_write(str(path), pcm, self._sr, subtype="PCM_16")
            logger.info("SegmentAudioDumpWorker: flushed %d trailing segment(s)", len(written))

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def is_alive(self) -> bool:
        return not self._stop_event.is_set()
