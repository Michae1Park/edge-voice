"""
Wires together: channel router -> VAD/segmenter -> STT transcriber,
using the producer/consumer thread model described in docs/design.md.
Owns startup, graceful shutdown, and failure-recovery (restart-on-crash)
for each stage.

Milestones 0-1 keep the fake VAD/STT workers. Real VAD arrives Milestone 3,
real STT Milestone 4. By Milestone 1, only the ingest path is real (MQTT).
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any

from edge_voice.config.settings import Settings
from edge_voice.pipeline.queues import make_ingest_queue, make_segment_queue
from edge_voice.pipeline.models import WorkerStatus, PipelineStatus


logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Builds and manages the full pipeline worker graph.

    Responsibilities:
    - Create queues and workers from Settings
    - Own startup order and graceful shutdown
    - Expose get_status() seam for health monitoring
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ingest_queue: queue.Queue | None = None
        self._routed_queue: queue.Queue | None = None
        self._segment_queue: queue.Queue | None = None
        self._audio_source: Any = None
        self._router: Any = None
        self._vad: Any = None
        self._stt: Any = None
        self._status = PipelineStatus(running=False)
        self._stop_event = threading.Event()

    @property
    def ingest_queue(self) -> queue.Queue:
        if self._ingest_queue is None:
            raise RuntimeError("Pipeline not built. Call build() first.")
        return self._ingest_queue

    def build(self) -> None:
        """Create queues and workers from Settings.

        Milestone 1: real MQTT subscriber for ingest, fake VAD/STT remain.
        """
        self._stop_event.clear()

        # Create shared queues
        self._ingest_queue = make_ingest_queue()
        self._routed_queue = make_ingest_queue()
        self._segment_queue = make_segment_queue()

        # Build audio source from settings config
        wav_file = self._get_wav_file()
        if wav_file:
            self._audio_source = self._build_wav_source(wav_file)
        else:
            self._audio_source = self._build_mic_source()

        # Channel router (stub - real router arriving Milestone 2)
        self._router = self._build_fake_router()

        # VAD worker (fake - real VAD arriving Milestone 3)
        self._vad = self._build_fake_vad()

        # STT worker (fake - real STT arriving Milestone 4)
        self._stt = self._build_fake_stt()

        self._status = PipelineStatus(
            workers=[
                WorkerStatus(name="audio_source", state="built"),
                WorkerStatus(name="router", state="built"),
                WorkerStatus(name="vad", state="built"),
                WorkerStatus(name="stt", state="built"),
            ],
            running=False,
        )
        logger.info(
            "Pipeline built with channels: %s", [c.channel_id for c in self._settings.mqtt.channels]
        )

    def start(self) -> None:
        """Start all workers in dependency order."""
        workers = self._get_workers()
        for w in workers:
            w.start()
        self._status.running = True
        self._status.workers = [WorkerStatus(name=w.name, state="running") for w in workers]
        logger.info("Pipeline started")

    def stop(self) -> None:
        """Stop all workers in reverse dependency order."""
        self._stop_event.set()
        workers = self._get_workers()
        for w in reversed(workers):
            try:
                w.stop()
            except AttributeError:
                logger.debug(f"Worker {w} does not implement a stop method; skipping.")
        self._status.running = False
        self._status.workers = [WorkerStatus(name=w.name, state="stopped") for w in workers]
        logger.info("Pipeline stopped")

    def wait(self) -> None:
        """Block until all workers finish or stop event is set."""
        workers = self._get_workers()
        for w in workers:
            w.join(timeout=10)
            alive = w.is_alive() if callable(w.is_alive) else False
            if alive:
                logger.warning("Worker %s did not stop within 10s", w.name)

    def get_status(self) -> PipelineStatus:
        """Return current pipeline status for health monitoring."""
        worker_states = {}
        workers = self._get_workers()
        for w in workers:
            alive = w.is_alive() if hasattr(w, "is_alive") else False
            worker_states[w.name] = {
                "alive": alive,
                "state": "running" if alive else "stopped",
            }
        return PipelineStatus(
            running=True,
            workers=[WorkerStatus(name=n, state=str(s["state"])) for n, s in worker_states.items()],
        )

    def run(self) -> None:
        """Build, start, wait for completion, and shut down."""
        self.build()
        self.start()
        self.wait()
        self.stop()

    def run_with_timer(self, duration_s: float = 30.0) -> None:
        """Run for a limited time, then shut down."""
        self.build()

        try:
            self.start()
            end = time.time() + duration_s
            while time.time() < end and not self._stop_event.is_set():
                self._stop_event.wait(1.0)
        except KeyboardInterrupt:
            logger.info("Ctrl-C received, shutting down...")
        finally:
            self.stop()
            self.wait()

    def _get_workers(self) -> list[Any]:
        """Return workers in startup order."""
        return [
            w for w in [self._audio_source, self._router, self._vad, self._stt] if w is not None
        ]

    def _get_wav_file(self) -> str | None:
        """Look for configured WAV file path in settings."""
        default_audio = self._settings.source.default_audio
        return default_audio if default_audio else None

    def _build_fake_router(self) -> Any:
        """Build the channel router worker."""
        from edge_voice.pipeline.fake_workers import FakeRouter

        if self._routed_queue is None or self._ingest_queue is None:
            raise RuntimeError("Queues not initialized")

        return FakeRouter(self._ingest_queue, self._routed_queue)

    def _build_fake_vad(self) -> Any:
        """Build the VAD worker."""
        from edge_voice.pipeline.fake_workers import FakeVADWorker

        if self._routed_queue is None or self._segment_queue is None:
            raise RuntimeError("Queues not initialized")

        return FakeVADWorker(self._routed_queue, self._segment_queue)

    def _build_fake_stt(self) -> Any:
        """Build the STT worker."""
        from edge_voice.pipeline.fake_workers import FakeSTTWorker

        if self._segment_queue is None:
            raise RuntimeError("Segment queue not initialized")

        def _on_transcript(event: Any) -> None:
            logger.info(
                "TRANSCRIPT channel=%s segment=%s [%.2f-%.2f] %r",
                event.channel_id,
                event.segment_id,
                event.start,
                event.end,
                event.text,
            )

        return FakeSTTWorker(self._segment_queue, _on_transcript)

    def _build_mic_source(self) -> Any:
        """Build a microphone audio source."""
        from edge_voice.utils.audio_generation.mic_source import MicSource

        assert self._ingest_queue is not None
        channel_ids = [c.channel_id for c in self._settings.mqtt.channels]
        return MicSource(self._ingest_queue, channel_ids)

    def _build_wav_source(self, wav_file: str) -> Any:
        """Build a WAV file audio source."""
        from edge_voice.utils.audio_generation.wav_source import WavSource

        assert self._ingest_queue is not None
        channel_ids = [c.channel_id for c in self._settings.mqtt.channels]
        return WavSource(
            self._ingest_queue,
            channel_ids,
            wav_file,
            sample_rate=self._settings.audio.sample_rate,
            chunk_samples=self._settings.audio.chunk_samples,
        )
