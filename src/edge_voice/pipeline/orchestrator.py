"""
Wires together: channel router -> VAD/segmenter -> STT transcriber,
using the producer/consumer thread model described in docs/design.md.
Owns startup, graceful shutdown, and failure-recovery (restart-on-crash)
for each stage.

Milestones 0-1 keep the fake VAD/STT workers. Real VAD arrives Milestone 3,
real STT Milestone 4. Milestone 2 introduces real MQTT ingest and channel
routing, replacing the fake router.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any

from edge_voice.config.settings import Settings
from edge_voice.pipeline.queues import make_ingest_queue, make_routed_queue, make_segment_queue
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

        Milestone 2: real MQTT ingest + real channel router.
        Fake VAD/STT remain for now.
        """
        self._stop_event.clear()

        # Create shared queues
        self._ingest_queue = make_ingest_queue()
        self._routed_queue = make_routed_queue()
        self._segment_queue = make_segment_queue()

        # Always use MQTT subscriber for audio ingestion (Milestone 2)
        self._audio_source = self._build_mqtt_subscriber()

        # Channel router (swapped in for fake router in Milestone 2)
        self._router = self._build_real_router()

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
                logger.debug("Worker %s does not implement a stop method; skipping.", w)
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

    def _build_mqtt_subscriber(self) -> Any:
        """Build the MQTT subscriber worker (Milestone 2)."""
        from edge_voice.audio_ingest.mqtt_client import MqttAudioIngest

        if self._ingest_queue is None:
            raise RuntimeError("Ingest queue not initialized")

        return MqttAudioIngest(self._settings.mqtt, self._ingest_queue)

    def _build_real_router(self) -> Any:
        """Build the real channel router worker (Milestone 2)."""
        from edge_voice.channel.router import ChannelRouter

        if self._ingest_queue is None or self._routed_queue is None:
            raise RuntimeError("Queues not initialized")

        channel_ids = [c.channel_id for c in self._settings.mqtt.channels]
        return ChannelRouter(self._ingest_queue, self._routed_queue, channel_ids)

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
