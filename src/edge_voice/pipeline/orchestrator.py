"""
Wires together: channel router -> VAD/segmenter -> STT transcriber,
using the producer/consumer thread model described in docs/design.md.
Owns startup, graceful shutdown, and failure-recovery (restart-on-crash)
for each stage.

Pipeline (Milestone 2):
    MQTT subscriber -> ingest_queue -> ChannelRouter -> router_queue -> PacketCopier -> { routed_queue (VAD), dump_queue (dump) } -> segment_queue -> FakeVADWorker -> FakeSTTWorker

The key insight: the dump worker must NOT share a queue with the VAD worker,
because both would compete for packets and only one would get each packet.
Instead, a PacketCopier fans out from router_queue to both.
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
        self._router_queue: queue.Queue | None = None  # direct router output
        self._routed_queue: queue.Queue | None = None  # goes to VAD (via PacketCopier)
        self._dump_queue: queue.Queue | None = None  # goes to dump worker (via PacketCopier)
        self._segment_queue: queue.Queue | None = None
        self._audio_source: Any = None
        self._router: Any = None
        self._copier: Any = None
        self._vad: Any = None
        self._stt: Any = None
        self._dump_worker: Any = None
        self._tracker: Any = None
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
        self._router_queue = make_routed_queue()  # router -> copier
        self._routed_queue = make_routed_queue()  # copier -> VAD
        self._dump_queue = make_routed_queue()  # copier -> dump (if enabled)
        self._segment_queue = make_segment_queue()

        # AudioDumpWorker for debugging (optional)
        self._build_audio_dump()

        # Always use MQTT subscriber for audio ingestion (Milestone 2)
        self._audio_source = self._build_mqtt_subscriber()

        # Channel router sends packets to router_queue (not directly to VAD)
        self._router = self._build_router()

        # Central packet tracker (single source of truth for per-channel state)
        self._tracker = self._build_packet_tracker()

        # VAD worker (fake - real VAD arriving Milestone 3)
        self._vad = self._build_vad()

        # STT worker (fake - real STT arriving Milestone 4)
        self._stt = self._build_fake_stt()

        # PacketCopier fans out router_queue to both VAD queue and dump queue
        self._build_packet_copier()

        self._status = PipelineStatus(
            workers=[
                WorkerStatus(name="audio_source", state="built"),
                WorkerStatus(name="router", state="built"),
                WorkerStatus(name="packet_copier", state="built" if self._copier else "disabled"),
                WorkerStatus(name="vad", state="built"),
                WorkerStatus(name="stt", state="built"),
                WorkerStatus(name="audio_dump", state="built" if self._dump_worker else "disabled"),
                WorkerStatus(name="packet_tracker", state="built" if self._tracker else "disabled"),
            ],
            running=False,
        )
        logger.info(
            "Pipeline built with channels: %s", [c.channel_id for c in self._settings.mqtt.channels]
        )
        logger.info(
            "Packet tracker enabled with %d channel(s)",
            len(self._tracker.channel_ids) if self._tracker else 0,
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
            w
            for w in [
                self._audio_source,
                self._router,
                self._copier,
                self._vad,
                self._stt,
                self._dump_worker,
            ]
            if w is not None
        ]

    def _build_mqtt_subscriber(self) -> Any:
        """Build the MQTT subscriber worker (Milestone 2)."""
        from edge_voice.audio_ingest.mqtt_client import MqttAudioIngest

        if self._ingest_queue is None:
            raise RuntimeError("Ingest queue not initialized")

        return MqttAudioIngest(self._settings.mqtt, self._ingest_queue)

    def _build_router(self) -> Any:
        """Build channel router worker (Milestone 2).

        Router sends valid packets to _router_queue (not directly to VAD).
        The PacketCopier fans out from there.
        """
        from edge_voice.channel.router import ChannelRouter

        if self._ingest_queue is None:
            raise RuntimeError("Ingest queue not initialized")
        if self._router_queue is None:
            raise RuntimeError("Router queue not initialized")

        channel_ids = [c.channel_id for c in self._settings.mqtt.channels]
        return ChannelRouter(self._ingest_queue, self._router_queue, channel_ids)

    def _build_packet_copier(self) -> None:
        """Fan out packets from router_queue to both VAD and dump queues."""
        from edge_voice.pipeline.packet_copier import PacketCopier

        if self._router_queue is None or self._routed_queue is None:
            raise RuntimeError("Router queues not initialized")

        # Inject the tracker as the callback — all packets flow through here
        track_cb = self._tracker.track if self._tracker else None
        assert self._dump_queue is not None
        self._copier = PacketCopier(
            self._router_queue, self._routed_queue, self._dump_queue, track_cb
        )

    def _build_packet_tracker(self) -> Any:
        """Build the central per-channel packet tracker.

        Initialized with known channel IDs so it can report expected
        channels even before any packets arrive.
        """
        from edge_voice.pipeline.packet_tracker import AudioPacketTracker

        channel_ids = [c.channel_id for c in self._settings.mqtt.channels]
        return AudioPacketTracker(channel_ids=channel_ids)

    def _build_vad(self) -> Any:
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

    def _build_audio_dump(self) -> None:
        """Build the AudioDumpWorker if enabled in settings."""
        if not self._settings.dump.enabled:
            logger.info("AudioDumpWorker disabled (dump.enabled=false)")
            return

        if self._dump_queue is None:
            raise RuntimeError("Dump queue not initialized")
        if self._segment_queue is None:
            raise RuntimeError("Segment queue not initialized")

        from edge_voice.audio_ingest.audio_dump import AudioDumpWorker

        self._dump_worker = AudioDumpWorker(
            routed_queue=self._dump_queue,
            output_dir=self._settings.dump.output_dir,
            channel_sample_rate=self._settings.audio.sample_rate,
            segment_secs=self._settings.dump.segment_secs,
        )
        logger.info(
            "AudioDumpWorker enabled: %s (segment=%ds)",
            self._settings.dump.output_dir,
            self._settings.dump.segment_secs,
        )
