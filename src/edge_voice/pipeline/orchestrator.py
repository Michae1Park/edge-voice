"""
Wires together: channel router -> VAD/segmenter -> STT transcriber,
using the producer/consumer thread model described in docs/design.md.
Owns startup, graceful shutdown for each stage.

Pipeline:
    MQTT subscriber -> ingest_queue -> ChannelRouter -> router_queue -> PacketCopier
        -> { routed_queue (VAD), dump_queue (dump) } -> segment_queue -> STT
"""

from __future__ import annotations

import logging
import queue
import threading
import time

from edge_voice.config.settings import Settings
from edge_voice.pipeline.queues import (
    make_dump_queue,
    make_ingest_queue,
    make_routed_queue,
    make_segment_queue,
)
from edge_voice.pipeline.queue_copier import QueueCopier

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Builds and manages the full pipeline worker graph."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ingest_queue: queue.Queue | None = None
        self._router_queue: queue.Queue | None = None
        self._routed_queue: queue.Queue | None = None
        self._dump_queue: queue.Queue | None = None
        self._segment_queue: queue.Queue | None = None
        self._audio_source: threading.Thread | None = None
        self._router: threading.Thread | None = None
        self._copier: QueueCopier | None = None
        self._vad: threading.Thread | None = None
        self._stt: threading.Thread | None = None
        self._dump_worker: threading.Thread | None = None
        self._segment_dump_worker: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._running = False

    @property
    def ingest_queue(self) -> queue.Queue:
        if self._ingest_queue is None:
            raise RuntimeError("Pipeline not built. Call build() first.")
        return self._ingest_queue

    # ── Public lifecycle ────────────────────────────────────────

    def build(self) -> None:
        """Create queues and workers from Settings."""
        self._stop_event.clear()
        self._running = False

        # Queues
        self._ingest_queue = make_ingest_queue(maxsize=self._settings.queues.ingest)
        self._router_queue = make_routed_queue(maxsize=self._settings.queues.routed)
        self._routed_queue = make_routed_queue(maxsize=self._settings.queues.routed)
        self._segment_queue = make_segment_queue(maxsize=self._settings.queues.segment)
        self._dump_queue = None
        if self._settings.dump.enabled:
            self._dump_queue = make_dump_queue(maxsize=self._settings.queues.dump)

        # Optional dump worker (needs _dump_queue)
        self._build_audio_dump()

        # Core workers
        self._audio_source = self._build_mqtt_subscriber()
        self._router = self._build_router()
        self._copier = QueueCopier(
            self._router_queue,
            self._routed_queue,
            self._dump_queue or self._routed_queue,
            put_timeout=0.2,
        )
        self._vad = self._build_vad()
        self._stt = self._build_fake_stt()
        if self._settings.segment_dump.enabled:
            self._segment_dump_worker = self._build_segment_dump()

        logger.info(
            "Pipeline built with channels: %s", [c.channel_id for c in self._settings.mqtt.channels]
        )

    def start(self) -> None:
        """Start all workers. Only once."""
        if self._running:
            return
        self._running = True
        for w in self._get_workers():
            w.start()
        logger.info("Pipeline started")

    def stop(self) -> None:
        """Signal all workers to stop."""
        self._running = False
        self._stop_event.set()
        for w in reversed(self._get_workers()):
            try:
                w.stop()  # type: ignore[attr-defined]
            except AttributeError:
                pass

    def wait(self) -> None:
        for w in self._get_workers():
            w.join(timeout=10)
            if hasattr(w, "is_alive"):
                alive = w.is_alive() if callable(w.is_alive) else w.is_alive
                if alive:
                    logger.warning("Worker %s did not stop within 10s", w.name)

    def get_status(self) -> dict:
        running = self._running
        workers = {w.name: ("running" if w.is_alive() else "stopped") for w in self._get_workers()}
        return {"running": running, "workers": workers}

    def run(self) -> None:
        self.build()
        self.start()
        self.wait()
        self.stop()

    # ── Timer variant ──────────────────────────────────────────

    def run_with_timer(self, duration_s: float = 30.0) -> None:
        self.build()
        try:
            self.start()
            end = time.time() + duration_s
            while time.time() < end:
                self._stop_event.wait(1.0)
                if not self._running:
                    break
        except KeyboardInterrupt:
            logger.info("Ctrl-C received, shutting down...")
        finally:
            self._running = False
            for w in self._get_workers():
                w.stop()  # type: ignore[attr-defined]
            self.wait()

    # ── Worker tracking ────────────────────────────────────────

    def _get_workers(self) -> list[threading.Thread]:
        workers = [
            self._audio_source,
            self._router,
            self._copier,
            self._vad,
            self._stt,
            self._dump_worker,
            self._segment_dump_worker,
        ]
        return [w for w in workers if w is not None]

    # ── Worker builders ────────────────────────────────────────

    def _build_mqtt_subscriber(self) -> threading.Thread:
        from edge_voice.audio_ingest.mqtt_client import MqttAudioIngest

        if self._ingest_queue is None:
            raise RuntimeError("Ingest queue not initialized")
        return MqttAudioIngest(self._settings.mqtt, self._ingest_queue)

    def _build_router(self) -> threading.Thread:
        from edge_voice.channel.router import ChannelRouter

        if self._ingest_queue is None or self._router_queue is None:
            raise RuntimeError("Queues not initialized")
        channels = [c.channel_id for c in self._settings.mqtt.channels]
        return ChannelRouter(self._ingest_queue, self._router_queue, channels)

    def _build_vad(self) -> threading.Thread:
        from edge_voice.pipeline.fake_workers import FakeVADWorker

        if self._routed_queue is None or self._segment_queue is None:
            raise RuntimeError("Queues not initialized")
        return FakeVADWorker(self._routed_queue, self._segment_queue)

    def _build_fake_stt(self) -> threading.Thread:
        from edge_voice.pipeline.fake_workers import FakeSTTWorker

        if self._segment_queue is None:
            raise RuntimeError("Segment queue not initialized")

        def _on_transcript(event) -> None:
            logger.info(
                "TRANSCRIPT channel=%s segment=%s [%.2f-%.2f] %r",
                event.channel_id,
                event.segment_id,
                event.start,
                event.end,
                event.text,
            )

        return FakeSTTWorker(self._segment_queue, _on_transcript)

    def _build_segment_dump(self) -> threading.Thread:
        from edge_voice.audio_ingest.segment_audio_dump import SegmentAudioDumpWorker

        if self._segment_queue is None:
            raise RuntimeError("Segment queue not initialized")
        return SegmentAudioDumpWorker(
            segment_queue=self._segment_queue,
            output_dir=self._settings.segment_dump.output_dir,
            channel_sample_rate=self._settings.audio.sample_rate,
        )

    def _build_audio_dump(self) -> None:
        if not self._settings.dump.enabled or self._dump_queue is None:
            return
        from edge_voice.audio_ingest.audio_dump import AudioDumpWorker

        self._dump_worker = AudioDumpWorker(
            routed_queue=self._dump_queue,
            output_dir=self._settings.dump.output_dir,
            channel_sample_rate=self._settings.audio.sample_rate,
            segment_secs=self._settings.dump.segment_secs,
        )
        logger.info("AudioDumpWorker enabled: %s", self._settings.dump.output_dir)
