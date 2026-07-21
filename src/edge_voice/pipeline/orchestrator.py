"""
Wires together: channel router -> VAD/segmenter -> STT transcriber,
using the producer/consumer thread model described in docs/design.md.
Owns startup, graceful shutdown for each stage.

Pipeline:
    MQTT subscriber -> ingest_queue -> ChannelRouter -> routed_queue -> VAD -> segment_queue -> STT
                                                       -> dump_queue (optional, debug)
                                                                     -> segment_dump_queue (optional, debug)
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
from edge_voice.audio_ingest.mqtt_client import MqttAudioIngest
from edge_voice.channel.router import ChannelRouter, RepacketizerConfig
from edge_voice.pipeline.transcript_hub import TranscriptHub
from edge_voice.vad.vad_worker import VADWorker, VADWorkerConfig
from edge_voice.stt.stt_worker import STTWorker, STTWorkerConfig

logger = logging.getLogger(__name__)

WORKER_JOIN_TIMEOUT_S = 10


class PipelineOrchestrator:
    """Builds and manages the full pipeline worker graph."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ingest_queue: queue.Queue | None = None
        self._routed_queue: queue.Queue | None = None
        self._dump_queue: queue.Queue | None = None
        self._segment_queue: queue.Queue | None = None
        self._segment_dump_queue: queue.Queue | None = None
        self._audio_source: threading.Thread | None = None
        self._router: threading.Thread | None = None
        self._vad: threading.Thread | None = None
        self._stt: threading.Thread | None = None
        self._dump_worker: threading.Thread | None = None
        self._segment_dump_worker: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._running = False
        # Doesn't depend on queues/workers, so it's safe to create once here
        # rather than in build() -- a webui/app.py holding a reference to it
        # doesn't need to care whether build() has run yet.
        self._transcript_hub = TranscriptHub(backlog=settings.webui.transcript_backlog)

    @property
    def ingest_queue(self) -> queue.Queue:
        if self._ingest_queue is None:
            raise RuntimeError("Pipeline not built. Call build() first.")
        return self._ingest_queue

    @property
    def transcripts(self) -> TranscriptHub:
        """Subscribe here (webui/app.py) for a live TranscriptEvent feed."""
        return self._transcript_hub

    # ── Public lifecycle ────────────────────────────────────────

    def build(self) -> None:
        """Create queues and workers from Settings."""
        self._stop_event.clear()
        self._running = False

        # Queues
        self._ingest_queue = make_ingest_queue(maxsize=self._settings.queues.ingest)
        self._routed_queue = make_routed_queue(maxsize=self._settings.queues.routed)
        self._segment_queue = make_segment_queue(maxsize=self._settings.queues.segment)
        self._dump_queue = None
        self._segment_dump_queue = None

        if self._settings.dump.enabled:
            self._dump_queue = make_dump_queue(maxsize=self._settings.queues.dump)
            self._dump_worker = self._build_audio_dump()
        if self._settings.segment_dump.enabled:
            self._segment_dump_queue = make_dump_queue(maxsize=self._settings.queues.dump)
            self._segment_dump_worker = self._build_segment_dump()

        # Core workers
        self._audio_source = self._build_mqtt_subscriber()
        self._router = self._build_router()
        self._vad = self._build_vad()
        self._stt = self._build_stt()

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
        """Stop workers upstream-first, draining each stage before the next.

        Both the order and the per-stage join matter. VADWorker flushes any
        in-progress segment when its run loop exits, so STT and the dump
        workers must still be alive to receive it. Signalling every worker at
        once (or downstream-first) races: VAD blocks up to its queue timeout
        before noticing the stop, by which point the consumers it is about to
        push to have already exited, and the stream's final utterance is
        silently dropped.
        """
        self._running = False
        self._stop_event.set()
        workers = self._get_workers()
        try:
            for w in workers:  # producers before their consumers
                self._signal(w)
                self._join(w)
        finally:
            # A second Ctrl-C can interrupt the drain mid-loop. Most workers
            # are non-daemon, so any left unsignalled would keep the process
            # alive forever -- signal them all before propagating.
            for w in workers:
                self._signal(w)

    def wait(self) -> None:
        # stop() already joins each worker in order; this is a backstop for
        # callers that invoke wait() on its own.
        for w in self._get_workers():
            self._join(w)

    @staticmethod
    def _signal(worker: threading.Thread) -> None:
        try:
            worker.stop()  # type: ignore[attr-defined]
        except AttributeError:
            pass

    @staticmethod
    def _join(worker: threading.Thread) -> None:
        # build() without start() is legal (tests do it), and join() raises
        # on a thread that was never started.
        if worker.ident is None:
            return
        worker.join(timeout=WORKER_JOIN_TIMEOUT_S)
        if worker.is_alive():
            logger.warning("Worker %s did not stop within %ss", worker.name, WORKER_JOIN_TIMEOUT_S)

    def get_status(self) -> dict:
        running = self._running
        workers = {w.name: ("running" if w.is_alive() else "stopped") for w in self._get_workers()}
        return {"running": running, "workers": workers}

    def run(self, duration_s: float | None = None) -> None:
        """Build, start, and run until stopped, Ctrl-C, or duration_s elapses."""
        self.build()
        try:
            self.start()
            end = time.time() + duration_s if duration_s is not None else None
            while self._running:
                if end is not None and time.time() >= end:
                    break
                self._stop_event.wait(1.0)
        except KeyboardInterrupt:
            logger.info("Ctrl-C received, shutting down...")
        finally:
            self.stop()
            self.wait()

    def run_with_timer(self, duration_s: float = 30.0) -> None:
        self.run(duration_s=duration_s)

    # ── Worker tracking ────────────────────────────────────────

    def _get_workers(self) -> list[threading.Thread]:
        """Workers in producer-before-consumer order.

        This ordering is load-bearing: stop() shuts down in this order so a
        stage that emits on shutdown (VADWorker.flush) still has live
        consumers. Keep producers ahead of anything reading their queues --
        router feeds dump_worker, vad feeds both stt and segment_dump_worker.
        """
        workers = [
            self._audio_source,
            self._router,
            self._vad,
            self._stt,
            self._dump_worker,
            self._segment_dump_worker,
        ]
        return [w for w in workers if w is not None]

    # ── Worker builders ────────────────────────────────────────

    def _build_mqtt_subscriber(self) -> threading.Thread:
        if self._ingest_queue is None:
            raise RuntimeError("Ingest queue not initialized")
        return MqttAudioIngest(self._settings.mqtt, self._ingest_queue)

    def _build_router(self) -> threading.Thread:
        if self._ingest_queue is None or self._routed_queue is None:
            raise RuntimeError("Queues not initialized")
        channels = [c.channel_id for c in self._settings.mqtt.channels]
        return ChannelRouter(
            self._ingest_queue,
            self._routed_queue,
            channels,
            dump_queue=self._dump_queue,
            repacketizer_config=RepacketizerConfig(
                incoming_ms=self._settings.repacketizer.incoming_ms,
                outgoing_ms=self._settings.repacketizer.outgoing_ms,
                sample_rate=self._settings.audio.sample_rate,
                bytes_per_sample=self._settings.repacketizer.bytes_per_sample,
            ),
        )

    def _build_vad(self) -> threading.Thread:
        if self._routed_queue is None or self._segment_queue is None:
            raise RuntimeError("Queues not initialized")

        return VADWorker(
            self._routed_queue,
            self._segment_queue,
            dump_queue=self._segment_dump_queue,
            config=VADWorkerConfig(
                threshold=self._settings.vad.threshold,
                sample_rate=self._settings.audio.sample_rate,
                rms_gate_enabled=self._settings.vad.rms_gate_enabled,
                silence_rms_floor=self._settings.vad.silence_rms_floor,
                preroll_chunks=self._settings.vad.preroll_chunks,
                min_silence_duration_ms=self._settings.vad.min_silence_duration_ms,
                speech_pad_ms=self._settings.vad.speech_pad_ms,
                idle_flush_s=self._settings.vad.idle_flush_s,
                segment_limits_enabled=self._settings.vad.segment_limits_enabled,
                max_segment_s=self._settings.vad.max_segment_s,
                soft_cut_s=self._settings.vad.soft_cut_s,
                soft_cut_lookahead_s=self._settings.vad.soft_cut_lookahead_s,
                soft_cut_min_dip=self._settings.vad.soft_cut_min_dip,
            ),
        )

    def _build_stt(self) -> threading.Thread:
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
            self._transcript_hub.publish(event)

        stt = self._settings.stt
        return STTWorker(
            self._segment_queue,
            _on_transcript,
            config=STTWorkerConfig(
                language=stt.language,
                model_arch=stt.model_arch,
                sample_rate=self._settings.audio.sample_rate,
                feed_windows=stt.feed_windows,
                feed_window_samples=self._settings.vad.window_samples,
                options={
                    "max_tokens_per_second": stt.max_tokens_per_second,
                    "identify_speakers": str(stt.identify_speakers).lower(),
                    "log_api_calls": str(stt.log_api_calls).lower(),
                    "save_input_wav_path": stt.save_input_wav_path,
                    "return_audio_data": str(stt.return_audio_data).lower(),
                },
            ),
        )

    def _build_segment_dump(self) -> threading.Thread:
        if self._segment_dump_queue is None:
            raise RuntimeError("Segment dump queue not initialized")
        from edge_voice.audio_ingest.segment_audio_dump import SegmentAudioDumpWorker

        worker = SegmentAudioDumpWorker(
            segment_queue=self._segment_dump_queue,
            output_dir=self._settings.segment_dump.output_dir,
            channel_sample_rate=self._settings.audio.sample_rate,
        )
        logger.info("SegmentAudioDumpWorker enabled: %s", self._settings.segment_dump.output_dir)
        return worker

    def _build_audio_dump(self) -> threading.Thread:
        if self._dump_queue is None:
            raise RuntimeError("Dump queue not initialized")
        from edge_voice.audio_ingest.audio_dump import AudioDumpWorker

        worker = AudioDumpWorker(
            dump_queue=self._dump_queue,
            output_dir=self._settings.dump.output_dir,
            channel_sample_rate=self._settings.audio.sample_rate,
            segment_secs=self._settings.dump.segment_secs,
        )
        logger.info("AudioDumpWorker enabled: %s", self._settings.dump.output_dir)
        return worker
