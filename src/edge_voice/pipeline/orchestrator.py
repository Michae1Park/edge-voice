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
from typing import Any, Callable

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
from edge_voice.pipeline.supervisor import Supervisor, SupervisedTarget
from edge_voice.observability.metrics import MetricsCollector, MetricsSnapshot
from edge_voice.health.reporting import build_health_report

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
        # In-process worker supervision (Milestone 6). None when
        # reliability.enabled is False -- the pipeline then behaves exactly as
        # it did before Milestone 6, with no supervisor thread at all.
        self._supervisor: Supervisor | None = None
        # Milestone 7 metrics aggregation. None when metrics.enabled is False
        # -- independent of `reliability.enabled` above (Supervisor is optional
        # input to this, not a hard dependency; see _build_metrics).
        self._metrics: MetricsCollector | None = None
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

        # Supervision layer (Milestone 6). Built here so its targets can close
        # over the worker attributes just assigned; started/stopped separately.
        self._supervisor = self._build_supervisor() if self._settings.reliability.enabled else None

        # Metrics aggregation (Milestone 7). Built after supervisor so its
        # `supervisor` callable can close over the attribute above; started
        # last / stopped first, same reasoning as supervisor -- see start()/stop().
        self._metrics = self._build_metrics() if self._settings.metrics.enabled else None

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
        # Supervisor starts LAST, once the workers it watches are already up --
        # otherwise its first scan could see a not-yet-started worker as a
        # crash. Stopped FIRST in stop(), symmetrically.
        if self._supervisor is not None:
            self._supervisor.start()
        # Metrics starts even later -- it reads the supervisor too. Stopped
        # before it in stop(), symmetrically.
        if self._metrics is not None:
            self._metrics.start()
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
        # Metrics down FIRST -- it reads workers and the supervisor, so it
        # should stop observing before either starts tearing down.
        if self._metrics is not None:
            self._signal(self._metrics)
            self._join(self._metrics)
        # Supervisor down next -- before we start tearing workers down, or it
        # would see them dying (because we are stopping them) and race to
        # "restart" them mid-shutdown. Joining it also drains any in-flight
        # restart thread, so no worker gets swapped out from under the teardown.
        if self._supervisor is not None:
            self._signal(self._supervisor)
            self._join(self._supervisor)
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
        # Base state from the thread itself; the supervisor (if any) refines it
        # with restarting/degraded, which a bare is_alive() can't distinguish
        # from healthy. This is the seam the Milestone 5 UI reads.
        workers = {w.name: ("running" if w.is_alive() else "stopped") for w in self._get_workers()}
        degraded = False
        if self._supervisor is not None:
            sup = self._supervisor.status()
            for name, info in sup.items():
                if name in workers and workers[name] == "running":
                    workers[name] = str(info["state"])
            degraded = self._supervisor.is_degraded()
            # The supervisor watches every worker above but nothing watches
            # it -- if its own thread dies, `degraded`/restart state here
            # just freezes at its last tick rather than reporting the loss.
            # Surface its own liveness the same way as any other worker.
            workers[self._supervisor.name] = "running" if self._supervisor.is_alive() else "stopped"
        return {"running": running, "degraded": degraded, "workers": workers}

    def queue_depths(self) -> dict[str, int]:
        """Current `.qsize()` for each queue that was actually built.

        Queues disabled by config (dump/segment_dump) or not yet built are
        omitted rather than reported as zero, so a caller can tell "not
        running" from "empty."
        """
        queues = {
            "ingest": self._ingest_queue,
            "routed": self._routed_queue,
            "segment": self._segment_queue,
            "dump": self._dump_queue,
            "segment_dump": self._segment_dump_queue,
        }
        return {name: q.qsize() for name, q in queues.items() if q is not None}

    def metrics_snapshot(self) -> MetricsSnapshot | None:
        """Latest MetricsCollector snapshot, or None if there isn't one.

        None means either metrics.enabled is False (no collector exists at
        all) or no tick has landed yet; health/reporting.py distinguishes the
        two, since it also knows the enabled flag.
        """
        return self._metrics.snapshot() if self._metrics is not None else None

    def channel_freshness(self) -> dict[str, float | None]:
        """Seconds since the last packet on each channel; None = never seen.

        Router-sourced on purpose (wall clock, "is this channel still sending
        audio") rather than VADWorker's monotonic idle_flush_s clock -- see
        the Milestone 7 freshness decision in docs/BUILDPLAN.md.

        Read through _w() rather than a captured reference so a supervisor
        restart's attribute swap is observed. Note the replacement router
        starts with an empty last-seen table, so every channel reads None
        again until its next packet -- that reads as "never seen", not as a
        dead channel.
        """
        router = self._w("_router")
        if router is None:  # build() hasn't run yet
            return {c.channel_id: None for c in self._settings.mqtt.channels}
        return {cid: router.get_freshness(cid) for cid in router.get_channel_ids()}

    def health(self) -> dict:
        """Assembled health object for webui's GET /api/status.

        A strict superset of get_status(); see health/reporting.py for the
        shape and for why the assembly lives there rather than here.
        """
        return build_health_report(
            status=self.get_status(),
            queue_depths=self.queue_depths(),
            snapshot=self.metrics_snapshot(),
            metrics_enabled=self._settings.metrics.enabled,
            freshness=self.channel_freshness(),
            stale_after_s=self._settings.health.stale_segment_warning_s,
        )

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

    # ── Supervision (Milestone 6) ──────────────────────────────

    def _build_supervisor(self) -> Supervisor:
        r = self._settings.reliability
        return Supervisor(
            self._build_supervisor_targets(),
            tick_interval_s=r.tick_interval_s,
            stall_timeout_s=r.stall_timeout_s,
            max_restarts=r.max_restarts,
            restart_window_s=r.restart_window_s,
            watchdog_enabled=r.watchdog_enabled,
        )

    # ── Metrics (Milestone 7) ──────────────────────────────────

    def _build_metrics(self) -> MetricsCollector:
        m = self._settings.metrics
        return MetricsCollector(
            queue_depths=self.queue_depths,
            stt_latency_s=lambda: self._w("_stt").last_latency_s,
            vad_silero_latencies_s=lambda: self._w("_vad").silero_latencies_s(),
            vad_rms_gate_latencies_s=lambda: self._w("_vad").rms_gate_latencies_s(),
            router_repacketize_latencies_s=lambda: self._w("_router").repacketize_latencies_s(),
            stt_channel_latencies_s=lambda: self._w("_stt").channel_latencies_s(),
            # self._supervisor is built once in build() and never swapped
            # (unlike the workers), so a direct closure over it is enough --
            # no need to route through _w().
            supervisor=lambda: self._supervisor,
            mqtt_connected=self._mqtt_connected,
            emit_interval_s=m.emit_interval_s,
            latency_log_decimals=m.latency_log_decimals,
        )

    def _mqtt_connected(self) -> bool | None:
        """None when the configured audio source isn't MQTT-based (e.g. a
        WavSource/MicSource in dev/test) -- see MetricsCollector's docstring."""
        return getattr(self._w("_audio_source"), "connected", None)

    def _w(self, attr: str) -> Any:
        """The current worker instance held at `attr`.

        Typed Any on purpose: the workers expose a uniform stopping /
        last_activity / pending_loss interface that mypy can't see through
        their threading.Thread base. Reading via this accessor (rather than a
        captured local) is also what lets a target observe the *replacement*
        worker after a restart swaps the attribute -- see SupervisedTarget.
        """
        return getattr(self, attr)

    @staticmethod
    def _queue_pending(q: queue.Queue | None) -> bool:
        return q is not None and q.qsize() > 0

    def _build_supervisor_targets(self) -> list[SupervisedTarget]:
        """Wire the supervisor's generic callables to our worker attributes.

        Restart closures route through _restart(attr, build_fn), which rebuilds
        the worker on the same queues and swaps it in.
        """
        return [
            # MQTT ingest is a source with no input queue: stall_detection off,
            # since "hasn't consumed a queue lately" isn't its liveness contract
            # (run() just blocks on the stop event; paho owns reconnect).
            SupervisedTarget(
                name="MqttAudioIngest",
                is_alive=lambda: self._w("_audio_source").is_alive(),
                is_stopping=lambda: self._w("_audio_source").stopping,
                last_activity=lambda: self._w("_audio_source").last_activity,
                restart=lambda: self._restart("_audio_source", self._build_mqtt_subscriber),
                stall_detection=False,
            ),
            SupervisedTarget(
                name="ChannelRouter",
                is_alive=lambda: self._w("_router").is_alive(),
                is_stopping=lambda: self._w("_router").stopping,
                last_activity=lambda: self._w("_router").last_activity,
                restart=lambda: self._restart("_router", self._build_router),
                input_pending=lambda: self._queue_pending(self._ingest_queue),
            ),
            SupervisedTarget(
                name="VADWorker",
                is_alive=lambda: self._w("_vad").is_alive(),
                is_stopping=lambda: self._w("_vad").stopping,
                last_activity=lambda: self._w("_vad").last_activity,
                restart=lambda: self._restart("_vad", self._build_vad),
                input_pending=lambda: self._queue_pending(self._routed_queue),
                pending_loss=lambda: self._w("_vad").pending_loss(),
            ),
            SupervisedTarget(
                name="STTWorker",
                is_alive=lambda: self._w("_stt").is_alive(),
                is_stopping=lambda: self._w("_stt").stopping,
                last_activity=lambda: self._w("_stt").last_activity,
                restart=lambda: self._restart("_stt", self._build_stt),
                input_pending=lambda: self._queue_pending(self._segment_queue),
            ),
        ]

    def _restart(self, attr: str, build_fn: Callable[[], threading.Thread]) -> None:
        """Replace worker `attr` with a fresh instance on the same queues.

        Runs on the supervisor's restart thread. A crashed worker's thread has
        already exited, so the join returns at once and the swap is clean. A
        *stalled* worker cannot be force-killed (Python has no thread kill): we
        signal it and join briefly; if it stays wedged it lingers as a zombie
        on the shared input queue, which is exactly the case the OS watchdog's
        full-process restart is the real remedy for -- the in-process swap here
        is best-effort so the pipeline resumes progress in the meantime.
        """
        old = getattr(self, attr)
        self._signal(old)
        self._join(old)
        if old.is_alive():
            logger.warning(
                "Orchestrator: %s did not exit after signal -- a zombie thread may "
                "linger on its input queue until a full process restart",
                getattr(old, "name", attr),
            )
        new = build_fn()
        setattr(self, attr, new)
        # Don't resurrect a worker into a pipeline that's already tearing down;
        # stop() sets both of these before joining the supervisor.
        if self._running and not self._stop_event.is_set():
            new.start()

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
            # Closes the segment-lifecycle trace (audio_ingest -> channel ->
            # vad -> stt -> transcript): the only orchestrator-level log line
            # carrying stage/channel_id/segment_id, since it's the segment's
            # final event even though this callback lives here, not in stt/.
            logger.info(
                "TRANSCRIPT channel=%s segment=%s [%.2f-%.2f] %r",
                event.channel_id,
                event.segment_id,
                event.start,
                event.end,
                event.text,
                extra={
                    "stage": "stt",
                    "channel_id": event.channel_id,
                    "segment_id": event.segment_id,
                },
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
