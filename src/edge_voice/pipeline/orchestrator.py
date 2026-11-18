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
from edge_voice.observability.logging import current_log_level, start_log_queue_listener
from edge_voice.vad.vad_worker import VADWorker, VADWorkerConfig
from edge_voice.stt.sensevoice.config import SenseVoiceConfig
from edge_voice.stt.stt_process import STTProcessHandle, new_mp_context
from edge_voice.stt.stt_worker import STTWorker, STTWorkerConfig
from edge_voice.pipeline.supervisor import Supervisor, SupervisedTarget
from edge_voice.observability.metrics import MetricsCollector, MetricsSnapshot
from edge_voice.health.reporting import build_health_report

logger = logging.getLogger(__name__)

WORKER_JOIN_TIMEOUT_S = 10

RECEIVER_POLL_TIMEOUT_S = 0.2


class PipelineOrchestrator:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ingest_queue: queue.Queue | None = None
        self._routed_queue: queue.Queue | None = None
        self._dump_queue: queue.Queue | None = None
        # mp.Queue in process mode, queue.Queue otherwise; both share put/get/qsize and raise queue.Full/Empty.
        self._segment_queues: dict[str, Any] | None = None
        self._segment_dump_queue: queue.Queue | None = None
        self._audio_source: threading.Thread | None = None
        self._router: threading.Thread | None = None
        self._vad: threading.Thread | None = None
        self._stt: dict[str, Any] = {}
        self._dump_worker: threading.Thread | None = None
        self._segment_dump_worker: threading.Thread | None = None
        self._mp_ctx: Any = None
        self._log_queue: Any = None
        self._log_listener: Any = None
        self._stt_receivers: dict[str, threading.Thread] = {}
        self._receivers_stop = threading.Event()
        # Process mode can't read worker attributes, so receivers mirror SttResult latency here.
        self._stt_latency_lock = threading.Lock()
        self._stt_last_latency_s: dict[str, float] = {}
        self._supervisor: Supervisor | None = None
        self._metrics: MetricsCollector | None = None
        self._stop_event = threading.Event()
        self._running = False
        self._transcript_hub = TranscriptHub(backlog=settings.webui.transcript_backlog)

    @property
    def ingest_queue(self) -> queue.Queue:
        if self._ingest_queue is None:
            raise RuntimeError("Pipeline not built. Call build() first.")
        return self._ingest_queue

    @property
    def transcripts(self) -> TranscriptHub:
        return self._transcript_hub

    def build(self) -> None:
        self._stop_event.clear()
        self._running = False

        self._ingest_queue = make_ingest_queue(maxsize=self._settings.queues.ingest)
        self._routed_queue = make_routed_queue(maxsize=self._settings.queues.routed)
        if self._settings.stt.use_processes:
            self._mp_ctx = new_mp_context()
            self._log_queue = self._mp_ctx.Queue()
            self._segment_queues = {
                c.channel_id: self._mp_ctx.Queue(maxsize=self._settings.queues.segment)
                for c in self._settings.mqtt.channels
            }
        else:
            self._segment_queues = {
                c.channel_id: make_segment_queue(maxsize=self._settings.queues.segment)
                for c in self._settings.mqtt.channels
            }
        self._dump_queue = None
        self._segment_dump_queue = None

        if self._settings.dump.enabled:
            self._dump_queue = make_dump_queue(maxsize=self._settings.queues.dump)
            self._dump_worker = self._build_audio_dump()
        if self._settings.segment_dump.enabled:
            self._segment_dump_queue = make_dump_queue(maxsize=self._settings.queues.dump)
            self._segment_dump_worker = self._build_segment_dump()

        self._audio_source = self._build_mqtt_subscriber()
        self._router = self._build_router()
        self._vad = self._build_vad()
        self._prefetch_stt_models()
        self._stt = {
            c.channel_id: self._build_stt(c.channel_id) for c in self._settings.mqtt.channels
        }

        self._supervisor = self._build_supervisor() if self._settings.reliability.enabled else None

        self._metrics = self._build_metrics() if self._settings.metrics.enabled else None

        logger.info(
            "Pipeline built with channels: %s", [c.channel_id for c in self._settings.mqtt.channels]
        )

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Before any child starts, so its first log record has a drained queue to go to.
        if self._settings.stt.use_processes and self._log_queue is not None:
            self._log_listener = start_log_queue_listener(self._log_queue)

        for w in self._get_workers():
            w.start()

        if self._settings.stt.use_processes:
            self._start_stt_receivers()
            self._await_stt_ready()

        # Started last so its first scan never sees an unstarted or still-loading worker as crashed.
        if self._supervisor is not None:
            self._supervisor.start()
        if self._metrics is not None:
            self._metrics.start()
        logger.info("Pipeline started")

    # Keeps model loading off the real-time path. A child that never reports ready is left for the supervisor.
    def _await_stt_ready(self) -> None:
        timeout = self._settings.stt.process_start_timeout_s
        deadline = time.monotonic() + timeout
        for channel_id, handle in self._stt.items():
            remaining = max(0.0, deadline - time.monotonic())
            if handle.wait_ready(remaining):
                continue
            logger.error(
                "STTWorker-%s did not become ready within %.0fs (model still loading, or it "
                "failed to start) -- leaving it to the supervisor",
                channel_id,
                timeout,
                extra={"channel_id": channel_id},
            )

    # Upstream-first with per-stage joins: VAD flushes its last segment on exit, so consumers must still be alive.
    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        if self._metrics is not None:
            self._signal(self._metrics)
            self._join(self._metrics)
        # Stop before workers, or it races to restart workers we are deliberately stopping.
        if self._supervisor is not None:
            self._signal(self._supervisor)
            self._join(self._supervisor)
        workers = self._get_workers()
        try:
            for w in workers:
                self._signal(w)
                self._join(w)
        finally:
            # A second Ctrl-C can interrupt the drain; non-daemon workers left unsignalled would hang the process.
            for w in workers:
                self._signal(w)
            # Receivers stop last: a child holding undelivered transcripts cannot exit.
            if self._settings.stt.use_processes:
                self._stop_stt_receivers()
                self._stop_log_listener()
                self._abandon_segment_queue_backlog()

    # An unread mp.Queue backlog leaves its feeder thread stuck in pipe_write, and multiprocessing joins
    # it without timeout at exit, hanging the whole process. Safe because STT is already stopped.
    def _abandon_segment_queue_backlog(self) -> None:
        if self._segment_queues is None:
            return
        for q in self._segment_queues.values():
            canceller = getattr(q, "cancel_join_thread", None)
            if callable(canceller):
                canceller()

    def _stop_log_listener(self) -> None:
        if self._log_listener is not None:
            self._log_listener.stop()
            self._log_listener = None

    def wait(self) -> None:
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
        if worker.ident is None:
            return
        worker.join(timeout=WORKER_JOIN_TIMEOUT_S)
        if not worker.is_alive():
            return
        logger.warning("Worker %s did not stop within %ss", worker.name, WORKER_JOIN_TIMEOUT_S)
        # Unlike threads, a wedged STT process can be reclaimed; a long decode under backlog can exceed the join timeout.
        killer = getattr(worker, "kill", None)
        if callable(killer):
            logger.warning("Worker %s did not exit after signal -- killing it", worker.name)
            killer()
            worker.join(timeout=WORKER_JOIN_TIMEOUT_S)

    def get_status(self) -> dict:
        running = self._running
        workers = {w.name: ("running" if w.is_alive() else "stopped") for w in self._get_workers()}
        degraded = False
        if self._supervisor is not None:
            sup = self._supervisor.status()
            for name, info in sup.items():
                if name in workers and workers[name] == "running":
                    workers[name] = str(info["state"])
            degraded = self._supervisor.is_degraded()
            # Nothing supervises the supervisor, so surface its own liveness.
            workers[self._supervisor.name] = "running" if self._supervisor.is_alive() else "stopped"
        return {"running": running, "degraded": degraded, "workers": workers}

    def queue_depths(self) -> dict[str, int]:
        queues = {
            "ingest": self._ingest_queue,
            "routed": self._routed_queue,
            "dump": self._dump_queue,
            "segment_dump": self._segment_dump_queue,
        }
        depths = {name: q.qsize() for name, q in queues.items() if q is not None}
        if self._segment_queues is not None:
            depths.update({f"segment_{cid}": q.qsize() for cid, q in self._segment_queues.items()})
        return depths

    def metrics_snapshot(self) -> MetricsSnapshot | None:
        return self._metrics.snapshot() if self._metrics is not None else None

    def channel_freshness(self) -> dict[str, float | None]:
        router = self._w("_router")
        if router is None:
            return {c.channel_id: None for c in self._settings.mqtt.channels}
        return {cid: router.get_freshness(cid) for cid in router.get_channel_ids()}

    def health(self) -> dict:
        return build_health_report(
            status=self.get_status(),
            queue_depths=self.queue_depths(),
            snapshot=self.metrics_snapshot(),
            metrics_enabled=self._settings.metrics.enabled,
            freshness=self.channel_freshness(),
            stale_after_s=self._settings.health.stale_segment_warning_s,
        )

    def run(self, duration_s: float | None = None) -> None:
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

    # Order is load-bearing: stop() shuts down producers before their consumers.
    def _get_workers(self) -> list[threading.Thread]:
        workers: list[threading.Thread | None] = [
            self._audio_source,
            self._router,
            self._vad,
            *self._stt.values(),
            self._dump_worker,
            self._segment_dump_worker,
        ]
        return [w for w in workers if w is not None]

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

    def _build_metrics(self) -> MetricsCollector:
        m = self._settings.metrics
        return MetricsCollector(
            queue_depths=self.queue_depths,
            stt_latency_s=self._stt_latency_s,
            vad_silero_latencies_s=lambda: self._w("_vad").silero_latencies_s(),
            vad_rms_gate_latencies_s=lambda: self._w("_vad").rms_gate_latencies_s(),
            router_repacketize_latencies_s=lambda: self._w("_router").repacketize_latencies_s(),
            stt_channel_latencies_s=self._stt_channel_latencies_s,
            supervisor=lambda: self._supervisor,
            mqtt_connected=self._mqtt_connected,
            emit_interval_s=m.emit_interval_s,
            latency_log_decimals=m.latency_log_decimals,
        )

    # Worst channel, i.e. the one closest to its real-time budget.
    def _stt_latency_s(self) -> float | None:
        values = list(self._stt_channel_latencies_s().values())
        return max(values) if values else None

    def _stt_channel_latencies_s(self) -> dict[str, float]:
        if self._settings.stt.use_processes:
            with self._stt_latency_lock:
                return dict(self._stt_last_latency_s)

        merged: dict[str, float] = {}
        for w in self._stt.values():
            merged.update(w.channel_latencies_s())  # type: ignore[attr-defined]
        return merged

    def _start_stt_receivers(self) -> None:
        self._receivers_stop.clear()
        for channel_id in self._stt:
            thread = threading.Thread(
                target=self._stt_receiver_loop,
                args=(channel_id,),
                name=f"STTReceiver-{channel_id}",
                daemon=True,
            )
            thread.start()
            self._stt_receivers[channel_id] = thread

    def _stt_receiver_loop(self, channel_id: str) -> None:
        while not self._receivers_stop.is_set():
            # Re-read each poll: a restart swaps in a new handle with a new transcript queue.
            handle = self._stt.get(channel_id)
            if handle is None:
                return
            try:
                result = handle.transcript_queue.get(timeout=RECEIVER_POLL_TIMEOUT_S)
            except queue.Empty:
                continue
            except (EOFError, OSError):
                # Queue gone (child killed); wait for a supervisor restart rather than exiting.
                self._receivers_stop.wait(RECEIVER_POLL_TIMEOUT_S)
                continue
            try:
                if result.stt_latency_s is not None:
                    with self._stt_latency_lock:
                        self._stt_last_latency_s[channel_id] = result.stt_latency_s
                self._publish_transcript(result.event, result.stt_latency_s)
            except Exception:
                logger.exception(
                    "STT receiver failed on a result from channel=%s",
                    channel_id,
                    extra={"channel_id": channel_id},
                )

    def _stop_stt_receivers(self) -> None:
        self._receivers_stop.set()
        for thread in list(self._stt_receivers.values()):
            thread.join(timeout=WORKER_JOIN_TIMEOUT_S)
        self._stt_receivers.clear()

    def _mqtt_connected(self) -> bool | None:
        return getattr(self._w("_audio_source"), "connected", None)

    # Read through this rather than a captured reference so a restart's replacement worker is observed.
    def _w(self, attr: str) -> Any:
        return getattr(self, attr)

    @staticmethod
    def _queue_pending(q: queue.Queue | None) -> bool:
        return q is not None and q.qsize() > 0

    def _build_supervisor_targets(self) -> list[SupervisedTarget]:
        return [
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
        ] + self._build_stt_supervisor_targets()

    def _build_stt_supervisor_targets(self) -> list[SupervisedTarget]:
        if self._segment_queues is None:
            raise RuntimeError("Segment queues not initialized")
        segment_queues = self._segment_queues

        targets: list[SupervisedTarget] = []
        for cid in self._stt:
            targets.append(self._build_one_stt_supervisor_target(cid, segment_queues))
        return targets

    def _build_one_stt_supervisor_target(
        self, cid: str, segment_queues: dict[str, queue.Queue]
    ) -> SupervisedTarget:
        stt = self._stt

        def is_alive() -> bool:
            return stt[cid].is_alive()

        def is_stopping() -> bool:
            return stt[cid].stopping  # type: ignore[attr-defined]

        def last_activity() -> float:
            return stt[cid].last_activity  # type: ignore[attr-defined]

        def restart() -> None:
            self._restart_dict_worker(stt, cid, lambda: self._build_stt(cid))

        def input_pending() -> bool:
            return self._queue_pending(segment_queues[cid])

        return SupervisedTarget(
            name=f"STTWorker-{cid}",
            is_alive=is_alive,
            is_stopping=is_stopping,
            last_activity=last_activity,
            restart=restart,
            input_pending=input_pending,
        )

    # A stalled thread can't be killed; it may linger as a zombie until the OS watchdog restarts the process.
    def _restart(self, attr: str, build_fn: Callable[[], threading.Thread]) -> None:
        old = getattr(self, attr)
        self._signal(old)
        self._join(old)
        if old.is_alive():
            # A native decoder segfault shows up here as -11.
            logger.warning(
                "Orchestrator: %s did not exit after signal -- a zombie thread may "
                "linger on its input queue until a full process restart",
                getattr(old, "name", attr),
            )
        new = build_fn()
        setattr(self, attr, new)
        if self._running and not self._stop_event.is_set():
            new.start()

    def _restart_dict_worker(
        self,
        workers: dict[str, Any],
        key: str,
        build_fn: Callable[[], Any],
    ) -> None:
        old = workers[key]
        self._signal(old)
        self._join(old)
        if old.is_alive():
            logger.warning(
                "Orchestrator: %s did not exit after signal -- a zombie thread may "
                "linger on its input queue until a full process restart",
                getattr(old, "name", key),
            )
        exitcode = getattr(old, "exitcode", None)
        if exitcode is not None and exitcode != 0:
            logger.warning(
                "Orchestrator: %s exited with code %s",
                getattr(old, "name", key),
                exitcode,
            )
        new = build_fn()
        workers[key] = new
        if self._running and not self._stop_event.is_set():
            new.start()
            # Wait for the model load, or the supervisor sees it as a stall and loops.
            waiter = getattr(new, "wait_ready", None)
            if callable(waiter):
                waiter(self._settings.stt.process_start_timeout_s)

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
        if self._routed_queue is None or self._segment_queues is None:
            raise RuntimeError("Queues not initialized")
        channels = [c.channel_id for c in self._settings.mqtt.channels]

        return VADWorker(
            self._routed_queue,
            self._segment_queues,
            channels,
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
                soft_cut_enabled=self._settings.vad.soft_cut_enabled,
                soft_cut_s=self._settings.vad.soft_cut_s,
                soft_cut_lookahead_s=self._settings.vad.soft_cut_lookahead_s,
                soft_cut_min_dip=self._settings.vad.soft_cut_min_dip,
                partial_interval_s=self._settings.vad.partial_interval_s,
                partial_min_segment_s=self._settings.vad.partial_min_segment_s,
            ),
        )

    def _publish_transcript(self, event, latency_s: float | None) -> None:
        # last_latency_s belongs to the latest final, so partials must not use the lifecycle-close branch below.
        if not event.is_final:
            logger.debug(
                "PARTIAL channel=%s segment=%s [%.2f-%.2f] %r",
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
            return

        latency = latency_s if latency_s is not None else float("nan")
        logger.info(
            "TRANSCRIPT channel=%s segment=%s [%.2f-%.2f] latency=%.3fs %r",
            event.channel_id,
            event.segment_id,
            event.start,
            event.end,
            latency,
            event.text,
            extra={
                "stage": "stt",
                "channel_id": event.channel_id,
                "segment_id": event.segment_id,
                "stt_latency_s": latency,
            },
        )
        self._transcript_hub.publish(event)

    def _build_stt(self, channel_id: str) -> Any:
        if self._segment_queues is None:
            raise RuntimeError("Segment queues not initialized")

        if self._settings.stt.use_processes:
            return STTProcessHandle(
                channel_id=channel_id,
                segment_queue=self._segment_queues[channel_id],
                config=self._stt_config(channel_id),
                ctx=self._mp_ctx,
                log_queue=self._log_queue,
                log_level=current_log_level(),
            )

        def _on_transcript(event) -> None:
            latency_s = (
                self._stt[channel_id].last_latency_s  # type: ignore[attr-defined]
                if event.is_final
                else None
            )
            self._publish_transcript(event, latency_s)

        return STTWorker(
            self._segment_queues[channel_id],
            _on_transcript,
            config=self._stt_config(channel_id),
            name=f"STTWorker-{channel_id}",
        )

    # Must stay picklable: process mode ships it to the child at spawn.
    def _stt_config(self, channel_id: str) -> STTWorkerConfig:
        stt = self._settings.stt
        sv = stt.sensevoice
        position = [c.channel_id for c in self._settings.mqtt.channels].index(channel_id)
        runtime = stt.runtime
        if runtime == "auto" and position >= sv.npu_max_channels:
            runtime = "cpu"
        return STTWorkerConfig(
            model=stt.model,
            sensevoice=SenseVoiceConfig(
                runtime=runtime,
                language=stt.language,
                use_itn=sv.use_itn,
                cache_dir=sv.cache_dir,
                cpu_model_dir=sv.cpu_model_dir,
                cpu_threads=sv.cpu_threads,
                npu_model_dir=sv.npu_model_dir,
                npu_window_s=sv.npu_window_s,
                npu_runtime_lib=sv.npu_runtime_lib,
                npu_core=position if runtime != "cpu" else None,
                probe_timeout_s=sv.npu_probe_timeout_s,
            ),
            language=stt.language,
            model_arch=stt.moonshine.model_arch,
            sample_rate=self._settings.audio.sample_rate,
            options={
                "max_tokens_per_second": stt.moonshine.max_tokens_per_second,
                "vad_threshold": stt.moonshine.vad_threshold,
                "identify_speakers": str(stt.moonshine.identify_speakers).lower(),
                "log_api_calls": str(stt.moonshine.log_api_calls).lower(),
                "save_input_wav_path": stt.moonshine.save_input_wav_path,
                "return_audio_data": str(stt.moonshine.return_audio_data).lower(),
            },
            partial_max_queue_depth=stt.partial_max_queue_depth,
        )

    # Download once in the parent so ~420 MB doesn't count against every child's start timeout.
    def _prefetch_stt_models(self) -> None:
        if self._settings.stt.model != "sensevoice":
            return
        from edge_voice.stt.sensevoice.factory import prefetch_models

        configs = [self._stt_config(c.channel_id).sensevoice for c in self._settings.mqtt.channels]
        try:
            prefetch_models(configs)
        except Exception as exc:
            logger.warning("SenseVoice model prefetch failed, workers will retry: %s", exc)

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
