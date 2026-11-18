from __future__ import annotations

import logging
import logging.handlers
import multiprocessing as mp
import signal
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from edge_voice.pipeline.models import TranscriptEvent
from edge_voice.stt.stt_worker import STTWorker, STTWorkerConfig

if TYPE_CHECKING:  # pragma: no cover - typing only
    from multiprocessing.context import SpawnContext

logger = logging.getLogger(__name__)

# Well under reliability.stall_timeout_s so sampling alone never looks stale.
HEARTBEAT_INTERVAL_S = 0.5

# Only matters if the parent's receiver died; blocking forever would wedge the child against shutdown.
RESULT_PUT_TIMEOUT_S = 5.0


# None means "no signal this tick"; raising would kill the bridge thread and with it stop handling.
def _read_self_cpu_ticks() -> int | None:
    try:
        with open("/proc/self/stat") as f:
            after_comm = f.read().rsplit(")", 1)[1].split()
        return int(after_comm[11]) + int(after_comm[12])
    except Exception:
        return None


@dataclass(slots=True)
class SttResult:
    event: TranscriptEvent
    stt_latency_s: float | None


# Formatting stays in the parent so structured JSON fields match a single-process run.
def _configure_child_logging(log_queue: Any, log_level: int) -> None:
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    root.addHandler(logging.handlers.QueueHandler(log_queue))
    root.setLevel(log_level)


# Liveness is CPU progress, not last dequeue: one long decode has no mid-call hook and would look
# like a hang (reproduced on RPi5 with near-continuous speech). A truly wedged worker burns no CPU,
# so its heartbeat still goes stale.
def _bridge(
    worker: STTWorker,
    stop_event: Any,
    heartbeat: Any,
) -> None:
    last_ticks = _read_self_cpu_ticks()
    while True:
        if stop_event.wait(HEARTBEAT_INTERVAL_S):
            worker.stop()
            return
        ticks = _read_self_cpu_ticks()
        if ticks is not None and last_ticks is not None and ticks > last_ticks:
            heartbeat.value = time.monotonic()
        else:
            heartbeat.value = worker.last_activity
        last_ticks = ticks


# Must stay module-level: spawn pickles the target by qualified name.
def stt_child_main(
    channel_id: str,
    config: STTWorkerConfig,
    segment_queue: Any,
    transcript_queue: Any,
    stop_event: Any,
    heartbeat: Any,
    ready_event: Any,
    log_queue: Any,
    log_level: int,
) -> None:
    # The parent drives shutdown; otherwise Ctrl-C hits every child mid-decode.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    _configure_child_logging(log_queue, log_level)

    holder: dict[str, STTWorker] = {}

    def _on_transcript(event: TranscriptEvent) -> None:
        worker = holder["worker"]
        latency = worker.last_latency_s if event.is_final else None
        try:
            transcript_queue.put(
                SttResult(event=event, stt_latency_s=latency), timeout=RESULT_PUT_TIMEOUT_S
            )
        except Exception:
            logger.exception(
                "STT child %s could not deliver transcript for segment=%s",
                channel_id,
                event.segment_id,
                extra={"channel_id": channel_id, "segment_id": event.segment_id},
            )

    try:
        worker = STTWorker(
            segment_queue,
            _on_transcript,
            config=config,
            name=f"STTWorker-{channel_id}",
        )
    except Exception:
        logger.exception("STT child %s failed to build its worker; exiting", channel_id)
        raise

    holder["worker"] = worker

    # Seed before announcing readiness so the parent never trusts an initial 0.0.
    heartbeat.value = time.monotonic()
    ready_event.set()

    bridge = threading.Thread(target=_bridge, args=(worker, stop_event, heartbeat), daemon=True)
    bridge.start()

    worker.run()


# Thread-shaped facade so orchestrator and supervisor treat processes and threads alike.
class STTProcessHandle:
    def __init__(
        self,
        channel_id: str,
        segment_queue: Any,
        config: STTWorkerConfig,
        ctx: "SpawnContext",
        log_queue: Any,
        log_level: int,
    ) -> None:
        self.channel_id = channel_id
        # Must match the orchestrator's SupervisedTarget name so status rows merge.
        self.name = f"STTWorker-{channel_id}"
        self.segment_queue = segment_queue
        self.transcript_queue = ctx.Queue()
        self._stop_event = ctx.Event()
        self._ready_event = ctx.Event()
        self._heartbeat = ctx.Value("d", 0.0)
        # Keep a reference: Process.start() drops its args, and a collected queue closes the read end,
        # blocking the child's first log record during model load.
        self._log_queue = log_queue
        self._proc = ctx.Process(
            target=stt_child_main,
            args=(
                channel_id,
                config,
                segment_queue,
                self.transcript_queue,
                self._stop_event,
                self._heartbeat,
                self._ready_event,
                log_queue,
                log_level,
            ),
            name=self.name,
            # If the parent dies hard, children die with it.
            daemon=True,
        )


    # pid, so orchestrator._join()'s `ident is None` never-started check still works.
    @property
    def ident(self) -> int | None:
        return self._proc.pid

    def start(self) -> None:
        self._proc.start()

    def join(self, timeout: float | None = None) -> None:
        self._proc.join(timeout)

    def is_alive(self) -> bool:
        return self._proc.is_alive()

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return bool(self._stop_event.is_set())

    # Reports "active now" until ready: model load (3.6-7.3s on RPi5) would otherwise look like a stall
    # and cause an endless restart loop. CLOCK_MONOTONIC is system-wide, so values compare across processes.
    @property
    def last_activity(self) -> float:
        if not self._ready_event.is_set():
            return time.monotonic()
        value = float(self._heartbeat.value)
        return value if value > 0.0 else time.monotonic()


    @property
    def ready(self) -> bool:
        return bool(self._ready_event.is_set())

    def wait_ready(self, timeout: float) -> bool:
        return bool(self._ready_event.wait(timeout))

    @property
    def exitcode(self) -> int | None:
        return self._proc.exitcode

    def kill(self) -> None:
        if self._proc.pid is not None and self._proc.is_alive():
            self._proc.kill()


# spawn, never fork: forking after native ONNX Runtime/torch state is loaded can corrupt the child.
def new_mp_context() -> "SpawnContext":
    return mp.get_context("spawn")
