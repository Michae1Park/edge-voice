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

HEARTBEAT_INTERVAL_S = 0.5

RESULT_PUT_TIMEOUT_S = 5.0


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


def _configure_child_logging(log_queue: Any, log_level: int) -> None:
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    root.addHandler(logging.handlers.QueueHandler(log_queue))
    root.setLevel(log_level)


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

    heartbeat.value = time.monotonic()
    ready_event.set()

    bridge = threading.Thread(target=_bridge, args=(worker, stop_event, heartbeat), daemon=True)
    bridge.start()

    worker.run()


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
        self.name = f"STTWorker-{channel_id}"
        self.segment_queue = segment_queue
        self.transcript_queue = ctx.Queue()
        self._stop_event = ctx.Event()
        self._ready_event = ctx.Event()
        self._heartbeat = ctx.Value("d", 0.0)
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
            daemon=True,
        )


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


def new_mp_context() -> "SpawnContext":
    return mp.get_context("spawn")
