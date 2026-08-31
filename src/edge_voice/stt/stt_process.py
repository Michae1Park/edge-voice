"""Run an STTWorker in its own OS process, one per channel.

Why this exists
───────────────
Moonshine's native decode does not release the GIL, so two STTWorker
*threads* serialize: measured 1.02x speedup from two threads on the RPi5
(scratch/probe_gil_release.py), i.e. no benefit at all. Separate processes
do overlap -- 84% of decode time genuinely concurrent, 1.70x throughput
(scratch/probe_mp_speedup.py). See docs/archived/STT_MULTIPROCESS_PLAN.md for the
full evidence and the design decisions this module implements.

What it does NOT change
───────────────────────
The decode logic itself. `stt_child_main` builds an ordinary `STTWorker`
and calls its ordinary `run()` loop -- the same `_handle_segment` /
`_transcribe` / repetitive-output guard that unit tests already cover.
Only *where* that loop runs, and *how* its output gets home, is different.
`STTWorker` remains usable as a plain thread (settings.stt.use_processes),
which is what keeps tests fast and synchronous.

Two adapters make the reuse work:
  - A bridge thread translates between the process-level primitives the
    parent owns (mp.Event, mp.Value) and the thread-level ones STTWorker
    already uses internally.
  - `STTProcessHandle` presents a `threading.Thread`-shaped surface, so
    orchestrator.py's `_get_workers()` / `_signal()` / `_join()` and
    supervisor.py's `SupervisedTarget` keep working unchanged.
"""

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

# How often the bridge thread copies the worker's activity clock out to the
# parent. Well under reliability.stall_timeout_s (10s), so the supervisor
# never sees a stale heartbeat from sampling alone.
HEARTBEAT_INTERVAL_S = 0.5

# How long the child blocks on its outbound put before giving up. The parent
# drains continuously, so this only matters if the receiver thread has died;
# blocking forever there would wedge the child against shutdown.
RESULT_PUT_TIMEOUT_S = 5.0


def _read_self_cpu_ticks() -> int | None:
    """Cumulative (utime+stime) CPU ticks for this process, from /proc/self/stat.

    Same fields as bench_pipeline_load.py's _read_proc (utime=14, stime=15,
    1-indexed in proc(5); comm split off by its last ')' since comm itself
    can contain spaces/parens). None on any read failure -- the caller must
    treat that as "no signal this tick", not crash the bridge thread, since
    a dead bridge thread means stop_event is never observed either.
    """
    try:
        with open("/proc/self/stat") as f:
            after_comm = f.read().rsplit(")", 1)[1].split()
        return int(after_comm[11]) + int(after_comm[12])
    except Exception:
        return None


@dataclass(slots=True)
class SttResult:
    """One transcript, on its way back from a child process.

    Carries the latency alongside the event on purpose: the parent's
    `_stt_latency_s`/`_stt_channel_latencies_s` used to read worker
    attributes directly, which is impossible across a process boundary.
    Piggybacking here needs no extra IPC and keeps the number paired with
    the segment it belongs to.

    `stt_latency_s` is None for partials, matching STTWorker's existing rule
    that `_handle_partial` never touches `last_latency_s`.
    """

    event: TranscriptEvent
    stt_latency_s: float | None


def _configure_child_logging(log_queue: Any, log_level: int) -> None:
    """Ship this process's log records to the parent, unformatted.

    Formatting stays in the parent so JsonFormatter's structured fields
    (stage/channel_id/segment_id/stt_latency_s) come out identical to a
    single-process run. QueueHandler.prepare() already handles making
    records picklable (it formats the message and drops exc_info).
    """
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
    """Translate parent-owned process primitives to the worker's thread ones.

    Runs alongside `worker.run()`: publishes a liveness clock to the shared
    heartbeat so the supervisor's stall check can see it, and converts the
    parent's mp stop signal into the threading stop the run loop already
    understands.

    Liveness = CPU progress, not just "a segment was dequeued"
    ─────────────────────────────────────────────────────────
    `worker.last_activity` (stt_worker.py) is stamped once, when a segment is
    dequeued, and not touched again until the next one -- because the actual
    decode is a single opaque, blocking `_transcribe()` call with no
    mid-call hook to update it from. That's indistinguishable from a real
    hang for as long as that one call runs. Segments are only bounded by
    `vad.max_segment_s` when `vad.segment_limits_enabled` is on (it defaults
    off); with it off, or with any segment slow enough on the deployed
    hardware, a single legitimate decode can run past stall_timeout_s (10s
    default) and get killed mid-work -- reproduced on the RPi5 with a
    near-continuous-speech clip (no pauses for VAD to cut on): STTWorker-rx/
    tx stalled and were SIGKILLed while still genuinely decoding, and kept
    re-stalling on the next long segment until the supervisor gave up and
    marked them DEGRADED.

    This thread is never blocked by that call (it's a separate thread in the
    same child process), so instead of just relaying `worker.last_activity`
    it independently checks whether the process is still burning CPU
    (/proc/self/stat, same technique as bench_pipeline_load.py's sampler)
    each tick, and republishes "now" for as long as ticks keep advancing --
    true regardless of segment length or decode speed. A worker actually
    wedged (blocked on a lock or syscall -- the real failure mode this
    exists to catch) burns no CPU, so its heartbeat still goes stale and
    still gets caught. `worker.last_activity` is kept as the value on a
    flat-CPU tick (rather than freezing the last heartbeat outright), so a
    freshly dequeued segment still reads as active immediately, and a
    genuinely idle worker's heartbeat still goes stale as before -- harmless
    either way since the supervisor only stall-restarts a worker that also
    has `input_pending()`.
    """
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
    """Child process entry point.

    MUST stay module-level: `spawn` pickles the target by qualified name, so
    a closure, lambda, or bound method fails at `Process.start()`.

    Order matters. Logging is installed first so a model-load failure is
    visible; `ready_event` is set only once the model is loaded and the run
    loop is about to start, because the supervisor's stall detection is
    gated on it (see STTProcessHandle.last_activity).
    """
    # The parent orchestrates shutdown via stop_event. Without this, Ctrl-C
    # goes to the whole process group and every child raises KeyboardInterrupt
    # mid-decode, burying the real shutdown in tracebacks.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    _configure_child_logging(log_queue, log_level)

    # Bound after construction: the callback needs the worker (for its
    # latency), and the worker needs the callback. STTWorker sets
    # _last_latency_s immediately before invoking on_transcript on this same
    # thread, so the value read here always belongs to this exact segment.
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

    # Seed before announcing readiness, so there is no window where the
    # parent trusts the heartbeat but it is still the initial 0.0.
    heartbeat.value = time.monotonic()
    ready_event.set()

    bridge = threading.Thread(target=_bridge, args=(worker, stop_event, heartbeat), daemon=True)
    bridge.start()

    # The ordinary run loop -- unchanged, and the reason this whole design
    # needs no fork of the decode path.
    worker.run()


class STTProcessHandle:
    """A `threading.Thread`-shaped facade over a per-channel STT process.

    orchestrator.py and supervisor.py duck-type on Thread (`name`, `ident`,
    `start`, `join`, `is_alive`, `stop`, `stopping`, `last_activity`), so
    presenting that exact surface is what lets `_get_workers()`,
    `_signal()`, `_join()`, `_restart_dict_worker()` and `SupervisedTarget`
    work against a process with no special-casing.

    Adds two things a thread cannot offer: `kill()` (a wedged thread is a
    permanent zombie; a wedged process is reclaimable) and `exitcode` (a
    segfault surfaces as -11 instead of silence).
    """

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
        # Must match the SupervisedTarget name the orchestrator builds, or
        # get_status() cannot merge supervisor state onto this worker's row.
        self.name = f"STTWorker-{channel_id}"
        self.segment_queue = segment_queue
        self.transcript_queue = ctx.Queue()
        self._stop_event = ctx.Event()
        self._ready_event = ctx.Event()
        self._heartbeat = ctx.Value("d", 0.0)
        # Held deliberately, not just passed through to Process(args=...):
        # Process.start() does `del self._target, self._args, self._kwargs`
        # to break a refcycle, so args are NOT kept alive by the Process.
        # If the caller passed a freshly-constructed queue inline and this
        # were the only other reference, it would be garbage collected in
        # the parent, closing the read end -- and the child would then block
        # writing its first log record during model load and never signal
        # ready. Cost two 180s test timeouts to find.
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
            # Safety net: if the parent dies hard, children die with it
            # rather than lingering with the model resident.
            daemon=True,
        )

    # ── threading.Thread-shaped surface ─────────────────────────

    @property
    def ident(self) -> int | None:
        """The child's pid. Named `ident` because orchestrator._join() checks
        `ident is None` to detect a never-started worker; mapping it to pid
        keeps that check working untouched."""
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
        """Monotonic clock of the child's last known-live moment, per _bridge().

        Not just "last dequeued segment" -- see _bridge()'s docstring for why
        that alone would misread a long-but-genuine decode as a hang. This is
        that CPU-progress-aware heartbeat, republished into shared memory.

        Cross-process safe: on Linux `time.monotonic()` is CLOCK_MONOTONIC,
        which is system-wide, so the child's value is directly comparable to
        the parent's.

        Reports "active right now" until the child is ready. That gate is
        load-bearing, not cosmetic: model load takes 3.6-7.3s on the RPi5
        (plus spawn+import), against a 10s stall_timeout_s, while segments
        pile up so `input_pending()` is already True. Without this, the
        supervisor would stall-restart a child that is merely still
        loading -- and the replacement would take just as long, producing an
        infinite restart loop that ends in DEGRADED on first boot.
        """
        if not self._ready_event.is_set():
            return time.monotonic()
        value = float(self._heartbeat.value)
        # Belt and braces for the sliver between ready and the first bridge
        # tick; a 0.0 here would read as "last active in 1970".
        return value if value > 0.0 else time.monotonic()

    # ── process-only extras ─────────────────────────────────────

    @property
    def ready(self) -> bool:
        return bool(self._ready_event.is_set())

    def wait_ready(self, timeout: float) -> bool:
        """Block until the child has loaded its model, or `timeout` elapses."""
        return bool(self._ready_event.wait(timeout))

    @property
    def exitcode(self) -> int | None:
        return self._proc.exitcode

    def kill(self) -> None:
        """SIGKILL the child. The reliability upgrade over threads: a wedged
        thread can only be abandoned, a wedged process can be reclaimed."""
        if self._proc.pid is not None and self._proc.is_alive():
            self._proc.kill()


def new_mp_context() -> "SpawnContext":
    """`spawn`, never `fork`.

    Forking a parent that has already loaded native ONNX Runtime / torch
    state can corrupt it in the child. The cost is a fresh interpreter and
    a full re-import per child, which is exactly why readiness is signalled
    explicitly rather than assumed.
    """
    return mp.get_context("spawn")
