"""Supervisor: the in-process recovery layer for the worker threads.

Milestone 6, layers 1-2 (docs/BUILDPLAN.md). One background thread that
watches the pipeline's worker threads and restarts any that crash or wedge,
backing off to a "degraded" state after too many restarts in a window. It
also drives the OS watchdog ping (layer 3) from the same tick, so the two
recovery layers share one heartbeat.

Deliberately generic
────────────────────
This module knows nothing about VAD, STT, or MQTT. It operates on a list of
`SupervisedTarget`s, each a bundle of callables (is this alive? did we ask it
to stop? when did it last do work? restart it). The orchestrator builds those
targets, wiring the callables to its own worker instances -- so the "how do I
rebuild a VADWorker" knowledge stays in the orchestrator, and this stays a
plain thread-watchdog.

Two failure modes, one that exit-detection alone would miss
───────────────────────────────────────────────────────────
  - Crash: the worker thread exited (is_alive() False) without us asking it to
    (is_stopping() False). Restart it.
  - Stall: the worker is still alive and NOT stopping, but has work waiting on
    its input queue that it hasn't touched for stall_timeout_s -- a deadlock or
    a hang inside a native call. Exit-detection can't see this (nothing
    exited) and neither can the OS watchdog (the rest of the process, this
    heartbeat included, keeps ticking fine). Treated the same as a crash.
    The input-queue check is what avoids false positives: a worker that is
    simply idle (no packets, empty queue) is never flagged.

Restart work runs off the tick
──────────────────────────────
Rebuilding a worker can be slow (e.g. a Moonshine model reload), and the same
tick that detects a crash also pings the watchdog. If a slow rebuild ran
inline, it could delay the ping past WatchdogSec and trip a spurious
whole-process restart on top of the one already in progress. So the tick only
*detects and dispatches*: the actual restart runs in a short-lived daemon
thread, and the heartbeat stays on schedule regardless.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

from edge_voice.pipeline import systemd_watchdog

logger = logging.getLogger(__name__)

# State strings surfaced through status() and, via the orchestrator, the UI.
STATE_RUNNING = "running"
STATE_RESTARTING = "restarting"
STATE_DEGRADED = "degraded"


@dataclass
class SupervisedTarget:
    """One worker the supervisor watches, as callables so it stays decoupled.

    The callables must read the *current* worker instance each call (i.e. close
    over the orchestrator attribute, not a captured instance), so that after a
    restart swaps in a fresh worker they observe the new one -- see
    PipelineOrchestrator._build_supervisor_targets.
    """

    name: str
    is_alive: Callable[[], bool]
    is_stopping: Callable[[], bool]
    last_activity: Callable[[], float]  # time.monotonic() of last work done
    restart: Callable[[], None]  # rebuild + start a fresh instance, swap it in
    # Whether work is waiting on this worker's input queue. Only meaningful for
    # stall detection; a source with no input queue leaves this False so it is
    # never stall-restarted (its liveness isn't a "consume the queue" contract).
    input_pending: Callable[[], bool] = field(default=lambda: False)
    # Reports in-progress work a restart would discard, logged as its own event
    # before the worker is replaced. Default: nothing to report.
    pending_loss: Callable[[], str | None] = field(default=lambda: None)
    stall_detection: bool = True


@dataclass
class _TargetState:
    target: SupervisedTarget
    restarts: deque[float] = field(default_factory=deque)  # monotonic times, windowed
    degraded: bool = False
    restarting: bool = False
    state: str = STATE_RUNNING


class Supervisor(threading.Thread):
    """Watches worker threads; restarts crashes and stalls; pings the watchdog."""

    def __init__(
        self,
        targets: list[SupervisedTarget],
        tick_interval_s: float = 2.0,
        stall_timeout_s: float = 10.0,
        max_restarts: int = 3,
        restart_window_s: float = 60.0,
        watchdog_enabled: bool = True,
        name: str = "Supervisor",
    ) -> None:
        super().__init__(name=name, daemon=True)
        self._states = {t.name: _TargetState(target=t) for t in targets}
        self._tick_interval_s = tick_interval_s
        self._stall_timeout_s = stall_timeout_s
        self._max_restarts = max_restarts
        self._restart_window_s = restart_window_s
        self._watchdog_enabled = watchdog_enabled
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._restart_threads: list[threading.Thread] = []

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def run(self) -> None:
        logger.info(
            "Supervisor started (watching %d worker(s); watchdog=%s)",
            len(self._states),
            "on" if (self._watchdog_enabled and systemd_watchdog.available()) else "off",
        )
        if self._watchdog_enabled:
            # Type=notify units wait for this before considering the service up.
            systemd_watchdog.notify("READY=1")

        while not self._stop_event.wait(self._tick_interval_s):
            # Ping FIRST, before any (potentially slow) detection/dispatch, so
            # the heartbeat never rides behind restart bookkeeping.
            if self._watchdog_enabled:
                systemd_watchdog.notify("WATCHDOG=1")
            self._scan()

        self._await_restarts()
        logger.info("Supervisor stopped")

    # ── Detection ────────────────────────────────────────────────

    def _scan(self) -> None:
        now = time.monotonic()
        for ts in list(self._states.values()):
            with self._lock:
                if ts.degraded or ts.restarting:
                    continue
            reason = self._failure_reason(ts.target, now)
            if reason is not None:
                self._trigger_restart(ts, reason)

    def _failure_reason(self, target: SupervisedTarget, now: float) -> str | None:
        """Why this target needs restarting, or None if it's healthy.

        An intentional stop (is_stopping True) is never a failure -- that's the
        orchestrator tearing the pipeline down, not a crash.
        """
        if target.is_stopping():
            return None
        if not target.is_alive():
            return "crashed (thread exited)"
        if (
            target.stall_detection
            and target.input_pending()
            and (now - target.last_activity()) > self._stall_timeout_s
        ):
            stalled_for = now - target.last_activity()
            return f"stalled ({stalled_for:.1f}s no progress with input queued)"
        return None

    # ── Restart (dispatched off the tick) ────────────────────────

    def _trigger_restart(self, ts: _TargetState, reason: str) -> None:
        now = time.monotonic()
        with self._lock:
            # Prune restarts outside the window, then apply the budget.
            while ts.restarts and (now - ts.restarts[0]) > self._restart_window_s:
                ts.restarts.popleft()

            if len(ts.restarts) >= self._max_restarts:
                ts.degraded = True
                ts.state = STATE_DEGRADED
                logger.error(
                    "Supervisor: %s %s -- exceeded %d restarts in %.0fs, marking DEGRADED "
                    "and giving up in-process (OS watchdog handles process-level recovery)",
                    ts.target.name,
                    reason,
                    self._max_restarts,
                    self._restart_window_s,
                )
                return

            ts.restarts.append(now)
            ts.restarting = True
            ts.state = STATE_RESTARTING
            attempt = len(ts.restarts)

        logger.warning(
            "Supervisor: %s %s -- restarting (attempt %d/%d in window)",
            ts.target.name,
            reason,
            attempt,
            self._max_restarts,
        )
        t = threading.Thread(
            target=self._restart_worker, args=(ts,), name=f"restart-{ts.target.name}", daemon=True
        )
        with self._lock:
            self._restart_threads.append(t)
        t.start()

    def _restart_worker(self, ts: _TargetState) -> None:
        # Surface any in-progress work the dead worker was holding, as its own
        # event, before we replace it and that state is gone for good.
        try:
            loss = ts.target.pending_loss()
        except Exception:
            loss = None
            logger.exception("Supervisor: %s pending_loss() raised", ts.target.name)
        if loss:
            logger.error(
                "Supervisor: %s lost in-progress audio on restart: %s", ts.target.name, loss
            )

        try:
            ts.target.restart()
            logger.info("Supervisor: %s restarted", ts.target.name)
            with self._lock:
                if not ts.degraded:
                    ts.state = STATE_RUNNING
        except Exception:
            logger.exception("Supervisor: %s restart FAILED", ts.target.name)
            # Leave state as-is; the next tick re-evaluates and may retry within
            # the remaining budget, or degrade if the budget is now spent.
        finally:
            with self._lock:
                ts.restarting = False

    def _await_restarts(self) -> None:
        """Join any in-flight restart threads on shutdown (bounded)."""
        with self._lock:
            threads = list(self._restart_threads)
        for t in threads:
            t.join(timeout=self._tick_interval_s)

    # ── Status (read by orchestrator.get_status) ─────────────────

    def status(self) -> dict[str, dict[str, object]]:
        with self._lock:
            return {
                name: {"state": ts.state, "restarts": len(ts.restarts)}
                for name, ts in self._states.items()
            }

    def is_degraded(self) -> bool:
        with self._lock:
            return any(ts.degraded for ts in self._states.values())
