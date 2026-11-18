from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

from edge_voice.pipeline import systemd_watchdog

logger = logging.getLogger(__name__)

STATE_RUNNING = "running"
STATE_RESTARTING = "restarting"
STATE_DEGRADED = "degraded"


# Callables must read the current worker each call so they observe a restart's replacement.
@dataclass
class SupervisedTarget:
    name: str
    is_alive: Callable[[], bool]
    is_stopping: Callable[[], bool]
    last_activity: Callable[[], float]
    restart: Callable[[], None]
    # False for sources with no input queue, so they're never stall-restarted.
    input_pending: Callable[[], bool] = field(default=lambda: False)
    pending_loss: Callable[[], str | None] = field(default=lambda: None)
    stall_detection: bool = True


@dataclass
class _TargetState:
    target: SupervisedTarget
    restarts: deque[float] = field(default_factory=deque)
    degraded: bool = False
    restarting: bool = False
    state: str = STATE_RUNNING


# Restarts run on short-lived threads so a slow model reload never delays the watchdog ping.
class Supervisor(threading.Thread):
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

    @property
    def max_restarts(self) -> int:
        return self._max_restarts

    @property
    def restart_window_s(self) -> float:
        return self._restart_window_s

    def run(self) -> None:
        logger.info(
            "Supervisor started (watching %d worker(s); watchdog=%s)",
            len(self._states),
            "on" if (self._watchdog_enabled and systemd_watchdog.available()) else "off",
        )
        if self._watchdog_enabled:
            systemd_watchdog.notify("READY=1")

        while not self._stop_event.wait(self._tick_interval_s):
            if self._watchdog_enabled:
                # Ping before detection/dispatch so the heartbeat never waits on restart bookkeeping.
                systemd_watchdog.notify("WATCHDOG=1")
            self._scan()

        self._await_restarts()
        logger.info("Supervisor stopped")


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


    def _trigger_restart(self, ts: _TargetState, reason: str) -> None:
        now = time.monotonic()
        with self._lock:
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
        try:
            # Log in-progress work the dead worker held before it's gone for good.
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
        finally:
            with self._lock:
                ts.restarting = False

    def _await_restarts(self) -> None:
        with self._lock:
            threads = list(self._restart_threads)
        for t in threads:
            t.join(timeout=self._tick_interval_s)


    def status(self) -> dict[str, dict[str, object]]:
        with self._lock:
            return {
                name: {"state": ts.state, "restarts": len(ts.restarts)}
                for name, ts in self._states.items()
            }

    def is_degraded(self) -> bool:
        with self._lock:
            return any(ts.degraded for ts in self._states.values())
