"""Tests for edge_voice.pipeline.supervisor.

These drive the Supervisor with fake targets rather than real worker threads
(Milestone 8's "kill a fake thread, assert it restarts" approach): the
supervisor is generic over SupervisedTarget callables, so a controllable fake
exercises crash/stall/degrade paths deterministically and fast.
"""

import threading
import time
from typing import Callable

import pytest

from edge_voice.pipeline import systemd_watchdog
from edge_voice.pipeline.supervisor import (
    STATE_DEGRADED,
    STATE_RUNNING,
    Supervisor,
    SupervisedTarget,
)


class FakeWorker:
    """A stand-in worker whose liveness/activity the test drives directly."""

    def __init__(self) -> None:
        self.alive = True
        self._stopping = False
        self.activity = time.monotonic()
        self.input_has_work = False
        self.restart_calls = 0
        self.revive_on_restart = True

    def target(
        self,
        name: str = "Fake",
        stall_detection: bool = True,
        pending_loss: Callable[[], str | None] | None = None,
    ) -> SupervisedTarget:
        return SupervisedTarget(
            name=name,
            is_alive=lambda: self.alive,
            is_stopping=lambda: self._stopping,
            last_activity=lambda: self.activity,
            restart=self._restart,
            input_pending=lambda: self.input_has_work,
            stall_detection=stall_detection,
            pending_loss=pending_loss or (lambda: None),
        )

    def _restart(self) -> None:
        self.restart_calls += 1
        if self.revive_on_restart:
            self.alive = True
            self.activity = time.monotonic()


def _wait_until(predicate, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def running_supervisor():
    started: list[Supervisor] = []

    def _start(targets, **kwargs):
        kwargs.setdefault("tick_interval_s", 0.05)
        kwargs.setdefault("watchdog_enabled", False)
        sup = Supervisor(targets, **kwargs)
        sup.start()
        started.append(sup)
        return sup

    yield _start

    for sup in started:
        sup.stop()
        sup.join(timeout=3)


# ── crash detection ──────────────────────────────────────────


def test_healthy_worker_is_not_restarted(running_supervisor):
    fake = FakeWorker()
    running_supervisor([fake.target()])
    time.sleep(0.2)  # several ticks
    assert fake.restart_calls == 0


def test_crashed_worker_is_restarted(running_supervisor):
    fake = FakeWorker()
    running_supervisor([fake.target()])
    fake.alive = False  # simulate a crash (thread exited, not asked to stop)
    assert _wait_until(lambda: fake.restart_calls >= 1)
    assert fake.alive is True  # revived by restart


def test_intentional_stop_is_not_a_crash(running_supervisor):
    fake = FakeWorker()
    fake._stopping = True  # we asked it to stop
    fake.alive = False  # ...and it exited
    running_supervisor([fake.target()])
    time.sleep(0.2)
    assert fake.restart_calls == 0


# ── stall detection ──────────────────────────────────────────


def test_stalled_worker_with_queued_input_is_restarted(running_supervisor):
    fake = FakeWorker()
    fake.input_has_work = True
    fake.activity = time.monotonic() - 100  # no progress for ages
    running_supervisor([fake.target()], stall_timeout_s=0.1)
    assert _wait_until(lambda: fake.restart_calls >= 1)


def test_idle_worker_with_empty_input_is_not_stalled(running_supervisor):
    fake = FakeWorker()
    fake.input_has_work = False  # nothing waiting -> idle, not stalled
    fake.activity = time.monotonic() - 100
    running_supervisor([fake.target()], stall_timeout_s=0.1)
    time.sleep(0.3)
    assert fake.restart_calls == 0


def test_stall_detection_can_be_disabled_per_target(running_supervisor):
    fake = FakeWorker()
    fake.input_has_work = True
    fake.activity = time.monotonic() - 100
    running_supervisor([fake.target(stall_detection=False)], stall_timeout_s=0.1)
    time.sleep(0.3)
    assert fake.restart_calls == 0


# ── degraded backoff ─────────────────────────────────────────


def test_worker_degrades_after_exceeding_restart_budget(running_supervisor):
    fake = FakeWorker()
    fake.alive = False  # crashed...
    fake.revive_on_restart = False  # ...and every restart leaves it crashed
    sup = running_supervisor([fake.target()], max_restarts=3, restart_window_s=60.0)
    assert _wait_until(sup.is_degraded)
    # Exactly the budget's worth of restarts, then it gives up in-process.
    # (restart_calls is bumped on the restart thread, so let it settle.)
    assert _wait_until(lambda: fake.restart_calls == 3)
    time.sleep(0.2)
    assert fake.restart_calls == 3  # no further restarts once degraded
    assert sup.status()["Fake"]["state"] == STATE_DEGRADED


def test_status_reports_running_for_healthy_worker(running_supervisor):
    fake = FakeWorker()
    sup = running_supervisor([fake.target()])
    time.sleep(0.15)
    assert sup.status()["Fake"]["state"] == STATE_RUNNING
    assert sup.is_degraded() is False


# ── pending-loss logging ─────────────────────────────────────


def test_pending_loss_is_logged_on_restart(running_supervisor, caplog):
    fake = FakeWorker()
    target = fake.target(pending_loss=lambda: "rx=1.50s")
    running_supervisor([target])
    fake.alive = False
    assert _wait_until(lambda: fake.restart_calls >= 1)
    assert _wait_until(lambda: "lost in-progress audio" in caplog.text)
    assert "rx=1.50s" in caplog.text


# ── watchdog pings ───────────────────────────────────────────


def test_watchdog_pings_when_enabled(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(systemd_watchdog, "notify", lambda state: calls.append(state) or True)

    fake = FakeWorker()
    sup = Supervisor([fake.target()], tick_interval_s=0.05, watchdog_enabled=True)
    sup.start()
    try:
        assert _wait_until(lambda: "READY=1" in calls)
        assert _wait_until(lambda: "WATCHDOG=1" in calls)
    finally:
        sup.stop()
        sup.join(timeout=3)


def test_no_watchdog_pings_when_disabled(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(systemd_watchdog, "notify", lambda state: calls.append(state) or True)

    fake = FakeWorker()
    sup = Supervisor([fake.target()], tick_interval_s=0.05, watchdog_enabled=False)
    sup.start()
    try:
        time.sleep(0.2)
    finally:
        sup.stop()
        sup.join(timeout=3)
    assert calls == []


# ── clean shutdown ───────────────────────────────────────────


def test_supervisor_stops_cleanly():
    fake = FakeWorker()
    sup = Supervisor([fake.target()], tick_interval_s=0.05, watchdog_enabled=False)
    sup.start()
    time.sleep(0.1)
    sup.stop()
    sup.join(timeout=3)
    assert not sup.is_alive()
    assert isinstance(threading.current_thread(), threading.Thread)  # sanity
