"""Tests for edge_voice.observability.metrics.

Drives MetricsCollector with fake callables rather than a real orchestrator/
workers, same approach test_supervisor.py takes for Supervisor: the
collector is generic over its input callables, so a controllable fake
exercises each aggregated field deterministically and fast.
"""

import time

import pytest

from edge_voice.observability.metrics import MetricsCollector


class _FakeSupervisor:
    def __init__(self, status=None, max_restarts=3, restart_window_s=60.0):
        self._status = status or {"VADWorker": {"state": "running", "restarts": 0}}
        self.max_restarts = max_restarts
        self.restart_window_s = restart_window_s

    def status(self):
        return self._status


def _wait_until(predicate, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def running_collector():
    started: list[MetricsCollector] = []

    def _start(**kwargs):
        kwargs.setdefault("queue_depths", lambda: {"ingest": 0})
        kwargs.setdefault("stt_latency_s", lambda: None)
        kwargs.setdefault("vad_silero_latencies_s", lambda: {})
        kwargs.setdefault("vad_rms_gate_latencies_s", lambda: {})
        kwargs.setdefault("router_repacketize_latencies_s", lambda: {})
        kwargs.setdefault("stt_channel_latencies_s", lambda: {})
        kwargs.setdefault("emit_interval_s", 0.05)
        collector = MetricsCollector(**kwargs)
        collector.start()
        started.append(collector)
        return collector

    yield _start

    for collector in started:
        collector.stop()
        collector.join(timeout=3)


def test_no_snapshot_before_first_tick():
    collector = MetricsCollector(
        queue_depths=lambda: {},
        stt_latency_s=lambda: None,
        vad_silero_latencies_s=lambda: {},
        vad_rms_gate_latencies_s=lambda: {},
        router_repacketize_latencies_s=lambda: {},
        stt_channel_latencies_s=lambda: {},
    )
    assert collector.snapshot() is None


def test_snapshot_carries_queue_depths_and_latency(running_collector):
    collector = running_collector(
        queue_depths=lambda: {"ingest": 3, "segment": 1},
        stt_latency_s=lambda: 0.42,
    )
    assert _wait_until(lambda: collector.snapshot() is not None)
    snapshot = collector.snapshot()
    assert snapshot.queue_depths == {"ingest": 3, "segment": 1}
    assert snapshot.stt_last_latency_s == 0.42


def test_snapshot_reflects_live_queue_depths(running_collector):
    depths = {"ingest": 0}
    collector = running_collector(queue_depths=lambda: dict(depths))
    assert _wait_until(lambda: collector.snapshot() is not None)
    depths["ingest"] = 7
    assert _wait_until(lambda: collector.snapshot().queue_depths["ingest"] == 7)


def test_no_supervisor_means_restart_fields_are_none(running_collector):
    collector = running_collector()  # default supervisor=lambda: None
    assert _wait_until(lambda: collector.snapshot() is not None)
    snapshot = collector.snapshot()
    assert snapshot.restart_status is None
    assert snapshot.max_restarts is None
    assert snapshot.restart_window_s is None


def test_supervisor_present_surfaces_restart_budget(running_collector):
    fake_sup = _FakeSupervisor(
        status={"STTWorker": {"state": "degraded", "restarts": 3}},
        max_restarts=3,
        restart_window_s=60.0,
    )
    collector = running_collector(supervisor=lambda: fake_sup)
    assert _wait_until(lambda: collector.snapshot() is not None)
    snapshot = collector.snapshot()
    assert snapshot.restart_status == {"STTWorker": {"state": "degraded", "restarts": 3}}
    assert snapshot.max_restarts == 3
    assert snapshot.restart_window_s == 60.0


def test_mqtt_connected_defaults_to_none(running_collector):
    collector = running_collector()  # default mqtt_connected=lambda: None
    assert _wait_until(lambda: collector.snapshot() is not None)
    assert collector.snapshot().mqtt_connected is None


def test_mqtt_connected_passes_through_bool(running_collector):
    collector = running_collector(mqtt_connected=lambda: True)
    assert _wait_until(lambda: collector.snapshot() is not None)
    assert collector.snapshot().mqtt_connected is True


def test_per_channel_latencies_default_to_empty(running_collector):
    collector = running_collector()  # all four default to lambda: {}
    assert _wait_until(lambda: collector.snapshot() is not None)
    snapshot = collector.snapshot()
    assert snapshot.vad_silero_latencies_s == {}
    assert snapshot.vad_rms_gate_latencies_s == {}
    assert snapshot.router_repacketize_latencies_s == {}
    assert snapshot.stt_channel_latencies_s == {}


def test_per_channel_latencies_pass_through(running_collector):
    collector = running_collector(
        vad_silero_latencies_s=lambda: {"rx": 0.012, "tx": 0.015},
        vad_rms_gate_latencies_s=lambda: {"rx": 0.0003},
        router_repacketize_latencies_s=lambda: {"rx": 0.0001, "tx": 0.0001},
        stt_channel_latencies_s=lambda: {"tx": 1.8},
    )
    assert _wait_until(lambda: collector.snapshot() is not None)
    snapshot = collector.snapshot()
    assert snapshot.vad_silero_latencies_s == {"rx": 0.012, "tx": 0.015}
    assert snapshot.vad_rms_gate_latencies_s == {"rx": 0.0003}
    assert snapshot.router_repacketize_latencies_s == {"rx": 0.0001, "tx": 0.0001}
    assert snapshot.stt_channel_latencies_s == {"tx": 1.8}


def test_per_channel_latencies_reflect_live_updates(running_collector):
    latencies = {"rx": 0.010}
    collector = running_collector(vad_silero_latencies_s=lambda: dict(latencies))
    assert _wait_until(lambda: collector.snapshot() is not None)
    latencies["rx"] = 0.020
    latencies["tx"] = 0.030
    assert _wait_until(
        lambda: collector.snapshot().vad_silero_latencies_s == {"rx": 0.020, "tx": 0.030}
    )


def test_latency_log_decimals_controls_log_line_rounding_not_snapshot(running_collector, caplog):
    caplog.set_level("INFO", logger="edge_voice.observability.metrics")
    collector = running_collector(
        vad_silero_latencies_s=lambda: {"rx": 0.000225313},
        latency_log_decimals=6,
    )
    assert _wait_until(lambda: collector.snapshot() is not None)

    # snapshot() always keeps full precision regardless of the log setting.
    assert collector.snapshot().vad_silero_latencies_s == {"rx": 0.000225313}
    # The log line respects latency_log_decimals (6 here, not the default 3),
    # so this sub-millisecond value is visible instead of rounding to 0.0.
    assert any("vad_silero={'rx': 0.000225}" in record.getMessage() for record in caplog.records)


def test_stop_halts_the_thread(running_collector):
    collector = running_collector()
    assert _wait_until(lambda: collector.snapshot() is not None)
    collector.stop()
    assert collector.stopping
    collector.join(timeout=3)
    assert not collector.is_alive()
