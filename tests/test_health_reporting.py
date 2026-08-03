"""Tests for edge_voice.health.reporting.

build_health_report is a pure function over plain values (see its module
docstring for why it takes values rather than the callables MetricsCollector/
SupervisedTarget use), so these need no orchestrator, no threads, and no
Settings -- just dicts and a hand-built MetricsSnapshot.
"""

import time
from typing import Any

import pytest

from edge_voice.health.reporting import build_health_report
from edge_voice.observability.metrics import MetricsSnapshot


def _snapshot(**overrides: Any) -> MetricsSnapshot:
    """A MetricsSnapshot with every required field defaulted."""
    fields: dict[str, Any] = {
        "queue_depths": {"ingest": 0},
        "stt_last_latency_s": 0.5,
        "mqtt_connected": True,
        "restart_status": {"VADWorker": {"state": "running", "restarts": 0}},
        "max_restarts": 3,
        "restart_window_s": 60.0,
        "vad_silero_latencies_s": {},
        "vad_rms_gate_latencies_s": {},
        "router_repacketize_latencies_s": {},
        "stt_channel_latencies_s": {},
    }
    fields.update(overrides)
    return MetricsSnapshot(**fields)


def _report(**overrides: Any) -> dict:
    """build_health_report with healthy defaults, overridable per test."""
    kwargs: dict[str, Any] = {
        "status": {"running": True, "degraded": False, "workers": {"VADWorker": "running"}},
        "queue_depths": {"ingest": 0, "routed": 0, "segment": 0},
        "snapshot": _snapshot(),
        "metrics_enabled": True,
        "freshness": {"rx": 0.4, "tx": 0.6},
        "stale_after_s": 30.0,
    }
    kwargs.update(overrides)
    return build_health_report(**kwargs)


# ── Superset guarantee ──────────────────────────────────────────


def test_report_passes_status_through_verbatim():
    status = {
        "running": True,
        "degraded": True,
        "workers": {"STTWorker": "degraded"},
        "some_future_field": 42,  # proves pass-through isn't a hand-copied allowlist
    }
    report = _report(status=status)
    for key, value in status.items():
        assert report[key] == value


# ── metrics provenance tri-state ────────────────────────────────


def test_metrics_disabled_nulls_snapshot_derived_fields():
    report = _report(metrics_enabled=False, snapshot=None)
    assert report["metrics"] == {"state": "disabled", "age_s": None}
    assert report["mqtt_connected"] is None
    assert report["restarts"] is None


def test_metrics_pending_before_first_tick():
    report = _report(metrics_enabled=True, snapshot=None)
    assert report["metrics"] == {"state": "pending", "age_s": None}
    assert report["mqtt_connected"] is None
    assert report["restarts"] is None


def test_metrics_ok_surfaces_snapshot_fields():
    report = _report(snapshot=_snapshot(mqtt_connected=False))
    assert report["metrics"]["state"] == "ok"
    assert report["mqtt_connected"] is False


def test_metrics_age_reflects_snapshot_taken_at():
    report = _report(snapshot=_snapshot(taken_at=time.monotonic() - 5.0))
    assert report["metrics"]["age_s"] == pytest.approx(5.0, abs=0.5)


def test_taken_at_is_never_serialized():
    # It's a time.monotonic() value -- meaningless outside this process.
    assert "taken_at" not in _report()


# ── queue depths are live, independent of metrics ───────────────


def test_queue_depths_present_even_when_metrics_disabled():
    # The whole point of sourcing these live rather than from the snapshot.
    report = _report(metrics_enabled=False, snapshot=None, queue_depths={"ingest": 7})
    assert report["queue_depths"] == {"ingest": 7}
    assert report["metrics"]["state"] == "disabled"


# ── restarts ────────────────────────────────────────────────────


def test_restarts_none_without_supervisor_while_metrics_ok():
    # Two different reasons restarts can be None; metrics.state tells them apart.
    report = _report(snapshot=_snapshot(restart_status=None, max_restarts=None))
    assert report["restarts"] is None
    assert report["metrics"]["state"] == "ok"


def test_restarts_collapse_to_counts_without_duplicating_worker_state():
    snapshot = _snapshot(
        restart_status={"VADWorker": {"state": "degraded", "restarts": 3}},
        max_restarts=3,
        restart_window_s=60.0,
    )
    report = _report(snapshot=snapshot)
    assert report["restarts"] == {"max": 3, "window_s": 60.0, "counts": {"VADWorker": 3}}
    # `workers` is the one place worker state lives; restarts must not shadow it.
    assert "degraded" not in str(report["restarts"])


# ── per-stage latency ───────────────────────────────────────────


def test_latencies_keyed_by_pipeline_stage():
    snapshot = _snapshot(
        router_repacketize_latencies_s={"rx": 5.6e-06},
        vad_silero_latencies_s={"rx": 0.000225},
        vad_rms_gate_latencies_s={"rx": 3.0e-07},
        stt_channel_latencies_s={"rx": 0.078},
    )
    assert _report(snapshot=snapshot)["latencies"] == {
        "router": {"rx": 5.6e-06},
        "vad_silero": {"rx": 0.000225},
        "vad_rms_gate": {"rx": 3.0e-07},
        "stt": {"rx": 0.078},
    }


def test_latencies_null_when_metrics_not_ok():
    assert _report(snapshot=None)["latencies"] is None
    assert _report(metrics_enabled=False, snapshot=None)["latencies"] is None


def test_scalar_stt_latency_is_not_shipped_alongside_per_channel():
    # It's the most recent call across all channels while latencies["stt"] is
    # per channel -- the two legitimately disagree, so only one ships.
    report = _report(snapshot=_snapshot(stt_last_latency_s=0.9))
    assert "stt_last_latency_s" not in report
    assert 0.9 not in report["latencies"]["stt"].values()


# ── per-channel freshness ───────────────────────────────────────


def test_never_seen_channel_is_not_stale():
    report = _report(freshness={"rx": None})
    assert report["channels"]["rx"] == {"freshness_s": None, "stale": False}


def test_channel_stale_past_threshold():
    report = _report(freshness={"rx": 31.0}, stale_after_s=30.0)
    assert report["channels"]["rx"] == {"freshness_s": 31.0, "stale": True}


def test_channel_fresh_under_threshold():
    report = _report(freshness={"rx": 29.0}, stale_after_s=30.0)
    assert report["channels"]["rx"]["stale"] is False


def test_stale_after_s_is_echoed_for_the_ui():
    assert _report(stale_after_s=45.0)["stale_after_s"] == 45.0


# ── verdict table ───────────────────────────────────────────────


def test_verdict_down_when_not_running():
    report = _report(status={"running": False, "degraded": False, "workers": {}})
    assert report["status"] == "down"


def test_verdict_warn_when_degraded():
    report = _report(status={"running": True, "degraded": True, "workers": {}})
    assert report["status"] == "warn"


def test_verdict_warn_when_mqtt_down():
    report = _report(snapshot=_snapshot(mqtt_connected=False))
    assert report["status"] == "warn"
    assert report["degraded"] is False  # warn without degraded is a valid combination


def test_verdict_ok_when_mqtt_unknown():
    # Non-MQTT audio source (WavSource/MicSource): unknown is not broken.
    report = _report(snapshot=_snapshot(mqtt_connected=None))
    assert report["status"] == "ok"


def test_verdict_ok_while_metrics_pending():
    # A kiosk that goes amber for the first 10s of every boot trains people
    # to ignore amber.
    assert _report(snapshot=None)["status"] == "ok"


def test_verdict_ok_when_a_channel_is_stale_and_nothing_else_is_wrong():
    # Executable record of a deliberate decision: on a phone-call box every
    # channel is legitimately stale between calls, so staleness stays a
    # per-channel signal and never reaches the top-level verdict.
    report = _report(freshness={"rx": 999.0}, stale_after_s=30.0)
    assert report["channels"]["rx"]["stale"] is True
    assert report["status"] == "ok"


def test_verdict_ok_when_all_clear():
    assert _report()["status"] == "ok"
