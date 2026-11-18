from __future__ import annotations

import time

from edge_voice.observability.metrics import MetricsSnapshot

STATUS_OK = "ok"
STATUS_WARN = "warn"
STATUS_DOWN = "down"

METRICS_OK = "ok"
METRICS_PENDING = "pending"
METRICS_DISABLED = "disabled"


# queue_depths and channels are sampled live; mqtt_connected and restarts come from the last
# metrics tick, whose age is reported in metrics.age_s.
def build_health_report(
    status: dict,
    queue_depths: dict[str, int],
    snapshot: MetricsSnapshot | None,
    metrics_enabled: bool,
    freshness: dict[str, float | None],
    stale_after_s: float,
) -> dict:
    report = dict(status)

    report["queue_depths"] = queue_depths
    report["channels"] = _channels(freshness, stale_after_s)
    report["stale_after_s"] = stale_after_s

    metrics, mqtt_connected, restarts, latencies = _metrics_block(snapshot, metrics_enabled)
    report["metrics"] = metrics
    report["mqtt_connected"] = mqtt_connected
    report["restarts"] = restarts
    report["latencies"] = latencies

    report["status"] = _verdict(bool(status.get("running")), status.get("degraded"), mqtt_connected)
    return report


def _metrics_block(
    snapshot: MetricsSnapshot | None, metrics_enabled: bool
) -> tuple[dict, bool | None, dict | None, dict | None]:
    if not metrics_enabled:
        return {"state": METRICS_DISABLED, "age_s": None}, None, None, None
    if snapshot is None:
        return {"state": METRICS_PENDING, "age_s": None}, None, None, None

    age_s = time.monotonic() - snapshot.taken_at
    return (
        {"state": METRICS_OK, "age_s": age_s},
        snapshot.mqtt_connected,
        _restarts(snapshot),
        _latencies(snapshot),
    )


# Drops per-worker state: the live, supervisor-refined copy already ships in `workers`.
def _restarts(snapshot: MetricsSnapshot) -> dict | None:
    if snapshot.restart_status is None:
        return None
    return {
        "max": snapshot.max_restarts,
        "window_s": snapshot.restart_window_s,
        "counts": {name: info.get("restarts") for name, info in snapshot.restart_status.items()},
    }


# Omits the cross-channel stt_last_latency_s, which would disagree with the per-channel values.
# vad_rms_gate times the gate-skip path (microseconds), not inference.
def _latencies(snapshot: MetricsSnapshot) -> dict:
    return {
        "router": snapshot.router_repacketize_latencies_s,
        "vad_silero": snapshot.vad_silero_latencies_s,
        "vad_rms_gate": snapshot.vad_rms_gate_latencies_s,
        "stt": snapshot.stt_channel_latencies_s,
    }


# Never-seen (None) is a startup state, not stale; it's also what every channel reads after a router restart.
def _channels(freshness: dict[str, float | None], stale_after_s: float) -> dict:
    return {
        channel_id: {
            "freshness_s": seconds,
            "stale": seconds is not None and seconds > stale_after_s,
        }
        for channel_id, seconds in freshness.items()
    }


# Channel staleness and unknown MQTT state deliberately don't affect the verdict: channels are
# legitimately idle between conversations, and an always-amber kiosk teaches operators to ignore it.
def _verdict(running: bool, degraded: object, mqtt_connected: bool | None) -> str:
    if not running:
        return STATUS_DOWN
    if degraded or mqtt_connected is False:
        return STATUS_WARN
    return STATUS_OK
