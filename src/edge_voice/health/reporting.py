"""Assembles the health object served by webui's GET /api/status.

Milestone 7, items 3-4 (docs/BUILDPLAN.md). This is the consumer the rest of
Milestone 7 was built for: `observability/metrics.py` aggregates on a tick and
`pipeline/supervisor.py` restarts on a tick, but until now nothing read either
except the log sink.

Why this is its own package, not part of observability/ or the supervisor
─────────────────────────────────────────────────────────────────────────
Those two are tick-driven *pushers*: background threads that wake on a timer
and act (restart a worker) or record (log a snapshot), whether or not anyone
is asking. Health is query-driven: assembled synchronously when a request
arrives, with no thread and no cadence of its own. It is a facade over both,
plus the router's per-channel freshness -- and `Supervisor` in particular is
deliberately generic ("knows nothing about VAD, STT, or MQTT", see its
docstring), so per-channel knowledge could not live there anyway.

Plain values, not callables
───────────────────────────
`MetricsCollector`/`SupervisedTarget` take injected callables because they
re-read their inputs on every tick, indefinitely. This takes plain values
instead: it reads each input exactly once per request, so a callable would
just be a value with extra ceremony. The deviation is deliberate, and it
keeps this module trivially testable -- no orchestrator, no threads, no
Settings, just dicts and a MetricsSnapshot.

Two clocks, each labelled
─────────────────────────
`queue_depths` and `channels` are sampled live, at request time.
`mqtt_connected` and `restarts` come from the last metrics tick and are
therefore up to `metrics.emit_interval_s` old -- `metrics.age_s` says exactly
how old. Nothing in the payload mixes the two silently.
"""

from __future__ import annotations

import time

from edge_voice.observability.metrics import MetricsSnapshot

STATUS_OK = "ok"
STATUS_WARN = "warn"
STATUS_DOWN = "down"

METRICS_OK = "ok"
METRICS_PENDING = "pending"
METRICS_DISABLED = "disabled"


def build_health_report(
    status: dict,
    queue_depths: dict[str, int],
    snapshot: MetricsSnapshot | None,
    metrics_enabled: bool,
    freshness: dict[str, float | None],
    stale_after_s: float,
) -> dict:
    """Combine live pipeline state and the last metrics tick into one object.

    `status` is `PipelineOrchestrator.get_status()` and is passed through
    verbatim -- running/degraded/workers keep their exact existing meaning
    and are never re-derived here (docs/BUILDPLAN.md is explicit about that).
    The result is a strict superset of it, so anything already reading those
    three fields keeps working.
    """
    report = dict(status)

    # Sampled live, at request time.
    report["queue_depths"] = queue_depths
    report["channels"] = _channels(freshness, stale_after_s)
    report["stale_after_s"] = stale_after_s

    # From the last metrics tick, with its own age attached.
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
    """Return (metrics provenance, mqtt_connected, restarts, latencies).

    Every snapshot-derived field is produced here, in one place, so they
    cannot drift apart on when they're available: either there's a usable
    snapshot and all of them are populated, or there isn't and all of them
    are None together.

    The provenance block is what lets a caller tell the two reasons a field
    can be None apart: "metrics isn't running" (state disabled/pending)
    versus "metrics is fine, that signal just isn't available" -- a non-MQTT
    audio source for mqtt_connected, no Supervisor for restarts.
    """
    if not metrics_enabled:
        return {"state": METRICS_DISABLED, "age_s": None}, None, None, None
    if snapshot is None:
        # Enabled but no tick has landed yet: up to metrics.emit_interval_s
        # after start, and the whole of a built-but-not-started pipeline.
        return {"state": METRICS_PENDING, "age_s": None}, None, None, None

    # taken_at is time.monotonic(), so it is only comparable in-process --
    # never serialize it; report the age it implies instead.
    age_s = time.monotonic() - snapshot.taken_at
    return (
        {"state": METRICS_OK, "age_s": age_s},
        snapshot.mqtt_connected,
        _restarts(snapshot),
        _latencies(snapshot),
    )


def _restarts(snapshot: MetricsSnapshot) -> dict | None:
    """Restart budget as {max, window_s, counts}, or None without a Supervisor.

    Deliberately drops the per-worker `state` string that `Supervisor.status()`
    also carries: live worker state already ships in `workers` (supervisor-
    refined), and the snapshot's copy is up to one tick older. Two
    identically-meaning worker-state fields of different ages in one payload
    is exactly the kind of silent disagreement docs/BUILDPLAN.md rejected
    VAD's clock alongside the router's to avoid.
    """
    if snapshot.restart_status is None:
        return None
    return {
        "max": snapshot.max_restarts,
        "window_s": snapshot.restart_window_s,
        "counts": {name: info.get("restarts") for name, info in snapshot.restart_status.items()},
    }


def _latencies(snapshot: MetricsSnapshot) -> dict:
    """Per-stage, per-channel latency, keyed by pipeline stage.

    Renamed from the snapshot's worker-oriented field names to stage names,
    so the UI can line each one up against the matching entry in `workers`
    without a second mapping table.

    Deliberately omits the snapshot's scalar `stt_last_latency_s`: it is the
    most recent call across *all* channels, whereas `stt` below is the most
    recent per channel, so the two legitimately disagree. Shipping both would
    put two similar-looking STT latency numbers in one payload with no way to
    explain the difference -- the same trap avoided with restart state.

    Note `vad_rms_gate` measures the gate-skip path, not inference: it is
    microseconds where `vad_silero` is milliseconds, and is empty whenever no
    channel has recently taken that path. Never compare the two as if they
    measured the same work.
    """
    return {
        "router": snapshot.router_repacketize_latencies_s,
        "vad_silero": snapshot.vad_silero_latencies_s,
        "vad_rms_gate": snapshot.vad_rms_gate_latencies_s,
        "stt": snapshot.stt_channel_latencies_s,
    }


def _channels(freshness: dict[str, float | None], stale_after_s: float) -> dict:
    """Per-channel freshness, flagged against the staleness threshold.

    A channel that has never been seen (None) is not stale -- "no packets
    yet" is a startup state, not a fault, and it is also what every channel
    reads immediately after a ChannelRouter restart, since the last-seen
    table lives on the router instance.
    """
    return {
        channel_id: {
            "freshness_s": seconds,
            "stale": seconds is not None and seconds > stale_after_s,
        }
        for channel_id, seconds in freshness.items()
    }


def _verdict(running: bool, degraded: object, mqtt_connected: bool | None) -> str:
    """Single top-level verdict for the kiosk pill.

    Channel staleness deliberately does NOT feed into this. On a phone-call
    box every channel is legitimately stale between calls (the threshold
    defaults to 30s), so folding it in would leave the kiosk amber most of
    the day and train operators to ignore amber -- destroying the signal for
    the things that do mean something. Staleness surfaces per-channel instead.

    `mqtt_connected is None` (unknown: non-MQTT source, or metrics not ready)
    is not a warning either -- unknown is not broken. And metrics being
    pending/disabled never warns: a kiosk that goes amber for the first ten
    seconds of every boot teaches the same bad lesson.
    """
    if not running:
        return STATUS_DOWN
    if degraded or mqtt_connected is False:
        return STATUS_WARN
    return STATUS_OK
