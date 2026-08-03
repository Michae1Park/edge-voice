# Health Monitoring (edge-voice)

How to read the kiosk display, and what `GET /api/status` actually reports.
Complements `ARCHITECTURE.md` §5 (Reliability) and §7 (Observability), which
cover the two subsystems this reads *from*.

## Why health is its own thing

`Supervisor` (reliability) and `MetricsCollector` (observability) are both
**tick-driven pushers** — background threads that wake on a timer and either
act (restart a worker) or record (log a snapshot), whether or not anyone is
asking. Health is **query-driven**: assembled synchronously when a request
arrives, with no thread and no cadence of its own. It's a facade over both,
plus the router's per-channel freshness.

`health/reporting.py` is a pure function over plain values — no orchestrator
reference, no worker instances, no threads. That keeps it trivially testable
and keeps `Supervisor` free to stay generic (its docstring commits to knowing
"nothing about VAD, STT, or MQTT").

---

## Reading the kiosk

Three rows, each answering a different question:

| Row | What it shows | Health signal |
|---|---|---|
| **Header** (top right) | `● LIVE` pill | **Overall verdict** — green `live` / amber `degraded` or `mqtt down` / grey `stopped` |
| **Strip 1** | `mqtt ok · q ingest:0 routed:0 segment:0 · rx 7.4s tx 7.4s · metrics 3s ago` | MQTT connectivity, backpressure, channel freshness, snapshot age |
| **Strip 2** | `● ingest · ● router 6µs · ● vad 225µs · ● stt 190ms` | **Per-module health** — the dot before each name |

### Per-module status (strip 2)

The dot before each module name *is* the health status. Easy to miss when
everything is running, since all four are then the same green.

| Dot | Worker state | Meaning |
|---|---|---|
| Green | `running` | Healthy |
| Amber | `restarting` | Supervisor is replacing it right now |
| Amber | `degraded` | Blew its restart budget — supervisor gave up in-process; the OS watchdog's full-process restart is the remaining recovery path |
| Grey | `stopped` | Not running |

**The dot and the latency come from different sources**, deliberately: the dot
reads `workers` (sampled live on every request), the latency reads the metrics
snapshot (needs a tick to land). So for the first ~10s after startup you see
four green dots with *blank* latencies — module health is known before any
timing data exists.

### What rx and tx are

The two **legs of a phone call**, each arriving as its own MQTT stream:

```yaml
channels:
  - topic: "stt/audio_chunks_rx"    # rx = received  — the remote party / caller
    channel_id: "rx"
  - topic: "stt/audio_chunks_tx"    # tx = transmitted — the local party
    channel_id: "tx"
```

Standard telephony naming. They stay separate the whole way through the
pipeline (each gets its own Silero VAD instance, see `ARCHITECTURE.md` §3) so
transcripts stay attributed to whoever actually spoke.

The number beside each is **freshness** — seconds since the last audio packet
arrived on that channel. It counts up while a channel is silent and turns
amber past `health.stale_segment_warning_s` (default 30s).

### Reading queue depths

Queue depth measures work **waiting**, not work done — it's a backpressure
indicator, so `0 0 0` is the healthy reading, not a broken one. Measured
per-stage cost against the ~50 packets/sec/channel arrival rate:

| Stage | Time per packet | Utilization at 2 channels |
|---|---|---|
| Router | ~5µs | ~0.05% |
| VAD | ~225µs | ~2% |

Each worker finishes a packet thousands of times faster than the next arrives,
so nothing piles up. Depths only move on **sustained** overload (STT falling
behind) or a **wedged worker** — the latter being exactly what `Supervisor`'s
stall detection watches for via `input_pending()`.

Two sampling caveats: the UI polls every 3s while these queues drain in under
a second, so a real transient spike is likely invisible; and depths are capped
by `queues.*` in config (`ingest: 256`, `routed: 128`, `segment: 64`), so a
reading *at* the cap means the producer is blocked, not merely busy.

### Reading latency

Units scale per value, because the stages differ by five orders of magnitude:

| Module | Typical | What's being timed |
|---|---|---|
| `router` | µs | One `Repacketizer.process()` call |
| `vad` | hundreds of µs | One Silero forward pass |
| `stt` | tens–hundreds of ms | One Moonshine inference call |

`ingest` has no latency by design — it's a source with nothing to time.

Each module shows its **slowest channel**: one number per module is all that
fits, and the max is the one that would breach a latency budget first.

---

## The `/api/status` payload

A strict **superset** of the old `{running, degraded, workers}` shape, so
anything already reading those three fields keeps working.

```jsonc
{
  // verbatim from orchestrator.get_status() — always present
  "running": true,
  "degraded": false,
  "workers": {"MqttAudioIngest": "running", "ChannelRouter": "running",
              "VADWorker": "running", "STTWorker": "running"},

  "status": "ok",                    // "ok" | "warn" | "down"

  // sampled live, at request time
  "queue_depths": {"ingest": 0, "routed": 0, "segment": 0},
  "channels": {"rx": {"freshness_s": 0.42, "stale": false},
               "tx": {"freshness_s": null, "stale": false}},
  "stale_after_s": 30.0,

  // provenance for everything below it
  "metrics": {"state": "ok", "age_s": 4.31},
  "mqtt_connected": true,
  "restarts": {"max": 3, "window_s": 60.0, "counts": {"VADWorker": 0}},
  "latencies": {"router": {"rx": 5.6e-06}, "vad_silero": {"rx": 0.000225},
                "vad_rms_gate": {}, "stt": {"rx": 0.078}}
}
```

### Two clocks, each labelled

| Group | Freshness |
|---|---|
| `queue_depths`, `channels`, `workers`, `running`, `degraded` | Sampled at request time |
| `mqtt_connected`, `restarts`, `latencies` | From the last metrics tick — `metrics.age_s` says how old |

Nothing mixes the two silently. That's why `metrics.age_s` exists rather than
being dropped as an internal detail: `MetricsCollector` is **not** supervised
(it's absent from `_build_supervisor_targets()`), so if that thread ever dies,
an ever-growing `age_s` is the only thing that would reveal it.

### When fields are null

| Field | Null when |
|---|---|
| `running` / `degraded` / `workers` / `status` / `stale_after_s` | Never |
| `queue_depths` | Never — read live, so it survives `metrics.enabled: false` |
| `channels` | Never — falls back to configured channel ids before `build()`, so UI rows don't appear/disappear |
| `channels[*].freshness_s` | Channel never seen — including before start, **and right after a `ChannelRouter` restart** (its last-seen table is per-instance) |
| `metrics.age_s` | `metrics.state != "ok"` |
| `mqtt_connected` | metrics not ok, **or** the audio source isn't MQTT-based (`WavSource`/`MicSource` in dev) |
| `restarts` | metrics not ok, **or** `reliability.enabled: false` (no supervisor exists) |
| `latencies` | metrics not ok |

`metrics.state` disambiguates the two reasons a snapshot-derived field can be
null:

| State | Meaning |
|---|---|
| `disabled` | `metrics.enabled: false` — no collector at all |
| `pending` | Enabled, no tick yet — up to `emit_interval_s` after start, and the whole of a built-but-not-started pipeline |
| `ok` | A snapshot is available; `age_s` says how old |

### The `status` verdict

```
down  ← not running
warn  ← running AND (degraded OR mqtt_connected is False)
ok    ← otherwise
```

- `mqtt_connected is None` (unknown / non-MQTT source) does **not** warn —
  unknown is not broken.
- `metrics.state` never affects the verdict — a kiosk that goes amber for the
  first ten seconds of every boot teaches operators to ignore amber.
- **Channel staleness never affects the verdict.** On a phone-call box every
  channel is legitimately stale between calls, so folding it in would leave
  the kiosk amber most of the day. Staleness surfaces per-channel only. This
  is pinned by a test (`test_verdict_ok_when_a_channel_is_stale_...`).
- `degraded` keeps its exact prior meaning (supervisor-flagged). The verdict
  uses a disjoint word (`warn`) so `status: "warn"` with `degraded: false`
  (MQTT down) doesn't read as a contradiction.

### Two fields deliberately *not* shipped

Both would put two similar-looking numbers of different ages in one payload:

- **`restart_status`'s per-worker `state`** — live worker state already ships
  in `workers`; the snapshot's copy is up to one tick older. Only the
  restart *counts* survive, as `restarts.counts`.
- **The scalar `stt_last_latency_s`** — it's the most recent call across *all*
  channels, while `latencies.stt` is per-channel, so the two legitimately
  disagree.

`MetricsSnapshot.taken_at` is also never serialized: it's a `time.monotonic()`
value, meaningless outside the process. It becomes `metrics.age_s` instead.

---

## Config

```yaml
health:
  stale_segment_warning_s: 30   # per-channel staleness threshold, in seconds
```

**Known misnomer:** this measures inbound *packet* freshness (router wall
clock), not segment finalization — the name predates that decision. Renaming
config was out of scope; see `docs/BUILDPLAN.md` Milestone 7 for the choice of
`ChannelRouter.get_freshness()` over `VADWorker`'s monotonic `last_packet_at`
(two clocks for different jobs; surfacing both would give two similar-looking
numbers that can legitimately disagree).

Staleness is also a weak alarm on this workload generally — a real one would
need call-state awareness, which nothing in the pipeline tracks today.

## Where the code lives

| Concern | File |
|---|---|
| Payload assembly | `src/edge_voice/health/reporting.py` |
| Orchestrator accessors | `PipelineOrchestrator.health()` / `.metrics_snapshot()` / `.channel_freshness()` |
| Endpoint | `src/edge_voice/webui/app.py` — `GET /api/status` |
| Kiosk rendering | `src/edge_voice/webui/templates/console.html` — `renderStrip()` / `renderModules()` |
| Tests | `tests/test_health_reporting.py` (unit), `tests/test_webui_app.py` (endpoint) |
