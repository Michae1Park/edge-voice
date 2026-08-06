# edge-voice Architecture (v0.3)

**Status:** Reflects the implementation through Milestone 7 (Observability +
Health). Everything §5/§6/§7 describes is built and wired; the remaining
planned work is Milestone 8 (test/CI gaps), plus two scoped-but-unstarted
features with their own design docs — `STREAMING_STT_PLAN.md` (blocked
upstream, see §10) and `CALL_LIFECYCLE_PLAN.md`. **§8 Web UI is a deliberate
exception**: it describes the target feature set the UI is meant to grow into,
not only what's built today (see the note at the top of that section for the
split).

## System Overview

```text
MQTT audio channels
        │
        ▼
   MQTT Ingest
        │
        ▼
  Channel Router  (re-packetizes to a fixed frame size)
        │
        ▼
 Per-channel Silero VAD ──── in-progress prefixes ──┐
        │                    (revisable partials)   │
        ▼                                           ▼
 Shared Moonshine STT ─────────────────────▶ Transcript Events
                                                    │
        ├─ Structured logs (JSON console + rotating file)
        ├─ Live transcript stream (SSE)
        └─ Future persistence

  Supervisor watches all four pipeline workers end-to-end (§5); an OS-level
  watchdog watches the whole process underneath that. A MetricsCollector
  thread samples the same workers on its own cadence (§7), and health
  reporting assembles that plus live pipeline state per request (§7).
```



## 1. Problem Statement

`edge-voice` transcribes two-party audio in near real time on resource-constrained edge devices such as Raspberry Pi 5 and Jetson. Phone calls are the reference workload, but nothing in the design is phone-specific — any two-party source where each party's audio arrives as its own stream fits (walkie-talkie/PTT links, radio bridges, two-mic meeting capture). Korean is the default language; Moonshine also supports Arabic, English, Spanish, Japanese, Ukrainian, Vietnamese, and Chinese, selected via configuration.

Each party's audio arrives as a separate MQTT stream ("call leg" below, for the reference workload). The system must:

- Produce ordered, channel-attributed transcripts.
- Operate reliably despite transient failures.
- Provide enough observability to diagnose issues without direct shell access.
- Run efficiently on limited CPU and memory resources.



## 2. Non-Goals

The following remain intentionally out of scope:

- Docker packaging.
- External metrics systems (Prometheus, Grafana, etc.).
- Multi-tenant deployments.
- Speaker diarization beyond channel attribution.
- Transcript persistence beyond logs and live streaming.



## 3. Architecture



### Core Decision: Per-Channel VAD, Shared STT

The system runs one Silero VAD model instance per channel, but a single shared Moonshine STT instance across all channels.

This differs from earlier prototypes that ran fully independent pipelines per audio source, and from an earlier version of this design that used one shared VAD instance for all channels.

**VAD is per-channel, not shared.** A single shared model was tried first and reverted: `VADIterator` only holds the segmentation state machine, the LSTM hidden state used for inference lives inside the model instance itself. Two channels interleaving packets against one shared model corrupted each other's hidden state — measured as doubled, garbage segment counts against recorded call fixtures. Each channel therefore gets its own model instance (one `VADIterator` + one Silero model per `channel_id`), which also removes any need for cross-channel locking since a channel's state is only ever touched by that channel's packets. The extra cost (~4MB, ~0.06s load) per channel is negligible next to correctness.

**STT remains shared.** Unlike VAD, Moonshine's `Transcriber.start()`/`stop()` fully resets its decoder state between segments (verified byte-for-byte against a fresh instance per segment), and the STT worker only ever processes one finalized segment at a time regardless of which channel it came from — there is no concurrent access to isolate. Sharing one instance also fits the turn-taking nature of phone conversations: the previous speaker's context shouldn't bleed into the next line regardless of channel, and it avoids holding a second copy of the model in memory (~175MB saved).

Reasons this split still favors resource efficiency and conversation semantics over one independent pipeline per audio source:

1. **Resource efficiency**
  - A full independent VAD+STT pipeline per channel would duplicate CPU and memory usage well beyond the per-channel VAD model's small footprint.
  - Phone conversations are typically turn-based, making parallel STT instances unnecessary.
2. **Conversation semantics**
  - Separate call legs represent one conversation rather than unrelated audio streams.
  - A shared STT stage preserves conversation ordering and simplifies downstream processing.



### Routing Model

```text
MQTT channel 1 ──┐               ┌─▶ VAD (channel 1) ─┐
                 ├─▶ Router ─────┤                     ├─▶ Shared STT ─▶ Transcript
MQTT channel 2 ──┘               └─▶ VAD (channel 2) ─┘
```

- Incoming audio packets are tagged with `channel_id`.
- Packets are placed onto a shared ingest queue.
- The router re-packetizes each channel's stream to a fixed outgoing frame size (independently configurable from the incoming frame size — e.g. 20ms arriving frames re-chunked to the 32ms window VAD expects) before handing packets on.
- VAD processes packets serially (one worker thread) but against independent per-channel model instances and state — no cross-channel interference, no locking required.
- Finalized speech segments are placed onto a shared STT queue, alongside the optional revisable prefixes described below.
- STT processes one segment at a time, against one shared model instance.
- Channel attribution is preserved throughout the pipeline.

Both models are loaded at worker construction, not lazily on the first packet
or segment: a model load on the real-time path would show up as a multi-second
stall in exactly the moment the pipeline is meant to be responsive, and it
would also register as a stall to the supervisor (§5).



### Partial (Revisable) Transcripts

The STT model in use (Moonshine `tiny` for Korean) is not a streaming arch, so
a transcript would otherwise only appear once VAD finalizes a segment — the
speaker has to stop talking before any text shows up. To close that gap
without a streaming model, VAD emits the **in-progress prefix** of a segment
on a configurable cadence (`vad.partial_interval_s`, skipped for turns shorter
than `vad.partial_min_segment_s`), carrying the same `segment_id` as the final
that will eventually close it.

The properties that keep this from corrupting anything downstream:

- A partial is marked (`SpeechSegment.is_partial` / `TranscriptEvent.is_final=False`)
  and consumers replace in place, keyed on `segment_id`. Exactly one
  `is_final=True` event ever closes a given segment.
- **Partials are droppable, finals are never dropped.** STT skips a partial
  when the segment queue is already at `stt.partial_max_queue_depth`, and VAD
  drops rather than blocks if the queue is full. On a constrained board the
  correct response to a backlog is to stop doing optional work.
- Partials are excluded from STT latency metrics and from the segment-audio
  dump — a partial transcribes a prefix, so mixing the two would make latency
  meaningless and the dumps redundant.

The cost is real and paid per partial: with no streaming model, each one
re-transcribes the whole prefix from scratch. The measured gain/cost tradeoff
behind the shipped defaults is recorded in `configs/default.yaml`. A genuine
streaming arch would remove the re-transcription entirely — see §10.

### Tradeoffs

Overlapping speech is processed sequentially rather than simultaneously.

This favors resource efficiency and implementation simplicity over perfect concurrent transcription. For the turn-taking two-party workloads this targets, that's an acceptable tradeoff.

If overlap-heavy workloads become common, additional STT workers can be introduced without redesigning the routing architecture.

Each finalized segment is fed to Moonshine in **one** `add_audio()` call rather than in windows. Windowed feeding was measured as strictly worse on real audio (~2.7x slower, since the decoder redoes real work per call, and prone to duplicated words at window boundaries).

## 4. Concurrency Model

The pipeline consists of four long-lived worker threads connected by bounded
queues, plus two observer threads (a supervisor and a metrics collector) that
watch those four, plus up to two optional debug dump workers.

### MQTT Ingest

- Subscribes to per-channel MQTT topics; receives audio packets.
- Tags packets with channel metadata.
- Pushes packets onto the ingest queue.
- Performs no expensive processing. Reconnects on its own (§5) — this is invisible to the supervisor, which only acts on a worker thread dying outright, not on a connection blip.



### Channel Router

- Consumes packets from the ingest queue.
- Validates `channel_id`, tracks per-channel last-seen timestamps.
- Re-packetizes to the fixed outgoing frame size VAD expects (see Routing Model above).
- Pushes re-packetized packets onto the routed queue; optionally mirrors raw packets to a debug dump queue.



### VAD Worker

- Consumes packets from the routed queue.
- Maintains per-channel VAD state (own model instance, own segmentation state machine, own preroll buffer, own `segment_id`).
- Mints a `segment_id` when a segment *starts*, not when it finalizes, so every event from VAD trigger onward can be traced by it (§7).
- Produces finalized speech segments onto the segment queue, plus optional in-progress prefixes; optionally mirrors finalized segments (never partials) to a debug dump queue.



### STT Worker

- Consumes finalized segments — and in-progress prefixes — from the segment queue.
- Runs Moonshine inference against the one shared `Transcriber`.
- Emits transcript events, and records per-call inference latency (overall and per channel) for §7.



### Supervisor

- Watches all four workers above (not the optional debug dump workers, and not the metrics collector).
- Restarts a worker that crashes or wedges; see §5 for the detection rules and the OS-level layer underneath it.
- Runs as its own thread with the same start/stop lifecycle as every other worker, so it works identically whether the process is running headless or hosting the web UI.
- Its own liveness is reported alongside the workers it watches — nothing supervises the supervisor, so if its thread dies, restart/degraded state would otherwise silently freeze at its last tick rather than reporting the loss.



### Metrics Collector

- Samples queue depths, STT/VAD/router latency, restart budget, and MQTT connectivity on its own interval (§7).
- Deliberately **not** folded into the supervisor's tick: the supervisor is generic ("a thread died, restart it") and has no business knowing about STT latency or MQTT.
- Starts after the supervisor and stops before it, so it never observes a pipeline mid-teardown.

This producer/consumer architecture prevents transcription latency from blocking audio ingestion.

For how these threads actually start, stop, and get joined (the `stop_event` pattern, shutdown ordering, and why a wedged worker can't be force-killed), see `THREADS.md`.

## 5. Reliability

Reliability is a primary design goal, split into two independent layers because
each catches a failure mode the other structurally cannot: in-process
supervision only works if the process is still scheduling threads at all — it
cannot rescue a deadlocked or OOM-killed process — which is exactly what the
OS-level watchdog underneath it is for.

### Worker Supervision (in-process)

Workers are supervised by a single generic thread-watchdog and restarted after
two distinct kinds of unexpected failure:

- **Crash** — a worker thread exits without having been asked to stop.
- **Stall** — a worker is still alive, has work waiting on its input queue, but
hasn't made progress in longer than a configured threshold (a deadlock, or a
hang inside a native call). Exit-based detection alone would miss this
entirely, since nothing exits; a worker that is simply idle with an empty
queue is never flagged.

Restarting a crashed worker means constructing a fresh instance on the same
queues, since a Python thread cannot be restarted once it has exited. A
genuinely wedged (not crashed) worker cannot be force-killed at all — the
supervisor signals it and moves on, and any thread that stays stuck lingers
until a full process restart clears it (see the OS watchdog below). If the
worker was a VAD worker with an in-progress segment, that in-progress audio is
lost on restart; this is logged as its own distinct event before the instance
is discarded, rather than folded silently into the generic restart log line.

More than a configured number of restarts within a rolling time window flips a
worker to **degraded**: the supervisor stops hot-restarting it in-process
(repeated restarts without backoff would just burn CPU on a constrained
board) and lets the OS watchdog's full-process restart be the recovery path
instead. Degraded status is surfaced through `orchestrator.get_status()` today;
see §8 for the current UI treatment.

### OS-Level Watchdog

The app periodically pings systemd (`sd_notify WATCHDOG=1`) from the same
supervisor thread that does the in-process checks above. If those pings stop
— because the process hung, deadlocked, or was OOM-killed, none of which
in-process supervision can detect from inside the same wedged process —
systemd restarts the whole unit. This is a no-op unless the process was
actually launched under a systemd unit with `NotifyAccess=` configured (i.e.
it's always safe and inert in dev, CI, or any off-device run), and the ping
must never share a code path with the (slower) worker-rebuild work above, so a
slow rebuild can't delay the heartbeat and trigger a spurious restart on top
of an already-in-progress one.

The unit also carries a **crash-loop breaker**
(`StartLimitIntervalSec=300s` / `StartLimitBurst=5`). One
detect-kill-restart cycle takes at least `WatchdogSec + RestartSec` (~13s at
the shipped values), which already exceeds systemd's 10s default limit
interval — so without a widened window the default burst limit could never
trip and a deterministic hang (a deadlock that recurs on every start, a poison
message) would restart forever. Five failures within five minutes now flip the
unit to `failed` instead, which needs a manual `systemctl reset-failed` to
clear. That's the deliberate tradeoff: an unattended box that reports a hard
fault beats one that silently crash-loops.

### Crash-Safe Local Storage

The deployment target can lose power at any instant. There is no database
anywhere in this application and transcript persistence is out of scope (§2),
so the only continuous local writes are two optional debug dump workers (raw
audio, and post-VAD segment audio). Both write via a temp-file-then-replace
pattern rather than writing the destination path directly, so a power cut
mid-write leaves the previous file intact instead of a truncated one. This is
deliberately not a database/WAL-style layer — there's nothing here that needs
one.

### MQTT Recovery

MQTT connections automatically reconnect using exponential backoff, handled
entirely inside the MQTT client itself — this is a connection-level retry, not
a worker restart, and never goes through the supervisor above (conflating the
two would make restart-count metrics noisy and meaningless).

Connection status is exposed through the health object (§7) as a tri-state:
connected, disconnected, or **unknown** — the last meaning the configured
audio source isn't MQTT-based at all (a `WavSource`/`MicSource` in dev/test).
Unknown is deliberately not treated as a fault anywhere.

### Fault Isolation

Malformed audio packets and inference failures are logged and discarded without terminating the worker loop that hit them — this is what keeps the failures above (crash, stall) genuinely rare rather than routine.

### Backpressure

Every queue is bounded, and depth is sampled both periodically (metrics, §7)
and live per health request. A queue disabled by config or not yet built is
omitted from that report rather than reported as zero, so "not running" is
distinguishable from "empty."

Backpressure is also acted on, not just observed: optional work is shed first
(partials are dropped when the segment queue is backed up), and the RMS gate
(`vad.rms_gate_enabled`, on by default) skips Silero inference entirely on
silent frames — which on an RPi5 is what keeps the routed queue from clogging
during the silence that dominates a two-party call.

For the exact call chains behind both layers above — `SupervisedTarget`'s field wiring, the tick loop, the restart budget, the kill/rebuild mechanics, and the sd_notify protocol — see `RELIABILITY.md`.

## 6. Configuration

Configuration is managed through a single typed settings model.

Configuration sources are applied in the following order:

1. Built-in defaults
2. Default configuration file
3. Local device overrides
4. Environment variables

All configuration updates are validated before being applied.

Everything the operational sections describe is a section of this same model,
overridable per-deployment the same way as anything else:

| Section | Governs |
|---|---|
| `reliability` | restart budget, stall threshold, tick/ping cadence, whether the watchdog pings at all (§5) |
| `logging` | master on/off switch, console and file sinks independently, level, JSON vs. pretty, rotation (§7) |
| `metrics` | whether the collector runs, its emit interval, log rounding (§7) |
| `health` | per-channel staleness threshold (§7) |
| `vad` / `stt` | segmentation thresholds, the RMS gate, partial cadence and backlog allowance (§3) |

There are no longer any settings stubs that nothing reads — `logging.json` and
`health.stale_segment_warning_s`, which sat unwired through Milestone 6, are
both consumed now.

## 7. Observability

Built (Milestone 7). Three pieces with deliberately different shapes: logging
is per-event, metrics is tick-driven aggregation, health is query-driven
assembly.

### Structured Logging

Structured JSON logs are the primary operational interface.

- **One master switch.** `logging.enabled: false` calls `logging.disable()`
  process-wide, so every log call becomes a no-op before any formatting or
  handler work — for runs where logging *overhead* matters, not just volume.
- **Two independent sinks.** A console/stderr handler (JSON, or pretty text
  when `logging.json: false` for dev) and a rotating file handler under
  `logging.output_dir`, which is always JSON regardless — a file sink exists
  for later grep/tooling, not for a human reading a terminal. Each run gets
  its own timestamped file rather than every run appending to one; rotation
  bounds any single run.
- The file sink is intentionally **not** routed through the atomic-write
  helper in §5: that protects whole-file overwrites, whereas this is an
  append-only stream where a power cut costs at most the line mid-write.
- JSON output does not escape non-ASCII (`ensure_ascii=False`) — the default
  language is Korean and every transcript line passes through here.

Every pipeline-stage module logs through an adapter that tags records with
`stage`, merging in whatever `channel_id`/`segment_id` a call site supplies.
(The stdlib's own `LoggerAdapter` *overwrites* rather than merges, which would
silently drop exactly those two fields.)

This is what makes a segment's lifecycle traceable end to end: filter the JSON
log on one `segment_id` and you get VAD trigger → finalize → STT → the
terminal `TRANSCRIPT` line (which also carries that segment's own inference
latency). Packets logged before VAD triggers are necessarily `channel_id`-only
— no segment exists yet to attribute them to.

### Metrics

A `MetricsCollector` thread aggregates on `metrics.emit_interval_s`, writing
one structured log line per tick and keeping the latest snapshot in memory for
health reporting to read directly (no round-trip through logs). No Prometheus
endpoint — out of scope (§2).

Aggregated per tick:

- STT inference latency — **pure inference time**, not queue-to-transcript.
  Queue depth already answers "is there a backlog"; folding that into latency
  would make one number mean two things. Tracked both as the most recent call
  overall and per channel.
- Per-channel VAD latency, split into Silero inference vs. the RMS-gate skip
  path — microseconds vs. milliseconds, never comparable as like for like.
- Per-channel router re-packetize latency.
- Queue depth, worker restart counts **against the budget** ("3 of 3 used",
  not a bare count), and MQTT connection status.

Log-line latency values are rounded (`metrics.latency_log_decimals`) for
readability; the in-memory snapshot always keeps full precision.

### Health Reporting

Assembled synchronously per request, with no thread and no cadence of its own
— a facade over the pipeline's live state, the last metrics snapshot, and the
router's per-channel freshness. It is a strict **superset** of the older
`{running, degraded, workers}` status, so nothing that read those three fields
needed changing.

Two design points worth carrying:

- **Two clocks, each labelled.** Queue depths and channel freshness are
  sampled live at request time; MQTT connectivity and restart counts come from
  the last metrics tick and ship with an explicit `age_s`. Nothing in the
  payload mixes the two silently, and a field that's null says *why* —
  "metrics disabled", "no tick yet", or "that signal doesn't apply here".
- **Channel staleness never turns the overall verdict amber.** Between calls
  every channel is legitimately stale, so folding staleness into the top-level
  status would leave the kiosk amber most of the day and train operators to
  ignore it. Staleness surfaces per channel instead.

Per-channel freshness comes from the router's wall clock ("is this channel
still sending audio"), never VAD's monotonic idle clock — both exist, but
they're different clocks answering different questions, and shipping both
would put two similar-looking numbers in one payload that can legitimately
disagree.

For the full `GET /api/status` payload, when each field is null, and how to
read the kiosk's status rows, see `HEALTH.md`.



## 8. Web UI

> **Note:** by design, this section describes the target feature set, not only
> what's built today.
>
> **Shipped:** a live transcript stream over **Server-Sent Events** (the
> closing line below says WebSockets — that's superseded; SSE was chosen
> since the data only flows one way and needs no channel for the browser to
> push back on), with partial transcripts replacing in place on `segment_id`;
> a clear button that wipes transcript history (`POST /api/transcripts/clear`);
> a status pill with three states (live / degraded / stopped); a status strip
> showing MQTT state, queue depths, per-channel freshness, and how old the
> metrics reading is; a module row giving each pipeline stage its own state
> chip plus its worst-channel latency, with the supervisor listed alongside;
> and a static legend bar (the kiosk has no mouse, so nothing may depend on
> hover to be legible). Start/stop exist as API endpoints only — still no page
> controls.
>
> **Not built:** restarting individual workers, viewing/editing configuration,
> and any dashboard beyond the strip above. Keeping those written down is
> deliberate — it's the intended direction for this UI, not stale scope.

The web interface provides:

### Control

- Start pipeline
- Stop pipeline
- Restart workers



### Configuration

- View effective configuration
- Edit local configuration
- Validate changes before applying



### Live Monitoring

- Real-time transcript stream
- Health dashboard
- Metrics dashboard

The initial implementation uses server-rendered pages and WebSockets to minimize complexity and resource usage.

## 9. Testing

Testing is performed at two levels.

### Unit Tests

Verify isolated component behavior using mocked dependencies.

Current coverage: pipeline orchestration/lifecycle, supervisor restart and
stall-detection behavior (crash, stall, degrade, and the OS-watchdog ping
plumbing, all driven with fake/controllable workers rather than real threads),
atomic file writes, the MQTT client, the transcript hub, the web UI's API
surface, metrics aggregation, health-report assembly, partial transcripts end
to end (emission cadence, droppability, and their exclusion from latency and
dumps), and — with a fake Silero model, so no real load — VAD's per-channel
latency accounting and constructor-time channel set, plus the router's
per-channel latency accounting.

Two gaps worth knowing about before you go looking:

- **VAD segmentation and routing *correctness*** are still verified through
  the integration fixture test below, not isolated unit tests. The VAD/router
  unit tests above cover latency plumbing and channel bookkeeping, not
  segment boundaries.
- **Config validation and the logging module have no dedicated unit tests** —
  both are exercised incidentally by everything that loads `Settings` or emits
  a log line.

### Integration Tests

Exercise multiple real components together using realistic MQTT and audio fixtures.

Examples include:

- MQTT → Router → VAD
- End-to-end transcription pipeline (real recorded duplex-call fixtures, checked against known-good segment counts)

This one is marked opt-in (needs a live MQTT broker) and excluded from the
default local/CI test run; run it explicitly with `pytest -m integration`.

### Continuous Integration

CI (`.github/workflows/ci.yml`) runs on every push to `main` and on every pull
request: lint (`ruff check`), formatting (`ruff format --check`), type
checking (`mypy`), and the default test suite. It does not run the opt-in
integration test above (no broker available in CI) or any performance
validation — performance is still verified manually, on target hardware.

## 10. Deferred Decisions

- Transcript persistence backend.
- Deployment/installation tooling beyond the systemd unit + `make install` /
  `make install-service` — containerization itself remains a non-goal (§2),
  but packaging/distribution beyond "clone the repo, apt the broker, install a
  unit file" is still open.
- **Streaming STT** (`STREAMING_STT_PLAN.md`) — scoped, not started, and
  blocked upstream rather than by us: Moonshine publishes streaming archs for
  English but only `TINY` for Korean. While the deployment language is Korean
  this is unreachable, and the partial transcripts in §3 are the standing
  workaround. `STTWorker` already drives the streaming session API
  (`start()`/`add_audio()`/`stop()` with line listeners), so the code delta if
  the language ever changes is small.
- **Call lifecycle + JSON audio payloads** (`CALL_LIFECYCLE_PLAN.md`) —
  scoped, not started. Call-start/call-end signals over MQTT, resetting the
  pipeline and clearing the UI per call, with audio moving from raw PCM to a
  JSON envelope. Two known hazards are already recorded there: `reset_channel`
  discards in-progress audio (right for a reconnect gap, wrong for a call end,
  which needs flush-then-reset), and the re-packetizer drifts timestamps
  silently across a discontinuity if it isn't reset too. Note the wire format
  is currently split — `wav_source.py` publishes a JSON envelope while
  `wav_source_raw.py` and `mic_source.py` publish raw PCM, and only raw PCM is
  what ingest actually consumes today.

Resolved since the last revision of this document:

- **Threads vs. asyncio** — decided as a hybrid, not exclusively one or the
other: every pipeline worker (including the supervisor) is a
`threading.Thread`; asyncio is used only at the FastAPI/web UI boundary,
bridging into the thread-based pipeline via a thread pool for any call that
blocks (e.g. stopping the pipeline).

