# edge-voice Architecture (v0.2)

**Status:** Reflects the implementation through Milestone 6 (Reliability). Sections
marked **(Planned)** describe Milestone 7/8 work that hasn't started yet — the
current behavior for those areas is spelled out alongside the plan, not left
implicit. **§8 Web UI is an exception by design**: it describes the target
feature set the UI is meant to grow into, not what's built today (see the note
at the top of that section for what's actually shipped).

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
 Per-channel Silero VAD
        │
        ▼
 Shared Moonshine STT
        │
        ▼
 Transcript Events
        │
        ├─ Structured logs
        ├─ Live transcript stream (SSE)
        └─ Future persistence

  Supervisor watches all four pipeline workers end-to-end (§5); an OS-level
  watchdog watches the whole process underneath that.
```

## 1. Problem Statement

`edge-voice` transcribes two-party phone calls in near real time on resource-constrained edge devices such as Raspberry Pi 5 and Jetson. Korean is the default language; Moonshine also supports Arabic, English, Spanish, Japanese, Ukrainian, Vietnamese, and Chinese, selected via configuration.

Each call leg arrives as a separate MQTT audio stream. The system must:

* Produce ordered, channel-attributed transcripts.
* Operate reliably despite transient failures.
* Provide enough observability to diagnose issues without direct shell access.
* Run efficiently on limited CPU and memory resources.

## 2. Non-Goals (v0.1)

The following are intentionally out of scope for the initial release:

* Docker packaging.
* External metrics systems (Prometheus, Grafana, etc.).
* Multi-tenant deployments.
* Speaker diarization beyond channel attribution.
* Transcript persistence beyond logs and live streaming.

## 3. Architecture

### Core Decision: Per-Channel VAD, Shared STT

The system runs one Silero VAD model instance per channel, but a single shared Moonshine STT instance across all channels.

This differs from earlier prototypes that ran fully independent pipelines per audio source, and from an earlier version of this design that used one shared VAD instance for all channels.

**VAD is per-channel, not shared.** A single shared model was tried first and reverted: `VADIterator` only holds the segmentation state machine, the LSTM hidden state used for inference lives inside the model instance itself. Two channels interleaving packets against one shared model corrupted each other's hidden state — measured as doubled, garbage segment counts against recorded call fixtures. Each channel therefore gets its own model instance (one `VADIterator` + one Silero model per `channel_id`), which also removes any need for cross-channel locking since a channel's state is only ever touched by that channel's packets. The extra cost (~4MB, ~0.06s load) per channel is negligible next to correctness.

**STT remains shared.** Unlike VAD, Moonshine's `Transcriber.start()`/`stop()` fully resets its decoder state between segments (verified byte-for-byte against a fresh instance per segment), and the STT worker only ever processes one finalized segment at a time regardless of which channel it came from — there is no concurrent access to isolate. Sharing one instance also fits the turn-taking nature of phone conversations: the previous speaker's context shouldn't bleed into the next line regardless of channel, and it avoids holding a second copy of the model in memory (~175MB saved).

Reasons this split still favors resource efficiency and conversation semantics over one independent pipeline per audio source:

1. **Resource efficiency**

   * A full independent VAD+STT pipeline per channel would duplicate CPU and memory usage well beyond the per-channel VAD model's small footprint.
   * Phone conversations are typically turn-based, making parallel STT instances unnecessary.

2. **Conversation semantics**

   * Separate call legs represent one conversation rather than unrelated audio streams.
   * A shared STT stage preserves conversation ordering and simplifies downstream processing.

### Routing Model

```text
MQTT channel 1 ──┐               ┌─▶ VAD (channel 1) ─┐
                 ├─▶ Router ─────┤                     ├─▶ Shared STT ─▶ Transcript
MQTT channel 2 ──┘               └─▶ VAD (channel 2) ─┘
```

* Incoming audio packets are tagged with `channel_id`.
* Packets are placed onto a shared ingest queue.
* The router re-packetizes each channel's stream to a fixed outgoing frame size (independently configurable from the incoming frame size — e.g. 20ms arriving frames re-chunked to the 32ms window VAD expects) before handing packets on.
* VAD processes packets serially (one worker thread) but against independent per-channel model instances and state — no cross-channel interference, no locking required.
* Finalized speech segments are placed onto a shared STT queue.
* STT processes one segment at a time, against one shared model instance.
* Channel attribution is preserved throughout the pipeline.

### Tradeoffs

Overlapping speech is processed sequentially rather than simultaneously.

This favors resource efficiency and implementation simplicity over perfect concurrent transcription. For typical phone-call workloads this is considered an acceptable tradeoff.

If overlap-heavy workloads become common, additional STT workers can be introduced without redesigning the routing architecture.

## 4. Concurrency Model

The pipeline consists of four long-lived worker threads connected by bounded queues, plus a fifth thread that supervises the other four end-to-end.

### MQTT Ingest

* Subscribes to per-channel MQTT topics; receives audio packets.
* Tags packets with channel metadata.
* Pushes packets onto the ingest queue.
* Performs no expensive processing. Reconnects on its own (§5) — this is invisible to the supervisor, which only acts on a worker thread dying outright, not on a connection blip.

### Channel Router

* Consumes packets from the ingest queue.
* Validates `channel_id`, tracks per-channel last-seen timestamps.
* Re-packetizes to the fixed outgoing frame size VAD expects (see Routing Model above).
* Pushes re-packetized packets onto the routed queue; optionally mirrors raw packets to a debug dump queue.

### VAD Worker

* Consumes packets from the routed queue.
* Maintains per-channel VAD state (own model instance, own segmentation state machine, own preroll buffer).
* Produces finalized speech segments onto the segment queue; optionally mirrors segments to a debug dump queue.

### STT Worker

* Consumes finalized segments from the segment queue.
* Runs Moonshine inference against the one shared `Transcriber`.
* Emits transcript events.

### Supervisor

* Watches all four workers above (not the optional debug dump workers).
* Restarts a worker that crashes or wedges; see §5 for the detection rules and the OS-level layer underneath it.
* Runs as its own thread with the same start/stop lifecycle as every other worker, so it works identically whether the process is running headless or hosting the web UI.

This producer/consumer architecture prevents transcription latency from blocking audio ingestion.

## 5. Reliability

Reliability is a primary design goal, split into two independent layers because
each catches a failure mode the other structurally cannot: in-process
supervision only works if the process is still scheduling threads at all — it
cannot rescue a deadlocked or OOM-killed process — which is exactly what the
OS-level watchdog underneath it is for.

### Worker Supervision (in-process)

Workers are supervised by a single generic thread-watchdog and restarted after
two distinct kinds of unexpected failure:

* **Crash** — a worker thread exits without having been asked to stop.
* **Stall** — a worker is still alive, has work waiting on its input queue, but
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

**(Planned)** Connection status is not yet exposed through status/health
reporting; that lands with the health object in §7.

### Fault Isolation

Malformed audio packets and inference failures are logged and discarded without terminating the worker loop that hit them — this is what keeps the failures above (crash, stall) genuinely rare rather than routine.

### Backpressure

Queues are bounded today.

**(Planned)** Queue depth is not yet tracked or surfaced anywhere; that's
scoped to the metrics work in §7, specifically so it isn't duplicated across
both the reliability and observability milestones.

## 6. Configuration

Configuration is managed through a single typed settings model.

Configuration sources are applied in the following order:

1. Built-in defaults
2. Default configuration file
3. Local device overrides
4. Environment variables

All configuration updates are validated before being applied.

The reliability behavior in §5 (restart budget, stall threshold, tick/ping
cadence, and whether the watchdog ping is emitted at all) is one section of
this same settings model, so it's overridable per-deployment the same way
everything else is.

## 7. Observability

**(Planned — Milestone 7, not yet built.)** The sections below describe the
target design. Today: logging exists (level is configurable, and a JSON-output
flag is already defined in the settings model) but log lines are plain
key/value text, not structured JSON, and carry no automatic channel/segment
context — the JSON flag isn't wired to anything yet. There is no metrics
aggregation and no health endpoint.

### Structured Logging

Structured JSON logs are the primary operational interface.

Log events include contextual information such as:

* Channel ID
* Pipeline stage
* Segment ID

This allows a segment's lifecycle to be traced across the pipeline.

### Metrics

Metrics are emitted as structured log events and aggregated in memory.

Examples include:

* STT inference latency
* Queue depth
* Worker restart counts
* MQTT connection status

### Health Reporting

A health endpoint exposes:

* Overall system status
* Worker status and restart counts
* Queue depths
* MQTT connectivity
* Per-channel activity freshness

## 8. Web UI

> **Note:** by design, this section describes the target feature set, not
> what's built today. What's actually shipped: pipeline start/stop (API only,
> no page controls yet), a live transcript stream over **Server-Sent Events**
> (the closing line below says WebSockets — that's superseded; SSE was chosen
> instead, since the data only flows one way and doesn't need a channel for
> the browser to push back on), and a status pill with three states (live /
> degraded / stopped). Everything else below — restarting individual workers,
> viewing/editing configuration, and any metrics or fuller health dashboard —
> is intentionally still aspirational; keeping it written down here is
> deliberate, since it's the intended direction for this UI, not stale scope.

The web interface provides:

### Control

* Start pipeline
* Stop pipeline
* Restart workers

### Configuration

* View effective configuration
* Edit local configuration
* Validate changes before applying

### Live Monitoring

* Real-time transcript stream
* Health dashboard
* Metrics dashboard

The initial implementation uses server-rendered pages and WebSockets to minimize complexity and resource usage.

## 9. Testing

Testing is performed at two levels.

### Unit Tests

Verify isolated component behavior using mocked dependencies.

Current coverage: pipeline orchestration/lifecycle, supervisor restart and
stall-detection behavior (crash, stall, degrade, and the OS-watchdog ping
plumbing, all driven with fake/controllable workers rather than real threads),
atomic file writes, the MQTT client, the transcript hub, and the web UI's API
surface.

VAD segmentation logic and channel routing correctness are currently verified
through the integration fixture test below rather than isolated unit tests —
worth knowing if you're looking for one and don't find it.

### Integration Tests

Exercise multiple real components together using realistic MQTT and audio fixtures.

Examples include:

* MQTT → Router → VAD
* End-to-end transcription pipeline (real recorded duplex-call fixtures, checked against known-good segment counts)

This one is marked opt-in (needs a live MQTT broker) and excluded from the
default local/CI test run; run it explicitly with `pytest -m integration`.

### Continuous Integration

CI (`.github/workflows/ci.yml`) runs on every push to `main` and on every pull
request: lint (`ruff check`), formatting (`ruff format --check`), type
checking (`mypy`), and the default test suite. It does not run the opt-in
integration test above (no broker available in CI) or any performance
validation — performance is still verified manually, on target hardware.

## 10. Deferred Decisions

* Transcript persistence backend.
* Enhanced overlap handling.
* Deployment/installation tooling beyond the systemd unit (`deploy/edge-voice.service`) — containerization itself remains a non-goal (§2), but packaging/distribution beyond "copy the repo and install a unit file" is still open.

Resolved since the last revision of this document:

* **Threads vs. asyncio** — decided as a hybrid, not exclusively one or the
  other: every pipeline worker (including the supervisor) is a
  `threading.Thread`; asyncio is used only at the FastAPI/web UI boundary,
  bridging into the thread-based pipeline via a thread pool for any call that
  blocks (e.g. stopping the pipeline).
