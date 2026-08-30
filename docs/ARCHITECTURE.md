# edge-voice Architecture

| | |
|---|---|
| **Version** | v0.3 |
| **Owner** | Michae1Park |
| **Last updated** | 2026-08-06 |
| **Status** | Describes the system as built. Anything not built says so inline, and §11 lists what's deferred. |

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
 Per-channel Moonshine STT ─────────────────▶ Transcript Events
 (one dedicated OS process
  per channel, decoding in
  parallel — see §3)
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

`edge-voice` transcribes two-party audio in near real time on a resource-constrained edge device. Phone calls are the reference workload, but nothing in the design is phone-specific — any two-party source where each party's audio arrives as its own stream fits (walkie-talkie/PTT links, radio bridges, two-mic meeting capture). Korean is the default language; Moonshine also supports Arabic, English, Spanish, Japanese, Ukrainian, Vietnamese, and Chinese, selected via configuration.

Each party's audio arrives as a separate MQTT stream ("call leg" below, for the reference workload). The system must:

- Produce ordered, channel-attributed transcripts.
- Operate reliably despite transient failures.
- Provide enough observability to diagnose issues without direct shell access.
- Run efficiently on limited CPU and memory resources.

### Performance Budget

Nearly every design decision below — the partial cadence, the RMS gate, one
`add_audio()` call per segment, shedding optional work under backlog — is a
latency or CPU optimization. Those can only be judged against stated targets,
so they belong here rather than implied. **The `TBD` rows are not yet
committed**: they need a measurement pass on the target board, and until then
this table records what's known and what isn't, rather than implying a budget
that was never set.

| Dimension | Target | Status |
|---|---|---|
| End-to-end latency, speech end → final transcript | TBD | Not yet measured as a single number; per-stage latency *is* collected (§7), so the pieces exist to sum |
| Time to first visible text on a turn | TBD | Measured for the partial-cadence tuning only: first text lands ~1.1s earlier than the final at the shipped defaults (see `configs/default.yaml`) |
| Sustained real-time factor (audio consumed ÷ wall clock, per channel) | ≥ 1.0 sustained across both channels | Not directly measured; queue depth (§7) is the current proxy — a growing routed or segment queue means the target is being missed |
| Concurrent channels | 2 (one per party) | Measured on the RPi5 target (see `docs/BENCHMARK.md`): a single shared STT decoder cannot sustain real-time dual-channel transcription (queue backlog grows unbounded), which is why STT moved to one dedicated decoder per channel — see §3 Tradeoffs |
| Memory | TBD ceiling | Known components: ~30-60MB per Moonshine `Transcriber` (varies by language/checkpoint, measured directly on RPi5 — see `docs/BENCHMARK.md` and `scratch/probe_decoder_memory.py`), one per channel, ~4MB per channel for Silero, plus bounded queues |
| CPU headroom | TBD | The RMS gate removes ~52% of Silero forward passes on duplex call audio, ~74% on a mostly-idle channel |


## 2. Non-Goals

The following remain intentionally out of scope:

- Docker packaging.
- External metrics systems (Prometheus, Grafana, etc.).
- Multi-tenant deployments.
- Speaker diarization beyond channel attribution.
- Transcript persistence beyond logs and live streaming.



## 3. Architecture

### Module Map

What each module is responsible for, in rough data-flow order. Paths are
relative to `src/edge_voice/` unless noted. `BUILDPLAN.md` maps these same
packages onto the milestones that built them.

| Module | Responsibility |
|---|---|
| **Entry point & configuration** | |
| `cli.py` | Parses args, loads `Settings`, configures logging, then either runs headless for a fixed duration or starts the pipeline and blocks serving the kiosk UI. `--mic` additionally spawns a mic-capture subprocess. |
| `config/settings.py` | The single typed (pydantic) settings model and its layered load: defaults → `configs/default.yaml` → `configs/local.yaml` → env vars, validated on load (§6). |
| **Pipeline stages** (the supervised worker threads) | |
| `audio_ingest/mqtt_client.py` | Subscribes to per-channel MQTT topics, wraps each payload as an `AudioPacket`, pushes to the ingest queue. Owns its own reconnect backoff and exposes `connected` for health (§5). |
| `channel/router.py` | Validates `channel_id`, tracks per-channel last-seen wall-clock time (the freshness signal in §7), re-packetizes to the fixed frame size VAD expects, forwards to the routed queue. |
| `vad/vad_worker.py` | Per-channel Silero segmentation: own model instance and state machine per channel, preroll, idle flush, soft/hard length cuts, `segment_id` minted at segment *start*, and the optional in-progress partials (§3). Emits each channel's segments to that channel's own segment queue. |
| `stt/stt_worker.py` | One dedicated instance per channel, each against its own Moonshine `Transcriber` — one `add_audio()` call per segment, repetitive-output guard, per-call and per-channel latency, partial handling and shedding. |
| **Composition & lifecycle** | |
| `pipeline/orchestrator.py` | Builds every queue and worker from `Settings` and owns start/stop ordering; the single seam exposing `get_status()`, `queue_depths()`, `channel_freshness()`, and `health()`. |
| `pipeline/models.py` | The shared vocabulary — `AudioPacket`, `SpeechSegment`, `TranscriptEvent` — so stages import one common type set rather than each other. |
| `pipeline/queues.py` | Factories for the bounded queues connecting stages; sizes come from `Settings.queues`. |
| `pipeline/fanout.py` | Puts one item onto a main destination plus an optional debug-dump queue, dropping and logging rather than blocking when either is full. |
| `pipeline/transcript_hub.py` | N-subscriber pub/sub for `TranscriptEvent`s with per-connection queues and backlog replay, so a kiosk reload isn't blank. Separate from `fanout.py` because the subscriber set changes per browser connect. |
| **Reliability** (§5) | |
| `pipeline/supervisor.py` | Generic thread-watchdog: detects crashes and stalls, restarts, tracks a windowed restart budget, flips to degraded. Knows nothing about VAD/STT/MQTT — it operates on `SupervisedTarget` callables the orchestrator wires up. |
| `pipeline/systemd_watchdog.py` | Dependency-free `sd_notify` client driven from the supervisor's tick; a complete no-op when `$NOTIFY_SOCKET` is unset (dev, CI, any off-device run). |
| `audio_ingest/atomic_write.py` | Temp-file-then-`os.replace()` helper, so a power cut mid-write leaves the previous file intact rather than a torn one. |
| **Observability & health** (§7) | |
| `observability/logging.py` | JSON formatter, the stage-tagging logger adapter that merges `channel_id`/`segment_id` from call sites, and the console/rotating-file sink setup. |
| `observability/metrics.py` | `MetricsCollector` thread: aggregates queue depths, per-stage latency, restart budget, and MQTT state into one log line plus an in-memory `MetricsSnapshot` per tick. |
| `health/reporting.py` | Assembles the `/api/status` object per request from live pipeline state, the last metrics snapshot, and router freshness — labelling which fields came from which clock. |
| **Web UI** (§8) | |
| `webui/app.py` | FastAPI app: `/api/status`, start/stop, the SSE transcript stream, and transcript clear. Runs in-process with the orchestrator; no MQTT anywhere in it. |
| `webui/templates/console.html` | The kiosk console page — transcript feed, status pill and strip, per-module chips, static legend bar. |
| **Dev/test tooling** (separate processes, never imported by the pipeline) | |
| `utils/audio_generation/wav_source_raw.py` | Replays `.wav` files at real-time pace as raw PCM over MQTT — the wire format ingest actually consumes; used by the integration fixture. |
| `utils/audio_generation/wav_source.py` | Same replay, but publishing a JSON envelope — the shape `docs/deferred/CALL_LIFECYCLE_PLAN.md` would standardize on (§11). |
| `utils/audio_generation/mic_source.py` | Captures a live mic at the device's native rate, resamples, publishes raw PCM over MQTT exactly like a real call leg. |
| `utils/audio_generation/fake_source.py` | Synthetic packet generator that skips MQTT entirely, from the pre-MQTT skeleton; kept for exercising the worker graph without audio. |
| `audio_ingest/audio_dump.py`, `audio_ingest/segment_audio_dump.py` | Optional debug workers writing raw and post-VAD audio to WAV, for inspecting VAD boundaries against the original recording. Off by default. |
| **Deployment** | |
| `deploy/edge-voice.service`, `Makefile` | systemd unit (watchdog, restart policy, crash-loop breaker) plus `make install` / `make install-service` for the apt and unit-file steps. |

### Core Decision: Per-Channel VAD and STT

The system runs one Silero VAD model instance per channel, and one dedicated Moonshine STT `Transcriber` per channel.

This differs from earlier prototypes that ran fully independent pipelines per audio source, and from earlier versions of this design that used one shared VAD instance for all channels, and — until this decision was reversed — one shared STT instance for all channels too.

**VAD is per-channel, not shared.** A single shared model was tried first and reverted: `VADIterator` only holds the segmentation state machine, the LSTM hidden state used for inference lives inside the model instance itself. Two channels interleaving packets against one shared model corrupted each other's hidden state — measured as doubled, garbage segment counts against recorded call fixtures. Each channel therefore gets its own model instance (one `VADIterator` + one Silero model per `channel_id`), which also removes any need for cross-channel locking since a channel's state is only ever touched by that channel's packets. The extra cost (~4MB, ~0.06s load) per channel is negligible next to correctness.

**STT is per-channel too, not shared.** This was originally the opposite decision: one shared decoder, on the reasoning that Moonshine's `Transcriber.start()`/`stop()` fully resets decoder state between segments (verified byte-for-byte against a fresh instance per segment, still true), that a single worker thread meant "there is no concurrent access to isolate," and that sharing avoided a second copy of the model in memory (~175MB estimated). Measured on the actual RPi5 target hardware (`docs/BENCHMARK.md`, `scratch/bench_pipeline_load.py`), that reasoning didn't hold up:

  - **Throughput**: one sequential decoder cannot sustain real-time dual-channel transcription. Total STT decode demand for two concurrent channels sums to ~95%+ of the real-time wall-clock budget on a single decoder, so its input queue backs up without bound. A single channel alone, given its own dedicated decoder, comfortably uses only ~65% of budget with real margin. Giving the one shared decoder more CPU cores doesn't fix it either — multi-threading a single `Transcriber` across cores measured *slower* than single-threaded, because thread-sync overhead on a model this small outweighs the benefit. The only lever that works is genuine parallelism: a second independent decoder instance.
  - **Memory**: the ~175MB estimate was never measured against this hardware/model. A second `Transcriber` costs on the order of tens of MB marginal RSS in practice (measured directly with `scratch/probe_decoder_memory.py`) — comfortably inside the RPi5's available memory at idle.

The turn-taking argument (a speaker's context shouldn't bleed into the next line regardless of channel) doesn't actually depend on sharing one instance — each channel's own dedicated `Transcriber` still resets fully between segments via the same `start()`/`stop()` lifecycle, so per-segment correctness is unchanged from the old design.

### Routing Model

```text
MQTT channel 1 ──┐               ┌─▶ VAD (channel 1) ─┬─▶ STT (channel 1) ─┐
                 ├─▶ Router ─────┤                     │                    ├─▶ Transcript
MQTT channel 2 ──┘               └─▶ VAD (channel 2) ─┴─▶ STT (channel 2) ─┘
```

- Incoming audio packets are tagged with `channel_id`.
- Packets are placed onto a shared ingest queue.
- The router re-packetizes each channel's stream to a fixed outgoing frame size (independently configurable from the incoming frame size — e.g. 20ms arriving frames re-chunked to the 32ms window VAD expects) before handing packets on.
- VAD processes packets serially (one worker thread) but against independent per-channel model instances and state — no cross-channel interference, no locking required.
- Finalized speech segments are placed onto that channel's own segment queue (one queue per channel), alongside the optional revisable prefixes described below.
- Each channel's STT worker processes its own segments in order, against its own dedicated `Transcriber` instance, in parallel with every other channel's worker.
- Channel attribution is preserved throughout the pipeline.

Every model is loaded at worker construction, not lazily on the first packet
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
- **Partials are shed first; finals are shed only as a last resort.** STT
  skips a partial when the segment queue is already at
  `stt.partial_max_queue_depth`, and VAD drops a partial rather than blocking
  when the queue is full. On a constrained board the correct response to a
  backlog is to stop doing optional work.
  **This is a scheduling policy, not a durability guarantee:** no policy
  deliberately discards a final, but a final is still enqueued with a bounded
  timeout, so if the segment queue stays full for that long the final is
  dropped too — logged at WARNING with its `channel_id` and `segment_id`, so
  the loss is attributable rather than silent. A backlog deep enough to reach
  that point means the pipeline is already failing its real-time budget (§1).
- Partials are excluded from STT latency metrics and from the segment-audio
  dump — a partial transcribes a prefix, so mixing the two would make latency
  meaningless and the dumps redundant.

The cost is real and paid per partial: with no streaming model, each one
re-transcribes the whole prefix from scratch. The measured gain/cost tradeoff
behind the shipped defaults is recorded in `configs/default.yaml`. A genuine
streaming arch would remove the re-transcription entirely — see §11.

### Tradeoffs

Overlapping speech is processed sequentially rather than simultaneously.

This favors resource efficiency and implementation simplicity over perfect concurrent transcription. For the turn-taking two-party workloads this targets, that's an acceptable tradeoff.

If overlap-heavy workloads become common, additional STT workers can be introduced without redesigning the routing architecture.

Each finalized segment is fed to Moonshine in **one** `add_audio()` call rather than in windows. Windowed feeding was measured as strictly worse on real audio (~2.7x slower, since the decoder redoes real work per call, and prone to duplicated words at window boundaries).

## 4. Concurrency Model

Four long-lived worker threads connected by bounded queues, plus two observer
threads watching those four, plus up to two optional debug dump workers. What
each one *does* is in the Module Map (§3); this section is about thread
ownership and the queue boundaries between them.

| Thread | Consumes | Produces | Supervised? |
|---|---|---|---|
| MQTT Ingest | (MQTT topics) | ingest queue | Yes — crash only |
| Channel Router | ingest queue | routed queue, + optional dump queue | Yes |
| VAD Worker | routed queue | segment queue, + optional segment-dump queue | Yes |
| STT Worker | segment queue | transcript events (callback) | Yes |
| Supervisor | — (polls the four above) | restart actions, watchdog pings | No — nothing supervises it |
| Metrics Collector | — (samples the four above) | log line + in-memory snapshot | No |
| Dump workers (×2, optional) | dump queues | WAV files | No |

The producer/consumer split is what keeps transcription latency from blocking
audio ingestion: a slow STT call backs up the segment queue without ever
stalling the MQTT callback thread.

Four points about the observer threads that the table can't carry:

- **MQTT Ingest is stall-exempt.** It has no input queue, and its run loop just
  blocks on the stop event while paho owns reconnect — "hasn't consumed
  anything lately" isn't a liveness signal for it, so only outright thread
  death counts (§5).
- **The supervisor has the same start/stop lifecycle as any worker**, so it
  behaves identically whether the process runs headless or hosts the web UI.
- **Its own liveness is reported alongside the workers it watches.** Nothing
  supervises the supervisor; if its thread died, restart and degraded state
  would silently freeze at its last tick instead of reporting the loss.
- **Metrics is separate from the supervisor's tick on purpose.** The
  supervisor is generic ("a thread died, restart it") and has no business
  knowing about STT latency or MQTT. Metrics starts after it and stops before
  it, so it never observes a pipeline mid-teardown.

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
until the OS watchdog's full-process restart clears it. If the
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
supervisor thread that does the in-process checks above; if those pings stop,
systemd restarts the whole unit. This is a no-op unless the process was
actually launched under a systemd unit with `NotifyAccess=` configured — it is
always safe and inert in dev, CI, or any off-device run.

The ping must never share a code path with the (slower) worker-rebuild work
above, so a slow rebuild can't delay the heartbeat and trigger a spurious
restart on top of an already-in-progress one.

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
`health.stale_segment_warning_s` — both of which were defined before anything
read them — are consumed now.

## 7. Observability

Three pieces with deliberately different shapes: logging is per-event,
metrics is tick-driven aggregation, health is query-driven assembly.

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

The only client is a kiosk-mode browser on the device's own attached display,
so the UI runs in-process with the pipeline, serves server-rendered pages, and
binds to loopback (see §10). There is no MQTT anywhere in it — all UI ↔
pipeline data flow is in-process.

### Live Monitoring

- **Transcript feed over Server-Sent Events.** One-directional (server →
  browser), so SSE avoids WebSocket's handshake and framing for a channel
  nothing pushes back on. Each connection gets its own subscriber queue,
  pre-seeded with recent backlog so a kiosk reload isn't blank.
- **Partials replace in place**, keyed on `segment_id`, until the one
  `is_final=True` event closes that segment (§3).
- **Status pill** with three states: live / degraded / stopped.
- **Status strip**: MQTT state, queue depths, per-channel freshness, and the
  age of the metrics reading behind those numbers (§7).
- **Per-module chips**: each pipeline stage's state plus its worst-channel
  latency, with the supervisor listed alongside the workers it watches.
- **Static legend bar.** The kiosk has no mouse, so nothing may depend on
  hover to be legible.

### Control

- Start and stop the pipeline — **API endpoints only** (`POST /api/start`,
  `POST /api/stop`); no page controls are wired to them yet.
- Clear transcript history (`POST /api/transcripts/clear`), which the page
  does expose as a button.

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

- Deployment/installation tooling beyond the systemd unit + `make install` /
  `make install-service` — containerization itself remains a non-goal (§2),
  but packaging/distribution beyond "clone the repo, apt the broker, install a
  unit file" is still open.
- **Streaming STT** (`docs/deferred/STREAMING_STT_PLAN.md`) — scoped, not started. Moonshine
  publishes streaming archs for English only, so `(language, arch)` becomes a
  validated pair rather than a free choice; that validation layer is the first
  piece of work in the plan. Measured finding worth carrying here: streaming is
  **3.3×–10.7× more compute**, not less — it buys latency-to-first-text and
  replaces the partial transcripts in §3, rather than making STT faster. Whether
  two streaming channels fit on a Pi is the open risk that decides the plan.
- **Call lifecycle + JSON audio payloads** (`docs/deferred/CALL_LIFECYCLE_PLAN.md`) —
  scoped, not started. Call-start/call-end signals over MQTT, resetting the
  pipeline and clearing the UI per call, with audio moving from raw PCM to a
  JSON envelope. Two known hazards are already recorded there: `reset_channel`
  discards in-progress audio (right for a reconnect gap, wrong for a call end,
  which needs flush-then-reset), and the re-packetizer drifts timestamps
  silently across a discontinuity if it isn't reset too. Note the wire format
  is currently split — `wav_source.py` publishes a JSON envelope while
  `wav_source_raw.py` and `mic_source.py` publish raw PCM, and only raw PCM is
  what ingest actually consumes today.
- **STT multiprocessing** (`STT_MULTIPROCESS_PLAN.md`) — **BUILT (2026-08-10);
  the RPi5 before/after measurement is the one piece still outstanding.** The per-channel
  `STTWorker` *threads* (this section, above) fixed unbounded queue growth but
  not genuine parallelism: confirmed on the RPi5, both on the real pipeline
  (decode calls alternate in lockstep between channels) and in isolation
  (`scratch/probe_gil_release.py`: two threads, two independent `Transcriber`
  instances, 1.02x speedup — no benefit). The GIL serializes decode compute
  regardless of thread/core count, and free-threaded Python is not yet a way
  out (ONNX Runtime force re-enables the GIL on 3.13t builds). The current fix
  works by keeping total demand under budget (~65% measured), which is a
  margin, not a capacity guarantee. The plan moves each channel's `STTWorker`
  to its own OS process — one per channel, cores 2 and 3, with ingest/router/
  VAD staying as threads on cores 0-1. Recomputed per core, the same RPi5 run
  would put the busiest STT core at ~40%, i.e. real headroom rather than a
  margin. VAD deliberately stays a thread and gets re-measured afterwards: it
  is currently GIL-blocked for the duration of every decode, so moving STT out
  speeds VAD up for free, and measuring it beforehand would measure
  contention that is about to disappear.

Resolved since the last revision of this document:

- **Threads vs. asyncio** — decided as a hybrid, not exclusively one or the
other: every pipeline worker (including the supervisor) is a
`threading.Thread`; asyncio is used only at the FastAPI/web UI boundary,
bridging into the thread-based pipeline via a thread pool for any call that
blocks (e.g. stopping the pipeline).

