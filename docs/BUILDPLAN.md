# edge-voice Build Plan

**Companion to:** `ARCHITECTURE.md` (v0.5.0)
**Purpose:** answer "what do I type next?" in under 10 seconds after time away.

---

## Package map

This plan tracks work against the actual source layout, not abstract "layers."
For reference, here's how the four conceptual stages from the architecture doc
map onto packages:

| Architecture stage          | Package(s)                  |
|------------------------------|------------------------------|
| (test-only) audio generation | `utils/audio_generation/` *(not shipped — runs as a separate process, see note below)* |
| Audio packet ingestion + routing | `audio_ingest/`, `channel/` |
| VAD                          | `vad/`                       |
| STT                          | `stt/`                       |
| Composition / lifecycle      | `pipeline/orchestrator.py`   |
| Fault tolerance              | `pipeline/supervisor.py`, `pipeline/systemd_watchdog.py`, `deploy/edge-voice.service` |
| Entry point                  | `cli.py`                     |
| Config                       | `config/`                    |
| Web UI                       | `webui/` (in-process with `cli.py`/`orchestrator`) |
| Observability                | `observability/logging.py`, `observability/metrics.py` |
| Health                       | `health/reporting.py`        |
| Deployment                   | `Makefile` (`install`, `install-service`), `deploy/edge-voice.service` |

**Note on `audio_generation`:** this is a dev/test tool, not a production
package — it simulates the real-world audio source (a phone call leg) by
either capturing the mic or replaying a `.wav` file and publishing it over
MQTT exactly like a real call leg would. It runs as its own **separate
process**, in its own terminal, and is never imported by `cli.py` or
`pipeline/orchestrator.py`. `edge-voice --mic` is a convenience that *spawns*
`mic_source.py` as a subprocess (and SIGINTs it on exit) — it is still a
separate process publishing over MQTT, not in-process capture. The two talk
only through the MQTT broker —
that's deliberate, not a shortcut: `audio_ingest` should never be able to
tell the difference between a simulated leg and a real one, and the only
way to guarantee that is to never let them share a process.

Shared dataclasses (`AudioPacket`, `SpeechSegment`, `TranscriptEvent`) live in
`pipeline/models.py` so that `audio_ingest`, `channel`, `vad`, and `stt` can
all import them without depending on each other directly.

### How the pieces wire together

```text
cli.py
  │  parses args, loads config.settings.Settings
  ▼
pipeline/orchestrator.py
  │  builds workers + queues from Settings, owns startup order
  │  and graceful shutdown (stop-event + join, proven in Milestone 0)
  ▼
pipeline/supervisor.py
  │  watches the worker threads orchestrator handed it, restarts
  │  on unexpected exit or stall, tracks restart counts / degraded status —
  │  doesn't know or care what a "channel router" is, just supervises threads
  ▼
audio_ingest/  →  channel/  →  vad/  →  stt/   (the actual worker threads)
  ▲                                        │
  │  observability/metrics.py samples all of the above on its own tick
  │  (queue depths, per-stage latency, restart budget, MQTT state)
  │                                        ▼
  └── health/reporting.py assembles metrics' latest snapshot + live
      pipeline state per request → webui GET /api/status
```

`utils/audio_generation/` sits entirely outside this tree. It's a separate
process publishing to the same MQTT broker `audio_ingest` subscribes to —
no import relationship in either direction.

**Failure-granularity note for later (Milestone 6):** MQTT
reconnect-with-backoff is a *connection-level* retry that lives inside
`audio_ingest`'s MQTT client itself — it is not a thread restart and should
never go through `supervisor`. `supervisor` only acts on the coarser,
rarer case: a worker thread dying outright from an unhandled exception.
Conflating the two will make restart-count metrics noisy and useless.

---

## STATUS (update this every session, even with one line)

```
Last updated: 2026-08-31
Current milestone: 8 — Testing & CI (partially pre-satisfied; see below)
Done: ms 0, 1, 2, 3, 4, 5, 6, 7
Since ms 7 (not part of any milestone — see "Unplanned work that landed"):
             STT moved to one dedicated decoder per channel, then to one OS
             process per channel (`docs/archived/STT_MULTIPROCESS_PLAN.md`,
             BUILT and verified on the RPi5, 2026-08-11) for genuine
             parallelism — see `ARCHITECTURE.md` §3 Core Decision; soft-cut
             latency fix (vad.soft_cut_enabled, default off — see
             `BENCHMARK.md`); MOONSHINE_ORT_SINGLE_THREAD=1 defaulted in cli
             and the systemd unit; webui console rebuilt as a two-panel
             sidebar+chat layout with static console.js/console.css
             (2026-08-30/31); install.sh device-bootstrap script + reworked
             README install/run docs; partial transcripts
             (vad.partial_interval_s, on by default), rms_gate_enabled
             flipped back on (RPi5 queue clogging), live mic capture over
             MQTT + `edge-voice --mic`, systemd crash-loop protection.
Next action: Milestone 8's remaining gaps — unit tests for `config`
             validation and `observability/logging.py`, and unit tests for
             VAD segmentation / router correctness (today only the
             integration fixture covers those). CI itself already runs
             lint + format + mypy + the default suite on push and PR.
Blocked on: nothing for ms 8. One scoped feature is parked:
             docs/deferred/STREAMING_STT_PLAN.md is on hold deliberately (2026-08-06,
             possibly indefinitely) — benchmarking showed no throughput win,
             see the doc's own status line. docs/deferred/CALL_LIFECYCLE_PLAN.md
             also awaits a decision to start. STT multiprocessing (formerly
             parked here) is done — see above.
```

---

## Milestone 0 — Fake end-to-end pipeline ✅ Done

**Goal:** prove the queue/worker skeleton works before any real audio,
routing, VAD, or STT is involved. Everything in this milestone is fake.

1. `pipeline/models.py`
   - `AudioPacket` (channel_id, timestamp, samples/bytes)
   - `SpeechSegment` (channel_id, start, end, audio)
   - `TranscriptEvent` (channel_id, text, segment_id, timestamps)
2. `pipeline/queues.py`
   - `ingest_queue`, `segment_queue` — bounded, sizes hardcoded for now
     (real config arrives in Milestone 1)
3. `utils/audio_generation/fake_source.py`
   - Pushes synthetic `AudioPacket`s on a timer for two fake `channel_id`s
   - No MQTT yet — pushes straight onto `ingest_queue`
4. `pipeline/fake_workers.py`
   - Fake routing: passes packets through untouched
   - Fake VAD: emits fixed-length fake `SpeechSegment`s
   - Fake STT: emits a canned `TranscriptEvent`
   - *(Removed once real `VADWorker`/`STTWorker` landed in Milestones 3–4 —
     nothing imported it anymore.)*
5. `main.py` wires the fake source + fake workers together, logs
   `TranscriptEvent`s to stdout.

**Done when:** `python main.py` runs for 30s, prints fake transcripts for two
fake channels, exits cleanly on Ctrl-C with no orphaned threads.

---

## Milestone 1 — Real config + cli.py entry point + real audio generation ✅ Done

**Goal:** replace the Milestone-0 throwaway wiring with the permanent
shape — `cli.py → orchestrator → workers` — and get real (non-fake) audio
flowing over MQTT, even though `audio_ingest` doesn't exist to consume it
yet.

1. `src/edge_voice/config/settings.py` — pydantic `Settings` with layered
   config: code defaults → `configs/default.yaml` → `configs/local.yaml`
   (gitignored) → env vars (`EDGE_VOICE__<SECTION>__<FIELD>`). Validation
   on load (e.g. `AudioSettings.format` must be one of `int16`/`int32`/
   `float32`, `WebUISettings.port` in range, `vad.partial_interval_s >= 0`). `_deep_merge()` helper for recursive YAML merging.
2. `src/edge_voice/pipeline/orchestrator.py` — `PipelineOrchestrator` class
   owns the wire shape: constructs `WavSource`/`MicSource` → `FakeRouter` →
   `FakeVADWorker` → `FakeSTTWorker` from `Settings`. Exposes
   `build()`, `start()`, `stop()`, `wait()`, `run()`, `run_with_timer()`,
   `get_status()` (returns `PipelineStatus`), `ingest_queue` property.
   `get_status()` is wired for Milestones 6/7.
3. `src/edge_voice/cli.py` — real entry point: `argparse` flags
   (`--run-secs`, `--debug`), `setup_logging()`, `parse_args()`, `main()`.
   Wired to `Settings.load()` + `PipelineOrchestrator`. Registered as
   `edge-voice` console script in `pyproject.toml` (`[project.scripts]`).
   **Decision recorded (2026-06-29), superseded 2026-07-21:** originally
   planned as a separate process; revisited once the deployment target was
   confirmed as a network-less SBC with a directly attached display — no
   reverse proxy or IPC boundary buys anything when the only client is a
   kiosk browser on the same machine, so the web UI now runs **in-process**
   with `cli.py`/`orchestrator` (see Milestone 5). `--config`/`--channels`/
   `--wav-file`/`--with-ui` flags still don't exist — add them if/when the
   features behind them actually land, not before. The one flag that has
   been added since is `--mic` (2026-08-05), which spawns `mic_source.py`
   as a subprocess.
4. `src/edge_voice/main.py` — its wiring logic moved to `orchestrator.py`,
   and the file itself has since been **deleted**: `python -m edge_voice.cli`
   already covers running without installing, so keeping a second entry point
   only risked the two drifting apart.
5. `src/edge_voice/utils/audio_generation/mic_source.py` — captures from
   the system mic, standalone CLI entry point via `main()`, no import of
   `pipeline`, `cli`, or `orchestrator`. **Completed 2026-08-05:** the MQTT
   publish was a stub print until then; it now captures at the device's
   native rate, resamples to the target, and publishes raw PCM frames —
   same wire format as `wav_source.py`, which is what
   `MqttAudioIngest` consumes.
6. `src/edge_voice/utils/audio_generation/wav_source_json.py` — reads `.wav`,
   resamples, streams at a 20ms real-time pace. Now publishes over MQTT as a
   JSON envelope (`{"samples_b64": ..., "timestamp": ...}`); `wav_source.py`
   is the raw-PCM variant used against the real pipeline, since raw PCM is
   what ingest consumes today. The envelope form is the shape
   `docs/deferred/CALL_LIFECYCLE_PLAN.md` would standardize on.
7. Both `audio_generation` sources verified: import lines contain no
   `pipeline`, `cli`, or `orchestrator` imports.

**Done when:** `edge-voice` console script starts the pipeline using real
`Settings`, AND `wav_source_json.py` (standalone) produces correctly-paced audio
packets, covering resampling, stereo-to-mono, queue-full drop, and custom
configs.

---

## Milestone 2 — Real audio ingestion + channel routing ✅ Done

```text
WavSource process
        |
        | MQTT publish
        v
 MQTT broker
        |
        | MQTT subscribe
        v
audio_ingest/mqtt_client.py
        |
        v
ingest_queue
        |
        v
channel/router.py
        |
        v
PacketCopier
        |---------> routed_queue ------> FakeVAD
        |                                      |
        | dump_queue                           v
        |                               FakeSTT
        v
audio_ingest/audio_dump.py
```

1. `audio_ingest/mqtt_client.py`
   - Subscribes to per-channel MQTT topics
   - Reconnects with exponential backoff (§5) — **this stays internal to
     the MQTT client**, it is not surfaced to `supervisor` as a worker
     restart (see failure-granularity note above)
   - Pushes raw packets onto the shared ingest queue
2. `channel/router.py`
   - Consumes from the ingest queue
   - Tags/validates `channel_id`, maintains per-channel bookkeeping
     (e.g. last-seen timestamp for the freshness check in §7)
   - Hands packets off toward VAD unchanged at this stage — routing logic
     stays separate from VAD logic so each is testable in isolation
3. Swap `utils/audio_generation`'s fake-worker counterparts for real
   `audio_ingest` + `channel` inside `pipeline/orchestrator.py`. VAD/STT
   stay fake.

**Done when:** killing/restarting the MQTT broker connection mid-run
triggers reconnect (inside `audio_ingest`, invisible to `supervisor`)
without crashing the process, and two channels driven by `wav_source.py`
(running in its own process) produce correctly-attributed (still-fake)
transcripts.

---

## Milestone 3 — Real shared Silero VAD ✅ Done

1. `vad/vad_worker.py` — single-threaded, per-channel demuxing worker: one
   `VADIterator` *and one Silero model instance* per `channel_id`, no lock
   needed since calls are serialized by construction.
   - **Diverged from the original plan below:** a single shared model
     protected by a lock was tried first and dropped — `VADIterator` only
     holds the state machine, the LSTM hidden state lives in the model
     itself, so interleaved channels sharing one model instance corrupted
     each other's state (measured as doubled, garbage segment counts on
     the recorded call fixtures). Each channel gets its own model; the
     extra ~4MB/channel is cheap.
   - Soft/hard segment-length cuts (`soft_cut_s`, `max_segment_s`,
     `soft_cut_lookahead_s`, `soft_cut_min_dip`) ported from the prototype,
     gated behind `segment_limits_enabled` (default off — see Milestone 4).
   - `idle_flush_s`: emits an in-progress segment after a channel goes
     quiet with no packets at all, so the final utterance of a stream
     isn't held until shutdown.
2. Swapped fake VAD for `vad/vad_worker.py` inside `pipeline/orchestrator.py`.

**Done when:** Two channels interleaved on the ingest queue produce
correctly segmented, channel-attributed `SpeechSegment`s with no crashes
under concurrent channel activity, verified against real recorded
duplex-call fixtures (`wav/rx_recorded_1.wav`, `wav/tx_recorded_1.wav`) —
see `tests/test_pipeline_integration.py`.

---

## Milestone 4 — Real shared Moonshine STT ✅ Done

**Reversed since — see `ARCHITECTURE.md` §3 Core Decision.** The "one shared
`Transcriber`" design below didn't survive contact with the RPi5: it couldn't
sustain real-time dual-channel throughput, so STT moved to one decoder per
channel, then to one OS process per channel
(`docs/archived/STT_MULTIPROCESS_PLAN.md`, built 2026-08-11). Kept here for
the original (now-superseded) reasoning, same as Milestone 3's own reversal
note below it.

1. `stt/stt_worker.py`
   - One shared `Transcriber` across *all* channels, not one per channel —
     `start()`/`stop()` fully resets Moonshine's decoder state (verified
     byte-for-byte against a fresh instance per segment), and `STTWorker`
     only ever handles one segment at a time regardless of channel, so
     there's no concurrency to isolate. Halves the memory footprint
     (~175MB/channel not held open) and matches the turn-taking nature of
     the audio.
   - Language + model arch configurable (`STTSettings.language`,
     `STTSettings.model_arch`). **Superseded 2026-08-04** by one
     `add_audio()` call per segment instead of windowed feeding — see
     "Unplanned work that landed" below for the numbers.
   - Repetitive-output guard: falls back to the best partial line when the
     decoder loops on itself (beam-search collapse at awkward boundaries)
2. Swapped fake STT for `stt/stt_worker.py` inside `pipeline/orchestrator.py`.
   Full pipeline is now real, end to end: `audio_generation` (separate
   process, test-only) → `audio_ingest` → `channel` → `vad` → `stt`.

**Done when:** a real two-channel `.wav`/MQTT fixture (via `wav_source.py`,
its own process) produces correct Korean transcripts in order, attributed
to the right channel. Verified against `wav/rx_recorded_1.wav` +
`wav/tx_recorded_1.wav`.

---

## Milestone 5 — Web UI ✅ Done (config editor deferred)

**Moved ahead of Reliability/Observability (2026-07-21):** the deployment
target is an SBC (RPi5-class) with no network at all, but with a display
attached — so the UI's only client is a kiosk-mode browser on the same
machine. That removed the reason to wait for Milestones 6/7 first: the UI
is a consumer of the `get_status()` seam that's existed since Milestone 1,
and it displays whatever that seam returns today — it'll show richer data
automatically once Milestones 6/7 add it, no UI rework needed either time.

1. FastAPI app — **`src/edge_voice/webui/app.py`** (not `tool/webui/`,
   the path floated when this milestone was only planned; `webui/` sits
   alongside `vad/`, `stt/`, `channel/` etc. as a top-level package, since
   unlike `utils/audio_generation/` it's not a dev-only tool). Served on
   `127.0.0.1` only (`WebUISettings.host`, was `0.0.0.0`). **Runs in-process
   with `cli.py`/`orchestrator`** — supersedes the separate-process decision
   recorded in Milestone 1. `cli.py main()`: the `--run-secs` path stays
   headless/no-UI (used by `tests/test_pipeline_integration.py`, which
   shouldn't need a port); the default (Ctrl-C) path now does
   `orchestrator.build()` + `start()`, then blocks in `uvicorn.run(app, ...)`
   instead of `orchestrator.run()`'s own wait loop, then `stop()` + `wait()`
   in a `finally` once uvicorn returns — verified by hand that both a plain
   Ctrl-C (SIGINT) and a `timeout`-style SIGTERM drain all workers and log a
   final `{running: false, ...}` status before the process exits.
2. Live transcript stream over **SSE** (`StreamingResponse`), not WebSocket
   — one-directional (server → browser), so SSE avoids WebSocket's
   handshake/framing for a channel nothing pushes back on. New
   `pipeline/transcript_hub.py`: `TranscriptHub` is a small N-subscriber
   pub/sub (same drop-and-log-on-`queue.Full` philosophy as `fanout_put`,
   but a dedicated type — `fanout_put` itself is fixed to one-or-two
   destinations, not a dynamic per-connection set). `orchestrator._on_transcript`
   publishes to it alongside the existing log line; `orchestrator.transcripts`
   exposes it. `subscribe()` pre-seeds the new queue with the recent backlog
   (`WebUISettings.transcript_backlog`, default 50) so a kiosk reload isn't
   blank while waiting for the next segment — a single queue shared across
   reconnects was rejected for the reason recorded here originally: it either
   drops everything published while a client was detached, or hands a stale
   backlog to whichever client reconnects first.
3. Control: `POST /api/start` / `POST /api/stop` call straight into
   `orchestrator.start()`/`stop()`/`wait()` (run via `run_in_threadpool` —
   `stop()` really does block on joining threads, so it can't run on the
   event-loop thread). No UI buttons wired to these yet — only the transcript
   feed and status pill were asked for this round. "Restart a single worker"
   stays out of scope until `pipeline/supervisor.py` exists (Milestone 6).
4. Status: `GET /api/status` is a plain passthrough of
   `orchestrator.get_status()`, polled by the page every 3s — current state,
   not a stream, so no queue. Drives the header's live/stopped pill. Same
   endpoint gets restart counts/degraded flags for free once Milestone 6
   lands, and the fuller health/metrics object once Milestone 7 lands.
5. **Deferred, not built this round:** config view/edit/validate. Add when
   there's an actual need to change config without shell access.
6. **No MQTT anywhere in this milestone**, as intended — all UI ↔
   orchestrator data flow is in-process (`TranscriptHub` for transcripts,
   direct calls for status/control).

**Visual design:** console/teleprinter identity, not a generic chat app —
see `webui/templates/console.html`. Single committed dark theme (no
light-mode variant): deliberate, since this runs on one dedicated always-on
kiosk display with no OS theme to defer to, not an oversight. One monospace
family throughout (hierarchy via size/weight/tracking, not a second
typeface) — ties directly to the subject: a live speech-to-text feed is a
modern teleprinter. Two functional channel hues instead of a decorative
accent: `tx` (local/outgoing) amber `#FFB454`, `rx` (remote/incoming) cyan
`#4DD8C4`, kept separate from the semantic `live`/`stopped` status color.
Messages render as squared, LED-dot-tagged bubbles — rx left, tx right —
not rounded chat cards. A prototype with staged sample dialogue was reviewed
and approved before wiring in real data.

**Done when:** on the device's attached display, a kiosk browser pointed at
`localhost` shows live transcripts as they're produced (verified: SSE
delivery, multi-subscriber fan-out, and backlog replay on connect all
covered in `tests/test_webui_app.py` / `tests/test_transcript_hub.py`, plus
a manual run against a live local pipeline) and shows the pipeline's
running/stopped state. Start/stop reachable via API, not yet from the page
itself; config editing not built.

---

## Milestone 6 — Reliability ✅ Done (watchdog + power-loss checks verify on-device)

**Shipped:** `pipeline/supervisor.py` (generic thread-watchdog: crash + stall
detection, windowed restart budget → degraded, VAD pending-loss logging),
`pipeline/systemd_watchdog.py` (dependency-free sd_notify, no-op off systemd),
`ReliabilitySettings` + `configs/default.yaml` block, orchestrator wiring
(supervisor starts last / stops first; `get_status()` now carries per-worker
state + a top-level `degraded`), the third **degraded** pill state in
`console.html`, atomic writes for both dump workers (`audio_ingest/atomic_write.py`),
and `deploy/edge-voice.service`. Tests: `test_supervisor.py`,
`test_systemd_watchdog.py`, `test_atomic_write.py`, plus orchestrator restart-
mechanics tests; the end-to-end `-m integration` run passes unchanged with
supervision on.

**Two acceptance checks remain on-device** (can't run in CI, same as Milestone
8's perf validation): the watchdog actually restarting a *hung* process
(`systemctl kill -s SIGSTOP`), and a real power-cut leaving the previous dump
WAV intact. The `.service` file documents both procedures.

**Design rationale, condensed — full detail in `RELIABILITY.md`:** the app
runs unattended on a no-internet edge box, so recovery has to be automatic.
Two independent layers, because each catches a failure the other structurally
cannot — in-process supervision (crash + stall detection via a per-worker
last-activity timestamp, `VADWorker`'s in-progress-segment loss logged
explicitly rather than silently dropped, degraded state after a restart
budget is exhausted) can't rescue a deadlock, a hang inside a native call, or
an OOM; the OS-level watchdog underneath (`sd_notify`, ticking on the
supervisor's own thread so a slow worker-rebuild can't delay the heartbeat)
is what catches that case, restarting the whole unit via systemd. Fault
isolation (malformed packet / inference exception → log + drop, never kill
the worker loop) and atomic writes for the two dump workers round out the
shipped scope. One control was scoped but never built: a manual "restart
worker" UI action — not needed, since automatic restarts plus the systemd
layer have covered every failure seen so far.

**Done when:**
- Deliberately raising inside `stt/worker.py` mid-run gets logged, the
  worker restarts via `supervisor`, the pipeline keeps transcribing,
  `orchestrator.get_status()` reflects the restart, and that restart is
  visible on the kiosk UI as a distinct "degraded" state (not
  indistinguishable from "live").
- `kill -9` on the whole process (simulating a hang layer 1 can't catch)
  results in systemd restarting it within `WatchdogSec`, no manual
  intervention.
- Pulling power mid-write to a segment-dump WAV file leaves the previous
  file valid and doesn't affect the pipeline on next boot.

---

## Milestone 7 — Observability + Health ✅ Done

**Shipped:** `observability/logging.py` (JSON formatter + merging stage
adapter + `configure_logging`), `observability/metrics.py` (`MetricsCollector`
thread), `health/reporting.py` (`build_health_report`), the four plumbing
prerequisites below, `webui`'s `/api/status` repointed to
`orchestrator.health()` with `console.html` rendering the new payload, and
`LoggingSettings`/`MetricsSettings`/`HealthSettings` all actually consumed —
no dead config stubs remain. Tests: `test_metrics.py`,
`test_health_reporting.py`, plus updates to `test_orchestrator.py`/
`test_webui_app.py` for the richer shape. Reference docs: `HEALTH.md`.

**Delivered beyond the plan below** (worth knowing, since the item text
doesn't mention them):

- **A file sink alongside the console one**, each independently toggleable,
  plus a `logging.enabled` master switch that `logging.disable()`s the
  process for runs where logging overhead itself matters. The plan had
  explicitly rejected a second sink (see the 2026-07-28 correction); the
  correction stands for *format selection* — what changed is that a
  headless kiosk with no terminal attached needs a durable local sink, so
  the file handler is always JSON regardless of the console's format.
- `ensure_ascii=False` in the JSON formatter — the default escaped every
  Korean transcript into `\uXXXX`.
- **Per-channel latency for VAD (Silero vs. RMS-gate path), router
  re-packetize, and STT**, not just the single scalar STT latency the plan
  called for — with `MetricsSettings.latency_log_decimals` controlling log
  rounding only, never the snapshot.
- Per-segment STT latency on the `TRANSCRIPT` log line itself
  (`stt_latency_s`), so a single segment's cost is greppable without
  correlating against a metrics tick.
- `Supervisor`'s own thread liveness in `get_status()` — nothing supervises
  the supervisor, so its death would otherwise freeze restart/degraded state
  silently rather than reporting it.
- `console.html` module chips (per-stage state + worst-channel latency) and
  the status strip, which is more than the "render at least one of each"
  the Done-when asked for.

**Two decisions recorded while building** (full rationale now in
`HEALTH.md`'s "Why health is its own thing"): health is query-driven, not a
tick-driven pusher like `metrics.py`/`supervisor.py`, which is why it takes
plain values rather than their callables; and the payload deliberately omits
the snapshot's per-worker `state` (already in `workers`, one tick fresher)
and the scalar `stt_last_latency_s` (the per-channel `stt` map is already
there, and the two legitimately disagree).

**Starting point, not a blank slate:** `config/settings.py` already has two
stubs anticipating this milestone that nothing reads today —
`LoggingSettings.is_json` (default `True`) and
`HealthSettings.stale_segment_warning_s`. `cli.py`'s `setup_logging()`
ignores `Settings.logging` entirely; nothing constructs a `HealthSettings`
consumer. This milestone wires those up rather than adding new config
sections for logging format / staleness threshold.

**Decisions recorded (2026-07-27):**
- **One console handler, not dual-sink — corrected 2026-07-28.** The
  original plan here was pretty-text-always-on-console plus a second JSON
  file sink gated by `is_json`. Building it surfaced that
  `configs/default.yaml` already documents different intent for the field
  that predates this milestone: `logging: json: true  # false -> pretty
  console renderer (dev)`. That's a single handler whose *formatter*
  `is_json` picks — JSON by default, pretty text only when a dev sets
  `logging.json: false` in `configs/local.yaml`. No `json_path`, no file
  handler — `observability/logging.py`'s `configure_logging()` just swaps
  the one `StreamHandler`'s formatter.
- **`segment_id` is minted at `start`, not at finalize.** Today
  `VADWorker._finalize_segment` is the only place a `segment_id` is
  generated (vad_worker.py:349), so every event before finalization —
  packet ingestion, routing, VAD triggering — has no id to log, only
  `channel_id`. To satisfy "trace one segment's full lifecycle... by
  `segment_id`," `_ChannelState` gains a `segment_id: str | None` field,
  assigned by a small `_new_segment_id(channel_id, state)` helper called
  both where `triggered` flips true (the `start` branch in
  `_handle_packet`) and where `_cut_segment` re-seeds the tail into a new
  in-progress segment (a soft/hard cut is genuinely a new segment starting,
  not a continuation, so it gets a fresh id there too — the emitted
  `SpeechSegment.segment_id` for the cut piece keeps its original id;
  the carried tail gets a new one). `_finalize_segment` reads
  `state.segment_id` instead of generating one.
- **Per-channel freshness uses `ChannelRouter.get_freshness()` only**, not
  `VADWorker`'s internal `last_packet_at` — full rationale (two different
  clocks answering different questions) now in `ARCHITECTURE.md` §7.
- **STT latency means pure inference time**, not queue-to-transcript —
  queue depth already answers "is there a backlog"; full rationale now in
  `ARCHITECTURE.md` §7 Metrics.

**New plumbing this milestone required** (all four landed):
1. ✅ `orchestrator.py` — a `queue_depths() -> dict[str, int]` method calling
   `.qsize()` on `_ingest_queue`/`_routed_queue`/`_segment_queue`/
   `_dump_queue`/`_segment_dump_queue`. Only `ingest_queue` has a public
   property today (orchestrator.py:66); the other four are private.
2. ✅ `audio_ingest/mqtt_client.py` — a public `connected` property reading
   the existing `_connected_event` (already set/cleared in
   `_on_connect`/`_on_disconnect`; just never exposed), same shape as the
   existing `stopping`/`last_activity` properties. **Health reporting must
   treat this as optional**: `orchestrator._audio_source` can be a
   `WavSource`/`MicSource` with no MQTT involved at all (dev/test runs) —
   report MQTT connectivity only when the configured source actually has
   a `connected` attribute, not as a hard requirement.
3. ✅ `pipeline/supervisor.py` — expose `max_restarts`/`restart_window_s` (or
   a combined ratio) so health/metrics can report "3/3 restarts used,"
   not just the bare count `status()` already returns.
4. ✅ `vad/vad_worker.py` — `_ChannelState.segment_id` field + the
   `_new_segment_id` helper described above (this is also new plumbing,
   not just a logging change — anything reading VAD state externally in
   the future benefits too).

1. ✅ `observability/logging.py` — `JsonFormatter` (stdlib `logging`, no
   new dependency), picked by `configure_logging()` based on
   `LoggingSettings.is_json` (see decision above); `cli.py` now loads
   `Settings` before configuring logging (was the other way around) so it
   can read `settings.logging_`. `get_stage_logger(name, stage)` gives
   each pipeline-stage module a `LoggerAdapter` that tags every record
   with `stage`, merging in any `channel_id`/`segment_id` a call site adds
   via `extra={}` (a custom `_MergingAdapter` — the stdlib default
   overwrites instead of merging). Wired into `audio_ingest` (all three
   modules), `channel/router.py`, `vad/vad_worker.py`, `stt/stt_worker.py`
   — the segment-lifecycle path, ~30 of the ~71 existing `logger.*` call
   sites, each given `channel_id`/`segment_id` where one was actually in
   scope at that call site. Plus one call site outside that path:
   `orchestrator.py`'s `_on_transcript` log line, since it's the segment
   lifecycle's terminal "→ transcript" event even though it's physically
   defined in `pipeline/`. `pipeline/supervisor.py`, `webui`, `config`,
   `utils` logs stay plain otherwise: worker/infra-level events, not
   per-segment ones.
2. ✅ `observability/metrics.py` — in-memory aggregation of STT latency (see
   above), queue depth (via `orchestrator.queue_depths()`), restart counts
   + budget (via the new `Supervisor` accessor), MQTT status (via the new
   `connected` property, when present). Runs as its own worker thread
   (`threading.Thread`, same shape as `VADWorker`/`Supervisor`), **not**
   folded into `Supervisor`'s tick — `Supervisor` is deliberately generic
   ("a thread died, restart it," per the package-map note above) and has
   no business knowing about STT latency or MQTT. New `MetricsSettings`
   (`enabled: bool`, `emit_interval_s: float`, mirroring
   `ReliabilitySettings.tick_interval_s`) controls its cadence; on each
   tick it logs one aggregated snapshot as a structured event (no
   Prometheus, per the existing out-of-scope note) and keeps the latest
   snapshot in memory for `health/reporting.py` to read directly (no need
   to round-trip through logs for that).
3. ✅ `health/reporting.py` — health object: overall status + per-worker
   state (from `orchestrator.get_status()`, unchanged — don't re-derive),
   queue depths + MQTT connectivity + restart budget (from
   `metrics.py`'s latest snapshot), per-channel freshness (from
   `ChannelRouter.get_freshness()` for each `get_channel_ids()`, flagged
   stale past `HealthSettings.stale_segment_warning_s` — the other
   currently-dead settings stub this milestone wires up).
4. ✅ `webui/app.py`'s `/api/status` returns this richer object instead of
   the bare `orchestrator.get_status()` passthrough it is today — as a
   superset of the current `{running, degraded, workers}` shape so nothing
   already reading those three fields breaks. **This is its own scope
   item, not a free repoint**: `console.html`'s `setStatus()` currently
   only branches on `.running`/`.degraded` for the one status pill: it
   needs new DOM/JS to actually render queue depths, MQTT state, and
   per-channel freshness, not just a richer payload nobody looks at.

**Still-open test gap from this milestone** (carries into Milestone 8):
`test_observability_logging.py` was never written, so the JSON formatter and
stage adapter are covered only incidentally by everything that logs.

**Done when:**
- Deliberately tracing one segment through a live run — `audio_ingest`
  receives its first packet, `channel` routes it, `vad` triggers/finalizes
  it, `stt` transcribes it — can be reconstructed from the JSON log sink
  alone by filtering on one `segment_id`, from VAD trigger onward (the
  provisional id assigned at `start` covers this; pre-VAD-trigger packet
  logs are necessarily `channel_id`-only, since no segment exists yet).
- `GET /api/status` reports queue depths, MQTT connectivity (when the
  audio source is MQTT-based), restart budget, and per-channel freshness
  — and the kiosk UI visibly renders at least one of each (not just
  carries it in the JSON unused).
- Killing the MQTT broker mid-run and letting a worker restart both show
  up in the health object within one `MetricsSettings.emit_interval_s`
  tick, without needing to grep logs to notice.

---

## Milestone 8 — Testing & CI (partially pre-satisfied)

Most of this arrived incrementally alongside Milestones 5–7 rather than as
its own pass. What's left is a short list of real gaps, not a from-scratch
milestone — the honest framing is "close the gaps," not "build testing."

1. Unit tests: `channel` routing, `vad` segmentation logic, `config`
   validation, `pipeline/supervisor.py` restart behavior in isolation
   (kill a fake thread, assert it restarts)
   - ✅ `pipeline/supervisor.py` — `test_supervisor.py` (crash, stall,
     degrade, watchdog ping), all driven with fake workers.
   - ⚠️ `channel` / `vad` — `test_router.py` and `test_vad_worker.py` exist
     but cover **latency accounting and channel bookkeeping**, not
     segmentation boundaries or routing correctness. Those are still only
     verified through the integration fixture in item 2. This is the main
     remaining gap.
   - ❌ `config` validation — no `test_settings.py`; the layered
     defaults → YAML → local → env merge and its validators are exercised
     only incidentally.
   - ❌ `observability/logging.py` — planned in Milestone 7's test list,
     never written (see the note there).
   - ✅ Not in the original list but shipped: `test_metrics.py`,
     `test_health_reporting.py`, `test_partial_transcripts.py`,
     `test_transcript_hub.py`, `test_webui_app.py`, `test_mqtt_client.py`,
     `test_atomic_write.py`, `test_systemd_watchdog.py`,
     `test_stt_worker.py`, `test_models.py`, `test_orchestrator.py`.
2. Integration tests:
   - `audio_generation` (wav_source, its own process) → `audio_ingest` →
     `channel` → `vad`
   - Full end-to-end fixture through `stt`
   - ✅ Both live in `test_pipeline_integration.py`, marked `integration`
     and excluded from the default run (`addopts = ["-m", "not integration"]`)
     since they need a live broker. Run with `pytest -m integration`.
3. CI workflow running both (perf validation stays manual, on-device)
   - ✅ `.github/workflows/ci.yml` runs on push to `main` and on every PR:
     `ruff check`, `ruff format --check`, `mypy --package edge_voice`,
     `pytest` — installing from `requirements-dev.txt` plus
     `portaudio19-dev`.
   - ❌ The integration job is **not** in CI (no broker there). Deciding
     whether to stand up a `mosquitto` service container, or to leave these
     as a deliberate on-device-only check like the Milestone 6 watchdog
     verification, is the open call.

**Done when:** CI is green on a clean clone with no manual setup beyond
`pip install` from the lockfile *(already true)*, and the three ❌/⚠️ gaps
above are either closed or explicitly recorded as won't-do.

---

## Unplanned work that landed after Milestone 7

Not part of any milestone — recorded here so the milestone list doesn't read
as the complete history of the project.

**Latency / quality**

- **Partial transcripts** (`vad.partial_interval_s`, PR #13, on by default).
  VAD emits the in-progress prefix of a segment on a cadence so text appears
  before the speaker stops — the standing workaround for having no streaming
  model. Droppable under backlog, excluded from latency metrics and audio
  dumps, carrying the eventual final's `segment_id` so the UI replaces in
  place. Tuning measurements are in `configs/default.yaml`.
- **One `add_audio()` per segment**, replacing windowed feeding: ~2.7x faster
  on real audio and it removed word duplication at window boundaries.
- **Eager model construction** for both VAD (per channel, issue #10) and STT
  (PR #12) — model loads moved off the real-time path into `__init__`, where
  they can't masquerade as a stall.
- **ONNX Runtime backend confirmation** logged once at startup, so "is this
  actually running on ORT" stops being a guess.
- `rms_gate_enabled` **back on by default**: the gate skips Silero inference
  on silent frames, which on an RPi5 is what keeps the routed queue from
  clogging. This supersedes the earlier decision to leave it off.
- `min_silence_duration_ms` reduced, and the segment-limit defaults retuned.

**Web UI**

- Clear button (`POST /api/transcripts/clear`) to wipe transcript history.
- rx/tx freshness labels clarified, legends consolidated into their own
  static bar — the kiosk has no mouse, so nothing may rely on hover.

**Audio sources / ops**

- `mic_source.py` now genuinely captures and publishes over MQTT (it was a
  stub print since Milestone 1), capturing at the device's native rate and
  resampling to the target.
- `edge-voice --mic` spawns it as a subprocess for convenience — still a
  separate process over MQTT, not in-process capture.
- `make install` now installs `mosquitto` + `mosquitto-clients` +
  `libportaudio2` via apt; `make install-service` deploys/restarts the
  systemd unit.
- systemd **crash-loop protection** (`StartLimitIntervalSec=300s`,
  `StartLimitBurst=5`): one detect-restart cycle already exceeds systemd's
  10s default window, so without widening it the default burst limit could
  never trip and a deterministic hang would restart forever.

---

## Parked — scoped, not started

Both have their own design docs. Neither is blocked on anything in the
milestones above.

- **`docs/deferred/CALL_LIFECYCLE_PLAN.md`** — call-start/call-end signals over MQTT
  (reset the pipeline, clear the UI per call) and JSON-wrapped audio payloads.
  The hard part is already identified: a race where in-flight audio from the
  old call drains through the queues *after* the UI clears. Also resolves the
  deferred `VADWorker.reset_channel()` question — plain reset on call-start,
  flush-then-reset on call-end — and the re-packetizer's silent timestamp
  drift across a discontinuity. Note the wire format is currently split:
  `wav_source_json.py` publishes a JSON envelope, while `wav_source.py` and
  `mic_source.py` publish raw PCM, which is all `MqttAudioIngest` consumes
  today.
- **`docs/deferred/STREAMING_STT_PLAN.md`** — **on hold (2026-08-06), deliberately, no
  committed timeline to resume.** Scoped and fully benchmarked, not merely
  unstarted: streaming turned out **not** to be a speed win — measured at
  3.3×–10.7× the compute of non-streaming segment decode, because every
  `update_interval` re-decodes the current line from scratch. It buys
  incremental text during an utterance (replacing the partial-transcript
  mechanism), not throughput, and the current VAD-bounded short-segment design
  is already close to the cheapest approach available. If resumed: the first
  deliverable would be a `(language, arch)` validation layer
  (`stt/model_registry.py`) rejecting an unsupported pair at startup instead of
  a `ValueError` from inside a worker thread — independent of streaming and
  useful on its own. Whether two streaming channels fit on a Pi is unmeasured
  and would decide whether the rest proceeds. English test audio is no longer
  missing (`wav/obama_2012.wav`), though it's a studio-quality monologue, not
  two-party telephone audio.

(STT multiprocessing was parked here previously — it's done; see "Unplanned
work that landed" above and `ARCHITECTURE.md` §3.)

---

## Out of scope reminders (don't accidentally build these)

Docker packaging, Prometheus/Grafana, multi-tenant deployments, speaker
diarization beyond channel attribution, transcript persistence beyond
logs/live streaming.