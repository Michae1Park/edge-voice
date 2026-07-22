# edge-voice Build Plan

**Companion to:** `ARCHITECTURE.md` (v0.1)
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
| Observability                | `observability/` *(planned — Milestone 7, not yet built)* |
| Health                       | `health/` *(planned — Milestone 7, not yet built)* |

**Note on `audio_generation`:** this is a dev/test tool, not a production
package — it simulates the real-world audio source (a phone call leg) by
either capturing the mic or replaying a `.wav` file and publishing it over
MQTT exactly like a real call leg would. It runs as its own **separate
process**, in its own terminal, and is never imported by `cli.py` or
`pipeline/orchestrator.py`. The two talk only through the MQTT broker —
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
  │  on unexpected exit, tracks restart counts / degraded status —
  │  doesn't know or care what a "channel router" is, just supervises threads
  ▼
audio_ingest/  →  channel/  →  vad/  →  stt/   (the actual worker threads)
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
Last updated: 2026-07-22
Current milestone: none
Done: ms 0, 1, 2, 3, 4, 5, 6
In progress: none
Next action: Milestone 7 — Observability + Health (observability/, health/)
Blocked on: on-device verification of the watchdog + power-loss cases (see
            Milestone 6 "Done when" -- the two on-box checks can't run in CI)
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
   on load (e.g. `AudioSettings.format` must be `"int16"`, `STTSettings.feed_windows > 0`,
   `WebUISettings.port > 0`). `_deep_merge()` helper for recursive YAML merging.
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
   `--wav-file`/`--with-ui` flags still don't exist yet — add them if/when
   the features behind them actually land, not before.
4. `src/edge_voice/main.py` — still exists but its wiring logic moved to
   `orchestrator.py`. Kept as dev convenience for running without installing.
5. `src/edge_voice/utils/audio_generation/mic_source.py` — `MicSource`
   class captures from system mic via pyaudio, publishes `AudioPacket`s
   over MQTT (MQTT publish not yet implemented — prints stub), standalone
   CLI entry point via `main()`, no import of `pipeline`, `cli`, or
   `orchestrator`.
6. `src/edge_voice/utils/audio_generation/wav_source.py` — `WavSource`
   class reads `.wav` via `soundfile`, resamples via `torchaudio`, streams
   at 20ms real-time pace to the ingest queue. No MQTT publish yet —
   pushes to in-memory queue.
7. Both `audio_generation` sources verified: import lines contain no
   `pipeline`, `cli`, or `orchestrator` imports.

**Done when:** `edge-voice` console script starts the pipeline using real
`Settings`, AND `wav_source.py` (standalone) produces correctly-paced audio
packets, covering resampling, stereo-to-mono, queue-full drop, and custom
configs.

---

## Milestone 2 — Real audio ingestion + channel routing ✅ Done

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

1. `stt/stt_worker.py`
   - One shared `Transcriber` across *all* channels, not one per channel —
     `start()`/`stop()` fully resets Moonshine's decoder state (verified
     byte-for-byte against a fresh instance per segment), and `STTWorker`
     only ever handles one segment at a time regardless of channel, so
     there's no concurrency to isolate. Halves the memory footprint
     (~175MB/channel not held open) and matches the turn-taking nature of
     the audio.
   - `feed_windows=64`, language + model arch configurable
     (`STTSettings.language`, `STTSettings.model_arch`)
   - Repetitive-output guard: falls back to the best partial line when the
     decoder loops on itself (beam-search collapse at awkward boundaries)
2. Swapped fake STT for `stt/stt_worker.py` inside `pipeline/orchestrator.py`.
   Full pipeline is now real, end to end: `audio_generation` (separate
   process, test-only) → `audio_ingest` → `channel` → `vad` → `stt`.

**Done when:** a real two-channel `.wav`/MQTT fixture (via `wav_source_raw.py`,
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

**Runs unattended on a no-internet edge box — no one is coming to SSH in and
restart it.** That constraint means two independent layers, because each
catches a failure mode the other structurally cannot:

- **In-process supervision** (1–2) only works if the process is still
  scheduling threads at all. It cannot rescue a deadlock, a hang inside a
  native call (torch/silero/moonshine), or an OOM — the supervisor is
  wedged right along with everything else in that case.
- **OS-level watchdog** (3) is the layer underneath that catches exactly
  that case, restarting the whole process from outside it.

1. `pipeline/supervisor.py` — restarts `audio_ingest`/`channel`/`vad`/`stt`
   worker threads on unexpected exit, tracks restart counts, flags
   "degraded" after N repeated failures within a window (§5). Must
   distinguish an intentional `stop_event`-triggered exit (from
   `orchestrator.stop()`) from a genuine crash — only the latter restarts.
   Once a worker is flagged degraded, stop hot-restarting it in-process —
   repeated restarts without backoff just burn CPU on a constrained board —
   and let layer 3 below (a full process restart) be the recovery path
   instead. `orchestrator.py` builds the workers and hands them to
   `supervisor.py` to watch — `supervisor` itself stays generic ("a thread
   died, restart it") rather than knowing what a VAD worker is.
   - **Also covers stalls, not just exits.** Exit-based detection alone
     misses a worker that deadlocks or blocks forever without crashing —
     that's invisible to both this layer (nothing exits) and layer 3 below
     (the rest of the process, including the watchdog heartbeat thread,
     keeps ticking fine). Each worker exposes a last-activity timestamp
     (updated once per packet/segment handled); `supervisor` polls it
     alongside `is_alive()` and treats "no activity for M seconds while
     upstream is still feeding it work" the same as an exit.
   - **Restarting `VADWorker` loses whatever segment was in progress** for
     the channel that was active — full recovery isn't realistically
     possible from a thread that just crashed unpredictably (its internal
     state is already suspect). Instead of losing this silently, before
     discarding the dead instance, inspect its `_channels` for any
     non-empty `segment_chunks` and log the loss explicitly (channel,
     seconds of audio) as its own distinct event — not folded into the
     generic "worker restarted" log line — so a crash that ate a live
     utterance is auditable after the fact, the same way the Milestone 3
     fixture work made channel state corruption auditable via segment
     counts.
2. Fault isolation: malformed packet / inference exception → log + drop,
   never kill the worker loop. **Largely already true** —
   `VADWorker.run()` and `STTWorker.run()` already wrap per-item handling
   in `try/except Exception: logger.exception(...)` and continue; this
   item is now an audit to confirm `MqttAudioIngest`/`ChannelRouter` have
   the same guard, not new code.
3. OS-level watchdog (systemd `WatchdogSec=`, or a hardware watchdog if the
   board has one): the app calls `sd_notify("WATCHDOG=1")` periodically. If
   the process hangs, deadlocks, or is OOM-killed — none of which item 1
   can detect from inside the same wedged process — systemd restarts it.
   This is the layer that actually delivers "restarts itself with nobody
   watching." **Lives on `Supervisor`'s own tick, not the UI's poll cadence**
   — `Supervisor` is itself a `threading.Thread` (same shape as
   `VADWorker`/`STTWorker`), started/stopped by `orchestrator.start()`/
   `stop()` like any other worker, so it has a consistent home whether
   `cli.py` is running headless (`run_with_timer()`) or hosting the kiosk
   UI (blocks in `uvicorn.run()` instead — neither path ticks the other).
   The ping must not share a code path with the (slower) worker-rebuild
   work in item 1 — a slow model reload delaying the ping could trigger a
   spurious watchdog restart on top of an already-in-progress one.
4. Flesh out the `get_status()` seam stubbed in Milestone 1 so it reports
   real per-worker state (running/restarting/degraded) sourced from
   `supervisor`, not from grepping logs.
5. Kiosk pill gets a third **degraded** state, distinct from live/stopped,
   sourced from item 4. **Correction to the Milestone 5 assumption** that
   the status panel "picks this up automatically" — checked
   `console.html`: `setRunning()` only branches on the boolean `.running`,
   so a degraded-but-still-running pipeline today renders identically to a
   fully healthy one. Deliberately scoped to just this one pill state, not
   a general fallback-screen system — this app has no sensors or
   peripherals to show fallback states for, only the pipeline itself.
6. Atomic writes for the two local file writers that run continuously by
   default on a power-loss-prone device — `SegmentAudioDumpWorker`
   (enabled by default) and `AudioDumpWorker` (opt-in) both call
   `sf.write()` straight to the destination path. Write to a temp path in
   the same directory and `os.replace()` onto the final filename instead,
   so a power cut mid-write leaves the previous file intact rather than a
   torn WAV. Deliberately **not** a database/WAL layer — there's no
   database anywhere in this app, and transcript persistence is already
   out of scope (see bottom of this doc); these two debug dump workers are
   the only continuous local writes that exist, and losing one in-flight
   file is already contained to that one file, not a shared store.
7. Wire up the "restart worker" control left out of scope in Milestone 5,
   now that `supervisor.py` exists for it to call into. Lower priority
   than 1–6 — the point of this milestone is *not* needing a human at the
   console — keep only if a manual override is still wanted for debugging.

**Scope, sized against `test_orchestrator.py` (236 lines) as the closest
existing precedent** — comparable to a full earlier milestone (VAD or STT),
not a small patch:

| Piece | Scope |
|---|---|
| `pipeline/supervisor.py` (new) | Largest, most novel piece — thread lifecycle, per-worker restart/backoff/degraded tracking, liveness polling, the VAD-loss logging special case |
| `orchestrator.py` changes | Moderate — touches the shutdown-ordering logic that was already subtly buggy once (Milestone 4/PR #7), so needs care, not just volume |
| Small additions to 4 worker files | Small each — a last-activity timestamp stamp per item handled |
| systemd unit + `sd_notify` helper | Small code (stdlib socket write), but **can't be fully verified off-device** — the socket call is unit-testable here; "`kill -9` → systemd actually restarts it" only proves out on the real box, same as Milestone 8 already treats perf validation (manual, on-device) |
| `console.html` | Small — one CSS class, one JS branch |
| Atomic writes (2 dump workers) | Small — one shared helper, two call sites |
| Tests | The other major chunk — a new `test_supervisor.py` in the same range as `test_orchestrator.py`, plus updates to the existing orchestrator/status tests |

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

## Milestone 7 — Observability + Health

1. `observability/logging.py` — structured JSON logs with `channel_id`,
   pipeline stage, `segment_id` on every relevant event
2. `observability/metrics.py` — in-memory aggregation of STT latency, queue
   depth, restart counts, MQTT status, emitted as log events (no Prometheus)
3. `health/reporting.py` — health object: overall status, per-worker
   status, queue depths, MQTT connectivity, per-channel activity freshness.
   Sources worker/restart status from `orchestrator.get_status()` rather
   than re-deriving it.
4. Point the Milestone 5 status panel at `health/reporting.py` and
   `observability/metrics.py` instead of the bare `get_status()` shape it
   started with.

**Done when:** you can trace one segment's full lifecycle (`audio_ingest` →
`channel` → `vad` → `stt` → transcript) through logs alone, by `segment_id`
— and the Milestone 5 UI's status panel reflects the same health/metrics
data.

---

## Milestone 8 — Testing & CI

1. Unit tests: `channel` routing, `vad` segmentation logic, `config`
   validation, `pipeline/supervisor.py` restart behavior in isolation
   (kill a fake thread, assert it restarts)
2. Integration tests:
   - `audio_generation` (wav_source, its own process) → `audio_ingest` →
     `channel` → `vad`
   - Full end-to-end fixture through `stt`
3. CI workflow running both (perf validation stays manual, on-device)

**Done when:** CI is green on a clean clone with no manual setup beyond
`pip install` from the lockfile.

---

## Out of scope reminders (don't accidentally build these)

Docker packaging, Prometheus/Grafana, multi-tenant deployments, speaker
diarization beyond channel attribution, transcript persistence beyond
logs/live streaming.