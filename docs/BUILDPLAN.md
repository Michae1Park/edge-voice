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
| Fault tolerance              | `pipeline/supervisor.py` *(planned — Milestone 5, not yet built)* |
| Entry point                  | `cli.py`                     |
| Config                       | `config/`                    |
| Observability                | `observability/` *(planned — Milestone 6, not yet built)* |
| Health                       | `health/` *(planned — Milestone 6, not yet built)* |

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

**Failure-granularity note for later (Milestone 5):** MQTT
reconnect-with-backoff is a *connection-level* retry that lives inside
`audio_ingest`'s MQTT client itself — it is not a thread restart and should
never go through `supervisor`. `supervisor` only acts on the coarser,
rarer case: a worker thread dying outright from an unhandled exception.
Conflating the two will make restart-count metrics noisy and useless.

---

## STATUS (update this every session, even with one line)

```
Last updated: 2026-07-21
Current milestone: none
Done: ms 0, 1, 2, 3, 4
In progress: none
Next action: Milestone 5 — Reliability (pipeline/supervisor.py)
Blocked on: nothing
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
   **Decision recorded (2026-06-29):** web UI runs as a separate process
   (Milestone 7); no `--with-ui`/`--config`/`--channels`/`--wav-file` flags
   exist yet — add them if/when the features behind them actually land,
   not before.
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

## Milestone 5 — Reliability

1. `pipeline/supervisor.py` — restarts `audio_ingest`/`channel`/`vad`/`stt`
   worker threads on unexpected exit, tracks restart counts, flags
   "degraded" after N repeated failures (§5). `orchestrator.py` builds the
   workers and hands them to `supervisor.py` to watch — `supervisor`
   itself stays generic ("a thread died, restart it") rather than
   knowing what a VAD worker is.
2. Fault isolation: malformed packet / inference exception → log + drop,
   never kill the worker loop
3. Bounded-queue backpressure: queue depth tracked per stage and exposed
   (feeds Milestone 6)
4. Flesh out the `get_status()` seam stubbed in Milestone 1 so it reports
   real per-worker state (running/restarting/degraded) sourced from
   `supervisor`, not from grepping logs.

**Done when:** deliberately raising inside `stt/worker.py` mid-run gets
logged, the worker restarts via `supervisor`, and the pipeline keeps
transcribing — and `orchestrator.get_status()` reflects the restart.

---

## Milestone 6 — Observability + Health

1. `observability/logging.py` — structured JSON logs with `channel_id`,
   pipeline stage, `segment_id` on every relevant event
2. `observability/metrics.py` — in-memory aggregation of STT latency, queue
   depth, restart counts, MQTT status, emitted as log events (no Prometheus)
3. `health/reporting.py` — health object: overall status, per-worker
   status, queue depths, MQTT connectivity, per-channel activity freshness.
   Sources worker/restart status from `orchestrator.get_status()` rather
   than re-deriving it.

**Done when:** you can trace one segment's full lifecycle (`audio_ingest` →
`channel` → `vad` → `stt` → transcript) through logs alone, by `segment_id`.

---

## Milestone 7 — Web UI

1. FastAPI app (`tool/webui/app.py`) serving server-rendered pages
2. Control: start/stop pipeline, restart workers — calls into
   `pipeline/orchestrator.py` / `pipeline/supervisor.py`, doesn't duplicate
   their logic
3. Config: view effective config, edit local override, validate before apply
4. Live monitoring: WebSocket transcript stream, health dashboard (reads
   `health/reporting.py`), metrics dashboard (reads `observability/metrics.py`)
5. Resolve the `--with-ui` decision flagged in Milestone 1 here if it
   wasn't already: same process as `cli.py`/`orchestrator`, or separate.

**Done when:** you can start the pipeline, watch live transcripts, and
restart a worker — all from the browser, no shell access needed.

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