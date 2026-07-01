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
| Fault tolerance              | `pipeline/supervisor.py`     |
| Entry point                  | `cli.py`                     |
| Config                       | `config/`                    |
| Observability                | `observability/`              |
| Health                       | `health/`                     |

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

**`main.py` is Milestone-0-only scaffolding**, not a second entry point. It
exists purely because Milestone 0 has no config system yet, so it hardcodes
two channel IDs and wires the fake workers directly. Once `cli.py` and
`config/settings.py` exist (Milestone 1), `main.py`'s wiring logic moves
into `pipeline/orchestrator.py` and `main.py` is either deleted or shrunk to
a 3-line dev convenience (`if __name__ == "__main__": cli.main()`) for
running without installing the package.

**Failure-granularity note for later (Milestone 5):** MQTT
reconnect-with-backoff is a *connection-level* retry that lives inside
`audio_ingest`'s MQTT client itself — it is not a thread restart and should
never go through `supervisor`. `supervisor` only acts on the coarser,
rarer case: a worker thread dying outright from an unhandled exception.
Conflating the two will make restart-count metrics noisy and useless.

---

## STATUS (update this every session, even with one line)

```
Last updated: 2026-06-29
Current milestone: none  
Done: ms 0, 1
In progress: none
Next action: n/a
Blocked on: nothing
```

---

## Milestone 0 — Fake end-to-end pipeline 

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
5. `main.py` wires the fake source + fake workers together, logs
   `TranscriptEvent`s to stdout.

**Done when:** `python main.py` runs for 30s, prints fake transcripts for two
fake channels, exits cleanly on Ctrl-C with no orphaned threads.

---

## Milestone 1 — Real config + cli.py entry point + real audio generation

**Goal:** replace the Milestone-0 throwaway wiring with the permanent
shape — `cli.py → orchestrator → workers` — and get real (non-fake) audio
flowing over MQTT, even though `audio_ingest` doesn't exist to consume it
yet.

1. `config/settings.py` — pydantic `Settings`, layered per §6:
   defaults → config file → local override file → env vars. Validate on load.
2. `pipeline/orchestrator.py` — move the wiring logic out of `main.py`
   here, parameterized by `Settings` instead of hardcoded constants. Same
   responsibilities `main.py` already proved out in Milestone 0: build
   workers, build queues, own startup order, own graceful shutdown
   (stop-event + join). Still wires the *fake* VAD/STT workers from
   Milestone 0 — only the ingest side gets real this milestone.
   - Expose a `get_status()`-shaped seam now (even if it just returns
     `{"workers": [...]}` for the moment) — Milestones 6/7 will need to
     query orchestrator/supervisor for live status, and retrofitting that
     later is more painful than stubbing it now.
3. `cli.py` — becomes the real entry point: parse args, load `Settings`,
   call `orchestrator.build_and_run(settings)`. This is what the
   `edge-voice` console script will point to.
   - **Decision needed before writing arg parsing:** does `cli.py` take a
     flag like `--with-ui` to optionally start the FastAPI app (Milestone
     7) in the same process, or is the web UI a separate process that just
     reads `health`/`observability` state? Write this down once decided —
     it changes whether the orchestrator and the web app share an event
     loop / thread set.
4. Shrink `main.py` to a 3-line dev convenience that calls `cli.main()`, or
   delete it outright — its wiring logic now lives in `orchestrator.py`.
5. `utils/audio_generation/mic_source.py` — captures from the system
   microphone, publishes audio chunks over MQTT tagged with a `channel_id`.
6. `utils/audio_generation/wav_source.py` — reads a `.wav` file and streams
   it over MQTT at real-time pace (not as fast as disk I/O allows), so it
   behaves like a live call leg for testing.
7. Both `audio_generation` sources must be runnable standalone, in their
   own process/terminal, with **no import of `pipeline`, `cli`, or
   `orchestrator`** — confirm this by checking their import lines, not just
   by running them.

**Done when:** `cli.py` (or the installed `edge-voice` console script)
starts the still-fake pipeline using real `Settings`, AND, in a separate
terminal/process, `wav_source.py` produces correctly-paced, channel-tagged
MQTT messages — confirmed independently (e.g. via `mosquitto_sub`), no
pipeline code involved in that check.

---

## Milestone 2 — Real audio ingestion + channel routing

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

## Milestone 3 — Real shared Silero VAD

1. `vad/worker.py`
   - One shared `Silero VAD` / `VADIterator` instance
   - Per-channel state dict (prototype had issues — **lock the full `vad_iter()` call, not just score
     calls**)
   - Port soft-cut logic (confidence-dip detection, `SOFT_CUT_S=5.0s`,
     `MAX_SEGMENT_S=7.0s`) from prototype, adapted to per-channel state
   - Window size `VAD_WINDOW_SAMPLES=512`
2. Swap fake VAD for `vad/worker.py` inside `pipeline/orchestrator.py`. STT
   stays fake.

**Done when:** two channels (mic + wav, or two wav sources, each its own
process) interleaved on the ingest queue produce correctly-segmented,
channel-attributed `SpeechSegment`s with no crash under concurrent channel
activity — write the interleaving test explicitly, don't just eyeball logs.

---

## Milestone 4 — Real shared Moonshine STT

1. `stt/worker.py`
   - Single shared Moonshine STT instance (`tiny-ko`, quantized)
   - `STT_FEED_WINDOWS=64`, `STT_LANGUAGE="ko"`
   - Port `best_partial` tracking + unigram/bigram repetition guard from
     prototype to handle beam-search collapse at awkward boundaries
2. Swap fake STT for `stt/worker.py` inside `pipeline/orchestrator.py`.
   Full pipeline is now real, end to end: `audio_generation` (separate
   process, test-only) → `audio_ingest` → `channel` → `vad` → `stt`.

**Done when:** a real two-channel `.wav`/MQTT fixture (via `wav_source.py`,
its own process) produces correct Korean transcripts in order, attributed
to the right channel.

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