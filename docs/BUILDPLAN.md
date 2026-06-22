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
| (test-only) audio generation | `tools/audio_generation/` *(not shipped — see note below)* |
| Audio packet ingestion + routing | `audio_ingest/`, `channel/` |
| VAD                          | `vad/`                       |
| STT                          | `stt/`                       |
| Orchestration / reliability   | `pipeline/`                  |
| Config                       | `config/`                    |
| Observability                | `observability/`              |
| Health                       | `health/`                     |

**Note on `audio_generation`:** this is a dev/test tool, not a production
package — it simulates the real-world audio source (a phone call leg) by
either capturing the mic or replaying a `.wav` file and publishing it over
MQTT exactly like a real call leg would. Because it's test-only, it lives
under `tools/audio_generation/`, outside the `src/` packages that ship.
Everything downstream of it (`audio_ingest` onward) can't tell the
difference between it and a real call leg — that's the point.

Shared dataclasses (`AudioPacket`, `SpeechSegment`, `TranscriptEvent`) live in
`pipeline/models.py` so that `audio_ingest`, `channel`, `vad`, and `stt` can
all import them without depending on each other directly.

---

## STATUS (update this every session, even with one line)

```
Last updated: 2026-06-22
Current milestone: 0 - Fake end-to-end pipeline
Done: nothing yet
In progress: pipeline/models.py
Next action: define AudioPacket, then write a throwaway test that constructs one
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
3. `tools/audio_generation/fake_source.py`
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

## Milestone 1 — Real config + real audio generation (test tool)

1. `config/settings.py` — pydantic `Settings`, layered per §6:
   defaults → config file → local override file → env vars. Validate on load.
2. `tools/audio_generation/mic_source.py` — captures from the system
   microphone, publishes audio chunks over MQTT tagged with a `channel_id`.
3. `tools/audio_generation/wav_source.py` — reads a `.wav` file and streams
   it over MQTT at real-time pace (not as fast as disk I/O allows), so it
   behaves like a live call leg for testing.
4. Both `audio_generation` sources should be runnable standalone (no
   pipeline dependency) so you can sanity-check MQTT traffic with a generic
   MQTT client before `audio_ingest` exists.

**Done when:** running `wav_source.py` against a test broker produces
correctly-paced, channel-tagged MQTT messages, confirmed independently
(e.g. via `mosquitto_sub`) — no pipeline code involved yet.

---

## Milestone 2 — Real audio ingestion + channel routing

1. `audio_ingest/mqtt_client.py`
   - Subscribes to per-channel MQTT topics
   - Reconnects with exponential backoff (§5)
   - Pushes raw packets onto the shared ingest queue
2. `channel/router.py`
   - Consumes from the ingest queue
   - Tags/validates `channel_id`, maintains per-channel bookkeeping
     (e.g. last-seen timestamp for the freshness check in §7)
   - Hands packets off toward VAD unchanged at this stage — routing logic
     stays separate from VAD logic so each is testable in isolation
3. Swap `tools/audio_generation` + fake routing for real
   `audio_ingest` + `channel` in `main.py`. VAD/STT stay fake.

**Done when:** killing/restarting the MQTT broker connection mid-run
triggers reconnect without crashing the process, and two channels driven by
`wav_source.py` produce correctly-attributed (still-fake) transcripts.

---

## Milestone 3 — Real shared Silero VAD

1. `vad/worker.py`
   - One shared `Silero VAD` / `VADIterator` instance
   - Per-channel state dict (this is the part that bit you in
     `asr-coastguard` — **lock the full `vad_iter()` call, not just score
     calls**)
   - Port soft-cut logic (confidence-dip detection, `SOFT_CUT_S=5.0s`,
     `MAX_SEGMENT_S=7.0s`) from `asr-coastguard`, adapted to per-channel state
   - Window size `VAD_WINDOW_SAMPLES=512`
2. Swap fake VAD for `vad/worker.py`. STT stays fake.

**Done when:** two channels (mic + wav, or two wav sources) interleaved on
the ingest queue produce correctly-segmented, channel-attributed
`SpeechSegment`s with no crash under concurrent channel activity — write
the interleaving test explicitly, don't just eyeball logs.

---

## Milestone 4 — Real shared Moonshine STT

1. `stt/worker.py`
   - Single shared Moonshine STT instance (`tiny-ko`, quantized)
   - `STT_FEED_WINDOWS=64`, `STT_LANGUAGE="ko"`
   - Port `best_partial` tracking + unigram/bigram repetition guard from
     `asr-coastguard` to handle beam-search collapse at awkward boundaries
   - Decide here whether to keep `torch+cpu` or switch to the
     `useful-moonshine-onnx` + `onnxruntime` backend you scoped earlier
     (NEON via `.ort` + `InferenceSession`) — don't silently default back to
     torch without writing down why.
2. Swap fake STT for `stt/worker.py`. Full pipeline is now real, end to end:
   `audio_generation` (test) → `audio_ingest` → `channel` → `vad` → `stt`.

**Done when:** a real two-channel `.wav`/MQTT fixture (via `wav_source.py`)
produces correct Korean transcripts in order, attributed to the right
channel.

---

## Milestone 5 — Reliability

1. `pipeline/supervisor.py` — restarts `audio_ingest`/`channel`/`vad`/`stt`
   workers on unexpected exit, tracks restart counts, flags "degraded"
   after N repeated failures (§5)
2. Fault isolation: malformed packet / inference exception → log + drop,
   never kill the worker loop
3. Bounded-queue backpressure: queue depth tracked per stage and exposed
   (feeds Milestone 6)

**Done when:** deliberately raising inside `stt/worker.py` mid-run gets
logged, the worker restarts, and the pipeline keeps transcribing.

---

## Milestone 6 — Observability + Health

1. `observability/logging.py` — structured JSON logs with `channel_id`,
   pipeline stage, `segment_id` on every relevant event
2. `observability/metrics.py` — in-memory aggregation of STT latency, queue
   depth, restart counts, MQTT status, emitted as log events (no Prometheus)
3. `health/reporting.py` — health object: overall status, per-worker
   status, queue depths, MQTT connectivity, per-channel activity freshness

**Done when:** you can trace one segment's full lifecycle (`audio_ingest` →
`channel` → `vad` → `stt` → transcript) through logs alone, by `segment_id`.

---

## Milestone 7 — Web UI

1. FastAPI app (`web/app.py`) serving server-rendered pages
2. Control: start/stop pipeline, restart workers
3. Config: view effective config, edit local override, validate before apply
4. Live monitoring: WebSocket transcript stream, health dashboard (reads
   `health/reporting.py`), metrics dashboard (reads `observability/metrics.py`)

**Done when:** you can start the pipeline, watch live transcripts, and
restart a worker — all from the browser, no shell access needed.

---

## Milestone 8 — Testing & CI

1. Unit tests: `channel` routing, `vad` segmentation logic, `config`
   validation
2. Integration tests:
   - `audio_generation` (wav_source) → `audio_ingest` → `channel` → `vad`
   - Full end-to-end fixture through `stt`
3. CI workflow running both (perf validation stays manual, on-device)

**Done when:** CI is green on a clean clone with no manual setup beyond
`pip install` from the lockfile.

---

## Out of scope reminders (don't accidentally build these)

Docker packaging, Prometheus/Grafana, multi-tenant deployments, speaker
diarization beyond channel attribution, transcript persistence beyond
logs/live streaming.