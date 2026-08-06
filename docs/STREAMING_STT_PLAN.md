# Streaming Moonshine STT — Design Plan

**Status:** On hold (2026-08-06) — deliberately, not for lack of time. The
analysis below is complete and still accurate; benchmarking showed streaming
is not a throughput win (§4.1) and the current VAD-bounded, short-segment
design is already close to the cheapest approach available (§8). No code was
written, and there is no committed timeline to resume — possibly never. If
this is picked up again, start from §9's suggested order rather than
re-deriving the findings below.
**Companion to:** `ARCHITECTURE.md`, `BUILDPLAN.md`

## Goal

Support Moonshine's `*-streaming` archs so transcripts appear *while* a speaker
is still talking, instead of only after `VADWorker` finalizes a segment — and
make the choice between streaming and non-streaming a validated configuration
concern rather than something that explodes deep inside the STT worker.

---

## 1. Terminology — "v1 / v2" is not how this versions

There is no v1/v2 in the `moonshine_voice` API. What we run is selected by
`stt.model_arch`, and the enum is:

| `ModelArch` | Streaming? |
|---|---|
| `TINY` ← **what we run today** | no |
| `BASE` | no |
| `TINY_STREAMING`, `BASE_STREAMING`, `SMALL_STREAMING`, `MEDIUM_STREAMING` | yes |

Separately, `moonshine-ai/moonshine-v2` is a **different GitHub repository**, not
the thing `*_STREAMING` refers to. Don't conflate the two when searching upstream.

So: "we're on a non-streaming model" is correct. "We're on v1" is not meaningful.

---

## 2. Model availability is a config concern, not a blocker

Streaming archs are published for **English only** (verified by probing the CDN
key space directly — `scratch/probe_moonshine_models.py`). Korean has `tiny` and
`base`, neither streaming. That is a fact about the catalog, not a reason to
stop: it means `(language, arch)` is a **validated combination**, and an
unsupported pair must be rejected cleanly at startup.

Availability today, and the shape any validation must encode:

| Language | Non-streaming | Streaming |
|---|---|---|
| `en` | `tiny`, `base` | `tiny-streaming`, `small-streaming`, `medium-streaming` |
| `ko` | `tiny`, `base`* | — |
| `ar`, `uk`, `vi`, `zh` | `tiny`*, `base` | — |
| `ja` | `tiny`, `base` | — |
| `es` | `base` only | — |

`*` reachable only via `_MODEL_REGISTRY_OVERRIDES` in `stt/stt_worker.py`.
`base-streaming` exists for no language. Re-check with the probe script after
upgrading `moonshine-voice`; upstream can publish or withdraw without a version
bump.

### 2.1 The language/arch selection layer (new work)

**Problem this solves.** Today the `(language, arch)` pair is resolved deep
inside `STTWorker._new_transcriber()`, at construction. An unsupported pair
surfaces as a `ValueError` from moonshine's resolver, thrown from inside a
worker thread the orchestrator is mid-way through building — an ugly traceback
for what is really a config mistake. Worse, the override table that makes
`ko`/`base` reachable lives in the same module, so "what is available" is
knowledge scattered across moonshine's registry and ours.

**Proposal: `stt/model_registry.py`**, one module owning model availability, so
nothing downstream has to ask moonshine directly.

Responsibilities:

- Own `_MODEL_REGISTRY_OVERRIDES` (moved out of `stt_worker.py`).
- `available_archs(language) -> list[str]` — moonshine's registry plus our
  overrides, merged. This is what an error message should print.
- `resolve(language, arch) -> (model_path, ModelArch)` — the single resolution
  path, replacing `STTWorker._resolve_model()`; raises a typed
  `UnsupportedModelError` (new, in `stt/`) rather than a bare `ValueError`.
- `is_streaming(arch) -> bool` — the capability flag §5 needs to pick a feeding
  strategy. Deriving this from the arch name is fine; it must have exactly one
  definition, not one per call site.

**Where validation runs.** At startup, before any worker is built — in
`cli.py`, after `Settings.load()` and `configure_logging()`, so the failure is
logged through the normal sink. `PipelineOrchestrator.build()` must not be
reached with an invalid pair.

**Graceful exit means:** log one ERROR line naming the requested pair and what
that language *does* offer, then `sys.exit(2)`. No traceback, no half-built
pipeline, no supervisor restart loop against a config error that will fail
identically every time. (Note the systemd unit's `Restart=always` — a config
error that crashes on every start is exactly what the crash-loop breaker in
`ARCHITECTURE.md` §5 exists to contain, but exiting cleanly is better than
relying on it.)

**Deliberately not doing:** a network check. Availability is answered from the
merged tables alone; whether the CDN still serves the file is discovered at
download time, and is a different failure (network/offline) with a different
remedy.

---

## 3. What the code already does — and the two APIs it uses

`stt/stt_worker.py` drives **two different moonshine entry points**, which is
easy to misread as redundancy. They are genuinely different calls:

| | `transcribe_without_streaming(audio, sr)` | `start()` / `add_audio()` / `stop()` |
|---|---|---|
| Operates on | the **Transcriber handle** | a **`Stream`** (lazily created default stream) |
| Session state | none | decoder KV cache, accumulated lines, listeners |
| Returns | a `Transcript` directly | nothing; output arrives as listener **events** |
| Used by | `_transcribe_partial` (partials) | `_transcribe` (finals) |

`Transcriber.add_audio/start/stop/add_listener` are **sugar**: each one calls
`get_default_stream().<same method>`. So the "streaming session API" the worker
uses today is just the default stream on a non-streaming model.

**What `Stream.add_audio` actually does** (`transcriber.py`): appends audio,
accumulates `_stream_time += len(audio)/sample_rate`, and when
`_stream_time - _last_update_time >= update_interval` (default **0.5**) calls
`update_transcription()` — a real decode pass. `stop()` triggers one more. So
feeding one 3s segment in a single `add_audio()` costs *two* native transcribe
calls, not one.

> **Correction to earlier scoping:** `update_interval` is measured in
> **accumulated audio seconds**, not wall-clock. §6's methodology warning
> claimed wall-clock; at the Python level the update cadence is deterministic
> given identical chunking. Any run-to-run variation must originate inside
> `libmoonshine.so`, which we cannot inspect — so the leak-detection advice in
> §6 still stands, but not for the stated reason.

### 3.1 Does the stream path make sense for a non-streaming model?

**Measured** (`tiny-ko`, one 3.0s speech segment from `wav/rx_recorded_1.wav`,
5 runs, x86):

| Path | best | avg | line updates | text |
|---|---|---|---|---|
| `start` → `add_audio` → `stop` | 0.116s | 0.126s | 1 | identical |
| `transcribe_without_streaming` | 0.108s | 0.113s | 0 | identical |

So the stream path costs **~7%** more for byte-identical output — the second
decode is nearly free, presumably cached natively when no new audio arrived.
Not the 2× the call count suggests.

**It is still doing something the one-shot cannot.** The repetitive-output guard
(`_make_collector`) falls back to `best_partial`, which is built from
`on_line_text_changed` events — *intermediate* line states. `transcribe_without_streaming`
returns only completed lines, so there is no fallback material; that is exactly
why `_transcribe_partial` has no fallback and simply discards a repetitive
partial.

**Verdict:** keep it, but document *why*, because it currently reads as an
accident. The 7% buys the repetition fallback on finals. If that guard is ever
removed or reworked, finals should move to `transcribe_without_streaming` — it
is simpler, has no stream state to reset between segments, and would remove the
`remove_all_listeners()` dance.

---

## 4. Measured facts to design against

### 4.1 Throughput — streaming is **slower**, not faster

This is the central correction to the original premise. Feeding small chunks to
a streaming model does **not** speed up transcription; it costs substantially
more compute, because every `update_interval` re-decodes.

**30s of real English speech** (`wav/obama_2012.wav`, downmixed to 16kHz mono),
x86 Ryzen, `tiny-en` vs `tiny-streaming-en`:

| Approach | total decode | RTF | notes |
|---|---|---|---|
| Non-streaming, 10 × 3s segments, one-shot each | **0.58s** | **51.4×** | today's design |
| Streaming, 0.5s chunks, `update_interval=0.5` | 6.21s | 4.8× | **10.7× more compute** |

Tuning the knob recovers a lot of it, monotonically trading updates for compute:

| `update_interval` | decode | RTF | line updates over 30s |
|---|---|---|---|
| 0.5 (default) | 5.70s | 5.3× | 57 |
| 1.0 | 3.76s | 8.0× | 35 |
| 2.0 | 2.57s | 11.7× | 23 |
| 5.0 | 1.93s | 15.5× | 15 |

**Even at `update_interval=5.0`, streaming costs 3.3× the non-streaming
segment decode.** There is no setting at which it is cheaper.

**So what does streaming actually buy?** Not throughput — *incremental output
during an utterance*. Today a final cannot appear until VAD finalizes the
segment (speech end + `min_silence_duration_ms`, plus decode). A streaming model
emits revisable text continuously, with no VAD wait and no boundary
duplication.

**Which is the same trade partials already make, done better.** Today's
partials re-transcribe the whole prefix from scratch at 1.8×–3.0× total
inference (`configs/default.yaml`). Streaming at `update_interval≈2.0` is
~4.4× the no-partials baseline for *finer* updates and cleaner continuity. So
the honest framing is: **streaming replaces the partial mechanism**, at a
somewhat higher and much more predictable cost — not "streaming makes STT
fast."

**Open risk — does this fit on a Pi?** Every number above is x86. At
`update_interval=2.0` streaming runs 11.7× real time on this machine, so two
channels of continuous speech need ~6× real time headroom. A Pi 5 is
materially slower than this box on int8 ONNX; if the gap is ~6×, two streaming
channels land near 1.0× real time — i.e. **marginal**. Measure on device before
committing (§9). Note VAD gating helps enormously here: a call channel is
silent most of the time, and silence never needs to be streamed at all (§5.2).

### 4.2 Memory — streams are free, Transcribers are not

Current `VmRSS`, `tiny-en-streaming`, x86, no inference between measurements:

| step | RSS | delta |
|---|---|---|
| baseline (imports only) | 34MB | — |
| + 1st `Transcriber` | 212MB | **+179MB** |
| + 1st stream on it | 215MB | +2MB |
| + 2nd stream on it | 215MB | **+0MB** |
| + 3rd stream on it | 215MB | +0MB |
| + 2nd `Transcriber` (same model) | 391MB | **+176MB** |
| + 3rd `Transcriber` (same model) | 533MB | +142MB |

For two channels: 2 `Transcriber`s ≈ 360MB, versus 2 `Stream`s on 1
`Transcriber` ≈ 182MB. That ~180MB delta is what §6 decides.

The `.ort` weights *are* memory-mapped (15 mappings across `frontend`,
`encoder`, `decoder_kv`, `adapter`, `cross_kv`), but a second `Transcriber`
still allocates its own ~180MB of session state, so file sharing buys little.

> **Measurement note:** do not use `resource.getrusage().ru_maxrss` — it reports
> *peak* RSS, is monotonic, and absorbs inference working memory. Doing so
> during scoping produced a bogus "+64MB per extra stream" figure; the real cost
> is ~0MB. Read `VmRSS` from `/proc/self/status`, and don't run inference
> between samples.

### 4.3 Streaming models are a different artifact

Non-streaming ships `encoder_model.ort` + `decoder_model_merged.ort`. Streaming
ships `frontend.ort`, `encoder.ort`, `decoder_kv.ort`, `adapter.ort`,
`cross_kv.ort`, plus a `streaming_config.json`:

```json
{ "frame_len": 80, "total_lookahead": 16, "encoder_dim": 320, "depth": 6 }
```

Fixed frame length and lookahead — built for incremental feeding.

**Stream API.** `Transcriber.create_stream(update_interval, flags,
transcribe_flags) -> Stream`; a `Stream` has its own `start` / `stop` /
`add_audio` / `add_listener` / `update_transcription` / `close`.

---

## 5. What actually has to change

**5.1 Feeding strategy becomes arch-dependent, not replaced.**
`stt_worker.py`'s docstring justifies one `add_audio()` per finalized segment
because windowed feeding measured 2.7× slower with word duplication. That
measurement is still valid **for non-streaming models** and must not be deleted
— it becomes conditional, not obsolete. A streaming arch wants continuous
frames; a non-streaming arch still wants one call per segment. `is_streaming()`
(§2.1) picks between them.

**5.2 Segmentation ownership — VAD stays, its job narrows.** This is the
largest design fork, and §4.1 settles part of it: because streaming costs
compute per unit of *audio fed*, streaming silence is pure waste, and a
two-party call channel is silent most of the time. So VAD must keep gating.
Two viable shapes:

- **(a) VAD segments, STT streams within a segment.** VAD still detects speech
  start/end and owns turn boundaries; instead of buffering to finalize, it
  forwards chunks as they arrive, and STT feeds them into a per-channel stream
  opened at speech start and closed at speech end. Keeps channel attribution,
  keeps `segment_id`, keeps silence out of the model, and deletes the partial
  machinery. **Recommended** — smallest change with the full latency benefit.
- **(b) VAD as pure gate, one long-lived stream per channel.** Line boundaries
  come from moonshine's `on_line_completed` instead of VAD. Loses VAD-defined
  turn structure and the `segment_id` lifecycle that §7 of `ARCHITECTURE.md`
  traces through the logs. Bigger rewrite for unclear gain.

Under (a), `SpeechSegment` grows a notion of "chunk of an open segment" or the
queue starts carrying a different message type — that interface is the real
design work.

**5.3 Per-channel decoder state.** "One shared `Transcriber`" rests on segments
being handled strictly one at a time with `start()`/`stop()` resetting state.
Continuous per-channel streaming needs **concurrent open sessions** — either one
`Transcriber` per channel (~360MB) or one `Transcriber` hosting one `Stream`
per channel (~182MB). Unresolved: §6.

**5.4 Partials are replaced, not accelerated.** `vad.partial_interval_s`,
`vad.partial_min_segment_s`, `stt.partial_max_queue_depth`,
`STTWorker._handle_partial` / `_transcribe_partial` / `partial_stats()`, and
`SpeechSegment.is_partial` all become dead weight under streaming — the stream
emits revisable text natively. They must stay for the non-streaming path, so
this is a fork in behavior, not a deletion.

**5.5 Re-tuning.** `stt.max_tokens_per_second: 18.0` and `_is_repetitive`'s
`repetitive_ratio: 0.45` were tuned against Korean non-streaming output.
Neither transfers.

**5.6 No config schema change needed.** `STTSettings.model_arch` already accepts
every streaming value as a `Literal`.

---

## 6. Open question — do two streams share decoder state?

**Unresolved.** Decides ~182MB vs ~360MB for two channels (§4.2).

**Evidence points to independent — multi-stream looks designed, not incidental:**

- Each stream gets its **own native handle** (`moonshine_create_stream`).
- Every C call takes **both** handles (`moonshine_start_stream(transcriber, stream)`,
  `moonshine_transcribe_add_audio_to_stream(...)`) — model and session state are
  explicitly separate arguments.
- Every event carries `stream_handle`, so output is attributable to its stream.
- Behaviourally: stream A fed speech + stream B fed silence on one `Transcriber`
  → B's transcript came back empty.
- 3 streams on one `Transcriber` cost ~0MB each.

None of that *proves* isolation — a library can hand out handles and still share
buffers underneath. Confirm before relying on it.

**Methodology warning.** The obvious test (run a channel solo, then interleaved,
compare transcripts) is unreliable: streaming line boundaries shift run-to-run
in practice. **Use leak detection instead** — feed A and B *different,
distinctive* sentences concurrently, then assert none of A's distinctive words
appear in B's transcript and vice versa. Robust to non-determinism.

---

## 7. Test audio — prerequisite now satisfied

`wav/obama_2012.wav` is **real English speech** (100.6s), which is what §4.1's
measurements ran against. The corpus is no longer Korean-only, and this plan is
no longer gated on obtaining English audio.

Caveats that cost real time during scoping and still apply:

- It is **stereo, 44.1kHz** — `wav_source_raw.py` refuses stereo against a
  single MQTT channel. Downmix first:
  `ffmpeg -i wav/obama_2012.wav -ac 1 -ar 16000 wav/obama_2012_mono.wav`.
- **Never run an English model against Korean audio** (or vice versa). It
  produces confident-looking nonsense that was initially misread as a
  streaming-model defect.
- `wav/long_youtube.wav` is **clipped** (peak = 1.000) and induces hallucination
  even in the matching-language case. Not a valid benchmark for anything.
- In the hallucination regime the model is non-deterministic run-to-run; on
  clean in-language audio it is stable. Instability is a *symptom of bad input*,
  not a model property.

Still missing: English audio that resembles the **deployment** workload —
two-party, telephone-band, turn-taking. A political speech is one continuous
speaker at studio quality, so §4.1's RTF figures are a best case for streaming
(no turn gaps to exploit, no VAD gating benefit).

---

## 8. Ruled out / decided

| Question | Answer |
|---|---|
| Is this an API-surface rewrite? | No — the stream API is already in use |
| Does `model_arch` config need extending? | No — `Literal` already lists the streaming archs |
| Can we run streaming in Korean? | **No** — no `ko` streaming model exists (§2). Not a blocker: it makes `(language, arch)` a validated pair |
| **Is streaming faster?** | **No — 3.3×–10.7× more compute (§4.1).** It buys latency-to-first-text, not throughput |
| Does streaming reduce partial cost? | **No — it replaces the partial mechanism** at higher, more predictable cost |
| Should non-streaming finals keep using the stream API? | Yes, for now — costs ~7%, buys the repetition fallback (§3.1) |
| Does VAD go away? | **No** — streaming silence is pure waste; gating is what makes streaming affordable (§5.2) |
| Is streaming quality comparable? | **Unknown** — not yet measured on comparable audio |

---

## 9. Suggested order when this resumes

1. **Build the selection layer (§2.1) first.** It is independent of every
   streaming question, is useful immediately (today a bad `(language, arch)`
   pair still throws from inside a worker thread), and is a prerequisite for
   letting the pipeline branch on `is_streaming`.
2. **Measure on the Pi** (§4.1 risk). One number decides the whole plan: RTF for
   `tiny-streaming-en` at `update_interval≈2.0` on target hardware. Below ~2×
   per channel, streaming is not viable for two channels and the rest of this
   plan is moot.
3. Get two-party English test audio (§7) — the current fixture overstates the
   streaming case.
4. Run the leak-detection test (§6) → decides per-channel memory cost.
5. Decide the §5.2 fork — (a) is recommended; commit before writing code.
6. Only then touch `stt_worker.py`, making the feeding strategy conditional on
   `is_streaming()` rather than replacing it, and rewriting the module docstring
   alongside.
