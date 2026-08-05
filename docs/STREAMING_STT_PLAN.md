# Streaming Moonshine STT — Design Plan

**Status:** Not started. Scoping only — no code written. **Blocked on a language
decision and on English test audio** (see §2, §7).
**Companion to:** `ARCHITECTURE.md`, `BUILDPLAN.md`

## Goal

Move STT from Moonshine's non-streaming `tiny` arch to one of the `*-streaming`
archs, so transcripts appear while a speaker is still talking instead of only
after `VADWorker` finalizes a segment.

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

## 2. Blocker — Korean has no streaming model

Measured against the live model index (`get_model_for_language` per arch):

| Language | Available archs |
|---|---|
| `ko` ← **our config** | `TINY` only |
| `en` | `TINY`, `BASE`, `TINY_STREAMING`, `SMALL_STREAMING`, `MEDIUM_STREAMING` |

(`BASE_STREAMING` is unavailable for both.)

`configs/default.yaml` already records this ("ko has no streaming model"), and
`STTSettings` docstring says the same. **Streaming is unreachable while the
spoken language is Korean.**

This is a decision, not a task:

- If deployment audio stays **Korean** → this plan is blocked upstream. Track /
  file a request for a Korean streaming model; build nothing.
- If deployment audio becomes **English** → proceed with §5.

---

## 3. What the code already does (less work than it looks)

`stt/stt_worker.py` **already drives the streaming session API** —
`start()` → `add_audio()` → `stop()` with `on_line_text_changed` /
`on_line_completed` listeners. That surface barely changes.

The non-streaming-ness lives in *how* it is fed, not in which calls it makes.

---

## 4. Measured facts to design against

**The two objects, since the distinction drives §6:**

- **`Transcriber`** — a *loaded model instance*: owns the ONNX sessions and
  weights. Expensive.
- **`Stream`** — *one conversation's decode state* through that model (decoder KV
  cache, accumulated lines, listeners), from `tr.create_stream()`. One
  `Transcriber` can host several.

**Memory** (current `VmRSS`, `tiny-en-streaming`, x86, no inference between
measurements):

| step | RSS | delta |
|---|---|---|
| baseline (imports only) | 34MB | — |
| + 1st `Transcriber` | 212MB | **+179MB** |
| + 1st stream on it | 215MB | +2MB |
| + 2nd stream on it | 215MB | **+0MB** |
| + 3rd stream on it | 215MB | +0MB |
| + 2nd `Transcriber` (same model) | 391MB | **+176MB** |
| + 3rd `Transcriber` (same model) | 533MB | +142MB |

**Streams are essentially free; each `Transcriber` costs ~180MB.** So for two
channels:

| approach | RSS |
|---|---|
| 2 `Transcriber`s | ~360MB |
| 2 `Stream`s on 1 `Transcriber` | ~182MB |

That ~180MB delta is what §6 decides, and why it matters on a Pi.

The `.ort` weights *are* memory-mapped (15 mappings across `frontend`, `encoder`,
`decoder_kv`, `adapter`, `cross_kv`), but a second `Transcriber` still allocates
its own ~180MB of session state, so file sharing buys little.

> **Measurement note:** do not use `resource.getrusage().ru_maxrss` here — it
> reports *peak* RSS, is monotonic, and absorbs inference working memory. Doing
> so during scoping produced a bogus "+64MB per extra stream" figure; the real
> cost is ~0MB. Read `VmRSS` from `/proc/self/status` and avoid running inference
> between samples.

**Streaming models are a different artifact.** Non-streaming ships
`encoder_model.ort` + `decoder_model_merged.ort`. Streaming ships `frontend.ort`,
`encoder.ort`, `decoder_kv.ort`, `adapter.ort`, `cross_kv.ort`, plus a
`streaming_config.json`:

```json
{ "frame_len": 80, "total_lookahead": 16, "encoder_dim": 320, "depth": 6 }
```

Fixed frame length and lookahead — built for incremental feeding.

**Stream API.** `Transcriber.create_stream(update_interval, flags,
transcribe_flags) -> Stream`; a `Stream` has its own `start` / `stop` /
`add_audio` / `add_listener` / `update_transcription` / `close`. The
`Transcriber.*` methods are sugar over a default stream.

---

## 5. What actually has to change

**5.1 Feeding strategy inverts.** `stt_worker.py`'s module docstring justifies
one `add_audio()` per *finalized* segment, because windowed feeding measured
**2.7x slower with word duplication** at window boundaries. That measurement was
taken against a non-streaming model and **stops applying** — streaming models are
built for continuous frames. The docstring must be rewritten, not just edited,
or it will actively mislead.

**5.2 Segmentation ownership becomes contested.** Today `VADWorker` owns
segmentation and hands over finalized `SpeechSegment`s; STT never sees raw audio.
A streaming model wants a continuous feed and emits its own line boundaries via
`on_line_completed`. Open design question: does VAD still cut segments, or drop
to gating silence only? This is the largest architectural fork in this plan.

**5.3 Per-channel decoder state.** The docstring's "one shared Transcriber"
decision rests on segments being handled strictly one at a time, with
`start()`/`stop()` resetting decoder state between them. Continuous per-channel
streaming needs **concurrent open sessions**, which that same reasoning says
cannot share an instance. Either one `Transcriber` per channel, or one
`Transcriber` hosting one `Stream` per channel — **unresolved, see §6**.

**5.4 Partials become nearly free.** Today every partial re-transcribes the whole
prefix from scratch (`configs/default.yaml` documents 1.8x–3.0x inference cost).
With streaming, partial output is the natural byproduct. `vad.partial_interval_s`,
`vad.partial_min_segment_s`, and `stt.partial_max_queue_depth` all become
obsolete or need re-tuning, as does `STTWorker._handle_partial` /
`_transcribe_partial` and `partial_stats()`.

**5.5 Re-tuning.** `stt.max_tokens_per_second: 30.0` and `_is_repetitive`'s
`repetitive_ratio: 0.45` were both tuned against Korean non-streaming output.
Neither transfers.

**5.6 No config schema change needed.** `STTSettings.model_arch` already accepts
every streaming value as a `Literal`.

---

## 6. Open question — do two streams share decoder state?

**Unresolved.** This decides ~182MB vs ~360MB of RSS for two channels (§4) —
streams are free, `Transcriber`s are not, so the answer is worth ~180MB.

**Evidence so far points to independent — multi-stream is a designed feature, not
incidental:**

- Each stream receives its **own native handle**:
  `moonshine_create_stream(transcriber._handle, flags)`.
- Every C call takes **both** handles —
  `moonshine_start_stream(transcriber_handle, stream_handle)`,
  `moonshine_transcribe_add_audio_to_stream(...)`. Model and session state are
  explicitly separate arguments.
- Every event carries `stream_handle` (`LineCompleted(line=..., stream_handle=...)`),
  so output is attributable to its originating stream. Listeners are per-stream;
  `Transcriber.add_listener` merely delegates to a lazily-created default stream.
- Behaviourally: stream A fed speech + stream B fed silence on one `Transcriber`
  → B's transcript came back empty. No gross audio bleed.
- Creating 3 streams on one `Transcriber` succeeded at ~0MB each (§4).

None of that *proves* decoder-state isolation — a library can hand out handles
and still share buffers underneath — so confirm before relying on it. The library
docs say nothing about the semantics either way.

**Design note:** because events carry `stream_handle`, per-channel attribution can
be done either with one collector per stream or one listener demultiplexing on
the handle.

**Methodology warning — do not repeat this mistake.** The obvious test (run a
channel solo, run it again interleaved, compare transcripts for equality) **does
not work**. Streaming decode is driven by a wall-clock `update_interval`, so line
boundaries land differently on every run; solo-vs-solo repeats of identical audio
already differ. Exact-match comparison cannot decide this.

**Use leak detection instead**, which is robust to non-determinism: feed channels
A and B *different, distinctive* sentences concurrently, then assert none of A's
distinctive words appear in B's transcript, and vice versa.

---

## 7. Test-audio prerequisite

**The entire `wav/` corpus is Korean.** There is no English sample to evaluate a
streaming switch against, and nothing in this plan can be validated until there
is.

This caused real wasted effort during scoping: English models were run against
Korean audio, which produced confident-looking but meaningless output —
repetitive hallucination that was initially misread as a streaming-model defect.
Two further traps found the same way:

- `wav/long_youtube.wav` is **clipped** (peak = 1.000) and induces hallucination
  even in the matching-language case. Not a valid benchmark for anything.
- In the hallucination regime the model is **non-deterministic** run-to-run;
  on clean in-language audio it is stable. Instability is therefore a *symptom of
  bad input*, not a model property — check input before concluding otherwise.

Synthetic speech (Moonshine ships TTS with `en-us` / `en-gb`) is available as a
fallback, but is an optimistic benchmark and says little about real-world
accuracy. Prefer real English recordings.

---

## 8. Ruled out / decided

| Question | Answer |
|---|---|
| Is this an API-surface rewrite? | No — the streaming session API is already in use |
| Does `model_arch` config need extending? | No — `Literal` already lists the streaming archs |
| Can we do this in Korean? | **No** — no `ko` streaming model exists |
| Does streaming reduce partial cost? | Yes — that's most of the benefit |
| Is streaming quality comparable? | **Unknown** — never validly measured (see §7) |

---

## 9. Suggested order when this resumes

1. Confirm deployment language is English (§2). If not, stop.
2. Obtain real English test audio (§7).
3. Run the leak-detection test (§6) → decides per-channel memory cost.
4. Decide the VAD/segmentation fork (§5.2) — biggest design commitment.
5. Only then touch `stt_worker.py`, rewriting its docstring alongside the code.
