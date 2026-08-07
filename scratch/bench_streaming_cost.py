#!/usr/bin/env python3
"""Streaming vs non-streaming: how long does each update take, and what does it say?

Three experiments:

    1. tiny-en            (non-streaming)  -- fed 250ms chunks
    2. tiny-streaming-en  (streaming)      -- fed 250ms chunks, identically
    3. tiny-en warm stream vs cold stream  -- does a stream reuse anything?

1 and 2 use the same audio, chunking and update_interval, so only the model
differs. Both are fed via transcriber.add_audio(), exactly as moonshine's own
README example does -- the non-streaming model accepts chunked feeding too
(edge-voice does it today), so the comparison isolates the model, not the API.

1 and 2 deliberately use no create_stream(): Transcriber.start/add_audio/stop/
add_listener are sugar over a lazily-created *default* stream whose
update_interval defaults to the Transcriber's own (0.5s), so
create_stream(update_interval=0.5) would build an identical object. Explicit
streams earn their keep only for a non-default interval, or for several
concurrent sessions on one model (one per channel -- see
docs/STREAMING_STT_PLAN.md §6). Experiment 3 needs both of those, which is why
it calls create_stream() directly.

What to watch in 1 and 2: add_audio() only buffers, EXCEPT when accumulated
audio crosses update_interval, at which point it decodes inline. With 250ms
chunks and a 500ms interval that is every 2nd chunk, so the slow rows are the
decodes. Cost climbs as the buffer grows.

Experiment 3 asks whether that climb could be softened by keeping a stream
open. AN EARLIER VERSION OF THIS SCRIPT GOT THE ANSWER WRONG TWICE:

  1. First compared warm updates against transcribe_without_streaming(), a
     DIFFERENT native call (~2x more expensive on its own) -- made the stream
     look like it reused nothing, when the difference was the API, not reuse.
  2. Fixed to use the same call (update_transcription() on both sides), but
     ran the entire warm sweep (1s..8s) BEFORE the entire cold sweep. Anything
     that warms up over the process's life -- CPU frequency off idle, ONNX
     Runtime's thread pool, page cache for the .ort files -- gets paid for
     during warm and is free by the time cold runs. That showed up as
     "warm costs the same or more than cold," most implausibly at 1s, where
     both conditions are each stream's very first-ever decode and should be
     identical, yet warm was ~2x cold.

This version alternates which sweep runs first across repetitions (plus one
untimed throwaway pass before anything is measured) and reports the median, so
process warmup lands on both conditions equally. Reproduced across 3 fresh runs:
warm is now consistently CHEAPER than cold, ~10-35% at most buffer lengths and
~2x specifically at 2s (unexplained -- flagged, not papered over). So for
tiny-en, a stream open across updates DOES retain something; what exactly is
not established here.

What the numbers support, and all that should be quoted from this script:

  - Per-update cost grows with accumulated audio for BOTH models.
  - tiny-streaming-en costs ~2x tiny-en per update at every point on this clip
    (note it is also a bigger model: 34M params vs 26M).
  - A warm (already-open) stream costs less per update than a cold stream
    given the same buffered duration -- some state is reused, amount unknown.
  - transcribe_without_streaming() costs ~2x moonshine_transcribe_stream on the
    same audio -- a different entry point, not a different amount of state.

Usage:
    python scratch/bench_streaming_cost.py
"""

import time
from typing import TypedDict

import soundfile as sf
from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch
from moonshine_voice.download import download_model_from_info

SR = 16000
CLIP_S = 10.0  # transcribe the first 8 seconds
CHUNK_S = 0.032  # 250ms audio chunks, as a live feed would deliver
UPDATE_INTERVAL_S = 1.0  # decode once per 500ms of audio (the Transcriber default)
DECODE_MS = 1.0  # above this, a row did real work rather than just buffering
WAV = "wav/chinese.wav"  # "wav/conversation_60s.wav" # "wav/obama_2012_mono.wav" #
MODEL_LN = "zh"  # "en" #
MODEL_SIZE = "tiny"


# Deliberately duplicated from STTWorker._MODEL_REGISTRY_OVERRIDES rather than
# imported, so this bench runs standalone against nothing but moonshine_voice.
# moonshine hardcodes a (language, arch) -> URL table and five published models
# are missing from it, zh/tiny included, so get_model_for_language() alone
# raises ValueError("Model not found...") on this script's own defaults. Keep
# in sync by hand; stt_worker.py carries the full explanation and the caveat
# that only ko/base among these has been verified.
_MODEL_REGISTRY_OVERRIDES = {
    ("ko", "base"): "https://download.moonshine.ai/model/base-ko/quantized/base-ko",
    ("ar", "tiny"): "https://download.moonshine.ai/model/tiny-ar/quantized/tiny-ar",
    ("uk", "tiny"): "https://download.moonshine.ai/model/tiny-uk/quantized/tiny-uk",
    ("vi", "tiny"): "https://download.moonshine.ai/model/tiny-vi/quantized/tiny-vi",
    ("zh", "tiny"): "https://download.moonshine.ai/model/tiny-zh/quantized/tiny-zh",
}


def resolve_model(language, arch_name):
    """(model_path, ModelArch), consulting the override table first.

    Overridden entries still go through moonshine's own
    download_model_from_info(), so they land in the same cache directory as
    everything else -- only the missing table row is supplied here. The
    spelling-model prefetch get_model_for_language() also does is skipped:
    it's optional, and only "en" publishes one.
    """
    arch = string_to_model_arch(arch_name)
    override_url = _MODEL_REGISTRY_OVERRIDES.get((language, arch_name))
    if override_url is None:
        return get_model_for_language(language, arch)
    return download_model_from_info(
        {
            "model_name": f"{arch_name}-{language}",
            "model_arch": arch,
            "language": language,
            "download_url": override_url,
        }
    )


audio, file_sr = sf.read(WAV, dtype="float32")
assert file_sr == SR, f"expected {SR}Hz, got {file_sr}"
assert audio.ndim == 1, f"expected mono, got {audio.shape[1]} channels"
audio = audio[: int(SR * CLIP_S)]
chunk_len = int(SR * CHUNK_S)
chunks = [audio[i : i + chunk_len] for i in range(0, len(audio), chunk_len)]
print(
    f"\n{CLIP_S:.0f}s of audio, {len(chunks)} x {CHUNK_S * 1000:.0f}ms chunks, "
    f"update_interval={UPDATE_INTERVAL_S * 1000:.0f}ms\n"
)


# Mutated by the listener below. A dict rather than a bare name so the listener
# can write to it without `global` -- moonshine calls this from inside add_audio().
#
# TypedDict, not a plain {"text": "", "completed": False} literal: with mixed
# value types (str and bool) mypy has no single type that fits both, so it
# falls back to `object` for every value -- which is neither Sized nor
# indexable, and breaks len(line["text"]) / line["text"][:58] below even
# though both are always strings at runtime. This gives each key its own type.
class _LineState(TypedDict):
    text: str
    completed: bool


line: _LineState = {"text": "", "completed": False}


def on_event(event):
    """Called by moonshine for every transcript event, during add_audio()."""
    name = type(event).__name__
    if name == "LineTextChanged":
        line["text"] = event.line.text
    elif name == "LineCompleted":
        line["text"] = event.line.text
        line["completed"] = True


# ── 1. NON-STREAMING MODEL ───────────────────────────────────────────────────

path, arch = resolve_model(MODEL_LN, MODEL_SIZE)
tr = Transcriber(
    path,
    arch,
    update_interval=UPDATE_INTERVAL_S,
    options={"max_tokens_per_second": "18.0", "vad_threshold": "0"},
)
tr.add_listener(on_event)

print(f"1. {MODEL_SIZE}-{MODEL_LN} (NON-streaming)")
print(f"   {'audio':>6} {'update':>9} {'chars':>6}  transcript so far")
line["text"], line["completed"] = "", False
tr.start()
times_nonstreaming = []
for i, chunk in enumerate(chunks, start=1):
    t0 = time.perf_counter()
    tr.add_audio(chunk.tolist(), SR)
    dt = (time.perf_counter() - t0) * 1000
    times_nonstreaming.append(dt)
    if dt < DECODE_MS:
        continue  # buffered only -- no decode ran, so no new text
    mark = "  <- LINE COMPLETED" if line["completed"] else ""
    print(
        f"   {i * CHUNK_S:>5.2f}s {dt:>8.1f}ms {len(line['text']):>6}  {line['text'][:58]!r}{mark}"
    )
    if line["completed"]:
        line["text"], line["completed"] = "", False

t0 = time.perf_counter()
transcript = tr.stop()
stop_ms = (time.perf_counter() - t0) * 1000
text = " ".join(x.text for x in transcript.lines).strip()
print(f"   {'stop()':>6} {stop_ms:>8.1f}ms")
print(f"   final: {text[:70]!r}")
decodes = [t for t in times_nonstreaming if t > DECODE_MS]
print(f"   decodes: {len(decodes)}  first={decodes[0]:.0f}ms  last={decodes[-1]:.0f}ms\n")


# ── 2. STREAMING MODEL ───────────────────────────────────────────────────────

# path, arch = get_model_for_language("en", string_to_model_arch("tiny-streaming"))
# tr_s = Transcriber(
#     path,
#     arch,
#     update_interval=UPDATE_INTERVAL_S,
#     options={"max_tokens_per_second": "18.0", "vad_threshold": "0"},
# )
# tr_s.add_listener(on_event)

# print("2. tiny-streaming-en (STREAMING)")
# print(f"   {'audio':>6} {'update':>9} {'chars':>6}  transcript so far")
# line["text"], line["completed"] = "", False
# tr_s.start()
# times_streaming = []
# for i, chunk in enumerate(chunks, start=1):
#     t0 = time.perf_counter()
#     tr_s.add_audio(chunk.tolist(), SR)
#     dt = (time.perf_counter() - t0) * 1000
#     times_streaming.append(dt)
#     if dt < DECODE_MS:
#         continue
#     mark = "  <- LINE COMPLETED" if line["completed"] else ""
#     print(
#         f"   {i * CHUNK_S:>5.2f}s {dt:>8.1f}ms {len(line['text']):>6}  {line['text'][:58]!r}{mark}"
#     )
#     if line["completed"]:
#         line["text"], line["completed"] = "", False

# t0 = time.perf_counter()
# transcript_s = tr_s.stop()
# stop_ms_s = (time.perf_counter() - t0) * 1000
# text_s = " ".join(x.text for x in transcript_s.lines).strip()
# print(f"   {'stop()':>6} {stop_ms_s:>8.1f}ms")
# print(f"   final: {text_s[:70]!r}")
# decodes_s = [t for t in times_streaming if t > DECODE_MS]
# print(f"   decodes: {len(decodes_s)}  first={decodes_s[0]:.0f}ms  last={decodes_s[-1]:.0f}ms\n")


# ── 3. DOES A STREAM REUSE ANYTHING BETWEEN UPDATES? ─────────────────────────
#
# If each update re-runs the whole accumulated buffer, then a stream that has
# been updating all along (WARM) should cost the same as a fresh stream seeing
# the same audio for the first time (COLD).
#
# Both sides must use the SAME native call. An earlier version of this compared
# against transcribe_without_streaming(), which is a different entry point --
# it costs ~2x moonshine_transcribe_stream on identical audio, so it made the
# stream look like it was reusing state when the difference was just the
# function. Same call, one variable, or the result means nothing.
#
# ORDER MATTERS TOO, and an earlier version got this wrong: it ran the whole
# warm sweep (1s..8s) first, then all 8 cold trials second. Anything that
# warms up over the life of the process -- CPU frequency ramping off idle,
# ONNX Runtime's thread pool, the OS page cache for the model's .ort files --
# gets paid for during the warm sweep and is free by the time cold runs. That
# showed up as "cold is cheaper," most visibly at 1s (each stream's very
# first-ever decode, so warm/cold should be identical there and were not).
# Fix: alternate which sweep goes first across repetitions, so process
# warmup lands on both conditions equally, and report the median.

# print("3. tiny-en: warm stream vs cold stream, same native call (order-balanced)")

# REPEATS = 6


# def warm_sweep():
#     st = tr.create_stream(update_interval=UPDATE_INTERVAL_S)
#     st.start()
#     for i in range(0, len(audio), chunk_len):
#         t0 = time.perf_counter()
#         st.add_audio(audio[i : i + chunk_len].tolist(), SR)
#         dt = (time.perf_counter() - t0) * 1000
#         secs = (i + chunk_len) / SR
#         if dt > DECODE_MS and abs(secs - round(secs)) < 1e-6:
#             warm_times[int(secs)].append(dt)
#     st.stop()
#     st.close()


# def cold_sweep():
#     for secs in range(1, int(CLIP_S) + 1):
#         st = tr.create_stream(update_interval=1e9)  # so no implicit update fires
#         st.start()
#         for i in range(0, SR * secs, chunk_len):
#             st.add_audio(audio[i : i + chunk_len].tolist(), SR)
#         t0 = time.perf_counter()
#         st.update_transcription()  # the first and only decode this stream does
#         cold_times[secs].append((time.perf_counter() - t0) * 1000)
#         st.stop()
#         st.close()


# def median(values):
#     ordered = sorted(values)
#     mid = len(ordered) // 2
#     if len(ordered) % 2:
#         return ordered[mid]
#     return (ordered[mid - 1] + ordered[mid]) / 2


# # dict[int, list[float]] stated explicitly: every value starts as an empty
# # list, and mypy has no element type to infer from an empty literal.
# warm_times: dict[int, list[float]] = {secs: [] for secs in range(1, int(CLIP_S) + 1)}
# cold_times: dict[int, list[float]] = {secs: [] for secs in range(1, int(CLIP_S) + 1)}

# # One untimed pass first, so first-ever-call overhead (JIT/cache warmup) is
# # paid before rep 0 rather than landing inside whichever sweep runs first.
# warm_sweep()
# warm_times = {secs: [] for secs in range(1, int(CLIP_S) + 1)}  # discard it

# for rep in range(REPEATS):
#     if rep % 2 == 0:
#         warm_sweep()
#         cold_sweep()
#     else:
#         cold_sweep()
#         warm_sweep()

# print(f"   {'buffer':>6} {'warm (median)':>14} {'cold (median)':>14}   ratio")
# for secs in range(1, int(CLIP_S) + 1):
#     w = median(warm_times[secs])
#     c = median(cold_times[secs])
#     print(f"   {secs:>5}s {w:>13.1f}ms {c:>13.1f}ms   {c / w:.2f}x")
# print()


# # ── COMPARISON ───────────────────────────────────────────────────────────────

# print("SUMMARY (x86, not a Pi -- compare the shapes, not the absolute numbers)")
# print(f"   {'':22} {'total':>9} {'slowest update':>16} {'last update':>13}")
# print(
#     f"   {'tiny-en (non-stream)':22} {sum(times_nonstreaming) + stop_ms:>8.0f}ms "
#     f"{max(times_nonstreaming):>15.0f}ms {decodes[-1]:>12.0f}ms"
# )
# print(
#     f"   {'tiny-streaming-en':22} {sum(times_streaming) + stop_ms_s:>8.0f}ms "
#     f"{max(times_streaming):>15.0f}ms {decodes_s[-1]:>12.0f}ms"
# )
# print()
# print("Per-update cost grows with accumulated audio for both models, and streaming")
# print("costs ~2x non-streaming per update throughout (it is also the bigger model).")
# print("Experiment 3 (order-balanced) shows warm streams cost LESS than cold ones at")
# print("the same buffered duration -- some state IS reused between updates, contrary")
# print("to what an order-confounded version of this comparison suggested.")
