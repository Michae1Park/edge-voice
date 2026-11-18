#!/usr/bin/env python3

import time
from typing import TypedDict

import soundfile as sf
from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch
from moonshine_voice.download import download_model_from_info

SR = 16000
CLIP_S = 10.0
CHUNK_S = 0.032
UPDATE_INTERVAL_S = 1.0
DECODE_MS = 1.0
WAV = "wav/chinese.wav"
MODEL_LN = "zh"
MODEL_SIZE = "tiny"


_MODEL_REGISTRY_OVERRIDES = {
    ("ko", "base"): "https://download.moonshine.ai/model/base-ko/quantized/base-ko",
    ("ar", "tiny"): "https://download.moonshine.ai/model/tiny-ar/quantized/tiny-ar",
    ("uk", "tiny"): "https://download.moonshine.ai/model/tiny-uk/quantized/tiny-uk",
    ("vi", "tiny"): "https://download.moonshine.ai/model/tiny-vi/quantized/tiny-vi",
    ("zh", "tiny"): "https://download.moonshine.ai/model/tiny-zh/quantized/tiny-zh",
}


def resolve_model(language, arch_name):
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


class _LineState(TypedDict):
    text: str
    completed: bool


line: _LineState = {"text": "", "completed": False}


def on_event(event):
    name = type(event).__name__
    if name == "LineTextChanged":
        line["text"] = event.line.text
    elif name == "LineCompleted":
        line["text"] = event.line.text
        line["completed"] = True


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
        continue
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


