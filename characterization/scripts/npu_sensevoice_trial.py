import json
from pathlib import Path
import os
import re
import statistics as st
import sys
import time

os.environ.setdefault("MOONSHINE_ORT_SINGLE_THREAD", "1")
import soundfile as sf

DATA, BACKEND = sys.argv[1], sys.argv[2]
MAX_S = float(os.environ.get("MAX_S", "10"))


def norm(t):
    return re.sub(r"[\s\W_]+", "", t)


def lev(a, b):
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


clips = []
for m in ("tts", "recorded", "conv"):
    for r in json.loads(Path(f"{DATA}/{m}_metadata.json").read_text()):
        if r["duration_seconds"] <= MAX_S:
            a, sr = sf.read(f"{DATA}/{r['filename']}", dtype="float32")
            clips.append((r["filename"], a, r["transcription"]))

if BACKEND == "moonshine":
    from moonshine_voice import ModelArch, Transcriber

    t = Transcriber(
        model_path=os.path.expanduser(
            "~/.cache/moonshine_voice/download.moonshine.ai/model/tiny-ko/quantized/tiny-ko"
        ),
        model_arch=ModelArch.TINY,
        options={"max_tokens_per_second": "20.0", "vad_threshold": "0"},
    )

    def run(a):
        return " ".join(line.text for line in t.transcribe_without_streaming(a, 16000).lines)
else:
    import sherpa_onnx

    kind, mdir = BACKEND.split(":", 1)
    model = f"{mdir}/model.rknn" if kind == "rknn" else f"{mdir}/model.int8.onnx"
    rec = sherpa_onnx.OfflineRecognizer.from_sense_voice(
        model=model,
        tokens=f"{mdir}/tokens.txt",
        num_threads=1,
        provider="rknn" if kind == "rknn" else "cpu",
        language="ko",
        use_itn=True,
    )

    def run(a):
        s = rec.create_stream()
        s.accept_waveform(16000, a)
        rec.decode_stream(s)
        return s.result.text


run(clips[0][1])
edits = chars = 0
rtfs = []
rows = []
for name, a, ref in clips:
    ts = []
    for _ in range(3):
        t0 = time.perf_counter()
        hyp = run(a)
        ts.append(time.perf_counter() - t0)
    lat = st.median(ts)
    rtfs.append(lat / (len(a) / 16000))
    e = lev(norm(ref), norm(hyp))
    edits += e
    chars += len(norm(ref))
    rows.append(
        {
            "clip": name,
            "dur": round(len(a) / 16000, 2),
            "lat_ms": round(lat * 1000, 1),
            "cer": round(e / max(1, len(norm(ref))), 3),
            "ref": ref,
            "hyp": hyp,
        }
    )
print(
    json.dumps(
        {
            "backend": BACKEND,
            "n": len(clips),
            "cer": round(edits / chars, 4),
            "median_rtf": round(st.median(rtfs), 3),
            "mean_lat_ms": round(1000 * st.mean(r["lat_ms"] for r in rows) / 1000, 1),
            "rows": rows,
        },
        ensure_ascii=False,
    )
)
