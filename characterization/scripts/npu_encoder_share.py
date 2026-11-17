import os
import statistics as st
import sys
import time

os.environ.setdefault("MOONSHINE_ORT_SINGLE_THREAD", "1")
import numpy as np
import onnxruntime as ort
import soundfile as sf
from moonshine_voice import ModelArch, Transcriber

MODEL = os.path.expanduser(
    "~/.cache/moonshine_voice/download.moonshine.ai/model/tiny-ko/quantized/tiny-ko"
)
ENC = sys.argv[1]
audio, sr = sf.read(sys.argv[2], dtype="float32")
if audio.ndim > 1:
    audio = audio[:, 0]
audio = np.interp(np.arange(0, len(audio), sr / 16000), np.arange(len(audio)), audio).astype(
    np.float32
)
opts = {"max_tokens_per_second": "20.0", "vad_threshold": "0"}
t = Transcriber(model_path=MODEL, model_arch=ModelArch.TINY, options=opts)
so = ort.SessionOptions()
so.intra_op_num_threads = 1
so.inter_op_num_threads = 1
enc = ort.InferenceSession(ENC, so, providers=["CPUExecutionProvider"])
name = enc.get_inputs()[0].name
print("enc inputs", [(i.name, i.shape) for i in enc.get_inputs()])
for secs in (2, 4, 6, 10):
    x = audio[: secs * 16000]
    if len(x) < secs * 16000:
        break
    e, f, txt = [], [], ""
    for i in range(6):
        a = time.perf_counter()
        enc.run(None, {name: x[None, :], "attention_mask": np.ones((1, len(x)), np.int64)})
        b = time.perf_counter()
        txt = " ".join(line.text for line in t.transcribe_without_streaming(x, 16000).lines)
        c = time.perf_counter()
        if i:
            e.append(b - a)
            f.append(c - b)
    me, mf = st.median(e), st.median(f)
    print(
        f"{secs:>2}s  encoder {me * 1000:7.1f} ms  full {mf * 1000:7.1f} ms  share {me / mf:5.1%}  chars {len(txt)}"
    )
