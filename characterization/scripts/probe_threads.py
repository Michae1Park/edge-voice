import os, sys, glob, time
from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch
def n(): return len(os.listdir("/proc/self/task"))
b0 = n()
path, arch = get_model_for_language("ko", string_to_model_arch("tiny"))
files = sorted(os.listdir(path)) if os.path.isdir(path) else [path]
t = Transcriber(model_path=path, model_arch=arch)
b1 = n()
import soundfile as sf, numpy as np
a, sr = sf.read("wav/rx_recorded_1.wav", dtype="float32")
if a.ndim > 1: a = a[:, 0]
a = a[: 3*sr]
t.start(); t.add_audio(a.tolist(), sr); 
time.sleep(0.3); mid = n()
t.stop(); time.sleep(0.5)
print(f"flag={os.environ.get('MOONSHINE_ORT_SINGLE_THREAD')} cpus_allowed={len(os.sched_getaffinity(0))} os.cpu_count={os.cpu_count()} model_files={[f for f in files if f.endswith(('.ort','.onnx'))]}")
print(f"threads: before_model={b0} after_load={b1} during_decode={mid} after={n()}")
