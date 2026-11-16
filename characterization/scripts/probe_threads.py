import os
import time

import soundfile as sf
from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch


def thread_count() -> int:
    return len(os.listdir("/proc/self/task"))


before_model = thread_count()
path, arch = get_model_for_language("ko", string_to_model_arch("tiny"))
files = sorted(os.listdir(path)) if os.path.isdir(path) else [path]
transcriber = Transcriber(model_path=path, model_arch=arch)
after_load = thread_count()

audio, sr = sf.read("wav/rx_recorded_1.wav", dtype="float32")
if audio.ndim > 1:
    audio = audio[:, 0]
audio = audio[: 3 * sr]

transcriber.start()
transcriber.add_audio(audio.tolist(), sr)
time.sleep(0.3)
during_decode = thread_count()
transcriber.stop()
time.sleep(0.5)

model_files = [f for f in files if f.endswith((".ort", ".onnx"))]
print(
    f"flag={os.environ.get('MOONSHINE_ORT_SINGLE_THREAD')} "
    f"cpus_allowed={len(os.sched_getaffinity(0))} os.cpu_count={os.cpu_count()} "
    f"model_files={model_files}"
)
print(
    f"threads: before_model={before_model} after_load={after_load} "
    f"during_decode={during_decode} after={thread_count()}"
)
