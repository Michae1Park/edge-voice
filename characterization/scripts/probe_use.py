import array
import os
import sys
import threading
import time
import wave
from pathlib import Path

from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch

CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
CALLS = 5


def snapshot_threads() -> dict[str, tuple[int, str]]:
    cpu_ticks_and_state = {}
    for tid in os.listdir("/proc/self/task"):
        try:
            stat = Path(f"/proc/self/task/{tid}/stat").read_text()
        except OSError:
            continue
        fields = stat.rsplit(")", 1)[1].split()
        utime_plus_stime = int(fields[11]) + int(fields[12])
        cpu_ticks_and_state[tid] = (utime_plus_stime, fields[0])
    return cpu_ticks_and_state


with wave.open(sys.argv[1]) as wav:
    sr = wav.getframerate()
    raw = wav.readframes(int(6 * sr))
samples = array.array("h")
samples.frombytes(raw)
audio = [x / 32768 for x in samples]

path, arch = get_model_for_language("ko", string_to_model_arch("tiny"))
transcriber = Transcriber(model_path=path, model_arch=arch)
pool_size = len(snapshot_threads()) - 1
for _ in range(2):
    transcriber.transcribe_without_streaming(audio, sr)

running_max = 0
stop_sampling = threading.Event()


def sample_running_threads() -> None:
    global running_max
    while not stop_sampling.is_set():
        running = sum(1 for _, state in snapshot_threads().values() if state == "R")
        running_max = max(running_max, running)
        time.sleep(0.004)


before = snapshot_threads()
sampler = threading.Thread(target=sample_running_threads, daemon=True)
sampler.start()
t0 = time.perf_counter()
for _ in range(CALLS):
    transcript = transcriber.transcribe_without_streaming(audio, sr)
wall = time.perf_counter() - t0
after = snapshot_threads()
stop_sampling.set()
sampler.join()

text = " ".join(line.text for line in transcript.lines)[:60]
print(f"  text: {text} | audio_s={len(audio) / sr:.1f} sr={sr}")
cpu = sorted(
    ((after[tid][0] - before.get(tid, (0,))[0]) / CLOCK_TICKS for tid in after),
    reverse=True,
)
total_cpu = sum(cpu)
busy = [c for c in cpu if c > 0.05 * wall]
print(
    f"flag={os.environ.get('MOONSHINE_ORT_SINGLE_THREAD')} "
    f"allowed_cpus={len(os.sched_getaffinity(0))} "
    f"threads_in_process={len(after)} (pool_created_at_load={pool_size})"
)
print(
    f"  decode wall/call={wall / CALLS * 1000:.0f}ms  "
    f"total CPU/call={total_cpu / CALLS * 1000:.0f}ms  "
    f"avg parallelism={total_cpu / wall:.2f}"
)
print(
    f"  threads with any CPU time: {sum(1 for c in cpu if c > 0)}   "
    f">5% of wall: {len(busy)}   "
    f"max simultaneously RUNNING (sampled): {running_max}"
)
print(
    f"  top CPU-s per thread: {[round(c, 2) for c in cpu[:8]]}  "
    f"| bottom: {[round(c, 2) for c in cpu[-3:]]}"
)
