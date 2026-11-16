import os, sys, time, wave, threading
from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch
TCK = os.sysconf("SC_CLK_TCK")
def snap():
    d = {}
    for tid in os.listdir("/proc/self/task"):
        try:
            s = open(f"/proc/self/task/{tid}/stat").read()
            r = s.rsplit(")", 1)[1].split()
            d[tid] = (int(r[11]) + int(r[12]), r[0])   # utime+stime ticks, state
        except OSError: pass
    return d
w = wave.open(sys.argv[1]); sr = w.getframerate()
raw = w.readframes(int(6 * sr)); import array
a = array.array("h"); a.frombytes(raw); audio = [x / 32768 for x in a]
path, arch = get_model_for_language("ko", string_to_model_arch("tiny"))
t = Transcriber(model_path=path, model_arch=arch)
pool = len(snap()) - 1
for _ in range(2): t.transcribe_without_streaming(audio, sr)      # warm-up
before = snap(); running_max = 0; stop = False
def sampler():
    global running_max
    while not stop:
        running_max = max(running_max, sum(1 for v in snap().values() if v[1] == "R")); time.sleep(0.004)
th = threading.Thread(target=sampler, daemon=True); th.start()
N = 5; t0 = time.perf_counter()
for _ in range(N): tr = t.transcribe_without_streaming(audio, sr)
wall = time.perf_counter() - t0
after = snap(); stop = True; th.join()
print('  text:', ' '.join(l.text for l in tr.lines)[:60], '| audio_s=%.1f sr=%d' % (len(audio)/sr, sr))
cpu = sorted(((after[k][0] - before.get(k, (0,))[0]) / TCK for k in after), reverse=True)
tot = sum(cpu); busy = [c for c in cpu if c > 0.05 * wall]
print(f"flag={os.environ.get('MOONSHINE_ORT_SINGLE_THREAD')} allowed_cpus={len(os.sched_getaffinity(0))} "
      f"threads_in_process={len(after)} (pool_created_at_load={pool})")
print(f"  decode wall/call={wall/N*1000:.0f}ms  total CPU/call={tot/N*1000:.0f}ms  avg parallelism={tot/wall:.2f}")
print(f"  threads with any CPU time: {sum(1 for c in cpu if c>0)}   >5% of wall: {len(busy)}   max simultaneously RUNNING (sampled): {running_max}")
print(f"  top CPU-s per thread: {[round(c,2) for c in cpu[:8]]}  | bottom: {[round(c,2) for c in cpu[-3:]]}")
