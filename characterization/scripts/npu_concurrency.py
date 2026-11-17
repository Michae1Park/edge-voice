import glob
import json
import multiprocessing as mp
import statistics as st
import sys
import time

import soundfile as sf

MODEL = sys.argv[1]
PINNED_MASKS = [0, -1, -2]


def worker(mask, clips, barrier, q, secs):
    import sherpa_onnx

    rec = sherpa_onnx.OfflineRecognizer.from_sense_voice(
        model=f"{MODEL}/model.rknn",
        tokens=f"{MODEL}/tokens.txt",
        num_threads=mask,
        provider="rknn",
        language="ko",
        use_itn=True,
    )

    def run(a):
        s = rec.create_stream()
        s.accept_waveform(16000, a)
        rec.decode_stream(s)

    run(clips[0])
    barrier.wait()
    lats, end, i = [], time.perf_counter() + secs, 0
    while time.perf_counter() < end:
        t0 = time.perf_counter()
        run(clips[i % len(clips)])
        lats.append(time.perf_counter() - t0)
        i += 1
    q.put(lats)


if __name__ == "__main__":
    mp.set_start_method("spawn")
    clips = [
        sf.read(f, dtype="float32")[0]
        for f in sorted(glob.glob("data/*.wav"))
        if sf.info(f).duration <= 5
    ]
    modes = sys.argv[2].split(",") if len(sys.argv) > 2 else ["auto", "pinned"]
    counts = [int(c) for c in sys.argv[3].split(",")] if len(sys.argv) > 3 else range(1, 7)
    for mode in modes:
        for n in counts:
            masks = [1] * n if mode == "auto" else [PINNED_MASKS[i % 3] for i in range(n)]
            b, q = mp.Barrier(n), mp.Queue()
            ps = [mp.Process(target=worker, args=(m, clips, b, q, 20)) for m in masks]
            [p.start() for p in ps]
            lats = [x for _ in ps for x in q.get(timeout=120)]
            [p.join() for p in ps]
            lats.sort()
            print(
                json.dumps(
                    {
                        "mode": mode,
                        "n": n,
                        "calls": len(lats),
                        "p50_ms": round(1000 * st.median(lats), 1),
                        "p95_ms": round(1000 * lats[int(0.95 * len(lats))], 1),
                        "throughput_per_s": round(len(lats) / 20, 2),
                    }
                ),
                flush=True,
            )
