#!/usr/bin/env python3
"""torch.hub Silero vs onnxruntime Silero: which explains the RPi5 speedup?

vad_worker.py switched its VAD backend from torch.hub.load(...) to
silero_vad.load_silero_vad(onnx=True) (see git log for
src/edge_voice/vad/vad_worker.py). Inference sped up noticeably on the
RPi5 afterward. Two candidate explanations:

  (1) onnxruntime's per-call inference is just faster than torch eager/JIT
      for this tiny (512-sample) window size.
  (2) torch's forward pass was GIL-bound and blocking some other thread
      from making progress, and onnxruntime releases the GIL more cleanly.

This script measures both directly, the same way probe_gil_release.py does
for the STT decoder: no orchestrator, no queues, just the model calls
themselves.

Note going in: VADWorker itself is single-threaded (see its module
docstring) -- one thread demultiplexes both channels off routed_queue, so
there is no VAD-internal channel parallelism to lose or gain here. This
script still measures (2) directly (two independent model instances, two
threads) since it's a fair general question about the backend, even though
the current architecture doesn't have two VAD threads to begin with.

Run this on the RPi5, not a dev box -- see docs/BENCHMARK.md's note on
probe_gil_release.py for why a many-core x86 box doesn't transfer.

Usage:
    python scratch/probe_vad_backend.py
    python scratch/probe_vad_backend.py --solo-iterations 500 --concurrent-seconds 3.0
"""

from __future__ import annotations

import argparse
import statistics
import threading
import time

import numpy as np
import torch

SR = 16000
WINDOW = 512


def load_torch_hub():
    model, _ = torch.hub.load(
        repo_or_dir="snakers4/silero-vad",
        model="silero_vad",
        force_reload=False,
        trust_repo=True,
        onnx=False,
    )
    return model


def load_onnx():
    from silero_vad import load_silero_vad

    return load_silero_vad(onnx=True)


def _make_window() -> torch.Tensor:
    arr = np.random.uniform(-0.2, 0.2, size=WINDOW).astype(np.float32)
    return torch.from_numpy(arr)


def _call(model, x: torch.Tensor):
    with torch.no_grad():
        return model(x, SR)


def bench_solo(name: str, model, n_warmup: int, n_iters: int) -> float:
    x = _make_window()
    for _ in range(n_warmup):
        _call(model, x)

    latencies = []
    for _ in range(n_iters):
        x = _make_window()
        t0 = time.perf_counter()
        _call(model, x)
        latencies.append(time.perf_counter() - t0)

    latencies.sort()
    mean = statistics.mean(latencies) * 1000
    median = latencies[len(latencies) // 2] * 1000
    p95 = latencies[int(len(latencies) * 0.95)] * 1000
    print(f"[{name}] solo single-thread: mean={mean:.3f}ms median={median:.3f}ms p95={p95:.3f}ms")
    return mean


def _worker_loop(model, stop_flag: threading.Event, counter: list, idx: int) -> None:
    x = _make_window()
    n = 0
    while not stop_flag.is_set():
        _call(model, x)
        n += 1
    counter[idx] = n


def bench_concurrent(name: str, load_fn, n_warmup: int, seconds: float) -> float:
    # Fresh model instance per thread -- same as VADWorker's per-channel
    # model (vad_worker.py's _new_channel_state docstring explains why one
    # shared model across channels corrupts LSTM state).
    model_a = load_fn()
    model_b = load_fn()
    x = _make_window()
    for _ in range(n_warmup):
        _call(model_a, x)
        _call(model_b, x)

    def solo_throughput(model) -> float:
        t_end = time.perf_counter() + seconds
        n = 0
        while time.perf_counter() < t_end:
            _call(model, x)
            n += 1
        return n / seconds

    solo_a = solo_throughput(model_a)
    solo_b = solo_throughput(model_b)
    solo_sum = solo_a + solo_b

    stop_flag = threading.Event()
    counter = [0, 0]
    t1 = threading.Thread(target=_worker_loop, args=(model_a, stop_flag, counter, 0))
    t2 = threading.Thread(target=_worker_loop, args=(model_b, stop_flag, counter, 1))
    t1.start()
    t2.start()
    time.sleep(seconds)
    stop_flag.set()
    t1.join()
    t2.join()

    conc_a = counter[0] / seconds
    conc_b = counter[1] / seconds
    conc_sum = conc_a + conc_b
    scaling = conc_sum / solo_sum if solo_sum else float("nan")

    print(f"[{name}] solo throughput: A={solo_a:.1f}/s B={solo_b:.1f}/s sum={solo_sum:.1f}/s")
    print(
        f"[{name}] concurrent (2 threads) throughput: A={conc_a:.1f}/s B={conc_b:.1f}/s "
        f"sum={conc_sum:.1f}/s  (scaling vs solo-sum: {scaling:.2f}x)"
    )
    return scaling


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--solo-warmup", type=int, default=20)
    parser.add_argument("--solo-iterations", type=int, default=500)
    parser.add_argument("--concurrent-seconds", type=float, default=3.0)
    args = parser.parse_args()

    # Isolate GIL/thread effects from torch's own intra-op parallelism --
    # otherwise a torch.hub call could win or lose purely on how many
    # threads it internally spins up, unrelated to the GIL question.
    torch.set_num_threads(1)

    print("Loading models...")
    torch_model = load_torch_hub()
    onnx_model = load_onnx()

    print("\n=== (1) Pure single-thread forward-pass latency ===")
    bench_solo("torch.hub", torch_model, args.solo_warmup, args.solo_iterations)
    bench_solo("onnxruntime", onnx_model, args.solo_warmup, args.solo_iterations)

    del torch_model, onnx_model

    print("\n=== (2) Two-thread concurrent scaling (GIL contention check) ===")
    print("Loading two independent instances per backend for the concurrency test...")
    bench_concurrent("torch.hub", load_torch_hub, args.solo_warmup, args.concurrent_seconds)
    bench_concurrent("onnxruntime", load_onnx, args.solo_warmup, args.concurrent_seconds)


if __name__ == "__main__":
    main()
