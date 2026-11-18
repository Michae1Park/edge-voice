# One-off: compare Silero VAD's torch.jit (torch.hub) backend against its onnxruntime backend,
# replicating the app's real per-channel usage (own model + VADIterator per channel, 320-sample/20ms
# chunks ...
# Usage: python scratch/bench_vad_backend.py

import time

import numpy as np
import torch

N_WARMUP = 50
N_CALLS = 2000
CHUNK_SAMPLES = 512
SAMPLE_RATE = 16000


def _load_jit():
    from silero_vad.utils_vad import init_jit_model
    import importlib_resources as impresources

    path = str(impresources.files("silero_vad.data").joinpath("silero_vad.jit"))
    return init_jit_model(path)


def _load_onnx():
    from silero_vad import load_silero_vad

    return load_silero_vad(onnx=True)


def _new_iterator(model):
    from silero_vad.utils_vad import VADIterator

    return VADIterator(model, threshold=0.5, sampling_rate=SAMPLE_RATE)


def bench(name, load_fn):
    t0 = time.perf_counter()
    model = load_fn()
    load_s = time.perf_counter() - t0

    vad_iter = _new_iterator(model)
    rng = np.random.default_rng(0)
    chunks = [
        torch.from_numpy(rng.uniform(-0.05, 0.05, CHUNK_SAMPLES).astype(np.float32))
        for _ in range(N_WARMUP + N_CALLS)
    ]

    for x in chunks[:N_WARMUP]:
        vad_iter(x)

    latencies = []
    for x in chunks[N_WARMUP:]:
        t0 = time.perf_counter()
        vad_iter(x)
        latencies.append(time.perf_counter() - t0)

    arr = np.array(latencies) * 1000
    print(f"\n=== {name} ===")
    print(f"load time:        {load_s * 1000:.1f} ms")
    print(f"per-call mean:    {arr.mean():.4f} ms")
    print(f"per-call p50:     {np.percentile(arr, 50):.4f} ms")
    print(f"per-call p95:     {np.percentile(arr, 95):.4f} ms")
    print(f"per-call max:     {arr.max():.4f} ms")
    print(f"total ({N_CALLS} calls): {arr.sum():.1f} ms")
    print(f"torch.get_num_threads(): {torch.get_num_threads()}")
    return arr


def bench_hub_load_overhead():
    import torch as _torch

    t0 = time.perf_counter()
    _torch.hub.load(
        repo_or_dir="snakers4/silero-vad",
        model="silero_vad",
        force_reload=False,
        trust_repo=True,
    )
    dt = time.perf_counter() - t0
    print("\n=== torch.hub.load() end-to-end (cached repo) ===")
    print(f"{dt * 1000:.1f} ms")


if __name__ == "__main__":
    jit_lat = bench("torch.jit (old backend)", _load_jit)
    onnx_lat = bench("onnxruntime (new backend)", _load_onnx)

    speedup = jit_lat.mean() / onnx_lat.mean()
    print("\n=== summary ===")
    print(f"onnxruntime is {speedup:.2f}x faster per call (mean) on this machine")

    bench_hub_load_overhead()
