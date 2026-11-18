#!/usr/bin/env python3
# Do two STT *processes* decode in parallel, where two threads did not?
# Usage: python scratch/probe_mp_speedup.py

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import time

import numpy as np
import soundfile as sf


def _load_audio(path: str, sample_rate: int) -> np.ndarray:
    data, file_sr = sf.read(path, dtype="int16")
    if data.ndim > 1:
        data = data[:, 0]
    if file_sr != sample_rate:
        import torch
        import torchaudio

        tensor = torch.as_tensor(data, dtype=torch.float32).unsqueeze(0)
        resampler = torchaudio.transforms.Resample(file_sr, sample_rate, lowpass_filter_width=64)
        data = resampler(tensor).squeeze(0).numpy().astype(np.int16)
    return data


def _decode_child(
    idx: int,
    pcm_bytes: bytes,
    sample_rate: int,
    iterations: int,
    model_path: str,
    model_arch,
    options: dict,
    ready_ev,
    go_ev,
    result_q,
) -> None:
    from moonshine_voice import Transcriber

    from edge_voice.stt.stt_worker import _make_collector

    transcriber = Transcriber(model_path, model_arch, options=options)
    samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32768.0
    samples_list = samples.clip(-1.0, 1.0).tolist()

    ready_ev.set()
    go_ev.wait()

    intervals = []
    for i in range(iterations):
        collector = _make_collector(0.45, f"probe-{idx}-{i}")
        transcriber.remove_all_listeners()
        transcriber.add_listener(collector)
        t0 = time.monotonic()
        transcriber.start()
        try:
            transcriber.add_audio(samples_list, sample_rate)
        finally:
            transcriber.stop()
        intervals.append((t0, time.monotonic()))

    result_q.put((idx, intervals))


def _run_round(ctx, n_children: int, child_args: tuple) -> tuple[float, list]:
    ready = [ctx.Event() for _ in range(n_children)]
    go = ctx.Event()
    result_q = ctx.Queue()

    procs = []
    for idx in range(n_children):
        p = ctx.Process(
            target=_decode_child,
            args=(idx, *child_args, ready[idx], go, result_q),
            daemon=True,
        )
        p.start()
        procs.append(p)

    for ev in ready:
        if not ev.wait(timeout=300):
            raise RuntimeError("a child never finished loading its model within 300s")

    t0 = time.monotonic()
    go.set()
    results = [result_q.get(timeout=600) for _ in range(n_children)]
    wall = time.monotonic() - t0
    for p in procs:
        p.join(timeout=30)
    return wall, [intervals for _, intervals in sorted(results)]


def _overlap_seconds(a: list, b: list) -> float:
    total = 0.0
    for a0, a1 in a:
        for b0, b1 in b:
            lo, hi = max(a0, b0), min(a1, b1)
            if hi > lo:
                total += hi - lo
    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--language", default="ko")
    parser.add_argument("--arch", default="tiny")
    parser.add_argument(
        "--iterations", type=int, default=8, help="Decode calls per child (default 8)"
    )
    parser.add_argument("--wav", default="wav/rx_recorded_1.wav")
    parser.add_argument("--sr", type=int, default=16000)
    parser.add_argument("--clip-s", type=float, default=3.0)
    parser.add_argument(
        "--no-single-thread",
        action="store_true",
        help="Leave MOONSHINE_ORT_SINGLE_THREAD unset. Measured 0.73x that way on a "
        "32-core dev box vs 1.69x with it -- each process spawns its own full ORT "
        "thread pool and they oversubscribe the machine. Off by default so this "
        "probe measures the deployed configuration, not a misleading one.",
    )
    args = parser.parse_args()

    if not args.no_single_thread:
        os.environ["MOONSHINE_ORT_SINGLE_THREAD"] = "1"
    mode = "unset (oversubscribed)" if args.no_single_thread else "1 (production config)"
    print(f"MOONSHINE_ORT_SINGLE_THREAD={mode}")

    from moonshine_voice import get_model_for_language, string_to_model_arch

    model_path, model_arch = get_model_for_language(args.language, string_to_model_arch(args.arch))
    options = {"max_tokens_per_second": "18.0", "vad_threshold": "0"}

    audio = _load_audio(args.wav, args.sr)
    pcm = audio[: int(args.sr * args.clip_s)].tobytes()
    child_args = (pcm, args.sr, args.iterations, model_path, model_arch, options)

    ctx = mp.get_context("spawn")

    print(f"{args.arch}-{args.language}, {args.iterations} decodes/child, model load excluded\n")

    seq_wall = 0.0
    for round_idx in range(2):
        wall, _ = _run_round(ctx, 1, child_args)
        print(f"Sequential round {round_idx + 1} (1 process): {wall:.2f}s")
        seq_wall += wall
    print(f"Sequential total, {args.iterations * 2} calls: {seq_wall:.2f}s")

    conc_wall, intervals = _run_round(ctx, 2, child_args)
    print(f"Concurrent (2 processes), {args.iterations * 2} calls: {conc_wall:.2f}s")

    overlap = _overlap_seconds(intervals[0], intervals[1])
    busiest = max(sum(b - a for a, b in ch) for ch in intervals)
    overlap_frac = overlap / busiest if busiest > 0 else 0.0
    speedup = seq_wall / conc_wall if conc_wall > 0 else float("nan")

    print(f"\nCompute overlap between the two processes: {overlap:.2f}s")
    print(f"  busiest child decoded for {busiest:.2f}s -> {overlap_frac:.0%} of it overlapped")
    print(f"Speedup, concurrent vs sequential: {speedup:.2f}x")
    print("(1.0x = no benefit; 2.0x = perfect two-core parallelism)")
    print("\nFor comparison, two THREADS on the RPi5 measured 1.02x (probe_gil_release.py).")

    print()
    if overlap_frac >= 0.8 and speedup >= 1.4:
        print(
            f"GATE PASSED: {overlap_frac:.0%} of decode time genuinely overlapped and throughput\n"
            f"rose {speedup:.2f}x (vs 1.02x for threads). Processes deliver real parallelism.\n"
            "Proceed to Step 1."
        )
        if speedup < 1.8:
            print(
                f"\nNote: {speedup:.2f}x, not ~2x, despite near-total overlap -- the two\n"
                "processes contend for something below the CPU (most likely memory\n"
                "bandwidth / shared cache on a small memory-bound model). Expect the\n"
                "real-world gain to track this number, not a theoretical 2x."
            )
    elif overlap_frac >= 0.8:
        print(
            f"GATE AMBIGUOUS: {overlap_frac:.0%} overlap (parallelism works) but only\n"
            f"{speedup:.2f}x throughput -- something below the CPU is the real limit.\n"
            "Investigate before building; the payoff will be small."
        )
    else:
        print(
            f"GATE FAILED: only {overlap_frac:.0%} of decode time overlapped ({speedup:.2f}x).\n"
            "STOP -- the processes are still serializing. Re-scope the plan.\n"
            "First check MOONSHINE_ORT_SINGLE_THREAD (see --no-single-thread)."
        )


if __name__ == "__main__":
    main()
