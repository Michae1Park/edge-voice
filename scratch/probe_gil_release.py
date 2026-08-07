#!/usr/bin/env python3
"""Does moonshine_voice's native decode call release the GIL?

Two Transcriber instances, two threads, each hammering the same
start/add_audio/stop cycle stt_worker.py's _transcribe() uses -- no
orchestrator, no queues, no VAD, nothing but the two decode calls
themselves. If the underlying native call releases the GIL for its
compute-heavy portion, two threads should make real progress at the same
time and a concurrent run should take noticeably less wall-clock time than
running the same two workloads one after another. If it doesn't release
the GIL, concurrent and sequential wall-clock time should be about the
same -- only one thread can ever actually be executing at once, regardless
of how many cores or how many Transcriber instances exist.

This isolates the specific question bench_pipeline_load.py's RPi5 run
couldn't answer on its own: it showed decode calls alternating in lockstep
(one channel's decode starts the instant the other's ends) even with two
independent per-channel Transcriber instances on two dedicated cores --
consistent with GIL serialization, but not a direct test of it, since the
real pipeline has other moving parts (VAD, queues, real conversational
turn-taking) that could in principle produce a similar-looking pattern for
other reasons. This script removes all of those and tests the one claim
directly: does the call itself release the GIL, yes or no.

Uses stt_worker.py's own _make_collector() for the listener, so the call
shape matches production exactly -- the only thing skipped is the
orchestrator/queue plumbing around it, which is irrelevant to whether the
GIL is held during the call.

Run this on the RPi5, not a dev box -- moonshine_voice ships different
compiled binaries per platform, and a dev-machine result (e.g. x86, many
cores) doesn't necessarily say anything about the ARM build's locking
behavior. A smoke test on a 32-core x86 dev box showed partial speedup
(~1.4x, real but incomplete overlap) with this same script -- that's not
"no GIL issue," it's a different binary; the RPi5 number is the one that
actually answers the question for deployment.

Usage:
    python scratch/probe_gil_release.py
    python scratch/probe_gil_release.py --language ko --arch tiny --iterations 10
"""

from __future__ import annotations

import argparse
import threading
import time

import numpy as np
import soundfile as sf


def _load_audio(path: str, sample_rate: int) -> np.ndarray:
    data, file_sr = sf.read(path, dtype="int16")
    if data.ndim > 1:
        data = data[:, 0]
    if file_sr != sample_rate:
        # Same resample path wav_source_raw.py/bench_ort_threads.py use --
        # the wav/*.wav fixtures aren't all natively at the pipeline's
        # configured rate.
        import torch
        import torchaudio

        tensor = torch.as_tensor(data, dtype=torch.float32).unsqueeze(0)
        resampler = torchaudio.transforms.Resample(file_sr, sample_rate, lowpass_filter_width=64)
        data = resampler(tensor).squeeze(0).numpy().astype(np.int16)
    return data


def _pcm_to_float32(samples: np.ndarray) -> np.ndarray:
    # Same normalization as STTWorker._pcm_to_float32 -- int16 range to [-1, 1].
    return (samples.astype(np.float32) / 32768.0).clip(-1.0, 1.0)


def _decode_loop(
    transcriber, samples_list: list, sample_rate: int, iterations: int, intervals: list
) -> None:
    """One full start/add_audio/stop cycle per iteration -- same shape as
    stt_worker.py's _transcribe(), repeated back to back with no gap, so
    any GIL release shows up as real wall-clock overlap between threads."""
    from edge_voice.stt.stt_worker import _make_collector

    for i in range(iterations):
        collector = _make_collector(0.45, f"probe-{i}")
        transcriber.remove_all_listeners()
        transcriber.add_listener(collector)
        t0 = time.perf_counter()
        transcriber.start()
        try:
            transcriber.add_audio(samples_list, sample_rate)
        finally:
            transcriber.stop()
        t1 = time.perf_counter()
        intervals.append((t0, t1))


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
    parser.add_argument(
        "--language", default="ko", help="Matches configs/default.yaml (default ko)"
    )
    parser.add_argument("--arch", default="tiny")
    parser.add_argument(
        "--iterations", type=int, default=8, help="Decode calls per thread (default 8)"
    )
    parser.add_argument("--wav", default="wav/rx_recorded_1.wav")
    parser.add_argument("--sr", type=int, default=16000)
    parser.add_argument(
        "--clip-s", type=float, default=3.0, help="Seconds of audio per decode call (default 3.0)"
    )
    args = parser.parse_args()

    from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch

    model_path, model_arch = get_model_for_language(args.language, string_to_model_arch(args.arch))
    # Same options as production (configs/default.yaml's stt: block).
    options = {"max_tokens_per_second": "18.0", "vad_threshold": "0"}

    audio = _load_audio(args.wav, args.sr)
    clip = audio[: int(args.sr * args.clip_s)]
    samples_list = _pcm_to_float32(clip).tolist()

    print(f"Loading two independent {args.arch}-{args.language} Transcriber instances...")
    tr_a = Transcriber(model_path, model_arch, options=options)
    tr_b = Transcriber(model_path, model_arch, options=options)

    # 1. SEQUENTIAL baseline: A's whole loop, then B's whole loop, no
    # threading at all -- the true cost of the work with zero concurrency
    # to possibly exploit, whatever the GIL situation turns out to be.
    intervals_a_seq: list = []
    intervals_b_seq: list = []
    t0 = time.perf_counter()
    _decode_loop(tr_a, samples_list, args.sr, args.iterations, intervals_a_seq)
    _decode_loop(tr_b, samples_list, args.sr, args.iterations, intervals_b_seq)
    sequential_wall_s = time.perf_counter() - t0
    print(f"\nSequential (no threads), {args.iterations * 2} total calls: {sequential_wall_s:.2f}s")

    # 2. CONCURRENT: same work, two threads, at the same time.
    intervals_a: list = []
    intervals_b: list = []
    thread_a = threading.Thread(
        target=_decode_loop, args=(tr_a, samples_list, args.sr, args.iterations, intervals_a)
    )
    thread_b = threading.Thread(
        target=_decode_loop, args=(tr_b, samples_list, args.sr, args.iterations, intervals_b)
    )
    t0 = time.perf_counter()
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()
    concurrent_wall_s = time.perf_counter() - t0
    print(f"Concurrent (2 threads), {args.iterations * 2} total calls:  {concurrent_wall_s:.2f}s")

    overlap_s = _overlap_seconds(intervals_a, intervals_b)
    total_compute_s = sum(b - a for a, b in intervals_a) + sum(b - a for a, b in intervals_b)
    speedup = sequential_wall_s / concurrent_wall_s if concurrent_wall_s > 0 else float("nan")

    print(f"\nOverlap between the two threads' decode calls: {overlap_s:.2f}s")
    print(f"Speedup from running concurrently vs sequentially: {speedup:.2f}x")
    print("(For reference: 1.0x = no benefit at all, 2.0x = perfect 2-core parallelism)")

    print()
    if speedup < 1.15 and overlap_s < 0.05 * total_compute_s:
        print(
            "VERDICT: ~1x speedup, negligible overlap -- consistent with the GIL (or some other\n"
            "lock) being held for the duration of the native call. Two threads bought nothing\n"
            "here, which matches the alternating decode pattern seen on the RPi5 pipeline run."
        )
    elif speedup > 1.7:
        print(
            "VERDICT: near-2x speedup, real overlap measured -- the native call DOES release\n"
            "the GIL. The RPi5 alternation pattern has some other cause, worth re-investigating\n"
            "(e.g. a lock inside moonshine_voice itself, not Python's GIL)."
        )
    else:
        print(
            "VERDICT: partial speedup -- some overlap, but not the full 2x two independent\n"
            "cores should give. The GIL may be released for only part of the call (e.g. the\n"
            "compute itself but not setup/teardown), or something else is contending too."
        )


if __name__ == "__main__":
    main()
