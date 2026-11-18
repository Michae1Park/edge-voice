#!/usr/bin/env python3
# How much RSS does a second Transcriber instance actually cost, on THIS hardware/model/moonshine-
# voice version -- not the "~175MB" figure in stt_worker.py's module docstring, which wasn't
# necessarily ...
# Usage: python scratch/probe_decoder_memory.py

from __future__ import annotations

import argparse
import gc
import time


def _rss_mb() -> float:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--language", default="ko", help="Matches configs/default.yaml (default ko)"
    )
    parser.add_argument("--arch", default="tiny")
    parser.add_argument(
        "--count", type=int, default=2, help="How many instances to load, one at a time (default 2)"
    )
    args = parser.parse_args()

    baseline = _rss_mb()
    print(f"baseline RSS: {baseline:.1f}MB (python interpreter, nothing loaded yet)\n")

    from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch

    model_path, model_arch = get_model_for_language(args.language, string_to_model_arch(args.arch))

    instances = []
    prev_rss = baseline
    first_instance_rss = None
    for i in range(1, args.count + 1):
        t0 = time.perf_counter()
        tr = Transcriber(
            model_path, model_arch, options={"max_tokens_per_second": "18.0", "vad_threshold": "0"}
        )
        load_s = time.perf_counter() - t0
        instances.append(tr)
        gc.collect()
        rss = _rss_mb()
        if i == 1:
            first_instance_rss = rss
        print(
            f"instance {i}: loaded in {load_s:.2f}s -- RSS {rss:.1f}MB "
            f"(+{rss - prev_rss:.1f}MB this instance, +{rss - baseline:.1f}MB total)"
        )
        prev_rss = rss

    print(f"\n{'=' * 70}")
    if args.count >= 2 and first_instance_rss is not None:
        marginal = (prev_rss - first_instance_rss) / (args.count - 1)
        print(
            f"Marginal cost per instance BEYOND the first (the number that "
            f"answers 'what would one more channel cost' -- the first "
            f"instance's jump includes one-time ONNX Runtime init a real "
            f"second channel wouldn't pay again): {marginal:.1f}MB"
        )
    print("stt_worker.py's docstring claims ~175MB/channel -- compare against the above.")
    print("Cross-check with `free -h` run alongside this script for the system-wide picture.")


if __name__ == "__main__":
    main()
