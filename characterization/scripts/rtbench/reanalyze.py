#!/usr/bin/env python3
"""Recompute summary.json (and the terminal report) from an existing
requests.csv + queue_samples.csv, without re-running the benchmark.

The raw per-request/per-sample CSVs are the source of truth (per the
benchmark's "do not only output aggregate statistics" requirement) --
this script exists so a classification-threshold or stats change can be
re-applied to already-collected data instead of burning another ~30
minutes of real hardware time. Reconstructs one LoadLevelResult per
experiment label found in the CSVs; nominal_duration_s and audio_duration_s
must be supplied since the raw rows don't carry them directly (a
request/sample already knows its OWN elapsed/duration, but not the run's
configured window length).

Usage:
    python characterization/scripts/rtbench/reanalyze.py \\
        --results-dir characterization/results_rtbench_pi \\
        --nominal-duration-s 180
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR.parent))

from rtbench.engine import LoadLevelResult, QueueSample, RequestRecord  # noqa: E402
from rtbench.run_bench import print_final_summary, print_level_summary, summarize_level  # noqa: E402

_LOAD_RE = re.compile(r"_(\d+(?:\.\d+)?)x$")


def _parse_load(experiment: str) -> float:
    m = _LOAD_RE.search(experiment)
    if not m:
        raise ValueError(f"Can't parse offered load out of experiment label {experiment!r}")
    return float(m.group(1))


def _load_requests(path: Path) -> list[RequestRecord]:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    out = []
    for r in rows:
        out.append(
            RequestRecord(
                timestamp=r["timestamp"],
                experiment=r["experiment"],
                iteration=int(r["iteration"]),
                audio_duration_s=float(r["audio_duration_s"]),
                arrival_time_s=float(r["arrival_time_s"]),
                asr_start_time_s=float(r["asr_start_time_s"]),
                asr_finish_time_s=float(r["asr_finish_time_s"]),
                queue_wait_ms=float(r["queue_wait_ms"]),
                service_time_ms=float(r["service_time_ms"]),
                e2e_latency_ms=float(r["e2e_latency_ms"]),
                rtf=float(r["rtf"]),
                queue_depth_at_arrival=int(r["queue_depth_at_arrival"]),
                queue_depth_at_start=int(r["queue_depth_at_start"]),
                cpu_percent=float(r["cpu_percent"]),
                cpu_freq_mhz=float(r["cpu_freq_mhz"]),
                cpu_temp_c=float(r["cpu_temp_c"]),
                rss_mb=float(r["rss_mb"]),
                success=r["success"] == "True",
                error=r["error"],
            )
        )
    return out


def _load_samples(path: Path) -> list[QueueSample]:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    out = []
    for r in rows:
        out.append(
            QueueSample(
                experiment=r["experiment"],
                elapsed_s=float(r["elapsed_s"]),
                queue_depth=int(float(r["queue_depth"])),
                cpu_percent=float(r["cpu_percent"]),
                cpu_freq_mhz=float(r["cpu_freq_mhz"]),
                cpu_temp_c=float(r["cpu_temp_c"]),
                rss_mb=float(r["rss_mb"]),
            )
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--nominal-duration-s", type=float, default=180.0)
    args = parser.parse_args()

    requests = _load_requests(args.results_dir / "requests.csv")
    samples = _load_samples(args.results_dir / "queue_samples.csv")

    experiments = sorted({r.experiment for r in requests}, key=lambda e: (e.split("_")[0], _parse_load(e)))
    levels: dict[str, LoadLevelResult] = {}
    for exp in experiments:
        exp_requests = [r for r in requests if r.experiment == exp]
        exp_samples = [s for s in samples if s.experiment == exp]
        audio_duration_s = exp_requests[0].audio_duration_s
        offered_load = _parse_load(exp)
        levels[exp] = LoadLevelResult(
            experiment=exp,
            offered_load=offered_load,
            audio_duration_s=audio_duration_s,
            arrival_interval_s=audio_duration_s / offered_load,
            nominal_duration_s=args.nominal_duration_s,
            requests=exp_requests,
            samples=exp_samples,
            n_offered=len(exp_requests),
            n_completed=sum(1 for r in exp_requests if r.success),
            n_failed=sum(1 for r in exp_requests if not r.success),
            wallclock_s=max((s.elapsed_s for s in exp_samples), default=0.0),
            drain_s=0.0,
            drain_timed_out=False,
        )

    exp2_levels = [lv for name, lv in levels.items() if name.startswith("exp2_")]
    exp3_levels = [lv for name, lv in levels.items() if name.startswith("exp3_")]
    exp2_summaries = [summarize_level(lv) for lv in exp2_levels]
    exp3_summaries = [summarize_level(lv) for lv in exp3_levels]

    print("=" * 70)
    print("EXPERIMENT 2 SUMMARY (recomputed)")
    print("=" * 70)
    for s in exp2_summaries:
        print_level_summary(s)

    print("\n" + "=" * 70)
    print("EXPERIMENT 3 SUMMARY (recomputed)")
    print("=" * 70)
    for s in exp3_summaries:
        print_level_summary(s)

    if exp2_summaries:
        print_final_summary(exp2_summaries + exp3_summaries, exp3_levels)

    summary_path = args.results_dir / "summary.json"
    summary_path.write_text(json.dumps({"exp2": exp2_summaries, "exp3": exp3_summaries}, indent=2, default=str))
    print(f"\nRewrote {summary_path}")


if __name__ == "__main__":
    main()
