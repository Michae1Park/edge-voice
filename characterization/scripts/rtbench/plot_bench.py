#!/usr/bin/env python3
"""Plots for run_bench.py's output -- reads requests.csv, queue_samples.csv,
and summary.json from a results directory, writes PNGs to <results-dir>/plots.

Experiment 2 (offered load on the x-axis, one point per load level):
    queue depth vs elapsed time (one line per load level)
    end-to-end latency vs offered load
    p50/p95/p99 latency vs offered load
    actual throughput vs offered load
    RTF vs offered load

Experiment 3 (elapsed time on the x-axis, one line per load level):
    queue depth vs elapsed time
    end-to-end latency vs elapsed time
    queue wait time vs elapsed time
    service time vs elapsed time
    RSS vs elapsed time
    temperature vs elapsed time
    CPU frequency vs elapsed time

Usage:
    python characterization/scripts/rtbench/plot_bench.py --results-dir characterization/results_rtbench
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

FLOAT_FIELDS_REQ = {
    "iteration", "audio_duration_s", "arrival_time_s", "asr_start_time_s", "asr_finish_time_s",
    "queue_wait_ms", "service_time_ms", "e2e_latency_ms", "rtf", "queue_depth_at_arrival",
    "queue_depth_at_start", "cpu_percent", "cpu_freq_mhz", "cpu_temp_c", "rss_mb",
}
FLOAT_FIELDS_SAMPLE = {"elapsed_s", "queue_depth", "cpu_percent", "cpu_freq_mhz", "cpu_temp_c", "rss_mb"}


def _read_csv(path: Path, float_fields: set[str]) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        for k in float_fields:
            try:
                row[k] = float(row[k])
            except (KeyError, ValueError):
                pass
        if "success" in row:
            row["success"] = row["success"] == "True"
    return rows


def _group(rows: list[dict], key: str = "experiment") -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(row)
    return grouped


def _sorted_levels(summaries: list[dict]) -> list[dict]:
    return sorted(summaries, key=lambda s: s["offered_load"])


def plot_exp2(summaries: list[dict], requests_by_exp: dict, samples_by_exp: dict, out_dir: Path) -> None:
    if not summaries:
        return
    levels = _sorted_levels(summaries)
    loads = [lv["offered_load"] for lv in levels]

    # 1. Queue depth vs elapsed time, one line per load level.
    fig, ax = plt.subplots(figsize=(9, 5))
    for lv in levels:
        samples = samples_by_exp.get(lv["experiment"], [])
        if not samples:
            continue
        xs = [s["elapsed_s"] for s in samples]
        ys = [s["queue_depth"] for s in samples]
        ax.plot(xs, ys, label=f"{lv['offered_load']:.2f}x ({lv['verdict']})")
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("Queue depth (requests)")
    ax.set_title("Experiment 2: queue depth vs elapsed time")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out_dir / "exp2_queue_depth_vs_time.png", dpi=130)
    plt.close(fig)

    # 2. End-to-end latency vs offered load (raw scatter + mean line).
    fig, ax = plt.subplots(figsize=(7, 5))
    for lv in levels:
        reqs = requests_by_exp.get(lv["experiment"], [])
        ax.scatter([lv["offered_load"]] * len(reqs), [r["e2e_latency_ms"] for r in reqs],
                   color="tab:blue", alpha=0.4, s=15)
    means = [lv["latency_ms"]["mean"] for lv in levels]
    ax.plot(loads, means, color="tab:red", marker="o", label="mean")
    ax.set_xlabel("Offered load (x realtime)")
    ax.set_ylabel("End-to-end latency (ms)")
    ax.set_title("Experiment 2: end-to-end latency vs offered load")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "exp2_latency_vs_load.png", dpi=130)
    plt.close(fig)

    # 3. p50/p95/p99 latency vs offered load.
    fig, ax = plt.subplots(figsize=(7, 5))
    for pct in ("p50", "p95", "p99"):
        ax.plot(loads, [lv["latency_ms"][pct] for lv in levels], marker="o", label=pct)
    ax.set_xlabel("Offered load (x realtime)")
    ax.set_ylabel("End-to-end latency (ms)")
    ax.set_title("Experiment 2: latency percentiles vs offered load")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "exp2_latency_percentiles_vs_load.png", dpi=130)
    plt.close(fig)

    # 4. Actual throughput vs offered load, with a y=x "keeping up" reference.
    fig, ax = plt.subplots(figsize=(7, 5))
    actual = [lv["actual_throughput_x_realtime"] for lv in levels]
    ax.plot(loads, actual, marker="o", label="actual throughput")
    lim = max(loads + actual) * 1.1
    ax.plot([0, lim], [0, lim], "k--", alpha=0.4, label="ideal (keeping up)")
    ax.set_xlabel("Offered load (x realtime)")
    ax.set_ylabel("Actual throughput (x realtime)")
    ax.set_title("Experiment 2: actual throughput vs offered load")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "exp2_throughput_vs_load.png", dpi=130)
    plt.close(fig)

    # 5. RTF vs offered load.
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(loads, [lv["rtf"]["mean"] for lv in levels], marker="o", label="mean RTF")
    ax.plot(loads, [lv["rtf"]["p95"] for lv in levels], marker="o", label="p95 RTF")
    ax.axhline(1.0, color="k", linestyle="--", alpha=0.4, label="RTF=1 (realtime)")
    ax.set_xlabel("Offered load (x realtime)")
    ax.set_ylabel("RTF (service_time / audio_duration)")
    ax.set_title("Experiment 2: RTF vs offered load")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "exp2_rtf_vs_load.png", dpi=130)
    plt.close(fig)


def _vs_elapsed(summaries: list[dict], requests_by_exp: dict, samples_by_exp: dict, out_dir: Path,
                 field: str, source: str, ylabel: str, title: str, filename: str, time_field: str) -> None:
    fig, ax = plt.subplots(figsize=(9, 5))
    for lv in _sorted_levels(summaries):
        rows = (requests_by_exp if source == "requests" else samples_by_exp).get(lv["experiment"], [])
        if not rows:
            continue
        xs = [r[time_field] for r in rows]
        ys = [r[field] for r in rows]
        ax.plot(xs, ys, marker=".", linewidth=1, markersize=3, label=f"{lv['offered_load']:.2f}x")
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / filename, dpi=130)
    plt.close(fig)


def plot_exp3(summaries: list[dict], requests_by_exp: dict, samples_by_exp: dict, out_dir: Path) -> None:
    if not summaries:
        return
    _vs_elapsed(summaries, requests_by_exp, samples_by_exp, out_dir,
                field="queue_depth", source="samples", ylabel="Queue depth (requests)",
                title="Experiment 3: queue depth vs elapsed time",
                filename="exp3_queue_depth_vs_time.png", time_field="elapsed_s")
    _vs_elapsed(summaries, requests_by_exp, samples_by_exp, out_dir,
                field="e2e_latency_ms", source="requests", ylabel="End-to-end latency (ms)",
                title="Experiment 3: end-to-end latency vs elapsed time",
                filename="exp3_latency_vs_time.png", time_field="asr_finish_time_s")
    _vs_elapsed(summaries, requests_by_exp, samples_by_exp, out_dir,
                field="queue_wait_ms", source="requests", ylabel="Queue wait time (ms)",
                title="Experiment 3: queue wait time vs elapsed time",
                filename="exp3_queue_wait_vs_time.png", time_field="arrival_time_s")
    _vs_elapsed(summaries, requests_by_exp, samples_by_exp, out_dir,
                field="service_time_ms", source="requests", ylabel="Service time (ms)",
                title="Experiment 3: service time vs elapsed time",
                filename="exp3_service_time_vs_time.png", time_field="asr_start_time_s")
    _vs_elapsed(summaries, requests_by_exp, samples_by_exp, out_dir,
                field="rss_mb", source="samples", ylabel="Process RSS (MB)",
                title="Experiment 3: RSS vs elapsed time",
                filename="exp3_rss_vs_time.png", time_field="elapsed_s")
    _vs_elapsed(summaries, requests_by_exp, samples_by_exp, out_dir,
                field="cpu_temp_c", source="samples", ylabel="CPU temperature (C)",
                title="Experiment 3: CPU temperature vs elapsed time",
                filename="exp3_temperature_vs_time.png", time_field="elapsed_s")
    _vs_elapsed(summaries, requests_by_exp, samples_by_exp, out_dir,
                field="cpu_freq_mhz", source="samples", ylabel="CPU frequency (MHz)",
                title="Experiment 3: CPU frequency vs elapsed time",
                filename="exp3_cpu_freq_vs_time.png", time_field="elapsed_s")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-dir", type=Path, required=True)
    args = parser.parse_args()

    summary = json.loads((args.results_dir / "summary.json").read_text())
    requests = _read_csv(args.results_dir / "requests.csv", FLOAT_FIELDS_REQ)
    samples = _read_csv(args.results_dir / "queue_samples.csv", FLOAT_FIELDS_SAMPLE)
    requests_by_exp = _group(requests)
    samples_by_exp = _group(samples)

    out_dir = args.results_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    plot_exp2(summary.get("exp2", []), requests_by_exp, samples_by_exp, out_dir)
    plot_exp3(summary.get("exp3", []), requests_by_exp, samples_by_exp, out_dir)

    print(f"Wrote plots to {out_dir}")


if __name__ == "__main__":
    main()
