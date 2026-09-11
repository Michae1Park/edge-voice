#!/usr/bin/env python3
"""Runs the length=9-15s single-source boundary sweep (built by
build_boundary_singlesource_audio.py) through bench_pipeline_load.py, one
condition at a time, with the SAME flags used for the p1_/p2_ boundary
sweep in results_boundary/ (single-channel rx, --disable-reliability,
--min-silence-duration-ms 50, --duration-s 300, --token-budget-multiplier
matched to each cell's TSM rate).

Must run on the target RPi5 (`asr`), not a dev machine -- these numbers are
meaningless off the target hardware. Takes roughly 14 * ~5.5min ~= 80
minutes end to end.

For each cell, in characterization/results_singlesource/:
  - <label>.csv        -- one row per segment (from bench_pipeline_load.py)
  - <label>_log.txt     -- full run log
  - <label>_trend.csv   -- 30s-windowed pre_stt/full latency means, for
                            plotting the growth curve
  - driver_log.txt       -- START/FINISH timestamps + verdict JSON, appended
  - verdicts.txt          -- one verdict JSON line per cell, appended
  - summary_table.md      -- the Cell/Density/Rate/Length/... table, written
                              fresh at the end from all verdicts + CSVs

Usage:
    python characterization/scripts/run_singlesource_sweep.py [--labels ss_len9_max ...]
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
METADATA_PATH = ROOT / "characterization" / "testaudio" / "metadata_boundary_singlesource.json"
WAV_DIR = ROOT / "characterization" / "testaudio"
RESULTS_DIR = ROOT / "characterization" / "results_singlesource"
BENCH_SCRIPT = ROOT / "scratch" / "bench_pipeline_load.py"
ANALYZE_SCRIPT = ROOT / "characterization" / "scripts" / "analyze_boundary_run.py"

DURATION_S = 300
MIN_SILENCE_MS = 50
WINDOW_S = 30.0


def _pct(values: list[float], p: float) -> float:
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def run_cell(label: str, meta: dict) -> dict:
    wav_path = WAV_DIR / meta["filename"]
    csv_path = RESULTS_DIR / f"{label}.csv"
    log_path = RESULTS_DIR / f"{label}_log.txt"
    driver_log = RESULTS_DIR / "driver_log.txt"

    cmd = [
        sys.executable, str(BENCH_SCRIPT),
        "--channels", "rx",
        "--wav", str(wav_path),
        "--duration-s", str(DURATION_S),
        "--disable-reliability",
        "--min-silence-duration-ms", str(MIN_SILENCE_MS),
        "--token-budget-multiplier", str(meta["rate"]),
        "--csv-out", str(csv_path),
    ]

    start = dt.datetime.now(dt.timezone.utc).isoformat()
    with driver_log.open("a") as dl:
        dl.write(f"=== STARTING {label} (rate={meta['rate']}x, length={meta['cropped_length_s']}s) at {start} ===\n")
        dl.flush()
        with log_path.open("w") as lf:
            subprocess.run(cmd, cwd=ROOT, stdout=lf, stderr=subprocess.STDOUT, check=False)
        finish = dt.datetime.now(dt.timezone.utc).isoformat()
        dl.write(f"=== FINISHED {label} at {finish} ===\n")

        verdict_proc = subprocess.run(
            [sys.executable, str(ANALYZE_SCRIPT), label, str(csv_path), str(log_path), str(METADATA_PATH)],
            cwd=ROOT, capture_output=True, text=True,
        )
        verdict_line = verdict_proc.stdout.strip()
        dl.write(verdict_line + "\n")
        dl.flush()

    with (RESULTS_DIR / "verdicts.txt").open("a") as vf:
        vf.write(verdict_line + "\n")

    result = json.loads(verdict_line[verdict_line.index("{"):]) if "{" in verdict_line else {}
    return result


def write_trend(label: str) -> None:
    csv_path = RESULTS_DIR / f"{label}.csv"
    trend_path = RESULTS_DIR / f"{label}_trend.csv"
    rows = list(csv.DictReader(open(csv_path)))
    buckets: dict[int, list[tuple[float, float]]] = {}
    for r in rows:
        e = float(r["elapsed_s"])
        pre = float(r["pre_stt_latency_ms"])
        full = float(r["full_latency_ms"])
        buckets.setdefault(int(e // WINDOW_S), []).append((pre, full))

    with trend_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["window_start_s", "n", "pre_stt_mean_ms", "full_latency_mean_ms"])
        for b in sorted(buckets):
            vals = buckets[b]
            w.writerow([
                b * WINDOW_S, len(vals),
                round(statistics.mean(v[0] for v in vals), 1),
                round(statistics.mean(v[1] for v in vals), 1),
            ])


def write_summary_table(labels: list[str], meta_all: dict) -> None:
    lines = [
        "| Cell | Density | Rate | Length | Segments captured | Full latency mean / median / max | Pre-STT wait mean / median / max | Status |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for label in labels:
        meta = meta_all[label]
        csv_path = RESULTS_DIR / f"{label}.csv"
        verdict_path = RESULTS_DIR / "verdicts.txt"
        if not csv_path.exists():
            continue
        rows = list(csv.DictReader(open(csv_path)))
        full = [float(r["full_latency_ms"]) for r in rows]
        pre = [float(r["pre_stt_latency_ms"]) for r in rows]
        verdicts = {}
        for line in open(verdict_path):
            lb = line.split(":")[0].strip()
            if "{" in line:
                verdicts[lb] = json.loads(line[line.index("{"):])
        v = verdicts.get(label, {})
        expected = v.get("expected_segments", "?")
        status = "PASS" if v.get("passed") else "FAIL"
        if not full:
            lines.append(f"| {label} | {meta['occupancy_actual']:.0%} | {meta['rate']}x | {meta['cropped_length_s']}s | 0 / {expected} | -- | -- | FAIL (no segments) |")
            continue
        lines.append(
            f"| {label} | {meta['occupancy_actual']:.0%} | {meta['rate']}x | {meta['cropped_length_s']}s | "
            f"{len(rows)} / {expected} | {statistics.mean(full):.0f} / {statistics.median(full):.0f} / {max(full):.0f} ms | "
            f"{statistics.mean(pre):.0f} / {statistics.median(pre):.0f} / {max(pre):.0f} ms | {status} |"
        )
    (RESULTS_DIR / "summary_table.md").write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--labels", nargs="*", default=None, help="Subset of cell labels to run (default: all 14)")
    args = parser.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    meta_all = json.loads(METADATA_PATH.read_text())
    labels = args.labels or list(meta_all.keys())

    for label in labels:
        print(f"=== {label} ===", flush=True)
        run_cell(label, meta_all[label])
        write_trend(label)

    write_summary_table(list(meta_all.keys()), meta_all)
    print(f"Done. See {RESULTS_DIR}/summary_table.md and per-cell *_trend.csv")


if __name__ == "__main__":
    main()
