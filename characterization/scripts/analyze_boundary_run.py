#!/usr/bin/env python3
"""Objective pass/fail for one boundary-search cell's sustained run.

Three failure/trend signals, computed from the CSV + log -- not eyeballed.
The "queue depth trend" heuristic printed by bench_pipeline_load.py already
proved unreliable once this session (missed the original combo5 clog
entirely, since it samples queue depth only at transcript-arrival, by
which point the backlog has already drained) -- this script only trusts
the packet-timestamp-derived latency columns and VAD's own segment-id
timestamps.

  1. Segment capture rate < 95% of expected -- catches VAD "going deaf"
     (the actual failure mode in the original, pre-fix combo5: most
     segments never produced a transcript at all, not that they were slow).
     Expected count = loops_completed * n_repeats_per_loop, read from the
     log's "N loop(s) done" line and the cell's metadata entry.

  2. pre_stt_latency_ms > 3x that segment's own stt_latency_ms for 3+
     CONSECUTIVE captured segments -- catches genuine sustained backlog.
     A one-off queueing hiccup (like the benign 10s-segment split found
     in the limit search) won't trip this; a real, sustained clog will.

  3. first_merge_elapsed_s -- time to the FIRST dropped-packet fingerprint,
     independent of (1) and (2), and earlier-firing than either. Root-caused
     in the p2_b_max investigation: when segment_limits_enabled is off and
     the pipeline falls behind, ingest/routed queues drop live audio
     packets outright: if a dropped packet lands in the silence gap between
     two speech bursts, VAD's iterator never sees a complete silence run
     there and never fires end/start, silently fusing the next burst into
     the current segment instead of starting a new one. That produces a
     segment-start-to-segment-start gap much larger than one atomic cycle
     (atomic_segment_duration_s + gap_s from metadata) -- flagged here at
     >1.3x that cycle. Segment ids are contiguous integers embedding their
     own start timestamp (rx-<start_ts>-<counter>), pulled from BOTH
     successful TRANSCRIPT lines and repetitive-discarded lines in the log
     (a merge event's decode very often trips the repetitive-output
     fallback and never reaches the CSV at all -- capture_rate alone would
     undercount how early this starts). elapsed_s here is approximated as
     (this segment's start timestamp - the first segment's start
     timestamp), which lines up with bench_pipeline_load.py's own
     elapsed_s convention since both start counting at pipeline start.

Usage:
    python characterization/scripts/analyze_boundary_run.py <label> <csv_path> <log_path> [metadata_path]
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

DEFAULT_METADATA_PATH = Path(__file__).resolve().parent.parent / "testaudio" / "metadata_boundary.json"

SEGMENT_ID_RE = re.compile(r"segment=rx-([0-9]+\.[0-9]+)-([0-9]+)")
MERGE_THRESHOLD_MULT = 1.3


def _segment_starts_from_log(log_text: str) -> list[tuple[int, float]]:
    """(counter, start_ts) for every segment id mentioned anywhere in the
    log -- successful transcripts and repetitive-discarded ones alike --
    sorted by counter, deduplicated."""
    seen = {}
    for start_str, counter_str in SEGMENT_ID_RE.findall(log_text):
        seen[int(counter_str)] = float(start_str)
    return sorted(seen.items())


def analyze(label: str, csv_path: Path, log_path: Path, metadata_path: Path = DEFAULT_METADATA_PATH) -> dict:
    meta = json.loads(metadata_path.read_text())[label]
    n_per_loop = meta["n_repeats"]
    atomic_cycle_s = meta["atomic_segment_duration_s"] + meta["gap_s"]

    log_text = log_path.read_text()
    m = re.search(r"(\d+) loop\(s\) done", log_text)
    loops = int(m.group(1)) if m else None
    expected = loops * n_per_loop if loops is not None else None

    rows = list(csv.DictReader(open(csv_path)))
    captured = len(rows)
    capture_rate = captured / expected if expected else None

    consecutive = 0
    first_fail_elapsed = None
    for r in rows:
        stt = float(r["stt_latency_ms"])
        pre = float(r["pre_stt_latency_ms"])
        if stt > 0 and pre > 3 * stt:
            consecutive += 1
            if consecutive >= 3 and first_fail_elapsed is None:
                first_fail_elapsed = float(r["elapsed_s"])
        else:
            consecutive = 0

    starts = _segment_starts_from_log(log_text)
    first_merge_elapsed = None
    n_merge_events = 0
    if starts:
        run_start_ts = starts[0][1]
        prev_ts = starts[0][1]
        for _, ts in starts[1:]:
            delta = ts - prev_ts
            if delta > MERGE_THRESHOLD_MULT * atomic_cycle_s:
                n_merge_events += 1
                if first_merge_elapsed is None:
                    first_merge_elapsed = round(prev_ts - run_start_ts, 2)
            prev_ts = ts

    # Pre-STT latency trend in 30s windows, to plot a growth curve even for
    # cells that never cross a hard fail threshold.
    window_s = 30.0
    windows: dict[int, list[float]] = {}
    for r in rows:
        bucket = int(float(r["elapsed_s"]) // window_s)
        windows.setdefault(bucket, []).append(float(r["pre_stt_latency_ms"]))
    pre_stt_trend = [
        {"window_start_s": b * window_s, "mean_pre_stt_ms": round(sum(v) / len(v), 1), "n": len(v)}
        for b, v in sorted(windows.items())
    ]

    capture_fail = capture_rate is not None and capture_rate < 0.95
    backlog_fail = first_fail_elapsed is not None
    passed = not capture_fail and not backlog_fail

    result = {
        "label": label,
        "loops_completed": loops,
        "expected_segments": expected,
        "captured_segments": captured,
        "capture_rate": round(capture_rate, 4) if capture_rate is not None else None,
        "capture_fail": capture_fail,
        "backlog_fail": backlog_fail,
        "first_fail_elapsed_s": first_fail_elapsed,
        "n_merge_events": n_merge_events,
        "first_merge_elapsed_s": first_merge_elapsed,
        "pre_stt_trend": pre_stt_trend,
        "passed": passed,
    }
    return result


if __name__ == "__main__":
    label, csv_path, log_path = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
    metadata_path = Path(sys.argv[4]) if len(sys.argv) > 4 else DEFAULT_METADATA_PATH
    result = analyze(label, csv_path, log_path, metadata_path)
    verdict = "PASS" if result["passed"] else "FAIL"
    print(f"{label}: {verdict}  {json.dumps(result)}")
