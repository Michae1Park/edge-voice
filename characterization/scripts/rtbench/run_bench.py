#!/usr/bin/env python3
"""Maximum-sustainable-throughput and overload characterization for a
non-streaming ASR (complete audio file/chunk in, transcription out).

Answers three questions, not just "how fast is one request":
  1. How much real-time workload can the system sustainably handle?
  2. Where does it become overloaded?
  3. What happens after it becomes overloaded?

Architecture (see engine.py for the implementation):

    audio producer (wall-clock paced, never waits on the ASR)
          |
          v
    request queue  <-- queue depth is the overload signal
          |
          v
    ASR worker (sequential, calls adapter.transcribe(audio_path))

The ASR itself is reached only through an adapter -- adapter.py -- so this
script never assumes how the ASR is invoked. Swap --adapter dummy in for
engine validation without a model, or point MoonshineAdapter at a different
language/model_arch/options if the deployed config changes.

Two experiments, run against the SAME 17.3s / ~92%-speech-occupancy audio
chunk (characterization/testaudio/natural17s_gap150ms.wav):

  Experiment 2 (primary) -- offered loads 0.75x/1.0x/1.25x/1.5x/2.0x
  realtime, ~3 min each: is throughput bounded at each load, or does the
  queue grow without bound?

  Experiment 3 -- offered loads 1.5x/2.0x/3.0x realtime, ~3 min each:
  once overloaded, what actually happens to the backlog, latency, service
  time, memory, CPU frequency, and temperature?

Usage:
    python characterization/scripts/rtbench/run_bench.py
    python characterization/scripts/rtbench/run_bench.py --adapter dummy --dummy-rtf 0.3
    python characterization/scripts/rtbench/run_bench.py --exp 2 --exp2-duration-s 60
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import asdict
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
CHARACTERIZATION_DIR = THIS_DIR.parent.parent
sys.path.insert(0, str(THIS_DIR.parent))  # for `import edge_voice...` via src on sys.path already, and rtbench as a package
sys.path.insert(0, str(THIS_DIR.parent.parent.parent / "src"))

from rtbench.adapter import build_adapter  # noqa: E402
from rtbench.engine import (  # noqa: E402
    LoadLevelResult,
    QueueSample,
    RequestRecord,
    linear_slope,
    run_load_level,
    stats,
    write_csv,
)

DEFAULT_AUDIO = CHARACTERIZATION_DIR / "testaudio" / "natural17s_gap150ms.wav"
DEFAULT_RESULTS_DIR = CHARACTERIZATION_DIR / "results_rtbench"

EXP2_LOADS = [0.75, 1.0, 1.25, 1.5, 2.0]
EXP3_LOADS = [1.5, 2.0, 3.0]
DEFAULT_LEVEL_DURATION_S = 180.0

# ── Classification thresholds ───────────────────────────────────────────
# Queue-depth slope during the ARRIVAL window only (drain phase excluded --
# it always trends down by construction and would bias this). Units: queue
# items per second.
SLOPE_SUSTAINABLE_MAX = 0.02   # ~<=3.6 items grown over a 180s window: noise
SLOPE_UNSUSTAINABLE_MIN = 0.08  # ~>=14.4 items grown over 180s: clear runaway
DEPTH_BOUND_SMALL = 5.0        # end-of-window depth this small reads as "bounded"
# Server utilization rho = mean_service_time / arrival_interval (a single
# sequential worker is a bounded queue iff rho < 1 -- standard M/D/1-style
# reasoning). This is the PRIMARY signal, not a request-counting ratio: an
# earlier version counted "requests whose finish time fell before the
# window's exact end" and divided by offered rate, which sounds equivalent
# but isn't -- on a short run (8-30 requests per level) the one request
# straddling the window boundary swings that ratio by 1/n (5-10% here),
# enough to cross a 0.97 threshold on pure edge noise even when queue depth
# measured continuously every 0.5s stayed at exactly 0 the entire run (this
# is exactly what happened the first time this classifier ran against real
# RPi5 data -- see git history). rho has no such edge case: it's a ratio of
# two per-request quantities, immune to where the observation window happens
# to end.
UTIL_SUSTAINABLE_MAX = 0.90
UTIL_UNSUSTAINABLE_MIN = 1.02


def classify_load(level: LoadLevelResult) -> tuple[str, str]:
    """(verdict, reason) -- SUSTAINABLE / BORDERLINE / UNSUSTAINABLE.

    Two independent signals:
      1. Utilization rho = mean_service_time_s / arrival_interval_s -- is
         the worker's own duty cycle below 1 with real margin, at 1, or
         over 1? (equivalent to RTF * offered_load).
      2. Queue-depth slope over the arrival window (elapsed_s <=
         nominal_duration_s -- the post-arrival drain phase always trends
         toward zero by construction and would make every level look
         healthy if included) -- catches cases rho alone wouldn't, e.g.
         service time itself degrading over the run (thermal throttling).

    Per the spec this bench implements: a load is NOT sustainable merely
    because the process is still alive -- rho and the slope are what catch
    a worker that's technically making progress but steadily losing ground.
    """
    window = [s for s in level.samples if s.elapsed_s <= level.nominal_duration_s]
    depths = [s.queue_depth for s in window]
    ts = [s.elapsed_s for s in window]
    slope = linear_slope(ts, depths) if len(ts) >= 2 else 0.0

    head = max(1, len(depths) // 5)
    start_depth = sum(depths[:head]) / head if depths else 0.0
    end_depth = sum(depths[-head:]) / head if depths else 0.0

    svc_mean_s = statistics.mean([r.service_time_ms / 1000.0 for r in level.requests]) if level.requests else 0.0
    rho = svc_mean_s / level.arrival_interval_s if level.arrival_interval_s else float("inf")

    reason = (
        f"utilization rho={rho:.2f} (service/interval), queue slope={slope:+.4f} depth/s, "
        f"end_depth={end_depth:.1f} (start={start_depth:.1f})"
    )

    if rho <= UTIL_SUSTAINABLE_MAX and slope <= SLOPE_SUSTAINABLE_MAX and end_depth <= DEPTH_BOUND_SMALL:
        return "SUSTAINABLE", reason
    if (
        rho >= UTIL_UNSUSTAINABLE_MIN
        or slope >= SLOPE_UNSUSTAINABLE_MIN
        or end_depth > max(3 * start_depth, 3 * DEPTH_BOUND_SMALL)
    ):
        return "UNSUSTAINABLE", reason
    return "BORDERLINE", reason


def summarize_level(level: LoadLevelResult) -> dict:
    verdict, reason = classify_load(level)
    lat = stats([r.e2e_latency_ms for r in level.requests])
    svc = stats([r.service_time_ms for r in level.requests])
    rtf = stats([r.rtf for r in level.requests if r.success])
    qwait = stats([r.queue_wait_ms for r in level.requests])
    window = [s for s in level.samples if s.elapsed_s <= level.nominal_duration_s]
    depths = [s.queue_depth for s in window]
    cpu = stats([s.cpu_percent for s in window])
    temp = stats([s.cpu_temp_c for s in window])
    rss = stats([s.rss_mb for s in window])
    offered_rate = 1.0 / level.arrival_interval_s
    in_window = [r for r in level.requests if r.asr_finish_time_s <= level.nominal_duration_s]
    actual_rate = len(in_window) / level.nominal_duration_s if level.nominal_duration_s else 0.0

    return {
        "experiment": level.experiment,
        "offered_load": level.offered_load,
        "offered_rate_per_s": offered_rate,
        "actual_rate_per_s": actual_rate,
        "actual_throughput_x_realtime": actual_rate * level.audio_duration_s,
        "n_offered": level.n_offered,
        "n_completed": level.n_completed,
        "n_failed": level.n_failed,
        "avg_queue_depth": sum(depths) / len(depths) if depths else 0.0,
        "max_queue_depth": max(depths) if depths else 0,
        "queue_growth_rate_per_s": linear_slope([s.elapsed_s for s in window], depths) if len(window) >= 2 else 0.0,
        "latency_ms": lat,
        "service_time_ms": svc,
        "queue_wait_ms": qwait,
        "rtf": rtf,
        "cpu_percent": cpu,
        "cpu_temp_c": temp,
        "rss_mb": rss,
        "verdict": verdict,
        "verdict_reason": reason,
        "wallclock_s": level.wallclock_s,
        "drain_s": level.drain_s,
        "drain_timed_out": level.drain_timed_out,
    }


def print_level_summary(s: dict) -> None:
    print(
        f"\n[{s['experiment']}] offered={s['offered_load']:.2f}x realtime "
        f"({s['offered_rate_per_s']:.4f} req/s)  -- verdict: {s['verdict']}"
    )
    print(f"    {s['verdict_reason']}")
    print(
        f"    requests: offered={s['n_offered']} completed={s['n_completed']} failed={s['n_failed']}  "
        f"actual throughput={s['actual_throughput_x_realtime']:.2f}x realtime "
        f"({s['actual_rate_per_s']:.4f} req/s)"
    )
    print(
        f"    queue depth: avg={s['avg_queue_depth']:.2f} max={s['max_queue_depth']} "
        f"growth={s['queue_growth_rate_per_s']:+.4f} depth/s"
    )
    lat = s["latency_ms"]
    print(
        f"    e2e latency (ms): mean={lat['mean']:.0f} p50={lat['p50']:.0f} p95={lat['p95']:.0f} "
        f"p99={lat['p99']:.0f} max={lat['max']:.0f} std={lat['std']:.0f}"
    )
    svc, rtf = s["service_time_ms"], s["rtf"]
    print(f"    service time (ms): mean={svc['mean']:.0f}   RTF: mean={rtf['mean']:.3f} p95={rtf['p95']:.3f}")
    cpu, temp, rss = s["cpu_percent"], s["cpu_temp_c"], s["rss_mb"]
    print(
        f"    CPU%={cpu['mean']:.0f} (max {cpu['max']:.0f})  temp={temp['mean']:.1f}C  "
        f"RSS={rss['mean']:.0f}MB (max {rss['max']:.0f})"
    )
    if s["drain_timed_out"]:
        print(f"    WARNING: backlog drain hit the {s['drain_s']:.0f}s safety ceiling, still not empty")
    elif s["drain_s"] > 1.0:
        print(f"    backlog fully drained {s['drain_s']:.1f}s after arrivals stopped")


def run_experiment(
    name: str,
    loads: list[float],
    audio_path: Path,
    audio_duration_s: float,
    duration_s: float,
    adapter,
    sample_interval_s: float,
    queue_maxsize: int,
    max_drain_s: float,
) -> list[LoadLevelResult]:
    results = []
    for load in loads:
        label = f"{name}_{load:.2f}x"
        print(f"\n=== {label}: {audio_duration_s:.2f}s chunks every {audio_duration_s / load:.2f}s "
              f"for {duration_s:.0f}s ===")
        level = run_load_level(
            experiment=label,
            offered_load=load,
            audio_path=str(audio_path),
            audio_duration_s=audio_duration_s,
            duration_s=duration_s,
            adapter=adapter.transcribe if hasattr(adapter, "transcribe") else adapter,
            sample_interval_s=sample_interval_s,
            queue_maxsize=queue_maxsize,
            max_drain_s=max_drain_s,
        )
        results.append(level)
        print_level_summary(summarize_level(level))
    return results


def _half_means(values: list[float]) -> tuple[float, float]:
    if not values:
        return float("nan"), float("nan")
    mid = max(1, len(values) // 2)
    first, second = values[:mid], values[mid:] or values[:mid]
    return sum(first) / len(first), sum(second) / len(second)


def overload_narrative(level: LoadLevelResult) -> dict:
    """Describes what happened during ONE overload run, for the final
    "what happens after overload" summary -- queue growth shape, latency/
    service-time/RSS trend, temperature, and CPU frequency, comparing the
    first half of the arrival window against the second half."""
    window = [s for s in level.samples if s.elapsed_s <= level.nominal_duration_s]
    depths = [s.queue_depth for s in window]
    ts = [s.elapsed_s for s in window]
    mid = max(1, len(ts) // 2)
    slope_first = linear_slope(ts[:mid], depths[:mid]) if len(ts[:mid]) >= 2 else 0.0
    slope_second = linear_slope(ts[mid:], depths[mid:]) if len(ts[mid:]) >= 2 else 0.0
    if slope_first <= 0 and slope_second <= 0:
        shape = "flat/draining"
    elif slope_second > slope_first * 1.3 and slope_second > 0:
        shape = "accelerating"
    else:
        shape = "approximately linear"

    lat_first, lat_second = _half_means([r.e2e_latency_ms for r in level.requests])
    svc_first, svc_second = _half_means([r.service_time_ms for r in level.requests])
    rss_first, rss_second = _half_means([s.rss_mb for s in window])
    temp_first, temp_second = _half_means([s.cpu_temp_c for s in window])
    freq_first, freq_second = _half_means([s.cpu_freq_mhz for s in window])

    throttling = (
        freq_first == freq_first  # not NaN
        and freq_second == freq_second
        and freq_first > 0
        and freq_second < 0.92 * freq_first
    )

    return {
        "load": level.offered_load,
        "queue_growth_shape": shape,
        "queue_slope_first_half": slope_first,
        "queue_slope_second_half": slope_second,
        "max_queue_depth": max(depths) if depths else 0,
        "latency_ms_first_half": lat_first,
        "latency_ms_second_half": lat_second,
        "service_ms_first_half": svc_first,
        "service_ms_second_half": svc_second,
        "rss_mb_first_half": rss_first,
        "rss_mb_second_half": rss_second,
        "temp_c_first_half": temp_first,
        "temp_c_second_half": temp_second,
        "freq_mhz_first_half": freq_first,
        "freq_mhz_second_half": freq_second,
        "throttling_suspected": throttling,
        "n_failed": level.n_failed,
        "n_offered": level.n_offered,
    }


def print_final_summary(all_summaries: list[dict], exp3_levels: list[LoadLevelResult]) -> None:
    """all_summaries: EVERY tested load level (Experiment 2 AND 3 combined)
    -- the sustainable/saturation boundary can be pinned down by either
    experiment's data (e.g. Exp2 tops out at 2.0x sustainable, but only
    Exp3's 3.0x point reveals where it actually breaks), so both must be
    considered together, not just Experiment 2 in isolation."""
    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)

    # One verdict per distinct offered load (Exp2/Exp3 overlap on some
    # loads, e.g. both test 1.5x/2.0x -- they always agree since both are
    # measuring the same worker capacity, so keeping either copy is fine).
    by_load = {s["offered_load"]: s for s in sorted(all_summaries, key=lambda s: s["offered_load"])}
    summaries = list(by_load.values())
    sustainable = [s for s in summaries if s["verdict"] == "SUSTAINABLE"]
    unsustainable = [s for s in summaries if s["verdict"] == "UNSUSTAINABLE"]

    if sustainable:
        best = max(sustainable, key=lambda s: s["offered_load"])
        print(f"Maximum sustainable offered load: {best['offered_load']:.2f}x realtime")
        print(f"Corresponding throughput: {best['actual_throughput_x_realtime']:.2f}x realtime")
    else:
        print("Maximum sustainable offered load: none of the tested loads were sustainable")

    if sustainable and unsustainable:
        low = max(s["offered_load"] for s in sustainable)
        high = min(s["offered_load"] for s in unsustainable if s["offered_load"] > low)
        print(f"Saturation begins between: {low:.2f}x and {high:.2f}x realtime")
    elif unsustainable:
        print(f"Saturation begins at or below: {min(s['offered_load'] for s in unsustainable):.2f}x realtime")
    else:
        print("Saturation begins: not reached at any tested load")

    if exp3_levels:
        worst = max(exp3_levels, key=lambda lv: lv.offered_load)
        n = overload_narrative(worst)
        print(f"\nAt overload (Experiment 3, {n['load']:.2f}x realtime):")
        print(f"  queue growth: {n['queue_growth_shape']} "
              f"(slope {n['queue_slope_first_half']:+.3f} -> {n['queue_slope_second_half']:+.3f} depth/s, "
              f"peak depth {n['max_queue_depth']})")
        print(f"  latency growth: {n['latency_ms_first_half']:.0f}ms -> {n['latency_ms_second_half']:.0f}ms "
              f"(first half -> second half)")
        print(f"  service time: {n['service_ms_first_half']:.0f}ms -> {n['service_ms_second_half']:.0f}ms")
        rss_delta = n["rss_mb_second_half"] - n["rss_mb_first_half"]
        print(f"  RSS behavior: {n['rss_mb_first_half']:.0f}MB -> {n['rss_mb_second_half']:.0f}MB "
              f"({'growing' if rss_delta > 20 else 'stable'})")
        print(f"  temperature: {n['temp_c_first_half']:.1f}C -> {n['temp_c_second_half']:.1f}C")
        print(f"  CPU frequency: {n['freq_mhz_first_half']:.0f}MHz -> {n['freq_mhz_second_half']:.0f}MHz "
              f"({'THROTTLING SUSPECTED' if n['throttling_suspected'] else 'stable'})")
        if n["n_failed"]:
            print(f"  failures: {n['n_failed']}/{n['n_offered']} requests failed")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--adapter", choices=["moonshine", "dummy"], default="moonshine")
    parser.add_argument("--audio", type=Path, default=DEFAULT_AUDIO)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--exp", choices=["2", "3", "both"], default="both")
    parser.add_argument("--exp2-loads", type=float, nargs="+", default=EXP2_LOADS)
    parser.add_argument("--exp3-loads", type=float, nargs="+", default=EXP3_LOADS)
    parser.add_argument("--exp2-duration-s", type=float, default=DEFAULT_LEVEL_DURATION_S)
    parser.add_argument("--exp3-duration-s", type=float, default=DEFAULT_LEVEL_DURATION_S)
    parser.add_argument("--sample-interval-s", type=float, default=0.5)
    parser.add_argument("--queue-maxsize", type=int, default=0, help="0 = unbounded (default, per spec)")
    parser.add_argument("--max-drain-s", type=float, default=600.0,
                         help="Safety ceiling on post-arrival backlog drain per load level (default 600s)")
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--dummy-service-s", type=float, default=1.0)
    parser.add_argument("--dummy-jitter-s", type=float, default=0.0)
    parser.add_argument("--dummy-rtf", type=float, default=None,
                         help="If set, DummyAdapter service time = rtf * audio_duration_s instead of --dummy-service-s")
    args = parser.parse_args()

    import soundfile as sf

    if not args.audio.exists():
        raise SystemExit(f"{args.audio} not found")
    audio_duration_s = sf.info(str(args.audio)).duration
    print(f"Audio chunk: {args.audio} ({audio_duration_s:.3f}s)")

    print(f"Loading ASR adapter ({args.adapter})...")
    t_load = time.monotonic()
    if args.adapter == "dummy":
        adapter = build_adapter("dummy", service_s=args.dummy_service_s, jitter_s=args.dummy_jitter_s, rtf=args.dummy_rtf)
    else:
        adapter = build_adapter("moonshine")
    print(f"Adapter ready in {time.monotonic() - t_load:.1f}s")

    for i in range(args.warmup_runs):
        t = time.monotonic()
        adapter.transcribe(str(args.audio))
        print(f"warm-up {i + 1}/{args.warmup_runs}: {time.monotonic() - t:.2f}s")

    args.results_dir.mkdir(parents=True, exist_ok=True)

    all_levels: list[LoadLevelResult] = []
    exp2_levels: list[LoadLevelResult] = []
    exp3_levels: list[LoadLevelResult] = []

    if args.exp in ("2", "both"):
        print("\n" + "#" * 70)
        print("# EXPERIMENT 2 -- maximum sustainable throughput")
        print("#" * 70)
        exp2_levels = run_experiment(
            "exp2", args.exp2_loads, args.audio, audio_duration_s, args.exp2_duration_s,
            adapter, args.sample_interval_s, args.queue_maxsize, args.max_drain_s,
        )
        all_levels.extend(exp2_levels)

    if args.exp in ("3", "both"):
        print("\n" + "#" * 70)
        print("# EXPERIMENT 3 -- deliberate overload")
        print("#" * 70)
        exp3_levels = run_experiment(
            "exp3", args.exp3_loads, args.audio, audio_duration_s, args.exp3_duration_s,
            adapter, args.sample_interval_s, args.queue_maxsize, args.max_drain_s,
        )
        all_levels.extend(exp3_levels)

    # ── Output: per-request CSV, per-sample CSV, per-level summary JSON ──
    all_requests = [r for lv in all_levels for r in lv.requests]
    all_samples = [s for lv in all_levels for s in lv.samples]
    write_csv(args.results_dir / "requests.csv", all_requests, RequestRecord)
    write_csv(args.results_dir / "queue_samples.csv", all_samples, QueueSample)
    print(f"\nWrote {len(all_requests)} request rows -> {args.results_dir / 'requests.csv'}")
    print(f"Wrote {len(all_samples)} queue-sample rows -> {args.results_dir / 'queue_samples.csv'}")

    exp2_summaries = [summarize_level(lv) for lv in exp2_levels]
    exp3_summaries = [summarize_level(lv) for lv in exp3_levels]
    summary_path = args.results_dir / "summary.json"
    summary_path.write_text(json.dumps({"exp2": exp2_summaries, "exp3": exp3_summaries}, indent=2, default=str))
    print(f"Wrote per-load summary -> {summary_path}")

    if exp2_summaries:
        print("\n" + "=" * 70)
        print("EXPERIMENT 2 SUMMARY")
        print("=" * 70)
        for s in exp2_summaries:
            print_level_summary(s)

    if exp3_summaries:
        print("\n" + "=" * 70)
        print("EXPERIMENT 3 SUMMARY")
        print("=" * 70)
        for s in exp3_summaries:
            print_level_summary(s)

    if exp2_summaries:
        print_final_summary(exp2_summaries + exp3_summaries, exp3_levels)


if __name__ == "__main__":
    main()
