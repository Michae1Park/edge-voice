#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import logging
import os

os.environ.setdefault("MOONSHINE_ORT_SINGLE_THREAD", "1")

import queue
import statistics
import sys
import threading
import time
from dataclasses import dataclass, fields
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cer import char_cer  # noqa: E402

from edge_voice.config.settings import Settings  # noqa: E402
from edge_voice.pipeline.orchestrator import PipelineOrchestrator  # noqa: E402
from edge_voice.utils.audio_generation import wav_source  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("run_experiment")

THIS_DIR = Path(__file__).resolve().parent
CHARACTERIZATION_DIR = THIS_DIR.parent
TESTDATA_DIR = CHARACTERIZATION_DIR / "testsegments"
TESTAUDIO_DIR = CHARACTERIZATION_DIR / "testaudio"
RESULTS_DIR = CHARACTERIZATION_DIR / "results"

WARMUP_WAV = TESTAUDIO_DIR / "warmup.wav"
WARMUP_SETTLE_S = 4.0

GRACE_AFTER_COMPLETE_S = 3.0
MAX_WAIT_AFTER_PUBLISH_S = 60.0

CLK_TCK = os.sysconf("SC_CLK_TCK")


def _read_proc(pid: str = "self") -> tuple[float, int]:
    rss_kb = 0
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                rss_kb = int(line.split()[1])
                break
    with open(f"/proc/{pid}/stat") as f:
        after_comm = f.read().rsplit(")", 1)[1].split()
        utime, stime = int(after_comm[11]), int(after_comm[12])
    return rss_kb / 1024.0, utime + stime


def _read_proc_tree(extra_pids: list[int]) -> tuple[float, int]:
    rss_mb, ticks = _read_proc("self")
    for pid in extra_pids:
        try:
            child_rss, child_ticks = _read_proc(str(pid))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        rss_mb += child_rss
        ticks += child_ticks
    return rss_mb, ticks


class ResourceSampler:

    def __init__(self, interval_s: float, extra_pids) -> None:
        self._interval_s = interval_s
        self._extra_pids = extra_pids
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.rss_samples: list[float] = []
        self.cpu_pct_samples: list[float] = []

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval_s * 4)

    def _run(self) -> None:
        rss_mb, last_ticks = _read_proc_tree(self._extra_pids())
        self.rss_samples.append(rss_mb)
        last_t = time.monotonic()
        while not self._stop.wait(self._interval_s):
            rss_mb, ticks = _read_proc_tree(self._extra_pids())
            now = time.monotonic()
            self.rss_samples.append(rss_mb)
            dt = now - last_t
            if dt > 0:
                self.cpu_pct_samples.append(100.0 * ((ticks - last_ticks) / CLK_TCK) / dt)
            last_ticks, last_t = ticks, now


class _SttLatencyCapture(logging.Handler):

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.by_segment: dict[str, float] = {}

    def emit(self, record: logging.LogRecord) -> None:
        segment_id = getattr(record, "segment_id", None)
        stt_latency_s = getattr(record, "stt_latency_s", None)
        if segment_id is not None and stt_latency_s is not None:
            self.by_segment[segment_id] = stt_latency_s


@dataclass
class SegmentRecord:
    condition: str
    repeat_idx: int
    order: int
    segment_id: str
    duration_s: float
    chars: int
    text: str
    full_latency_ms: float
    stt_latency_ms: float | None
    audio_s_per_decode_s: float | None


@dataclass
class RunResult:
    condition: str
    repeat_idx: int
    n_segments: int
    n_expected: int
    cer: float
    content_s_per_wallclock_s: float
    jobs_per_s: float
    wallclock_s: float
    rss_mean_mb: float
    rss_peak_mb: float
    cpu_mean_pct: float
    rss_first_half_mb: float
    rss_second_half_mb: float
    cpu_first_half_pct: float
    cpu_second_half_pct: float


def _first_second_half_means(values: list[float]) -> tuple[float, float]:
    if not values:
        return float("nan"), float("nan")
    mid = max(1, len(values) // 2)
    first, second = values[:mid], values[mid:] or values[:mid]
    return statistics.mean(first), statistics.mean(second)


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[idx]


def _stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {k: float("nan") for k in ("n", "mean", "std", "p50", "p95", "p99", "min", "max")}
    return {
        "n": len(values),
        "mean": statistics.mean(values),
        "std": statistics.stdev(values) if len(values) >= 2 else 0.0,
        "p50": _percentile(values, 50),
        "p95": _percentile(values, 95),
        "p99": _percentile(values, 99),
        "min": min(values),
        "max": max(values),
    }


def _fmt_stats(label: str, s: dict[str, float], unit: str) -> str:
    return (
        f"{label}: n={s['n']:.0f}  mean={s['mean']:.2f}{unit}  std={s['std']:.2f}{unit}  "
        f"p50={s['p50']:.2f}{unit}  p95={s['p95']:.2f}{unit}  p99={s['p99']:.2f}{unit}  "
        f"min={s['min']:.2f}{unit}  max={s['max']:.2f}{unit}"
    )


def _stt_child_pids(orch: PipelineOrchestrator) -> list[int]:
    pids = []
    for worker in orch._stt.values():
        pid = getattr(worker, "ident", None)
        if pid is not None and hasattr(worker, "transcript_queue"):
            pids.append(pid)
    return pids


def run_one(
    condition: dict,
    channel: str,
    repeat_idx: int,
    disable_reliability: bool = False,
    max_wait_s: float = MAX_WAIT_AFTER_PUBLISH_S,
) -> tuple[RunResult, list[SegmentRecord]]:
    settings = Settings.load()
    settings.stt.model = "moonshine"
    configured_ids = {c.channel_id for c in settings.mqtt.channels}
    if channel not in configured_ids:
        raise SystemExit(
            f"--channel {channel!r} not in configs' mqtt.channels ({sorted(configured_ids)})"
        )
    settings.mqtt.channels = [c for c in settings.mqtt.channels if c.channel_id == channel]

    if disable_reliability:
        settings.vad.segment_limits_enabled = False
        settings.vad.idle_flush_s = 0.0
        settings.reliability.enabled = False
        logger.warning(
            "[%s] RELIABILITY DISABLED: segment_limits_enabled=False, idle_flush_s=0, "
            "reliability.enabled=False -- a segment or worker can now hang indefinitely, "
            "bounded only by this harness's own --max-wait-s=%.0f timeout",
            condition["label"],
            max_wait_s,
        )

    speed = condition.get("speed")
    if speed is not None:
        base_rate = float(settings.stt.moonshine.max_tokens_per_second)
        scaled_rate = base_rate * speed
        settings.stt.moonshine.max_tokens_per_second = str(scaled_rate)
        logger.info(
            "[%s] scaling max_tokens_per_second %.1f -> %.1f (speed=%.2gx)",
            condition["label"],
            base_rate,
            scaled_rate,
            speed,
        )

    orch = PipelineOrchestrator(settings)
    orch.build()

    stt_capture = _SttLatencyCapture()
    logging.getLogger("edge_voice.pipeline.orchestrator").addHandler(stt_capture)

    segment_records: list[SegmentRecord] = []
    sub = orch.transcripts.subscribe()
    drain_stop = threading.Event()
    n_expected = condition["repeats"]
    all_arrived = threading.Event()
    in_warmup = threading.Event()
    in_warmup.set()

    def _drain() -> None:
        order = 0
        while True:
            try:
                ev = sub.get(timeout=0.5)
            except queue.Empty:
                if drain_stop.is_set():
                    return
                continue
            if not ev.is_final:
                continue
            if in_warmup.is_set():
                continue
            stt_s = stt_capture.by_segment.get(ev.segment_id)
            duration_s = ev.end - ev.start
            segment_records.append(
                SegmentRecord(
                    condition=condition["label"],
                    repeat_idx=repeat_idx,
                    order=order,
                    segment_id=ev.segment_id,
                    duration_s=duration_s,
                    chars=len(ev.text),
                    text=ev.text,
                    full_latency_ms=(ev.created_at - ev.end) * 1000,
                    stt_latency_ms=stt_s * 1000 if stt_s is not None else None,
                    audio_s_per_decode_s=duration_s / stt_s if stt_s else None,
                )
            )
            order += 1
            if len(segment_records) >= n_expected:
                all_arrived.set()

    drain_thread = threading.Thread(target=_drain, daemon=True)
    sampler = ResourceSampler(0.2, extra_pids=lambda: _stt_child_pids(orch))

    orch.start()
    drain_thread.start()
    sampler.start()
    time.sleep(0.5)

    logger.info(
        "[%s repeat %d] warm-up: publishing %s, then a %.1fs settle window (all discarded, not measured)",
        condition["label"],
        repeat_idx,
        WARMUP_WAV,
        WARMUP_SETTLE_S,
    )
    wav_source.main(["--wav", str(WARMUP_WAV), "--channels", channel])
    time.sleep(WARMUP_SETTLE_S)
    in_warmup.clear()
    sampler.rss_samples.clear()
    sampler.cpu_pct_samples.clear()

    wav_path = TESTAUDIO_DIR / condition["filename"]
    logger.info(
        "[%s repeat %d] publishing %s on channel %s (expect %d segments)",
        condition["label"],
        repeat_idx,
        wav_path,
        channel,
        n_expected,
    )
    publish_started = time.monotonic()
    wav_source.main(["--wav", str(wav_path), "--channels", channel])
    publish_done = time.monotonic()

    all_arrived.wait(timeout=max(0.0, max_wait_s - (publish_done - publish_started)))
    time.sleep(GRACE_AFTER_COMPLETE_S)
    run_end = time.monotonic()

    sampler.stop()
    drain_stop.set()
    drain_thread.join(timeout=2.0)
    orch.transcripts.unsubscribe(sub)
    orch.stop()
    orch.wait()
    logging.getLogger("edge_voice.pipeline.orchestrator").removeHandler(stt_capture)

    n_got = len(segment_records)
    if n_got != n_expected:
        logger.warning(
            "[%s repeat %d] expected %d final segments, got %d -- CER/throughput below still "
            "computed against what actually arrived",
            condition["label"],
            repeat_idx,
            n_expected,
            n_got,
        )

    hypothesis = " ".join(r.text for r in segment_records)
    cer = char_cer(condition["reference_transcript_concat"], hypothesis)

    wallclock_s = run_end - publish_started
    content_s = condition["speech_time_s"]

    rss_first, rss_second = _first_second_half_means(sampler.rss_samples)
    cpu_first, cpu_second = _first_second_half_means(sampler.cpu_pct_samples)

    result = RunResult(
        condition=condition["label"],
        repeat_idx=repeat_idx,
        n_segments=n_got,
        n_expected=n_expected,
        cer=cer,
        content_s_per_wallclock_s=content_s / wallclock_s if wallclock_s > 0 else float("nan"),
        jobs_per_s=n_got / wallclock_s if wallclock_s > 0 else float("nan"),
        wallclock_s=wallclock_s,
        rss_mean_mb=statistics.mean(sampler.rss_samples) if sampler.rss_samples else float("nan"),
        rss_peak_mb=max(sampler.rss_samples) if sampler.rss_samples else float("nan"),
        cpu_mean_pct=statistics.mean(sampler.cpu_pct_samples)
        if sampler.cpu_pct_samples
        else float("nan"),
        rss_first_half_mb=rss_first,
        rss_second_half_mb=rss_second,
        cpu_first_half_pct=cpu_first,
        cpu_second_half_pct=cpu_second,
    )
    return result, segment_records


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=5,
        help="Independent trials per density condition (default 5)",
    )
    parser.add_argument("--channel", default="rx", help="MQTT channel to publish on (default rx)")
    parser.add_argument(
        "--conditions",
        nargs="+",
        default=["extreme", "high", "medium", "low"],
        help="Which density conditions to run (default: all four)",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=TESTAUDIO_DIR / "metadata.json",
        help="Path to testaudio metadata (default: characterization/testaudio/metadata.json)",
    )
    parser.add_argument("--results-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument(
        "--disable-reliability",
        action="store_true",
        help="Stress-testing only: turn off vad.segment_limits_enabled, vad.idle_flush_s, and "
        "reliability.enabled (script-level override, configs/default.yaml untouched) so a "
        "genuine clog/freeze can manifest instead of being silently recovered from. "
        "Strongly consider raising --max-wait-s alongside this.",
    )
    parser.add_argument(
        "--max-wait-s",
        type=float,
        default=MAX_WAIT_AFTER_PUBLISH_S,
        help=f"How long to wait for all expected segments after publish finishes before giving "
        f"up on this run (default {MAX_WAIT_AFTER_PUBLISH_S:.0f}s) -- raise this well above the "
        "file's own duration with --disable-reliability, since nothing else will force a stuck "
        "segment to finalize.",
    )
    args = parser.parse_args()

    if not args.metadata.exists():
        raise SystemExit(f"{args.metadata} not found -- run build_test_audio.py first")
    all_conditions = {c["label"]: c for c in json.loads(args.metadata.read_text())}
    missing = set(args.conditions) - set(all_conditions)
    if missing:
        raise SystemExit(
            f"Unknown condition(s) {sorted(missing)} -- available: {sorted(all_conditions)}"
        )

    args.results_dir.mkdir(parents=True, exist_ok=True)

    all_segment_records: list[SegmentRecord] = []
    all_run_results: list[RunResult] = []

    for label in args.conditions:
        condition = all_conditions[label]
        logger.info(
            "=== Condition %r: gap=%.1fs N=%d total=%.1fs occupancy=%.1f%% ===",
            label,
            condition["gap_s"],
            condition["repeats"],
            condition["total_duration_s"],
            condition["occupancy_actual"] * 100,
        )
        for repeat_idx in range(args.repeats):
            result, segments = run_one(
                condition,
                args.channel,
                repeat_idx,
                disable_reliability=args.disable_reliability,
                max_wait_s=args.max_wait_s,
            )
            all_run_results.append(result)
            all_segment_records.extend(segments)
            logger.info(
                "[%s repeat %d] segments=%d/%d  CER=%.1f%%  content-s/wallclock-s=%.2f  "
                "wallclock=%.1fs  RSS mean=%.0fMB peak=%.0fMB (1st-half %.0f -> 2nd-half %.0f)  "
                "CPU mean=%.0f%% (1st-half %.0f%% -> 2nd-half %.0f%%)",
                label,
                repeat_idx,
                result.n_segments,
                result.n_expected,
                result.cer * 100,
                result.content_s_per_wallclock_s,
                result.wallclock_s,
                result.rss_mean_mb,
                result.rss_peak_mb,
                result.rss_first_half_mb,
                result.rss_second_half_mb,
                result.cpu_mean_pct,
                result.cpu_first_half_pct,
                result.cpu_second_half_pct,
            )

    seg_csv = args.results_dir / "segments.csv"
    with open(seg_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([field.name for field in fields(SegmentRecord)])
        for r in all_segment_records:
            writer.writerow([getattr(r, field.name) for field in fields(SegmentRecord)])
    logger.info("Wrote %d segment rows to %s", len(all_segment_records), seg_csv)

    run_csv = args.results_dir / "runs.csv"
    with open(run_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([field.name for field in fields(RunResult)])
        for r in all_run_results:
            writer.writerow([getattr(r, field.name) for field in fields(RunResult)])
    logger.info("Wrote %d run rows to %s", len(all_run_results), run_csv)

    summary = {}
    for label in args.conditions:
        seg_rows = [r for r in all_segment_records if r.condition == label]
        latency_rows = [r for r in seg_rows if r.order != 0]
        run_rows = [r for r in all_run_results if r.condition == label]
        summary[label] = {
            "condition": all_conditions[label],
            "full_latency_ms": _stats([r.full_latency_ms for r in latency_rows]),
            "stt_latency_ms": _stats(
                [r.stt_latency_ms for r in latency_rows if r.stt_latency_ms is not None]
            ),
            "audio_s_per_decode_s": _stats(
                [r.audio_s_per_decode_s for r in latency_rows if r.audio_s_per_decode_s is not None]
            ),
            "cer_pct": _stats([r.cer * 100 for r in run_rows]),
            "content_s_per_wallclock_s": _stats([r.content_s_per_wallclock_s for r in run_rows]),
            "jobs_per_s": _stats([r.jobs_per_s for r in run_rows]),
            "rss_mean_mb": _stats([r.rss_mean_mb for r in run_rows]),
            "rss_peak_mb": _stats([r.rss_peak_mb for r in run_rows]),
            "cpu_mean_pct": _stats([r.cpu_mean_pct for r in run_rows]),
        }
    summary_json = args.results_dir / "summary.json"
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    print("\n" + "=" * 100)
    print("SUMMARY (pooled across repeats; CER/throughput/RSS/CPU are per-run scalars, n=repeats)")
    print("=" * 100)
    for label in args.conditions:
        s = summary[label]
        c = all_conditions[label]
        print(
            f"\n--- {label} density (gap={c['gap_s']}s, occupancy actual={c['occupancy_actual']:.1%}, "
            f"target={c['occupancy_target']:.0%}) ---"
        )
        print("  " + _fmt_stats("Full E2E latency", s["full_latency_ms"], "ms"))
        print("  " + _fmt_stats("  of which STT decode", s["stt_latency_ms"], "ms"))
        print(
            "  "
            + _fmt_stats(
                "Decode efficiency (audio-s decoded per decode-s)", s["audio_s_per_decode_s"], "x"
            )
        )
        print("  " + _fmt_stats("CER", s["cer_pct"], "%"))
        print(
            "  "
            + _fmt_stats("Throughput (content-s/wallclock-s)", s["content_s_per_wallclock_s"], "x")
        )
        print("  " + _fmt_stats("Throughput (jobs/s)", s["jobs_per_s"], "/s"))
        print("  " + _fmt_stats("RSS mean", s["rss_mean_mb"], "MB"))
        print("  " + _fmt_stats("RSS peak", s["rss_peak_mb"], "MB"))
        print("  " + _fmt_stats("CPU mean", s["cpu_mean_pct"], "%"))
    print(f"\nWrote {seg_csv}, {run_csv}, {summary_json}")


if __name__ == "__main__":
    main()
