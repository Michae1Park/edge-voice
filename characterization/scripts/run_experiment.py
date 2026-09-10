#!/usr/bin/env python3
"""Speech-density characterization: E2E latency, throughput, CER, CPU/RSS.

Measures the real PipelineOrchestrator's behavior (system treated as a
black box -- no config changes, whatever configs/default.yaml + env
currently is on this machine is what runs) against the three density test
files built by build_test_audio.py:

    extreme_density.wav (~95% speech occupancy, 0.15s gaps)
    high_density.wav    (~85% speech occupancy, 0.5s gaps)
    medium_density.wav  (~60% speech occupancy, 2.0s gaps)
    low_density.wav     (~30% speech occupancy, 7.0s gaps)

For each condition, the file is played through the pipeline once
(real-time paced, single channel) per repeat, 5 repeats by default, each a
fresh orchestrator build (independent trial, not a looped replay within
one run) so RSS/CPU reflect a clean start every time.

Each repeat starts with a WARM-UP: one throwaway utterance
(testsegments/3s.wav) is published and its final transcript awaited and
discarded before the timed condition file is published. Without this, the
first STT inference call after a fresh orchestrator build is measurably
slower than steady-state (ONNX Runtime kernel/threadpool warm-up, not a
density effect) -- confirmed in an earlier run of this script, where
low-density's 4-segment runs (one slow first call out of 4) showed a
higher mean latency than high-density's 8-segment runs (same slow first
call diluted across 8), even though every condition tiles the identical
utterance. The warm-up removes that confound so cross-condition latency
comparisons reflect density, not cold-start luck. RSS/CPU sample buffers
are reset right after warm-up completes, so the warm-up's own startup
transient doesn't bias this run's resource stats either.

Per-repeat, four kinds of numbers come out:
  - full_latency_ms / stt_latency_ms per finalized segment (same
    instrumentation as scratch/bench_pipeline_load.py -- see that file's
    docstring for exactly why these are read where they are).
  - CER: reference is the known transcript of the tiled utterance repeated
    N times (testaudio/metadata.json); hypothesis is the final transcripts
    concatenated in arrival order. Single channel + in-order-per-channel
    STT processing (see stt_worker.py) means arrival order == audio order,
    so no alignment step is needed. See cer.py for why whitespace is
    stripped before diffing.
  - throughput: content-seconds of speech (from testaudio/metadata.json)
    divided by the wall-clock time from first audio publish to the last
    final transcript -- "how many seconds of speech content this run
    processed per second of wall time," the same metric used in
    docs/EXPERIMENT_LOG.md's speed-sweep experiments. Also reports
    segments/s (jobs/s).
  - RSS/CPU sampled across the whole run (ResourceSampler, ported from
    bench_pipeline_load.py) -- to confirm density doesn't cause a resource
    blowup, not to re-litigate the concurrency/memory findings already in
    docs/EXPERIMENT_LOG.md (this script always runs a single channel).

Across the 5 repeats per condition: segment-level latencies and RSS/CPU
samples are POOLED (matching docs/EXPERIMENT_LOG.md's convention) before
computing mean/std/p50/p95/p99. CER and throughput are one number per
repeat (5 data points) -- percentiles on n=5 are approximate at best; take
p95/p99 there as "close to the max of 5," not a real tail estimate.

Requires a real MQTT broker (configs/default.yaml, localhost:1883 by
default) and loads the real VAD/STT models -- same as
scratch/bench_pipeline_load.py, deliberately not mocked.

Usage:
    python characterization/scripts/run_experiment.py
    python characterization/scripts/run_experiment.py --repeats 5 --channel rx
    python characterization/scripts/run_experiment.py --conditions high low
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os

# Read by libmoonshine's onnxruntime session setup (native getenv), forcing
# onnxruntime's intra-op thread pool to 1 -- must be set before the STT
# worker process/model is created. This is NOT optional plumbing: cli.py
# (the real `edge-voice` entry point) and deploy/edge-voice.service both set
# this for every real deployment. This script drives PipelineOrchestrator
# directly (bypassing cli.py, same as scratch/bench_pipeline_load.py), so
# without setting it here too, onnxruntime defaults to spreading every
# inference call's intra-op work across ALL available cores -- observed
# directly on the RPi5 (htop showing all 4 cores busy on a single-channel
# run, which should only ever occupy one dedicated core). That made an
# earlier run of this script measure un-representative behavior, not a
# density effect.
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

# One throwaway utterance (+ baked-in trailing silence, see
# build_test_audio.py), published and discarded before each timed run --
# see module docstring's WARM-UP section. WARMUP_SETTLE_S is a flat sleep
# after publishing it, covering the natural pause-detection + a full
# decode (~1.6-2.0s on the RPi5) plus margin. This is TIME-based rather
# than "wait for exactly one discarded transcript" because the raw
# testsegments/3s.wav recording (used directly, before warmup.wav existed)
# was observed to sometimes produce more than one final transcript around
# its own boundary -- discarding everything for a fixed settle window
# sidesteps needing to count discarded events at all.
WARMUP_WAV = TESTAUDIO_DIR / "warmup.wav"
WARMUP_SETTLE_S = 4.0

# How long to keep waiting for trailing decodes after all expected segments
# have arrived, and the hard ceiling if segments never fully arrive (a
# dropped/merged segment shouldn't hang the whole sweep).
GRACE_AFTER_COMPLETE_S = 3.0
MAX_WAIT_AFTER_PUBLISH_S = 60.0

CLK_TCK = os.sysconf("SC_CLK_TCK")


def _read_proc(pid: str = "self") -> tuple[float, int]:
    """(rss_mb, cpu_ticks) for one process -- see bench_pipeline_load.py."""
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
    """Background RSS/CPU sampler -- ported from bench_pipeline_load.py."""

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
    """Per-segment STT decode time off the orchestrator's own log line --
    see bench_pipeline_load.py's docstring for why this, not
    orch._stt[...].last_latency_s, is race-free."""

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
    # Content-seconds decoded per second of decode time -- audio duration /
    # decode time, per utterance. Distinct from run-level throughput
    # (content_s_per_wallclock_s in RunResult), which is pinned to file
    # occupancy at real-time playback and doesn't reflect decode efficiency
    # at all. This one does: it drops as speaking rate rises (decode cost
    # doesn't shrink as fast as audio duration does), which is the actual
    # "is decode keeping up with denser content" signal. Caveat: on badly
    # degraded audio the decoder can abandon a segment early (hallucinate a
    # short repetitive line and stop) rather than genuinely decoding faster
    # -- this ratio looks artificially GOOD in that failure mode, so always
    # read it alongside CER, not alone.
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
    # First-half vs second-half of THIS run's own sample history -- a single
    # mean can't distinguish "steady 800MB the whole run" from "started at
    # 700MB, climbing toward 900MB when we cut it off." The latter is a
    # clog/backlog signature; the former isn't. Only meaningful for
    # --disable-reliability runs long/unstable enough to have one.
    rss_first_half_mb: float
    rss_second_half_mb: float
    cpu_first_half_pct: float
    cpu_second_half_pct: float


def _first_second_half_means(values: list[float]) -> tuple[float, float]:
    """(first-half mean, second-half mean) of a sample history, in the order
    sampled -- the growth signature of a clog/backlog, see RunResult."""
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
    configured_ids = {c.channel_id for c in settings.mqtt.channels}
    if channel not in configured_ids:
        raise SystemExit(f"--channel {channel!r} not in configs' mqtt.channels ({sorted(configured_ids)})")
    settings.mqtt.channels = [c for c in settings.mqtt.channels if c.channel_id == channel]

    # Stress-testing only ("how far can this be pushed before it clogs or
    # freezes, unmitigated"): the density experiment found combo5-style
    # conditions triggering vad.idle_flush_s and vad.max_segment_s -- the
    # very safety nets that stop a wedged/never-ending segment from actually
    # showing the clog. Turning them off (plus reliability.enabled, the
    # in-process supervisor that would otherwise restart a stalled worker
    # within stall_timeout_s=10s) lets a genuine freeze or unbounded
    # backlog actually manifest instead of being silently recovered from.
    # Script-level override only -- configs/default.yaml is untouched.
    if disable_reliability:
        settings.vad.segment_limits_enabled = False
        settings.vad.idle_flush_s = 0.0
        settings.reliability.enabled = False
        logger.warning(
            "[%s] RELIABILITY DISABLED: segment_limits_enabled=False, idle_flush_s=0, "
            "reliability.enabled=False -- a segment or worker can now hang indefinitely, "
            "bounded only by this harness's own --max-wait-s=%.0f timeout",
            condition["label"], max_wait_s,
        )

    # Speaking-rate experiment only: the decoder's token budget is
    # base_max_tokens_per_second * audio_duration_s -- when TSM compresses
    # the same content into shorter audio, that budget shrinks even though
    # the amount of content to decode hasn't, causing mid-sentence
    # truncation unrelated to whether the speech is acoustically harder to
    # recognize (confirmed via isolated single-utterance decodes outside
    # this harness). Scaling the budget by the same `speed` factor used to
    # compress the audio cancels that out, isolating the acoustic-difficulty
    # question. Script-level override only -- configs/default.yaml (the
    # real deployed system's config) is left untouched; density conditions
    # have no "speed" key and are unaffected.
    speed = condition.get("speed")
    if speed is not None:
        base_rate = float(settings.stt.max_tokens_per_second)
        scaled_rate = base_rate * speed
        settings.stt.max_tokens_per_second = str(scaled_rate)
        logger.info(
            "[%s] scaling max_tokens_per_second %.1f -> %.1f (speed=%.2gx)",
            condition["label"], base_rate, scaled_rate, speed,
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
                # Discard everything until the warm-up settle window ends
                # (main thread clears this flag) -- see WARMUP_SETTLE_S.
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
    time.sleep(0.5)  # let workers come up before publishing

    logger.info(
        "[%s repeat %d] warm-up: publishing %s, then a %.1fs settle window (all discarded, not measured)",
        condition["label"], repeat_idx, WARMUP_WAV, WARMUP_SETTLE_S,
    )
    wav_source.main(["--wav", str(WARMUP_WAV), "--channels", channel])
    time.sleep(WARMUP_SETTLE_S)
    in_warmup.clear()
    # Drop whatever RSS/CPU samples the warm-up itself generated -- its own
    # startup transient isn't part of this run's density measurement.
    sampler.rss_samples.clear()
    sampler.cpu_pct_samples.clear()

    wav_path = TESTAUDIO_DIR / condition["filename"]
    logger.info(
        "[%s repeat %d] publishing %s on channel %s (expect %d segments)",
        condition["label"], repeat_idx, wav_path, channel, n_expected,
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
            condition["label"], repeat_idx, n_expected, n_got,
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
        cpu_mean_pct=statistics.mean(sampler.cpu_pct_samples) if sampler.cpu_pct_samples else float("nan"),
        rss_first_half_mb=rss_first,
        rss_second_half_mb=rss_second,
        cpu_first_half_pct=cpu_first,
        cpu_second_half_pct=cpu_second,
    )
    return result, segment_records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repeats", type=int, default=5, help="Independent trials per density condition (default 5)")
    parser.add_argument("--channel", default="rx", help="MQTT channel to publish on (default rx)")
    parser.add_argument(
        "--conditions", nargs="+", default=["extreme", "high", "medium", "low"],
        help="Which density conditions to run (default: all four)",
    )
    parser.add_argument(
        "--metadata", type=Path, default=TESTAUDIO_DIR / "metadata.json",
        help="Path to testaudio metadata (default: characterization/testaudio/metadata.json)",
    )
    parser.add_argument("--results-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument(
        "--disable-reliability", action="store_true",
        help="Stress-testing only: turn off vad.segment_limits_enabled, vad.idle_flush_s, and "
        "reliability.enabled (script-level override, configs/default.yaml untouched) so a "
        "genuine clog/freeze can manifest instead of being silently recovered from. "
        "Strongly consider raising --max-wait-s alongside this.",
    )
    parser.add_argument(
        "--max-wait-s", type=float, default=MAX_WAIT_AFTER_PUBLISH_S,
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
        raise SystemExit(f"Unknown condition(s) {sorted(missing)} -- available: {sorted(all_conditions)}")

    args.results_dir.mkdir(parents=True, exist_ok=True)

    all_segment_records: list[SegmentRecord] = []
    all_run_results: list[RunResult] = []

    for label in args.conditions:
        condition = all_conditions[label]
        logger.info(
            "=== Condition %r: gap=%.1fs N=%d total=%.1fs occupancy=%.1f%% ===",
            label, condition["gap_s"], condition["repeats"], condition["total_duration_s"],
            condition["occupancy_actual"] * 100,
        )
        for repeat_idx in range(args.repeats):
            result, segments = run_one(
                condition, args.channel, repeat_idx,
                disable_reliability=args.disable_reliability,
                max_wait_s=args.max_wait_s,
            )
            all_run_results.append(result)
            all_segment_records.extend(segments)
            logger.info(
                "[%s repeat %d] segments=%d/%d  CER=%.1f%%  content-s/wallclock-s=%.2f  "
                "wallclock=%.1fs  RSS mean=%.0fMB peak=%.0fMB (1st-half %.0f -> 2nd-half %.0f)  "
                "CPU mean=%.0f%% (1st-half %.0f%% -> 2nd-half %.0f%%)",
                label, repeat_idx, result.n_segments, result.n_expected, result.cer * 100,
                result.content_s_per_wallclock_s, result.wallclock_s, result.rss_mean_mb,
                result.rss_peak_mb, result.rss_first_half_mb, result.rss_second_half_mb,
                result.cpu_mean_pct, result.cpu_first_half_pct, result.cpu_second_half_pct,
            )

    # --- Per-segment CSV (raw rows across all conditions/repeats) ---
    seg_csv = args.results_dir / "segments.csv"
    with open(seg_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([field.name for field in fields(SegmentRecord)])
        for r in all_segment_records:
            writer.writerow([getattr(r, field.name) for field in fields(SegmentRecord)])
    logger.info("Wrote %d segment rows to %s", len(all_segment_records), seg_csv)

    # --- Per-run CSV (one row per repeat) ---
    run_csv = args.results_dir / "runs.csv"
    with open(run_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([field.name for field in fields(RunResult)])
        for r in all_run_results:
            writer.writerow([getattr(r, field.name) for field in fields(RunResult)])
    logger.info("Wrote %d run rows to %s", len(all_run_results), run_csv)

    # --- Aggregate summary per condition ---
    # order==0 (the first counted segment of each run) is excluded from the
    # LATENCY stats only: it's occasionally corrupted by a still-uncharacterized
    # boundary artifact between the warm-up publish and the real file's publish
    # (two separate wav_source.main()/MQTT-session calls on the same channel) --
    # its duration_s/timing can be inflated by several seconds even though its
    # transcribed TEXT is unaffected (confirmed correct against the reference).
    # CER, throughput, and RSS/CPU don't depend on this segment's duration_s,
    # so only full_latency_ms/stt_latency_ms filter it out here.
    summary = {}
    for label in args.conditions:
        seg_rows = [r for r in all_segment_records if r.condition == label]
        latency_rows = [r for r in seg_rows if r.order != 0]
        run_rows = [r for r in all_run_results if r.condition == label]
        summary[label] = {
            "condition": all_conditions[label],
            "full_latency_ms": _stats([r.full_latency_ms for r in latency_rows]),
            "stt_latency_ms": _stats([r.stt_latency_ms for r in latency_rows if r.stt_latency_ms is not None]),
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
        print("  " + _fmt_stats("Decode efficiency (audio-s decoded per decode-s)", s["audio_s_per_decode_s"], "x"))
        print("  " + _fmt_stats("CER", s["cer_pct"], "%"))
        print("  " + _fmt_stats("Throughput (content-s/wallclock-s)", s["content_s_per_wallclock_s"], "x"))
        print("  " + _fmt_stats("Throughput (jobs/s)", s["jobs_per_s"], "/s"))
        print("  " + _fmt_stats("RSS mean", s["rss_mean_mb"], "MB"))
        print("  " + _fmt_stats("RSS peak", s["rss_peak_mb"], "MB"))
        print("  " + _fmt_stats("CPU mean", s["cpu_mean_pct"], "%"))
    print(f"\nWrote {seg_csv}, {run_csv}, {summary_json}")


if __name__ == "__main__":
    main()
