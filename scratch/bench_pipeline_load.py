#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import logging
import os
import queue
import re
import statistics
import threading
import time
from collections.abc import Callable
from typing import Any
from dataclasses import dataclass, fields

from edge_voice.config.settings import Settings
from edge_voice.pipeline.orchestrator import PipelineOrchestrator
from edge_voice.utils.audio_generation import wav_source

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("bench_pipeline_load")

CLK_TCK = os.sysconf("SC_CLK_TCK")
CAP_EPSILON_S = 0.15


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

    def __init__(
        self,
        rss_interval_s: float,
        cpu_interval_s: float,
        run_started: float,
        extra_pids: Callable[[], list[int]] | None = None,
    ) -> None:
        self._rss_interval_s = rss_interval_s
        self._cpu_interval_s = cpu_interval_s
        self._run_started = run_started
        self._extra_pids = extra_pids or (lambda: [])
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.rss_samples: list[tuple[float, float]] = []
        self.cpu_pct_samples: list[float] = []

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._rss_interval_s * 4)

    def _run(self) -> None:
        rss_mb, last_cpu_ticks = _read_proc_tree(self._extra_pids())
        now = time.monotonic()
        self.rss_samples.append((now - self._run_started, rss_mb))
        last_cpu_t = now
        while not self._stop.wait(self._rss_interval_s):
            rss_mb, ticks = _read_proc_tree(self._extra_pids())
            now = time.monotonic()
            self.rss_samples.append((now - self._run_started, rss_mb))
            if now - last_cpu_t >= self._cpu_interval_s:
                dt = now - last_cpu_t
                cpu_pct = 100.0 * ((ticks - last_cpu_ticks) / CLK_TCK) / dt if dt > 0 else 0.0
                self.cpu_pct_samples.append(cpu_pct)
                last_cpu_ticks, last_cpu_t = ticks, now


def _parse_cores(spec: str) -> set[int]:
    return {int(x) for x in spec.split(",") if x.strip()}


def _parse_core_map(spec: str) -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    for pair in spec.split(","):
        pair = pair.strip()
        if not pair:
            continue
        channel, _, cores = pair.partition("=")
        result[channel.strip()] = {int(c) for c in cores.split("+") if c.strip()}
    return result


def _pin_worker(worker: Any, cores: set[int], label: str) -> None:
    if not hasattr(os, "sched_setaffinity"):
        logger.warning("os.sched_setaffinity unavailable on this platform -- skipping (%s)", label)
        return
    if worker is None:
        logger.warning("%s: no such worker in this build (reliability/metrics disabled?)", label)
        return
    os_id = getattr(worker, "native_id", None)
    kind = "tid"
    if os_id is None:
        os_id = getattr(worker, "ident", None)
        kind = "pid"
    if os_id is None:
        logger.warning("%s: no OS id yet (not started?) -- skipping", label)
        return
    os.sched_setaffinity(os_id, cores)
    logger.info("Pinned %s (%s=%d) to cores %s", label, kind, os_id, sorted(cores))


def _pin_main_thread(cores: set[int]) -> None:
    if not hasattr(os, "sched_setaffinity"):
        return
    os.sched_setaffinity(0, cores)
    logger.info("Pinned main thread to cores %s", sorted(cores))


class _SttLatencyCapture(logging.Handler):

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.by_segment: dict[str, float] = {}

    def emit(self, record: logging.LogRecord) -> None:
        segment_id = getattr(record, "segment_id", None)
        stt_latency_s = getattr(record, "stt_latency_s", None)
        if segment_id is not None and stt_latency_s is not None:
            self.by_segment[segment_id] = stt_latency_s


SEGMENT_ID_RE = re.compile(r"^[a-z]+-([0-9]+\.[0-9]+)-([0-9]+)$")
PLATEAU_LATENCY_MULT = 3.0
PLATEAU_CONSECUTIVE = 3
MERGE_THRESHOLD_MULT = 1.3


class EarlyStopWatchdog:

    def __init__(self, atomic_cycle_s: float | None) -> None:
        self._atomic_cycle_s = atomic_cycle_s
        self._consecutive = 0
        self._last_start_by_channel: dict[str, float] = {}
        self.trigger_time: float | None = None
        self.trigger_reason: str | None = None

    def _fire(self, reason: str) -> None:
        if self.trigger_time is None:
            self.trigger_time = time.monotonic()
            self.trigger_reason = reason

    def observe(self, record: "SegmentRecord") -> None:
        if self.trigger_time is not None:
            return

        if record.stt_latency_ms and record.pre_stt_latency_ms is not None:
            if record.pre_stt_latency_ms > PLATEAU_LATENCY_MULT * record.stt_latency_ms:
                self._consecutive += 1
                if self._consecutive >= PLATEAU_CONSECUTIVE:
                    self._fire(
                        f"sustained latency plateau ({self._consecutive} consecutive segments "
                        f"with pre_stt > {PLATEAU_LATENCY_MULT:.0f}x stt latency)"
                    )
                    return
            else:
                self._consecutive = 0

        if self._atomic_cycle_s:
            m = SEGMENT_ID_RE.match(record.segment_id)
            if m:
                start_ts = float(m.group(1))
                prev = self._last_start_by_channel.get(record.channel_id)
                self._last_start_by_channel[record.channel_id] = start_ts
                if (
                    prev is not None
                    and (start_ts - prev) > MERGE_THRESHOLD_MULT * self._atomic_cycle_s
                ):
                    self._fire(
                        f"merge event detected (segment-start gap {start_ts - prev:.2f}s > "
                        f"{MERGE_THRESHOLD_MULT:.1f}x atomic cycle {self._atomic_cycle_s:.2f}s) "
                        f"on channel={record.channel_id}"
                    )


@dataclass
class SegmentRecord:
    elapsed_s: float
    channel_id: str
    segment_id: str
    duration_s: float
    hit_cap: bool
    chars: int
    full_latency_ms: float
    stt_latency_ms: float | None
    pre_stt_latency_ms: float | None
    q_ingest: int
    q_routed: int
    q_segment: int


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[idx]


def _summary(label: str, values: list[float], unit: str) -> str:
    if not values:
        return f"{label}: no samples"
    line = (
        f"{label}: n={len(values)}  "
        f"min={min(values):.1f}{unit}  "
        f"mean={statistics.mean(values):.1f}{unit}  "
        f"median={statistics.median(values):.1f}{unit}  "
        f"p95={_percentile(values, 95):.1f}{unit}  "
        f"max={max(values):.1f}{unit}"
    )
    if len(values) >= 20:
        line += f"  p99={_percentile(values, 99):.1f}{unit}"
    return line


def _queue_depth_trend(records: list[SegmentRecord]) -> str:
    if len(records) < 4:
        return "not enough segments to compare halves (need >= 4)"

    def total_depth(r: SegmentRecord) -> int:
        return r.q_ingest + r.q_routed + r.q_segment

    mid = len(records) // 2
    first_half, second_half = records[:mid], records[mid:]
    first_mean = statistics.mean(total_depth(r) for r in first_half)
    second_mean = statistics.mean(total_depth(r) for r in second_half)
    peak = max(records, key=total_depth)

    verdict = (
        "flat/draining -- no sign of falling behind"
        if second_mean <= first_mean + 0.5
        else "GROWING -- pipeline may be falling behind real-time"
    )
    return (
        f"first half mean={first_mean:.2f}  second half mean={second_mean:.2f}  "
        f"peak={total_depth(peak)} at elapsed_s={peak.elapsed_s:.1f} ({verdict})"
    )


def _duration_bucket(duration_s: float) -> str:
    if duration_s < 1.0:
        return "0-1s"
    if duration_s < 2.0:
        return "1-2s"
    if duration_s < 3.0:
        return "2-3s"
    if duration_s < 5.0:
        return "3-5s"
    return "5s+"


def _print_grouped(records: list[SegmentRecord], key: str) -> None:
    groups: dict[str, list[SegmentRecord]] = {}
    for r in records:
        k = _duration_bucket(r.duration_s) if key == "duration_bucket" else getattr(r, key)
        groups.setdefault(str(k), []).append(r)
    for k in sorted(groups):
        rs = groups[k]
        full = [r.full_latency_ms for r in rs]
        stt = [r.stt_latency_ms for r in rs if r.stt_latency_ms is not None]
        print(
            f"    {k:>16}: n={len(rs):<3} "
            f"full mean={statistics.mean(full):>7.1f}ms  "
            f"stt mean={(statistics.mean(stt) if stt else float('nan')):>7.1f}ms"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--duration-s",
        type=float,
        default=180.0,
        help="Total sustained run time -- loops the WAV pair to fill it (default 180s)",
    )
    parser.add_argument(
        "--rss-interval-s",
        type=float,
        default=0.2,
        help="RSS sampling interval -- dense, to catch transient peaks (default 0.2s)",
    )
    parser.add_argument(
        "--cpu-interval-s",
        type=float,
        default=1.0,
        help="CPU%% averaging window -- coarser on purpose, see docstring (default 1.0s)",
    )
    parser.add_argument(
        "--stt-cores",
        default=None,
        metavar="C,C,...",
        help="Pin every channel's STTWorker to this SAME core set, e.g. '2,3' "
        "(default: unpinned). This does not give each channel an exclusive "
        "core -- sched_setaffinity({2,3}) lets the kernel schedule either "
        "worker onto either core, so concurrent decodes still contend within "
        "the set. Use --stt-core-map instead for strict one-core-per-channel "
        "isolation. Combine with MOONSHINE_ORT_SINGLE_THREAD -- see docstring.",
    )
    parser.add_argument(
        "--stt-core-map",
        default=None,
        metavar="CH=C[+C...],CH=C[+C...],...",
        help="Pin each channel's STTWorker to its OWN disjoint core(s), e.g. "
        "'rx=2,tx=3' (one core each) or 'rx=2+3,tx=4' (rx gets two cores, "
        "tx one). Unlike --stt-cores (one shared set for ALL STT workers, "
        "which the kernel is then free to schedule either worker onto), this "
        "gives each channel exclusive use of its own listed core(s) for a "
        "true no-contention isolation test. Overrides --stt-cores if both "
        "are given.",
    )
    parser.add_argument(
        "--other-cores",
        default=None,
        metavar="C,C,...",
        help="Pin every other worker + this script's main thread to these cores, "
        "disjoint from --stt-cores, for true dedication rather than a mere "
        "preference (default: unpinned, only meaningful alongside --stt-cores).",
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        default=["rx", "tx"],
        help="Channels to publish to -- e.g. 'rx' alone to isolate single-channel "
        "throughput from dual-channel concurrency (default: rx tx)",
    )
    parser.add_argument(
        "--wav",
        nargs="+",
        default=["wav/rx_recorded_1.wav", "wav/tx_recorded_1.wav"],
        help="One WAV file per channel, in the same order as --channels (default: "
        "the recorded call-leg pair in wav/, matching the default rx tx channels -- "
        "override both together for single-channel, e.g. --channels rx --wav wav/rx_recorded_1.wav)",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="WAV replay speed multiplier -- 1.0 is real-time (default, matches a "
        "live feed). Values > 1.0 publish audio faster than real-time, for finding "
        "the pipeline's throughput ceiling: push --speed up until the queue-depth "
        "verdict below flips from 'flat/draining' to 'GROWING', which is the point "
        "the full pipeline (ingest through STT) can no longer keep up.",
    )
    parser.add_argument(
        "--grace-s",
        type=float,
        default=5.0,
        help="Wait after the last loop for trailing STT decodes (default 5s)",
    )
    parser.add_argument(
        "--csv-out", default=None, help="Optional path to write one row per segment as CSV"
    )
    parser.add_argument(
        "--disable-reliability",
        action="store_true",
        help="Stress-testing only: turn off vad.segment_limits_enabled, vad.idle_flush_s, and "
        "reliability.enabled (script-level override, configs/default.yaml untouched) so a "
        "genuine clog/freeze can manifest instead of being silently recovered from. Combine "
        "with a long --duration-s and a looped WAV to actually build sustained backlog -- a "
        "single short clip has nothing to pile up behind a stuck segment.",
    )
    parser.add_argument(
        "--token-budget-multiplier",
        type=float,
        default=1.0,
        help="Multiplies stt.max_tokens_per_second by this factor -- use e.g. 2.0 when --wav "
        "is itself a time-scale-modified (TSM) clip at 2x speed, so the decoder's token budget "
        "matches the compressed audio's actual content instead of starving on it (see "
        "characterization/scripts/run_experiment.py for why this matters). Default 1.0 = no change.",
    )
    parser.add_argument(
        "--early-stop",
        action="store_true",
        help="Stop the run early once a genuine plateau or merge event is detected (see "
        "EarlyStopWatchdog), instead of always running the full --duration-s. Lets a "
        "multi-cell sweep move on from an already-failing cell without burning its whole "
        "budget re-confirming what's already clear. Off by default -- existing callers that "
        "want the full duration unconditionally are unaffected.",
    )
    parser.add_argument(
        "--early-stop-grace-s",
        type=float,
        default=10.0,
        help="Once --early-stop triggers, keep running this much longer (to capture a bit of "
        "post-trigger confirmation) before actually stopping (default 10s). Actual overshoot "
        "can exceed this by up to one more full loop of --wav, since the feed loop only checks "
        "between loops, not mid-playback (wav_source.py's playback call isn't interruptible).",
    )
    parser.add_argument(
        "--merge-atomic-cycle-s",
        type=float,
        default=None,
        help="Atomic segment cycle length (atomic_segment_duration_s + gap_s) for --early-stop's "
        "merge-event detection -- the same figure characterization/scripts/build_boundary_audio.py "
        "records per cell in its metadata. Omit to disable merge-event detection (plateau "
        "detection alone still runs if --early-stop is set).",
    )
    parser.add_argument(
        "--min-silence-duration-ms",
        type=int,
        default=None,
        help="Diagnostic override for vad.min_silence_duration_ms (default: whatever's in "
        "configs/default.yaml, currently 100ms). Tests whether Silero's end-of-speech "
        "detection becomes reliable again once the threshold is well below the audio's own "
        "gap length, vs. the gap being only ~2x the threshold. Script-level override only.",
    )
    args = parser.parse_args()

    settings = Settings.load()
    settings.stt.model = "moonshine"

    configured_ids = {c.channel_id for c in settings.mqtt.channels}
    requested_ids = set(args.channels)
    unknown = requested_ids - configured_ids
    if unknown:
        raise SystemExit(
            f"--channels {sorted(unknown)} not in configs/default.yaml's mqtt.channels "
            f"({sorted(configured_ids)})"
        )
    if requested_ids != configured_ids:
        dropped = configured_ids - requested_ids
        logger.info(
            "Restricting orchestrator to channel(s) %s -- not building STT worker(s) for "
            "%s, so this is a true %d-process isolation test, not %d channels with %s idle",
            sorted(requested_ids),
            sorted(dropped),
            len(requested_ids),
            len(configured_ids),
            sorted(dropped),
        )
        settings.mqtt.channels = [
            c for c in settings.mqtt.channels if c.channel_id in requested_ids
        ]

    max_segment_s = settings.vad.max_segment_s
    segment_limits_enabled = settings.vad.segment_limits_enabled
    if not segment_limits_enabled:
        logger.info(
            "vad.segment_limits_enabled=false in this config -- max_segment_s=%.1f is not "
            "actually enforced, so 'hit_cap' below will always be False (segments are cut "
            "on natural pauses / idle-flush only, however long that takes)",
            max_segment_s,
        )
    if args.disable_reliability:
        settings.vad.segment_limits_enabled = False
        settings.vad.idle_flush_s = 0.0
        settings.reliability.enabled = False
        logger.warning(
            "RELIABILITY DISABLED: segment_limits_enabled=False, idle_flush_s=0, "
            "reliability.enabled=False -- a segment or worker can now hang indefinitely, "
            "bounded only by --duration-s + --grace-s"
        )
    if args.token_budget_multiplier != 1.0:
        base_rate = float(settings.stt.moonshine.max_tokens_per_second)
        scaled_rate = base_rate * args.token_budget_multiplier
        settings.stt.moonshine.max_tokens_per_second = str(scaled_rate)
        logger.info(
            "Scaling max_tokens_per_second %.1f -> %.1f (multiplier=%.2gx)",
            base_rate,
            scaled_rate,
            args.token_budget_multiplier,
        )
    if args.min_silence_duration_ms is not None:
        old_ms = settings.vad.min_silence_duration_ms
        settings.vad.min_silence_duration_ms = args.min_silence_duration_ms
        logger.warning(
            "Overriding vad.min_silence_duration_ms %d -> %d -- diagnostic only, tests "
            "whether Silero's end-of-speech detection is unreliable at this file's own "
            "gap length specifically, not a recommended production value",
            old_ms,
            args.min_silence_duration_ms,
        )

    orch = PipelineOrchestrator(settings)
    orch.build()

    stt_capture = _SttLatencyCapture()
    logging.getLogger("edge_voice.pipeline.orchestrator").addHandler(stt_capture)

    records: list[SegmentRecord] = []
    sub = orch.transcripts.subscribe()
    drain_stop = threading.Event()
    run_started = time.monotonic()
    watchdog = EarlyStopWatchdog(args.merge_atomic_cycle_s) if args.early_stop else None

    def _drain() -> None:
        while True:
            try:
                ev = sub.get(timeout=0.5)
            except queue.Empty:
                if drain_stop.is_set():
                    return
                continue
            if not ev.is_final:
                continue
            stt_s = stt_capture.by_segment.get(ev.segment_id)
            duration_s = ev.end - ev.start
            depths = orch.queue_depths()
            records.append(
                SegmentRecord(
                    elapsed_s=time.monotonic() - run_started,
                    channel_id=ev.channel_id,
                    segment_id=ev.segment_id,
                    duration_s=duration_s,
                    hit_cap=segment_limits_enabled and duration_s >= max_segment_s - CAP_EPSILON_S,
                    chars=len(ev.text),
                    full_latency_ms=(ev.created_at - ev.end) * 1000,
                    stt_latency_ms=stt_s * 1000 if stt_s is not None else None,
                    pre_stt_latency_ms=(ev.created_at - ev.end - stt_s) * 1000
                    if stt_s is not None
                    else None,
                    q_ingest=depths.get("ingest", -1),
                    q_routed=depths.get("routed", -1),
                    q_segment=depths.get(f"segment_{ev.channel_id}", -1),
                )
            )
            if watchdog is not None:
                watchdog.observe(records[-1])

    drain_thread = threading.Thread(target=_drain, daemon=True)

    def _stt_child_pids() -> list[int]:
        pids = []
        for worker in orch._stt.values():
            pid = getattr(worker, "ident", None)
            if pid is not None and hasattr(worker, "transcript_queue"):
                pids.append(pid)
        return pids

    sampler = ResourceSampler(
        args.rss_interval_s, args.cpu_interval_s, run_started, extra_pids=_stt_child_pids
    )

    orch.start()
    drain_thread.start()
    sampler.start()
    time.sleep(0.5)

    all_stt_cores: set[int] = set()
    if args.stt_core_map:
        core_map = _parse_core_map(args.stt_core_map)
        for cid, stt_worker in orch._stt.items():
            if cid not in core_map:
                logger.warning(
                    "--stt-core-map has no entry for channel %s -- leaving unpinned", cid
                )
                continue
            _pin_worker(stt_worker, core_map[cid], f"STTWorker-{cid}")
            all_stt_cores |= core_map[cid]
    elif args.stt_cores:
        all_stt_cores = _parse_cores(args.stt_cores)
        for cid, stt_worker in orch._stt.items():
            _pin_worker(stt_worker, all_stt_cores, f"STTWorker-{cid}")
    if args.other_cores:
        other_cores = _parse_cores(args.other_cores)
        if all_stt_cores & other_cores:
            logger.warning(
                "STT cores and --other-cores overlap (%s) -- not a real dedication test",
                all_stt_cores & other_cores,
            )
        for label, worker in (
            ("MqttAudioIngest", orch._audio_source),
            ("ChannelRouter", orch._router),
            ("VADWorker", orch._vad),
            ("Supervisor", orch._supervisor),
            ("MetricsCollector", orch._metrics),
        ):
            _pin_worker(worker, other_cores, label)
        _pin_main_thread(other_cores)

    logger.info(
        "Replaying %s on channels %s in a loop for %.0fs at %.1fx speed (Ctrl-C to stop early)",
        args.wav,
        args.channels,
        args.duration_s,
        args.speed,
    )
    wav_source_argv = ["--wav", *args.wav, "--channels", *args.channels]
    if args.speed != 1.0:
        wav_source_argv += ["--speed", str(args.speed)]

    started = time.monotonic()
    loops = 0
    stopped_early = False
    try:
        while time.monotonic() - started < args.duration_s:
            if (
                watchdog is not None
                and watchdog.trigger_time is not None
                and time.monotonic() - watchdog.trigger_time >= args.early_stop_grace_s
            ):
                stopped_early = True
                break
            wav_source.main(wav_source_argv)
            loops += 1
    except KeyboardInterrupt:
        logger.info("Interrupted -- wrapping up")

    elapsed = time.monotonic() - started
    if stopped_early:
        logger.warning(
            "EARLY STOP after %d loop(s) / %.0fs: %s (elapsed_s=%.1f), "
            "%.0fs grace period elapsed -- skipping the rest of --duration-s=%.0fs",
            loops,
            elapsed,
            watchdog.trigger_reason,
            watchdog.trigger_time - run_started,
            args.early_stop_grace_s,
            args.duration_s,
        )
    logger.info(
        "%d loop(s) done in %.0fs, waiting %.0fs for trailing decodes", loops, elapsed, args.grace_s
    )
    time.sleep(args.grace_s)

    sampler.stop()
    drain_stop.set()
    drain_thread.join(timeout=2.0)
    orch.transcripts.unsubscribe(sub)
    orch.stop()
    orch.wait()
    logging.getLogger("edge_voice.pipeline.orchestrator").removeHandler(stt_capture)

    print("\n" + "=" * 96)
    print(
        f"PER-SEGMENT -- {loops} loop(s) x {args.wav} on {args.channels}, "
        f"{len(records)} final transcripts"
    )
    print("=" * 96)
    print(
        f"{'elapsed_s':>9} {'ch':>3} {'dur_s':>6} {'cap':>4} {'chars':>6} "
        f"{'full_ms':>8} {'stt_ms':>8} {'pre_ms':>8} {'q(i/r/s)':>10}"
    )
    for r in records:
        stt_s = f"{r.stt_latency_ms:.1f}" if r.stt_latency_ms is not None else "?"
        pre_s = f"{r.pre_stt_latency_ms:.1f}" if r.pre_stt_latency_ms is not None else "?"
        print(
            f"{r.elapsed_s:>9.1f} {r.channel_id:>3} {r.duration_s:>6.2f} "
            f"{'Y' if r.hit_cap else '':>4} {r.chars:>6} {r.full_latency_ms:>8.1f} "
            f"{stt_s:>8} {pre_s:>8} {r.q_ingest:>3}/{r.q_routed:>1}/{r.q_segment:>1}"
        )

    if args.csv_out:
        with open(args.csv_out, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([field.name for field in fields(SegmentRecord)])
            for r in records:
                writer.writerow([getattr(r, field.name) for field in fields(SegmentRecord)])
        print(f"\nWrote {len(records)} rows to {args.csv_out}")

    full_ms = [r.full_latency_ms for r in records]
    stt_ms = [r.stt_latency_ms for r in records if r.stt_latency_ms is not None]
    pre_ms = [r.pre_stt_latency_ms for r in records if r.pre_stt_latency_ms is not None]

    print("\nNote: RSS/CPU cover this process only -- the MQTT broker is a")
    print("separate OS process (check with `ps aux | grep mosquitto` if needed).")
    print(
        "Note: queue depths are sampled when this script observes the transcript,"
        "\nnot at the moment STT actually started decoding it -- context, not a"
        "\nprecise at-decode-time reading."
    )

    rss_values = [mb for _, mb in sampler.rss_samples]

    print("\n" + "=" * 96)
    print("SUMMARY")
    print("=" * 96)
    print(_summary("Full latency (mic-arrival -> transcript)", full_ms, "ms"))
    print(_summary("  of which STT decode", stt_ms, "ms"))
    print(_summary("  of which pre-STT (VAD wait + queueing)", pre_ms, "ms"))
    print(_summary("RSS", rss_values, "MB") + f"  (sampled every {args.rss_interval_s}s)")
    if sampler.rss_samples:
        peak_elapsed, peak_mb = max(sampler.rss_samples, key=lambda s: s[1])
        print(f"  peak: {peak_mb:.1f}MB at elapsed_s={peak_elapsed:.1f}")
    print(
        _summary("CPU", sampler.cpu_pct_samples, "%")
        + f"  ({os.cpu_count()} cores available, 100%=1 core, "
        f"averaged over {args.cpu_interval_s}s windows)"
    )

    if records:
        print("\nBy segment duration:")
        _print_grouped(records, "duration_bucket")
        print("\nBy channel:")
        _print_grouped(records, "channel_id")
        if segment_limits_enabled:
            print(f"\nBy hit_cap (max_segment_s={max_segment_s}, enforced in this config):")
            _print_grouped(records, "hit_cap")
        else:
            print(
                f"\n(Not grouping by hit_cap: vad.segment_limits_enabled=false, "
                f"max_segment_s={max_segment_s} isn't enforced -- see duration buckets above instead.)"
            )
        print("\nQueue depth trend (the 'running out of CPU' signal, not raw CPU%):")
        print(f"    {_queue_depth_trend(records)}")

    if full_ms:
        print("\n" + "-" * 96)
        print(
            f"TL;DR: {len(records)} segments over {loops} loop(s) at {args.speed:.1f}x speed -- "
            f"latency p50={_percentile(full_ms, 50):.0f}ms p95={_percentile(full_ms, 95):.0f}ms "
            f"max={max(full_ms):.0f}ms  |  "
            f"RSS mean={statistics.mean(rss_values):.0f}MB peak={max(rss_values):.0f}MB  |  "
            f"CPU mean={statistics.mean(sampler.cpu_pct_samples) if sampler.cpu_pct_samples else 0:.0f}%  |  "
            f"queue trend: {_queue_depth_trend(records)}"
        )
        print("-" * 96)


if __name__ == "__main__":
    main()
