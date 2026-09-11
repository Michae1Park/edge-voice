"""Producer/consumer throughput-benchmark engine.

    audio producer (wall-clock paced)
          |
          v
    +-------------+
    | request     |
    | queue       |
    +-------------+
          |
          v
    +-------------+
    | ASR worker  |  (single thread, sequential, calls adapter(audio_path))
    +-------------+
          |
          v
    transcription

The producer determines request arrival times purely from wall-clock time
(a monotonic schedule, drift-corrected) and NEVER waits on the ASR worker.
If the worker falls behind, the queue grows -- that growth is the whole
point of this benchmark, not something to paper over with a closed loop
(transcribe(); sleep(interval); repeat), which would silently throttle
arrivals to match service time and hide overload entirely.

Everything here is generic over `adapter`: any callable/object exposing
`transcribe(audio_path) -> str` plugs in -- see adapter.py.
"""

from __future__ import annotations

import csv
import os
import queue
import statistics
import threading
import time
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Callable

_CLK_TCK = os.sysconf("SC_CLK_TCK")


# ── System metrics (no psutil dependency -- /proc + /sys, matching the
#    convention already used by characterization/scripts/run_experiment.py) ──


def _read_rss_mb(pid: str = "self") -> float:
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except FileNotFoundError:
        pass
    return float("nan")


def _read_cpu_ticks(pid: str = "self") -> int:
    try:
        with open(f"/proc/{pid}/stat") as f:
            after_comm = f.read().rsplit(")", 1)[1].split()
        return int(after_comm[11]) + int(after_comm[12])  # utime + stime
    except FileNotFoundError:
        return 0


def _read_cpu_freq_mhz() -> float:
    for path in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_cur_freq"):
        try:
            return int(path.read_text().strip()) / 1000.0
        except (OSError, ValueError):
            continue
    return float("nan")


def _read_cpu_temp_c() -> float:
    for zone in sorted(Path("/sys/class/thermal").glob("thermal_zone*")):
        try:
            return int((zone / "temp").read_text().strip()) / 1000.0
        except (OSError, ValueError):
            continue
    return float("nan")


class _CpuPercentTracker:
    """% of one CPU core busy, for this process, since the last call --
    same tick-delta approach as run_experiment.py's ResourceSampler, just
    inlined here so engine.py has no cross-import on that script."""

    def __init__(self) -> None:
        self._last_ticks = _read_cpu_ticks()
        self._last_t = time.monotonic()

    def sample(self) -> float:
        ticks = _read_cpu_ticks()
        now = time.monotonic()
        dt = now - self._last_t
        pct = 100.0 * ((ticks - self._last_ticks) / _CLK_TCK) / dt if dt > 0 else 0.0
        self._last_ticks, self._last_t = ticks, now
        return pct


# ── Records ──────────────────────────────────────────────────────────────


@dataclass
class RequestRecord:
    timestamp: str
    experiment: str
    iteration: int
    audio_duration_s: float
    arrival_time_s: float
    asr_start_time_s: float
    asr_finish_time_s: float
    queue_wait_ms: float
    service_time_ms: float
    e2e_latency_ms: float
    rtf: float
    queue_depth_at_arrival: int
    queue_depth_at_start: int
    cpu_percent: float
    cpu_freq_mhz: float
    cpu_temp_c: float
    rss_mb: float
    success: bool
    error: str


@dataclass
class QueueSample:
    experiment: str
    elapsed_s: float
    queue_depth: int
    cpu_percent: float
    cpu_freq_mhz: float
    cpu_temp_c: float
    rss_mb: float


@dataclass
class LoadLevelResult:
    experiment: str
    offered_load: float
    audio_duration_s: float
    arrival_interval_s: float
    nominal_duration_s: float
    requests: list[RequestRecord]
    samples: list[QueueSample]
    n_offered: int
    n_completed: int
    n_failed: int
    wallclock_s: float  # producer window + drain, this level's own clock
    drain_s: float
    drain_timed_out: bool


# ── Stats helpers ────────────────────────────────────────────────────────


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[idx]


def stats(values: list[float]) -> dict[str, float]:
    values = [v for v in values if v == v]  # drop NaN
    if not values:
        return {k: float("nan") for k in ("n", "mean", "std", "p50", "p95", "p99", "max", "min")}
    return {
        "n": len(values),
        "mean": statistics.mean(values),
        "std": statistics.stdev(values) if len(values) >= 2 else 0.0,
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values),
        "min": min(values),
    }


def linear_slope(xs: list[float], ys: list[float]) -> float:
    """Least-squares slope of ys vs xs (units of ys per unit of xs)."""
    n = len(xs)
    if n < 2:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den = sum((x - mean_x) ** 2 for x in xs)
    return num / den if den else 0.0


# ── Producer/consumer run ────────────────────────────────────────────────


def run_load_level(
    experiment: str,
    offered_load: float,
    audio_path: str,
    audio_duration_s: float,
    duration_s: float,
    adapter: Callable[[str], str],
    sample_interval_s: float = 0.5,
    queue_maxsize: int = 0,
    max_drain_s: float = 600.0,
) -> LoadLevelResult:
    """Run one offered-load level: producer paces arrivals at
    offered_load x realtime for duration_s, a single ASR worker thread
    drains the queue sequentially, then (arrivals stopped) the worker keeps
    draining any backlog -- up to max_drain_s -- before this level ends.
    Draining fully (rather than discarding backlog) is deliberate: nothing
    is ever dropped from the queue, per the benchmark's requirements; the
    drain itself is also useful signal (how long overload takes to recover
    once arrivals stop). max_drain_s is only a safety ceiling against a
    genuinely wedged worker, not a normal code path.
    """
    interval_s = audio_duration_s / offered_load
    q: "queue.Queue[tuple[int, float, int]]" = queue.Queue(maxsize=queue_maxsize)
    records: list[RequestRecord] = []
    samples: list[QueueSample] = []
    records_lock = threading.Lock()

    producer_done = threading.Event()
    worker_done = threading.Event()
    stop_sampler = threading.Event()

    cpu_tracker_worker = _CpuPercentTracker()
    cpu_tracker_sampler = _CpuPercentTracker()

    t0 = time.monotonic()
    n_offered = 0

    def producer_loop() -> None:
        nonlocal n_offered
        iteration = 0
        end_time = t0 + duration_s
        while True:
            next_time = t0 + iteration * interval_s
            if next_time >= end_time:
                break
            now = time.monotonic()
            sleep_s = next_time - now
            if sleep_s > 0:
                time.sleep(sleep_s)
            iteration += 1
            arrival_time = time.monotonic()
            # qsize() then +1, not qsize() after put(): avoids a race where
            # the worker could dequeue this same item between put() and the
            # qsize() read, which would otherwise undercount depth by one.
            depth_at_arrival = q.qsize() + 1
            q.put((iteration, arrival_time, depth_at_arrival))
            n_offered = iteration
        producer_done.set()

    def worker_loop() -> None:
        while True:
            try:
                iteration, arrival_time, depth_at_arrival = q.get(timeout=0.2)
            except queue.Empty:
                if producer_done.is_set() and q.empty():
                    worker_done.set()
                    return
                continue

            depth_at_start = q.qsize()
            start = time.monotonic()
            success, error, text = True, "", None
            try:
                text = adapter(audio_path)
            except Exception as exc:  # noqa: BLE001 -- record it, keep the worker alive
                success = False
                error = f"{type(exc).__name__}: {exc}"
            finish = time.monotonic()

            rec = RequestRecord(
                timestamp=datetime.now().isoformat(timespec="milliseconds"),
                experiment=experiment,
                iteration=iteration,
                audio_duration_s=audio_duration_s,
                arrival_time_s=arrival_time - t0,
                asr_start_time_s=start - t0,
                asr_finish_time_s=finish - t0,
                queue_wait_ms=(start - arrival_time) * 1000.0,
                service_time_ms=(finish - start) * 1000.0,
                e2e_latency_ms=(finish - arrival_time) * 1000.0,
                rtf=(finish - start) / audio_duration_s,
                queue_depth_at_arrival=depth_at_arrival,
                queue_depth_at_start=depth_at_start,
                cpu_percent=cpu_tracker_worker.sample(),
                cpu_freq_mhz=_read_cpu_freq_mhz(),
                cpu_temp_c=_read_cpu_temp_c(),
                rss_mb=_read_rss_mb(),
                success=success,
                error=error,
            )
            with records_lock:
                records.append(rec)

            if producer_done.is_set() and q.empty():
                worker_done.set()
                return

    def sampler_loop() -> None:
        while not stop_sampler.wait(sample_interval_s):
            samples.append(
                QueueSample(
                    experiment=experiment,
                    elapsed_s=time.monotonic() - t0,
                    queue_depth=q.qsize(),
                    cpu_percent=cpu_tracker_sampler.sample(),
                    cpu_freq_mhz=_read_cpu_freq_mhz(),
                    cpu_temp_c=_read_cpu_temp_c(),
                    rss_mb=_read_rss_mb(),
                )
            )

    producer_thread = threading.Thread(target=producer_loop, name=f"producer-{experiment}", daemon=True)
    worker_thread = threading.Thread(target=worker_loop, name=f"asr-worker-{experiment}", daemon=True)
    sampler_thread = threading.Thread(target=sampler_loop, name=f"sampler-{experiment}", daemon=True)

    producer_thread.start()
    worker_thread.start()
    sampler_thread.start()

    producer_thread.join()
    drain_start = time.monotonic()
    worker_done.wait(timeout=max_drain_s)
    drain_timed_out = not worker_done.is_set()
    drain_s = max(0.0, time.monotonic() - drain_start)

    stop_sampler.set()
    sampler_thread.join(timeout=sample_interval_s * 4)

    wallclock_s = time.monotonic() - t0
    with records_lock:
        final_records = list(records)
    n_completed = sum(1 for r in final_records if r.success)
    n_failed = sum(1 for r in final_records if not r.success)

    return LoadLevelResult(
        experiment=experiment,
        offered_load=offered_load,
        audio_duration_s=audio_duration_s,
        arrival_interval_s=interval_s,
        nominal_duration_s=duration_s,
        requests=final_records,
        samples=samples,
        n_offered=n_offered,
        n_completed=n_completed,
        n_failed=n_failed,
        wallclock_s=wallclock_s,
        drain_s=drain_s,
        drain_timed_out=drain_timed_out,
    )


# ── CSV output ───────────────────────────────────────────────────────────


def write_csv(path: Path, rows: list, row_type: type) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    field_names = [f.name for f in fields(row_type)]
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(field_names)
        for r in rows:
            writer.writerow([getattr(r, name) for name in field_names])
