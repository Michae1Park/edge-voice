#!/usr/bin/env python3
"""End-to-end (mic -> transcript) latency and RAM/CPU footprint, on the real
pipeline under sustained dual-channel load -- not the STT-only harness in
bench_streaming_cost.py.

Builds and starts the real PipelineOrchestrator in-process (same pattern as
demo_supervisor.py), then repeatedly replays a dual-channel WAV pair through
it via wav_source_raw.py (real-time paced, same as a live feed) to simulate
sustained mic input on both channels for --duration-s total.

Why segment length isn't swept like bench_streaming_cost.py's 1s..10s buffer
------------------------------------------------------------------------------
That sweep is specific to the non-streaming STT model re-decoding a growing
buffer from scratch -- an artifact of that harness, not something the real
pipeline does. Here, VAD hands STT a bounded segment (capped at
`vad.max_segment_s`, usually shorter, cut on a detected pause) exactly once.
So segment length is a real *outcome* of natural conversational audio, not a
knob to sweep -- this script records it per segment instead (see CONDITIONS
below) so its effect on latency is visible without inventing an input that
doesn't occur in production.

Three things are measured per segment, all read off instrumentation the
pipeline already has -- nothing changed in src/:

  - full_latency_ms = created_at - end. `AudioPacket.timestamp` is
    `time.time()` at MQTT ingest (audio_ingest/mqtt_client.py) and
    propagates UNCHANGED through ChannelRouter -> SpeechSegment.start/end ->
    TranscriptEvent.start/end (channel/router.py, vad/vad_worker.py,
    stt/stt_worker.py) -- so `.end` is real wall-clock time, not
    audio-relative. `.created_at` is `time.time()` when STT produces the
    event. Final transcripts only -- partials are revisable, not settled.

  - stt_latency_ms: the orchestrator's own per-segment STT decode time
    (`last_latency_s`, the same number in its "TRANSCRIPT ... latency=" log
    line), captured via a logging.Handler attached to that logger --
    SYNCHRONOUS with the STT worker thread, so it can't race the transcript
    itself: `_on_transcript` (orchestrator.py) logs it before publishing to
    TranscriptHub, and this script's async drain thread only ever sees an
    event after it's published. Reading `orch._stt.last_latency_s` directly
    instead would race across channels (it's one shared mutable attribute,
    last writer wins) -- the log capture sidesteps that entirely.

  - pre_stt_latency_ms = full - stt: everything before STT even started --
    VAD's pause/idle-flush wait plus any queue backpressure. The prior
    smoke test showed this dominating (~2s) on a segment VAD idle-flushed,
    against ~150ms of STT decode -- non-STT latency is not trivial.

CONDITIONS recorded per segment, since latency isn't just one number:
  - duration_s (end - start) and hit_cap (duration_s at/near
    settings.vad.max_segment_s) -- segment length, and whether it was a
    hard-cap cut rather than a natural pause.
  - channel_id -- rx/tx may have different speech patterns (e.g. one side
    talks in longer stretches).
  - chars -- transcript length, a rough proxy for decode cost.
  - queue depths (ingest/routed/segment) at the moment this script observed
    the transcript -- backpressure context, not an exact at-decode-time
    reading (there's no cheap synchronous hook for that without reaching
    further into orchestrator internals than this does).
  - elapsed_s since the run started -- to eyeball drift/warmup/thermal
    effects over a long sustained run, e.g. on the RPi5.

RAM/CPU are summed across this process AND the STT child processes, read
from /proc/<pid>/{status,stat}. Summing is essential once STT runs in its
own processes (settings.stt.use_processes): sampling /proc/self alone would
exclude the most expensive stage entirely and hide the very thing this
measures -- CPU rising past 100% as two decoders run in parallel. A
separately running MQTT broker is still not included (check `ps aux | grep
mosquitto` if that's wanted too). RSS and CPU are sampled at DIFFERENT
cadences on purpose -- they fail differently, so they need different
resolution:

  - RSS (--rss-interval-s, default 0.2s): what kills a process is hitting a
    ceiling (OOM), not the average, and segment decode is often shorter
    than a 1s sampling gap -- a transient spike (e.g. both channels
    decoding at once) could be allocated and freed between two 1s-apart
    samples and never show up. Sampled densely so a real peak isn't missed;
    reported as both a summary stat and the single highest reading with
    when it happened.

  - CPU (--cpu-interval-s, default 1.0s): the failure mode here isn't a
    spike, it's the pipeline systematically falling behind real-time --
    which only shows up as SUSTAINED utilization, not an instant reading
    (a 100%-for-50ms burst while a segment decodes is normal and fine).
    Averaging over a shorter window than this just adds noise without
    adding signal.

Queue depths (already recorded per segment -- see CONDITIONS) are printed
as a first-half-of-run vs second-half-of-run comparison too: growing queues
over the course of a sustained run are the actual leading indicator of
"running out of CPU" on an edge device, more directly than the CPU% number
itself.

Requires a real MQTT broker at the configured host/port (configs/default.yaml,
localhost:1883 by default -- `make install` sets this up via mosquitto on
Debian/RPi) and loads the real VAD/STT models, same as running `edge-voice`
directly. This is deliberately the real pipeline, not a mock.

Optional CPU pinning (--stt-cores / --other-cores)
---------------------------------------------------
Works for both STT modes, because Linux puts thread IDs and process IDs in
one namespace and sched_setaffinity takes either. Which attribute holds the
usable id differs, and _pin_worker() handles that: a `threading.Thread`
exposes the OS thread id as `native_id` (its `ident` is Python's own handle
and is NOT valid here), while an `STTProcessHandle` deliberately exposes
the child's pid as `ident`. --stt-cores confines the STT workers;
--other-cores (optional) confines every other worker
(ingest/router/VAD/supervisor/metrics) plus this script's own main thread
to a disjoint set, for a true dedication test rather than a preference.
Linux-only (os.sched_setaffinity).

Pinning alone does NOT stop ONNX Runtime from spawning its own internal
threads per inference call. That matters more than it sounds: with two STT
processes each spawning a full thread pool, they oversubscribe the machine
and end up SLOWER than sequential (measured 0.73x vs 1.69x on a 32-core dev
box -- see scratch/probe_mp_speedup.py). Always set
MOONSHINE_ORT_SINGLE_THREAD=1 alongside these flags.

Looping the same clip means segments near each loop boundary can be cut
oddly (abrupt silence->speech at the splice) -- a small fraction of samples,
not corrected for here.

Usage:
    python scratch/bench_pipeline_load.py
    python scratch/bench_pipeline_load.py --duration-s 300 --rss-interval-s 0.1
    python scratch/bench_pipeline_load.py --wav wav/rx_recorded_2.wav wav/tx_recorded_2.wav
    python scratch/bench_pipeline_load.py --csv-out /tmp/segments.csv

    # Single channel, to isolate whether the pipeline keeps up with ONE
    # channel of speech before blaming dual-channel concurrency:
    python scratch/bench_pipeline_load.py --channels rx --wav wav/rx_recorded_1.wav

    # Dedicate cores 2-3 to STT, everything else confined to 0-1 (combine
    # with MOONSHINE_ORT_SINGLE_THREAD=1, see scratch/bench_ort_threads.py):
    MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \\
        --stt-cores 2,3 --other-cores 0,1
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import queue
import statistics
import threading
import time
from collections.abc import Callable
from typing import Any
from dataclasses import dataclass, fields

from edge_voice.config.settings import Settings
from edge_voice.pipeline.orchestrator import PipelineOrchestrator
from edge_voice.utils.audio_generation import wav_source_raw

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("bench_pipeline_load")

CLK_TCK = os.sysconf("SC_CLK_TCK")
# How close duration_s has to be to max_segment_s to count as a hard-cap cut
# rather than a coincidentally-long natural pause-based one.
CAP_EPSILON_S = 0.15


def _read_proc(pid: str = "self") -> tuple[float, int]:
    """(rss_mb, cpu_ticks) for one process, from /proc/<pid>/{status,stat}.

    cpu_ticks is cumulative (utime+stime) since process start -- callers diff
    two readings to get CPU used over an interval. Linux-only, same
    assumption as the rest of this repo's deployment target (RPi5).
    """
    rss_kb = 0
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                rss_kb = int(line.split()[1])
                break
    with open(f"/proc/{pid}/stat") as f:
        # Fields are 1-indexed in proc(5); utime=14, stime=15. Split off the
        # comm field by its LAST ')' rather than by position -- comm can
        # itself contain spaces/parens, but never a ')' after the real one.
        after_comm = f.read().rsplit(")", 1)[1].split()
        utime, stime = int(after_comm[11]), int(after_comm[12])
    return rss_kb / 1024.0, utime + stime


def _read_proc_tree(extra_pids: list[int]) -> tuple[float, int]:
    """Summed (rss_mb, cpu_ticks) across this process and `extra_pids`.

    Load-bearing since STT moved to child processes: sampling /proc/self
    alone would exclude the single most expensive part of the pipeline, and
    would make the whole point of the change -- CPU rising above 100% as two
    decoders run in parallel -- invisible. A dead pid is skipped rather than
    raising: children come and go across a supervisor restart.
    """
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
    """Background RSS/CPU%% sampler across the pipeline's whole process tree
    -- see _read_proc_tree() and the module docstring's RAM/CPU section for
    why RSS and CPU run on different cadences.
    """

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
        # Re-read every sample rather than captured once: a supervisor
        # restart replaces a child, and its pid changes with it.
        self._extra_pids = extra_pids or (lambda: [])
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        # (elapsed_s, rss_mb) so a peak can be reported with *when* it
        # happened, not just its value.
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
            # CPU only recomputed once cpu_interval_s has actually elapsed --
            # sampling RSS densely doesn't mean we also want a CPU% reading
            # over a window too short to be meaningful (see docstring).
            if now - last_cpu_t >= self._cpu_interval_s:
                dt = now - last_cpu_t
                cpu_pct = 100.0 * ((ticks - last_cpu_ticks) / CLK_TCK) / dt if dt > 0 else 0.0
                self.cpu_pct_samples.append(cpu_pct)
                last_cpu_ticks, last_cpu_t = ticks, now


def _parse_cores(spec: str) -> set[int]:
    return {int(x) for x in spec.split(",") if x.strip()}


def _pin_worker(worker: Any, cores: set[int], label: str) -> None:
    """Pin a worker to `cores`, whether it's a thread or an STT process.

    Linux puts thread IDs and process IDs in one namespace, so
    sched_setaffinity takes either. Which attribute holds the usable id
    differs though, and getting it wrong silently pins the wrong thing:
      - threading.Thread: `native_id` is the OS TID. (`ident` is Python's
        own thread handle and is NOT valid here.)
      - STTProcessHandle: `ident` is the child's pid, by design -- see its
        docstring.
    So `native_id` must be preferred, falling back to `ident` only for the
    process handles that have no `native_id` at all.
    """
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
    os.sched_setaffinity(0, cores)  # 0 = calling thread, not "the process"
    logger.info("Pinned main thread to cores %s", sorted(cores))


class _SttLatencyCapture(logging.Handler):
    """Captures stt_latency_s per segment_id off the orchestrator's own
    TRANSCRIPT log line (orchestrator.py's `_on_transcript`) -- see the
    module docstring's stt_latency_ms entry for why this, not
    `orch._stt.last_latency_s`, is race-free.
    """

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
    # No numpy dependency here -- nearest-rank on a sorted copy is precise
    # enough for a benchmark summary, and stdlib-only matches the rest of
    # this script's no-new-deps constraint.
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
    # p99 is noise below ~20 samples (nearest-rank just returns the max) --
    # only show it once it can say something p95/max don't already.
    if len(values) >= 20:
        line += f"  p99={_percentile(values, 99):.1f}{unit}"
    return line


def _queue_depth_trend(records: list[SegmentRecord]) -> str:
    """First-half-of-run vs second-half-of-run mean total queue depth.

    Records are already in the order they were observed (chronological), so
    splitting the list in half is splitting the run in half. This is the
    "running out of CPU" signal, not CPU% itself -- see module docstring:
    queues staying flat means the pipeline is draining work as fast as it
    arrives; queues growing over the run means it's systematically falling
    behind, regardless of what any single CPU% reading says.
    """
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
    # Fixed, config-independent buckets -- unlike hit_cap, this says nothing
    # about whether a limit was enforced, just how long the segment was.
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
        help="Pin STTWorker to these CPU cores, e.g. '2,3' (default: unpinned). "
        "Combine with MOONSHINE_ORT_SINGLE_THREAD -- see docstring.",
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
        "--grace-s",
        type=float,
        default=5.0,
        help="Wait after the last loop for trailing STT decodes (default 5s)",
    )
    parser.add_argument(
        "--csv-out", default=None, help="Optional path to write one row per segment as CSV"
    )
    args = parser.parse_args()

    settings = Settings.load()
    max_segment_s = settings.vad.max_segment_s
    segment_limits_enabled = settings.vad.segment_limits_enabled
    if not segment_limits_enabled:
        logger.info(
            "vad.segment_limits_enabled=false in this config -- max_segment_s=%.1f is not "
            "actually enforced, so 'hit_cap' below will always be False (segments are cut "
            "on natural pauses / idle-flush only, however long that takes)",
            max_segment_s,
        )
    orch = PipelineOrchestrator(settings)
    orch.build()

    stt_capture = _SttLatencyCapture()
    logging.getLogger("edge_voice.pipeline.orchestrator").addHandler(stt_capture)

    records: list[SegmentRecord] = []
    sub = orch.transcripts.subscribe()
    drain_stop = threading.Event()
    run_started = time.monotonic()

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
                    # Per-channel since orchestrator.queue_depths() moved to
                    # one segment queue per channel (each channel now has its
                    # own dedicated STTWorker) -- this row's own channel's
                    # queue is the relevant one, not some other channel's.
                    q_segment=depths.get(f"segment_{ev.channel_id}", -1),
                )
            )

    drain_thread = threading.Thread(target=_drain, daemon=True)

    def _stt_child_pids() -> list[int]:
        """pids of the STT children, if STT is running in process mode.

        Empty in thread mode (workers have no `ident`-as-pid), which is
        exactly right: there everything already lives in this process.
        """
        pids = []
        for worker in orch._stt.values():
            pid = getattr(worker, "ident", None)
            # A thread also has `ident`, but no `transcript_queue` -- that's
            # what distinguishes a process handle from a worker thread here.
            if pid is not None and hasattr(worker, "transcript_queue"):
                pids.append(pid)
        return pids

    sampler = ResourceSampler(
        args.rss_interval_s, args.cpu_interval_s, run_started, extra_pids=_stt_child_pids
    )

    orch.start()
    drain_thread.start()
    sampler.start()
    time.sleep(0.5)  # let workers actually come up before publishing / before native_id exists

    if args.stt_cores:
        # orch._stt is now one dedicated worker per channel (dict), not a
        # single attribute -- pin every channel's STT worker to the same
        # core set, since "dedicate cores to STT" means all of them, not
        # just one channel's.
        for cid, stt_worker in orch._stt.items():
            _pin_worker(stt_worker, _parse_cores(args.stt_cores), f"STTWorker-{cid}")
    if args.other_cores:
        other_cores = _parse_cores(args.other_cores)
        if args.stt_cores and _parse_cores(args.stt_cores) & other_cores:
            logger.warning(
                "--stt-cores and --other-cores overlap (%s) -- not a real dedication test",
                _parse_cores(args.stt_cores) & other_cores,
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
        "Replaying %s on channels %s in a loop for %.0fs (Ctrl-C to stop early)",
        args.wav,
        args.channels,
        args.duration_s,
    )
    started = time.monotonic()
    loops = 0
    try:
        while time.monotonic() - started < args.duration_s:
            wav_source_raw.main(["--wav", *args.wav, "--channels", *args.channels])
            loops += 1
    except KeyboardInterrupt:
        logger.info("Interrupted -- wrapping up")

    elapsed = time.monotonic() - started
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

    # Caveats first, so the numbers that follow are the last thing on screen
    # rather than trailing off after them.
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
            f"TL;DR: {len(records)} segments over {loops} loop(s) -- "
            f"latency p50={_percentile(full_ms, 50):.0f}ms p95={_percentile(full_ms, 95):.0f}ms "
            f"max={max(full_ms):.0f}ms  |  "
            f"RSS mean={statistics.mean(rss_values):.0f}MB peak={max(rss_values):.0f}MB  |  "
            f"CPU mean={statistics.mean(sampler.cpu_pct_samples) if sampler.cpu_pct_samples else 0:.0f}%"
        )
        print("-" * 96)


if __name__ == "__main__":
    main()
