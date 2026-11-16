#!/usr/bin/env python3
"""Where does "pre-STT overhead" (full latency - STT decode) go?

measure_sbc.py reports full_latency = transcript.created_at - segment.end and
stt_latency = decode only; the gap is ~22 ms on an RPi5 but ~800 ms on an OPI5.
This runs the same real-pipeline scenario with in-process instrumentation (no
src/ changes; methods are wrapped at class level) and splits the gap into:

  end -> finalize   segment.end (audio clock) -> VADWorker._finalize_segment.
                    Ingest + router + VAD detection lag: how far behind real
                    time the audio is being processed, plus VAD's own hold-off.
  finalize -> decode  queue + IPC + transcriber start, up to decode start
                    (created_at - stt_latency, i.e. decode start in wall time).
  decode            STT decode itself.

Also records, per stage, how far behind wall clock each packet is
(time.time() - packet.timestamp) at router input and VAD input, the wall time
of every VADWorker._handle_packet call (Silero real-time factor vs the 32 ms
window), and peak queue depths.

Works whether the checkout's package is `edge_voice` or `ondevice_stt`.

Usage:
    python characterization/scripts/profile_pre_stt.py --cores 4-7
    MOONSHINE_ORT_SINGLE_THREAD=0 python characterization/scripts/profile_pre_stt.py --cores 4-7
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import logging
import os
import platform
import queue
import statistics
import sys
import threading
import time
from pathlib import Path

os.environ.setdefault("MOONSHINE_ORT_SINGLE_THREAD", "1")

THIS_DIR = Path(__file__).resolve().parent
ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(THIS_DIR))

PKG = next(p for p in ("edge_voice", "ondevice_stt") if importlib.util.find_spec(p))


def _imp(mod: str):
    return importlib.import_module(f"{PKG}.{mod}")


Settings = _imp("config.settings").Settings
PipelineOrchestrator = _imp("pipeline.orchestrator").PipelineOrchestrator
wav_source = _imp("utils.audio_generation.wav_source")
VADWorker = _imp("vad.vad_worker").VADWorker
Repacketizer = _imp("channel.router").Repacketizer

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("profile_pre_stt")

DEFAULT_WAV = ROOT / "wav" / "rx_recorded_1.wav"
WARMUP_WAV = THIS_DIR.parent / "testaudio" / "warmup.wav"
RESULTS_DIR = THIS_DIR.parent / "results" / "sbc_compare"


def _parse_cores(spec: str) -> set[int]:
    cores: set[int] = set()
    for part in spec.split(","):
        lo, _, hi = part.strip().partition("-")
        cores.update(range(int(lo), int(hi or lo) + 1))
    return cores


def _stats(v: list[float]) -> dict[str, float]:
    if not v:
        return {"n": 0}
    s = sorted(v)
    return {
        "n": len(s),
        "mean": statistics.fmean(s),
        "p50": s[len(s) // 2],
        "p95": s[min(len(s) - 1, round(0.95 * (len(s) - 1)))],
        "max": s[-1],
    }


class Probe:
    """Collects timings once `armed` (i.e. after warm-up)."""

    def __init__(self) -> None:
        self.armed = False
        self.n_partials = 0
        self.lock = threading.Lock()
        self.router_lag_ms: list[float] = []
        self.vad_lag_ms: list[float] = []
        self.vad_call_ms: list[float] = []
        self.finalize: dict[str, tuple[float, float]] = {}  # segment_id -> (end_ts, wall)
        self.peak_depth: dict[str, int] = {}
        self.depth_series: list[tuple[float, dict[str, int]]] = []

    def install(self) -> None:
        probe = self
        orig_process = Repacketizer.process
        orig_handle = VADWorker._handle_packet
        orig_final = VADWorker._finalize_segment

        def process(self_, packet):  # type: ignore[no-untyped-def]
            if probe.armed:
                with probe.lock:
                    probe.router_lag_ms.append((time.time() - packet.timestamp) * 1000)
            return orig_process(self_, packet)

        def handle(self_, packet):  # type: ignore[no-untyped-def]
            t0 = time.time()
            if probe.armed:
                with probe.lock:
                    probe.vad_lag_ms.append((t0 - packet.timestamp) * 1000)
            try:
                return orig_handle(self_, packet)
            finally:
                if probe.armed:
                    with probe.lock:
                        probe.vad_call_ms.append((time.time() - t0) * 1000)

        def finalize(self_, channel_id, state, end_ts):  # type: ignore[no-untyped-def]
            seg_id = state.segment_id
            if probe.armed and seg_id is not None:
                with probe.lock:
                    probe.finalize[seg_id] = (end_ts, time.time())
            return orig_final(self_, channel_id, state, end_ts)

        Repacketizer.process = process  # type: ignore[method-assign]
        VADWorker._handle_packet = handle  # type: ignore[method-assign]
        VADWorker._finalize_segment = finalize  # type: ignore[method-assign]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--wav", type=Path, default=DEFAULT_WAV)
    ap.add_argument("--channel", default="rx")
    ap.add_argument("--cores")
    ap.add_argument("--tag", default="", help="suffix for the result filename")
    ap.add_argument("--settle-s", type=float, default=4.0)
    ap.add_argument("--max-wait-s", type=float, default=60.0)
    args = ap.parse_args()

    if args.cores:
        os.sched_setaffinity(0, _parse_cores(args.cores))
    affinity = sorted(os.sched_getaffinity(0))

    probe = Probe()
    probe.install()

    settings = Settings.load()
    settings.mqtt.channels = [c for c in settings.mqtt.channels if c.channel_id == args.channel]
    orch = PipelineOrchestrator(settings)
    orch.build()

    stt_by_seg: dict[str, float] = {}

    class _Cap(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            sid = getattr(record, "segment_id", None)
            lat = getattr(record, "stt_latency_s", None)
            if sid is not None and lat is not None:
                stt_by_seg[sid] = lat

    orch_log = logging.getLogger(f"{PKG}.pipeline.orchestrator")
    orch_log.setLevel(logging.INFO)
    orch_log.addHandler(_Cap(level=logging.INFO))

    events: list[dict] = []
    sub = orch.transcripts.subscribe()
    stop = threading.Event()

    def drain() -> None:
        while True:
            try:
                ev = sub.get(timeout=0.5)
            except queue.Empty:
                if stop.is_set():
                    return
                continue
            if not ev.is_final and probe.armed:
                probe.n_partials += 1
            if ev.is_final and probe.armed:
                events.append({"segment_id": ev.segment_id, "end": ev.end, "start": ev.start,
                               "created_at": ev.created_at, "text": ev.text})

    def sample_depths() -> None:
        while not stop.is_set():
            d = {}
            for name in ("_ingest_queue", "_routed_queue"):
                q = getattr(orch, name, None)
                if q is not None:
                    d[name.strip("_")] = q.qsize()
            for cid, q in (getattr(orch, "_segment_queues", None) or {}).items():
                d[f"segment_{cid}"] = q.qsize()
            if probe.armed:
                for k, v in d.items():
                    probe.peak_depth[k] = max(probe.peak_depth.get(k, 0), v)
                probe.depth_series.append((time.time(), d))
            time.sleep(0.05)

    threads = [threading.Thread(target=drain, daemon=True), threading.Thread(target=sample_depths, daemon=True)]
    orch.start()
    for t in threads:
        t.start()
    time.sleep(0.5)

    wav_source.main(["--wav", str(WARMUP_WAV), "--channels", args.channel])
    time.sleep(args.settle_s)
    probe.armed = True
    t_pub0 = time.time()
    wav_source.main(["--wav", str(args.wav), "--channels", args.channel])
    t_pub1 = time.time()
    deadline = time.time() + max(0.0, args.max_wait_s - (t_pub1 - t_pub0))
    while time.time() < deadline and len(events) < 4:
        time.sleep(0.2)
    time.sleep(3.0)
    probe.armed = False

    stop.set()
    for t in threads:
        t.join(timeout=2)
    orch.transcripts.unsubscribe(sub)
    orch.stop()
    orch.wait()

    rows = []
    for e in events:
        sid = e["segment_id"]
        stt_s = stt_by_seg.get(sid)
        fin = probe.finalize.get(sid)
        if stt_s is None or fin is None:
            continue
        end_ts, fin_wall = fin
        decode_start = e["created_at"] - stt_s
        rows.append({
            "segment_id": sid,
            "dur_s": e["end"] - e["start"],
            "full_ms": (e["created_at"] - e["end"]) * 1000,
            "end_to_finalize_ms": (fin_wall - end_ts) * 1000,
            "finalize_to_decode_ms": (decode_start - fin_wall) * 1000,
            "decode_ms": stt_s * 1000,
        })

    summary = {
        "hostname": platform.node(),
        "cores": args.cores,
        "affinity": affinity,
        "partial_interval_s": settings.vad.partial_interval_s,
        "n_partial_events": probe.n_partials,
        "moonshine_single_thread": os.environ.get("MOONSHINE_ORT_SINGLE_THREAD"),
        "publish_wall_s": t_pub1 - t_pub0,
        "segments": rows,
        "router_lag_ms": _stats(probe.router_lag_ms),
        "vad_lag_ms": _stats(probe.vad_lag_ms),
        "vad_call_ms": _stats(probe.vad_call_ms),
        "vad_window_ms": 32.0,
        "peak_queue_depth": probe.peak_depth,
    }
    tag = f"-{args.tag}" if args.tag else ""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / f"{summary['hostname']}-profile{tag}.json"
    out.write_text(json.dumps(summary, indent=2))

    print("\n" + "=" * 90)
    print(f"{summary['hostname']}  cores={args.cores} affinity={affinity}  ORT_SINGLE={summary['moonshine_single_thread']}")
    print("=" * 90)
    print(f"{'segment':<28}{'dur_s':>6}{'full':>8}{'end->fin':>10}{'fin->dec':>10}{'decode':>8}   (ms)")
    for r in rows:
        print(f"{r['segment_id'][-26:]:<28}{r['dur_s']:>6.2f}{r['full_ms']:>8.0f}{r['end_to_finalize_ms']:>10.0f}"
              f"{r['finalize_to_decode_ms']:>10.0f}{r['decode_ms']:>8.0f}")
    for k in ("router_lag_ms", "vad_lag_ms", "vad_call_ms"):
        s = summary[k]
        if s.get("n"):
            print(f"{k:<14} n={s['n']:<5} mean={s['mean']:.1f} p50={s['p50']:.1f} p95={s['p95']:.1f} max={s['max']:.1f}")
    print(f"partial_interval_s={summary['partial_interval_s']}  partial events seen={probe.n_partials}")
    print("peak queue depth:", summary["peak_queue_depth"])
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
