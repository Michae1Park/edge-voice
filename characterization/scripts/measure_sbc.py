#!/usr/bin/env python3
"""Cross-SBC characterization: E2E latency + CER on one real recording.

Unlike run_experiment.py (synthetic density-tiled single-sentence audio,
built for the density-sweep experiment), this measures the real
PipelineOrchestrator's behavior on an actual recording with a known,
human-verified transcript -- wav/rx_recorded_1.wav by default, the same
fixture tests/test_pipeline_integration.py uses to verify VAD segment
counts (EXPECTED_SEGMENTS["rx"] == 4).

Intended to be run unmodified on each SBC under comparison (same config,
same file, same script) so results land side by side in
characterization/results/sbc_compare/<hostname>.json.

Per run:
  - full_latency_ms / stt_latency_ms per finalized segment -- same
    instrumentation as run_experiment.py / bench_pipeline_load.py.
  - CER: reference is the fixture's transcript file (utterances separated
    by a "-----" line); hypothesis is the final transcripts concatenated
    in arrival order. Single channel + in-order-per-channel STT (see
    stt_worker.py) means arrival order == audio order, so no alignment
    step is needed. See cer.py for why whitespace is stripped before
    diffing.

Requires a real MQTT broker (configs/default.yaml, localhost:1883 by
default) and loads the real VAD/STT models -- not mocked.

Usage:
    python characterization/scripts/measure_sbc.py
    python characterization/scripts/measure_sbc.py --repeats 3
    python characterization/scripts/measure_sbc.py --cores 4-7   # pin (big.LITTLE)
    python characterization/scripts/measure_sbc.py --wav wav/tx_recorded_1.wav \\
        --transcript wav/tx_recorded_1_transcription.txt --channel tx
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform

os.environ.setdefault("MOONSHINE_ORT_SINGLE_THREAD", "1")

import queue
import re
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cer import char_cer  # noqa: E402

from edge_voice.config.settings import Settings  # noqa: E402
from edge_voice.pipeline.orchestrator import PipelineOrchestrator  # noqa: E402
from edge_voice.utils.audio_generation import wav_source  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("measure_sbc")

THIS_DIR = Path(__file__).resolve().parent
ROOT = THIS_DIR.parent.parent
CHARACTERIZATION_DIR = THIS_DIR.parent
DEFAULT_WAV = ROOT / "wav" / "rx_recorded_1.wav"
DEFAULT_TRANSCRIPT = ROOT / "wav" / "rx_recorded_1_transcription.txt"
WARMUP_WAV = CHARACTERIZATION_DIR / "testaudio" / "warmup.wav"
RESULTS_DIR = CHARACTERIZATION_DIR / "results" / "sbc_compare"

WARMUP_SETTLE_S = 4.0
GRACE_AFTER_COMPLETE_S = 3.0
MAX_WAIT_AFTER_PUBLISH_S = 60.0


def _parse_cores(spec: str) -> set[int]:
    """Parse a taskset-style core list, e.g. "4-7" or "0,2,4-5"."""
    cores: set[int] = set()
    for part in spec.split(","):
        lo, _, hi = part.strip().partition("-")
        cores.update(range(int(lo), int(hi or lo) + 1))
    return cores


def _load_reference(transcript_path: Path) -> list[str]:
    """Split a "-----"-delimited transcript file into per-utterance lines."""
    raw = transcript_path.read_text(encoding="utf-8")
    parts = re.split(r"-{3,}", raw)
    lines = [p.strip() for p in parts if p.strip()]
    if not lines:
        raise SystemExit(f"No utterances found in {transcript_path}")
    return lines


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
    repeat_idx: int
    order: int
    segment_id: str
    duration_s: float
    text: str
    full_latency_ms: float
    stt_latency_ms: float | None


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[idx]


def _stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {k: float("nan") for k in ("n", "mean", "p50", "p95", "min", "max")}
    return {
        "n": len(values),
        "mean": sum(values) / len(values),
        "p50": _percentile(values, 50),
        "p95": _percentile(values, 95),
        "min": min(values),
        "max": max(values),
    }


def _fmt_stats(label: str, s: dict[str, float], unit: str) -> str:
    return (
        f"{label}: n={s['n']:.0f}  mean={s['mean']:.1f}{unit}  p50={s['p50']:.1f}{unit}  "
        f"p95={s['p95']:.1f}{unit}  min={s['min']:.1f}{unit}  max={s['max']:.1f}{unit}"
    )


def run_one(
    wav_path: Path,
    reference_lines: list[str],
    channel: str,
    repeat_idx: int,
    max_wait_s: float,
    mqtt_port: int | None = None,
    warmup_settle_s: float = WARMUP_SETTLE_S,
) -> tuple[dict, list[SegmentRecord]]:
    settings = Settings.load()
    configured_ids = {c.channel_id for c in settings.mqtt.channels}
    if channel not in configured_ids:
        raise SystemExit(f"--channel {channel!r} not in configs' mqtt.channels ({sorted(configured_ids)})")
    settings.mqtt.channels = [c for c in settings.mqtt.channels if c.channel_id == channel]
    port_args = []
    if mqtt_port is not None:
        settings.mqtt.broker_port = mqtt_port
        port_args = ["--port", str(mqtt_port)]

    orch = PipelineOrchestrator(settings)
    orch.build()

    stt_capture = _SttLatencyCapture()
    logging.getLogger("edge_voice.pipeline.orchestrator").addHandler(stt_capture)

    segment_records: list[SegmentRecord] = []
    sub = orch.transcripts.subscribe()
    drain_stop = threading.Event()
    n_expected = len(reference_lines)
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
            if not ev.is_final or in_warmup.is_set():
                continue
            stt_s = stt_capture.by_segment.get(ev.segment_id)
            segment_records.append(
                SegmentRecord(
                    repeat_idx=repeat_idx,
                    order=order,
                    segment_id=ev.segment_id,
                    duration_s=ev.end - ev.start,
                    text=ev.text,
                    full_latency_ms=(ev.created_at - ev.end) * 1000,
                    stt_latency_ms=stt_s * 1000 if stt_s is not None else None,
                )
            )
            order += 1
            if len(segment_records) >= n_expected:
                all_arrived.set()

    drain_thread = threading.Thread(target=_drain, daemon=True)

    orch.start()
    drain_thread.start()
    time.sleep(0.5)  # let workers come up before publishing

    logger.info("[repeat %d] warm-up: publishing %s, then a %.1fs settle window", repeat_idx, WARMUP_WAV, warmup_settle_s)
    wav_source.main(["--wav", str(WARMUP_WAV), "--channels", channel, *port_args])
    time.sleep(warmup_settle_s)
    in_warmup.clear()

    logger.info("[repeat %d] publishing %s on channel %s (expect %d segments)", repeat_idx, wav_path, channel, n_expected)
    publish_started = time.monotonic()
    wav_source.main(["--wav", str(wav_path), "--channels", channel, *port_args])
    publish_done = time.monotonic()

    all_arrived.wait(timeout=max(0.0, max_wait_s - (publish_done - publish_started)))
    time.sleep(GRACE_AFTER_COMPLETE_S)

    drain_stop.set()
    drain_thread.join(timeout=2.0)
    orch.transcripts.unsubscribe(sub)
    orch.stop()
    orch.wait()
    logging.getLogger("edge_voice.pipeline.orchestrator").removeHandler(stt_capture)

    n_got = len(segment_records)
    if n_got != n_expected:
        logger.warning(
            "[repeat %d] expected %d final segments, got %d -- CER below still computed against "
            "what actually arrived", repeat_idx, n_expected, n_got,
        )

    hypothesis = " ".join(r.text for r in segment_records)
    reference = " ".join(reference_lines)
    cer = char_cer(reference, hypothesis)

    result = {
        "repeat_idx": repeat_idx,
        "n_segments": n_got,
        "n_expected": n_expected,
        "cer": cer,
        "hypothesis": hypothesis,
    }
    return result, segment_records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--wav", type=Path, default=DEFAULT_WAV)
    parser.add_argument("--transcript", type=Path, default=DEFAULT_TRANSCRIPT)
    parser.add_argument("--channel", default="rx")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--results-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument(
        "--cores",
        help="Pin this process (and the STT/VAD child processes it spawns) to these CPUs, "
        'taskset-style, e.g. "4-7". Needed on big.LITTLE SBCs (RK3588: 0-3 = A55, 4-7 = A76) '
        "where the scheduler may otherwise place decode on a little core.",
    )
    parser.add_argument(
        "--mqtt-port",
        type=int,
        help="Broker port for both the pipeline and the wav publisher. Point at a private "
        "mosquitto so a live service on the default port doesn't also transcribe the benchmark audio.",
    )
    parser.add_argument(
        "--warmup-settle-s",
        type=float,
        default=WARMUP_SETTLE_S,
        help="Wait after the warm-up clip before measuring. Must exceed the warm-up segment's "
        "decode time, or its transcript leaks into the measured window (seen on A55: ~10s decode).",
    )
    parser.add_argument("--max-wait-s", type=float, default=MAX_WAIT_AFTER_PUBLISH_S)
    args = parser.parse_args()

    if not args.wav.exists():
        raise SystemExit(f"{args.wav} not found")
    if not args.transcript.exists():
        raise SystemExit(f"{args.transcript} not found")
    reference_lines = _load_reference(args.transcript)

    # Before any worker starts: children inherit affinity from the parent.
    if args.cores:
        os.sched_setaffinity(0, _parse_cores(args.cores))
    affinity = sorted(os.sched_getaffinity(0))
    logger.info("CPU affinity: %s", affinity)

    args.results_dir.mkdir(parents=True, exist_ok=True)

    all_segment_records: list[SegmentRecord] = []
    all_run_results: list[dict] = []
    for repeat_idx in range(args.repeats):
        result, segments = run_one(
            args.wav, reference_lines, args.channel, repeat_idx, args.max_wait_s, args.mqtt_port,
            args.warmup_settle_s,
        )
        all_run_results.append(result)
        all_segment_records.extend(segments)
        logger.info(
            "[repeat %d] segments=%d/%d  CER=%.1f%%", repeat_idx, result["n_segments"], result["n_expected"],
            result["cer"] * 100,
        )

    latency_rows = [r for r in all_segment_records if r.order != 0]  # see run_experiment.py's note on order==0
    summary = {
        "hostname": platform.node(),
        "machine": platform.machine(),
        "wav": str(args.wav.relative_to(ROOT)) if args.wav.is_relative_to(ROOT) else str(args.wav),
        "channel": args.channel,
        "repeats": args.repeats,
        "cores": args.cores,
        "affinity": affinity,
        "full_latency_ms": _stats([r.full_latency_ms for r in latency_rows]),
        "stt_latency_ms": _stats([r.stt_latency_ms for r in latency_rows if r.stt_latency_ms is not None]),
        "cer_pct": _stats([r["cer"] * 100 for r in all_run_results]),
        "runs": all_run_results,
    }

    suffix = f"-cores{args.cores.replace(',', '_')}" if args.cores else ""
    out_path = args.results_dir / f"{summary['hostname']}{suffix}.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    print("\n" + "=" * 100)
    print(f"SBC: {summary['hostname']} ({summary['machine']})  wav={summary['wav']}  n_repeats={args.repeats}")
    print("=" * 100)
    print("  " + _fmt_stats("Full E2E latency", summary["full_latency_ms"], "ms"))
    print("  " + _fmt_stats("  of which STT decode", summary["stt_latency_ms"], "ms"))
    print("  " + _fmt_stats("CER", summary["cer_pct"], "%"))
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
