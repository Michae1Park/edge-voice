#!/usr/bin/env python3
"""Does cutting long VAD segments shorter reduce *total* STT inference time,
or does it just spread the same total latency across more, smaller numbers?

Feeds the same long, pause-free recording through the real pipeline once per
max_segment_s cutoff in CUTOFFS (plus one run with segment_limits_enabled=False
as an "unlimited" baseline), and sums stt_latency_s across every TRANSCRIPT
line in each run. Plots total latency and per-segment latency against cutoff.

If Moonshine's encoder cost is roughly linear in audio length, total latency
should stay flat across cutoffs. If it's superlinear (e.g. full self-attention
over the whole segment, O(n^2)-ish), shorter cutoffs should show a lower
total. This only measures wall-clock inference time as STTWorker already
defines it (see stt_worker.py:_handle_segment) -- pure _transcribe() time, no
queueing.

No MQTT broker needed -- same trick as scratch/demo_observability.py: builds
the real PipelineOrchestrator but pushes AudioPacket frames straight onto
ingest_queue instead of going through MqttAudioIngest.

Each run is settled by waiting on VADWorker's own idle-flush (config.vad.
idle_flush_s), not by tearing the pipeline down -- orchestrator.stop()'s
per-worker join has a 10s timeout (WORKER_JOIN_TIMEOUT_S), which a single
long segment's transcription can easily exceed. Waiting for idle-flush before
ever calling stop() sidesteps that entirely.

Usage:
    python scratch/demo_segment_cut_latency.py
"""

from __future__ import annotations

import json
import logging
import time

import matplotlib.pyplot as plt
import soundfile as sf
import torch
import torchaudio

from edge_voice.config.settings import Settings
from edge_voice.observability.logging import JsonFormatter, configure_logging
from edge_voice.pipeline.models import AudioPacket
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

# One continuous, pause-free utterance -- see scratch/demo_observability.py's
# comment on why this fixture (unlike rx_recorded_1.wav) is the interesting
# one here: with segment_limits_enabled=False it never gets a natural VAD
# `end`, so the *entire* recording becomes a single segment. That's the
# extreme "unlimited" baseline the swept cutoffs are compared against.
WAV_PATH = "wav/conversation_60s.wav"
CHANNEL_ID = "rx"
DRAIN_TIMEOUT_S = 240.0
PLOT_PATH = "scratch/segment_cut_latency.png"

# max_segment_s values to sweep, shortest first. None = segment_limits_enabled
# False (unlimited baseline, i.e. one segment for the whole file).
CUTOFFS: list[float | None] = [2.0, 3.0, 5.0, 7.0, 10.0, None]


class _CaptureHandler(logging.Handler):
    """Collects every formatted JSON line in memory so totals can be computed
    after the run, without scraping the terminal. Same pattern as
    scratch/demo_observability.py."""

    def __init__(self, formatter: logging.Formatter) -> None:
        super().__init__()
        self.setFormatter(formatter)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def _extract_latencies(json_lines: list[str]) -> list[float]:
    """Every stt_latency_s value logged so far -- one per TRANSCRIPT line
    (see orchestrator.py:_on_transcript)."""
    latencies: list[float] = []
    for line in json_lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        latency = payload.get("stt_latency_s")
        if latency is not None:
            latencies.append(float(latency))
    return latencies


def feed_wav(
    orch: PipelineOrchestrator, path: str, channel_id: str, incoming_ms: float, sample_rate: int
) -> None:
    """Chunk a WAV file into incoming_ms frames and push them straight onto
    ingest_queue, bypassing MqttAudioIngest entirely. Ported from
    scratch/demo_observability.py -- see that module for why a fake
    monotonically-advancing clock is fine here."""
    data, file_sr = sf.read(path, dtype="int16")
    if data.ndim > 1:
        data = data[:, 0]
    if file_sr != sample_rate:
        tensor = torch.as_tensor(data, dtype=torch.float32).unsqueeze(0)
        resampler = torchaudio.transforms.Resample(file_sr, sample_rate, lowpass_filter_width=64)
        data = resampler(tensor).squeeze(0).numpy().astype("int16")
        print(f"...resampled {path} {file_sr} -> {sample_rate} Hz")

    chunk_samples = round(incoming_ms * sample_rate / 1000.0)
    n_chunks = len(data) // chunk_samples
    ts = time.time()
    for i in range(n_chunks):
        chunk = data[i * chunk_samples : (i + 1) * chunk_samples]
        orch.ingest_queue.put(
            AudioPacket(channel_id=channel_id, timestamp=ts, samples=chunk.tobytes()), timeout=5.0
        )
        ts += incoming_ms / 1000.0
    print(
        f"...fed {n_chunks} packets ({n_chunks * incoming_ms / 1000.0:.1f}s of audio) "
        f"on channel {channel_id!r}"
    )


def run_once(max_segment_s: float | None) -> tuple[int, float, list[float]]:
    """Runs the pipeline over WAV_PATH with the given max_segment_s cutoff
    (None = segment_limits_enabled False, i.e. unlimited) and returns
    (segment_count, total_stt_latency_s, per_segment_latencies).
    """
    settings = Settings.load()
    settings.logging_.is_json = True  # force JSON regardless of configs/local.yaml
    settings.logging_.console_enabled = False  # keep this script's own output readable
    settings.logging_.file_enabled = False  # this is throwaway data, not worth a log file
    settings.reliability.watchdog_enabled = False  # no systemd here
    if max_segment_s is None:
        settings.vad.segment_limits_enabled = False
    else:
        settings.vad.segment_limits_enabled = True
        settings.vad.max_segment_s = max_segment_s
        # Keep soft_cut_s meaningfully below the hard cap so there's room to
        # find a natural pause, same shape as configs/default.yaml's
        # soft_cut_s=3.0/max_segment_s=5.0 (2s of headroom).
        settings.vad.soft_cut_s = max(1.0, max_segment_s - 2.0)

    configure_logging(settings.logging_)
    capture = _CaptureHandler(JsonFormatter())
    root = logging.getLogger()
    root.addHandler(capture)

    orch = PipelineOrchestrator(settings)
    orch.build()
    orch.start()
    time.sleep(0.5)

    feed_wav(
        orch,
        WAV_PATH,
        CHANNEL_ID,
        incoming_ms=settings.repacketizer.incoming_ms,
        sample_rate=settings.audio.sample_rate,
    )

    # Settle by watching for VADWorker's own idle-flush (config.vad.
    # idle_flush_s) to fire and STT to catch up -- not by tearing the
    # pipeline down. A single long segment's transcription can exceed
    # orchestrator.stop()'s WORKER_JOIN_TIMEOUT_S (10s), so this script never
    # calls stop() until it has already confirmed every expected segment
    # landed. "Settled" = queues empty and segment count unchanged for a
    # full idle_flush_s + 1s margin.
    deadline = time.time() + DRAIN_TIMEOUT_S
    last_count = -1
    stable_since: float | None = None
    settle_window = settings.vad.idle_flush_s + 1.0
    while time.time() < deadline:
        depths = orch.queue_depths()
        queues_empty = depths.get("routed", 0) == 0 and depths.get("segment", 0) == 0
        count = len(_extract_latencies(capture.lines))
        if queues_empty and count > 0 and count == last_count:
            stable_since = stable_since or time.time()
            if time.time() - stable_since >= settle_window:
                break
        else:
            stable_since = None
        last_count = count
        time.sleep(0.5)
    else:
        print(f"WARNING: settle timed out after {DRAIN_TIMEOUT_S}s -- results may be incomplete")

    orch.stop()  # queues are already empty and STT idle by now, so this is a clean, fast teardown
    orch.wait()
    root.removeHandler(capture)

    latencies = _extract_latencies(capture.lines)
    return len(latencies), sum(latencies), latencies


def plot_results(results: list[tuple[float | None, int, float, list[float]]]) -> None:
    labels = [f"{c:g}s" if c is not None else "unlimited" for c, _, _, _ in results]
    totals = [total for _, _, total, _ in results]
    counts = [n for _, n, _, _ in results]

    fig, (ax_total, ax_scatter) = plt.subplots(1, 2, figsize=(12, 5))

    ax_total.bar(labels, totals, color="#4C72B0")
    for i, n in enumerate(counts):
        ax_total.text(i, totals[i], f"n={n}", ha="center", va="bottom", fontsize=9)
    ax_total.set_xlabel("max_segment_s cutoff")
    ax_total.set_ylabel("total STT latency (s)")
    ax_total.set_title(f"Total STT inference time vs. cutoff\n({WAV_PATH})")

    for i, (cutoff, _, _, per_segment) in enumerate(results):
        xs = [i] * len(per_segment)
        ax_scatter.scatter(xs, per_segment, alpha=0.6, color="#DD8452")
    ax_scatter.set_xticks(range(len(labels)))
    ax_scatter.set_xticklabels(labels)
    ax_scatter.set_xlabel("max_segment_s cutoff")
    ax_scatter.set_ylabel("per-segment STT latency (s)")
    ax_scatter.set_title("Per-segment latency spread")

    fig.tight_layout()
    fig.savefig(PLOT_PATH, dpi=150)
    print(f"\nSaved plot to {PLOT_PATH}")


def main() -> None:
    results: list[tuple[float | None, int, float, list[float]]] = []
    for cutoff in CUTOFFS:
        label = f"max_segment_s={cutoff}" if cutoff is not None else "segment_limits_enabled=False"
        section(f"Run: {label}")
        n, total, per_segment = run_once(cutoff)
        print(f"segments={n} total_stt_latency_s={total:.3f} per_segment={per_segment}")
        results.append((cutoff, n, total, per_segment))

    section("Comparison")
    print(f"{'cutoff':15}{'segments':>10}{'total latency (s)':>20}")
    for cutoff, n, total, _ in results:
        label = f"{cutoff:g}s" if cutoff is not None else "unlimited"
        print(f"{label:15}{n:>10}{total:>20.3f}")

    baseline_total = next(total for cutoff, _, total, _ in results if cutoff is None)
    if baseline_total > 0:
        print(f"\n% change in total STT latency vs. unlimited baseline ({baseline_total:.3f}s):")
        for cutoff, _, total, _ in results:
            if cutoff is None:
                continue
            pct = (baseline_total - total) / baseline_total * 100
            print(f"  max_segment_s={cutoff:g}: {pct:+.1f}%")

    plot_results(results)


if __name__ == "__main__":
    main()
