#!/usr/bin/env python3
# Manual demo of Milestone 7's observability/metrics.py (see docs/BUILDPLAN.md).
# Usage: python scratch/demo_metrics.py

from __future__ import annotations

import time

import soundfile as sf
import torch
import torchaudio

from edge_voice.config.settings import Settings
from edge_voice.observability.logging import configure_logging
from edge_voice.pipeline.models import AudioPacket
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

WAV_PATH = "wav/rx_recorded_1.wav"
CHANNEL_ID = "rx"


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def feed_wav(
    orch: PipelineOrchestrator, path: str, channel_id: str, incoming_ms: float, sample_rate: int
) -> None:
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


def print_snapshot(orch: PipelineOrchestrator, label: str) -> None:
    collector = orch._metrics  # type: ignore[attr-defined]
    snapshot = collector.snapshot() if collector is not None else None
    if snapshot is None:
        print(f"{label}: no snapshot yet (first tick hasn't landed)")
        return
    latency = f"{snapshot.stt_last_latency_s:.3f}s" if snapshot.stt_last_latency_s else None
    print(
        f"{label}: queues={snapshot.queue_depths} stt_latency={latency} "
        f"mqtt_connected={snapshot.mqtt_connected} "
        f"restart_budget={snapshot.restart_status} "
        f"(max_restarts={snapshot.max_restarts}, restart_window_s={snapshot.restart_window_s})"
    )


def main() -> None:
    settings = Settings.load()
    settings.reliability.watchdog_enabled = False
    settings.metrics.emit_interval_s = 1.0

    configure_logging(settings.logging_)

    orch = PipelineOrchestrator(settings)
    orch.build()
    orch.start()
    time.sleep(0.5)

    section("1. Before any audio: queues empty, STT latency unset")
    time.sleep(1.5)
    print_snapshot(orch, "snapshot")

    section("2. Feeding audio directly onto ingest_queue (bypassing MQTT)")
    feed_wav(
        orch,
        WAV_PATH,
        CHANNEL_ID,
        incoming_ms=settings.repacketizer.incoming_ms,
        sample_rate=settings.audio.sample_rate,
    )

    section("3. Watching snapshots tick while VAD + STT catch up")
    deadline = time.time() + 60.0
    while time.time() < deadline:
        print_snapshot(orch, f"t+{settings.metrics.emit_interval_s:.0f}s-ish")
        depths = orch.queue_depths()
        if depths.get("routed", 0) == 0 and depths.get("segment", 0) == 0:
            break
        time.sleep(settings.metrics.emit_interval_s)

    section("4. Final snapshot -- STT latency should now be non-null")
    time.sleep(settings.metrics.emit_interval_s + 0.5)
    print_snapshot(orch, "final")

    section("Shutting down")
    orch.stop()
    orch.wait()


if __name__ == "__main__":
    main()
