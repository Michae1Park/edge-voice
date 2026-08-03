#!/usr/bin/env python3
"""Manual demo of the Milestone 7 observability plumbing (see docs/BUILDPLAN.md).

Feeds a real WAV recording through the live pipeline with JSON logging
enabled, then reconstructs one segment's full lifecycle -- audio_ingest ->
channel -> vad -> stt -- from the JSON log sink alone by filtering on its
segment_id. That reconstruction is exactly the Milestone 7 "Done when"
criterion for observability/logging.py. queue_depths(), MQTT connectivity,
and the supervisor's restart budget (all wired in 255513e but not yet read
by any caller) are also printed as a sanity check ahead of metrics.py /
health/reporting.py.

No MQTT broker needed -- like scratch/demo_supervisor.py, this builds the
real PipelineOrchestrator but pushes AudioPacket frames straight onto
ingest_queue instead of going through MqttAudioIngest, so it works whether
or not a broker happens to be reachable.

Usage:
    python scratch/demo_observability.py
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict

import soundfile as sf
import torch
import torchaudio

from edge_voice.config.settings import Settings
from edge_voice.observability.logging import JsonFormatter, configure_logging
from edge_voice.pipeline.models import AudioPacket
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

# Same fixture tests/test_pipeline_integration.py uses -- documented to
# produce 4 distinct VAD segments over 30s of real speech-with-pauses,
# unlike conversation_60s.wav (one uninterrupted utterance -> a single
# segment only finalized at shutdown, which defeats the point of watching
# queue depths drain mid-run).
WAV_PATH = "wav/rx_recorded_1.wav"
CHANNEL_ID = "rx"


class _CaptureHandler(logging.Handler):
    """Collects every formatted JSON line in memory, in addition to the live
    copy configure_logging() sends to stderr -- so the trace-reconstruction
    step below can filter on segment_id after the run instead of scraping
    the terminal."""

    def __init__(self, formatter: logging.Formatter) -> None:
        super().__init__()
        self.setFormatter(formatter)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def feed_wav(
    orch: PipelineOrchestrator, path: str, channel_id: str, incoming_ms: float, sample_rate: int
) -> None:
    """Chunk a WAV file into incoming_ms frames and push them straight onto
    ingest_queue, bypassing MqttAudioIngest entirely -- same trick
    demo_supervisor.py uses to inject SpeechSegments straight onto
    segment_queue. Only the first packet's timestamp matters (it anchors
    the repacketizer's clock; see channel/router.py's Repacketizer.process),
    so a monotonically-advancing fake clock is as good as a real one.
    """
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


def reconstruct_one_segment(json_lines: list[str]) -> None:
    by_segment: dict[str, list[dict]] = defaultdict(list)
    for line in json_lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue  # shouldn't happen -- JsonFormatter is the only formatter attached
        segment_id = payload.get("segment_id")
        if segment_id:
            by_segment[segment_id].append(payload)

    if not by_segment:
        print("No segment_id-tagged log lines captured -- no speech was detected in the fed audio.")
        return

    # The segment with the most distinct stages is the fullest trace to show off.
    segment_id, records = max(
        by_segment.items(), key=lambda kv: len({r.get("stage") for r in kv[1]})
    )
    stages_seen = [r.get("stage") for r in records]
    print(f"Reconstructed trace for segment_id={segment_id!r} (stages seen: {stages_seen})\n")
    for r in records:
        print(f"  [{r['timestamp']}] {r['level']:<7} {r.get('stage', '?'):<8} {r['message']}")


def main() -> None:
    settings = Settings.load()
    settings.logging_.is_json = True  # force JSON regardless of configs/local.yaml
    settings.reliability.watchdog_enabled = False  # no systemd here

    configure_logging(settings.logging_)  # live JSON lines to stderr, as in production
    capture = _CaptureHandler(JsonFormatter())
    logging.getLogger().addHandler(capture)

    orch = PipelineOrchestrator(settings)
    orch.build()
    orch.start()
    time.sleep(0.5)

    section("1. Feeding audio directly onto ingest_queue (bypassing MQTT)")
    feed_wav(
        orch,
        WAV_PATH,
        CHANNEL_ID,
        incoming_ms=settings.repacketizer.incoming_ms,
        sample_rate=settings.audio.sample_rate,
    )

    section("2. Draining: waiting for VAD + STT to catch up")
    deadline = time.time() + 60.0
    while time.time() < deadline:
        depths = orch.queue_depths()
        if depths.get("routed", 0) == 0 and depths.get("segment", 0) == 0:
            break
        time.sleep(1.0)
    print(f"queue_depths(): {orch.queue_depths()}")

    section("3. Live plumbing snapshot (Milestone 7 accessors, none wired to a caller yet)")
    print(f"get_status():   {orch.get_status()}")
    print(f"queue_depths(): {orch.queue_depths()}")
    # Reaching into worker internals on purpose, same as demo_supervisor.py --
    # orchestrator.get_status() doesn't surface these yet, so there's no
    # public accessor to call instead.
    print(f"MQTT connected: {orch._audio_source.connected}")  # type: ignore[union-attr]
    sup = orch._supervisor
    if sup is not None:
        print(
            f"restart budget: max_restarts={sup.max_restarts} restart_window_s={sup.restart_window_s}"
        )
        print(f"supervisor status(): {sup.status()}")

    section("4. Reconstructing one segment's trace from the JSON log sink alone")
    reconstruct_one_segment(capture.lines)

    section("Shutting down")
    orch.stop()
    orch.wait()


if __name__ == "__main__":
    main()
