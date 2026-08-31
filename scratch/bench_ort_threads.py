"""Ad-hoc: does MOONSHINE_ORT_SINGLE_THREAD change inference latency?

libmoonshine.so links onnxruntime and reads this env var at session-creation
time (found via `strings`/`ldd`). Confirmed by disassembling
`ort_maybe_force_single_thread` in libmoonshine.so (2026-08-11): it's
`getenv("MOONSHINE_ORT_SINGLE_THREAD")`, and forces intra_op_num_threads=1
UNLESS the value is NULL, empty, or the literal string "0" -- those three
all jump to the same skip-forcing branch. So unset / "" / "0" are identical
(ORT's default multi-threaded intra-op pool); "1" or any other non-empty,
non-"0" string forces single-thread.

Must set EDGE_VOICE_STT__USE_PROCESSES=false when running this -- since
settings.stt.use_processes defaults True, without the override this goes
through two STT *processes* (rx/tx) instead of the single in-process decoder
this script is meant to isolate, conflating this question with the
multiprocess one (see docs/archived/STT_MULTIPROCESS_PLAN.md §3).

Dev-box pre-check (x86, 32 cores, 2026-08-11) DID show the effect clearly --
unset/=0 spread the decode across ~all 32 cores and ran slower (1.55s/1.34s
total latency); =1 stayed on 1 core and ran faster (0.94s). See
docs/archived/STT_MULTIPROCESS_PLAN.md §3.1 for the full numbers. That corrects the
assumption below this note used to make (that a 32-core box couldn't show a
4-core-relevant effect) -- it can, and did. Still run this on the RPi5
before trusting the *magnitude* for the deployed config: 4 ARM cores with
real memory-bandwidth limits and thermal throttling risk is a different
regime than 32 x86 cores with headroom to spare.

Feeds the real tx/rx call recording through the real pipeline once, same
feed-straight-onto-ingest_queue trick as scratch/demo_segment_cut_latency.py,
and reports per-segment + total STT latency. Run it three times, from three
separate shells, with the env var unset / =0 / =1, and diff the totals:

    EDGE_VOICE_STT__USE_PROCESSES=false python3 scratch/bench_ort_threads.py
    EDGE_VOICE_STT__USE_PROCESSES=false MOONSHINE_ORT_SINGLE_THREAD=0 python3 scratch/bench_ort_threads.py
    EDGE_VOICE_STT__USE_PROCESSES=false MOONSHINE_ORT_SINGLE_THREAD=1 python3 scratch/bench_ort_threads.py

Prefer `mpstat -P ALL 1` (sampled to a file, in the background, for the
run's duration) over watching `htop` live -- it gives a per-core busy-%
log you can diff afterward instead of a read that's gone once the run
ends. If only one core stays busy during a long segment's transcription,
that run is single-threaded, regardless of what the totals say. That's the
more reliable signal; the latency numbers alone can be noisy on a Pi
(thermal throttling, other processes).
"""

from __future__ import annotations

import json
import logging
import os
import time

import soundfile as sf
import torch
import torchaudio

from edge_voice.config.settings import Settings
from edge_voice.observability.logging import JsonFormatter, configure_logging
from edge_voice.pipeline.models import AudioPacket
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

TX_PATH = "wav/tx_recorded_1.wav"
RX_PATH = "wav/rx_recorded_1.wav"
DRAIN_TIMEOUT_S = 240.0


class _CaptureHandler(logging.Handler):
    def __init__(self, formatter: logging.Formatter) -> None:
        super().__init__()
        self.setFormatter(formatter)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


def _load(path: str, sample_rate: int):
    data, file_sr = sf.read(path, dtype="int16")
    if data.ndim > 1:
        data = data[:, 0]
    if file_sr != sample_rate:
        tensor = torch.as_tensor(data, dtype=torch.float32).unsqueeze(0)
        resampler = torchaudio.transforms.Resample(file_sr, sample_rate, lowpass_filter_width=64)
        data = resampler(tensor).squeeze(0).numpy().astype("int16")
    return data


def feed_both(orch: PipelineOrchestrator, incoming_ms: float, sample_rate: int) -> None:
    tx = _load(TX_PATH, sample_rate)
    rx = _load(RX_PATH, sample_rate)
    chunk_samples = round(incoming_ms * sample_rate / 1000.0)

    packets: list[tuple[float, AudioPacket]] = []
    for channel_id, data in (("tx", tx), ("rx", rx)):
        n_chunks = len(data) // chunk_samples
        for i in range(n_chunks):
            chunk = data[i * chunk_samples : (i + 1) * chunk_samples]
            t = i * incoming_ms / 1000.0
            packets.append(
                (t, AudioPacket(channel_id=channel_id, timestamp=t, samples=chunk.tobytes()))
            )
    packets.sort(key=lambda pair: pair[0])

    base_ts = time.time()
    for t, packet in packets:
        packet.timestamp = base_ts + t
        orch.ingest_queue.put(packet, timeout=5.0)
    print(
        f"...fed {len(packets)} packets (tx {len(tx) / sample_rate:.1f}s, rx {len(rx) / sample_rate:.1f}s)"
    )


def run_once() -> list[dict]:
    settings = Settings.load()
    settings.logging_.is_json = True
    settings.logging_.console_enabled = False
    settings.logging_.file_enabled = False
    settings.reliability.watchdog_enabled = False

    configure_logging(settings.logging_)
    capture = _CaptureHandler(JsonFormatter())
    root = logging.getLogger()
    root.addHandler(capture)

    orch = PipelineOrchestrator(settings)
    orch.build()
    orch.start()
    time.sleep(0.5)

    feed_both(
        orch, incoming_ms=settings.repacketizer.incoming_ms, sample_rate=settings.audio.sample_rate
    )

    deadline = time.time() + DRAIN_TIMEOUT_S
    last_count = -1
    stable_since: float | None = None
    settle_window = settings.vad.idle_flush_s + 1.0
    while time.time() < deadline:
        depths = orch.queue_depths()
        queues_empty = depths.get("routed", 0) == 0 and depths.get("segment", 0) == 0
        count = sum(1 for line in capture.lines if '"stt_latency_s"' in line)
        if queues_empty and count > 0 and count == last_count:
            stable_since = stable_since or time.time()
            if time.time() - stable_since >= settle_window:
                break
        else:
            stable_since = None
        last_count = count
        time.sleep(0.5)
    else:
        print(f"WARNING: settle timed out after {DRAIN_TIMEOUT_S}s")

    orch.stop()
    orch.wait()
    root.removeHandler(capture)

    results = []
    for line in capture.lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "stt_latency_s" in payload:
            results.append(payload)
    return results


def main() -> None:
    env_val = os.environ.get("MOONSHINE_ORT_SINGLE_THREAD")
    print(f"MOONSHINE_ORT_SINGLE_THREAD={env_val!r}")
    print(
        "Watch `htop` (press 1 for per-core view) in another session now -- "
        "the interesting window is the ~5s segment partway through.\n"
    )

    results = run_once()

    print(f"\n{'channel':8}{'segment':22}{'dur_s':>8}{'latency_s':>11}  text")
    total_audio = 0.0
    total_latency = 0.0
    for r in sorted(results, key=lambda r: r["message"]):
        msg = r["message"]
        parts = msg.split(" ", 4)
        channel = parts[1].split("=")[1]
        segment = parts[2].split("=")[1]
        window = parts[3].strip("[]")
        start_s, end_s = (float(x) for x in window.split("-"))
        dur = end_s - start_s
        latency = r["stt_latency_s"]
        text = msg.split("'", 1)[1].rstrip("'") if "'" in msg else ""
        total_audio += dur
        total_latency += latency
        print(f"{channel:8}{segment:22}{dur:8.2f}{latency:11.3f}  {text}")

    print(
        f"\nMOONSHINE_ORT_SINGLE_THREAD={env_val!r}  "
        f"segments={len(results)} total_audio_s={total_audio:.2f} "
        f"total_latency_s={total_latency:.2f} "
        f"max_latency_s={max(r['stt_latency_s'] for r in results):.3f}"
    )


if __name__ == "__main__":
    main()
