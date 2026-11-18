# On-board check of the NPU STT backend through the production worker path.
# Usage: python npu_worker_check.py DATA_DIR MODEL_DIR RUNTIME_LIB [CHANNELS]

from __future__ import annotations

import json
import logging
import re
import statistics as st
import sys
import threading
import time
from pathlib import Path

import numpy as np
import soundfile as sf

from edge_voice.config.settings import MQTTChannels, Settings
from edge_voice.pipeline.models import SpeechSegment
from edge_voice.pipeline.orchestrator import PipelineOrchestrator
from edge_voice.stt.stt_process import STTProcessHandle, new_mp_context

MAX_CLIP_S = 5.0
READY_TIMEOUT_S = 180.0
RESULT_TIMEOUT_S = 60.0


def norm(text: str) -> str:
    return re.sub(r"[\s\W_]+", "", text)


def lev(a: str, b: str) -> int:
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def load_clips(data: Path) -> list[tuple[str, bytes, str]]:
    clips = []
    for meta in ("tts", "recorded", "conv"):
        for row in json.loads((data / f"{meta}_metadata.json").read_text()):
            if row["duration_seconds"] <= MAX_CLIP_S:
                audio, _ = sf.read(data / row["filename"], dtype="int16")
                clips.append(
                    (row["filename"], audio.astype(np.int16).tobytes(), row["transcription"])
                )
    return clips


def drain_logs(log_queue, lines: list[str]) -> None:
    while True:
        record = log_queue.get()
        if record is None:
            return
        lines.append(f"{record.levelname} {record.getMessage()}")


def run(settings: Settings, clips, label: str) -> dict:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._settings = settings
    ctx = new_mp_context()
    log_queue = ctx.Queue()
    logs: list[str] = []
    drainer = threading.Thread(target=drain_logs, args=(log_queue, logs), daemon=True)
    drainer.start()

    channel_ids = [c.channel_id for c in settings.mqtt.channels]
    handles = {
        cid: STTProcessHandle(
            cid, ctx.Queue(), orchestrator._stt_config(cid), ctx, log_queue, logging.INFO
        )
        for cid in channel_ids
    }
    t0 = time.monotonic()
    for h in handles.values():
        h.start()
    ready = all(h.wait_ready(READY_TIMEOUT_S) for h in handles.values())
    startup_s = time.monotonic() - t0

    results: dict[str, list] = {cid: [] for cid in channel_ids}
    if ready:
        for i, (name, audio, _) in enumerate(clips):
            for cid, h in handles.items():
                h.segment_queue.put(
                    SpeechSegment(cid, 0.0, len(audio) / 32000, audio, f"{cid}-{i}-{name}")
                )
            for cid, h in handles.items():
                res = h.transcript_queue.get(timeout=RESULT_TIMEOUT_S)
                results[cid].append((res.event.text, res.stt_latency_s))

    for h in handles.values():
        h.stop()
        h.segment_queue.put(None)
        h.join(timeout=10)
    log_queue.put(None)
    drainer.join(timeout=5)

    summary = {"label": label, "ready": ready, "startup_s": round(startup_s, 1), "channels": {}}
    for cid, rows in results.items():
        if not rows:
            continue
        edits = sum(lev(norm(ref), norm(text)) for (_, _, ref), (text, _) in zip(clips, rows))
        chars = sum(len(norm(ref)) for _, _, ref in clips)
        lats = sorted(lat for _, lat in rows)
        summary["channels"][cid] = {
            "segments": len(rows),
            "cer": round(edits / chars, 4),
            "p50_ms": round(1000 * st.median(lats), 1),
            "p95_ms": round(1000 * lats[int(0.95 * (len(lats) - 1))], 1),
        }
    summary["stt_logs"] = [line for line in logs if "runtime=" in line or "NPU" in line]
    return summary


def main(argv: list[str]) -> None:
    data, model_dir, runtime_lib = Path(argv[1]), argv[2], argv[3]
    n_channels = int(argv[4]) if len(argv) > 4 else 3
    clips = load_clips(data)
    channels = [MQTTChannels(topic=f"t{i}", channel_id=f"ch{i}") for i in range(n_channels)]
    sensevoice = {"npu_model_dir": model_dir, "npu_runtime_lib": runtime_lib}

    for label, stt in (
        ("sensevoice_npu", {"runtime": "npu"}),
        ("sensevoice_auto", {"runtime": "auto"}),
        (
            "sensevoice_bad_npu_model_falls_back_to_cpu",
            {"runtime": "npu", "sensevoice": {"npu_model_dir": "/nonexistent"}},
        ),
        ("sensevoice_cpu", {"runtime": "cpu"}),
        ("moonshine_cpu", {"model": "moonshine", "runtime": "cpu"}),
    ):
        stt.setdefault("sensevoice", {})
        stt["sensevoice"] = {**sensevoice, **stt["sensevoice"]}
        settings = Settings(mqtt={"channels": channels}, stt=stt)
        print(json.dumps(run(settings, clips, label), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main(sys.argv)
