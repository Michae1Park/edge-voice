"""WAV file audio source that publishes PCM frames to MQTT for the pipeline.

Reads a WAV file, converts to target sample rate, splits into 20ms chunks,
and publishes each frame as a JSON envelope to the configured MQTT topics:

{"samples_b64": "<base64>", "timestamp": <float>}

Run as a separate process:
    python -m edge_voice.utils.audio_generation.wav_source --wav file.wav --channels rx tx
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import math
import time
from collections.abc import Sequence

import numpy as np
import paho.mqtt.client as mqtt  # type: ignore[import-untyped]
import soundfile as sf
import torch
import torchaudio

logger = logging.getLogger(__name__)


def _open_wav(path: str) -> tuple[int, np.ndarray]:
    """Open WAV and return (sample_rate, data as int16 numpy array)."""
    data, sr = sf.read(path, dtype="int16")
    if data.ndim == 2:
        data = (data[:, 0].astype(np.int32) + data[:, 1].astype(np.int32)) // 2
        data = data.astype(np.int16)
    return sr, data


def _resample(data: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
    if src_sr == dst_sr:
        return data
    tensor = torch.as_tensor(data, dtype=torch.float32).unsqueeze(0)
    resampler = torchaudio.transforms.Resample(src_sr, dst_sr, lowpass_filter_width=64)
    return resampler(tensor).squeeze(0).numpy().astype(np.int16)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Publish a WAV file to MQTT for the edge-voice pipeline"
    )
    parser.add_argument("--wav", required=True, help="Path to WAV file")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument(
        "--channels",
        nargs="+",
        default=["rx", "tx"],
        help="Channels to publish to (e.g. rx tx)",
    )
    parser.add_argument("--sr", type=int, default=16_000, help="Target sample rate")
    parser.add_argument("--chunk", type=int, default=320, help="Chunk size in samples")
    args = parser.parse_args(argv)

    topic_map = {ch: f"stt/audio_chunks_{ch}" for ch in args.channels}

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    connected = False

    def on_connect(_client, _userdata, flags, rc, props):
        nonlocal connected
        connected = True

    client.on_connect = on_connect  # type: ignore[assignment]
    client.connect(args.broker, args.port)
    client.loop_start()

    while not connected:
        time.sleep(0.05)

    file_sr, raw_data = _open_wav(args.wav)
    if file_sr != args.sr:
        logger.info("WavSource: resampling %d → %d Hz", file_sr, args.sr)
        raw_data = _resample(raw_data, file_sr, args.sr)

    total_chunks = math.ceil(len(raw_data) / args.chunk)
    logger.info(
        "WavSource: %s %d Hz %d samples → %d frames × %d channel(s) → %s",
        args.wav,
        file_sr,
        len(raw_data),
        total_chunks,
        len(args.channels),
        args.broker,
    )

    start = time.time()
    frame_num = 0
    frame_duration_s = args.chunk / args.sr

    for i in range(0, len(raw_data), args.chunk):
        chunk = raw_data[i : i + args.chunk]
        if len(chunk) < args.chunk:
            chunk = np.pad(chunk, (0, args.chunk - len(chunk)), "constant")

        for ch in args.channels:
            topic = topic_map[ch]
            payload = {
                "samples_b64": base64.b64encode(chunk.tobytes()).decode(),
                "timestamp": time.time(),
            }
            client.publish(topic, json.dumps(payload).encode(), qos=1)
            frame_num += 1

        # Real-time pacing
        elapsed = time.time() - start
        expected = (frame_num / len(args.channels)) * frame_duration_s
        time.sleep(max(0, expected - elapsed))

    client.loop_stop()
    elapsed_total = time.time() - start
    logger.info(
        "WavSource: done — %d frames in %.2fs (%.1fx real-time)",
        frame_num / len(args.channels),
        elapsed_total,
        (frame_num / len(args.channels) * frame_duration_s) / elapsed_total,
    )


if __name__ == "__main__":
    main()
