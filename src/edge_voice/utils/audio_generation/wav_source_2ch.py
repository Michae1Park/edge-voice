"""WAV file audio source that publishes raw PCM packets to MQTT for two separate channels.

Reads two WAV files (rx and tx), converts to target sample rate, splits into chunks,
and publishes each frame as raw bytes to the configured MQTT topics:

<raw_bytes>

Run as a separate process:
    python utils/wav_source_2ch.py
"""

from __future__ import annotations

import argparse
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
        description="Publish two WAV files to MQTT for the edge-voice pipeline"
    )
    parser.add_argument("--rx_wav", default="wav/rx_recorded_1.wav", help="Path to RX WAV file")
    parser.add_argument("--tx_wav", default="wav/tx_recorded_1.wav", help="Path to TX WAV file")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--sr", type=int, default=16_000, help="Target sample rate")
    parser.add_argument("--chunk", type=int, default=320, help="Chunk size in samples")
    args = parser.parse_args(argv)

    rx_topic = "stt/audio_chunks_rx"
    tx_topic = "stt/audio_chunks_tx"

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.connect(args.broker, args.port)
    client.loop_start()

    logger.info("WavSource2Ch: connecting to %s:%d", args.broker, args.port)
    while not client.is_connected():
        time.sleep(0.05)

    file_sr_rx, rx_data = _open_wav(args.rx_wav)
    file_sr_tx, tx_data = _open_wav(args.tx_wav)

    if file_sr_rx != args.sr:
        logger.info("WavSource2Ch: resampling RX %d → %d Hz", file_sr_rx, args.sr)
        rx_data = _resample(rx_data, file_sr_rx, args.sr)
    if file_sr_tx != args.sr:
        logger.info("WavSource2Ch: resampling TX %d → %d Hz", file_sr_tx, args.sr)
        tx_data = _resample(tx_data, file_sr_tx, args.sr)

    rx_chunks = math.ceil(len(rx_data) / args.chunk)
    tx_chunks = math.ceil(len(tx_data) / args.chunk)
    total_frames = max(rx_chunks, tx_chunks)

    logger.info(
        "WavSource2Ch: %d frames × 2 channels → %s (QoS=1)",
        total_frames,
        args.broker,
    )

    start = time.time()
    frame_duration_s = args.chunk / args.sr
    rx_idx = 0
    tx_idx = 0

    for frame_i in range(total_frames):
        # RX channel
        if rx_idx < len(rx_data):
            rx_chunk = rx_data[rx_idx : rx_idx + args.chunk]
            if len(rx_chunk) < args.chunk:
                rx_chunk = np.pad(rx_chunk, (0, args.chunk - len(rx_chunk)), "constant")
            client.publish(rx_topic, rx_chunk.tobytes(), qos=1)
            rx_idx += args.chunk

        # TX channel
        if tx_idx < len(tx_data):
            tx_chunk = tx_data[tx_idx : tx_idx + args.chunk]
            if len(tx_chunk) < args.chunk:
                tx_chunk = np.pad(tx_chunk, (0, args.chunk - len(tx_chunk)), "constant")
            client.publish(tx_topic, tx_chunk.tobytes(), qos=1)
            tx_idx += args.chunk

        # Real-time pacing
        elapsed = time.time() - start
        expected = frame_i * frame_duration_s
        time.sleep(max(0, expected - elapsed))

    client.loop_stop()
    elapsed_total = time.time() - start
    logger.info(
        "WavSource2Ch: done — %d frames in %.2fs (%.1fx real-time)",
        total_frames,
        elapsed_total,
        (total_frames * frame_duration_s) / elapsed_total if elapsed_total > 0 else 0,
    )


if __name__ == "__main__":
    main()
