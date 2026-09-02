"""WAV file audio source that publishes PCM frames to MQTT for the pipeline.

Reads one or more WAV files, converts to target sample rate, splits into
20ms chunks, and publishes each frame as raw PCM bytes (no envelope) to the
configured MQTT topics.

Run as a separate process:
    # One stereo/mono file, split across channels
    python -m edge_voice.utils.audio_generation.wav_source --wav call.wav --channels rx tx

    # One file per channel
    python -m edge_voice.utils.audio_generation.wav_source --wav rx.wav tx.wav --channels rx tx
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
    """Open WAV and return (sample_rate, data as int16 numpy array).

    Data is 1-D (samples,) for mono files, or 2-D (samples, channels) for
    multi-channel files — channels are kept separate, never downmixed.
    """
    data, sr = sf.read(path, dtype="int16")
    return sr, data


def _resample_1d(data: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
    if src_sr == dst_sr:
        return data
    tensor = torch.as_tensor(data, dtype=torch.float32).unsqueeze(0)
    resampler = torchaudio.transforms.Resample(src_sr, dst_sr, lowpass_filter_width=64)
    return resampler(tensor).squeeze(0).numpy().astype(np.int16)


def _resample(data: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
    """Resample mono (samples,) or multi-channel (samples, channels) int16 data."""
    if src_sr == dst_sr:
        return data
    if data.ndim == 1:
        return _resample_1d(data, src_sr, dst_sr)
    return np.stack(
        [_resample_1d(data[:, c], src_sr, dst_sr) for c in range(data.shape[1])],
        axis=1,
    )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Publish a WAV file to MQTT for the edge-voice pipeline"
    )
    parser.add_argument(
        "--wav",
        nargs="+",
        required=True,
        help=(
            "WAV path(s). Either one file (mono, duplicated to all channels; "
            "or multi-channel, split in file order across --channels), or "
            "exactly one file per --channels entry, in the same order."
        ),
    )
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

    if len(args.wav) == 1:
        file_sr, raw_data = _open_wav(args.wav[0])
        if file_sr != args.sr:
            logger.info("WavSource: resampling %d → %d Hz", file_sr, args.sr)
            raw_data = _resample(raw_data, file_sr, args.sr)

        n_wav_channels = 1 if raw_data.ndim == 1 else raw_data.shape[1]
        n_out_channels = len(args.channels)

        if n_wav_channels == n_out_channels:
            # One WAV channel per MQTT channel, in file order (e.g. left→rx, right→tx).
            channel_data = {
                ch: (raw_data if raw_data.ndim == 1 else raw_data[:, i])
                for i, ch in enumerate(args.channels)
            }
        elif n_wav_channels == 1:
            # Mono file: duplicate the same audio to every MQTT channel.
            channel_data = {ch: raw_data for ch in args.channels}
        else:
            raise ValueError(
                f"WAV has {n_wav_channels} channel(s) but {n_out_channels} MQTT "
                f"channels were requested ({args.channels}) — counts must match, "
                "or the WAV must be mono."
            )
    elif len(args.wav) == len(args.channels):
        # One separate WAV file per channel, e.g. --wav rx.wav tx.wav --channels rx tx
        channel_data = {}
        for ch, path in zip(args.channels, args.wav):
            file_sr, data = _open_wav(path)
            if data.ndim != 1:
                raise ValueError(
                    f"{path} has {data.shape[1]} channels — per-channel WAV files must be mono."
                )
            if file_sr != args.sr:
                logger.info("WavSource: resampling %s %d → %d Hz", path, file_sr, args.sr)
                data = _resample(data, file_sr, args.sr)
            channel_data[ch] = data

        # Files may differ slightly in length; pad the shorter ones with silence.
        max_len = max(len(d) for d in channel_data.values())
        for ch, data in channel_data.items():
            if len(data) < max_len:
                channel_data[ch] = np.pad(data, (0, max_len - len(data)), "constant")
    else:
        raise ValueError(
            f"Got {len(args.wav)} --wav path(s) but {len(args.channels)} "
            f"--channels ({args.channels}) — pass either one WAV file total, "
            "or exactly one WAV file per channel."
        )

    n_samples = len(next(iter(channel_data.values())))
    total_chunks = math.ceil(n_samples / args.chunk)
    logger.info(
        "WavSource: %s %d samples → %d frames → %s → %s",
        args.wav,
        n_samples,
        total_chunks,
        {ch: topic_map[ch] for ch in args.channels},
        args.broker,
    )

    start = time.time()
    frame_num = 0
    frame_duration_s = args.chunk / args.sr

    for i in range(0, n_samples, args.chunk):
        for ch in args.channels:
            chunk = channel_data[ch][i : i + args.chunk]
            if len(chunk) < args.chunk:
                chunk = np.pad(chunk, (0, args.chunk - len(chunk)), "constant")
            client.publish(topic_map[ch], chunk.tobytes(), qos=1)

        frame_num += 1

        # Real-time pacing
        elapsed = time.time() - start
        expected = frame_num * frame_duration_s
        time.sleep(max(0, expected - elapsed))

    client.loop_stop()
    elapsed_total = time.time() - start
    logger.info(
        "WavSource: done — %d frames in %.2fs (%.1fx real-time)",
        frame_num,
        elapsed_total,
        (frame_num * frame_duration_s) / elapsed_total,
    )


if __name__ == "__main__":
    main()
