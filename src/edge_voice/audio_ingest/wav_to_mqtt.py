"""WAV file audio source that reads a file and pushes 20ms packets.

This is a library class that pushes AudioPacket objects onto a queue.Queue.
The MQTT boundary is in MqttAudioIngest which subscribes to the topics below.
"""

from __future__ import annotations

import base64
import json
import logging
import math
import queue
import threading
import time

import numpy as np
import soundfile as sf
import torch
import torchaudio

logger = logging.getLogger(__name__)

FRAME_SIZE = 320  # 20 ms @ 16 kHz mono int16


def open_wav(path: str) -> tuple[int, int, np.ndarray]:
    """Open WAV and return (sample_rate, num_channels, data as int16 numpy array)."""
    data, sr = sf.read(path, dtype="int16")
    nch = 1 if data.ndim == 1 else data.shape[1]
    return sr, nch, data


def resample(data: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
    if src_sr == dst_sr:
        return data
    tensor = torch.as_tensor(data, dtype=torch.float32).unsqueeze(0)
    resampler = torchaudio.transforms.Resample(src_sr, dst_sr, lowpass_filter_width=64)
    return resampler(tensor).squeeze(0).numpy().astype(np.int16)


class WavSource(threading.Thread):
    """Parses a .wav file and pushes 20ms audio packets onto a queue.

    Publishes to MQTT topics stt/audio_chunks_rx / stt/audio_chunks_tx
    (default) with corresponding channel_ids rx / tx.
    """

    def __init__(
        self,
        ingest_queue: queue.Queue,
        channels: list[str],
        wav_path: str,
        sample_rate: int = 16_000,
        chunk_samples: int = FRAME_SIZE,
        mqtt_broker: str = "localhost",
        mqtt_port: int = 1883,
    ) -> None:
        super().__init__(name="WavSource", daemon=False)
        self._ingest_queue = ingest_queue
        self._channels = channels
        self._wav_path = wav_path
        self._sample_rate = sample_rate
        self._chunk_samples = chunk_samples
        self._mqtt_broker = mqtt_broker
        self._mqtt_port = mqtt_port
        self._stopped = threading.Event()

    @property
    def topic_map(self) -> dict[str, str]:
        """Map channel_id → MQTT topic."""
        return {c: f"stt/audio_chunks_{c}" for c in self._channels}

    def run(self) -> None:
        """Read WAV and publish PCM frames to MQTT topics."""
        file_sr, nch, raw_data = open_wav(self._wav_path)
        logger.info(
            "WavSource: %s %d Hz %d samples → %d channel(s) → %s:%d",
            self._wav_path,
            file_sr,
            len(raw_data),
            len(self._channels),
            self._mqtt_broker,
            self._mqtt_port,
        )

        if file_sr != self._sample_rate:
            logger.info("WavSource: resampling %d → %d Hz", file_sr, self._sample_rate)
            raw_data = resample(raw_data, file_sr, self._sample_rate)

        if nch == 2:
            raw_data = (raw_data[:, 0].astype(np.int32) + raw_data[:, 1].astype(np.int32)) // 2
            raw_data = raw_data.astype(np.int16)

        # Use MQTT to publish to pipeline
        total_chunks = math.ceil(len(raw_data) / self._chunk_samples)
        start_time = time.time()
        frame_num = 0
        frame_duration_s = self._chunk_samples / self._sample_rate

        # --- MQTT publish loop ------
        try:
            import paho.mqtt.client as mqtt  # type: ignore[import-untyped]

            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            )
            connected = threading.Event()

            def on_connect(_c, _u, flags, rc, props):
                connected.set()

            client.on_connect = on_connect  # type: ignore[assignment]
            client.connect(self._mqtt_broker, self._mqtt_port)
            client.loop_start()
            connected.wait(timeout=5.0)

            for i in range(0, len(raw_data), self._chunk_samples):
                if self._stopped.is_set():
                    break

                chunk = raw_data[i : i + self._chunk_samples]
                if len(chunk) < self._chunk_samples:
                    chunk = np.pad(chunk, (0, self._chunk_samples - len(chunk)), "constant")

                for ch in self._channels:
                    topic = f"stt/audio_chunks_{ch}"
                    payload = {
                        "samples_b64": base64.b64encode(chunk.tobytes()).decode(),
                        "timestamp": time.time(),
                    }
                    client.publish(topic, json.dumps(payload).encode(), qos=1)
                    frame_num += 1

                elapsed = time.time() - start_time
                expected = (frame_num / len(self._channels)) * frame_duration_s
                sleep_time = max(0, expected - elapsed)
                time.sleep(sleep_time)

            client.loop_stop()
            logger.info(
                "WavSource: done — %d frames in %.2fs (%.1fx real-time)",
                frame_num // len(self._channels),
                time.time() - start_time,
                (frame_num / len(self._channels) * frame_duration_s)
                / max(0.001, time.time() - start_time),
            )
        except Exception:  # noqa: BLE001
            logger.exception("WavSource MQTT publish failed, pushing to ingest_queue instead")
            self._fallback_push(raw_data, total_chunks, start_time)

    def _fallback_push(self, raw_data: np.ndarray, total_chunks: int, start_time: float) -> None:
        """Fallback: push directly to ingest_queue if MQTT fails."""
        frame_duration_s = self._chunk_samples / self._sample_rate
        frame_num = 0

        for i in range(0, len(raw_data), self._chunk_samples):
            if self._stopped.is_set():
                break

            chunk = raw_data[i : i + self._chunk_samples]
            if len(chunk) < self._chunk_samples:
                chunk = np.pad(chunk, (0, self._chunk_samples - len(chunk)), "constant")

            channel = self._channels[0]  # single channel fallback
            packet = type(
                "AudioPacket",
                (),
                {
                    "channel_id": channel,
                    "timestamp": time.time(),
                    "samples": chunk.tobytes(),
                    "topic": f"stt/audio_chunks_{channel}",
                },
            )()

            try:
                self._ingest_queue.put(packet, timeout=0.01)
                frame_num += 1
            except queue.Full:
                logger.warning("ingest_queue full; dropping packet for %s", channel)

            elapsed = time.time() - start_time
            expected = frame_num * frame_duration_s
            delay = expected - elapsed
            if delay > 0:
                time.sleep(delay)

        logger.info(
            "WavSource: played %d packets (%.2fs duration)",
            frame_num,
            total_chunks * 0.02,
        )

    def stop(self) -> None:
        """Signal the worker to stop after the current packet."""
        self._stopped.set()

    def is_alive(self) -> bool:
        """Return True while the worker is not stopped."""
        return not self._stopped.is_set()
