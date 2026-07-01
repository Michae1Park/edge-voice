"""Tests for wav_source.py."""

import queue
import struct
import time
from pathlib import Path
from typing import Any

import numpy as np

from edge_voice.utils.audio_generation.wav_source import WavSource

_TEST_CHUNK_SAMPLES = 320
_TEST_SAMPLE_RATE = 16_000


def _make_wav(
    path: Path,
    duration_s: float = 1.0,
    sample_rate: int = _TEST_SAMPLE_RATE,
    channels: int = 1,
    samples: np.ndarray | None = None,
) -> np.ndarray:
    """Create a minimal valid WAV file on disk and return the raw int16 data."""
    if samples is None:
        samples = np.random.randint(
            -32768, 32767, size=int(duration_s * sample_rate), dtype=np.int16
        )

    bits_per_sample = 16
    block_align = channels * bits_per_sample // 8
    byte_rate = sample_rate * channels * bits_per_sample // 8
    num_frames = len(samples)
    data_size = num_frames * block_align

    with open(path, "wb") as f:
        f.write(b"RIFF")
        f.write(struct.pack("<I", 36 + data_size))
        f.write(b"WAVE")
        f.write(b"fmt ")
        f.write(struct.pack("<I", 16))
        f.write(struct.pack("<H", 1))  # PCM
        f.write(struct.pack("<H", channels))
        f.write(struct.pack("<I", sample_rate))
        f.write(struct.pack("<I", byte_rate))
        f.write(struct.pack("<H", block_align))
        f.write(struct.pack("<H", bits_per_sample))
        f.write(b"data")
        f.write(struct.pack("<I", data_size))
        f.write(samples.tobytes())

    return samples


# ------
# WavSource
# ------


class TestWavSource:
    def test_wavsource_pushes_packets(self, tmp_path: Path):
        data = np.arange(640, dtype=np.int16)
        wav_path = tmp_path / "test.wav"
        _make_wav(wav_path, samples=data)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))
        source.run()

        packets_collected = []
        while not q.empty():
            packets_collected.append(q.get())

        assert len(packets_collected) == 2  # 640 samples / 320 per chunk = 2
        assert all(p.channel_id == "ch1" for p in packets_collected)

    def test_wavsource_multiple_channels(self, tmp_path: Path):
        data = np.arange(_TEST_CHUNK_SAMPLES, dtype=np.int16)
        wav_path = tmp_path / "test.wav"
        _make_wav(wav_path, samples=data)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1", "ch2"], str(wav_path))
        source.run()

        packets_collected = []
        while not q.empty():
            packets_collected.append(q.get())

        assert len(packets_collected) == 2  # 1 chunk * 2 channels
        channel_ids = [p.channel_id for p in packets_collected]
        assert "ch1" in channel_ids
        assert "ch2" in channel_ids

    def test_wavsource_stop(self, tmp_path: Path):
        # Create a 60-second WAV file -- will only play a few packets before stopping
        data = np.arange(_TEST_SAMPLE_RATE * 60, dtype=np.int16)
        wav_path = tmp_path / "long.wav"
        _make_wav(wav_path, samples=data)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))
        source.start()

        # Let it play briefly
        time.sleep(0.1)
        source.stop()
        source.join(timeout=5)

        assert not source.is_alive()

    def test_wavsource_small_patch_last_chunk(self, tmp_path: Path):
        # Non-multiple of _TEST_CHUNK_SAMPLES exercises the padding path
        data = np.arange(100, dtype=np.int16)
        wav_path = tmp_path / "short.wav"
        _make_wav(wav_path, samples=data)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))
        source.run()

        packets_collected = []
        while not q.empty():
            packets_collected.append(q.get())

        assert len(packets_collected) == 1
        # Padded chunk should be CHUNK_SAMPLES * 2 bytes (160 bytes)
        assert len(packets_collected[0].samples) == _TEST_CHUNK_SAMPLES * 2

    def test_wavsource_queue_full_drops(self, tmp_path: Path):
        small_q: queue.Queue[Any] = queue.Queue(maxsize=1)
        data = np.arange(_TEST_SAMPLE_RATE, dtype=np.int16)  # 1 second of audio
        wav_path = tmp_path / "long.wav"
        _make_wav(wav_path, samples=data)

        source = WavSource(small_q, ["ch1"], str(wav_path))
        source.run()

        assert small_q.qsize() == 1

    def test_wavsource_is_alive_during_run(self, tmp_path: Path):
        data = np.arange(_TEST_SAMPLE_RATE, dtype=np.int16)
        wav_path = tmp_path / "test.wav"
        _make_wav(wav_path, samples=data)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))

        assert not source._stopped.is_set()
        source.start()
        time.sleep(0.05)
        assert not source._stopped.is_set()
        source.stop()
        source.join(timeout=5)

    def test_wavsource_mono_from_stereo(self, tmp_path: Path):
        # Create stereo data where left and right channels differ
        left = np.array([1000, 2000, 3000, 4000], dtype=np.int16)
        right = np.array([500, 1500, 2500, 3500], dtype=np.int16)
        stereo = np.empty(8, dtype=np.int16)
        stereo[0::2] = left
        stereo[1::2] = right
        wav_path = tmp_path / "stereo.wav"
        _make_wav(wav_path, samples=stereo, channels=2)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))
        source.run()

        packet = q.get()
        expected_mono = ((left.astype(np.int32) + right.astype(np.int32)) // 2).astype(np.int16)
        received = np.frombuffer(packet.samples, dtype=np.int16)
        np.testing.assert_array_equal(received[:4], expected_mono)

    def test_wavsource_resampling(self, tmp_path: Path):
        # Create a 48kHz WAV file -- should be resampled to 16kHz
        data = np.arange(48000, dtype=np.int16)
        wav_path = tmp_path / "48k.wav"
        _make_wav(wav_path, samples=data, sample_rate=48000)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))
        source.run()

        packets_collected = []
        while not q.empty():
            packets_collected.append(q.get())

        # 48000 source samples -> resampled to 16000 samples -> 16000/320 = 50 packets
        assert len(packets_collected) == 50

    def test_wavsource_thread_name(self, tmp_path: Path):
        data = np.arange(_TEST_CHUNK_SAMPLES, dtype=np.int16)
        wav_path = tmp_path / "test.wav"
        _make_wav(wav_path, samples=data)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))
        assert source.name == "WavSource"
        assert source.daemon is True

    def test_wavsource_uses_custom_configs(self, tmp_path: Path):
        """Verify that overriding sample_rate and chunk_samples works."""
        # 480 samples with custom sample_rate of 32kHz, 640 chunk size
        data = np.arange(480, dtype=np.int16)
        wav_path = tmp_path / "test.wav"
        _make_wav(wav_path, samples=data, sample_rate=32000)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(
            q,
            ["ch1"],
            str(wav_path),
            sample_rate=16_000,
            chunk_samples=320,
        )
        source.run()

        packets_collected = []
        while not q.empty():
            packets_collected.append(q.get())

        # 480 -> resampled to 16_000: 180 frames -> 180/320 = 1 chunk (padded to 320)
        assert len(packets_collected) == 1
        assert len(packets_collected[0].samples) == 320 * 2  # bytes
