"""Tests for wav_source.py."""

import queue
import struct
import time
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from edge_voice.utils.audio_generation.wav_source import (
    CHUNK_SAMPLES,
    TARGET_SAMPLE_RATE,
    WavSource,
    open_wav,
    resample,
    _create_packet,
)


def _make_wav(
    path: Path,
    duration_s: float = 1.0,
    sample_rate: int = TARGET_SAMPLE_RATE,
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


# --------------------------------------------------------------------------- #
# open_wav
# --------------------------------------------------------------------------- #


class TestOpenWav:
    def test_open_mono_16k(self, tmp_path: Path):
        data = np.arange(320, dtype=np.int16)
        wav_path = tmp_path / "mono.wav"
        _make_wav(wav_path, samples=data)

        sr, ch, ba, frames, out = open_wav(str(wav_path))
        assert sr == TARGET_SAMPLE_RATE
        assert ch == 1
        assert ba == 2
        assert frames == len(data) // 2
        np.testing.assert_array_equal(out, data)

    def test_open_stereo(self, tmp_path: Path):
        data = np.arange(640, dtype=np.int16)
        wav_path = tmp_path / "stereo.wav"
        _make_wav(wav_path, samples=data, channels=2)

        sr, ch, ba, frames, out = open_wav(str(wav_path))
        assert ch == 2

    def test_invalid_riff_header(self, tmp_path: Path):
        wav_path = tmp_path / "bad.wav"
        wav_path.write_bytes(b"NOTW\x00\x00\x00\x00")
        with pytest.raises(ValueError, match="Invalid RIFF header"):
            open_wav(str(wav_path))

    def test_invalid_wave_header(self, tmp_path: Path):
        wav_path = tmp_path / "bad.wav"
        wav_path.write_bytes(b"RIFF\x00\x00\x00\x00NOTW")
        with pytest.raises(ValueError, match="Invalid WAVE header"):
            open_wav(str(wav_path))

    def test_missing_data_chunk(self, tmp_path: Path):
        wav_path = tmp_path / "nodata.wav"
        with open(wav_path, "wb") as f:
            f.write(b"RIFF")
            f.write(struct.pack("<I", 36))
            f.write(b"WAVE")
            f.write(b"fmt ")
            f.write(struct.pack("<I", 16))
            f.write(struct.pack("<H", 1))  # PCM
            f.write(struct.pack("<H", 1))
            f.write(struct.pack("<I", 16000))
            f.write(struct.pack("<I", 32000))
            f.write(struct.pack("<H", 2))
            f.write(struct.pack("<H", 16))
            f.write(b"\x00" * 16)  # skip chunk
        with pytest.raises(ValueError, match="Reached end of file"):
            open_wav(str(wav_path))

    def test_unsupported_bit_depth(self, tmp_path: Path):
        wav_path = tmp_path / "24bit.wav"
        with open(wav_path, "wb") as f:
            f.write(b"RIFF")
            f.write(struct.pack("<I", 42))
            f.write(b"WAVE")
            f.write(b"fmt ")
            f.write(struct.pack("<I", 16))
            f.write(struct.pack("<H", 6))  # non-PCM format
            f.write(struct.pack("<H", 1))
            f.write(struct.pack("<I", 16000))
            f.write(struct.pack("<I", 32000))
            f.write(struct.pack("<H", 4))
            f.write(struct.pack("<H", 24))  # 24-bit
            f.write(b"data")
            f.write(struct.pack("<I", 0))
        with pytest.raises(ValueError, match="Unsupported WAV format"):
            open_wav(str(wav_path))


# --------------------------------------------------------------------------- #
# resample
# --------------------------------------------------------------------------- #


class TestResample:
    def test_no_resampling_needed(self):
        data = np.arange(100, dtype=np.int16)
        result = resample(data, 16000, 16000)
        np.testing.assert_array_equal(result, data)

    def test_upsample(self):
        data = np.array([0, 1000, 2000, 3000], dtype=np.int16)
        result = resample(data, 8000, 16000)
        assert result.dtype == np.int16
        assert len(result) == 8  # duration * 16000 = 4 * 16000 = 64, but with 4 samples...

    def test_downsample(self):
        data = np.arange(160, dtype=np.int16)
        result = resample(data, 32000, 8000)
        assert result.dtype == np.int16
        assert len(result) == 40  # 160/32000 * 8000 = 40

    def test_preserves_dtype(self):
        data = np.random.randint(-32768, 32767, 100, dtype=np.int16)
        result = resample(data, 8000, 48000)
        assert result.dtype == np.int16


# --------------------------------------------------------------------------- #
# _create_packet
# --------------------------------------------------------------------------- #


class TestCreatePacket:
    def test_creates_valid_packet(self):
        channel_id = "test-ch"
        samples = np.array([100, 200, 300], dtype=np.int16)
        ts = 12345.678

        packet = _create_packet(channel_id, samples, ts)

        assert packet.channel_id == channel_id
        assert packet.timestamp == ts
        assert packet.samples == samples.tobytes()

    def test_default_timestamp(self):
        channel_id = "test-ch"
        samples = np.array([100], dtype=np.int16)
        packet = _create_packet(channel_id, samples)

        assert isinstance(packet.timestamp, float)
        assert packet.timestamp > 0

    def test_empty_samples(self):
        samples = np.array([], dtype=np.int16)
        packet = _create_packet("ch", samples)
        assert packet.samples == b""


# --------------------------------------------------------------------------- #
# WavSource
# --------------------------------------------------------------------------- #


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
        data = np.arange(320, dtype=np.int16)
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
        # Create a 60-second WAV file — will only play a few packets before stopping
        data = np.arange(16000 * 60, dtype=np.int16)
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
        # Non-multiple of CHUNK_SAMPLES exercises the padding path
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
        # Padded chunk should be CHUNK_SAMPLES bytes worth (320 * 2 bytes per int16)
        assert len(packets_collected[0].samples) == CHUNK_SAMPLES * 2

    def test_wavsource_queue_full_drops(self, tmp_path: Path):
        small_q: queue.Queue[Any] = queue.Queue(maxsize=1)
        data = np.arange(16000, dtype=np.int16)  # 1 second of audio
        wav_path = tmp_path / "long.wav"
        _make_wav(wav_path, samples=data)

        source = WavSource(small_q, ["ch1"], str(wav_path))
        source.run()

        assert small_q.qsize() == 1

    def test_wavsource_is_alive_during_run(self, tmp_path: Path):
        data = np.arange(16000, dtype=np.int16)
        wav_path = tmp_path / "test.wav"
        _make_wav(wav_path, samples=data)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))

        # is_alive checks whether _stopped is not set, not thread status
        assert source.is_alive()  # stopped flag is not set yet
        source.start()
        time.sleep(0.05)
        assert source.is_alive()
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
        # Mono should be average of left and right (padded to CHUNK_SAMPLES)
        expected_mono = ((left.astype(np.int32) + right.astype(np.int32)) // 2).astype(np.int16)
        received = np.frombuffer(packet.samples, dtype=np.int16)
        np.testing.assert_array_equal(received[:4], expected_mono)

    def test_wavsource_resampling(self, tmp_path: Path):
        # Create a 48kHz WAV file — should be resampled to 16kHz
        data = np.arange(48000, dtype=np.int16)
        wav_path = tmp_path / "48k.wav"

        with open(wav_path, "wb") as f:
            f.write(b"RIFF")
            f.write(struct.pack("<I", 36 + len(data) * 2))
            f.write(b"WAVE")
            f.write(b"fmt ")
            f.write(struct.pack("<I", 16))
            f.write(struct.pack("<H", 1))  # PCM
            f.write(struct.pack("<H", 1))
            f.write(struct.pack("<I", 48000))
            f.write(struct.pack("<I", 96000))
            f.write(struct.pack("<H", 2))
            f.write(struct.pack("<H", 16))
            f.write(b"data")
            f.write(struct.pack("<I", len(data) * 2))
            f.write(data.tobytes())

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))
        source.run()

        packets_collected = []
        while not q.empty():
            packets_collected.append(q.get())

        # 48000 source samples -> resampled to 16000 samples -> 16000/320 = 50 packets
        assert len(packets_collected) == 50

    def test_wavsource_thread_name(self, tmp_path: Path):
        data = np.arange(CHUNK_SAMPLES, dtype=np.int16)
        wav_path = tmp_path / "test.wav"
        _make_wav(wav_path, samples=data)

        q: queue.Queue[Any] = queue.Queue()
        source = WavSource(q, ["ch1"], str(wav_path))
        assert source.name == "WavSource"
        assert source.daemon is True
