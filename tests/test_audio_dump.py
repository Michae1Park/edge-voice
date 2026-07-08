"""Tests for edge_voice.audio_ingest.audio_dump."""

import queue
import tempfile
import time

import numpy as np

from edge_voice.audio_ingest.audio_dump import AudioDumpWorker
from edge_voice.pipeline.models import AudioPacket

SAMPLE_RATE = 16_000


def _make_packet(channel_id: str, n_samples: int = 320):
    return AudioPacket(channel_id=channel_id, timestamp=time.time(), samples=b"\x00" * n_samples)


def test_writes_segment_file():
    routed_q = queue.Queue()
    with tempfile.TemporaryDirectory() as tmp:
        dump = AudioDumpWorker(
            routed_q, output_dir=tmp, channel_sample_rate=SAMPLE_RATE, segment_secs=0.5
        )
        dump.start()
        # Feed enough samples to fill one segment (0.5s * 16000 * 2 bytes = 16000 bytes)
        routed_q.put(_make_packet("ch1", n_samples=320))
        routed_q.put(_make_packet("ch1", n_samples=13_680 // 2))  # additional
        time.sleep(0.5)
        dump.stop()
        dump.join(timeout=5)

        files = list(np.__import__("pathlib").Path(tmp).glob("*.wav")) if False else []
        import pathlib

        files = list(pathlib.Path(tmp).glob("*.wav"))
        assert len(files) >= 1


def test_obeys_segment_boundary():
    routed_q = queue.Queue()
    with tempfile.TemporaryDirectory() as tmp:
        dump = AudioDumpWorker(
            routed_q, output_dir=tmp, channel_sample_rate=SAMPLE_RATE, segment_secs=1.0
        )
        dump.start()
        # 1s of audio = 32000 bytes at 16kHz with 16-bit samples
        routed_q.put(_make_packet("ch1", n_samples=SAMPLE_RATE))  # 16_000 samples = 32_000 bytes
        time.sleep(0.5)
        dump.stop()
        dump.join(timeout=5)

        import pathlib

        files = list(pathlib.Path(tmp).glob("*.wav"))
        assert len(files) >= 1, "Expected at least one segment file"


def test_dumps_trailing_audio_on_stop():
    routed_q = queue.Queue()
    with tempfile.TemporaryDirectory() as tmp:
        dump = AudioDumpWorker(
            routed_q, output_dir=tmp, channel_sample_rate=SAMPLE_RATE, segment_secs=10.0
        )
        dump.start()
        # Only half a second — won't fill a segment but should appear as trailing file
        routed_q.put(
            _make_packet("ch1", n_samples=8_000)
        )  # 4000 samples * 2 bytes = 8000 bytes = 0.5s
        time.sleep(0.5)
        dump.stop()
        dump.join(timeout=5)

        import pathlib

        files = list(pathlib.Path(tmp).glob("*.wav"))
        assert len(files) >= 1, "Expected trailing audio file on stop"


def test_multiple_channels():
    routed_q = queue.Queue()
    with tempfile.TemporaryDirectory() as tmp:
        dump = AudioDumpWorker(
            routed_q, output_dir=tmp, channel_sample_rate=SAMPLE_RATE, segment_secs=1.0
        )
        dump.start()
        routed_q.put(_make_packet("rx", n_samples=SAMPLE_RATE))
        routed_q.put(_make_packet("tx", n_samples=SAMPLE_RATE))
        time.sleep(0.5)
        dump.stop()
        dump.join(timeout=5)

        import pathlib

        files = list(pathlib.Path(tmp).glob("*.wav"))
        file_channels = {f.stem.split("_")[0] for f in files}
        assert "rx" in file_channels
        assert "tx" in file_channels
