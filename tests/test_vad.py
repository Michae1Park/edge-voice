"""Tests for FakeVADWorker."""

import queue
import time

from edge_voice.pipeline.fake_workers import FakeVADWorker
from edge_voice.pipeline.models import AudioPacket, SpeechSegment


# -- helpers ----


def _make_packet(channel_id: str, ts: float, n_samples: int = 320) -> AudioPacket:
    return AudioPacket(channel_id=channel_id, timestamp=ts, samples=b"\x00" * n_samples)


def _wait_get(q: queue.Queue, timeout: float = 2.0) -> object:
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return None


# -- FakeVADWorker -----


def test_fake_vad_emits_segment_after_threshold():
    routed_q = queue.Queue()
    segment_q = queue.Queue()
    vad = FakeVADWorker(routed_q, segment_q)
    vad.start()

    for i in range(10):
        routed_q.put(_make_packet("rx", ts=float(i) * 0.02))

    segment = _wait_get(segment_q)
    assert isinstance(segment, SpeechSegment)
    assert segment.channel_id == "rx"
    assert len(segment.audio) > 0

    vad.stop()
    vad.join(timeout=3)


def test_fake_vad_buffer_resets_on_segment():
    routed_q = queue.Queue()
    segment_q = queue.Queue()
    vad = FakeVADWorker(routed_q, segment_q)
    vad.start()

    for i in range(10):
        routed_q.put(_make_packet("rx", ts=float(i)))
    seg1 = _wait_get(segment_q)
    assert seg1 is not None

    for i in range(10):
        routed_q.put(_make_packet("rx", ts=float(i) + 100))
    seg2 = _wait_get(segment_q)
    assert seg2 is not None

    assert seg1.segment_id != seg2.segment_id

    vad.stop()
    vad.join(timeout=3)


def test_fake_vad_handles_multiple_channels():
    routed_q = queue.Queue()
    segment_q = queue.Queue()
    vad = FakeVADWorker(routed_q, segment_q)
    vad.start()

    for i in range(10):
        routed_q.put(_make_packet("rx", ts=float(i)))
        routed_q.put(_make_packet("tx", ts=float(i)))
    segments = [_wait_get(segment_q) for _ in range(2)]
    assert all(s is not None for s in segments)
    ch_ids = {s.channel_id for s in segments if s}
    assert ch_ids == {"rx", "tx"}

    vad.stop()
    vad.join(timeout=3)


def test_fake_vad_stops_cleanly():
    routed_q = queue.Queue()
    segment_q = queue.Queue()
    vad = FakeVADWorker(routed_q, segment_q)
    vad.start()
    time.sleep(0.1)
    vad.stop()
    vad.join(timeout=3)
    assert not vad.is_alive()


def test_fake_vad_ignores_empty_packets():
    routed_q = queue.Queue()
    segment_q = queue.Queue()
    vad = FakeVADWorker(routed_q, segment_q)
    vad.start()
    time.sleep(0.5)
    vad.stop()
    vad.join(timeout=3)
    assert not vad.is_alive()
    assert segment_q.empty()
