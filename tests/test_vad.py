"""Tests for FakeVADWorker, PacketCopier, and PacketTracker."""

import queue
import time

from edge_voice.pipeline.fake_workers import FakeVADWorker
from edge_voice.pipeline.models import AudioPacket, SpeechSegment
from edge_voice.pipeline.packet_copier import PacketCopier
from edge_voice.pipeline.packet_tracker import AudioPacketTracker


# ── helpers ──────────────────


def _make_packet(channel_id: str, ts: float, n_samples: int = 320) -> AudioPacket:
    return AudioPacket(channel_id=channel_id, timestamp=ts, samples=b"\x00" * n_samples)


def _wait_get(q: queue.Queue, timeout: float = 2.0) -> object:
    """Get from queue with timeout, return None on timeout."""
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return None


# ── FakeVADWorker ──


def test_fake_vad_emits_segment_after_threshold():
    """VAD emits a SpeechSegment after PACKETS_PER_FAKE_SEGMENT packets."""
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
    """VAD resets its per-channel buffer after emitting a segment."""
    routed_q = queue.Queue()
    segment_q = queue.Queue()
    vad = FakeVADWorker(routed_q, segment_q)
    vad.start()

    # First segment
    for i in range(10):
        routed_q.put(_make_packet("rx", ts=float(i)))
    seg1 = _wait_get(segment_q)
    assert seg1 is not None

    # Second segment
    for i in range(10):
        routed_q.put(_make_packet("rx", ts=float(i) + 100))
    seg2 = _wait_get(segment_q)
    assert seg2 is not None

    assert seg1.segment_id != seg2.segment_id

    vad.stop()
    vad.join(timeout=3)


def test_fake_vad_handles_multiple_channels():
    """VAD tracks separate buffers per channel."""
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
    """VAD continues running even when no packets are sent."""
    routed_q = queue.Queue()
    segment_q = queue.Queue()
    vad = FakeVADWorker(routed_q, segment_q)
    vad.start()
    time.sleep(0.5)
    vad.stop()
    vad.join(timeout=3)
    assert not vad.is_alive()
    assert segment_q.empty()


# ── PacketCopier tests ──


def test_packet_copier_fans_to_both_outputs():
    """Each input packet appears in both output queues."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()
    copier = PacketCopier(src, dst1, dst2)
    copier.start()

    pkt = _make_packet("rx", ts=0.0)
    src.put(pkt)
    # Allow copier thread to process
    time.sleep(0.3)

    got1 = _wait_get(dst1)
    got2 = _wait_get(dst2)
    assert got1 is pkt
    assert got2 is pkt

    copier.stop()
    copier.join(timeout=3)


def test_packet_copier_calls_track_callback():
    """Tracker callback is invoked on each forwarded packet."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()
    packets_received = []
    callback_called = False

    def dummy_track(pkt):
        nonlocal callback_called
        callback_called = True
        packets_received.append(pkt)

    copier = PacketCopier(src, dst1, dst2, track_callback=dummy_track)
    copier.start()

    pkt = _make_packet("rx", ts=0.0)
    src.put(pkt)
    time.sleep(0.3)

    assert callback_called
    assert len(packets_received) == 1
    assert packets_received[0].channel_id == "rx"

    copier.stop()
    copier.join(timeout=3)


def test_packet_copier_stops_cleanly():
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()
    copier = PacketCopier(src, dst1, dst2)
    copier.start()
    time.sleep(0.1)
    copier.stop()
    copier.join(timeout=3)
    assert not copier.is_alive()


def test_packet_copier_handles_no_tracker():
    """Copier works without a track callback."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()
    copier = PacketCopier(src, dst1, dst2)
    copier.start()

    src.put(_make_packet("rx", ts=0.0))
    time.sleep(0.3)

    assert dst1.get(timeout=2) is not None
    assert dst2.get(timeout=2) is not None

    copier.stop()
    copier.join(timeout=3)


# ── PacketTracker tests ──


def test_track_creates_channel():
    """First track calls on new channel auto-creates ChannelState."""
    tracker = AudioPacketTracker(channel_ids=["rx", "tx"])
    pkt = _make_packet("rx", ts=1.0)
    snap = tracker.track(pkt)
    assert snap["channel_id"] == "rx"
    assert snap["packet_count"] == 1
    assert snap["last_seen_age"] >= 0


def test_track_updates_packet_count():
    tracker = AudioPacketTracker()
    for i in range(5):
        tracker.track(_make_packet("rx", ts=float(i)))
    snap = tracker.track(_make_packet("rx", ts=5.0))
    assert snap["packet_count"] == 6


def test_track_updating_segment_timestamps():
    """Segment start/end timestamps advance as packets arrive."""
    tracker = AudioPacketTracker()
    tracker.track(_make_packet("rx", ts=0.5))
    snap1 = tracker.track(_make_packet("rx", ts=1.0))
    snap2 = tracker.track(_make_packet("rx", ts=1.5))
    assert snap1["current_segment_start"] == 0.5
    assert snap1["current_segment_end"] == 1.0
    assert snap2["current_segment_end"] == 1.5
    assert snap2["duration_s"] == 1.0


def test_track_multiple_channels_independent():
    """Channel packet counts are independent."""
    tracker = AudioPacketTracker()
    tracker.track(_make_packet("rx", ts=1.0))
    tracker.track(_make_packet("tx", ts=1.0))
    snaps = tracker.dump_channels()
    counts = {s["channel_id"]: s["packet_count"] for s in snaps}
    assert counts["rx"] == 1
    assert counts["tx"] == 1


def test_dump_channels_empty():
    tracker = AudioPacketTracker()
    assert tracker.dump_channels() == []


def test_dump_channels_after_track():
    tracker = AudioPacketTracker()
    tracker.track(_make_packet("rx", ts=1.0))
    tracker.track(_make_packet("tx", ts=1.0))
    snaps = tracker.dump_channels()
    assert len(snaps) == 2
    assert {s["channel_id"] for s in snaps} == {"rx", "tx"}


def test_on_segment_start_resets_counters():
    tracker = AudioPacketTracker()
    for _ in range(5):
        tracker.track(_make_packet("rx", ts=1.0))
    tracker.on_segment_start("rx")
    snap = tracker.track(_make_packet("rx", ts=2.0))
    assert snap["packet_count"] == 6  # total still counts
    assert snap["current_segment_packets"] == 1


def test_on_segment_start_with_no_channel_returns_none():
    tracker = AudioPacketTracker()
    assert tracker.on_segment_start("unknown") is None


def test_on_segment_end_returns_channel():
    tracker = AudioPacketTracker()
    tracker.track(_make_packet("rx", ts=1.0))
    result = tracker.on_segment_end("rx")
    assert result is not None
    assert result.channel_id == "rx"


def test_on_segment_end_no_channel_returns_none():
    tracker = AudioPacketTracker()
    assert tracker.on_segment_end("unknown") is None


def test_expected_channels_preserved_when_no_packets():
    tracker = AudioPacketTracker(channel_ids=["rx", "tx"])
    assert tracker.channel_ids == {"rx", "tx"}
    assert tracker.dump_channels() == []
