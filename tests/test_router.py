"""Tests for edge_voice.channel.router."""

import queue
import time

from edge_voice.channel.router import ChannelRouter
from edge_voice.pipeline.models import AudioPacket


def _make_packet(channel_id: str, ts: float = 0.0, n_samples: int = 320) -> AudioPacket:
    return AudioPacket(channel_id=channel_id, timestamp=ts, samples=b"\x00" * n_samples)


def test_valid_packet_routed():
    ingest_q = queue.Queue()
    routed_q = queue.Queue()
    router = ChannelRouter(ingest_q, routed_q, channel_ids=["ch1", "ch2"])
    router.start()
    ingest_q.put(_make_packet("ch1"))
    assert routed_q.get(timeout=2)
    router.stop()
    router.join(timeout=3)


def test_invalid_channel_dropped():
    ingest_q = queue.Queue()
    routed_q = queue.Queue()
    router = ChannelRouter(ingest_q, routed_q, channel_ids=["ch1"])
    router.start()
    ingest_q.put(_make_packet("unknown"))
    assert routed_q.empty()
    router.stop()
    router.join(timeout=3)


def test_multiple_channels():
    ingest_q = queue.Queue()
    routed_q = queue.Queue()
    router = ChannelRouter(ingest_q, routed_q, channel_ids=["rx", "tx"])
    router.start()
    ingest_q.put(_make_packet("rx"))
    ingest_q.put(_make_packet("tx"))
    assert len([routed_q.get(timeout=2) for _ in range(2)]) == 2
    router.stop()
    router.join(timeout=3)


def test_freshness_after_packet():
    ingest_q = queue.Queue()
    routed_q = queue.Queue()
    router = ChannelRouter(ingest_q, routed_q, channel_ids=["ch1"])
    router.start()
    ingest_q.put(_make_packet("ch1"))
    time.sleep(0.2)
    age = router.get_freshness("ch1")
    assert age is not None and age >= 0.1
    router.stop()
    router.join(timeout=3)


def test_get_channel_ids():
    ingest_q = queue.Queue()
    routed_q = queue.Queue()
    router = ChannelRouter(ingest_q, routed_q, channel_ids=["tx", "rx", "meta"])
    assert router.get_channel_ids() == ["meta", "rx", "tx"]


def test_stop_event():
    ingest_q = queue.Queue()
    routed_q = queue.Queue()
    router = ChannelRouter(ingest_q, routed_q, channel_ids=["ch1"])
    router.start()
    router.stop()
    router.join(timeout=3)
    assert not router.is_alive()


def test_dump_queue_parameter_accepted():
    """Router accepts dump_queue kwarg without error (fanout handled by PacketCopier)."""
    ingest_q = queue.Queue()
    routed_q = queue.Queue()
    dump_q = queue.Queue()
    router = ChannelRouter(ingest_q, routed_q, channel_ids=["ch1"], dump_queue=dump_q)
    assert router is not None
    router.stop()


def test_multiple_packets_routed():
    ingest_q = queue.Queue()
    routed_q = queue.Queue()
    router = ChannelRouter(ingest_q, routed_q, channel_ids=["ch1"])
    router.start()
    for i in range(5):
        ingest_q.put(_make_packet("ch1", ts=float(i)))
    packets = [routed_q.get(timeout=2) for _ in range(5)]
    assert len(packets) == 5
    assert all(p.channel_id == "ch1" for p in packets)
    router.stop()
    router.join(timeout=3)
