"""Tests for the per-channel repacketize latency tracking added to
ChannelRouter."""

import queue
import time

import pytest

from edge_voice.channel.router import ChannelRouter
from edge_voice.pipeline.models import AudioPacket

# 20ms @ 16kHz, int16 mono -- matches RepacketizerConfig's default incoming_ms.
INCOMING_BYTES = 640


def _wait_until(predicate, timeout: float = 2.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def running_router():
    started: list[ChannelRouter] = []

    def _start(ingest_queue, channel_ids=("rx", "tx")):
        router = ChannelRouter(
            ingest_queue=ingest_queue,
            routed_queue=queue.Queue(),
            channel_ids=list(channel_ids),
        )
        router.start()
        started.append(router)
        return router

    yield _start

    for router in started:
        router.stop()
        router.join(timeout=3)


def test_repacketize_latency_recorded_per_channel(running_router):
    ingest = queue.Queue()
    router = running_router(ingest)
    ingest.put(AudioPacket(channel_id="rx", timestamp=0.0, samples=bytes(INCOMING_BYTES)))

    assert _wait_until(lambda: "rx" in router.repacketize_latencies_s())
    assert router.repacketize_latencies_s()["rx"] >= 0.0


def test_unknown_channel_does_not_record_latency(running_router):
    ingest = queue.Queue()
    router = running_router(ingest, channel_ids=("rx",))
    ingest.put(AudioPacket(channel_id="unknown", timestamp=0.0, samples=bytes(INCOMING_BYTES)))

    time.sleep(0.2)  # give the router a moment to (not) process it
    assert router.repacketize_latencies_s() == {}


def test_rejected_packet_does_not_record_latency(running_router):
    ingest = queue.Queue()
    router = running_router(ingest)
    # Wrong size for the default RepacketizerConfig -> Repacketizer.process
    # raises ValueError, which run() catches and drops without recording.
    ingest.put(AudioPacket(channel_id="rx", timestamp=0.0, samples=bytes(10)))

    time.sleep(0.2)
    assert router.repacketize_latencies_s() == {}


def test_latencies_are_independent_per_channel(running_router):
    ingest = queue.Queue()
    router = running_router(ingest)
    ingest.put(AudioPacket(channel_id="rx", timestamp=0.0, samples=bytes(INCOMING_BYTES)))
    ingest.put(AudioPacket(channel_id="tx", timestamp=0.0, samples=bytes(INCOMING_BYTES)))

    assert _wait_until(lambda: set(router.repacketize_latencies_s()) == {"rx", "tx"})


def test_accessor_returns_copy_not_live_reference(running_router):
    ingest = queue.Queue()
    router = running_router(ingest)
    ingest.put(AudioPacket(channel_id="rx", timestamp=0.0, samples=bytes(INCOMING_BYTES)))
    assert _wait_until(lambda: "rx" in router.repacketize_latencies_s())

    snapshot = router.repacketize_latencies_s()
    snapshot["rx"] = -1.0
    assert router.repacketize_latencies_s()["rx"] != -1.0
