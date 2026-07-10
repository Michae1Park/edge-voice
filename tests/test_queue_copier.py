"""Tests for QueueCopier."""

import queue
import time

from edge_voice.pipeline.queue_copier import QueueCopier


def _make_item(label: str):
    """Create a simple dict to use as a queue item."""
    return {"label": label, "ts": time.time()}


def _wait_get(q: queue.Queue, timeout: float = 2.0):
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return None


# ── Basic fan-out ───────────────────────────────────────────


def test_fans_to_both_destinations():
    """Each input item appears in both output queues."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()

    copier = QueueCopier(src, dst1, dst2)
    copier.start()

    item = _make_item("fan1")
    src.put(item)
    time.sleep(0.3)

    got1 = _wait_get(dst1)
    got2 = _wait_get(dst2)

    assert got1 is item
    assert got2 is item

    copier.stop()
    copier.join(timeout=3)


def test_fans_multiple_items():
    """Ten items all appear in both output queues."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()

    copier = QueueCopier(src, dst1, dst2)
    copier.start()

    items = [_make_item(str(i)) for i in range(10)]
    for item in items:
        src.put(item)

    time.sleep(0.5)

    for item in items:
        assert _wait_get(dst1) is item
        assert _wait_get(dst2) is item

    copier.stop()
    copier.join(timeout=3)


# ── Track callback ───────────────────────────────────────────


def test_calls_track_callback():
    """Track callback is invoked once per forwarded item."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()

    collected: list = []

    def track(item):
        collected.append(item)

    copier = QueueCopier(src, dst1, dst2, track_callback=track)
    copier.start()

    item = _make_item("tracked")
    src.put(item)
    time.sleep(0.3)

    assert len(collected) == 1
    assert collected[0] is item

    copier.stop()
    copier.join(timeout=3)


def test_track_callback_exceptions_are_ignored():
    """A callback that raises does not break the copier."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()

    def bad_track(item):
        raise RuntimeError("oops")

    copier = QueueCopier(src, dst1, dst2, track_callback=bad_track)
    copier.start()

    item = _make_item("safe")
    src.put(item)
    time.sleep(0.3)

    # Copier should still forward despite the bad callback
    assert _wait_get(dst1) is item
    assert _wait_get(dst2) is item

    copier.stop()
    copier.join(timeout=3)


# ── Queue full handling ──────────────────────────────────────


def test_dst1_full_waits_then_warns():
    """When dst1 is full, copier blocks briefly then logs a warning and does not forward to dst2."""
    full_dst1 = queue.Queue(maxsize=1)
    full_dst1.put(_make_item("preload"))  # fill

    src = queue.Queue()
    dst2 = queue.Queue()

    copier = QueueCopier(src, full_dst1, dst2, put_timeout=0.2)
    copier.start()

    item = _make_item("overflow")
    src.put(item)
    time.sleep(0.6)

    # dst1 should still have only the preload (the copier gave up after timeout)
    assert full_dst1.qsize() == 1

    # src item was consumed but dst1 full, dst2 may or may not get it
    # depending on ordering of puts
    copier.stop()
    copier.join(timeout=3)


def test_dst2_full_does_not_affect_dst1():
    """When dst2 is full, dst1 still receives the item."""
    full_dst2 = queue.Queue(maxsize=1)
    full_dst2.put(_make_item("preload"))

    src = queue.Queue()
    dst1 = queue.Queue()

    copier = QueueCopier(src, dst1, full_dst2, put_timeout=0.2)
    copier.start()

    item = _make_item("survives")
    src.put(item)
    time.sleep(0.5)

    assert _wait_get(dst1) is item

    copier.stop()
    copier.join(timeout=3)


# ── Stop / lifecycle ─────────────────────────────────────────


def test_stop_clears_stopping_flag():
    copier = QueueCopier(queue.Queue(), queue.Queue(), queue.Queue())
    assert not copier.stopping
    copier.stop()
    assert copier.stopping


def test_stop_with_no_items():
    """Stopping immediately when idle works cleanly."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()

    copier = QueueCopier(src, dst1, dst2)
    copier.start()
    time.sleep(0.1)
    copier.stop()
    copier.join(timeout=3)

    assert not copier.is_alive()


def test_stop_pending_items_still_forwarded():
    """Items already in src are forwarded before the copier stops."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()

    copier = QueueCopier(src, dst1, dst2)
    copier.start()

    # Pre-fill src
    item1 = _make_item("item1")
    src.put(item1)

    # Immediately stop (copier should still process the pending item)
    time.sleep(0.05)
    copier.stop()
    copier.join(timeout=3)

    assert _wait_get(dst1) is item1


def test_stop_with_track_callback():
    """Track callback is called for remaining items before stop."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()

    track_count = [0]

    def track(item):
        track_count[0] += 1

    copier = QueueCopier(src, dst1, dst2, track_callback=track)
    copier.start()

    item = _make_item("counted")
    src.put(item)
    time.sleep(0.3)

    copier.stop()
    copier.join(timeout=3)

    assert track_count[0] == 1


# ── No tracking ──────────────────────────────────────────────


def test_no_track_callback_works():
    """Copier works without a track callback (default path)."""
    src = queue.Queue()
    dst1 = queue.Queue()
    dst2 = queue.Queue()

    copier = QueueCopier(src, dst1, dst2)
    copier.start()

    item = _make_item("no_track")
    src.put(item)
    time.sleep(0.3)

    assert _wait_get(dst1) is item
    assert _wait_get(dst2) is item

    copier.stop()
    copier.join(timeout=3)


# ── Put timeout variations ───────────────────────────────────


def test_put_timeout_zero_is_nonblocking():
    """put_timeout=0 uses non-blocking put (never blocks)."""
    full_dst1 = queue.Queue(maxsize=1)
    full_dst1.put(_make_item("preload"))

    src = queue.Queue()
    dst2 = queue.Queue()

    copier = QueueCopier(src, full_dst1, dst2, put_timeout=0)
    copier.start()

    item = _make_item("nonblock")
    src.put(item)
    time.sleep(0.4)

    assert full_dst1.qsize() == 1

    copier.stop()
    copier.join(timeout=3)
