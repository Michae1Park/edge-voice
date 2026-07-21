"""Tests for edge_voice.pipeline.transcript_hub."""

import queue

from edge_voice.pipeline.models import TranscriptEvent
from edge_voice.pipeline.transcript_hub import TranscriptHub


def _event(text: str = "hello", channel_id: str = "rx") -> TranscriptEvent:
    return TranscriptEvent(channel_id=channel_id, segment_id="seg-1", text=text, start=0.0, end=1.0)


def test_subscribe_returns_empty_queue_with_no_backlog():
    hub = TranscriptHub()
    sub = hub.subscribe()
    assert sub.empty()


def test_publish_reaches_existing_subscriber():
    hub = TranscriptHub()
    sub = hub.subscribe()
    event = _event()
    hub.publish(event)
    assert sub.get_nowait() is event


def test_publish_reaches_multiple_subscribers():
    hub = TranscriptHub()
    sub_a = hub.subscribe()
    sub_b = hub.subscribe()
    event = _event()
    hub.publish(event)
    assert sub_a.get_nowait() is event
    assert sub_b.get_nowait() is event


def test_subscribe_replays_backlog():
    hub = TranscriptHub(backlog=10)
    hub.publish(_event("first"))
    hub.publish(_event("second"))
    sub = hub.subscribe()
    assert sub.get_nowait().text == "first"
    assert sub.get_nowait().text == "second"
    assert sub.empty()


def test_backlog_is_bounded():
    hub = TranscriptHub(backlog=2)
    hub.publish(_event("first"))
    hub.publish(_event("second"))
    hub.publish(_event("third"))
    sub = hub.subscribe()
    assert sub.get_nowait().text == "second"
    assert sub.get_nowait().text == "third"
    assert sub.empty()


def test_unsubscribe_stops_delivery():
    hub = TranscriptHub()
    sub = hub.subscribe()
    hub.unsubscribe(sub)
    hub.publish(_event())
    assert sub.empty()


def test_unsubscribe_unknown_queue_is_a_noop():
    hub = TranscriptHub()
    stray: "queue.Queue" = queue.Queue()
    hub.unsubscribe(stray)  # must not raise


def test_full_subscriber_queue_drops_without_raising():
    hub = TranscriptHub()
    sub = hub.subscribe()
    # Fill the subscriber's queue past its maxsize; publish() must swallow
    # queue.Full for that subscriber rather than propagating it to the
    # STTWorker thread that's actually calling publish().
    try:
        while True:
            sub.put_nowait(_event())
    except queue.Full:
        pass
    hub.publish(_event("overflow"))  # should not raise
