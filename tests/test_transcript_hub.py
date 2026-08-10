"""Tests for edge_voice.pipeline.transcript_hub."""

import queue

from edge_voice.pipeline.models import TranscriptEvent
from edge_voice.pipeline.transcript_hub import TranscriptHub


def _event(text: str = "hello", channel_id: str = "rx", is_final: bool = True) -> TranscriptEvent:
    return TranscriptEvent(
        channel_id=channel_id,
        segment_id="seg-1",
        text=text,
        start=0.0,
        end=1.0,
        is_final=is_final,
    )


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


def test_clear_empties_the_replay_backlog():
    hub = TranscriptHub()
    hub.publish(_event("first"))
    hub.clear()
    sub = hub.subscribe()
    assert sub.empty()


def test_clear_does_not_affect_live_subscribers():
    hub = TranscriptHub()
    sub = hub.subscribe()
    hub.publish(_event("first"))
    hub.clear()
    hub.publish(_event("second"))
    assert sub.get_nowait().text == "first"
    assert sub.get_nowait().text == "second"


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


# -- partials -----------------------------


def test_partials_reach_live_subscribers():
    hub = TranscriptHub()
    sub = hub.subscribe()
    event = _event(text="in progress", is_final=False)

    hub.publish(event)

    assert sub.get_nowait() is event


def test_partials_are_kept_out_of_the_replay_backlog():
    """The backlog is fixed-size and replayed to every new subscriber, so a
    superseded prefix there costs a real transcript its slot."""
    hub = TranscriptHub()
    hub.publish(_event(text="in progress", is_final=False))
    hub.publish(_event(text="settled"))

    replayed = hub.subscribe()
    texts = []
    while not replayed.empty():
        texts.append(replayed.get_nowait().text)

    assert texts == ["settled"]


def test_backlog_replays_in_spoken_order_not_publish_order():
    """Each channel decodes independently (its own process in the deployed
    config), so a short utterance can be published ahead of a longer one
    spoken earlier on the other channel. A reconnecting client must still
    see the conversation in the order it happened -- matching what the live
    view shows, which orders by timestamp too."""
    hub = TranscriptHub()

    spoken_first = TranscriptEvent(
        channel_id="rx", segment_id="rx-1", text="spoken first", start=10.0, end=15.0
    )
    spoken_second = TranscriptEvent(
        channel_id="tx", segment_id="tx-1", text="spoken second", start=16.0, end=17.0
    )
    # The later utterance finishes decoding first and is published first.
    hub.publish(spoken_second)
    hub.publish(spoken_first)

    sub = hub.subscribe()
    replayed = [sub.get_nowait().text for _ in range(2)]

    assert replayed == ["spoken first", "spoken second"]
