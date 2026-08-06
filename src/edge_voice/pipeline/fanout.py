"""Fan an item out to two destination queues: forwarded to the main pipeline,
optionally copied for debugging (e.g. audio/segment dump)."""

from __future__ import annotations

import logging
import queue
from typing import Callable, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)

DEFAULT_PUT_TIMEOUT_S = 0.2


def _item_context(item: object) -> dict[str, object]:
    """`channel_id`/`segment_id` off the item, for the drop log lines below.

    A drop here is the one point where a packet or a finalized segment leaves
    the pipeline without reaching the next stage. Logging that generically
    ("dropping item") makes it the single event in a segment's lifecycle that
    can't be found by filtering the log on its `segment_id` -- precisely the
    event you'd be looking for. Read duck-typed rather than typed, since this
    module is generic over what it forwards: an AudioPacket has only
    `channel_id`, a SpeechSegment has both, anything else contributes nothing.
    """
    context: dict[str, object] = {}
    for field in ("channel_id", "segment_id"):
        value = getattr(item, field, None)
        if value is not None:
            context[field] = value
    return context


def fanout_put(
    item: T,
    dst_queue: queue.Queue[T],
    dump_queue: queue.Queue[T] | None = None,
    track_callback: Callable[[T], None] | None = None,
    put_timeout: float = DEFAULT_PUT_TIMEOUT_S,
) -> None:
    """Forward *item* to dst_queue, and optionally copy it to dump_queue.

    Called inline from whichever thread already produces items for
    dst_queue (e.g. ChannelRouter, the VAD/segment stage) -- no separate
    thread needed since this does no independent blocking work.
    """
    if track_callback is not None:
        try:
            track_callback(item)
        except Exception:
            logger.exception("track_callback raised -- continuing")

    try:
        dst_queue.put(item, timeout=put_timeout)
    except queue.Full:
        # Real data loss, not a shed optional extra: whatever this was (an
        # audio packet, a finalized segment) does not reach the next stage.
        logger.warning(
            "dst_queue full after %.2fs -- dropping %s",
            put_timeout,
            type(item).__name__,
            extra=_item_context(item),
        )

    if dump_queue is not None:
        try:
            dump_queue.put(item, timeout=put_timeout)
        except queue.Full:
            # Debug-only copy; losing it costs nothing the pipeline needs.
            logger.debug(
                "dump_queue full -- dropping %s copy",
                type(item).__name__,
                extra=_item_context(item),
            )
