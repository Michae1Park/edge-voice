"""Fan an item out to two destination queues: forwarded to the main pipeline,
optionally copied for debugging (e.g. audio/segment dump)."""

from __future__ import annotations

import logging
import queue
from typing import Callable, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)

DEFAULT_PUT_TIMEOUT_S = 0.2


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
        logger.warning("dst_queue full -- dropping item")

    if dump_queue is not None:
        try:
            dump_queue.put(item, timeout=put_timeout)
        except queue.Full:
            logger.debug("dump_queue full -- dropping item")
