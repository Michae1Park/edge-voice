from __future__ import annotations

import logging
import queue
from typing import Callable, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)

DEFAULT_PUT_TIMEOUT_S = 0.2


# Tag drop logs with channel_id/segment_id so a lost segment can be found by filtering on its id.
def _item_context(item: object) -> dict[str, object]:
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
    if track_callback is not None:
        try:
            track_callback(item)
        except Exception:
            logger.exception("track_callback raised -- continuing")

    try:
        dst_queue.put(item, timeout=put_timeout)
    except queue.Full:
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
            logger.debug(
                "dump_queue full -- dropping %s copy",
                type(item).__name__,
                extra=_item_context(item),
            )
