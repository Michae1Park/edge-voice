from __future__ import annotations

import queue


def make_ingest_queue(maxsize: int) -> queue.Queue:
    return queue.Queue(maxsize=maxsize)


def make_routed_queue(maxsize: int) -> queue.Queue:
    return queue.Queue(maxsize=maxsize)


def make_segment_queue(maxsize: int) -> queue.Queue:
    return queue.Queue(maxsize=maxsize)


def make_dump_queue(maxsize: int) -> queue.Queue:
    return queue.Queue(maxsize=maxsize)
