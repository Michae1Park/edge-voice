"""Bounded queues connecting pipeline stages.

Queue sizes come from ``Settings.queues`` (defaults in code), configurable
via ``configs/default.yaml`` under the ``queues`` key, or via
``EDGE_VOICE__QUEUE__INGEST`` etc.
"""

from __future__ import annotations

import queue as _queue

from edge_voice.config.settings import Settings

_settings: Settings | None = None


def _get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings.load()
    return _settings


def _maxsize(key: str, maxsize: int | None = None) -> int:
    return maxsize or getattr(_get_settings().queues, key)


def make_ingest_queue(maxsize: int | None = None) -> _queue.Queue:
    return _queue.Queue(maxsize=_maxsize("ingest", maxsize))


def make_routed_queue(maxsize: int | None = None) -> _queue.Queue:
    return _queue.Queue(maxsize=_maxsize("routed", maxsize))


def make_segment_queue(maxsize: int | None = None) -> _queue.Queue:
    return _queue.Queue(maxsize=_maxsize("segment", maxsize))


def make_dump_queue(maxsize: int | None = None) -> _queue.Queue:
    return _queue.Queue(maxsize=_maxsize("dump", maxsize))
