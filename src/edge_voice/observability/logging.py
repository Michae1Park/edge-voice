"""
Structured logging for the pipeline.

`configs/default.yaml` already records the intent this wires up:

    logging:
      level: "INFO"
      json: true     # false -> pretty console renderer (dev)

One console handler; `LoggingSettings.is_json` picks its formatter -- JSON
by default, the original plain-text formatter when a dev sets
`logging.json: false` in `configs/local.yaml`. No separate file sink.

`get_stage_logger` gives each pipeline-stage module a logger that tags every
record with `stage`, so call sites only need to add `channel_id`/`segment_id`
via `extra={...}` when those are actually known (e.g. not yet, before a VAD
segment exists).
"""

from __future__ import annotations

import json
import logging
from typing import Any, MutableMapping

from edge_voice.config.settings import LoggingSettings

_CONSOLE_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"

# Set via extra={} at call sites / _MergingAdapter -- not stdlib LogRecord
# attributes, so each is only present on records that actually set it.
_STRUCTURED_FIELDS = ("stage", "channel_id", "segment_id")


class JsonFormatter(logging.Formatter):
    """One JSON object per line: timestamp, level, logger, message, plus
    whichever of stage/channel_id/segment_id the call site attached."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _STRUCTURED_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class _MergingAdapter(logging.LoggerAdapter[logging.Logger]):
    """LoggerAdapter that merges its bound fields with a call's own extra=.

    The stdlib default (LoggerAdapter.process) overwrites kwargs['extra']
    with self.extra instead of merging, which would silently drop a call
    site's channel_id/segment_id. This merges, call-site keys winning on
    conflict.
    """

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        bound = self.extra or {}
        extra = kwargs.get("extra") or {}
        kwargs["extra"] = {**bound, **extra}
        return msg, kwargs


def get_stage_logger(name: str, stage: str) -> logging.LoggerAdapter[logging.Logger]:
    """A logger for one pipeline stage; every record it emits carries `stage`."""
    return _MergingAdapter(logging.getLogger(name), {"stage": stage})


def configure_logging(settings: LoggingSettings, debug: bool = False) -> None:
    """Top-level logger config shared across all modules.

    `debug` always forces DEBUG level regardless of settings.level -- it's
    the CLI's --debug flag, a deliberate override of whatever's configured.
    """
    level = logging.DEBUG if debug else getattr(logging, settings.level.upper(), logging.INFO)

    handler = logging.StreamHandler()
    formatter: logging.Formatter = (
        JsonFormatter() if settings.is_json else logging.Formatter(_CONSOLE_FORMAT)
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
