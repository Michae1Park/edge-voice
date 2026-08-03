"""
Structured logging for the pipeline.

`logging.enabled` is a master switch: when false, `logging.disable()` makes
every `logger.log()` call process-wide a cheap no-op, skipped before any
formatting or handler work happens. For performance-sensitive runs where
logging overhead itself matters, not just log volume.

When enabled, two independently-toggleable handlers are available --
`logging.console_enabled` for stderr (JSON or pretty-text per `logging.json`)
and `logging.file_enabled` for a rotating file under `logging.output_dir`
(created on demand, always JSON regardless of the console format, since a
file sink exists for later grep/tooling, not for a human reading the
terminal). The file is named with the timestamp of when this function ran
(`edge-voice-<started-at>.log`), so each run gets its own file instead of
every run appending to (or rotating through) the same one. Rotation
(`max_bytes`/`backup_count`) still bounds any single run's file if it grows
large, the same way the audio dump workers bound their own local writes,
rather than growing without limit on storage-constrained hardware.

The file sink is deliberately not run through `atomic_write`: that module
protects a *whole-file overwrite* from a torn/truncated result, but this is
an append-only stream -- everything already flushed to disk before a power
cut stays intact; the worst case is losing the one line that was mid-write,
not corrupting anything already written.

`get_stage_logger` gives each pipeline-stage module a logger that tags every
record with `stage`, so call sites only need to add `channel_id`/`segment_id`
via `extra={...}` when those are actually known (e.g. not yet, before a VAD
segment exists).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, MutableMapping

from edge_voice.config.settings import LoggingSettings

_CONSOLE_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"

# Set via extra={} at call sites / _MergingAdapter -- not stdlib LogRecord
# attributes, so each is only present on records that actually set it.
# stt_latency_s is orchestrator._on_transcript's addition to the TRANSCRIPT
# line -- the segment's own STTWorker.last_latency_s, not an aggregate.
_STRUCTURED_FIELDS = ("stage", "channel_id", "segment_id", "stt_latency_s")


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
        # ensure_ascii=False: this app's default language is Korean, and every
        # transcript line goes through here -- the default (True) would escape
        # every non-ASCII character into \uXXXX, making logs unreadable for
        # the app's primary use case. Still valid JSON/UTF-8 either way.
        return json.dumps(payload, default=str, ensure_ascii=False)


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
    if not settings.enabled:
        logging.disable(logging.CRITICAL)
        return
    logging.disable(logging.NOTSET)  # undo a prior disable, e.g. across test runs

    level = logging.DEBUG if debug else getattr(logging, settings.level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    if settings.console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            JsonFormatter() if settings.is_json else logging.Formatter(_CONSOLE_FORMAT)
        )
        root.addHandler(console_handler)

    if settings.file_enabled:
        log_dir = Path(settings.output_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        started_at = datetime.now().strftime("%Y%m%d-%H%M%S")
        file_handler = RotatingFileHandler(
            log_dir / f"edge-voice-{started_at}.log",
            maxBytes=settings.max_bytes,
            backupCount=settings.backup_count,
        )
        file_handler.setFormatter(JsonFormatter())
        root.addHandler(file_handler)
