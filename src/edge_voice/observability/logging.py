from __future__ import annotations

import json
import logging
from datetime import datetime
from logging.handlers import QueueListener, RotatingFileHandler
from pathlib import Path
from typing import Any, MutableMapping

from edge_voice.config.settings import LoggingSettings

_CONSOLE_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"

_STRUCTURED_FIELDS = ("stage", "channel_id", "segment_id", "stt_latency_s")


class JsonFormatter(logging.Formatter):
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
        # Keep Korean transcripts readable instead of \uXXXX escapes.
        return json.dumps(payload, default=str, ensure_ascii=False)


# The stdlib LoggerAdapter overwrites a call's extra= instead of merging it.
class _MergingAdapter(logging.LoggerAdapter[logging.Logger]):
    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        bound = self.extra or {}
        extra = kwargs.get("extra") or {}
        kwargs["extra"] = {**bound, **extra}
        return msg, kwargs


def get_stage_logger(name: str, stage: str) -> logging.LoggerAdapter[logging.Logger]:
    return _MergingAdapter(logging.getLogger(name), {"stage": stage})


def current_log_level() -> int:
    return logging.getLogger().getEffectiveLevel()


# Must run even with no handlers: it also drains the queue, and an unread queue blocks the child.
def start_log_queue_listener(log_queue: Any) -> QueueListener:
    root = logging.getLogger()
    listener = QueueListener(log_queue, *root.handlers, respect_handler_level=True)
    listener.start()
    return listener


def configure_logging(settings: LoggingSettings, debug: bool = False) -> None:
    if not settings.enabled:
        logging.disable(logging.CRITICAL)
        return
    logging.disable(logging.NOTSET)

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
