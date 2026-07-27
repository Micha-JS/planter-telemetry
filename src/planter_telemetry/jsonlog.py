"""Structured JSON logging shared by every entrypoint.

One JSON object per line on stderr. The convention across the codebase: the
log *message* is a constant event name ("stats", "dead_letter", ...) and all
variability travels in ``extra={...}``, so lines are machine-parseable
without regex archaeology. stderr is deliberate — the capture CLI writes
JSONL data to stdout, and logs must never pollute it.

Line shape:

    {"ts": "2026-07-27T12:00:00.123456+00:00", "level": "INFO",
     "service": "ingestion", "logger": "planter_telemetry.ingestion",
     "event": "stats", "ingested": 42, ...}
"""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

# Everything a LogRecord carries that is not caller-supplied `extra`.
# Computed from a probe record so new stdlib attributes (e.g. taskName in
# 3.12) are excluded automatically; "message"/"asctime" appear only after
# Formatter.format() has run, so they are added by hand.
_RESERVED: frozenset[str] = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
}


class JsonFormatter(logging.Formatter):
    """Render records as one JSON object per line."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        line: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "service": self._service,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                line[key] = value
        if record.exc_info:
            line["exc"] = self.formatException(record.exc_info)
        # default=str: a datetime or enum in `extra` must never crash logging.
        return json.dumps(line, ensure_ascii=False, default=str)


def configure(service: str, *, level: int = logging.INFO) -> None:
    """Install the JSON formatter on the root logger.

    Called once per process entrypoint (never from library code, so
    in-process test runs don't fight pytest over the root logger).
    """
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter(service))
    logging.basicConfig(level=level, handlers=[handler])
