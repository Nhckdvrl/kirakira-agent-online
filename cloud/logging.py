"""Secret-safe JSON logging setup for Cloud processes."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
import os
import re


_SECRET_PATTERNS = (
    re.compile(r"(?i)(api[_-]?key|authorization|password|token|secret)\s*[:=]\s*[^\s,;]+"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+"),
    re.compile(r"(?i)(postgres(?:ql)?://[^:/\s]+:)[^@\s]+@"),
)


def redact_text(value: object, *, limit: int = 500) -> str:
    text = str(value or "")
    for pattern in _SECRET_PATTERNS:
        replacement = r"\1[REDACTED]" if pattern.groups else "[REDACTED]"
        text = pattern.sub(replacement, text)
    return text[: max(1, int(limit))]


def safe_exception_summary(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {redact_text(exc)}"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_text(record.getMessage(), limit=4000),
        }
        fields = getattr(record, "cloud_fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["exception_type"] = record.exc_info[0].__name__
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_cloud_logging() -> None:
    level = os.getenv("KIRAKIRA_LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
