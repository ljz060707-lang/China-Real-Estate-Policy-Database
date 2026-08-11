"""Private, file-backed logging for Dashboard read failures.

Dashboard exceptions are useful for diagnosing transient snapshot and schema
problems, but their tracebacks must never be rendered to ordinary users.  The
logger is configured lazily from the resolved CRPD settings so tests and
production use their own log roots without sharing handlers.
"""

from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from typing import Any

from policydb.settings import Settings


def _logger_for(settings: Settings) -> logging.Logger:
    log_path = settings.logs / "dashboard" / "dashboard.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"policydb.dashboard.{str(log_path).lower()}")
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    resolved_path = str(log_path.resolve()).lower()
    if not any(
        str(getattr(handler, "baseFilename", "")).lower() == resolved_path
        for handler in logger.handlers
    ):
        handler = RotatingFileHandler(
            log_path,
            maxBytes=2 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)
    return logger


def log_dashboard_exception(
    settings: Settings,
    message: str,
    *,
    component: str,
    operation: str | None = None,
    data_source: str | None = None,
    relation: str | None = None,
    query: str | None = None,
    error: BaseException,
) -> None:
    """Persist a redacted diagnostic record with the current exception traceback."""

    fields: dict[str, Any] = {
        "component": component,
        "operation": operation,
        "data_source": data_source,
        "relation": relation,
        "query": query,
        "exception_type": type(error).__name__,
        "exception_message": str(error),
    }
    payload = json.dumps(fields, ensure_ascii=False, default=str, sort_keys=True)
    _logger_for(settings).error(
        "%s | %s",
        message,
        payload,
        exc_info=(type(error), error, error.__traceback__),
    )


__all__ = ["log_dashboard_exception"]
