"""
Structured logging with automatic pipeline/run/request context propagation.

Uses Python stdlib only: contextvars for async-safe context, logging.Filter
for automatic injection, and custom formatters for JSON (file) and
human-readable (console) output.

Context propagates automatically through asyncio.create_task() calls.
"""
from __future__ import annotations

import contextvars
import json
import logging
import logging.handlers
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional


# ---------------------------------------------------------------------------
# Context variables — automatically propagated to asyncio child tasks
# ---------------------------------------------------------------------------

pipeline_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "pipeline_id", default=None,
)
pipeline_name_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "pipeline_name", default=None,
)
run_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "run_id", default=None,
)
request_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "request_id", default=None,
)
component_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "component", default=None,
)


# ---------------------------------------------------------------------------
# Pipeline context manager
# ---------------------------------------------------------------------------

class PipelineContext:
    """Context manager that sets pipeline/run/component context on entry
    and restores previous values on exit.

    Works with both sync ``with`` and async ``async with``.
    Uses ContextVar.reset(token) so nested/sequential usage in loops
    cleanly restores previous values instead of leaking.

    Example::

        async with PipelineContext(pid, pname, run_id=rid, component="runner"):
            log.info("This log automatically includes pipeline and run context")
    """

    def __init__(
        self,
        pipeline_id: str,
        pipeline_name: str,
        run_id: Optional[str] = None,
        component: Optional[str] = None,
    ):
        self._pipeline_id = pipeline_id
        self._pipeline_name = pipeline_name
        self._run_id = run_id
        self._component = component
        self._tokens: list = []

    def __enter__(self):
        self._tokens.append(pipeline_id_var.set(self._pipeline_id))
        self._tokens.append(pipeline_name_var.set(self._pipeline_name))
        if self._run_id:
            self._tokens.append(run_id_var.set(self._run_id))
        if self._component:
            self._tokens.append(component_var.set(self._component))
        return self

    def __exit__(self, *exc):
        for token in reversed(self._tokens):
            token.var.reset(token)
        self._tokens.clear()
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, *exc):
        return self.__exit__(*exc)

    def set_run_id(self, run_id: str):
        """Update run_id within an existing context."""
        self._tokens.append(run_id_var.set(run_id))


# ---------------------------------------------------------------------------
# Request context helpers
# ---------------------------------------------------------------------------

def set_request_id(request_id: Optional[str] = None) -> contextvars.Token:
    """Set request_id in the current context. Returns token for reset."""
    return request_id_var.set(request_id or str(uuid.uuid4()))


def get_request_id() -> Optional[str]:
    return request_id_var.get()


# ---------------------------------------------------------------------------
# Context filter — injects contextvar values into every LogRecord
# ---------------------------------------------------------------------------

_CONTEXT_FIELDS = ("pipeline_id", "pipeline_name", "run_id", "request_id", "component")
_CONTEXT_VARS = (pipeline_id_var, pipeline_name_var, run_id_var, request_id_var, component_var)


class ContextFilter(logging.Filter):
    """Injects contextvar values into LogRecord attributes.

    Every log record gets these extra attributes (None when unset):
    pipeline_id, pipeline_name, run_id, request_id, component.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        for field, var in zip(_CONTEXT_FIELDS, _CONTEXT_VARS):
            setattr(record, field, var.get())
        return True


# ---------------------------------------------------------------------------
# JSON formatter — one JSON object per line
# ---------------------------------------------------------------------------

class JSONFormatter(logging.Formatter):
    """Produces one JSON object per line for log aggregation tools.

    Fields: timestamp, level, logger, message, plus all non-None context fields.
    Exception info is included as 'exception' field.
    """

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add context fields only when set (keeps JSON compact)
        for field in _CONTEXT_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                entry[field] = value

        if record.exc_info and record.exc_info[1] is not None:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, default=str)


# ---------------------------------------------------------------------------
# Console formatter — human-readable with inline context
# ---------------------------------------------------------------------------

class ConsoleFormatter(logging.Formatter):
    """Human-readable format with inline context tag.

    Examples::

        2026-03-08 14:32:15 INFO  agent.autonomous -- [demo-orders | run:abc12345] Extracted 30 rows
        2026-03-08 14:32:45 INFO  api.server -- [req:f47ac10b] GET /api/pipelines 200 (42ms)
        2026-03-08 14:33:00 INFO  main -- Starting Pipeline Agent...
    """

    def format(self, record: logging.LogRecord) -> str:
        parts = []
        pname = getattr(record, "pipeline_name", None)
        rid = getattr(record, "run_id", None)
        reqid = getattr(record, "request_id", None)

        if pname:
            parts.append(pname)
        if rid:
            parts.append(f"run:{rid[:8]}")
        if reqid:
            parts.append(f"req:{reqid[:8]}")

        ctx_tag = f" [{' | '.join(parts)}]" if parts else ""

        ts = datetime.fromtimestamp(
            record.created, tz=timezone.utc,
        ).strftime("%Y-%m-%d %H:%M:%S")
        base = f"{ts} {record.levelname:<5} {record.name} --{ctx_tag} {record.getMessage()}"

        if record.exc_info and record.exc_info[1] is not None:
            base += "\n" + self.formatException(record.exc_info)

        return base


# ---------------------------------------------------------------------------
# Setup function
# ---------------------------------------------------------------------------

def setup_logging(
    log_level: str = "INFO",
    log_dir: str = "./data/logs",
    max_bytes: int = 50 * 1024 * 1024,
    backup_count: int = 5,
    json_logging: bool = True,
) -> None:
    """Configure logging with context-aware formatters and log rotation.

    Args:
        log_level: Root log level (from LOG_LEVEL env var).
        log_dir: Directory for log files.
        max_bytes: Max size per log file before rotation (default 50 MB).
        backup_count: Number of rotated log files to keep (default 5).
        json_logging: If True, file output is JSON; if False, human-readable.
    """
    os.makedirs(log_dir, exist_ok=True)
    level = getattr(logging, log_level.upper(), logging.INFO)

    ctx_filter = ContextFilter()

    # Console handler: human-readable
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ConsoleFormatter())
    console_handler.addFilter(ctx_filter)

    # File handler: JSON with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "pipeline-agent.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    file_handler.setFormatter(
        JSONFormatter() if json_logging else ConsoleFormatter(),
    )
    file_handler.addFilter(ctx_filter)

    # Configure root logger
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Suppress noisy third-party loggers
    for noisy in ("asyncpg", "uvicorn.access", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
