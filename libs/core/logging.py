from __future__ import annotations

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from typing import Any, Dict, Mapping, Optional

import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from .config import Settings

_configured = False

TRACE_HEADER = "x-trace-id"
SYMBOL_HEADER = "x-symbol"
SIGNAL_HEADER = "x-signal-code"

trace_id_var: ContextVar[str | None] = ContextVar("trace_id", default=None)
symbol_var: ContextVar[str | None] = ContextVar("symbol", default=None)
signal_code_var: ContextVar[str | None] = ContextVar("signal_code", default=None)


def configure_logging(settings: Optional[Settings] = None) -> None:
    """Configure structlog and stdlib logging once per process."""
    global _configured
    if _configured:
        return

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, (settings.log_level if settings else "INFO").upper(), logging.INFO),
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.ExceptionPrettyPrinter(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )
    _configured = True


def _bind_contextvars(**values: str) -> None:
    structlog.contextvars.bind_contextvars(**{k: v for k, v in values.items() if v})


def bind_trace_context(
    trace_id: str | None = None,
    *,
    symbol: str | None = None,
    signal_code: str | None = None,
) -> str:
    """Bind or update the tracing context for the current request/task."""

    resolved_trace_id = trace_id or trace_id_var.get() or str(uuid.uuid4())
    trace_id_var.set(resolved_trace_id)
    if symbol is not None:
        symbol_var.set(symbol)
    if signal_code is not None:
        signal_code_var.set(signal_code)

    _bind_contextvars(
        trace_id=resolved_trace_id,
        symbol=symbol_var.get() or "",
        signal_code=signal_code_var.get() or "",
    )
    return resolved_trace_id


def update_trace_context(*, symbol: str | None = None, signal_code: str | None = None) -> None:
    """Update symbol/signal metadata for downstream logging within the same trace."""

    if symbol is not None:
        symbol_var.set(symbol)
    if signal_code is not None:
        signal_code_var.set(signal_code)
    _bind_contextvars(
        symbol=symbol_var.get() or "",
        signal_code=signal_code_var.get() or "",
    )


def clear_trace_context() -> None:
    """Clear context vars for the current task to avoid cross-request leakage."""

    trace_id_var.set(None)
    symbol_var.set(None)
    signal_code_var.set(None)
    structlog.contextvars.clear_contextvars()


def current_trace_context() -> Dict[str, str]:
    context: Dict[str, str] = {}
    trace_id = trace_id_var.get()
    symbol = symbol_var.get()
    signal_code = signal_code_var.get()
    if trace_id:
        context["trace_id"] = trace_id
    if symbol:
        context["symbol"] = symbol
    if signal_code:
        context["signal_code"] = signal_code
    return context


def attach_trace_metadata(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a copy of payload with trace metadata injected at the top level."""

    enriched = dict(payload)
    for key, value in current_trace_context().items():
        enriched.setdefault(key, value)
    return enriched


class TraceContextMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware that manages trace context based on inbound headers."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        structlog.contextvars.clear_contextvars()
        trace_id = request.headers.get(TRACE_HEADER)
        symbol = request.headers.get(SYMBOL_HEADER)
        signal_code = request.headers.get(SIGNAL_HEADER)

        trace_id = bind_trace_context(trace_id=trace_id, symbol=symbol, signal_code=signal_code)

        request.state.trace_id = trace_id
        request.state.symbol = symbol
        request.state.signal_code = signal_code

        try:
            response = await call_next(request)
        finally:
            clear_trace_context()

        response.headers.setdefault(TRACE_HEADER, trace_id)
        return response


def encode_with_trace(payload: Mapping[str, Any]) -> str:
    """Serialize payload with trace metadata appended."""

    return json.dumps(attach_trace_metadata(payload))


def get_logger(name: str | None = None):
    """
    Return a structlog logger, ensuring logging is configured once.

    Falls back to default configuration when no Settings are provided.
    """
    if not _configured:
        configure_logging()
    return structlog.get_logger(name) if name else structlog.get_logger()
