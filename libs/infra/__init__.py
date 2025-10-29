"""Infrastructure integrations (IBKR, databases, messaging)."""

from .db import get_session_factory
from .redis_bus import RedisBus

# Lazy import to avoid circular dependency with libs.core.timeutil
def __getattr__(name):
    if name in ("IBClient", "OptionContractQuote", "build_ibkr_client", "get_next_order_id"):
        from .ibkr_client import IBClient, OptionContractQuote, build_ibkr_client, get_next_order_id
        return locals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "get_session_factory",
    "build_ibkr_client",
    "get_next_order_id",
    "RedisBus",
    "IBClient",
    "OptionContractQuote",
]
