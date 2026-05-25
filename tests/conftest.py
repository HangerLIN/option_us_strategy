from __future__ import annotations

import os
import sys
import types
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT / "apps"

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///./test.db")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("IB_HOST", "127.0.0.1")
os.environ.setdefault("IB_PORT", "4002")
os.environ.setdefault("IB_CLIENT_ID", "1")
os.environ.setdefault("IB_ACCOUNT", "DU0000001")
os.environ.setdefault("PAPER", "1")

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

if "prometheus_client" not in sys.modules:
    prometheus_module = types.ModuleType("prometheus_client")

    class _MetricStub:
        def labels(self, *args: object, **kwargs: object) -> "_MetricStub":
            return self

        def inc(self, *args: object, **kwargs: object) -> None:
            return None

        def set(self, *args: object, **kwargs: object) -> None:
            return None

        def observe(self, *args: object, **kwargs: object) -> None:
            return None

    def _metric_factory(*args: object, **kwargs: object) -> _MetricStub:
        return _MetricStub()

    prometheus_module.Counter = _metric_factory  # type: ignore[attr-defined]
    prometheus_module.Gauge = _metric_factory  # type: ignore[attr-defined]
    prometheus_module.Histogram = _metric_factory  # type: ignore[attr-defined]
    prometheus_module.CONTENT_TYPE_LATEST = "text/plain"  # type: ignore[attr-defined]
    prometheus_module.generate_latest = lambda: b""  # type: ignore[attr-defined]

    sys.modules["prometheus_client"] = prometheus_module


@event.listens_for(Engine, "connect")
def _register_sqlite_date_trunc(dbapi_connection, connection_record) -> None:
    if hasattr(dbapi_connection, "create_function"):
        dbapi_connection.create_function("date_trunc", 2, lambda _part, value: value)
