import os
import tempfile
from pathlib import Path


_health_db_fd, _health_db_path = tempfile.mkstemp(
    prefix="backtest-health-",
    suffix=".db",
)
os.close(_health_db_fd)

os.environ.setdefault("APP_ENV", "test")
os.environ["DATABASE_URL"] = f"sqlite:///{_health_db_path}"
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("IB_HOST", "127.0.0.1")
os.environ.setdefault("IB_PORT", "4002")
os.environ.setdefault("IB_CLIENT_ID", "1")
os.environ.setdefault("IB_ACCOUNT", "DU0000001")
os.environ.setdefault("PAPER", "1")
os.environ.setdefault("VIX_REQUIRED", "true")
os.environ.setdefault("RISK_NOTIONAL_CAP", "5000000")

from fastapi.testclient import TestClient  # noqa: E402

from sqlalchemy import create_engine  # noqa: E402

from libs.core import get_settings  # noqa: E402
from libs.db import Base  # noqa: E402

get_settings.cache_clear()  # type: ignore[attr-defined]

engine = create_engine(os.environ["DATABASE_URL"], future=True)
Base.metadata.create_all(bind=engine)

from apps.backtest.api import app as backtest_app  # noqa: E402
from apps.exec_svc.main import app as exec_app  # noqa: E402
from apps.risk_svc.main import app as risk_app  # noqa: E402
from apps.signal_svc.main import app as signal_app  # noqa: E402


def test_health_endpoints() -> None:
    services = [
        ("risk_svc", risk_app),
        ("exec_svc", exec_app),
        ("signal_svc", signal_app),
        ("backtest_api", backtest_app),
    ]

    for service_name, app in services:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/healthz")
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["code"] == "OK"
        assert payload["data"]["service"] == service_name

    Path(_health_db_path).unlink(missing_ok=True)
