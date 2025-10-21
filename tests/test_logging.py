from fastapi import FastAPI
from fastapi.testclient import TestClient

from libs.core import (
    TRACE_HEADER,
    TraceContextMiddleware,
    attach_trace_metadata,
    bind_trace_context,
    clear_trace_context,
    current_trace_context,
    update_trace_context,
)


def test_attach_trace_metadata_enriches_payload() -> None:
    clear_trace_context()
    trace_id = bind_trace_context(trace_id="trace-123", symbol="AAPL")
    update_trace_context(signal_code="strat-01")
    enriched = attach_trace_metadata({"event": "test"})
    assert enriched["trace_id"] == trace_id
    assert enriched["symbol"] == "AAPL"
    assert enriched["signal_code"] == "strat-01"
    assert enriched["event"] == "test"
    clear_trace_context()


def test_trace_context_middleware_propagates_headers() -> None:
    app = FastAPI()
    app.add_middleware(TraceContextMiddleware)

    @app.get("/inspect")
    def inspect():  # type: ignore[return-value]
        return current_trace_context()

    headers = {
        TRACE_HEADER: "abc-123",
        "x-symbol": "MSFT",
        "x-signal-code": "sig-01",
    }

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/inspect", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["trace_id"] == "abc-123"
    assert payload["symbol"] == "MSFT"
    assert payload["signal_code"] == "sig-01"
