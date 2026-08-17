from __future__ import annotations

from datetime import date, datetime, time
from types import MethodType

from libs.core import EASTERN
import libs.infra.ibkr_client as ibkr_client
from libs.infra.ibkr_client import IBClient


def _fake_session_window(session_date: date, *, tz=None):
    zone = tz or EASTERN
    return (
        datetime.combine(session_date, time(9, 30), tzinfo=EASTERN).astimezone(zone),
        datetime.combine(session_date, time(16, 0), tzinfo=EASTERN).astimezone(zone),
    )


def test_req_historical_1m_equity_uses_extended_window_when_rth_disabled(monkeypatch) -> None:
    client = object.__new__(IBClient)
    captured: dict[str, object] = {}
    monkeypatch.setattr(ibkr_client, "trading_session_window", _fake_session_window)

    def fake_req_historical_1m(self, symbol, start, end, use_rth, **kwargs):
        captured["symbol"] = symbol
        captured["start"] = start
        captured["end"] = end
        captured["use_rth"] = use_rth
        captured["kwargs"] = kwargs
        return []

    client.req_historical_1m = MethodType(fake_req_historical_1m, client)

    IBClient.req_historical_1m_equity(client, "AAPL", date(2026, 5, 22), rth_only=False)

    assert captured["symbol"] == "AAPL"
    assert captured["use_rth"] is False
    assert captured["start"].astimezone(EASTERN).time() == time(8, 0)
    assert captured["end"].astimezone(EASTERN).time() == time(16, 1)


def test_req_historical_1m_equity_keeps_rth_window_when_enabled(monkeypatch) -> None:
    client = object.__new__(IBClient)
    captured: dict[str, object] = {}
    monkeypatch.setattr(ibkr_client, "trading_session_window", _fake_session_window)

    def fake_req_historical_1m(self, symbol, start, end, use_rth, **kwargs):
        captured["start"] = start
        captured["end"] = end
        captured["use_rth"] = use_rth
        return []

    client.req_historical_1m = MethodType(fake_req_historical_1m, client)

    IBClient.req_historical_1m_equity(client, "AAPL", date(2026, 5, 22), rth_only=True)

    assert captured["use_rth"] is True
    assert captured["start"].astimezone(EASTERN).time() == time(9, 30)
    assert captured["end"].astimezone(EASTERN).time() == time(16, 1)
