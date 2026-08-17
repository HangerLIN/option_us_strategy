from __future__ import annotations

from datetime import date
from decimal import Decimal

from apps.backtest.pipeline.premarket import PremarketTop5Builder


class _StubResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _StubSession:
    def __init__(self):
        self.calls = []

    def execute(self, stmt, params):
        sql = str(stmt)
        self.calls.append((sql, dict(params)))
        if "FROM bars1m_equity" in sql:
            return _StubResult((Decimal("123.45"),))
        return _StubResult(None)


def test_fallback_prev_close_uses_bars_when_daily_snapshot_missing():
    session = _StubSession()
    builder = PremarketTop5Builder(
        session,
        preearn_days_min=3,
        preearn_days_max=5,
        preearn_atr_pct_max=Decimal("0.02"),
    )

    value = builder._fallback_prev_close("AAPL", date(2026, 6, 9))

    assert value == Decimal("123.450000")
    assert any("FROM bars1m_equity" in sql for sql, _ in session.calls)


def test_load_daily_metrics_uses_latest_prior_daily_rows():
    class _Session:
        def __init__(self):
            self.calls = []
            self._idx = 0

        def execute(self, stmt, params):
            sql = str(stmt)
            self.calls.append((sql, dict(params)))
            self._idx += 1
            if self._idx == 1:
                return type("R", (), {"fetchall": lambda self_: [("AAPL", Decimal("307.88"))]})()
            if self._idx == 2:
                return type(
                    "R",
                    (),
                    {"fetchall": lambda self_: [("AAPL", Decimal("303.54"), Decimal("0.28"))]},
                )()
            return type("R", (), {"fetchone": lambda self_: None})()

    session = _Session()
    builder = PremarketTop5Builder(
        session,
        preearn_days_min=3,
        preearn_days_max=5,
        preearn_atr_pct_max=Decimal("0.02"),
    )

    metrics = builder._load_daily_metrics(date(2026, 6, 9), ["AAPL"])

    assert metrics["AAPL"]["prev_close"] == Decimal("307.880000")
    assert metrics["AAPL"]["close_rth"] == Decimal("307.880000")
    assert metrics["AAPL"]["sma60"] == Decimal("303.540000")
    assert metrics["AAPL"]["sma60_slope"] == Decimal("0.280000")
    assert "trade_date_et::date < :trade_date" in session.calls[0][0]
    assert "trade_date_et::date < :trade_date" in session.calls[1][0]
