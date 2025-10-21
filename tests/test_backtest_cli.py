from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Dict

import apps.backtest.main as backtest_main


def test_main_runs_without_explicit_symbols(monkeypatch):
    trade_dates = [date(2024, 1, 2), date(2024, 1, 3)]
    build_calls: list[Dict[str, Any]] = []
    run_calls: list[Dict[str, Any]] = []

    class DummySession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def commit(self):
            pass

        def rollback(self):
            pass

    def fake_create_engine(url: str):
        return object()

    def fake_sessionmaker(bind) -> Any:
        def _factory():
            return DummySession()

        return _factory

    monkeypatch.setattr(backtest_main, "create_engine", fake_create_engine)
    monkeypatch.setattr(backtest_main, "sessionmaker", fake_sessionmaker)

    class DummyBacktestDAO:
        def __init__(self, session, option_bar_table, option_chain_table):
            assert option_bar_table
            assert option_chain_table

        def fetch_trade_dates_between(self, *, start_date, end_date):
            assert start_date <= end_date
            return trade_dates

    monkeypatch.setattr(backtest_main, "BacktestDAO", DummyBacktestDAO)

    class DummyUniverse:
        code = "ref_market_cap:GLOBAL"
        symbols = ["NVDA", "MSFT"]
        metadata: Dict[str, Any] = {}

    class DummyResolver:
        def __init__(self, session):
            pass

        def resolve(self, spec):
            return DummyUniverse()

    monkeypatch.setattr(backtest_main, "UniverseResolver", DummyResolver)

    class DummyBuilder:
        def __init__(self, session, *, preearn_days_min, preearn_days_max, preearn_atr_pct_max):
            assert preearn_days_min == 3
            assert preearn_days_max == 5
            assert Decimal(str(preearn_atr_pct_max)) == Decimal("0.02")

        def build_for_date(self, **kwargs):
            build_calls.append(kwargs)
            if kwargs["trade_date"] == trade_dates[0]:
                return [{"symbol": "NVDA", "ret_0925_0930": Decimal("0.05")}]
            return [{"symbol": "MSFT", "ret_0925_0930": Decimal("0.03")}]

    monkeypatch.setattr(backtest_main, "PremarketTop5Builder", DummyBuilder)

    def fake_run_backtest(**kwargs):
        run_calls.append(kwargs)
        return len(run_calls)

    monkeypatch.setattr(backtest_main, "run_backtest", fake_run_backtest)

    class DummySettings:
        database_url = "postgresql://localhost/test"
        option_bar_table = "bars1m_option"
        option_chain_table = "option_chain_meta"
        preearn_days_min = 3
        preearn_days_max = 5
        preearn_atr_pct_max = Decimal("0.02")
        risk_service_url = "http://localhost"
        redis_url = "redis://localhost"

    monkeypatch.setattr(backtest_main, "get_settings", lambda: DummySettings())
    monkeypatch.setattr(backtest_main, "configure_logging", lambda settings: None)

    result = backtest_main.main(
        [
            "--start",
            "2024-01-02T09:30:00-05:00",
            "--end",
            "2024-01-05T16:00:00-05:00",
        ]
    )

    assert result == 0
    assert len(build_calls) == len(trade_dates)
    assert [call["symbols"] for call in build_calls] == [["NVDA", "MSFT"], ["NVDA", "MSFT"]]
    assert [entry["symbol"] for entry in run_calls] == ["NVDA", "MSFT"]
