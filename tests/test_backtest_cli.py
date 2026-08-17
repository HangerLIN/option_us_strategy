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
    assert all(entry["option_pricing_mode"] == "full_feed" for entry in run_calls)


def test_main_skips_symbols_missing_option_data(monkeypatch):
    trade_dates = [date(2024, 1, 2)]
    run_calls: list[str] = []

    class DummySession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def commit(self):
            pass

        def rollback(self):
            pass

    monkeypatch.setattr(backtest_main, "create_engine", lambda url: object())
    monkeypatch.setattr(backtest_main, "sessionmaker", lambda bind: lambda: DummySession())

    class DummyBacktestDAO:
        def __init__(self, session, option_bar_table, option_chain_table):
            pass

        def fetch_trade_dates_between(self, *, start_date, end_date):
            return trade_dates

    monkeypatch.setattr(backtest_main, "BacktestDAO", DummyBacktestDAO)

    class DummyUniverse:
        code = "ref_market_cap:GLOBAL"
        symbols = ["NOW", "AAPL", "MSFT"]
        metadata: Dict[str, Any] = {}

    class DummyResolver:
        def __init__(self, session):
            pass

        def resolve(self, spec):
            return DummyUniverse()

    monkeypatch.setattr(backtest_main, "UniverseResolver", DummyResolver)

    class DummyBuilder:
        def __init__(self, session, *, preearn_days_min, preearn_days_max, preearn_atr_pct_max):
            pass

        def build_for_date(self, **kwargs):
            return [
                {"symbol": "NOW", "ret_0925_0930": Decimal("0.05")},
                {"symbol": "AAPL", "ret_0925_0930": Decimal("0.03")},
                {"symbol": "MSFT", "ret_0925_0930": Decimal("0.02")},
            ]

    monkeypatch.setattr(backtest_main, "PremarketTop5Builder", DummyBuilder)

    def fake_run_backtest(**kwargs):
        symbol = kwargs["symbol"]
        run_calls.append(symbol)
        if symbol == "NOW":
            raise RuntimeError("No option contracts available for NOW on 2024-01-02")
        if symbol == "AAPL":
            raise RuntimeError("No option L1 data for AAPL signal contracts")
        return 42

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
            "2024-01-02T16:00:00-05:00",
        ]
    )

    assert result == 0
    assert run_calls == ["NOW", "AAPL", "MSFT"]


def test_main_passes_v8_research_modes_to_backtest(monkeypatch):
    trade_dates = [date(2024, 1, 2)]
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

    monkeypatch.setattr(backtest_main, "create_engine", lambda url: object())
    monkeypatch.setattr(backtest_main, "sessionmaker", lambda bind: lambda: DummySession())

    class DummyBacktestDAO:
        def __init__(self, session, option_bar_table, option_chain_table):
            pass

        def fetch_trade_dates_between(self, *, start_date, end_date):
            return trade_dates

    monkeypatch.setattr(backtest_main, "BacktestDAO", DummyBacktestDAO)

    class DummyUniverse:
        code = "ref_market_cap:GLOBAL"
        symbols = ["AAPL"]
        metadata: Dict[str, Any] = {}

    class DummyResolver:
        def __init__(self, session):
            pass

        def resolve(self, spec):
            return DummyUniverse()

    monkeypatch.setattr(backtest_main, "UniverseResolver", DummyResolver)

    class DummyBuilder:
        def __init__(self, session, *, preearn_days_min, preearn_days_max, preearn_atr_pct_max):
            pass

        def build_for_date(self, **kwargs):
            return [{"symbol": "AAPL", "ret_0925_0930": Decimal("0.03")}]

    monkeypatch.setattr(backtest_main, "PremarketTop5Builder", DummyBuilder)
    monkeypatch.setattr(backtest_main, "run_backtest", lambda **kwargs: run_calls.append(kwargs) or len(run_calls))

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

    for mode in (
        "v8a",
        "v8b",
        "v8_ab",
        "v8b2_loose",
        "v8b2",
        "v8b2_room040",
        "v8b2_strict",
        "v8b3",
        "v8_guard_filter",
        "v8_guard_filter_followthrough",
        "v8_guard_rsi_floor",
        "v8_guard_rsi_floor_core_room_025",
        "v8_guard_rsi_floor_core_room_035",
        "v8_guard_continuation",
        "v8_guard_continuation_v2",
        "v8_guard_scalp_observe",
        "v8_guard_stop",
    ):
        run_calls.clear()
        result = backtest_main.main(
            [
                "--start",
                "2024-01-02T09:30:00-05:00",
                "--end",
                "2024-01-02T16:00:00-05:00",
                "--symbols",
                "AAPL",
                "--mode",
                mode,
                "--track",
                "equity",
            ]
        )
        assert result == 0
        assert run_calls[0]["strategy_variant"] == mode
        assert run_calls[0]["track"] == "equity"


def test_main_passes_event_time_option_pricing_mode(monkeypatch):
    trade_dates = [date(2024, 1, 2)]
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

    monkeypatch.setattr(backtest_main, "create_engine", lambda url: object())
    monkeypatch.setattr(backtest_main, "sessionmaker", lambda bind: lambda: DummySession())

    class DummyBacktestDAO:
        def __init__(self, session, option_bar_table, option_chain_table):
            pass

        def fetch_trade_dates_between(self, *, start_date, end_date):
            return trade_dates

    monkeypatch.setattr(backtest_main, "BacktestDAO", DummyBacktestDAO)

    class DummyUniverse:
        code = "ref_market_cap:GLOBAL"
        symbols = ["AAPL"]
        metadata: Dict[str, Any] = {}

    class DummyResolver:
        def __init__(self, session):
            pass

        def resolve(self, spec):
            return DummyUniverse()

    monkeypatch.setattr(backtest_main, "UniverseResolver", DummyResolver)

    class DummyBuilder:
        def __init__(self, session, *, preearn_days_min, preearn_days_max, preearn_atr_pct_max):
            pass

        def build_for_date(self, **kwargs):
            return [{"symbol": "AAPL", "ret_0925_0930": Decimal("0.03")}]

    monkeypatch.setattr(backtest_main, "PremarketTop5Builder", DummyBuilder)
    monkeypatch.setattr(backtest_main, "run_backtest", lambda **kwargs: run_calls.append(kwargs) or 1)

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
            "2024-01-02T16:00:00-05:00",
            "--symbols",
            "AAPL",
            "--track",
            "option",
            "--option-pricing-mode",
            "event_time",
        ]
    )

    assert result == 0
    assert run_calls[0]["option_pricing_mode"] == "event_time"
