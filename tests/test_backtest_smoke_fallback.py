from __future__ import annotations

import importlib.util
import sys
from argparse import Namespace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import apps.backtest.dao as backtest_dao


def _load_smoke_module():
    root = Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "run_backtest_smoke.py"
    spec = importlib.util.spec_from_file_location("run_backtest_smoke_test", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_select_complete_symbols_skips_incomplete_candidates(monkeypatch, tmp_path):
    smoke = _load_smoke_module()

    args = Namespace(
        symbols_limit=1,
        contracts_cache_dir=str(tmp_path),
        min_equity_coverage=0.85,
        min_option_coverage=0.77,
    )

    class DummyBacktestDAO:
        def __init__(self, session, option_bar_table, option_chain_table):
            pass

    class DummySession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummySettings:
        option_bar_table = "bars1m_option"
        option_chain_table = "option_chain_meta"

    monkeypatch.setattr(backtest_dao, "BacktestDAO", DummyBacktestDAO)
    monkeypatch.setattr(
        smoke,
        "_candidate_symbols_for_trade_date",
        lambda *args, **kwargs: ["RGTI", "AMD", "NVDA"],
    )
    monkeypatch.setattr(
        smoke,
        "_session_bounds",
        lambda trade_date: (
            datetime(2026, 5, 22, 13, 30, tzinfo=timezone.utc),
            datetime(2026, 5, 22, 20, 1, tzinfo=timezone.utc),
        ),
    )

    def fake_readiness(settings, session_factory, trade_date, symbol, **kwargs):
        if symbol == "RGTI":
            return False, "RGTI: missing cached contracts"
        return True, symbol

    monkeypatch.setattr(smoke, "_symbol_has_complete_local_data", fake_readiness)

    selected, reasons = smoke._select_complete_symbols_for_trade_date(
        args,
        DummySettings(),
        lambda: DummySession(),
        date(2026, 5, 22),
    )

    assert selected == ["AMD"]
    assert reasons == ["RGTI: missing cached contracts"]


def test_main_falls_back_to_latest_complete_trade_date(monkeypatch):
    smoke = _load_smoke_module()

    candidate_dates = [
        date(2026, 5, 27),
        date(2026, 5, 26),
        date(2026, 5, 22),
    ]
    process_calls: list[tuple[date, list[str] | None]] = []

    class DummySettings:
        pass

    summary = smoke.SmokeRunSummary(
        trade_date=date(2026, 5, 22),
        start_utc=datetime(2026, 5, 22, 13, 30, tzinfo=timezone.utc),
        end_utc=datetime(2026, 5, 22, 20, 1, tzinfo=timezone.utc),
        contract_file=Path(".cache/contracts_2026-05-22_AMD.json"),
        symbols=[],
        run_ids={"AMD": 158},
    )
    args = Namespace(
        env_file=".env",
        print_ib_config=False,
        lookback_days=6,
        max_retries=1,
        symbols_limit=1,
        contracts_cache_dir=".cache",
        min_equity_coverage=0.85,
        min_option_coverage=0.77,
        option_market_data_type=4,
        auto_ingest=False,
    )

    monkeypatch.setattr(smoke, "_parse_args", lambda: args)
    monkeypatch.setattr(smoke, "_load_env_file", lambda path: None)
    monkeypatch.setattr(smoke, "get_settings", lambda: DummySettings())
    monkeypatch.setattr(smoke, "configure_logging", lambda settings: None)
    monkeypatch.setattr(smoke, "_validate_environment", lambda: None)
    monkeypatch.setattr(smoke, "get_session_factory", lambda settings: object())
    monkeypatch.setattr(smoke, "_candidate_trade_dates", lambda session_factory, lookback: candidate_dates)
    monkeypatch.setattr(
        smoke,
        "_find_latest_complete_trade_date",
        lambda args, settings, session_factory, trade_dates: smoke.TradeDatePlan(
            trade_date=date(2026, 5, 22),
            symbols=["AMD"],
        ),
    )

    def fake_process_trade_date(
        args: Namespace,
        settings: Any,
        session_factory: Any,
        trade_date: date,
        *,
        symbols_override=None,
    ):
        process_calls.append((trade_date, list(symbols_override) if symbols_override else None))
        return summary

    monkeypatch.setattr(smoke, "_process_trade_date", fake_process_trade_date)
    monkeypatch.setattr(smoke, "_print_summary", lambda summary: None)
    monkeypatch.setattr(smoke, "_exercise_api", lambda summary: None)

    result = smoke.main()

    assert result == 0
    assert process_calls == [(date(2026, 5, 22), ["AMD"])]


def test_resolve_cached_contracts_accepts_multi_symbol_cache(tmp_path):
    smoke = _load_smoke_module()
    cache_file = tmp_path / "contracts_2026-05-22_opening_top5.json"
    cache_file.write_text(
        """
[
  {"symbol": "AMD", "option_right": "CALL", "expiry": "20260529", "strike": 470.0, "conid": 1, "min_tick": 0.01},
  {"symbol": "AMD", "option_right": "PUT", "expiry": "20260529", "strike": 467.5, "conid": 2, "min_tick": 0.01},
  {"symbol": "ANET", "option_right": "CALL", "expiry": "20260529", "strike": 155.0, "conid": 3, "min_tick": 0.01}
]
        """.strip(),
        encoding="utf-8",
    )

    path, descriptors = smoke._resolve_cached_contracts(
        tmp_path,
        date(2026, 5, 22),
        ["AMD"],
    )

    assert path == cache_file
    assert [desc.symbol for desc in descriptors] == ["AMD", "AMD"]
