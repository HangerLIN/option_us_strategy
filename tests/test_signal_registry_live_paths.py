from __future__ import annotations

import ast
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from apps.signal_svc.engine import SignalEngine
from libs.schemas.signals import (
    BUY_SIGNAL_CODES,
    SignalEnvelope,
    SignalPushItem,
    SignalSide,
    is_sell_signal,
)


def _load_handle_signal_payload():
    source_path = Path(__file__).resolve().parents[1] / "apps" / "exec_svc" / "main.py"
    module_ast = ast.parse(source_path.read_text(encoding="utf-8"))
    function_node = next(
        node
        for node in module_ast.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_handle_signal_payload"
    )
    ast.fix_missing_locations(function_node)
    calls: list[tuple[str, str, str]] = []

    async def fake_force_close(strategy_code: str, symbol: str, trace_id: str) -> None:
        calls.append((strategy_code, symbol, trace_id))

    async def fake_buy_signal(payload: Dict[str, object], trace_id: str) -> None:
        calls.append(("BUY", str(payload.get("symbol")), trace_id))

    namespace = {
        "Dict": Dict,
        "is_sell_signal": is_sell_signal,
        "_force_close_symbol": fake_force_close,
        "_handle_buy_signal": fake_buy_signal,
    }
    exec(compile(ast.Module(body=[function_node], type_ignores=[]), str(source_path), "exec"), namespace)
    return namespace["_handle_signal_payload"], calls


def test_exec_signal_payload_closes_registered_sell_signals() -> None:
    handler, calls = _load_handle_signal_payload()

    for signal_code in (
        "SIG_OVERNIGHT_GAP_EXIT",
        "SIG_AM_SELL_C1",
        "SIG_AM_CONFLUENCE_SELL_S2",
    ):
        asyncio.run(
            handler(
                {
                    "strategy_code": "core-vol",
                    "symbol": "AAPL",
                    "side": "BUY",
                    "signal_code": signal_code,
                    "trace_id": f"trace-{signal_code}",
                }
            )
        )

    assert calls == [
        ("core-vol", "AAPL", "trace-SIG_OVERNIGHT_GAP_EXIT"),
        ("core-vol", "AAPL", "trace-SIG_AM_SELL_C1"),
        ("core-vol", "AAPL", "trace-SIG_AM_CONFLUENCE_SELL_S2"),
    ]


def test_exec_signal_payload_closes_explicit_sell_side() -> None:
    handler, calls = _load_handle_signal_payload()

    asyncio.run(
        handler(
            {
                "strategy_code": "core-vol",
                "symbol": "MSFT",
                "side": "SELL",
                "signal_code": "SIG_CUSTOM_EXIT",
                "trace_id": "trace-sell",
            }
        )
    )

    assert calls == [("core-vol", "MSFT", "trace-sell")]


def test_risk_service_buy_registry_includes_am_signals() -> None:
    from apps.risk_svc.service import RiskService

    assert RiskService._BUY_SIGNALS is BUY_SIGNAL_CODES
    assert "SIG_AM_BOTTOM_A1" in RiskService._BUY_SIGNALS
    assert "SIG_AM_CONFLUENCE_BUY_A2" in RiskService._BUY_SIGNALS


class _RowsResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _OneResult:
    def __init__(self, value):
        self._value = value

    def fetchone(self):
        if self._value is None:
            return None
        return (self._value,)


class _OptionOpenSession:
    def __init__(self, current_rows, open_price):
        self._current_rows = current_rows
        self._open_price = open_price
        self.calls = 0

    def execute(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 1:
            return _RowsResult(self._current_rows)
        return _OneResult(self._open_price)


def _option_row(current_price: str) -> dict[str, object]:
    return {
        "conid": 1001,
        "expiry": datetime(2025, 1, 8, tzinfo=timezone.utc),
        "strike": Decimal("100"),
        "current_price": Decimal(current_price),
        "bid": Decimal("1.95"),
        "ask": Decimal("2.05"),
        "volume": 500,
        "open_interest": 1000,
    }


def test_option_price_exceeds_open_compares_selected_call_price() -> None:
    engine = SignalEngine.__new__(SignalEngine)
    ts_end = datetime(2025, 1, 2, 20, 0, tzinfo=timezone.utc)

    session = _OptionOpenSession([_option_row("2.00")], Decimal("1.50"))
    assert engine._option_price_exceeds_open(session, "AAPL", ts_end.date(), ts_end)

    session = _OptionOpenSession([_option_row("1.25")], Decimal("1.50"))
    assert not engine._option_price_exceeds_open(session, "AAPL", ts_end.date(), ts_end)


def test_option_price_exceeds_open_fails_open_when_data_missing() -> None:
    engine = SignalEngine.__new__(SignalEngine)
    ts_end = datetime(2025, 1, 2, 20, 0, tzinfo=timezone.utc)

    session = _OptionOpenSession([], Decimal("1.50"))
    assert not engine._option_price_exceeds_open(session, "AAPL", ts_end.date(), ts_end)

    session = _OptionOpenSession([_option_row("2.00")], None)
    assert not engine._option_price_exceeds_open(session, "AAPL", ts_end.date(), ts_end)


def test_option_price_exceeds_open_uses_underlying_symbol_column() -> None:
    db = create_engine("sqlite:///:memory:", future=True)
    with Session(db, future=True) as session:
        session.execute(
            text(
                """
                CREATE TABLE bars1m_option (
                    conid INTEGER,
                    ts_end TIMESTAMP,
                    underlying_symbol TEXT,
                    expiry DATE,
                    strike NUMERIC,
                    "right" TEXT,
                    bid NUMERIC,
                    ask NUMERIC,
                    mid NUMERIC,
                    last NUMERIC,
                    volume INTEGER,
                    open_interest INTEGER,
                    delta NUMERIC,
                    underlying_price NUMERIC
                )
                """
            )
        )
        trade_date = datetime(2025, 1, 2, tzinfo=timezone.utc).date()
        expiry = "2025-01-08"
        session.execute(
            text(
                """
                INSERT INTO bars1m_option (
                    conid, ts_end, underlying_symbol, expiry, strike, "right",
                    bid, ask, mid, last, volume, open_interest, delta, underlying_price
                ) VALUES (
                    1001, :open_ts, 'AAPL', :expiry, 105, 'CALL',
                    1.45, 1.55, 1.50, NULL, 1000, 2000, 0.40, 100
                ), (
                    1001, :current_ts, 'AAPL', :expiry, 105, 'CALL',
                    1.95, 2.05, 2.00, NULL, 1000, 2000, 0.40, 100
                )
                """
            ),
            {
                "open_ts": datetime(2025, 1, 2, 14, 31, tzinfo=timezone.utc),
                "current_ts": datetime(2025, 1, 2, 20, 0, tzinfo=timezone.utc),
                "expiry": expiry,
            },
        )
        session.commit()

        engine = SignalEngine.__new__(SignalEngine)
        engine._option_symbol_columns = {}

        assert engine._option_price_exceeds_open(
            session,
            "AAPL",
            trade_date,
            datetime(2025, 1, 2, 20, 0, tzinfo=timezone.utc),
        )


def test_backtest_can_open_position_runs_data_filters() -> None:
    engine = SignalEngine.__new__(SignalEngine)
    engine._is_backtest = True
    engine._metrics_enabled = False
    engine._preearn_days_min = 0
    engine._preearn_days_max = 0
    engine._preearn_atr_pct_max = Decimal("0")
    engine._positions = {}
    engine._settings = SimpleNamespace(max_concurrent_top=10)
    engine._daily_ma60_value = lambda *args, **kwargs: Decimal("101")

    df = pd.DataFrame(
        [
            {
                "close": Decimal("100"),
                "ao": Decimal("1"),
                "obv": Decimal("1"),
                "rvol6": Decimal("1"),
            }
        ]
    )

    allowed, addition = engine._can_open_position(
        "AAPL",
        df=df,
        session=object(),
        ts_end=datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc),
    )

    assert (allowed, addition) == (False, False)


class _PushSession:
    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def test_push_signals_passes_context_to_can_open_position() -> None:
    engine = SignalEngine.__new__(SignalEngine)
    session = _PushSession()
    ts_end = datetime(2025, 1, 2, 20, 0, tzinfo=timezone.utc)
    df = pd.DataFrame([{"close": Decimal("100")}])
    calls = []

    engine._session_factory = lambda: session
    engine._history_window = 180
    history_calls = []

    def load_history(*args, **kwargs):
        history_calls.append((args, kwargs))
        return df

    engine._load_history = load_history

    def can_open(symbol, passed_df=None, passed_session=None, passed_ts=None):
        calls.append((symbol, passed_df, passed_session, passed_ts))
        return False, False

    engine._can_open_position = can_open
    signal = SignalEnvelope(
        strategy_code="core-vol",
        symbol="AAPL",
        signal_code="SIG_AM_BOTTOM_A1",
        side=SignalSide.BUY,
        confidence=0.9,
        generated_at=ts_end,
    )

    result = engine.push_signals([SignalPushItem(signal=signal)])

    assert result[0]["accepted"] is False
    assert result[0]["reason"] == "portfolio_limit"
    assert len(calls) == 1
    assert calls[0][0] == "AAPL"
    assert calls[0][1] is df
    assert calls[0][2] is session
    assert calls[0][3] == ts_end
    assert history_calls[0][1] == {"limit": 180}
