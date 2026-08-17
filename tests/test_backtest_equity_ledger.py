from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd

from apps.backtest.bt_runner import (
    _record_equity_trade_ledger,
    _simulate_signal_lifecycle,
)
from libs.schemas.signals import SignalSide


def _signal(ts, code: str, side: SignalSide) -> SimpleNamespace:
    return SimpleNamespace(ts_end=ts, signal_code=code, side=side, option_hint={})


class _FakeDAO:
    def __init__(self) -> None:
        self.signals = []
        self.trades = []

    def record_signal(self, payload) -> None:
        self.signals.append(dict(payload))

    def record_trade(self, payload) -> None:
        self.trades.append(dict(payload))

    def fetch_vix_value(self, *, ts) -> Decimal:
        return Decimal("15")


def test_equity_ledger_writes_matching_buy_and_sell_rows() -> None:
    dao = _FakeDAO()
    entry = datetime(2026, 5, 22, 13, 30, tzinfo=timezone.utc)
    exit_ = datetime(2026, 5, 22, 14, 0, tzinfo=timezone.utc)

    trade = SimpleNamespace(
        symbol="AMD",
        signal_code="SIG_1030_REVERSAL_CALL_BUY",
        entry_ts=entry,
        exit_ts=exit_,
        entry_price=Decimal("100"),
        exit_price=Decimal("102"),
        exit_signal_code="SIG_1030_REVERSAL_TIME_EXIT",
    )

    _record_equity_trade_ledger(dao=dao, run_id=7, trade=trade)

    assert len(dao.trades) == 2
    buy, sell = dao.trades
    assert buy["side"] == "BUY"
    assert buy["asset_type"] == "EQUITY"
    assert buy["price"] == Decimal("100")
    assert buy["trade_ts"] == entry
    assert buy["reason_code"] == "SIG_1030_REVERSAL_CALL_BUY"
    assert buy["trace_id"] == sell["trace_id"]

    assert sell["side"] == "SELL"
    assert sell["asset_type"] == "EQUITY"
    assert sell["price"] == Decimal("102")
    assert sell["trade_ts"] == exit_
    assert sell["reason_code"] == "SIG_1030_REVERSAL_TIME_EXIT"
    assert sell["quantity"] == Decimal("1")
    assert sell["fees"] == Decimal("0")
    assert sell["slippage"] == Decimal("0")


def test_simulate_signal_lifecycle_rejects_no_position_sell() -> None:
    dao = _FakeDAO()
    frame = pd.DataFrame(columns=["close"])
    sell_ts = datetime(2026, 5, 22, 13, 32, tzinfo=timezone.utc)

    trades = _simulate_signal_lifecycle(
        dao=dao,
        symbol="AMD",
        frame=frame,
        signals=[_signal(sell_ts, "SIG_EXIT_BOX2MID", SignalSide.SELL)],
        run_id=9,
        signal_asset_type="OPTION",
        accumulator=None,
        lifecycle_log=None,
    )

    assert trades == []
    assert len(dao.signals) == 1
    recorded = dao.signals[0]
    assert recorded["accepted"] is False
    reason = json.loads(recorded["reason"])
    assert reason["status"] == "BLOCK"
    assert reason["reason"] == "BLOCK:EXIT_POSITION_NOT_FOUND"


def test_simulate_signal_lifecycle_returns_closed_equity_round_trip() -> None:
    dao = _FakeDAO()
    entry = datetime(2026, 5, 22, 13, 30, tzinfo=timezone.utc)
    exit_ = datetime(2026, 5, 22, 14, 0, tzinfo=timezone.utc)
    frame = pd.DataFrame(
        {"close": [Decimal("100"), Decimal("102")]},
        index=pd.DatetimeIndex([entry, exit_]),
    )

    trades = _simulate_signal_lifecycle(
        dao=dao,
        symbol="AMD",
        frame=frame,
        signals=[
            _signal(entry, "SIG_1030_REVERSAL_CALL_BUY", SignalSide.BUY),
            _signal(exit_, "SIG_1030_REVERSAL_TIME_EXIT", SignalSide.SELL),
        ],
        run_id=9,
        signal_asset_type="EQUITY",
        accumulator=None,
        lifecycle_log=None,
    )

    assert len(trades) == 1
    trade = trades[0]
    assert trade.entry_price == Decimal("100")
    assert trade.exit_price == Decimal("102")
    assert trade.signal_code == "SIG_1030_REVERSAL_CALL_BUY"
    assert trade.exit_signal_code == "SIG_1030_REVERSAL_TIME_EXIT"
