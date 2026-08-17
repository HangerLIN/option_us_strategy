from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from apps.backtest.strategy.option_signal_strategy import (
    ContractSelection,
    OptionSignalStrategy,
)
from libs.schemas.exec import ExecutionMode, OrderSide


def _strategy(anchor: str) -> OptionSignalStrategy:
    strategy = OptionSignalStrategy.__new__(OptionSignalStrategy)
    strategy._fill_time_anchor = anchor
    strategy._cached_bar_interval_seconds = 60.0
    return strategy


def _contract() -> ContractSelection:
    return ContractSelection(
        conid=1,
        symbol="NOW",
        expiry=datetime(2026, 5, 29, tzinfo=timezone.utc),
        strike=Decimal("103"),
        right="CALL",
        bid=Decimal("3.80"),
        ask=Decimal("4.10"),
        mid=Decimal("3.95"),
        min_tick=Decimal("0.01"),
        option_right="CALL",
        open_interest=0,
        volume=0,
        dte=7,
    )


def test_feed_aware_anchors_use_marketable_entry_mode() -> None:
    assert _strategy("next_bar_close")._entry_execution_mode() is ExecutionMode.MARKETABLE
    assert _strategy("next_bar_open")._entry_execution_mode() is ExecutionMode.MARKETABLE
    assert _strategy("legacy")._entry_execution_mode() is ExecutionMode.ADAPTIVE


def test_feed_aware_entry_path_posts_ceiling_above_ask() -> None:
    path = _strategy("next_bar_close")._feed_aware_entry_path(_contract())

    assert path.ttl_seconds == 120
    assert path.steps[0].price == Decimal("8.20")
    assert path.steps[0].price > _contract().ask


def test_anchored_fill_time_shifts_only_open_anchor_on_buy() -> None:
    current_dt = datetime(2026, 5, 22, 13, 49, tzinfo=timezone.utc)
    buy = SimpleNamespace(side=OrderSide.BUY)
    sell = SimpleNamespace(side=OrderSide.SELL)

    assert _strategy("next_bar_close")._anchored_fill_time(buy, current_dt) == current_dt
    assert (
        _strategy("next_bar_open")._anchored_fill_time(buy, current_dt)
        == current_dt - timedelta(seconds=60)
    )
    assert _strategy("next_bar_open")._anchored_fill_time(sell, current_dt) == current_dt
