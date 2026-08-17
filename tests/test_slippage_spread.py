from __future__ import annotations

from decimal import Decimal

from apps.backtest.broker.slippage_spread import build_execution_path
from libs.schemas.exec import ExecutionMode, OrderSide


def test_adaptive_buy_starts_at_mid_then_chases_to_marketable() -> None:
    path = build_execution_path(
        mode=ExecutionMode.ADAPTIVE,
        side=OrderSide.BUY,
        min_tick=Decimal("0.05"),
        mid=Decimal("21.60"),
        spread=Decimal("3.20"),
        bid=Decimal("20.00"),
        ask=Decimal("23.20"),
    )

    assert [step.stage for step in path.steps] == [
        "mid",
        "mid-20pct-spread",
        "mid-40pct-spread",
        "marketable",
    ]
    assert path.steps[0].price == Decimal("21.60")
    assert path.steps[-1].price >= Decimal("23.20")


def test_adaptive_sell_starts_at_mid_then_chases_to_marketable() -> None:
    path = build_execution_path(
        mode=ExecutionMode.ADAPTIVE,
        side=OrderSide.SELL,
        min_tick=Decimal("0.05"),
        mid=Decimal("21.60"),
        spread=Decimal("3.20"),
        bid=Decimal("20.00"),
        ask=Decimal("23.20"),
    )

    assert path.steps[0].price == Decimal("21.60")
    assert path.steps[-1].price <= Decimal("20.00")


def test_marketable_buy_crosses_to_ask_or_better() -> None:
    path = build_execution_path(
        mode=ExecutionMode.MARKETABLE,
        side=OrderSide.BUY,
        min_tick=Decimal("0.05"),
        mid=Decimal("21.60"),
        spread=Decimal("3.20"),
        bid=Decimal("20.00"),
        ask=Decimal("23.20"),
    )
    assert len(path.steps) == 1
    assert path.steps[0].price >= Decimal("23.20")


def test_marketable_sell_crosses_to_bid_or_better() -> None:
    path = build_execution_path(
        mode=ExecutionMode.MARKETABLE,
        side=OrderSide.SELL,
        min_tick=Decimal("0.05"),
        mid=Decimal("21.60"),
        spread=Decimal("3.20"),
        bid=Decimal("20.00"),
        ask=Decimal("23.20"),
    )
    assert len(path.steps) == 1
    assert path.steps[0].price <= Decimal("20.00")
