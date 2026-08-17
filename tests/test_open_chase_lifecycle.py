from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import backtrader as bt

from apps.backtest.strategy.option_signal_strategy import (
    ContractSelection,
    ManagedOrder,
    OptionSignalStrategy,
)
from libs.execution.pricing import ExecutionPath, PriceStep
from libs.schemas.exec import ExecutionMode, OrderSide
from libs.schemas.signals import SignalEnvelope, SignalSide


def _signal(generated_at: datetime) -> SignalEnvelope:
    return SignalEnvelope(
        strategy_code="core-vol",
        symbol="NOW",
        signal_code="SIG_OPEN_CHASE_BUY",
        side=SignalSide.BUY,
        confidence=1.0,
        reason={},
        risk_hint={"size": 1},
        option_hint={},
        ttl_seconds=180,
        cooldown_seconds=600,
        generated_at=generated_at,
    )


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
        quote_ts=datetime(2026, 5, 22, 13, 49, tzinfo=timezone.utc),
    )


def _managed(signal_time: datetime) -> ManagedOrder:
    signal = _signal(signal_time)
    contract = _contract()
    return ManagedOrder(
        order=SimpleNamespace(status=1),
        path=ExecutionPath([PriceStep(0, Decimal("3.95"), "mid")], ttl_seconds=30),
        next_step=1,
        start_dt=signal_time,
        submit_dt=signal_time,
        attempts=0,
        signal=signal,
        contract=contract,
        side=OrderSide.BUY,
        mode=ExecutionMode.ADAPTIVE,
        trace_id="trace",
        signal_context={
            "underlying_price": Decimal("102.03"),
            "vwap": Decimal("102.00"),
            "above_vwap": True,
            "above_orh": False,
            "option_bid": Decimal("2.55"),
            "option_ask": Decimal("3.60"),
            "option_mid": Decimal("3.075"),
        },
        original_limit_price=Decimal("3.95"),
        current_limit_price=Decimal("4.10"),
    )


def _strategy() -> OptionSignalStrategy:
    strategy = OptionSignalStrategy.__new__(OptionSignalStrategy)
    strategy._stale_guard_enabled = True
    strategy._settings = SimpleNamespace(
        open_chase_max_signal_to_fill_seconds=90,
        open_chase_require_above_vwap_at_fill=True,
        open_chase_require_above_orh_at_fill=False,
        open_chase_reject_option_spread_pct_gt=Decimal("0.10"),
        open_chase_max_underlying_adverse_move_pct=Decimal("0.0025"),
    )
    return strategy


def _ttl_strategy(managed: ManagedOrder) -> tuple[OptionSignalStrategy, list[dict]]:
    strategy = OptionSignalStrategy.__new__(OptionSignalStrategy)
    strategy._stale_guard_enabled = False
    strategy._pending_orders = {managed.order.ref: managed}
    events: list[dict] = []

    strategy.cancel = lambda order: None
    strategy._refresh_contract_from_feed = lambda contract, current_dt: contract
    strategy._build_path = lambda signal, contract, mode: ExecutionPath(
        [PriceStep(0, Decimal("3.80"), mode.value)], ttl_seconds=30
    )

    refs = iter([managed.order.ref + 100])
    strategy._resubmit = lambda managed_order, price: SimpleNamespace(
        status=bt.Order.Submitted, ref=next(refs)
    )
    strategy._build_context = lambda contract, current_dt: {}
    strategy._record_order_event = lambda managed_order, **payload: events.append(payload)
    strategy._record_block = lambda *args, **kwargs: None
    return strategy, events


def test_stale_fill_guard_rejects_18_minute_late_fill() -> None:
    strategy = _strategy()
    signal_time = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)
    managed = _managed(signal_time)
    fill_time = signal_time + timedelta(minutes=18)

    reason = strategy._stale_reject_reason(
        managed,
        fill_time.replace(tzinfo=None),
        context={
            "underlying_price": Decimal("103.70"),
            "above_vwap": True,
            "above_orh": True,
            "option_spread_pct": Decimal("0.0759"),
        },
    )

    assert reason == "signal_to_fill_seconds>90"


def test_stale_fill_guard_rejects_wide_spread() -> None:
    strategy = _strategy()
    signal_time = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)
    managed = _managed(signal_time)

    reason = strategy._stale_reject_reason(
        managed,
        signal_time.replace(tzinfo=None) + timedelta(seconds=30),
        context={
            "underlying_price": Decimal("102.20"),
            "above_vwap": True,
            "above_orh": False,
            "option_spread_pct": Decimal("0.32"),
        },
    )

    assert reason == "option_spread_pct>0.10"


def test_stale_fill_guard_rejects_missing_fresh_quote() -> None:
    strategy = _strategy()
    signal_time = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)
    managed = _managed(signal_time)

    reason = strategy._stale_reject_reason(
        managed,
        signal_time.replace(tzinfo=None) + timedelta(seconds=30),
        context={"option_quote_missing": True},
    )

    assert reason == "option_quote_missing_at_fill"


def test_ttl_expiry_skips_intermediate_reprice_stages() -> None:
    now = datetime(2026, 5, 22, 13, 32)
    managed = _managed(datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc))
    managed.order = SimpleNamespace(status=bt.Order.Submitted, ref=5)
    managed.path = ExecutionPath(
        [
            PriceStep(0, Decimal("3.95"), "mid"),
            PriceStep(5, Decimal("4.00"), "reprice"),
        ],
        ttl_seconds=30,
    )
    managed.start_dt = now - timedelta(seconds=31)
    strategy, events = _ttl_strategy(managed)

    strategy._advance_paths(now)

    assert managed.attempts == 1
    assert events[-1]["event_type"] == "ORDER_TTL_FALLBACK"
    assert list(strategy._pending_orders) == [105]


def test_reprice_jumps_to_latest_due_stage_from_original_submit_time() -> None:
    now = datetime(2026, 5, 22, 13, 31, 20)
    managed = _managed(datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc))
    managed.order = SimpleNamespace(status=bt.Order.Submitted, ref=6)
    managed.path = ExecutionPath(
        [
            PriceStep(0, Decimal("3.95"), "mid"),
            PriceStep(5, Decimal("4.00"), "mid-20pct-spread"),
            PriceStep(15, Decimal("4.05"), "mid-40pct-spread"),
            PriceStep(25, Decimal("4.10"), "marketable"),
        ],
        ttl_seconds=30,
    )
    managed.start_dt = now - timedelta(seconds=20)
    strategy, events = _ttl_strategy(managed)

    strategy._advance_paths(now)

    assert managed.attempts == 0
    assert managed.current_limit_price == Decimal("4.05")
    assert managed.next_step == 3
    assert events[-1]["event_type"] == "ORDER_REPRICE"
    assert events[-1]["reason_code"] == "mid-40pct-spread"
    assert list(strategy._pending_orders) == [106]


def test_exit_ttl_fallback_promotes_sell_to_marketable() -> None:
    now = datetime(2026, 5, 22, 13, 32)
    managed = _managed(datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc))
    managed.side = OrderSide.SELL
    managed.order = SimpleNamespace(status=bt.Order.Submitted, ref=10)
    managed.start_dt = now - timedelta(seconds=31)
    strategy, events = _ttl_strategy(managed)

    strategy._advance_paths(now)

    assert managed.ttl_fallback_done is True
    assert managed.attempts == 1
    assert events[-1]["event_type"] == "EXIT_TTL_FALLBACK"
    assert list(strategy._pending_orders) == [110]


def test_exit_ttl_second_failure_promotes_forced_liquidation() -> None:
    now = datetime(2026, 5, 22, 13, 33)
    managed = _managed(datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc))
    managed.side = OrderSide.SELL
    managed.order = SimpleNamespace(status=bt.Order.Submitted, ref=20)
    managed.start_dt = now - timedelta(seconds=31)
    managed.attempts = 1
    strategy, events = _ttl_strategy(managed)

    strategy._advance_paths(now)

    assert managed.forced_exit is True
    assert managed.attempts == 2
    assert events[-1]["event_type"] == "EXIT_FORCED_LIQUIDATION"
    assert list(strategy._pending_orders) == [120]


def test_forced_liquidation_order_is_not_resubmitted_repeatedly() -> None:
    now = datetime(2026, 5, 22, 13, 34)
    managed = _managed(datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc))
    managed.side = OrderSide.SELL
    managed.order = SimpleNamespace(status=bt.Order.Submitted, ref=30)
    managed.start_dt = now - timedelta(seconds=31)
    managed.attempts = 2
    managed.forced_exit = True
    strategy, events = _ttl_strategy(managed)
    cancelled: list[object] = []
    strategy.cancel = cancelled.append

    strategy._advance_paths(now)

    assert managed.attempts == 2
    assert managed.order.ref == 30
    assert list(strategy._pending_orders) == [30]
    assert cancelled == []
    assert events == []


def test_order_event_quantity_uses_actual_order_size() -> None:
    signal_time = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)
    managed = _managed(signal_time)
    managed.side = OrderSide.SELL
    managed.order = SimpleNamespace(status=bt.Order.Submitted, ref=40, size=-2)
    captured: dict[str, object] = {}
    strategy = OptionSignalStrategy.__new__(OptionSignalStrategy)
    strategy._record_order_event_payload = lambda **payload: captured.update(payload)

    strategy._record_order_event(
        managed,
        event_type="ORDER_SUBMITTED",
        reason_code="TEST",
        event_dt=signal_time.replace(tzinfo=None),
        fill_price=None,
        context={},
    )

    assert captured["quantity"] == Decimal("2")
    assert captured["order_ref"] == 40


def test_fill_slippage_is_measured_against_mid_at_fill() -> None:
    assert OptionSignalStrategy._fill_slippage(
        OrderSide.BUY,
        Decimal("9.10"),
        Decimal("8.90"),
    ) == Decimal("0.20")
    assert OptionSignalStrategy._fill_slippage(
        OrderSide.SELL,
        Decimal("8.00"),
        Decimal("8.35"),
    ) == Decimal("0.35")
