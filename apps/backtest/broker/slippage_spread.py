from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import List

from libs.schemas.exec import ExecutionMode, OrderSide


@dataclass(frozen=True)
class PriceStep:
    """Represents a scheduled price adjustment."""

    offset_sec: int
    price: Decimal
    stage: str  # e.g. passive, semi, force


@dataclass(frozen=True)
class ExecutionPath:
    steps: List[PriceStep]
    ttl_seconds: int = 30


def _round_to_tick(price: Decimal, min_tick: Decimal | None, side: OrderSide) -> Decimal:
    if min_tick is None or min_tick <= 0:
        return price
    ticks = price / min_tick
    rounding = ROUND_UP if side == OrderSide.BUY else ROUND_DOWN
    ticks = ticks.quantize(Decimal("1"), rounding=rounding)
    return ticks * min_tick


def build_execution_path(
    *,
    mode: ExecutionMode,
    side: OrderSide,
    min_tick: Decimal | None,
    mid: Decimal | None,
    spread: Decimal | None,
    bid: Decimal | None,
    ask: Decimal | None,
) -> ExecutionPath:
    if mode == ExecutionMode.PASSIVE:
        return _passive_path(side=side, min_tick=min_tick, mid=mid, spread=spread)
    if mode == ExecutionMode.MARKETABLE:
        return _marketable_path(
            side=side, min_tick=min_tick, mid=mid, spread=spread, bid=bid, ask=ask
        )
    if mode == ExecutionMode.FORCE:
        return _force_path(side=side, min_tick=min_tick, bid=bid, ask=ask)
    raise ValueError(f"Unsupported execution mode: {mode}")


def _passive_path(
    *,
    side: OrderSide,
    min_tick: Decimal | None,
    mid: Decimal | None,
    spread: Decimal | None,
) -> ExecutionPath:
    if mid is None or spread is None:
        raise ValueError("Passive path requires mid and spread")

    base_price = _round_to_tick(mid, min_tick, side)

    if side == OrderSide.BUY:
        nudged = mid + spread * Decimal("0.2")
    else:
        nudged = mid - spread * Decimal("0.2")

    nudged_price = _round_to_tick(nudged, min_tick, side)

    steps = [
        PriceStep(offset_sec=0, price=base_price, stage="passive"),
        PriceStep(offset_sec=15, price=nudged_price, stage="semi-passive"),
    ]
    return ExecutionPath(steps=steps, ttl_seconds=30)


def _marketable_path(
    *,
    side: OrderSide,
    min_tick: Decimal | None,
    mid: Decimal | None,
    spread: Decimal | None,
    bid: Decimal | None,
    ask: Decimal | None,
) -> ExecutionPath:
    if side == OrderSide.BUY:
        if ask is None:
            raise ValueError("Marketable BUY requires ask price")
        price_candidates = [ask]
        if mid is not None and spread is not None:
            price_candidates.append(mid + spread * Decimal("0.6"))
        if mid is not None:
            price_candidates.append(mid)
        price = min(price_candidates)
    else:
        if bid is None:
            raise ValueError("Marketable SELL requires bid price")
        price_candidates = [bid]
        if mid is not None and spread is not None:
            price_candidates.append(mid - spread * Decimal("0.6"))
        if mid is not None:
            price_candidates.append(mid)
        price = max(price_candidates)

    rounded = _round_to_tick(price, min_tick, side)
    return ExecutionPath(
        steps=[PriceStep(offset_sec=0, price=rounded, stage="marketable")], ttl_seconds=30
    )


def _force_path(
    *,
    side: OrderSide,
    min_tick: Decimal | None,
    bid: Decimal | None,
    ask: Decimal | None,
) -> ExecutionPath:
    if side == OrderSide.BUY:
        if ask is None:
            raise ValueError("Force BUY requires ask price")
        base = ask
        direction = Decimal("1")
    else:
        if bid is None:
            raise ValueError("Force SELL requires bid price")
        base = bid
        direction = Decimal("-1")

    base = _round_to_tick(base, min_tick, side)
    steps = [PriceStep(offset_sec=0, price=base, stage="force-0")]

    if min_tick is None or min_tick <= 0:
        tick = Decimal("0")
    else:
        tick = min_tick

    for jump in range(1, 4):
        if tick == 0:
            break
        price = base + direction * tick * Decimal(jump)
        price = _round_to_tick(price, min_tick, side)
        steps.append(PriceStep(offset_sec=jump * 5, price=price, stage=f"force-{jump}"))

    return ExecutionPath(steps=steps, ttl_seconds=30)
