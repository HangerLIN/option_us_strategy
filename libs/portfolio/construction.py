from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Any, Iterable, Protocol, Sequence

from libs.schemas.assets import AssetType, InstrumentRef
from libs.schemas.exec import ExecutionMode, OrderSide
from libs.schemas.signals import SignalEnvelope


@dataclass(frozen=True, slots=True)
class Candidate:
    instrument: InstrumentRef
    signal: SignalEnvelope
    score: Decimal
    expected_edge: Decimal | None = None
    liquidity_score: Decimal | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AllocationBudget:
    strategy_code: str
    total_notional: Decimal
    max_symbol_notional: Decimal | None = None
    max_asset_notional: dict[AssetType, Decimal] = field(default_factory=dict)
    reserve_cash: Decimal = Decimal("0")


@dataclass(frozen=True, slots=True)
class PortfolioDecision:
    strategy_code: str
    instrument: InstrumentRef
    side: OrderSide
    quantity: Decimal
    target_notional: Decimal
    signal_code: str
    score: Decimal
    execution_mode: ExecutionMode = ExecutionMode.ADAPTIVE
    reason: dict[str, Any] = field(default_factory=dict)


class CandidateSelector(Protocol):
    def select(self, candidates: Sequence[Candidate], *, limit: int | None = None) -> list[Candidate]:
        ...


class SignalScorer(Protocol):
    def score(self, signal: SignalEnvelope, context: dict[str, Any]) -> Decimal:
        ...


class PositionSizer(Protocol):
    def size(
        self,
        candidate: Candidate,
        *,
        budget: AllocationBudget,
        price: Decimal,
        selected_count: int,
    ) -> Decimal:
        ...


class PortfolioConstructor(Protocol):
    def construct(
        self,
        candidates: Sequence[Candidate],
        *,
        budget: AllocationBudget,
        prices: dict[str, Decimal],
    ) -> list[PortfolioDecision]:
        ...


class RebalancePolicy(Protocol):
    def should_rebalance(self, *, now_context: dict[str, Any], current_positions: Iterable[Any]) -> bool:
        ...


class TopRankCandidateSelector:
    def select(self, candidates: Sequence[Candidate], *, limit: int | None = None) -> list[Candidate]:
        ordered = sorted(candidates, key=lambda item: item.score, reverse=True)
        return ordered[:limit] if limit is not None else ordered


class ReasonFieldSignalScorer:
    def __init__(self, field: str = "score", default: Decimal = Decimal("1")) -> None:
        self._field = field
        self._default = default

    def score(self, signal: SignalEnvelope, context: dict[str, Any]) -> Decimal:
        raw = signal.reason.get(self._field, context.get(self._field, self._default))
        try:
            return Decimal(str(raw))
        except Exception:
            return self._default


class FixedFractionPositionSizer:
    def __init__(self, fraction: Decimal = Decimal("1")) -> None:
        if fraction <= 0:
            raise ValueError("fraction must be positive")
        self._fraction = fraction

    def size(
        self,
        candidate: Candidate,
        *,
        budget: AllocationBudget,
        price: Decimal,
        selected_count: int,
    ) -> Decimal:
        if price <= 0:
            raise ValueError("price must be positive")
        count = max(1, selected_count)
        available = max(Decimal("0"), budget.total_notional - budget.reserve_cash)
        notional = available * self._fraction / Decimal(count)
        if budget.max_symbol_notional is not None:
            notional = min(notional, budget.max_symbol_notional)
        asset_cap = budget.max_asset_notional.get(candidate.instrument.asset_type)
        if asset_cap is not None:
            notional = min(notional, asset_cap / Decimal(count))
        return (notional / price).quantize(Decimal("1"), rounding=ROUND_DOWN)


class EqualWeightPortfolioConstructor:
    def __init__(
        self,
        *,
        selector: CandidateSelector | None = None,
        sizer: PositionSizer | None = None,
        max_candidates: int | None = None,
    ) -> None:
        self._selector = selector or TopRankCandidateSelector()
        self._sizer = sizer or FixedFractionPositionSizer()
        self._max_candidates = max_candidates

    def construct(
        self,
        candidates: Sequence[Candidate],
        *,
        budget: AllocationBudget,
        prices: dict[str, Decimal],
    ) -> list[PortfolioDecision]:
        selected = self._selector.select(candidates, limit=self._max_candidates)
        decisions: list[PortfolioDecision] = []
        for candidate in selected:
            price = self._price_for(candidate.instrument, prices)
            if price is None or price <= 0:
                continue
            qty = self._sizer.size(
                candidate,
                budget=budget,
                price=price,
                selected_count=len(selected),
            )
            if qty <= 0:
                continue
            decisions.append(
                PortfolioDecision(
                    strategy_code=budget.strategy_code,
                    instrument=candidate.instrument,
                    side=OrderSide.BUY,
                    quantity=qty,
                    target_notional=qty * price,
                    signal_code=candidate.signal.signal_code,
                    score=candidate.score,
                    reason={
                        "construction": "equal_weight",
                        "candidate_score": str(candidate.score),
                    },
                )
            )
        return decisions

    @staticmethod
    def _price_for(instrument: InstrumentRef, prices: dict[str, Decimal]) -> Decimal | None:
        keys = [
            instrument.symbol.upper(),
            f"{instrument.asset_type.value}:{instrument.symbol.upper()}",
        ]
        if instrument.asset_type == AssetType.OPTION and instrument.option_right and instrument.strike and instrument.expiry:
            expiry_value = instrument.expiry.strftime("%Y%m%d")
            keys.append(
                f"OPTION:{instrument.symbol.upper()}:{expiry_value}:{instrument.option_right}:{instrument.strike}"
            )
        for key in keys:
            value = prices.get(key)
            if value is not None:
                return value
        return None


class NoopRebalancePolicy:
    def should_rebalance(self, *, now_context: dict[str, Any], current_positions: Iterable[Any]) -> bool:
        return False
