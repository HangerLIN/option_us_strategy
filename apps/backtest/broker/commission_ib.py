from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from backtrader.comminfo import CommInfoBase

OPTION_MULTIPLIER = Decimal("100")


@dataclass
class CommissionSchedule:
    """Represents IBKR option commission components."""

    per_contract: Decimal = Decimal("0.65")  # IBKR tiered default
    exchange_fee: Decimal = Decimal("0.015")
    regulatory_fee: Decimal = Decimal("0.00218")
    minimum_per_order: Decimal = Decimal("1.00")

    def total_per_contract(self) -> Decimal:
        return self.per_contract + self.exchange_fee + self.regulatory_fee


class IBOptionCommissionInfo(CommInfoBase):
    """Backtrader commission info implementing the IBKR option schedule."""

    params = dict(automargin=True, stocklike=False)

    def __init__(
        self,
        schedule: CommissionSchedule | None = None,
        multiplier: Decimal | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._schedule = schedule or CommissionSchedule()
        self._multiplier = multiplier or OPTION_MULTIPLIER

    def getcommission(self, size, price, pseudoexec=None) -> float:  # noqa: D401 - Backtrader signature
        contracts = Decimal(abs(size))
        if contracts == 0:
            return 0.0

        per_contract = self._schedule.total_per_contract()
        gross = contracts * per_contract
        commission = max(gross, self._schedule.minimum_per_order)
        return float(commission)

    @property
    def schedule(self) -> CommissionSchedule:
        return self._schedule
