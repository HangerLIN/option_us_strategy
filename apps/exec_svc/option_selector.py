from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable, Optional, Sequence

import structlog
from ibapi.contract import Contract, ContractDetails

from libs.core import EASTERN
from libs.infra import IBClient


@dataclass(frozen=True)
class OptionQuote:
    bid: Decimal
    ask: Decimal
    mid: Decimal
    spread: Decimal
    volume: int
    open_interest: int
    delta: Optional[Decimal]


@dataclass(frozen=True)
class OptionSelection:
    contract: Contract
    quote: OptionQuote
    expiry: date
    dte: int
    min_tick: Decimal
    market_rule_id: Optional[int]


@dataclass
class _Candidate:
    detail: ContractDetails
    quote: OptionQuote
    expiry: date
    dte: int
    strike_distance: Decimal


class OptionSelector:
    """Helper for selecting tradable options contracts aligned with execution policy."""

    GENERIC_TICKS = "100,101,106"

    def __init__(self, ib_client: IBClient, *, snapshot_timeout: float = 5.0) -> None:
        self._ib_client = ib_client
        self._snapshot_timeout = snapshot_timeout
        self._logger = structlog.get_logger(__name__)

    # ------------------------------------------------------------------ Public API
    def select_call(
        self,
        symbol: str,
        *,
        otm_offsets: Sequence[int] = (2, 3, 4, 5),
        dte_range: tuple[int, int] = (2, 7),
        delta_band: tuple[Decimal, Decimal] = (Decimal("0.35"), Decimal("0.45")),
    ) -> OptionSelection:
        """Select the best call option contract for the given underlying symbol."""
        if not otm_offsets:
            raise ValueError("otm_offsets must contain at least one element")
        min_dte, max_dte = dte_range
        if min_dte > max_dte:
            raise ValueError("Invalid dte_range: minimum exceeds maximum")

        underlying_price = self._fetch_underlying_price(symbol)
        option_params = self._ib_client.req_opt_params(symbol)
        if not option_params:
            raise RuntimeError(f"No option parameters returned for {symbol}")

        underlying_details = self._ib_client.req_contract_details(
            self._ib_client.stock_contract(symbol)
        )
        if not underlying_details:
            raise RuntimeError(f"Unable to load underlying contract details for {symbol}")
        underlying_con_id = underlying_details[0].contract.conId

        today_et = datetime.now(EASTERN).date()
        candidates: list[_Candidate] = []
        seen: set[tuple[str, Decimal, str]] = set()

        for entry in option_params:
            if entry.get("underlying_con_id") not in {None, underlying_con_id}:
                continue
            trading_class = entry.get("trading_class")
            exchange = entry.get("exchange") or "SMART"
            multiplier = entry.get("multiplier") or "100"
            strike_values: list[Decimal] = []
            for raw_strike in entry.get("strikes", []):
                strike_value = _to_decimal(raw_strike)
                if strike_value is not None:
                    strike_values.append(strike_value)
            strikes = sorted(set(strike_values))
            if not strikes:
                continue

            atm_index = _find_atm_index(strikes, underlying_price)
            if atm_index is None:
                continue

            for expiry_raw in entry.get("expirations", []):
                expiry_date = _parse_expiry(expiry_raw)
                if expiry_date is None:
                    continue
                dte = (expiry_date - today_et).days
                if dte < min_dte or dte > max_dte:
                    continue

                for offset in otm_offsets:
                    candidate_index = atm_index + offset
                    if candidate_index >= len(strikes):
                        continue
                    strike = strikes[candidate_index]
                    dedupe_key = (expiry_date.isoformat(), strike, trading_class or "")
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)

                    contract = _build_option_contract(
                        symbol=symbol,
                        exchange=exchange,
                        multiplier=multiplier,
                        trading_class=trading_class,
                        expiry=expiry_date,
                        strike=strike,
                        right="C",
                    )

                    details = self._ib_client.req_contract_details(contract)
                    if not details:
                        self._logger.debug(
                            "option_selector.contract_not_found",
                            symbol=symbol,
                            expiry=expiry_date.isoformat(),
                            strike=str(strike),
                            trading_class=trading_class,
                        )
                        continue
                    detail = _choose_detail(details, trading_class)
                    quote = self._fetch_option_quote(detail.contract)
                    if quote is None:
                        continue

                    if not _passes_liquidity(quote):
                        self._logger.info(
                            "option_selector.liquidity_reject",
                            symbol=symbol,
                            expiry=expiry_date.isoformat(),
                            strike=str(strike),
                            spread=str(quote.spread),
                            volume=quote.volume,
                            open_interest=quote.open_interest,
                        )
                        continue

                    strike_distance = abs(strike - underlying_price)
                    candidates.append(
                        _Candidate(
                            detail=detail,
                            quote=quote,
                            expiry=expiry_date,
                            dte=dte,
                            strike_distance=strike_distance,
                        )
                    )

        if not candidates:
            raise RuntimeError(f"No eligible option candidates for {symbol}")

        lower_delta, upper_delta = delta_band
        selected = min(
            candidates,
            key=lambda item: (
                item.quote.spread,
                _delta_penalty(item.quote.delta, lower_delta, upper_delta),
                -item.quote.open_interest,
                -item.quote.volume,
                item.strike_distance,
            ),
        )

        min_tick, market_rule_id = self._resolve_min_tick(selected.detail, selected.quote.mid)

        self._logger.info(
            "option_selector.selected",
            symbol=symbol,
            expiry=selected.expiry.isoformat(),
            strike=float(selected.detail.contract.strike),
            dte=selected.dte,
            spread=str(selected.quote.spread),
            volume=selected.quote.volume,
            open_interest=selected.quote.open_interest,
            min_tick=str(min_tick),
            market_rule_id=market_rule_id,
        )

        return OptionSelection(
            contract=selected.detail.contract,
            quote=selected.quote,
            expiry=selected.expiry,
            dte=selected.dte,
            min_tick=min_tick,
            market_rule_id=market_rule_id,
        )

    # ------------------------------------------------------------------ Internals
    def _fetch_underlying_price(self, symbol: str) -> Decimal:
        snapshot = self._ib_client.market_data_snapshot(
            self._ib_client.stock_contract(symbol),
            timeout=self._snapshot_timeout,
        )
        bid = _to_decimal(snapshot.get("bid"))
        ask = _to_decimal(snapshot.get("ask"))
        last = _to_decimal(snapshot.get("last"))
        mark = _to_decimal(snapshot.get("mark"))

        price_candidates = [
            value for value in (bid, ask, mark, last) if value is not None and value > 0
        ]
        if not price_candidates:
            raise RuntimeError(f"Unable to determine underlying price for {symbol}")

        if bid is not None and ask is not None and bid > 0 and ask > 0:
            return (bid + ask) / Decimal("2")
        return price_candidates[0]

    def _fetch_option_quote(self, contract: Contract) -> OptionQuote | None:
        snapshot = self._ib_client.market_data_snapshot(
            contract,
            generic_ticks=self.GENERIC_TICKS,
            timeout=self._snapshot_timeout,
        )
        bid = _to_decimal(snapshot.get("bid"))
        ask = _to_decimal(snapshot.get("ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask <= bid:
            return None

        mid = (bid + ask) / Decimal("2")
        spread = ask - bid
        volume = int(snapshot.get("volume") or 0)
        open_interest = int(snapshot.get("open_interest") or 0)
        delta = _to_decimal(snapshot.get("delta"))

        return OptionQuote(
            bid=bid,
            ask=ask,
            mid=mid,
            spread=spread,
            volume=volume,
            open_interest=open_interest,
            delta=delta,
        )

    def _resolve_min_tick(
        self, detail: ContractDetails, price: Decimal
    ) -> tuple[Decimal, Optional[int]]:
        if detail.marketRuleIds:
            rule_ids = [
                int(part) for part in detail.marketRuleIds.split(",") if part.strip().isdigit()
            ]
        else:
            rule_ids = []

        if rule_ids:
            increments = self._ib_client.req_market_rule(rule_ids[0])
            tick = _min_tick_from_increments(increments, price)
            return tick, rule_ids[0]

        min_tick = detail.minTick or 0.01
        return Decimal(str(min_tick)), None


# --------------------------------------------------------------------------- Helpers
def _to_decimal(value: object) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError):
        return None


def _parse_expiry(expiry: str) -> Optional[date]:
    if not expiry:
        return None
    normalized = expiry.split(" ")[0].split(":")[0]
    if len(normalized) < 8 or not normalized[:8].isdigit():
        return None
    try:
        return datetime.strptime(normalized[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _find_atm_index(strikes: Sequence[Decimal], underlying_price: Decimal) -> Optional[int]:
    for idx, strike in enumerate(strikes):
        if strike >= underlying_price:
            return idx
    return None


def _build_option_contract(
    *,
    symbol: str,
    exchange: str,
    multiplier: str,
    trading_class: Optional[str],
    expiry: date,
    strike: Decimal,
    right: str,
) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "OPT"
    contract.currency = "USD"
    contract.exchange = exchange
    contract.lastTradeDateOrContractMonth = expiry.strftime("%Y%m%d")
    contract.strike = float(strike)
    contract.right = right
    contract.multiplier = multiplier
    if trading_class:
        contract.tradingClass = trading_class
    return contract


def _choose_detail(
    details: Iterable[ContractDetails], trading_class: Optional[str]
) -> ContractDetails:
    if trading_class:
        for detail in details:
            if detail.contract.tradingClass == trading_class:
                return detail
    return next(iter(details))


def _passes_liquidity(quote: OptionQuote) -> bool:
    if quote.volume < 100 or quote.open_interest < 500:
        return False
    if quote.mid <= 0:
        return False
    threshold = max(Decimal("0.10"), quote.mid * Decimal("0.05"))
    return quote.spread <= threshold


def _delta_penalty(
    delta: Optional[Decimal],
    lower: Decimal,
    upper: Decimal,
) -> int:
    if delta is None:
        return 1
    abs_delta = abs(delta)
    return 0 if lower <= abs_delta <= upper else 1


def _min_tick_from_increments(increments: Iterable[object], price: Decimal) -> Decimal:
    candidate = Decimal("0.01")
    price_f = float(price)
    for entry in increments or []:
        low_edge = getattr(entry, "lowEdge", None)
        increment = getattr(entry, "increment", None)
        if low_edge is None or increment is None:
            continue
        if price_f >= float(low_edge):
            candidate = Decimal(str(increment))
        else:
            break
    return candidate
