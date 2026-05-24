from __future__ import annotations

import uuid
from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Callable, Deque, Dict, Iterable, Iterator, List, Optional, Tuple

import backtrader as bt

from apps.backtest.broker.slippage_spread import ExecutionPath, build_execution_path
from apps.backtest.dao import BacktestDAO
from apps.backtest.risk_adapter import ExposureLike, OrderIntent, RiskCtx
from libs.core import get_settings
from apps.backtest.signal_source import SignalEvent
from libs.schemas.exec import ExecutionMode, OrderSide
from libs.schemas.signals import SignalEnvelope, SignalSide


@dataclass
class ContractSelection:
    conid: Optional[int]
    symbol: str
    expiry: datetime
    strike: Decimal
    right: str
    bid: Decimal
    ask: Decimal
    mid: Decimal
    min_tick: Decimal
    option_right: str
    open_interest: int
    volume: int
    dte: int
    delta: Optional[Decimal] = None
    underlying_price: Optional[Decimal] = None


@dataclass
class ManagedOrder:
    order: bt.Order
    path: ExecutionPath
    next_step: int
    start_dt: datetime
    attempts: int
    signal: SignalEnvelope
    contract: ContractSelection
    side: OrderSide
    mode: ExecutionMode
    trace_id: str


OptionSelector = Callable[[SignalEnvelope, datetime], Optional[ContractSelection]]


class OptionSignalStrategy(bt.Strategy):
    params = dict(
        dao=None,
        signals=(),
        option_selector=None,
        run_id=None,
        strategy_code="core-vol",
        risk_ctx=None,
        risk_prechecker=None,
        option_feeds=None,
    )

    def __init__(self) -> None:
        if self.p.dao is None or not isinstance(self.p.dao, BacktestDAO):
            raise ValueError("OptionSignalStrategy requires BacktestDAO via 'dao' param")
        self.dao: BacktestDAO = self.p.dao
        self._risk_ctx: RiskCtx | None = self.p.risk_ctx
        self._risk_prechecker = self.p.risk_prechecker
        if self._risk_ctx is None or self._risk_prechecker is None:
            raise ValueError("OptionSignalStrategy requires risk_ctx and risk_prechecker")

        if self.p.option_selector is None:
            self.option_selector = self._default_option_selector
        else:
            if not callable(self.p.option_selector):
                raise ValueError("option_selector must be callable")
            self.option_selector = self.p.option_selector
        self.strategy_code: str = self.p.strategy_code
        self.run_id: Optional[int] = self.p.run_id

        signal_iterable: Iterable[SignalEvent] = self.p.signals or []
        self._signal_iter: Iterator[SignalEvent] = iter(signal_iterable)
        self._pending_event: Optional[SignalEvent] = None
        self._signal_buffer: Deque[Tuple[SignalEnvelope, str]] = deque()
        self._pending_orders: Dict[int, ManagedOrder] = {}
        self._positions_qty: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self._positions_mark: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self._open_contracts: Dict[str, ContractSelection] = {}
        self._risk_limit: Decimal = Decimal(str(get_settings().risk_notional_cap))
        raw_feeds = self.p.option_feeds or {}
        self._option_feeds: Dict[tuple[str, str], bt.feeds.PandasData] = {}
        for key, feed in raw_feeds.items():
            norm_key = self._normalise_feed_key(key)
            self._option_feeds[norm_key] = feed

    # ------------------------------------------------------------------
    def next(self) -> None:
        current_dt = self.data.datetime.datetime(0)
        self._load_signals_up_to(current_dt)
        self._process_signals_up_to(current_dt)
        self._advance_paths(current_dt)

    # ------------------------------------------------------------------
    def _process_signals_up_to(self, current_dt: datetime) -> None:
        while self._signal_buffer and self._signal_buffer[0][0].generated_at <= current_dt:
            signal, trace_id = self._signal_buffer.popleft()
            if signal.side == SignalSide.SELL:
                self._handle_exit_signal(signal, trace_id, current_dt)
            else:
                self._handle_entry_signal(signal, trace_id, current_dt)

    def _load_signals_up_to(self, current_dt: datetime) -> None:
        while True:
            event = self._next_event()
            if event is None:
                break
            if event.ts_end <= current_dt:
                envelope = self._event_to_envelope(event)
                self._signal_buffer.append((envelope, event.trace_id))
            else:
                self._pending_event = event
                break

    def _next_event(self) -> Optional[SignalEvent]:
        if self._pending_event is not None:
            event = self._pending_event
            self._pending_event = None
            return event
        try:
            return next(self._signal_iter)
        except StopIteration:
            return None

    def _event_to_envelope(self, event: SignalEvent) -> SignalEnvelope:
        return SignalEnvelope(
            strategy_code=self.strategy_code,
            symbol=event.symbol,
            signal_code=event.signal_code,
            side=event.side,
            confidence=1.0,
            reason=event.reason,
            risk_hint=event.risk_hint,
            option_hint=event.option_hint,
            ttl_seconds=event.ttl_seconds,
            cooldown_seconds=event.cooldown_seconds,
            generated_at=event.ts_end,
        )

    # ------------------------------------------------------------------
    def _handle_entry_signal(self, signal: SignalEnvelope, trace_id: str, now: datetime) -> None:
        contract = self.option_selector(signal, now)
        if contract is None:
            self._record_signal(signal, accepted=False, reason="BLOCK:OPTION_NOT_FOUND")
            return

        path = self._build_path(signal, contract, ExecutionMode.PASSIVE)
        first_step = path.steps[0]

        size = self._parse_size(signal)
        trace_value = trace_id or str(uuid.uuid4())
        self._positions_mark[contract.symbol] = contract.mid
        risk_decision = self._risk_prechecker(
            OrderIntent(
                strategy_code=self.strategy_code,
                symbol=signal.symbol,
                signal_code=signal.signal_code,
                side="BUY",
                quantity=Decimal(size),
                limit_price=Decimal(first_step.price),
                option_right=contract.option_right,
                timestamp=signal.generated_at,
                trace_id=trace_value,
                exposures=self._build_exposures(),
                current_notional=self._current_notional(),
                pending_notional=Decimal(size) * contract.mid * Decimal("100"),
                utilisation=self._compute_utilisation(
                    Decimal(size) * contract.mid * Decimal("100")
                ),
                implied_vol=None,
                option_strike=contract.strike,
                option_expiry=contract.expiry,
                option_open_interest=contract.open_interest,
                option_volume=contract.volume,
                option_bid=contract.bid,
                option_ask=contract.ask,
                option_mid=contract.mid,
                option_spread=contract.ask - contract.bid,
                option_dte=contract.dte,
                option_otm_steps=None,
            )
        )

        if not risk_decision.approved:
            reason = risk_decision.reason_code or "BLOCK:RISK"
            self._record_signal(signal, accepted=False, reason=reason)
            self._record_block(signal, contract, trace_value, reason)
            return

        self._record_signal(signal, accepted=True, reason="PASS")
        self._submit_order(signal, contract, ExecutionMode.PASSIVE, path, trace_value)

    # ------------------------------------------------------------------
    def _handle_exit_signal(self, signal: SignalEnvelope, trace_id: str, now: datetime) -> None:
        contract = self._open_contracts.get(signal.symbol.upper())
        if contract is None:
            self._record_signal(signal, accepted=False, reason="BLOCK:EXIT_POSITION_NOT_FOUND")
            return

        path = self._build_path(signal, contract, ExecutionMode.FORCE)
        self._positions_mark[contract.symbol] = contract.mid
        trace_value = trace_id or str(uuid.uuid4())
        self._record_signal(signal, accepted=True, reason="EXIT")
        self._submit_order(signal, contract, ExecutionMode.FORCE, path, trace_value)

    # ------------------------------------------------------------------
    def _submit_order(
        self,
        signal: SignalEnvelope,
        contract: ContractSelection,
        mode: ExecutionMode,
        path: ExecutionPath,
        trace_id: Optional[str] = None,
    ) -> None:
        first_step = path.steps[0]
        side = (
            OrderSide.BUY
            if mode != ExecutionMode.FORCE or signal.side == SignalSide.BUY
            else OrderSide.SELL
        )

        if signal.signal_code.startswith("SIG_EXIT") or signal.side == SignalSide.SELL:
            side = OrderSide.SELL
        elif side != OrderSide.BUY:
            side = OrderSide.SELL

        size = self._parse_size(signal)
        if side == OrderSide.SELL:
            open_qty = abs(self._positions_qty.get(contract.symbol, Decimal("0")))
            if open_qty > 0:
                size = max(1, int(open_qty))
        if trace_id is None:
            trace_id = str(uuid.uuid4())

        data_feed = self._resolve_option_feed(contract)
        if side == OrderSide.BUY:
            order = self.buy(
                data=data_feed,
                exectype=bt.Order.Limit,
                price=float(first_step.price),
                size=size,
            )
        else:
            order = self.sell(
                data=data_feed,
                exectype=bt.Order.Limit,
                price=float(first_step.price),
                size=size,
            )

        managed = ManagedOrder(
            order=order,
            path=path,
            next_step=1,
            start_dt=self.data.datetime.datetime(0),
            attempts=0,
            signal=signal,
            contract=contract,
            side=side,
            mode=mode,
            trace_id=trace_id,
        )
        self._pending_orders[order.ref] = managed

    # ------------------------------------------------------------------
    def _build_path(
        self,
        signal: SignalEnvelope,
        contract: ContractSelection,
        mode: ExecutionMode,
    ) -> ExecutionPath:
        mid = contract.mid
        spread = contract.ask - contract.bid
        path = build_execution_path(
            mode=mode,
            side=OrderSide.BUY if signal.side == SignalSide.BUY else OrderSide.SELL,
            min_tick=contract.min_tick,
            mid=mid,
            spread=spread,
            bid=contract.bid,
            ask=contract.ask,
        )
        return path

    # ------------------------------------------------------------------
    def _advance_paths(self, current_dt: datetime) -> None:
        for ref, managed in list(self._pending_orders.items()):
            order = managed.order
            if order.status not in (bt.Order.Submitted, bt.Order.Accepted):
                continue

            elapsed = (current_dt - managed.start_dt).total_seconds()

            while managed.next_step < len(managed.path.steps):
                step = managed.path.steps[managed.next_step]
                if elapsed < step.offset_sec:
                    break
                managed.next_step += 1
                self.cancel(order)
                new_order = self._resubmit(managed, step.price)
                managed.order = new_order
                managed.start_dt = current_dt
                self._pending_orders[new_order.ref] = managed
                del self._pending_orders[ref]
                ref = new_order.ref
                order = new_order
                break

            if elapsed >= managed.path.ttl_seconds:
                self.cancel(order)
                if managed.attempts == 0:
                    managed.attempts += 1
                    retry_path = self._build_path(
                        managed.signal, managed.contract, ExecutionMode.MARKETABLE
                    )
                    managed.path = retry_path
                    managed.next_step = 1
                    managed.start_dt = current_dt
                    new_order = self._resubmit(managed, retry_path.steps[0].price)
                    managed.order = new_order
                    self._pending_orders[new_order.ref] = managed
                    del self._pending_orders[ref]
                else:
                    self._record_block(
                        managed.signal,
                        managed.contract,
                        managed.trace_id,
                        "BLOCK:TTL_EXPIRED",
                    )
                    del self._pending_orders[ref]

    def _resubmit(self, managed: ManagedOrder, price: Decimal) -> bt.Order:
        size = self._parse_size(managed.signal)
        if managed.side == OrderSide.SELL:
            open_qty = abs(self._positions_qty.get(managed.contract.symbol, Decimal("0")))
            if open_qty > 0:
                size = max(1, int(open_qty))
        data_feed = self._resolve_option_feed(managed.contract)
        if managed.side == OrderSide.BUY:
            return self.buy(
                data=data_feed, exectype=bt.Order.Limit, price=float(price), size=size
            )
        return self.sell(data=data_feed, exectype=bt.Order.Limit, price=float(price), size=size)

    # ------------------------------------------------------------------
    def notify_order(self, order: bt.Order) -> None:
        managed = self._pending_orders.get(order.ref)
        if managed is None:
            return

        if order.status in (bt.Order.Completed, bt.Order.Partial):
            executed_qty = Decimal(order.executed.size)
            executed_price = Decimal(order.executed.price)
            symbol = managed.contract.symbol
            if managed.side == OrderSide.BUY:
                self._positions_qty[symbol] += executed_qty
                self._open_contracts[symbol.upper()] = managed.contract
            else:
                self._positions_qty[symbol] -= executed_qty
                if self._positions_qty[symbol] <= 0:
                    self._open_contracts.pop(symbol.upper(), None)
            self._positions_mark[symbol] = executed_price
            if self.run_id is not None:
                payload = {
                    "run_id": self.run_id,
                    "symbol": managed.contract.symbol,
                    "side": "BUY" if managed.side == OrderSide.BUY else "SELL",
                    "quantity": Decimal(order.executed.size),
                    "price": Decimal(order.executed.price),
                    "trade_ts": self.data.datetime.datetime(0),
                    "trace_id": managed.trace_id,
                    "option_right": managed.contract.option_right,
                    "strike": managed.contract.strike,
                    "expiry": managed.contract.expiry,
                    "fees": Decimal("0"),
                    "slippage": managed.path.steps[
                        min(max(managed.next_step - 1, 0), len(managed.path.steps) - 1)
                    ].price
                    - managed.contract.mid,
                    "reason_code": managed.signal.signal_code,
                }
                self.dao.record_trade(payload)
            del self._pending_orders[order.ref]

        elif order.status in (bt.Order.Canceled, bt.Order.Expired, bt.Order.Rejected):
            del self._pending_orders[order.ref]

    # ------------------------------------------------------------------
    def _record_signal(self, signal: SignalEnvelope, *, accepted: bool, reason: str) -> None:
        if self.run_id is None:
            return
        payload = {
            "run_id": self.run_id,
            "ts_end": signal.generated_at,
            "symbol": signal.symbol,
            "signal_code": signal.signal_code,
            "accepted": accepted,
            "reason": reason,
        }
        self.dao.record_signal(payload)

    # ------------------------------------------------------------------
    def _default_option_selector(
        self, signal: SignalEnvelope, ts: datetime
    ) -> Optional[ContractSelection]:
        # The live strategy expresses every entry as a CALL and exits by selling the
        # originally opened CALL. The default backtest selector mirrors the entry side;
        # exit handling should normally use _open_contracts and not call this fallback.
        option_right = "CALL"
        trade_date = ts.date()
        dte_min, dte_max = self._hint_range(signal.option_hint.get("dte"), default=(2, 7))
        delta_low, delta_high = self._hint_decimal_range(
            signal.option_hint.get("delta"), default=(Decimal("0.35"), Decimal("0.45"))
        )
        candidates = self.dao.fetch_option_candidates(
            trade_date=trade_date,
            underlying_symbol=signal.symbol,
            option_right=option_right,
            dte_min=dte_min,
            dte_max=dte_max,
        )
        filtered: List[tuple[tuple[Decimal, ...], ContractSelection]] = []
        for candidate in candidates:
            bid = candidate.bid
            ask = candidate.ask
            mid = candidate.mid
            oi = candidate.open_interest
            vol = candidate.volume
            min_tick = candidate.min_tick
            strike_value = candidate.strike
            if None in (bid, ask, mid, oi, vol, min_tick, strike_value):
                continue
            bid_d = Decimal(str(bid))
            ask_d = Decimal(str(ask))
            mid_d = Decimal(str(mid))
            spread_d = ask_d - bid_d
            if ask_d <= bid_d or spread_d <= 0:
                continue
            if Decimal(str(oi)) < Decimal("500") or Decimal(str(vol)) < Decimal("100"):
                continue
            threshold = max(Decimal("0.10"), mid_d * Decimal("0.05"))
            if spread_d > threshold:
                continue
            expiry = candidate.expiry
            if expiry is None:
                continue
            if not isinstance(expiry, datetime):
                expiry = datetime.combine(expiry, datetime.min.time())
            open_interest_val = candidate.open_interest
            volume_val = candidate.volume
            dte_val = candidate.dte
            try:
                conid_int = int(candidate.conid) if candidate.conid is not None else 0
                open_interest_int = int(open_interest_val) if open_interest_val is not None else 0
                volume_int = int(volume_val) if volume_val is not None else 0
                dte_int = int(dte_val) if dte_val is not None else 0
            except (TypeError, ValueError):
                continue

            delta_value = Decimal(str(candidate.delta)) if candidate.delta is not None else None
            underlying = (
                Decimal(str(candidate.underlying_price))
                if candidate.underlying_price is not None
                else None
            )
            selection = ContractSelection(
                conid=conid_int or None,
                symbol=candidate.underlying_symbol or signal.symbol,
                expiry=expiry,
                strike=Decimal(str(strike_value)),
                right=option_right,
                bid=bid_d,
                ask=ask_d,
                mid=mid_d,
                min_tick=Decimal(str(min_tick)),
                option_right=option_right,
                open_interest=open_interest_int,
                volume=volume_int,
                dte=dte_int,
                delta=delta_value,
                underlying_price=underlying,
            )
            strike_distance = abs(selection.strike - underlying) if underlying is not None else Decimal("0")
            sort_key = (
                Decimal(-volume_int),
                self._delta_penalty(delta_value, delta_low, delta_high),
                spread_d,
                Decimal(-open_interest_int),
                strike_distance,
            )
            filtered.append((sort_key, selection))

        if not filtered:
            return None

        filtered.sort(key=lambda item: item[0])
        return filtered[0][1]

    def _parse_size(self, signal: SignalEnvelope) -> int:
        if isinstance(signal.risk_hint, dict):
            value = signal.risk_hint.get("size", 1)
            if isinstance(value, int):
                return max(1, value)
            if isinstance(value, str):
                digits = "".join(ch for ch in value if ch.isdigit())
                if digits:
                    try:
                        return max(1, int(digits))
                    except ValueError:
                        return 1
                return 1
            try:
                size = int(value)
            except (TypeError, ValueError):
                size = 1
            return max(1, size)
        return 1

    @staticmethod
    def _hint_range(value, *, default: tuple[int, int]) -> tuple[int, int]:
        source = value if isinstance(value, (list, tuple, set)) else default
        parsed: list[int] = []
        for item in source:
            try:
                parsed.append(int(item))
            except (TypeError, ValueError):
                continue
        if len(parsed) >= 2:
            return min(parsed), max(parsed)
        if len(parsed) == 1:
            return parsed[0], parsed[0]
        return default

    @staticmethod
    def _hint_decimal_range(value, *, default: tuple[Decimal, Decimal]) -> tuple[Decimal, Decimal]:
        source = value if isinstance(value, (list, tuple, set)) else default
        parsed: list[Decimal] = []
        for item in source:
            try:
                parsed.append(Decimal(str(item)))
            except (InvalidOperation, TypeError, ValueError):
                continue
        if len(parsed) >= 2:
            return min(parsed), max(parsed)
        if len(parsed) == 1:
            return parsed[0], parsed[0]
        return default

    @staticmethod
    def _delta_penalty(delta: Optional[Decimal], low: Decimal, high: Decimal) -> Decimal:
        if delta is None:
            return Decimal("999")
        abs_delta = abs(delta)
        if low <= abs_delta <= high:
            return Decimal("0")
        if abs_delta < low:
            return low - abs_delta
        return abs_delta - high

    def _build_exposures(self) -> List[ExposureLike]:
        exposures: List[ExposureLike] = []
        for symbol, qty in self._positions_qty.items():
            if qty != 0:
                exposures.append(ExposureLike(symbol=symbol.upper(), open_quantity=qty))
        return exposures

    def _current_notional(self) -> Decimal:
        total = Decimal("0")
        for symbol, qty in self._positions_qty.items():
            mark = self._positions_mark.get(symbol, Decimal("0"))
            total += abs(qty) * mark * Decimal("100")
        return total

    def _compute_utilisation(self, pending_notional: Decimal) -> float:
        limit = self._risk_limit
        if limit <= 0:
            return 0.0
        value = (self._current_notional() + pending_notional) / limit
        return float(value)

    def _record_block(
        self, signal: SignalEnvelope, contract: ContractSelection, trace_id: str, reason_code: str
    ) -> None:
        if self.run_id is None:
            return
        mid_price = contract.mid if contract else Decimal("0")
        payload = {
            "run_id": self.run_id,
            "symbol": signal.symbol,
            "signal_code": signal.signal_code,
            "side": "BLOCK",
            "quantity": Decimal("0"),
            "price": Decimal(mid_price),
            "trade_ts": signal.generated_at,
            "trace_id": trace_id,
            "option_right": contract.option_right if contract else None,
            "strike": contract.strike if contract else None,
            "expiry": contract.expiry if contract else None,
            "fees": Decimal("0"),
            "slippage": Decimal("0"),
            "reason_code": reason_code,
        }
        self.dao.record_block(payload)

    # ------------------------------------------------------------------
    def _normalise_feed_key(self, key: tuple[str, str]) -> tuple[str, str]:
        symbol, right = key
        return symbol.upper(), right.upper()

    def _feed_key_for_contract(self, contract: ContractSelection) -> tuple[str, str]:
        return (contract.symbol.upper(), contract.option_right.upper())

    def _resolve_option_feed(self, contract: ContractSelection):
        feed = self._option_feeds.get(self._feed_key_for_contract(contract))
        if feed is None:
            raise RuntimeError(f"Option data feed missing for {contract.symbol} {contract.option_right}")
        return feed
