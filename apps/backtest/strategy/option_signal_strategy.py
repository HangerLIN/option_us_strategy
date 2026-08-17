from __future__ import annotations

import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Deque, Dict, Iterable, Iterator, List, Optional, Tuple

import backtrader as bt
import pandas as pd

from apps.backtest.broker.slippage_spread import ExecutionPath, build_execution_path
from apps.backtest.dao import BacktestDAO
from apps.backtest.lifecycle_log import BacktestLifecycleLogger
from apps.backtest.risk_adapter import ExposureLike, OrderIntent, RiskCtx
from libs.core import EASTERN, get_settings
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
    quote_ts: Optional[datetime] = None


@dataclass
class ManagedOrder:
    order: bt.Order
    path: ExecutionPath
    next_step: int
    start_dt: datetime
    submit_dt: datetime
    attempts: int
    signal: SignalEnvelope
    contract: ContractSelection
    side: OrderSide
    mode: ExecutionMode
    trace_id: str
    signal_context: Dict[str, Any]
    original_limit_price: Decimal
    current_limit_price: Decimal
    ttl_fallback_done: bool = False
    forced_exit: bool = False
    invalid_fill: bool = False
    invalid_fill_reason: str | None = None


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
        strategy_variant=None,
        lifecycle_log=None,
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
        self._lifecycle_log: BacktestLifecycleLogger | None = self.p.lifecycle_log
        self.symbol: str = self.data._name.upper()

        signal_iterable: Iterable[SignalEvent] = self.p.signals or []
        self._signal_iter: Iterator[SignalEvent] = (
            event for event in signal_iterable if event.symbol.upper() == self.symbol
        )
        self._pending_event: Optional[SignalEvent] = None
        self._signal_buffer: Deque[Tuple[SignalEnvelope, str]] = deque()
        self._pending_orders: Dict[int, ManagedOrder] = {}
        self._positions_qty: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self._positions_mark: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self._open_contracts: Dict[str, ContractSelection] = {}
        self._open_entry: Dict[str, Dict[str, Any]] = {}
        self._entry_highs: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self._profit_taken: Dict[str, set[str]] = defaultdict(set)
        self._risk_limit: Decimal = Decimal(str(get_settings().risk_notional_cap))
        self._settings = get_settings()
        self._strategy_variant = str(
            self.p.strategy_variant
            or getattr(self._settings, "backtest_strategy_variant", "v1")
            or "v1"
        ).lower()
        self._stale_guard_enabled = self._strategy_variant in {
            "v4",
            "v6",
            "v7",
            "combined-v2",
            "stale-fill-guard",
            "conservative-execution",
        }
        self._separated_exit_enabled = self._strategy_variant in {
            "v5",
            "v6",
            "v7",
            "combined-v2",
            "separated-exit-policy",
            "conservative-execution",
        }
        self._eod_exit_time = self._parse_hhmm(
            str(getattr(self._settings, "open_chase_eod_exit_time", "15:55"))
        )
        raw_feeds = self.p.option_feeds or {}
        self._option_feeds: Dict[tuple[object, ...], bt.feeds.PandasData] = {}
        for key, feed in raw_feeds.items():
            norm_key = self._normalise_feed_key(key)
            self._option_feeds[norm_key] = feed

    # ------------------------------------------------------------------
    def _emit_lifecycle(self, event: str, **fields: Any) -> None:
        lifecycle_log = getattr(self, "_lifecycle_log", None)
        if lifecycle_log is None:
            return
        fields.setdefault("symbol", getattr(self, "symbol", None))
        lifecycle_log.emit(event, **fields)

    def next(self) -> None:
        current_dt = _as_utc_naive(self.data.datetime.datetime(0))
        self._load_signals_up_to(current_dt)
        self._process_signals_up_to(current_dt)
        self._apply_open_chase_policy(current_dt)
        self._advance_paths(current_dt)
        self._force_eod_exits(current_dt)

    def stop(self) -> None:
        self._emit_lifecycle(
            "STRATEGY_FINISHED",
            open_positions={
                symbol: quantity
                for symbol, quantity in self._positions_qty.items()
                if quantity != 0
            },
            pending_order_refs=list(self._pending_orders),
            buffered_signals=len(self._signal_buffer),
        )

    # ------------------------------------------------------------------
    def _process_signals_up_to(self, current_dt: datetime) -> None:
        while (
            self._signal_buffer
            and _as_utc_naive(self._signal_buffer[0][0].generated_at) <= current_dt
        ):
            signal, trace_id = self._signal_buffer.popleft()
            self._emit_lifecycle(
                "SIGNAL_RECEIVED",
                trace_id=trace_id,
                event_time=signal.generated_at,
                signal_code=signal.signal_code,
                side=signal.side,
                confidence=signal.confidence,
                ttl_seconds=signal.ttl_seconds,
                reason=signal.reason,
            )
            if signal.side == SignalSide.SELL:
                self._handle_exit_signal(signal, trace_id, current_dt)
            else:
                self._handle_entry_signal(signal, trace_id, current_dt)

    def _load_signals_up_to(self, current_dt: datetime) -> None:
        while True:
            event = self._next_event()
            if event is None:
                break
            if _as_utc_naive(event.ts_end) <= current_dt:
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
        trace_value = trace_id or str(uuid.uuid4())
        contract = self.option_selector(signal, now)
        if contract is None:
            self._record_signal(
                signal,
                accepted=False,
                reason="BLOCK:OPTION_NOT_FOUND",
                trace_id=trace_value,
            )
            return

        path = self._build_path(signal, contract, ExecutionMode.ADAPTIVE)
        first_step = path.steps[0]

        size = self._parse_size(signal)
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
                option_quote_ts=contract.quote_ts,
                allow_missing_option_liquidity_metrics=self._risk_ctx.allow_missing_option_liquidity_metrics,
            )
        )

        if not risk_decision.approved:
            reason = risk_decision.reason_code or "BLOCK:RISK"
            self._record_signal(signal, accepted=False, reason=reason, trace_id=trace_value)
            self._record_block(signal, contract, trace_value, reason)
            return

        execution_contract = self._refresh_contract_from_feed(contract, now) or contract
        execution_path = self._build_path(signal, execution_contract, ExecutionMode.ADAPTIVE)
        self._record_signal(signal, accepted=True, reason="PASS", trace_id=trace_value)
        self._submit_order(
            signal, execution_contract, ExecutionMode.ADAPTIVE, execution_path, trace_value
        )

    # ------------------------------------------------------------------
    def _handle_exit_signal(self, signal: SignalEnvelope, trace_id: str, now: datetime) -> None:
        trace_value = trace_id or str(uuid.uuid4())
        contract = self._open_contracts.get(signal.symbol.upper())
        if contract is None:
            if self._cancel_pending_entry(signal.symbol):
                self._record_signal(
                    signal,
                    accepted=True,
                    reason="CANCEL_PENDING_ENTRY",
                    trace_id=trace_value,
                )
                return
            self._record_signal(
                signal,
                accepted=False,
                reason="BLOCK:EXIT_POSITION_NOT_FOUND",
                trace_id=trace_value,
            )
            return

        execution_contract = self._refresh_contract_from_feed(contract, now) or contract
        path = self._build_path(signal, execution_contract, ExecutionMode.ADAPTIVE)
        self._positions_mark[execution_contract.symbol] = execution_contract.mid
        self._record_signal(signal, accepted=True, reason="EXIT", trace_id=trace_value)
        self._submit_order(signal, execution_contract, ExecutionMode.ADAPTIVE, path, trace_value)

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
        current_dt = _as_utc_naive(self.data.datetime.datetime(0))
        signal_context = self._build_context(contract, _as_utc_naive(signal.generated_at))
        original_limit = Decimal(first_step.price)
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
            start_dt=current_dt,
            submit_dt=current_dt,
            attempts=0,
            signal=signal,
            contract=contract,
            side=side,
            mode=mode,
            trace_id=trace_id,
            signal_context=signal_context,
            original_limit_price=original_limit,
            current_limit_price=original_limit,
        )
        self._pending_orders[order.ref] = managed
        self._record_order_event(
            managed,
            event_type="ORDER_SUBMITTED",
            reason_code=signal.signal_code,
            event_dt=current_dt,
            fill_price=None,
            context=self._build_context(contract, current_dt),
        )

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

            stale_reason = self._stale_reject_reason(managed, current_dt)
            if stale_reason is not None:
                self.cancel(order)
                managed.invalid_fill = True
                managed.invalid_fill_reason = stale_reason
                self._record_order_event(
                    managed,
                    event_type="STALE_FILL_REJECTED",
                    reason_code="STALE_FILL_REJECTED",
                    event_dt=current_dt,
                    fill_price=None,
                    context=self._build_context(managed.contract, current_dt),
                    stale_reject_reason=stale_reason,
                )
                del self._pending_orders[ref]
                continue

            elapsed = (current_dt - managed.start_dt).total_seconds()
            if elapsed >= managed.path.ttl_seconds:
                self._handle_path_ttl(managed, ref, order, current_dt)
                continue

            latest_due_step = None
            while managed.next_step < len(managed.path.steps):
                step = managed.path.steps[managed.next_step]
                if elapsed < step.offset_sec:
                    break
                managed.next_step += 1
                latest_due_step = step

            if latest_due_step is None:
                continue

            self.cancel(order)
            new_order = self._resubmit(managed, latest_due_step.price)
            managed.order = new_order
            managed.current_limit_price = Decimal(latest_due_step.price)
            self._pending_orders[new_order.ref] = managed
            del self._pending_orders[ref]
            self._record_order_event(
                managed,
                event_type="ORDER_REPRICE",
                reason_code=latest_due_step.stage,
                event_dt=current_dt,
                fill_price=None,
                context=self._build_context(managed.contract, current_dt),
            )

    def _handle_path_ttl(
        self,
        managed: ManagedOrder,
        ref: int,
        order: bt.Order,
        current_dt: datetime,
    ) -> None:
        # FORCE is the terminal execution stage. Keep the existing marketable
        # order live instead of creating an unbounded resubmission loop.
        if managed.side == OrderSide.SELL and managed.forced_exit:
            return
        self.cancel(order)
        if managed.attempts == 0:
            managed.attempts += 1
            managed.ttl_fallback_done = True
            managed.contract = (
                self._refresh_contract_from_feed(managed.contract, current_dt) or managed.contract
            )
            retry_path = self._build_path(
                managed.signal, managed.contract, ExecutionMode.MARKETABLE
            )
            managed.path = retry_path
            managed.next_step = 1
            managed.start_dt = current_dt
            managed.current_limit_price = retry_path.steps[0].price
            new_order = self._resubmit(managed, retry_path.steps[0].price)
            managed.order = new_order
            self._pending_orders[new_order.ref] = managed
            del self._pending_orders[ref]
            event_type = (
                "EXIT_TTL_FALLBACK" if managed.side == OrderSide.SELL else "ORDER_TTL_FALLBACK"
            )
            self._record_order_event(
                managed,
                event_type=event_type,
                reason_code=event_type,
                event_dt=current_dt,
                fill_price=None,
                context=self._build_context(managed.contract, current_dt),
            )
            return

        if managed.side == OrderSide.SELL:
            managed.forced_exit = True
            managed.attempts += 1
            managed.contract = (
                self._refresh_contract_from_feed(managed.contract, current_dt) or managed.contract
            )
            force_path = self._build_path(managed.signal, managed.contract, ExecutionMode.FORCE)
            managed.path = force_path
            managed.next_step = 1
            managed.start_dt = current_dt
            managed.current_limit_price = force_path.steps[0].price
            new_order = self._resubmit(managed, force_path.steps[0].price)
            managed.order = new_order
            self._pending_orders[new_order.ref] = managed
            del self._pending_orders[ref]
            self._record_order_event(
                managed,
                event_type="EXIT_FORCED_LIQUIDATION",
                reason_code="EXIT_FORCED_LIQUIDATION",
                event_dt=current_dt,
                fill_price=None,
                context=self._build_context(managed.contract, current_dt),
            )
            return

        self._record_block(
            managed.signal,
            managed.contract,
            managed.trace_id,
            "BLOCK:TTL_EXPIRED",
        )
        self._record_order_event(
            managed,
            event_type="ORDER_TTL_EXPIRED",
            reason_code="BLOCK:TTL_EXPIRED",
            event_dt=current_dt,
            fill_price=None,
            context=self._build_context(managed.contract, current_dt),
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
            return self.buy(data=data_feed, exectype=bt.Order.Limit, price=float(price), size=size)
        return self.sell(data=data_feed, exectype=bt.Order.Limit, price=float(price), size=size)

    def _apply_open_chase_policy(self, current_dt: datetime) -> None:
        if not self._separated_exit_enabled:
            return
        for symbol, entry in list(self._open_entry.items()):
            qty = self._positions_qty.get(symbol, Decimal("0"))
            if qty <= 0:
                continue
            signal_code = str(entry.get("signal_code") or "")
            if signal_code not in {"SIG_OPEN_CHASE_BUY", "SIG_OPEN_CHASE_ORB_BUY"}:
                continue
            if self._has_pending_sell(symbol):
                continue
            reason = self._open_chase_exit_reason(symbol, entry, current_dt)
            if reason is None:
                continue
            signal = SignalEnvelope(
                strategy_code=self.strategy_code,
                symbol=symbol,
                signal_code=reason,
                side=SignalSide.SELL,
                confidence=1.0,
                reason={"source": "open_chase_exit_policy"},
                risk_hint={"size": "exit"},
                option_hint={"direction": "reduce"},
                ttl_seconds=30,
                cooldown_seconds=1,
                generated_at=current_dt.replace(tzinfo=timezone.utc),
            )
            contract = self._open_contracts.get(symbol)
            if contract is None:
                continue
            execution_contract = self._refresh_contract_from_feed(contract, current_dt) or contract
            path = self._build_path(signal, execution_contract, ExecutionMode.ADAPTIVE)
            trace_id = str(uuid.uuid4())
            self._record_signal(signal, accepted=True, reason=reason, trace_id=trace_id)
            self._submit_order(
                signal,
                execution_contract,
                ExecutionMode.ADAPTIVE,
                path,
                trace_id,
            )

    def _open_chase_exit_reason(
        self, symbol: str, entry: Dict[str, Any], current_dt: datetime
    ) -> str | None:
        row = self._current_equity_row()
        if row is None:
            return None
        close = self._decimal_from_row(row, "close")
        high = self._decimal_from_row(row, "high")
        if close is None:
            return None
        if high is not None:
            self._entry_highs[symbol] = max(self._entry_highs.get(symbol, Decimal("0")), high)
        context = self._build_context(self._open_contracts[symbol], current_dt)
        vwap = context.get("vwap")
        entry_low = entry.get("entry_bar_low")
        orl = entry.get("opening_range_low")
        entry_price = entry.get("entry_price")
        current_mid = context.get("option_mid")
        opened_at = entry.get("opened_at")
        elapsed = (current_dt - opened_at).total_seconds() if isinstance(opened_at, datetime) else 0
        if (
            bool(getattr(self._settings, "open_chase_hard_stop_below_vwap", True))
            and vwap is not None
            and close < vwap
        ):
            return "OPEN_CHASE_FAILURE_BELOW_VWAP"
        if orl is not None and close < orl:
            return "OPEN_CHASE_FAILURE_BELOW_ORL"
        if (
            bool(getattr(self._settings, "open_chase_hard_stop_below_entry_bar_low", True))
            and entry_low is not None
            and close < entry_low
        ):
            return "OPEN_CHASE_FAILURE_BELOW_ENTRY_LOW"
        if entry_price and current_mid:
            loss_pct = (entry_price - current_mid) / entry_price
            if loss_pct >= Decimal(
                str(getattr(self._settings, "open_chase_max_option_loss_pct", Decimal("0.20")))
            ):
                return "OPEN_CHASE_FAILURE_OPTION_LOSS"
            profit_pct = (current_mid - entry_price) / entry_price
            if (
                profit_pct
                >= Decimal(
                    str(
                        getattr(
                            self._settings, "open_chase_second_take_profit_pct", Decimal("0.50")
                        )
                    )
                )
                and "tp50" not in self._profit_taken[symbol]
            ):
                self._profit_taken[symbol].add("tp50")
                return "OPEN_CHASE_TAKE_PROFIT_50"
            if (
                profit_pct
                >= Decimal(
                    str(
                        getattr(self._settings, "open_chase_first_take_profit_pct", Decimal("0.25"))
                    )
                )
                and "tp25" not in self._profit_taken[symbol]
            ):
                self._profit_taken[symbol].add("tp25")
                return "OPEN_CHASE_TAKE_PROFIT_25"
        no_profit_minutes = int(getattr(self._settings, "open_chase_no_profit_exit_minutes", 10))
        if (
            elapsed >= no_profit_minutes * 60
            and entry_price
            and current_mid
            and current_mid <= entry_price
        ):
            return "OPEN_CHASE_TIME_STOP_NO_PROFIT"
        no_new_high_minutes = int(getattr(self._settings, "open_chase_no_new_high_minutes", 8))
        entry_high = entry.get("entry_bar_high")
        if (
            elapsed >= no_new_high_minutes * 60
            and entry_high is not None
            and self._entry_highs.get(symbol, Decimal("0")) <= entry_high
        ):
            return "OPEN_CHASE_FAILURE_NO_FOLLOW_THROUGH"
        deadline = self._parse_hhmm(
            str(getattr(self._settings, "open_chase_trend_confirm_deadline", "10:00"))
        )
        current_et = current_dt.replace(tzinfo=timezone.utc).astimezone(EASTERN)
        if (
            current_et.time() >= deadline
            and entry_price
            and current_mid
            and current_mid <= entry_price
        ):
            return "OPEN_CHASE_TIME_STOP_NO_TREND_CONFIRM"
        return None

    def _has_pending_sell(self, symbol: str) -> bool:
        for managed in self._pending_orders.values():
            if managed.contract.symbol.upper() == symbol.upper() and managed.side == OrderSide.SELL:
                return True
        return False

    def _force_eod_exits(self, current_dt: datetime) -> None:
        current_et = current_dt.replace(tzinfo=timezone.utc).astimezone(EASTERN)
        if current_et.time() < self._eod_exit_time:
            return
        for symbol, contract in list(self._open_contracts.items()):
            if self._positions_qty.get(symbol, Decimal("0")) <= 0:
                continue
            if self._has_pending_sell(symbol):
                continue
            signal = SignalEnvelope(
                strategy_code=self.strategy_code,
                symbol=symbol,
                signal_code="OPEN_CHASE_EOD_EXIT",
                side=SignalSide.SELL,
                confidence=1.0,
                reason={"source": "eod_forced_exit"},
                risk_hint={"size": "exit"},
                option_hint={"direction": "reduce"},
                ttl_seconds=30,
                cooldown_seconds=1,
                generated_at=current_dt.replace(tzinfo=timezone.utc),
            )
            refreshed = self._refresh_contract_from_feed(contract, current_dt)
            if refreshed is None:
                trace_id = str(uuid.uuid4())
                qty = abs(self._positions_qty.get(symbol, Decimal("0")))
                context = self._build_context(contract, current_dt)
                entry_snapshot = dict(self._open_entry.get(symbol.upper(), {}))
                self._record_signal(
                    signal,
                    accepted=True,
                    reason="EXIT_NO_QUOTE_CONSERVATIVE_MARK",
                    trace_id=trace_id,
                )
                self._record_order_event_payload(
                    signal=signal,
                    contract=contract,
                    trace_id=trace_id,
                    side=OrderSide.SELL,
                    event_type="EXIT_NO_QUOTE_CONSERVATIVE_MARK",
                    reason_code="EXIT_NO_QUOTE_CONSERVATIVE_MARK",
                    event_dt=current_dt,
                    signal_context=context,
                    fill_context=context,
                    mode=ExecutionMode.FORCE,
                    original_limit_price=Decimal("0"),
                    final_limit_price=Decimal("0"),
                    order_submit_time=current_dt,
                    attempts=0,
                    ttl_seconds=0,
                    fill_price=Decimal("0"),
                    quantity=qty,
                    stale_reject_reason="no_quote_at_eod",
                )
                if self.run_id is not None:
                    self.dao.record_trade(
                        {
                            "run_id": self.run_id,
                            "symbol": contract.symbol,
                            "asset_type": "OPTION",
                            "side": "SELL",
                            "quantity": -qty,
                            "price": Decimal("0"),
                            "trade_ts": current_dt,
                            "trace_id": trace_id,
                            "option_right": contract.option_right,
                            "strike": contract.strike,
                            "expiry": contract.expiry,
                            "fees": Decimal("0"),
                            "slippage": Decimal("0"),
                            "reason_code": "EXIT_NO_QUOTE_CONSERVATIVE_MARK",
                        }
                    )
                self._positions_qty[symbol] = Decimal("0")
                self._open_contracts.pop(symbol, None)
                self._open_entry.pop(symbol, None)
                self._emit_lifecycle(
                    "POSITION_CLOSED",
                    trace_id=trace_id,
                    event_time=current_dt,
                    signal_code="OPEN_CHASE_EOD_EXIT",
                    side="SELL",
                    executed_quantity=-qty,
                    fill_price=Decimal("0"),
                    position_before=qty,
                    position_after=Decimal("0"),
                    entry_price=entry_snapshot.get("entry_price"),
                    reason="no_quote_at_eod_conservative_zero_mark",
                    option_right=contract.option_right,
                    strike=contract.strike,
                    expiry=contract.expiry,
                )
                continue
            path = self._build_path(signal, refreshed, ExecutionMode.FORCE)
            trace_id = str(uuid.uuid4())
            self._record_signal(
                signal,
                accepted=True,
                reason="OPEN_CHASE_EOD_EXIT",
                trace_id=trace_id,
            )
            self._submit_order(signal, refreshed, ExecutionMode.FORCE, path, trace_id)

    def _cancel_pending_entry(self, symbol: str) -> bool:
        cancelled = False
        symbol_upper = symbol.upper()
        for ref, managed in list(self._pending_orders.items()):
            if managed.contract.symbol.upper() != symbol_upper:
                continue
            if managed.side != OrderSide.BUY:
                continue
            if managed.order.status not in (bt.Order.Submitted, bt.Order.Accepted):
                continue
            self.cancel(managed.order)
            self._record_block(
                managed.signal,
                managed.contract,
                managed.trace_id,
                "CANCEL_PENDING_ENTRY",
            )
            self._record_order_event(
                managed,
                event_type="ORDER_CANCELLED",
                reason_code="CANCEL_PENDING_ENTRY",
                event_dt=_as_utc_naive(self.data.datetime.datetime(0)),
                fill_price=None,
                context=self._build_context(
                    managed.contract, _as_utc_naive(self.data.datetime.datetime(0))
                ),
            )
            del self._pending_orders[ref]
            cancelled = True
        return cancelled

    def _refresh_contract_from_feed(
        self, contract: ContractSelection, ts: datetime
    ) -> Optional[ContractSelection]:
        feed = self._option_feeds.get(self._feed_key_for_contract(contract))
        if feed is None:
            return None
        df = getattr(getattr(feed, "p", None), "dataname", None)
        if df is None or len(df.index) == 0:
            return None
        ts_aware = (
            ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)
        )
        end = ts_aware.astimezone(df.index.tz)
        rows = df[df.index <= end]
        if rows.empty:
            return None
        row = rows.iloc[-1]
        quote_ts = self._source_quote_timestamp(row, rows.index[-1])
        if max(0.0, (ts_aware - quote_ts).total_seconds()) > 60:
            return None
        try:
            bid = Decimal(str(row["bid"]))
            ask = Decimal(str(row["ask"]))
            mid = Decimal(str(row["mid"]))
        except (InvalidOperation, TypeError, ValueError, KeyError):
            return None
        if bid <= 0 or ask <= bid or mid <= 0:
            return None
        return replace(contract, bid=bid, ask=ask, mid=mid, quote_ts=quote_ts)

    # ------------------------------------------------------------------
    def notify_order(self, order: bt.Order) -> None:
        managed = self._pending_orders.get(order.ref)
        if managed is None:
            return

        if order.status in (bt.Order.Completed, bt.Order.Partial):
            executed_qty = Decimal(str(order.executed.size))
            executed_price = Decimal(str(order.executed.price))
            symbol = managed.contract.symbol
            position_before = self._positions_qty.get(symbol, Decimal("0"))
            entry_snapshot = dict(self._open_entry.get(symbol.upper(), {}))
            current_dt = _as_utc_naive(self.data.datetime.datetime(0))
            fill_context = self._build_context(managed.contract, current_dt)
            stale_reason = self._stale_reject_reason(
                managed, current_dt, context=fill_context, fill_price=executed_price
            )
            if stale_reason is not None and managed.side == OrderSide.BUY:
                managed.invalid_fill = True
                managed.invalid_fill_reason = stale_reason
                self._record_order_event(
                    managed,
                    event_type="INVALID_FILL_DETECTED",
                    reason_code="STALE_FILL_REJECTED",
                    event_dt=current_dt,
                    fill_price=executed_price,
                    context=fill_context,
                    stale_reject_reason=stale_reason,
                )
                raise RuntimeError(
                    f"Broker executed invalid entry fill for order {order.ref}: {stale_reason}"
                )
            if managed.side == OrderSide.BUY:
                self._positions_qty[symbol] += executed_qty
                self._open_contracts[symbol.upper()] = managed.contract
                entry_bar = self._current_equity_row()
                self._open_entry[symbol.upper()] = {
                    "signal_code": managed.signal.signal_code,
                    "opened_at": current_dt,
                    "entry_price": executed_price,
                    "entry_underlying": fill_context.get("underlying_price"),
                    "entry_vwap": fill_context.get("vwap"),
                    "entry_bar_low": self._decimal_from_row(entry_bar, "low"),
                    "entry_bar_high": self._decimal_from_row(entry_bar, "high"),
                    "opening_range_high": fill_context.get("opening_range_high"),
                    "opening_range_low": fill_context.get("opening_range_low"),
                    "invalid_fill": managed.invalid_fill,
                    "invalid_fill_reason": managed.invalid_fill_reason,
                    "ttl_fallback_done": managed.ttl_fallback_done,
                }
                self._entry_highs[symbol.upper()] = self._decimal_from_row(
                    entry_bar, "high"
                ) or Decimal("0")
            else:
                self._positions_qty[symbol] += executed_qty
                if self._positions_qty[symbol] <= 0:
                    self._open_contracts.pop(symbol.upper(), None)
                    self._open_entry.pop(symbol.upper(), None)
                    self._profit_taken.pop(symbol.upper(), None)
            self._positions_mark[symbol] = executed_price
            execution_slippage = self._fill_slippage(
                managed.side,
                executed_price,
                fill_context.get("option_mid"),
            )
            if self.run_id is not None:
                payload = {
                    "run_id": self.run_id,
                    "symbol": managed.contract.symbol,
                    "asset_type": "OPTION",
                    "side": "BUY" if managed.side == OrderSide.BUY else "SELL",
                    "quantity": Decimal(str(order.executed.size)),
                    "price": Decimal(str(order.executed.price)),
                    "trade_ts": _as_utc_naive(self.data.datetime.datetime(0)),
                    "trace_id": managed.trace_id,
                    "option_right": managed.contract.option_right,
                    "strike": managed.contract.strike,
                    "expiry": managed.contract.expiry,
                    "fees": Decimal(str(order.executed.comm or 0)),
                    "slippage": execution_slippage,
                    "reason_code": managed.signal.signal_code,
                }
                self.dao.record_trade(payload)
            self._record_order_event(
                managed,
                event_type="ORDER_FILLED",
                reason_code=managed.signal.signal_code,
                event_dt=current_dt,
                fill_price=executed_price,
                context=fill_context,
                stale_reject_reason=managed.invalid_fill_reason,
            )
            position_after = self._positions_qty.get(symbol, Decimal("0"))
            if managed.side == OrderSide.BUY:
                position_event = (
                    "POSITION_OPENED"
                    if position_before <= 0 < position_after
                    else "POSITION_INCREASED"
                )
                gross_pnl = None
            else:
                position_event = "POSITION_CLOSED" if position_after <= 0 else "POSITION_REDUCED"
                entry_price = entry_snapshot.get("entry_price")
                gross_pnl = (
                    (executed_price - entry_price) * abs(executed_qty) * Decimal("100")
                    if isinstance(entry_price, Decimal)
                    else None
                )
            self._emit_lifecycle(
                position_event,
                trace_id=managed.trace_id,
                order_ref=order.ref,
                event_time=current_dt,
                signal_code=managed.signal.signal_code,
                side=managed.side,
                fill_status=order.getstatusname(),
                executed_quantity=executed_qty,
                fill_price=executed_price,
                fees=Decimal(str(order.executed.comm or 0)),
                slippage=execution_slippage,
                position_before=position_before,
                position_after=position_after,
                entry_price=entry_snapshot.get("entry_price"),
                gross_pnl_before_entry_fees=gross_pnl,
                option_right=managed.contract.option_right,
                strike=managed.contract.strike,
                expiry=managed.contract.expiry,
            )
            del self._pending_orders[order.ref]

        elif order.status in (bt.Order.Canceled, bt.Order.Expired, bt.Order.Rejected):
            fill_reject_reason = self._order_info_value(order, "fill_reject_reason")
            if fill_reject_reason:
                managed.invalid_fill = True
                managed.invalid_fill_reason = str(fill_reject_reason)
                current_dt = _as_utc_naive(self.data.datetime.datetime(0))
                self._record_order_event(
                    managed,
                    event_type="STALE_FILL_REJECTED",
                    reason_code="STALE_FILL_REJECTED",
                    event_dt=current_dt,
                    fill_price=None,
                    context=self._build_context(managed.contract, current_dt),
                    stale_reject_reason=str(fill_reject_reason),
                )
                del self._pending_orders[order.ref]
                return
            event_type = {
                bt.Order.Canceled: "ORDER_CANCELLED",
                bt.Order.Expired: "ORDER_EXPIRED",
                bt.Order.Rejected: "ORDER_REJECTED",
            }.get(order.status, "ORDER_CLOSED")
            self._record_order_event(
                managed,
                event_type=event_type,
                reason_code=event_type,
                event_dt=_as_utc_naive(self.data.datetime.datetime(0)),
                fill_price=None,
                context=self._build_context(
                    managed.contract, _as_utc_naive(self.data.datetime.datetime(0))
                ),
            )
            del self._pending_orders[order.ref]

    def _validate_order_fill(self, order: bt.Order, fill_price: float) -> str | None:
        """Validate a candidate fill before the quote-aware broker executes it."""

        managed = self._pending_orders.get(order.ref)
        if managed is None:
            return None
        current_dt = _as_utc_naive(self.data.datetime.datetime(0))
        context = self._build_context(managed.contract, current_dt)
        return self._stale_reject_reason(
            managed,
            current_dt,
            context=context,
            fill_price=Decimal(str(fill_price)),
        )

    @staticmethod
    def _order_info_value(order: bt.Order, key: str) -> object | None:
        info = getattr(order, "info", None)
        if info is None:
            return None
        if isinstance(info, dict):
            return info.get(key)
        return getattr(info, key, None)

    # ------------------------------------------------------------------
    def _record_signal(
        self,
        signal: SignalEnvelope,
        *,
        accepted: bool,
        reason: str,
        trace_id: str | None = None,
    ) -> None:
        payload = {
            "run_id": self.run_id,
            "ts_end": signal.generated_at,
            "symbol": signal.symbol,
            "asset_type": signal.asset_type.value,
            "signal_code": signal.signal_code,
            "accepted": accepted,
            "reason": reason,
        }
        if self.run_id is not None:
            self.dao.record_signal(payload)
        self._emit_lifecycle(
            "SIGNAL_ACCEPTED" if accepted else "SIGNAL_BLOCKED",
            trace_id=trace_id,
            event_time=signal.generated_at,
            signal_code=signal.signal_code,
            side=signal.side,
            accepted=accepted,
            reason=reason,
            signal_reason=signal.reason,
            risk_hint=signal.risk_hint,
            option_hint=signal.option_hint,
        )
        if accepted and signal.side == SignalSide.SELL:
            self._emit_lifecycle(
                "EXIT_TRIGGERED",
                trace_id=trace_id,
                event_time=signal.generated_at,
                signal_code=signal.signal_code,
                side=signal.side,
                reason=reason,
            )

    # ------------------------------------------------------------------
    def _default_option_selector(
        self, signal: SignalEnvelope, ts: datetime
    ) -> Optional[ContractSelection]:
        # The live strategy expresses every entry as a CALL and exits by selling the
        # originally opened CALL. The default backtest selector mirrors the entry side;
        # exit handling should normally use _open_contracts and not call this fallback.
        option_right = "CALL"
        ts_aware = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
        trade_date = ts_aware.astimezone(EASTERN).date()
        dte_min, dte_max = self._hint_range(signal.option_hint.get("dte"), default=(2, 7))
        delta_low, delta_high = self._hint_decimal_range(
            signal.option_hint.get("delta"), default=(Decimal("0.35"), Decimal("0.45"))
        )
        feed_selected = self._select_from_option_feeds(
            signal.symbol, option_right, ts_aware, delta_low, delta_high
        )
        if feed_selected is not None:
            return feed_selected
        if self._option_feeds:
            return None

        candidates = self.dao.fetch_option_candidates(
            trade_date=trade_date,
            underlying_symbol=signal.symbol,
            option_right=option_right,
            dte_min=dte_min,
            dte_max=dte_max,
        )
        liquidity_required = bool(get_settings().option_liquidity_required)
        filtered: List[tuple[tuple[Decimal | float, ...], ContractSelection]] = []
        for candidate in candidates:
            bid = candidate.bid
            ask = candidate.ask
            mid = candidate.mid
            min_tick = candidate.min_tick
            strike_value = candidate.strike
            if None in (bid, ask, mid, min_tick, strike_value):
                continue
            bid_d = Decimal(str(bid))
            ask_d = Decimal(str(ask))
            mid_d = Decimal(str(mid))
            spread_d = ask_d - bid_d
            if ask_d <= bid_d or spread_d <= 0:
                continue
            threshold = max(Decimal("0.10"), mid_d * Decimal("0.05"))
            if liquidity_required and spread_d > threshold:
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
            if not self._delta_allowed(delta_value, delta_low, delta_high):
                continue
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
                quote_ts=ts_aware,
            )
            strike_distance = (
                abs(selection.strike - underlying) if underlying is not None else Decimal("0")
            )
            sort_key = (
                0.0,
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

    def _select_from_option_feeds(
        self,
        symbol: str,
        option_right: str,
        ts: datetime,
        delta_low: Decimal,
        delta_high: Decimal,
    ) -> Optional[ContractSelection]:
        candidates: List[tuple[tuple[Decimal | float, ...], ContractSelection]] = []
        window_start = ts - timedelta(minutes=15)
        for feed_key, feed in self._option_feeds.items():
            feed_symbol = str(feed_key[0]).upper() if len(feed_key) > 0 else ""
            feed_right = str(feed_key[1]).upper() if len(feed_key) > 1 else ""
            if feed_symbol != symbol.upper() or feed_right != option_right.upper():
                continue
            contract = self._contract_from_feed(feed, symbol, option_right, ts, window_start)
            if contract is None:
                continue
            if not self._delta_allowed(contract.delta, delta_low, delta_high):
                continue
            spread_d = contract.ask - contract.bid
            strike_distance = (
                abs(contract.strike - contract.underlying_price)
                if contract.underlying_price is not None
                else Decimal("0")
            )
            quote_age = (
                max(0.0, (ts - contract.quote_ts).total_seconds()) if contract.quote_ts else 0.0
            )
            sort_key = (
                quote_age,
                Decimal(-contract.volume),
                self._delta_penalty(contract.delta, delta_low, delta_high),
                spread_d,
                Decimal(-contract.open_interest),
                strike_distance,
            )
            candidates.append((sort_key, contract))
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0])
        return candidates[0][1]

    def _contract_from_feed(
        self,
        feed,
        symbol: str,
        option_right: str,
        ts: datetime,
        window_start: datetime,
    ) -> Optional[ContractSelection]:
        line = getattr(feed, "p", None)
        contract_payload = getattr(line, "contract", None) if line is not None else None
        if not isinstance(contract_payload, dict):
            return None
        df = getattr(feed, "p", None).dataname if getattr(feed, "p", None) is not None else None
        if df is None or len(df.index) == 0:
            return None
        end_et = ts.astimezone(timezone.utc).astimezone(df.index.tz)
        start_et = window_start.astimezone(timezone.utc).astimezone(df.index.tz)
        rows = df[(df.index >= start_et) & (df.index <= end_et)]
        if rows.empty:
            return None
        row = rows.iloc[-1]
        quote_ts = self._source_quote_timestamp(row, rows.index[-1])
        if max(0.0, (ts - quote_ts).total_seconds()) > 60:
            return None
        try:
            bid = Decimal(str(row["bid"]))
            ask = Decimal(str(row["ask"]))
            mid = Decimal(str(row["mid"]))
        except (InvalidOperation, TypeError, ValueError, KeyError):
            return None
        if bid <= 0 or ask <= 0 or ask <= bid or mid <= 0:
            return None
        spread = ask - bid
        threshold = max(Decimal("0.10"), mid * Decimal("0.05"))
        if bool(get_settings().option_liquidity_required) and spread > threshold:
            return None
        try:
            strike = Decimal(str(contract_payload["strike"]))
            expiry = contract_payload["expiry"]
            conid = (
                int(contract_payload["conid"])
                if contract_payload.get("conid") is not None
                else None
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return None
        if not isinstance(expiry, datetime):
            expiry = datetime.combine(expiry, datetime.min.time(), tzinfo=timezone.utc)
        trade_date = ts.astimezone(timezone.utc).date()
        dte = (expiry.date() - trade_date).days
        if dte < 0 or dte > 7:
            return None
        volume = int(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else 0
        open_interest = (
            int(row["open_interest"])
            if "open_interest" in row and pd.notna(row["open_interest"])
            else 0
        )
        delta = None
        if "delta" in row and pd.notna(row["delta"]):
            try:
                delta = Decimal(str(row["delta"]))
            except (InvalidOperation, TypeError, ValueError):
                delta = None
        underlying_price = None
        if "underlying_price" in row and pd.notna(row["underlying_price"]):
            try:
                underlying_price = Decimal(str(row["underlying_price"]))
            except (InvalidOperation, TypeError, ValueError):
                underlying_price = None
        return ContractSelection(
            conid=conid,
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            right=option_right,
            bid=bid,
            ask=ask,
            mid=mid,
            min_tick=Decimal("0.01"),
            option_right=option_right,
            open_interest=open_interest,
            volume=volume,
            dte=dte,
            delta=delta,
            underlying_price=underlying_price,
            quote_ts=quote_ts,
        )

    @staticmethod
    def _source_quote_timestamp(row, fallback_index) -> datetime:
        source = row.get("source_quote_ts")
        if source is not None and pd.notna(source):
            value = pd.Timestamp(source).to_pydatetime()
        else:
            value = fallback_index.to_pydatetime()
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _fill_slippage(
        side: OrderSide,
        executed_price: Decimal,
        mid_at_fill: object,
    ) -> Decimal:
        try:
            mid = Decimal(str(mid_at_fill))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal("0")
        if mid <= 0:
            return Decimal("0")
        if side == OrderSide.BUY:
            return executed_price - mid
        return mid - executed_price

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

    def _stale_reject_reason(
        self,
        managed: ManagedOrder,
        current_dt: datetime,
        *,
        context: Dict[str, Any] | None = None,
        fill_price: Decimal | None = None,
    ) -> str | None:
        if not self._stale_guard_enabled or managed.side != OrderSide.BUY:
            return None
        if managed.signal.signal_code not in {"SIG_OPEN_CHASE_BUY", "SIG_OPEN_CHASE_ORB_BUY"}:
            return None
        signal_dt = _as_utc_naive(managed.signal.generated_at)
        elapsed = (current_dt - signal_dt).total_seconds()
        max_elapsed = int(getattr(self._settings, "open_chase_max_signal_to_fill_seconds", 90))
        if elapsed > max_elapsed:
            return f"signal_to_fill_seconds>{max_elapsed}"
        ctx = context or self._build_context(managed.contract, current_dt)
        if ctx.get("option_quote_missing") is True:
            return "option_quote_missing_at_fill"
        if (
            bool(getattr(self._settings, "open_chase_require_above_vwap_at_fill", True))
            and ctx.get("above_vwap") is False
        ):
            return "below_vwap_at_fill"
        if (
            bool(getattr(self._settings, "open_chase_require_above_orh_at_fill", False))
            and ctx.get("above_orh") is False
        ):
            return "below_orh_at_fill"
        spread_pct = ctx.get("option_spread_pct")
        spread_limit = Decimal(
            str(getattr(self._settings, "open_chase_reject_option_spread_pct_gt", Decimal("0.10")))
        )
        if spread_pct is not None and spread_pct > spread_limit:
            return f"option_spread_pct>{spread_limit}"
        signal_underlying = managed.signal_context.get("underlying_price")
        fill_underlying = ctx.get("underlying_price")
        adverse_limit = Decimal(
            str(
                getattr(
                    self._settings,
                    "open_chase_max_underlying_adverse_move_pct",
                    Decimal("0.0025"),
                )
            )
        )
        if signal_underlying and fill_underlying:
            adverse = (signal_underlying - fill_underlying) / signal_underlying
            if adverse > adverse_limit:
                return f"underlying_adverse_move>{adverse_limit}"
        return None

    def _build_context(self, contract: ContractSelection, ts: datetime) -> Dict[str, Any]:
        equity_rows = self._equity_rows_until(ts)
        current = equity_rows.iloc[-1] if not equity_rows.empty else None
        close = self._decimal_from_row(current, "close")
        vwap_value = self._vwap(equity_rows)
        or_high, or_low = self._opening_range(equity_rows, ts)
        quote = self._quote_context(contract, ts)
        return {
            "underlying_price": close,
            "vwap": vwap_value,
            "opening_range_high": or_high,
            "opening_range_low": or_low,
            "above_vwap": None if close is None or vwap_value is None else close > vwap_value,
            "above_orh": None if close is None or or_high is None else close > or_high,
            **quote,
        }

    def _quote_context(self, contract: ContractSelection, ts: datetime) -> Dict[str, Any]:
        refreshed = self._refresh_contract_from_feed(contract, ts)
        source = refreshed or contract
        bid = source.bid
        ask = source.ask
        mid = source.mid
        spread = ask - bid if ask is not None and bid is not None else None
        spread_pct = spread / mid if spread is not None and mid and mid > 0 else None
        return {
            "option_bid": bid,
            "option_ask": ask,
            "option_mid": mid,
            "option_spread_abs": spread,
            "option_spread_pct": spread_pct,
            "option_quote_missing": refreshed is None,
        }

    def _equity_rows_until(self, ts: datetime) -> pd.DataFrame:
        df = getattr(getattr(self.data, "p", None), "dataname", None)
        if df is None or len(df.index) == 0:
            return pd.DataFrame()
        ts_aware = (
            ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)
        )
        end = ts_aware.astimezone(df.index.tz)
        rows = df[df.index <= end]
        idx_et = rows.index.tz_convert(EASTERN)
        return rows[idx_et.time >= time(9, 30)]

    def _current_equity_row(self):
        rows = self._equity_rows_until(_as_utc_naive(self.data.datetime.datetime(0)))
        if rows.empty:
            return None
        return rows.iloc[-1]

    @staticmethod
    def _decimal_from_row(row, column: str) -> Decimal | None:
        if row is None:
            return None
        try:
            value = row[column]
        except (KeyError, TypeError):
            return None
        if pd.isna(value):
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None

    def _vwap(self, rows: pd.DataFrame) -> Decimal | None:
        if rows.empty or "close" not in rows or "volume" not in rows:
            return None
        frame = rows.dropna(subset=["close", "volume"])
        if frame.empty:
            return None
        volume = frame["volume"].astype(float)
        total_volume = volume.sum()
        if total_volume <= 0:
            return None
        value = (frame["close"].astype(float) * volume).sum() / total_volume
        return Decimal(str(value))

    def _opening_range(
        self, rows: pd.DataFrame, ts: datetime
    ) -> tuple[Decimal | None, Decimal | None]:
        if rows.empty:
            return None, None
        minutes = int(getattr(self._settings, "open_chase_opening_range_minutes", 5))
        ts_aware = (
            ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)
        )
        current_et = ts_aware.astimezone(EASTERN)
        start = current_et.replace(hour=9, minute=30, second=0, microsecond=0)
        end = start + timedelta(minutes=max(1, minutes) - 1)
        idx_et = rows.index.tz_convert(EASTERN)
        window = rows[(idx_et >= start) & (idx_et <= end)]
        if window.empty:
            return None, None
        return Decimal(str(window["high"].max())), Decimal(str(window["low"].min()))

    def _record_order_event(
        self,
        managed: ManagedOrder,
        *,
        event_type: str,
        reason_code: str,
        event_dt: datetime,
        fill_price: Decimal | None,
        context: Dict[str, Any],
        stale_reject_reason: str | None = None,
    ) -> None:
        try:
            order_quantity = abs(Decimal(str(managed.order.size)))
        except (InvalidOperation, TypeError, ValueError):
            order_quantity = Decimal(str(self._parse_size(managed.signal)))
        self._record_order_event_payload(
            signal=managed.signal,
            contract=managed.contract,
            trace_id=managed.trace_id,
            side=managed.side,
            event_type=event_type,
            reason_code=reason_code,
            event_dt=event_dt,
            signal_context=managed.signal_context,
            fill_context=context,
            mode=managed.mode,
            original_limit_price=managed.original_limit_price,
            final_limit_price=managed.current_limit_price,
            order_submit_time=managed.submit_dt,
            attempts=managed.attempts,
            ttl_seconds=managed.path.ttl_seconds,
            fill_price=fill_price,
            quantity=order_quantity,
            stale_reject_reason=stale_reject_reason,
            order_ref=managed.order.ref,
        )

    def _record_order_event_payload(
        self,
        *,
        signal: SignalEnvelope,
        contract: ContractSelection,
        trace_id: str,
        side: OrderSide,
        event_type: str,
        reason_code: str,
        event_dt: datetime,
        signal_context: Dict[str, Any],
        fill_context: Dict[str, Any],
        mode: ExecutionMode,
        original_limit_price: Decimal,
        final_limit_price: Decimal,
        order_submit_time: datetime | None = None,
        attempts: int,
        ttl_seconds: int,
        fill_price: Decimal | None,
        quantity: Decimal | None,
        stale_reject_reason: str | None = None,
        order_ref: int | None = None,
    ) -> None:
        signal_time = _as_utc_naive(signal.generated_at).replace(tzinfo=timezone.utc)
        event_time = (
            event_dt if event_dt.tzinfo is not None else event_dt.replace(tzinfo=timezone.utc)
        )
        submit_time = order_submit_time or event_dt
        submit_time = (
            submit_time
            if submit_time.tzinfo is not None
            else submit_time.replace(tzinfo=timezone.utc)
        )
        signal_to_fill = None
        if fill_price is not None or event_type in {"STALE_FILL_REJECTED", "INVALID_FILL_DETECTED"}:
            signal_to_fill = Decimal(
                str(
                    (
                        event_time.astimezone(timezone.utc).replace(tzinfo=None)
                        - _as_utc_naive(signal.generated_at)
                    ).total_seconds()
                )
            )
        is_stale_fill = bool(
            stale_reject_reason and str(stale_reject_reason).startswith("signal_to_fill_seconds>")
        )
        payload = {
            "run_id": self.run_id,
            "trace_id": trace_id,
            "symbol": signal.symbol,
            "asset_type": signal.asset_type.value,
            "signal_code": signal.signal_code,
            "side": side.value if hasattr(side, "value") else str(side),
            "event_type": event_type,
            "reason_code": reason_code,
            "signal_time": signal_time,
            "order_submit_time": submit_time,
            "fill_time": event_time if fill_price is not None else None,
            "signal_to_fill_seconds": signal_to_fill,
            "order_ttl_seconds": ttl_seconds,
            "is_stale_fill": is_stale_fill,
            "order_attempts": attempts,
            "execution_mode": mode.value if hasattr(mode, "value") else str(mode),
            "original_limit_price": original_limit_price,
            "final_limit_price": final_limit_price,
            "fill_price": fill_price,
            "option_bid_at_signal": signal_context.get("option_bid"),
            "option_ask_at_signal": signal_context.get("option_ask"),
            "option_mid_at_signal": signal_context.get("option_mid"),
            "option_bid_at_fill": fill_context.get("option_bid"),
            "option_ask_at_fill": fill_context.get("option_ask"),
            "option_mid_at_fill": fill_context.get("option_mid"),
            "option_spread_abs_at_fill": fill_context.get("option_spread_abs"),
            "option_spread_pct_at_fill": fill_context.get("option_spread_pct"),
            "underlying_price_at_signal": signal_context.get("underlying_price"),
            "underlying_price_at_fill": fill_context.get("underlying_price"),
            "vwap_at_signal": signal_context.get("vwap"),
            "vwap_at_fill": fill_context.get("vwap"),
            "opening_range_high": fill_context.get("opening_range_high")
            or signal_context.get("opening_range_high"),
            "opening_range_low": fill_context.get("opening_range_low")
            or signal_context.get("opening_range_low"),
            "above_vwap_at_signal": signal_context.get("above_vwap"),
            "above_vwap_at_fill": fill_context.get("above_vwap"),
            "above_orh_at_signal": signal_context.get("above_orh"),
            "above_orh_at_fill": fill_context.get("above_orh"),
            "stale_reject_reason": stale_reject_reason,
            "option_right": contract.option_right,
            "strike": contract.strike,
            "expiry": contract.expiry,
            "quantity": quantity,
            "payload": {
                "signal_reason": signal.reason,
                "option_quote_missing_at_fill": fill_context.get("option_quote_missing"),
            },
        }
        if self.run_id is not None:
            self.dao.record_order_event(payload)
        self._emit_lifecycle(
            event_type,
            order_ref=order_ref,
            **payload,
        )

    @staticmethod
    def _parse_hhmm(value: str) -> time:
        hour, minute = value.split(":", 1)
        return time(hour=int(hour), minute=int(minute))

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
    def _delta_allowed(delta: Optional[Decimal], low: Decimal, high: Decimal) -> bool:
        if delta is None:
            return True
        return low <= abs(delta) <= high

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
        if self.run_id is not None:
            self.dao.record_block(payload)
        self._emit_lifecycle(
            "RISK_BLOCKED",
            event_time=signal.generated_at,
            **payload,
        )

    # ------------------------------------------------------------------
    def _normalise_feed_key(self, key: tuple[object, ...]) -> tuple[object, ...]:
        if len(key) >= 5:
            symbol, right, conid, expiry, strike = key[:5]
            return (
                str(symbol).upper(),
                str(right).upper(),
                int(conid) if conid is not None else None,
                str(expiry),
                str(strike),
            )
        symbol, right = key[:2]
        return str(symbol).upper(), str(right).upper()

    def _feed_key_for_contract(self, contract: ContractSelection) -> tuple[object, ...]:
        expiry = (
            contract.expiry.date().isoformat()
            if isinstance(contract.expiry, datetime)
            else str(contract.expiry)
        )
        return (
            contract.symbol.upper(),
            contract.option_right.upper(),
            int(contract.conid) if contract.conid is not None else None,
            expiry,
            str(contract.strike),
        )

    def _resolve_option_feed(self, contract: ContractSelection):
        feed = self._option_feeds.get(self._feed_key_for_contract(contract))
        if feed is None:
            feed = self._option_feeds.get((contract.symbol.upper(), contract.option_right.upper()))
        if feed is None:
            raise RuntimeError(
                f"Option data feed missing for {contract.symbol} {contract.option_right}"
            )
        return feed


def _as_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)
