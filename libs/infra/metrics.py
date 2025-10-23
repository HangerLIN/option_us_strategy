from __future__ import annotations

import threading
from contextlib import contextmanager
from decimal import Decimal
from typing import Iterator, Optional

from prometheus_client import Counter, Gauge, Histogram

__all__ = [
    # 已有指标
    "observe_bar_latency",
    "record_signal_emitted",
    "record_risk_block",
    "record_force_close",
    "set_kill_switch_state",
    "set_risk_vix",
    "set_risk_drawdown",
    "set_risk_used_r",
    "record_order_submission",
    "record_order_accept",
    "record_order_block",
    "observe_slippage",
    # rt_engine新增指标
    "record_tick_received",
    "set_subscribed_symbols_count",
    "set_ibkr_connection_status",
    "record_ibkr_reconnection",
    "observe_indicator_calculation",
    "record_indicator_failure",
    "set_cached_bars_count",
    "record_top5_selection",
    "observe_top5_calculation",
    "set_top5_candidates_count",
    # signal_svc新增指标
    "record_signal_by_type",
    "observe_signal_generation",
    "set_active_signals_count",
    "record_signal_filtered",
    "set_signal_generation_rate",
    "record_websocket_push_failure",
    # risk_svc新增指标
    "set_position_count",
    "set_total_exposure",
    "set_max_single_exposure_pct",
    "set_account_equity",
    "set_available_capital",
    "set_capital_utilization",
    "set_limit_usage",
    "record_limit_approaching",
    # exec_svc新增指标
    "set_orders_by_status",
    "observe_order_execution_latency",
    "record_order_timeout",
    "set_pending_order_age",
    "observe_option_chain_fetch",
    "record_option_chain_failure",
    "observe_delta_deviation",
    "record_no_suitable_contract",
    # pnl_svc新增指标
    "set_realized_pnl_today",
    "set_unrealized_pnl_today",
    "set_cumulative_pnl",
    "set_trades_count_today",
    "set_win_rate",
    "observe_trade_pnl",
    "set_position_market_value",
    "set_position_cost_basis",
    "set_avg_holding_duration",
    "record_exit_trigger",
]


_BAR_LATENCY_SECONDS = Histogram(
    "bars_latency_seconds",
    "Latency between bar close and processing.",
    buckets=(
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        20.0,
    ),
)

_SIGNALS_EMITTED_TOTAL = Counter(
    "signals_emitted_total",
    "Total number of trading signals emitted.",
    labelnames=("signal_code",),
)

_RISK_BLOCK_TOTAL = Counter(
    "risk_block_events_total",
    "Number of orders blocked by risk evaluation grouped by reason.",
    labelnames=("reason_code",),
)

_FORCE_CLOSE_TOTAL = Counter(
    "risk_force_close_total",
    "Number of force close actions triggered by risk module.",
    labelnames=("reason_code",),
)

_KILL_SWITCH_GAUGE = Gauge(
    "risk_kill_switch_state",
    "Current kill-switch state (1 active, 0 inactive).",
)

_RISK_VIX_GAUGE = Gauge(
    "risk_vix_last",
    "Latest VIX reading as seen by risk module.",
)

_RISK_DRAWDOWN_GAUGE = Gauge(
    "risk_drawdown_r",
    "Current drawdown expressed in R units.",
)

_RISK_USED_R_GAUGE = Gauge(
    "risk_used_r",
    "Total risk units currently deployed.",
)

_ORDERS_SUBMITTED_TOTAL = Counter(
    "orders_submitted_total",
    "Orders submitted to execution.",
    labelnames=("mode",),
)

_ORDERS_ACCEPTED_TOTAL = Counter(
    "orders_accepted_total",
    "Orders that passed validation and were routed to the broker.",
    labelnames=("mode",),
)

_ORDERS_BLOCKED_TOTAL = Counter(
    "orders_blocked_total",
    "Orders rejected before reaching the broker.",
    labelnames=("stage", "reason_code"),
)

_ORDERS_CONVERSION_RATE = Gauge(
    "orders_conversion_rate",
    "Ratio of accepted orders to total submissions.",
)

_ORDER_SLIPPAGE_BPS = Histogram(
    "order_slippage_bps",
    "Observed absolute slippage between limit and fill price expressed in basis points.",
    labelnames=("strategy_code",),
    buckets=(
        0.5,
        1,
        2,
        3,
        5,
        7,
        10,
        15,
        20,
        30,
        40,
        50,
        75,
        100,
        150,
        200,
        300,
        500,
        750,
        1000,
    ),
)

# ============================================================
# rt_engine 指标
# ============================================================

_RT_TICKS_RECEIVED_TOTAL = Counter(
    "rt_ticks_received_total",
    "Total number of tick data received.",
    labelnames=("symbol",),
)

_RT_SUBSCRIBED_SYMBOLS = Gauge(
    "rt_subscribed_symbols_count",
    "Current number of subscribed symbols.",
)

_RT_IBKR_CONNECTION_STATUS = Gauge(
    "rt_ibkr_connection_status",
    "IBKR connection status (1=connected, 0=disconnected).",
)

_RT_IBKR_RECONNECTIONS = Counter(
    "rt_ibkr_reconnections_total",
    "Number of IBKR reconnection attempts.",
    labelnames=("reason",),
)

_RT_INDICATOR_CALCULATION_SECONDS = Histogram(
    "rt_indicator_calculation_seconds",
    "Time spent calculating indicators.",
    labelnames=("indicator",),
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

_RT_INDICATOR_FAILURES = Counter(
    "rt_indicator_failures_total",
    "Number of indicator calculation failures.",
    labelnames=("indicator", "reason"),
)

_RT_CACHED_BARS = Gauge(
    "rt_cached_bars_count",
    "Number of cached bar data points.",
    labelnames=("symbol", "timeframe"),
)

_RT_TOP5_SELECTION_TOTAL = Counter(
    "rt_top5_selection_total",
    "Total number of Top5 selection runs.",
)

_RT_TOP5_CALCULATION_SECONDS = Histogram(
    "rt_top5_calculation_seconds",
    "Time spent on Top5 calculation.",
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0),
)

_RT_TOP5_CANDIDATES = Gauge(
    "rt_top5_candidates_count",
    "Number of candidates meeting Top5 criteria.",
)

# ============================================================
# signal_svc 指标
# ============================================================

_SIGNALS_BY_TYPE = Counter(
    "signals_by_type_total",
    "Signals classified by type and direction.",
    labelnames=("type", "direction"),
)

_SIGNAL_GENERATION_SECONDS = Histogram(
    "signal_generation_latency_seconds",
    "Time spent generating signals.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0),
)

_ACTIVE_SIGNALS = Gauge(
    "signals_active_count",
    "Current number of active signals.",
    labelnames=("status",),
)

_SIGNALS_FILTERED = Counter(
    "signals_filtered_total",
    "Number of signals filtered out.",
    labelnames=("reason",),
)

_SIGNAL_GENERATION_RATE = Gauge(
    "signals_generation_rate_per_minute",
    "Signal generation rate per minute.",
)

_WEBSOCKET_PUSH_FAILURES = Counter(
    "signals_websocket_push_failures_total",
    "WebSocket signal push failures.",
)

# ============================================================
# risk_svc 指标
# ============================================================

_RISK_POSITION_COUNT = Gauge(
    "risk_position_count",
    "Current number of open positions.",
)

_RISK_TOTAL_EXPOSURE = Gauge(
    "risk_total_exposure_usd",
    "Total risk exposure in USD.",
)

_RISK_MAX_SINGLE_EXPOSURE = Gauge(
    "risk_max_single_exposure_pct",
    "Maximum single position exposure as percentage.",
)

_RISK_ACCOUNT_EQUITY = Gauge(
    "risk_account_equity_usd",
    "Current account equity in USD.",
)

_RISK_AVAILABLE_CAPITAL = Gauge(
    "risk_available_capital_usd",
    "Available capital for trading.",
)

_RISK_CAPITAL_UTILIZATION = Gauge(
    "risk_capital_utilization_pct",
    "Capital utilization percentage.",
)

_RISK_LIMIT_USAGE = Gauge(
    "risk_limit_usage_pct",
    "Risk limit usage percentage.",
    labelnames=("limit_type",),
)

_RISK_LIMIT_APPROACHING = Counter(
    "risk_limit_approaching_total",
    "Number of times limit approaching threshold was hit.",
    labelnames=("limit_type", "threshold"),
)

# ============================================================
# exec_svc 指标
# ============================================================

_ORDERS_BY_STATUS = Gauge(
    "orders_by_status_count",
    "Number of orders by status.",
    labelnames=("status",),
)

_ORDER_EXECUTION_LATENCY = Histogram(
    "order_execution_latency_seconds",
    "Order execution latency.",
    labelnames=("stage",),
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
)

_ORDER_TIMEOUT_TOTAL = Counter(
    "orders_timeout_total",
    "Number of order timeouts.",
    labelnames=("timeout_type",),
)

_PENDING_ORDER_AGE = Gauge(
    "orders_pending_age_seconds",
    "Age of oldest pending order in seconds.",
)

_OPTION_CHAIN_FETCH_SECONDS = Histogram(
    "option_chain_fetch_latency_seconds",
    "Option chain fetch latency.",
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0),
)

_OPTION_CHAIN_FAILURES = Counter(
    "option_chain_fetch_failures_total",
    "Option chain fetch failures.",
    labelnames=("reason",),
)

_OPTION_DELTA_DEVIATION = Histogram(
    "option_delta_deviation",
    "Deviation from target delta.",
    labelnames=("target_delta",),
    buckets=(0.01, 0.02, 0.05, 0.1, 0.15, 0.2),
)

_NO_SUITABLE_CONTRACT = Counter(
    "option_no_suitable_contract_total",
    "Number of times no suitable option contract was found.",
)

# ============================================================
# pnl_svc 指标
# ============================================================

_PNL_REALIZED_TODAY = Gauge(
    "pnl_realized_today_usd",
    "Realized PnL for today in USD.",
)

_PNL_UNREALIZED_TODAY = Gauge(
    "pnl_unrealized_today_usd",
    "Unrealized PnL for today in USD.",
)

_PNL_CUMULATIVE = Gauge(
    "pnl_cumulative_usd",
    "Cumulative PnL in USD.",
)

_PNL_TRADES_COUNT_TODAY = Gauge(
    "pnl_trades_count_today",
    "Number of trades executed today.",
)

_PNL_WIN_RATE = Gauge(
    "pnl_win_rate_pct",
    "Winning trade percentage.",
)

_PNL_PER_TRADE = Histogram(
    "pnl_per_trade_usd",
    "PnL distribution per trade.",
    buckets=(-1000, -500, -200, -100, -50, 0, 50, 100, 200, 500, 1000, 2000),
)

_PNL_POSITION_MARKET_VALUE = Gauge(
    "pnl_position_market_value_usd",
    "Current position market value.",
)

_PNL_POSITION_COST_BASIS = Gauge(
    "pnl_position_cost_basis_usd",
    "Current position cost basis.",
)

_PNL_AVG_HOLDING_DURATION = Gauge(
    "pnl_avg_holding_duration_minutes",
    "Average holding duration in minutes.",
)

_PNL_EXIT_TRIGGER = Counter(
    "pnl_exit_trigger_total",
    "Exit trigger events.",
    labelnames=("reason",),
)


class _OrderLifecycleMetrics:
    """Track aggregate order counts to maintain conversion gauge."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._submitted = 0
        self._accepted = 0

    def record_submission(self, mode: str) -> None:
        _ORDERS_SUBMITTED_TOTAL.labels(mode=mode).inc()
        with self._lock:
            self._submitted += 1
            self._update_rate()

    def record_accept(self, mode: str) -> None:
        _ORDERS_ACCEPTED_TOTAL.labels(mode=mode).inc()
        with self._lock:
            self._accepted += 1
            self._update_rate()

    def record_block(self, stage: str, reason_code: str) -> None:
        _ORDERS_BLOCKED_TOTAL.labels(stage=stage, reason_code=reason_code).inc()
        with self._lock:
            self._update_rate()

    def _update_rate(self) -> None:
        if self._submitted == 0:
            _ORDERS_CONVERSION_RATE.set(0)
            return
        _ORDERS_CONVERSION_RATE.set(self._accepted / self._submitted)


_ORDER_METRICS = _OrderLifecycleMetrics()


def observe_bar_latency(latency_seconds: float) -> None:
    """Record latency between bar close and processing."""

    if latency_seconds < 0:
        latency_seconds = 0.0
    _BAR_LATENCY_SECONDS.observe(latency_seconds)


def record_signal_emitted(signal_code: str) -> None:
    """Increment counter for emitted signals."""

    _SIGNALS_EMITTED_TOTAL.labels(signal_code=signal_code).inc()


def record_risk_block(reason_code: str) -> None:
    """Increment counter for risk block reasons."""

    _RISK_BLOCK_TOTAL.labels(reason_code=reason_code).inc()


def record_force_close(reason_code: str) -> None:
    """Increment counter for force-close triggers."""

    _FORCE_CLOSE_TOTAL.labels(reason_code=reason_code).inc()


def set_kill_switch_state(active: bool) -> None:
    """Expose kill switch state as a gauge."""

    _KILL_SWITCH_GAUGE.set(1 if active else 0)


def set_risk_vix(value: Optional[Decimal]) -> None:
    """Update VIX gauge."""

    if value is None:
        return
    try:
        _RISK_VIX_GAUGE.set(float(value))
    except (ValueError, TypeError):  # pragma: no cover - defensive
        return


def set_risk_drawdown(value: Optional[Decimal]) -> None:
    if value is None:
        return
    try:
        _RISK_DRAWDOWN_GAUGE.set(float(value))
    except (ValueError, TypeError):  # pragma: no cover - defensive
        return


def set_risk_used_r(value: Optional[Decimal]) -> None:
    if value is None:
        return
    try:
        _RISK_USED_R_GAUGE.set(float(value))
    except (ValueError, TypeError):  # pragma: no cover - defensive
        return


def record_order_submission(mode: str) -> None:
    """Mark that an order submission attempt occurred."""

    _ORDER_METRICS.record_submission(mode)


def record_order_accept(mode: str) -> None:
    """Mark that an order passed validation and was routed."""

    _ORDER_METRICS.record_accept(mode)


def record_order_block(stage: str, reason_code: str) -> None:
    """Mark that an order was blocked prior to routing."""

    _ORDER_METRICS.record_block(stage, reason_code)


def observe_slippage(
    strategy_code: Optional[str], limit_price: Optional[Decimal], fill_price: Decimal
) -> None:
    """Record slippage in basis points when both prices are available."""

    if strategy_code is None or limit_price is None:
        return
    if limit_price <= 0:
        return
    diff = abs(fill_price - limit_price)
    try:
        bps = float((diff / limit_price) * Decimal("10000"))
    except (ZeroDivisionError, OverflowError):
        return
    _ORDER_SLIPPAGE_BPS.labels(strategy_code=strategy_code).observe(bps)


# ============================================================
# rt_engine 函数
# ============================================================

def record_tick_received(symbol: str) -> None:
    """Record a tick data reception."""
    _RT_TICKS_RECEIVED_TOTAL.labels(symbol=symbol).inc()


def set_subscribed_symbols_count(count: int) -> None:
    """Update the number of subscribed symbols."""
    _RT_SUBSCRIBED_SYMBOLS.set(count)


def set_ibkr_connection_status(connected: bool) -> None:
    """Update IBKR connection status."""
    _RT_IBKR_CONNECTION_STATUS.set(1 if connected else 0)


def record_ibkr_reconnection(reason: str) -> None:
    """Record an IBKR reconnection attempt."""
    _RT_IBKR_RECONNECTIONS.labels(reason=reason).inc()


@contextmanager
def observe_indicator_calculation(indicator: str) -> Iterator[None]:
    """Context manager to measure indicator calculation time."""
    with _RT_INDICATOR_CALCULATION_SECONDS.labels(indicator=indicator).time():
        yield


def record_indicator_failure(indicator: str, reason: str) -> None:
    """Record an indicator calculation failure."""
    _RT_INDICATOR_FAILURES.labels(indicator=indicator, reason=reason).inc()


def set_cached_bars_count(symbol: str, timeframe: str, count: int) -> None:
    """Update cached bars count for a symbol."""
    _RT_CACHED_BARS.labels(symbol=symbol, timeframe=timeframe).set(count)


def record_top5_selection() -> None:
    """Record a Top5 selection execution."""
    _RT_TOP5_SELECTION_TOTAL.inc()


def observe_top5_calculation(duration_seconds: float) -> None:
    """Record Top5 calculation duration."""
    _RT_TOP5_CALCULATION_SECONDS.observe(duration_seconds)


def set_top5_candidates_count(count: int) -> None:
    """Update the number of Top5 candidates."""
    _RT_TOP5_CANDIDATES.set(count)


# ============================================================
# signal_svc 函数
# ============================================================

def record_signal_by_type(signal_type: str, direction: str) -> None:
    """Record signal by type (entry/exit) and direction (long/short)."""
    _SIGNALS_BY_TYPE.labels(type=signal_type, direction=direction).inc()


def observe_signal_generation(duration_seconds: float) -> None:
    """Record signal generation latency."""
    _SIGNAL_GENERATION_SECONDS.observe(duration_seconds)


def set_active_signals_count(status: str, count: int) -> None:
    """Update active signals count by status."""
    _ACTIVE_SIGNALS.labels(status=status).set(count)


def record_signal_filtered(reason: str) -> None:
    """Record a filtered signal."""
    _SIGNALS_FILTERED.labels(reason=reason).inc()


def set_signal_generation_rate(rate: float) -> None:
    """Update signal generation rate per minute."""
    _SIGNAL_GENERATION_RATE.set(rate)


def record_websocket_push_failure() -> None:
    """Record a WebSocket push failure."""
    _WEBSOCKET_PUSH_FAILURES.inc()


# ============================================================
# risk_svc 函数
# ============================================================

def set_position_count(count: int) -> None:
    """Update current position count."""
    _RISK_POSITION_COUNT.set(count)


def set_total_exposure(amount_usd: float) -> None:
    """Update total exposure in USD."""
    _RISK_TOTAL_EXPOSURE.set(amount_usd)


def set_max_single_exposure_pct(percentage: float) -> None:
    """Update maximum single position exposure percentage."""
    _RISK_MAX_SINGLE_EXPOSURE.set(percentage)


def set_account_equity(amount_usd: float) -> None:
    """Update account equity in USD."""
    _RISK_ACCOUNT_EQUITY.set(amount_usd)


def set_available_capital(amount_usd: float) -> None:
    """Update available capital."""
    _RISK_AVAILABLE_CAPITAL.set(amount_usd)


def set_capital_utilization(percentage: float) -> None:
    """Update capital utilization percentage."""
    _RISK_CAPITAL_UTILIZATION.set(percentage)


def set_limit_usage(limit_type: str, percentage: float) -> None:
    """Update risk limit usage percentage."""
    _RISK_LIMIT_USAGE.labels(limit_type=limit_type).set(percentage)


def record_limit_approaching(limit_type: str, threshold: str) -> None:
    """Record a limit approaching event."""
    _RISK_LIMIT_APPROACHING.labels(limit_type=limit_type, threshold=threshold).inc()


# ============================================================
# exec_svc 函数
# ============================================================

def set_orders_by_status(status: str, count: int) -> None:
    """Update order count by status."""
    _ORDERS_BY_STATUS.labels(status=status).set(count)


def observe_order_execution_latency(stage: str, duration_seconds: float) -> None:
    """Record order execution latency for a specific stage."""
    _ORDER_EXECUTION_LATENCY.labels(stage=stage).observe(duration_seconds)


def record_order_timeout(timeout_type: str) -> None:
    """Record an order timeout."""
    _ORDER_TIMEOUT_TOTAL.labels(timeout_type=timeout_type).inc()


def set_pending_order_age(age_seconds: float) -> None:
    """Update the age of the oldest pending order."""
    _PENDING_ORDER_AGE.set(age_seconds)


def observe_option_chain_fetch(duration_seconds: float) -> None:
    """Record option chain fetch latency."""
    _OPTION_CHAIN_FETCH_SECONDS.observe(duration_seconds)


def record_option_chain_failure(reason: str) -> None:
    """Record an option chain fetch failure."""
    _OPTION_CHAIN_FAILURES.labels(reason=reason).inc()


def observe_delta_deviation(target_delta: str, deviation: float) -> None:
    """Record deviation from target delta."""
    _OPTION_DELTA_DEVIATION.labels(target_delta=target_delta).observe(abs(deviation))


def record_no_suitable_contract() -> None:
    """Record inability to find suitable option contract."""
    _NO_SUITABLE_CONTRACT.inc()


# ============================================================
# pnl_svc 函数
# ============================================================

def set_realized_pnl_today(amount_usd: float) -> None:
    """Update today's realized PnL."""
    _PNL_REALIZED_TODAY.set(amount_usd)


def set_unrealized_pnl_today(amount_usd: float) -> None:
    """Update today's unrealized PnL."""
    _PNL_UNREALIZED_TODAY.set(amount_usd)


def set_cumulative_pnl(amount_usd: float) -> None:
    """Update cumulative PnL."""
    _PNL_CUMULATIVE.set(amount_usd)


def set_trades_count_today(count: int) -> None:
    """Update today's trade count."""
    _PNL_TRADES_COUNT_TODAY.set(count)


def set_win_rate(percentage: float) -> None:
    """Update win rate percentage."""
    _PNL_WIN_RATE.set(percentage)


def observe_trade_pnl(amount_usd: float) -> None:
    """Record PnL for a single trade."""
    _PNL_PER_TRADE.observe(amount_usd)


def set_position_market_value(amount_usd: float) -> None:
    """Update position market value."""
    _PNL_POSITION_MARKET_VALUE.set(amount_usd)


def set_position_cost_basis(amount_usd: float) -> None:
    """Update position cost basis."""
    _PNL_POSITION_COST_BASIS.set(amount_usd)


def set_avg_holding_duration(minutes: float) -> None:
    """Update average holding duration in minutes."""
    _PNL_AVG_HOLDING_DURATION.set(minutes)


def record_exit_trigger(reason: str) -> None:
    """Record an exit trigger event."""
    _PNL_EXIT_TRIGGER.labels(reason=reason).inc()
