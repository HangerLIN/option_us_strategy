from __future__ import annotations

import threading
from decimal import Decimal
from typing import Optional

from prometheus_client import Counter, Gauge, Histogram

__all__ = [
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
