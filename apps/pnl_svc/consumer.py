from __future__ import annotations

import asyncio
from datetime import date
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy.orm import Session, sessionmaker

from libs.core import EASTERN, get_settings
from libs.db import PnLDAO, RiskStateDAO
from libs.db.models import Order
from libs.infra.metrics import (
    observe_slippage,
    observe_trade_pnl,
    set_cumulative_pnl,
    set_position_cost_basis,
    set_position_market_value,
    set_realized_pnl_today,
    set_trades_count_today,
    set_unrealized_pnl_today,
    set_win_rate,
)
from libs.infra.redis_bus import RedisBus
from libs.schemas.assets import AssetType
from libs.schemas.events import ExecutionFill

LOGGER = structlog.get_logger(__name__)


class ExecutionFillConsumer:
    """Consume execution fills and maintain live PnL/position state."""

    STREAM = "execution_fills"
    GROUP = "pnl-svc"
    CONSUMER = "pnl-consumer"

    def __init__(self, redis_bus: RedisBus, session_factory: sessionmaker) -> None:
        self._redis_bus = redis_bus
        self._session_factory = session_factory
        self._task: Optional[asyncio.Task] = None
        self._running = False
        settings = get_settings()
        self._metrics_enabled = bool(settings.monitoring_enabled)
        self._current_trade_date: Optional[date] = None
        self._today_realized = Decimal("0")
        self._today_trades = 0
        self._today_wins = 0
        self._cumulative_realized = Decimal("0")

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self._redis_bus.ensure_group(self.STREAM, self.GROUP, start_id="0")
        self._task = asyncio.create_task(self._run())
        LOGGER.info("pnl_consumer.started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:  # pragma: no cover - cancellation path
                pass
        await self._redis_bus.close()
        LOGGER.info("pnl_consumer.stopped")

    def _emit_metric(self, func, *args) -> None:
        if not self._metrics_enabled:
            return
        try:
            func(*args)
        except Exception:  # pragma: no cover - metrics best-effort
            pass

    def _rotate_counters(self, trade_date: date) -> None:
        if self._current_trade_date == trade_date:
            return
        self._current_trade_date = trade_date
        self._today_realized = Decimal("0")
        self._today_trades = 0
        self._today_wins = 0

    def _update_metrics(
        self,
        fill: ExecutionFill,
        realized: Decimal,
        notional: Decimal,
        position,
    ) -> None:
        if not self._metrics_enabled:
            return
        trade_date = fill.filled_at.astimezone(EASTERN).date()
        self._rotate_counters(trade_date)

        self._today_realized += realized
        self._cumulative_realized += realized
        self._today_trades += 1
        if realized > 0:
            self._today_wins += 1

        win_rate_pct = (
            float(self._today_wins) / float(self._today_trades) * 100
            if self._today_trades > 0
            else 0.0
        )

        def _to_float(value: Decimal) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):  # pragma: no cover - defensive
                return 0.0

        market_value = abs(notional)
        multiplier = Decimal("100") if fill.asset_type == AssetType.OPTION else Decimal("1")
        cost_basis = abs(Decimal(position.quantity)) * Decimal(position.avg_price) * multiplier

        self._emit_metric(set_realized_pnl_today, _to_float(self._today_realized))
        self._emit_metric(set_cumulative_pnl, _to_float(self._cumulative_realized))
        self._emit_metric(set_trades_count_today, self._today_trades)
        self._emit_metric(set_win_rate, win_rate_pct)
        self._emit_metric(observe_trade_pnl, _to_float(realized))
        self._emit_metric(set_position_market_value, _to_float(market_value))
        self._emit_metric(set_position_cost_basis, _to_float(cost_basis))
        self._emit_metric(set_unrealized_pnl_today, 0.0)

    async def _run(self) -> None:
        while self._running:
            try:
                entries = await self._redis_bus.consume(
                    self.STREAM,
                    self.GROUP,
                    self.CONSUMER,
                    count=50,
                    block_ms=5000,
                )
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("pnl_consumer.consume_failed")
                await asyncio.sleep(1)
                continue

            if not entries:
                continue

            for entry in entries:
                payload = entry.get("payload", {})
                try:
                    fill = ExecutionFill(**payload)
                except Exception:
                    LOGGER.exception("pnl_consumer.invalid_payload", payload=payload)
                    continue
                await self._process_fill(fill)

    async def _process_fill(self, fill: ExecutionFill) -> None:
        if not fill.strategy_code:
            LOGGER.warning("pnl_consumer.missing_strategy", trace_id=fill.trace_id)
            return

        session: Session = self._session_factory()
        try:
            dao = PnLDAO(session)
            fees = Decimal(fill.fees or 0)
            realized, position = dao.apply_fill(
                strategy_code=fill.strategy_code,
                symbol=fill.symbol,
                asset_type=fill.asset_type.value,
                side=fill.side,
                quantity=Decimal(fill.fill_quantity),
                price=Decimal(fill.fill_price),
                fees=fees,
                filled_at=fill.filled_at,
            )

            multiplier = Decimal("100") if fill.asset_type == AssetType.OPTION else Decimal("1")
            notional = abs(position.quantity) * position.avg_price * multiplier
            detail = {"strategy": fill.strategy_code, "asset_type": fill.asset_type.value}
            states = [
                {
                    "ts": fill.filled_at,
                    "symbol": fill.symbol,
                    "metric_code": "NET_QTY",
                    "metric_value": Decimal(position.quantity),
                    "detail": detail,
                },
                {
                    "ts": fill.filled_at,
                    "symbol": fill.symbol,
                    "metric_code": "REALIZED_PNL",
                    "metric_value": realized,
                    "detail": detail,
                },
                {
                    "ts": fill.filled_at,
                    "symbol": fill.symbol,
                    "metric_code": "NOTIONAL",
                    "metric_value": notional,
                    "detail": detail,
                },
            ]
            RiskStateDAO(session).insert_states(states)
            order = session.get(Order, fill.order_id)
            limit_price: Optional[Decimal] = None
            if order and order.limit_price is not None:
                limit_price = Decimal(order.limit_price)
            observe_slippage(fill.strategy_code, limit_price, Decimal(fill.fill_price))
            self._update_metrics(fill, realized, notional, position)
            session.commit()
        except Exception:
            session.rollback()
            LOGGER.exception(
                "pnl_consumer.apply_failed", symbol=fill.symbol, strategy=fill.strategy_code
            )
        finally:
            session.close()
