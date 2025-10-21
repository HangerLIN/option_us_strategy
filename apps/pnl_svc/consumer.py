from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy.orm import Session, sessionmaker

from libs.db import PnLDAO, RiskStateDAO
from libs.db.models import Order
from libs.infra.metrics import observe_slippage
from libs.infra.redis_bus import RedisBus
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
                side=fill.side,
                quantity=Decimal(fill.fill_quantity),
                price=Decimal(fill.fill_price),
                fees=fees,
                filled_at=fill.filled_at,
            )

            notional = abs(position.quantity) * position.avg_price * Decimal("100")
            states = [
                {
                    "ts": fill.filled_at,
                    "symbol": fill.symbol,
                    "metric_code": "NET_QTY",
                    "metric_value": Decimal(position.quantity),
                    "detail": {"strategy": fill.strategy_code},
                },
                {
                    "ts": fill.filled_at,
                    "symbol": fill.symbol,
                    "metric_code": "REALIZED_PNL",
                    "metric_value": realized,
                    "detail": {"strategy": fill.strategy_code},
                },
                {
                    "ts": fill.filled_at,
                    "symbol": fill.symbol,
                    "metric_code": "NOTIONAL",
                    "metric_value": notional,
                    "detail": {"strategy": fill.strategy_code},
                },
            ]
            RiskStateDAO(session).insert_states(states)
            order = session.get(Order, fill.order_id)
            limit_price: Optional[Decimal] = None
            if order and order.limit_price is not None:
                limit_price = Decimal(order.limit_price)
            observe_slippage(fill.strategy_code, limit_price, Decimal(fill.fill_price))
            session.commit()
        except Exception:
            session.rollback()
            LOGGER.exception(
                "pnl_consumer.apply_failed", symbol=fill.symbol, strategy=fill.strategy_code
            )
        finally:
            session.close()
