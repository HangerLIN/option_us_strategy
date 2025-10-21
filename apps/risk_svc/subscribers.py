from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional

from sqlalchemy.orm import Session, sessionmaker

import structlog

from libs.infra.redis_bus import RedisBus
from libs.schemas.events import ExecutionFill, RiskParamReload
from libs.schemas.risk import RiskLimits
from libs.db.dao import PnLIntradayDAO, RiskStateDAO, StrategyPositionDAO

from .service import RiskService
from .state import RiskStateAggregator

LOGGER = structlog.get_logger(__name__)

_OPTION_MULTIPLIER = Decimal("100")

StreamHandler = Callable[[Dict[str, Any]], Awaitable[None]]


class RiskStreamConsumers:
    """Manage RiskSvc Redis stream subscriptions."""

    def __init__(
        self,
        redis_bus: RedisBus,
        risk_service: RiskService,
        *,
        session_factory: sessionmaker,
        on_limits_reloaded: Optional[Callable[[RiskLimits], None]] = None,
        state_aggregator: Optional[RiskStateAggregator] = None,
    ) -> None:
        self._bus = redis_bus
        self._session_factory = session_factory
        self._risk_service = risk_service
        self._on_limits_reloaded = on_limits_reloaded
        self._state_aggregator = state_aggregator
        self._tasks: list[asyncio.Task[Any]] = []

    async def ensure_groups(self) -> None:
        await asyncio.gather(
            self._bus.ensure_group("bars_closed", "risk_svc", start_id="0"),
            self._bus.ensure_group("signals", "risk_svc", start_id="0"),
            self._bus.ensure_group("execution_fills", "risk_svc", start_id="0"),
            self._bus.ensure_group("risk_param_reload", "risk_svc", start_id="0"),
        )

    def start(self) -> Iterable[asyncio.Task[Any]]:
        consumers = [
            ("bars_closed", "risk-bars", self._handle_bars_closed),
            ("signals", "risk-signals", self._handle_signal),
            ("execution_fills", "risk-fills", self._handle_execution_fill),
            ("risk_param_reload", "risk-params", self._handle_param_reload),
        ]
        for stream, consumer_name, handler in consumers:
            task = asyncio.create_task(self._consume_loop(stream, consumer_name, handler))
            self._tasks.append(task)
        return tuple(self._tasks)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                LOGGER.debug("risk_subscriber.task_cancelled", name=task.get_name())
        self._tasks.clear()

    async def _consume_loop(self, stream: str, consumer: str, handler: StreamHandler) -> None:
        LOGGER.info("risk_subscriber.loop_started", stream=stream, consumer=consumer)
        try:
            while True:
                try:
                    entries = await self._bus.consume(
                        stream,
                        "risk_svc",
                        consumer,
                        count=20,
                        block_ms=5000,
                    )
                except Exception:
                    LOGGER.exception("risk_subscriber.consume_failed", stream=stream)
                    await asyncio.sleep(1)
                    continue

                if not entries:
                    continue

                for entry in entries:
                    try:
                        await handler(entry)
                    except Exception:  # pragma: no cover - defensive
                        LOGGER.exception("risk_subscriber.handler_failed", stream=stream)
        except asyncio.CancelledError:
            LOGGER.info("risk_subscriber.loop_stopped", stream=stream, consumer=consumer)
            raise

    async def _handle_bars_closed(self, entry: Dict[str, Any]) -> None:
        payload = entry.get("payload") or {}
        trace_id = entry.get("trace_id")
        LOGGER.info("risk_subscriber.bars_closed", trace_id=trace_id, payload=payload)
        if self._state_aggregator is not None:
            try:
                await self._state_aggregator.handle_bars_closed(payload, trace_id)
            except Exception:  # pragma: no cover - defensive
                LOGGER.exception("risk_subscriber.state_update_failed", trace_id=trace_id)

    async def _handle_signal(self, entry: Dict[str, Any]) -> None:
        payload = entry.get("payload") or {}
        trace_id = entry.get("trace_id")
        LOGGER.info("risk_subscriber.signal_received", trace_id=trace_id, payload=payload)
        status = (payload.get("status") or "").lower()
        signal_code = payload.get("signal_code")
        symbol = payload.get("symbol")
        strategy_code = str(payload.get("strategy_code") or "core-vol")
        ts_emitted_raw = payload.get("generated_at") or payload.get("created_at")
        ts_event = self._parse_ts(ts_emitted_raw) if ts_emitted_raw else datetime.now(timezone.utc)

        if status == "emitted" and symbol:
            self._risk_service.signal_emitted(strategy_code, symbol, signal_code, ts_event)
        elif status == "filled" and symbol:
            filled_ts = payload.get("filled_at") or payload.get("resolved_at")
            ts_filled = self._parse_ts(filled_ts) if filled_ts else datetime.now(timezone.utc)
            self._risk_service.signal_filled(strategy_code, symbol, ts_filled)
        elif status == "ttl_expired" and symbol and self._risk_service.is_buy_signal(signal_code):
            ts_value = payload.get("generated_at") or payload.get("expiry_ts")
            ts_expired = self._parse_ts(ts_value) if ts_value else datetime.now(timezone.utc)
            await self._risk_service.record_missed_entry(
                symbol,
                strategy_code,
                signal_code,
                ts_expired,
                redis_bus=self._bus,
                trace_id=trace_id,
            )
            self._risk_service.signal_expired(strategy_code, symbol, ts_expired)
        elif status == "sell_signal" and symbol:
            sell_ts = payload.get("generated_at") or payload.get("resolved_at")
            ts_sell = self._parse_ts(sell_ts) if sell_ts else datetime.now(timezone.utc)
            self._risk_service.signal_sell(strategy_code, symbol, signal_code, ts_sell)

    async def _handle_execution_fill(self, entry: Dict[str, Any]) -> None:
        payload = entry.get("payload") or {}
        trace_id = entry.get("trace_id")
        LOGGER.info("risk_subscriber.execution_fill", trace_id=trace_id, payload=payload)

        try:
            fill = ExecutionFill.model_validate(payload)
        except Exception:
            LOGGER.exception("risk_subscriber.fill_invalid", trace_id=trace_id, payload=payload)
            return

        strategy_code = fill.strategy_code or "core-vol"
        if fill.strategy_code is None:
            LOGGER.warning(
                "risk_subscriber.fill_missing_strategy",
                trace_id=trace_id,
                symbol=fill.symbol,
                execution_id=fill.execution_id,
            )
        option_right = fill.option_right
        if option_right is None:
            LOGGER.warning(
                "risk_subscriber.fill_missing_option",
                trace_id=trace_id,
                symbol=fill.symbol,
                execution_id=fill.execution_id,
            )
            return

        filled_at = fill.filled_at
        if filled_at.tzinfo is None:
            filled_at = filled_at.replace(tzinfo=timezone.utc)

        self._risk_service.signal_filled(strategy_code, fill.symbol, filled_at)

        session: Session = self._session_factory()
        try:
            position_dao = StrategyPositionDAO(session)
            realized, position = position_dao.apply_option_fill(
                strategy_code=strategy_code,
                symbol=fill.symbol,
                option_right=option_right,
                side=fill.side,
                quantity=fill.fill_quantity,
                price=fill.fill_price,
                fees=fill.fees if fill.fees is not None else Decimal("0"),
                delta=fill.delta,
                conid=fill.conid,
                strike=fill.strike,
                expiry=fill.expiry.date() if fill.expiry else None,
                signal_code=fill.signal_code,
                filled_at=filled_at,
            )

            pnl_dao = PnLIntradayDAO(session)
            fees = Decimal(str(fill.fees)) if fill.fees is not None else Decimal("0")
            pnl_dao.insert_points(
                [
                    {
                        "ts": filled_at,
                        "strategy_code": strategy_code,
                        "symbol": fill.symbol,
                        "realized": realized,
                        "unrealized": Decimal("0"),
                        "fees": fees,
                    }
                ]
            )

            risk_state_dao = RiskStateDAO(session)
            previous_realized = risk_state_dao.latest_for_symbol(fill.symbol, "REALIZED_PNL")
            realized_previous = (
                Decimal(str(previous_realized.metric_value))
                if previous_realized and previous_realized.metric_value is not None
                else Decimal("0")
            )
            realized_delta = realized - fees
            realized_total = realized_previous + realized_delta
            net_qty = Decimal(str(position.open_quantity))
            mark_price = Decimal(str(position.mark_price))
            notional = abs(net_qty) * mark_price * _OPTION_MULTIPLIER

            risk_state_dao.insert_states(
                [
                    {
                        "ts": filled_at,
                        "symbol": fill.symbol,
                        "metric_code": "NET_QTY",
                        "metric_value": net_qty,
                        "detail": {
                            "strategy_code": strategy_code,
                            "option_right": option_right,
                            "conid": fill.conid,
                        },
                    },
                    {
                        "ts": filled_at,
                        "symbol": fill.symbol,
                        "metric_code": "NOTIONAL",
                        "metric_value": notional,
                        "detail": {
                            "strategy_code": strategy_code,
                            "option_right": option_right,
                        },
                    },
                    {
                        "ts": filled_at,
                        "symbol": fill.symbol,
                        "metric_code": "REALIZED_PNL",
                        "metric_value": realized_total,
                        "detail": {
                            "strategy_code": strategy_code,
                            "signal_code": fill.signal_code,
                            "execution_id": fill.execution_id,
                            "increment": str(realized_delta),
                        },
                    },
                ]
            )

            await self._risk_service.release_symbol_block(
                session,
                symbol=fill.symbol,
                strategy_code=strategy_code,
                trace_id=trace_id,
                redis_bus=self._bus,
                reason="signal_filled",
            )

            session.commit()
        except Exception:
            session.rollback()
            LOGGER.exception(
                "risk_subscriber.fill_processing_failed",
                trace_id=trace_id,
                payload=payload,
            )
        finally:
            session.close()

    async def _handle_param_reload(self, entry: Dict[str, Any]) -> None:
        payload = entry.get("payload") or {}
        trace_id = entry.get("trace_id")
        try:
            event = RiskParamReload(**payload)
        except Exception:
            LOGGER.exception("risk_subscriber.param_reload_invalid", payload=payload)
            return

        limits = self._risk_service.reload_limits()
        if self._on_limits_reloaded is not None:
            self._on_limits_reloaded(limits)
        LOGGER.info(
            "limits reloaded",
            trace_id=trace_id or event.trace_id,
            notional_cap=str(limits.notional_cap),
            updated_at=str(limits.updated_at),
            triggered_by=event.triggered_by,
        )

    @staticmethod
    def _parse_ts(raw: str) -> datetime:
        try:
            ts = datetime.fromisoformat(raw)
        except Exception:
            return datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
