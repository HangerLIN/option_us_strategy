from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from queue import Queue
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Response
from ibapi.scanner import ScannerSubscription
from ibapi.ticktype import TickTypeEnum
from pydantic import BaseModel, Field, model_validator

from libs.core import (
    TraceContextMiddleware,
    attach_trace_metadata,
    configure_logging,
    get_settings,
    update_trace_context,
    utc_now,
)
from libs.infra import RedisBus, build_ibkr_client
from libs.infra.metrics import (
    set_ibkr_connection_status,
    record_ibkr_reconnection,
    record_tick_received,
    set_subscribed_symbols_count,
)
from libs.infra.ibkr_client import IBClient
from libs.schemas.common import ServiceHealth
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from libs.schemas.events import MarketDataEvent

import structlog

LOGGER = structlog.get_logger(__name__)

settings = get_settings()
configure_logging(settings)

redis_bus = RedisBus(settings.redis_url)
ib_client: Optional[IBClient] = None

app = FastAPI(title="Market Data Gateway", version="0.1.0")
app.add_middleware(TraceContextMiddleware)


def _require_ib_client() -> IBClient:
    client = ib_client
    if client is None:
        raise RuntimeError("IBKR client not initialised")
    return client


@dataclass
class SubscriptionState:
    req_id: int
    alias: str
    symbol: str
    queue: Queue
    security_type: str
    exchange: str
    currency: str
    bid: Decimal | None = None
    ask: Decimal | None = None
    task: asyncio.Task | None = field(default=None, repr=False)


subscriptions: Dict[int, SubscriptionState] = {}
subscription_lock = asyncio.Lock()
watchdog_task: Optional[asyncio.Task] = None


async def _ensure_ib_connection() -> None:
    client = _require_ib_client()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, client.connect_and_wait)
    try:
        if settings.monitoring_enabled:
            set_ibkr_connection_status(True)
    except Exception:  # pragma: no cover - metrics best-effort
        pass


async def _connection_watchdog() -> None:
    client = _require_ib_client()
    loop = asyncio.get_running_loop()
    try:
        while True:
            await asyncio.sleep(5)
            if not client.isConnected():
                LOGGER.warning("md_gw.watchdog_reconnect_start")
                try:
                    await loop.run_in_executor(None, client.connect_and_wait)
                    LOGGER.info("md_gw.watchdog_reconnect_success")
                    try:
                        if settings.monitoring_enabled:
                            set_ibkr_connection_status(True)
                            record_ibkr_reconnection("watchdog")
                    except Exception:
                        pass
                except Exception:  # pragma: no cover - defensive
                    LOGGER.exception("md_gw.watchdog_reconnect_failed")
                    await asyncio.sleep(10)
    except asyncio.CancelledError:  # pragma: no cover - cooperative shutdown
        LOGGER.info("md_gw.watchdog_stopped")
        raise


async def _publish_top_of_book(state: SubscriptionState) -> None:
    if state.bid is None or state.ask is None:
        return
    if state.bid <= 0 or state.ask <= 0:
        return
    event = MarketDataEvent(
        symbol=state.symbol,
        bid=state.bid,
        ask=state.ask,
        timestamp=utc_now(),
    )
    payload = event.model_dump()
    payload.setdefault("trace_id", str(uuid4()))
    enriched = attach_trace_metadata(payload)
    trace_id = enriched.get("trace_id")
    if not trace_id:
        LOGGER.debug("md_gw.publish_skipped_missing_trace", symbol=state.symbol)
        return
    try:
        await redis_bus.publish("market-data", enriched, trace_id=trace_id)
        LOGGER.debug(
            "md_gw.market_event_published",
            symbol=state.symbol,
            bid=str(state.bid),
            ask=str(state.ask),
            trace_id=trace_id,
        )
    except Exception:  # pragma: no cover - external dependency
        LOGGER.exception("md_gw.publish_failed", symbol=state.symbol, trace_id=trace_id)


async def _consume_subscription(state: SubscriptionState) -> None:
    loop = asyncio.get_running_loop()
    LOGGER.info("md_gw.subscription_stream_start", alias=state.alias, req_id=state.req_id)
    try:
        while True:
            try:
                payload = await loop.run_in_executor(None, state.queue.get)
            except Exception:  # pragma: no cover - defensive
                LOGGER.exception("md_gw.subscription_queue_error", alias=state.alias)
                break
            if payload is None:
                break
            event_type = payload.get("type")
            if event_type == "price":
                tick_type = payload.get("field")
                price = payload.get("price")
                if price is None:
                    continue
                value = Decimal(str(price))
                if tick_type == TickTypeEnum.BID:
                    state.bid = value
                elif tick_type == TickTypeEnum.ASK:
                    state.ask = value
            elif event_type == "rt_volume":
                price = payload.get("price")
                if price is None:
                    continue
                value = Decimal(str(price))
                state.bid = state.bid or value
                state.ask = state.ask or value
            elif event_type == "generic":
                continue
            elif event_type == "size":
                continue
            await _publish_top_of_book(state)
            try:
                if settings.monitoring_enabled:
                    # 使用 alias 作为标签，避免与纯 symbol 冲突
                    record_tick_received(state.alias)
            except Exception:  # pragma: no cover
                pass
    except asyncio.CancelledError:  # pragma: no cover - cooperative shutdown
        LOGGER.debug("md_gw.subscription_stream_cancelled", alias=state.alias)
    finally:
        LOGGER.info("md_gw.subscription_stream_end", alias=state.alias, req_id=state.req_id)


async def _unsubscribe(req_id: int) -> None:
    client = _require_ib_client()
    async with subscription_lock:
        state = subscriptions.pop(req_id, None)
    if state is None:
        return
    if state.task is not None:
        state.task.cancel()
    try:
        state.queue.put_nowait(None)
    except Exception:  # pragma: no cover - defensive
        pass
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, client.unsubscribe_l1, state.alias)
    except Exception:  # pragma: no cover - external dependency
        LOGGER.exception("md_gw.unsubscribe_failed", alias=state.alias)
    try:
        if settings.monitoring_enabled:
            async with subscription_lock:
                set_subscribed_symbols_count(len(subscriptions))
    except Exception:  # pragma: no cover
        pass


class MarketDataSubscription(BaseModel):
    symbol: str = Field(..., max_length=16)
    exchange: str = Field("SMART", max_length=16)
    currency: str = Field("USD", max_length=8)
    security_type: str = Field("STK", max_length=8)
    generic_ticks: str = Field("233", max_length=64)


class HistoricalBarsRequest(BaseModel):
    symbol: str = Field(..., max_length=16)
    start: datetime
    end: datetime
    use_rth: bool = Field(True)
    exchange: str = Field("SMART", max_length=16)
    currency: str = Field("USD", max_length=8)

    @model_validator(mode="after")
    def validate_window(self) -> "HistoricalBarsRequest":
        if self.end <= self.start:
            raise ValueError("end must be after start")
        window_minutes = (self.end - self.start).total_seconds() / 60
        if window_minutes > settings.hist_backfill_window_min:
            raise ValueError(
                f"historical window exceeds configured limit of {settings.hist_backfill_window_min} minutes"
            )
        return self


class ScannerRequest(BaseModel):
    scan_code: str = Field("TOP_PERC_GAIN", max_length=64)
    above_price: float = Field(5.0, ge=0)
    above_volume: int = Field(100000, ge=0)
    location_code: str = Field("STK.US.MAJOR", max_length=64)
    instrument: str = Field("STK", max_length=16)
    stock_type_filter: Optional[str] = Field("STOCK", max_length=16)
    limit: int = Field(50, ge=1, le=50)


@app.on_event("startup")
async def startup_event() -> None:
    global ib_client
    LOGGER.info("Starting market data gateway")
    ib_client = build_ibkr_client(settings)
    await redis_bus.ping()
    await _ensure_ib_connection()
    global watchdog_task
    watchdog_task = asyncio.create_task(_connection_watchdog())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    LOGGER.info("Stopping market data gateway")
    await redis_bus.close()
    if watchdog_task is not None:
        watchdog_task.cancel()
        try:
            await watchdog_task
        except asyncio.CancelledError:
            pass
    if ib_client is not None:
        async with subscription_lock:
            active = list(subscriptions.keys())
        for req_id in active:
            try:
                await _unsubscribe(req_id)
            except Exception:  # pragma: no cover - best effort cleanup
                LOGGER.exception("md_gw.unsubscribe_during_shutdown_failed", req_id=req_id)
        ib_client.disconnect_and_stop()


@app.get("/healthz", response_model=ServiceHealth)
async def healthcheck() -> ServiceHealth:
    return ServiceHealth(status="ok", service="md_gw", timestamp=utc_now())


@app.get("/metrics", summary="Prometheus 指标")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/subscriptions")
async def subscribe_market_data(payload: MarketDataSubscription) -> dict[str, int]:
    try:
        client = _require_ib_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    update_trace_context(symbol=payload.symbol)
    alias = f"{payload.security_type}:{payload.symbol}:{uuid4().hex[:8]}"
    loop = asyncio.get_running_loop()

    try:
        req_id, queue = await loop.run_in_executor(
            None,
            lambda: client.subscribe_l1(
                payload.symbol,
                sec_type=payload.security_type,
                exchange=payload.exchange,
                currency=payload.currency,
                generic_ticks=payload.generic_ticks,
                alias=alias,
            ),
        )
    except Exception as exc:  # pragma: no cover - external dependency
        LOGGER.exception("md_gw.subscription_failed", symbol=payload.symbol)
        raise HTTPException(status_code=502, detail=f"IBKR subscription failed: {exc}") from exc

    state = SubscriptionState(
        req_id=req_id,
        alias=alias,
        symbol=payload.symbol.upper(),
        queue=queue,
        security_type=payload.security_type,
        exchange=payload.exchange,
        currency=payload.currency,
    )
    state.task = asyncio.create_task(_consume_subscription(state))

    async with subscription_lock:
        subscriptions[req_id] = state
        try:
            if settings.monitoring_enabled:
                set_subscribed_symbols_count(len(subscriptions))
        except Exception:  # pragma: no cover
            pass

    LOGGER.info(
        "md_gw.subscription_created",
        symbol=payload.symbol,
        req_id=req_id,
        alias=alias,
    )
    return {"ticker_id": req_id}


@app.delete("/subscriptions/{ticker_id}")
async def unsubscribe_market_data(ticker_id: int) -> dict[str, str]:
    try:
        _require_ib_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    async with subscription_lock:
        if ticker_id not in subscriptions:
            raise HTTPException(status_code=404, detail="Subscription not found")
    await _unsubscribe(ticker_id)
    LOGGER.info("md_gw.subscription_removed", ticker_id=ticker_id)
    return {"status": "unsubscribed"}


@app.post("/events/market")
async def forward_market_event(event: MarketDataEvent) -> dict[str, str]:
    update_trace_context(symbol=event.symbol)
    payload = event.model_dump()
    payload.setdefault("trace_id", str(uuid4()))
    enriched = attach_trace_metadata(payload)
    trace_id = str(enriched["trace_id"])
    await redis_bus.publish("market-data", enriched, trace_id=trace_id)
    return {"status": "relayed"}


@app.post("/historical/bars")
async def get_historical_bars(request: HistoricalBarsRequest) -> List[dict[str, object]]:
    try:
        client = _require_ib_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    loop = asyncio.get_running_loop()
    try:
        bars = await loop.run_in_executor(
            None,
            lambda: client.req_historical_1m(
                request.symbol,
                request.start,
                request.end,
                use_rth=request.use_rth,
                exchange=request.exchange,
                currency=request.currency,
            ),
        )
    except Exception as exc:  # pragma: no cover - external dependency
        LOGGER.exception("md_gw.historical_failed", symbol=request.symbol)
        raise HTTPException(status_code=502, detail=f"Historical request failed: {exc}") from exc
    return [dict(bar) for bar in bars]


@app.post("/scanner/premarket")
async def run_premarket_scanner(request: ScannerRequest) -> List[dict[str, object]]:
    try:
        client = _require_ib_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    subscription = ScannerSubscription()
    subscription.instrument = request.instrument
    subscription.scanCode = request.scan_code
    subscription.abovePrice = request.above_price
    subscription.aboveVolume = request.above_volume
    subscription.locationCode = request.location_code
    subscription.stockTypeFilter = request.stock_type_filter
    subscription.numberOfRows = request.limit

    loop = asyncio.get_running_loop()
    try:
        results = await loop.run_in_executor(None, client.scanner_premarket, subscription)
    except Exception as exc:  # pragma: no cover - external dependency
        LOGGER.exception("md_gw.scanner_failed", scan_code=request.scan_code)
        raise HTTPException(status_code=502, detail=f"Scanner request failed: {exc}") from exc
    return [dict(result) for result in results]


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Market Data Gateway utilities")
    parser.add_argument(
        "--selfcheck", action="store_true", help="Run IBKR connectivity self-check and exit"
    )
    args = parser.parse_args(argv)

    if args.selfcheck:
        configure_logging(settings)
        client = IBClient(settings)
        try:
            client.run_selfcheck()
        finally:
            client.disconnect_and_stop()
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
