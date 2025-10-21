from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Iterator, Optional

from fastapi import Depends, FastAPI, Response
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from libs.core import configure_logging, get_settings, utc_now
from libs.db import PnLDAO
from libs.db.dao import StrategyPositionDAO
from libs.infra.db import get_session_factory
from libs.infra.redis_bus import RedisBus
from libs.schemas.common import ServiceHealth

from .consumer import ExecutionFillConsumer

LOGGER = logging.getLogger(__name__)

settings = get_settings()
configure_logging(settings)
session_factory = get_session_factory(settings)
consumer: Optional[ExecutionFillConsumer] = None

app = FastAPI(title="PnL Service", version="0.1.0")


class PnLSnapshot(BaseModel):
    strategy_code: str
    gross_notional: Decimal
    unrealised_pnl: Decimal
    timestamp: str


class PositionRecord(BaseModel):
    strategy_code: str
    symbol: str
    quantity: Decimal
    avg_price: Decimal
    unrealized_pnl: Decimal


class DailyPnLRecord(BaseModel):
    trade_date: str
    strategy_code: str
    symbol: str
    realized: Decimal
    unrealized: Decimal
    fees: Decimal


def get_position_dao() -> Iterator[StrategyPositionDAO]:
    session = session_factory()
    try:
        yield StrategyPositionDAO(session)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_session() -> Iterator:
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@app.get("/healthz", response_model=ServiceHealth)
async def healthcheck() -> ServiceHealth:
    return ServiceHealth(status="ok", service="pnl_svc", timestamp=utc_now())


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/pnl/snapshot", response_model=list[PnLSnapshot])
async def pnl_snapshot(
    strategy_code: str | None = None,
    dao: StrategyPositionDAO = Depends(get_position_dao),
) -> list[PnLSnapshot]:
    positions = dao.list_positions(strategy_code=strategy_code)
    option_multiplier = Decimal("100")
    report: dict[str, dict[str, Decimal]] = {}

    for position in positions:
        group = report.setdefault(
            position.strategy_code,
            {"gross": Decimal("0"), "pnl": Decimal("0")},
        )
        gross = Decimal(abs(position.open_quantity)) * position.mark_price * option_multiplier
        group["gross"] += gross
        pnl = (
            (position.mark_price - position.avg_open_price)
            * Decimal(position.open_quantity)
            * option_multiplier
        )
        group["pnl"] += pnl

    return [
        PnLSnapshot(
            strategy_code=strategy,
            gross_notional=values["gross"],
            unrealised_pnl=values["pnl"],
            timestamp=utc_now().isoformat(),
        )
        for strategy, values in report.items()
    ]


@app.get("/positions", response_model=list[PositionRecord])
async def list_positions(
    strategy_code: str | None = None, session=Depends(get_session)
) -> list[PositionRecord]:
    dao = PnLDAO(session)
    positions = dao.list_positions(strategy_code=strategy_code)
    return [
        PositionRecord(
            strategy_code=position.strategy_code,
            symbol=position.symbol,
            quantity=position.quantity,
            avg_price=position.avg_price,
            unrealized_pnl=position.unrealized_pnl,
        )
        for position in positions
    ]


@app.get("/pnl/daily", response_model=list[DailyPnLRecord])
async def list_daily_pnl(
    strategy_code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    session=Depends(get_session),
) -> list[DailyPnLRecord]:
    dao = PnLDAO(session)
    start = date.fromisoformat(start_date) if start_date else None
    end = date.fromisoformat(end_date) if end_date else None
    records = dao.list_daily_pnl(strategy_code=strategy_code, start_date=start, end_date=end)
    return [
        DailyPnLRecord(
            trade_date=record.trade_date.isoformat(),
            strategy_code=record.strategy_code,
            symbol=record.symbol,
            realized=record.realized,
            unrealized=record.unrealized,
            fees=record.fees,
        )
        for record in records
    ]


@app.on_event("startup")
async def start_consumer() -> None:
    if settings.app_env == "test":
        return
    global consumer
    consumer = ExecutionFillConsumer(
        redis_bus=RedisBus(settings.redis_url), session_factory=session_factory
    )
    await consumer.start()


@app.on_event("shutdown")
async def stop_consumer() -> None:
    if consumer:
        await consumer.stop()
