from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Iterator

import structlog
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from libs.core import EASTERN, configure_logging, get_settings, utc_now
from libs.db.dao import StrategyPositionDAO
from libs.infra.db import get_session_factory
from libs.infra import build_ibkr_client
from libs.schemas.common import ServiceHealth
from libs.schemas.signals import SignalInstruction, SignalSide

LOGGER = structlog.get_logger(__name__)

settings = get_settings()
configure_logging(settings)
session_factory = get_session_factory(settings)

# 全局IBKR客户端单例（在startup时初始化）
_ib_client_singleton = None

app = FastAPI(title="Real-Time Engine", version="0.1.0")


@app.on_event("startup")
async def startup_event():
    """应用启动时初始化全局IBKR客户端"""
    global _ib_client_singleton
    LOGGER.info("rt_api.startup", message="Initializing global IBKR client")
    _ib_client_singleton = build_ibkr_client(settings)
    LOGGER.info("rt_api.startup.complete", message="Global IBKR client initialized")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时断开IBKR连接"""
    global _ib_client_singleton
    if _ib_client_singleton is not None:
        LOGGER.info("rt_api.shutdown", message="Disconnecting global IBKR client")
        try:
            _ib_client_singleton.disconnect_and_stop()
        except Exception as exc:
            LOGGER.warning("rt_api.shutdown.disconnect_failed", error=str(exc))
        _ib_client_singleton = None


class EvaluationResult(BaseModel):
    approved: bool
    limit_utilisation: float = Field(..., ge=0, le=1)
    recommended_size: int
    timestamp: str


class Top5TriggerResponse(BaseModel):
    status: str
    message: str
    trade_date: str
    timestamp: str


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


@app.get("/healthz", response_model=ServiceHealth)
async def healthcheck() -> ServiceHealth:
    return ServiceHealth(status="ok", service="rt_engine", timestamp=utc_now())


@app.post("/engine/evaluate", response_model=EvaluationResult)
async def evaluate_signal(
    signal: SignalInstruction,
    dao: StrategyPositionDAO = Depends(get_position_dao),
) -> EvaluationResult:
    exposures = dao.list_positions(strategy_code=signal.strategy_code)
    option_multiplier = Decimal("100")
    book_notional = sum(
        Decimal(abs(position.open_quantity)) * position.mark_price * option_multiplier
        for position in exposures
    )

    base_size = 10 if signal.side == SignalSide.BUY else 8
    recommended_size = max(1, int(base_size * signal.confidence))
    pending_notional = Decimal(recommended_size) * Decimal("1000")

    limit = Decimal("7500000")
    utilisation = float((book_notional + pending_notional) / limit)
    approved = utilisation < 1.0

    return EvaluationResult(
        approved=approved,
        limit_utilisation=min(utilisation, 1.0),
        recommended_size=recommended_size,
        timestamp=utc_now().isoformat(),
    )


def _run_top5_selection(trade_date: date) -> None:
    """后台任务：运行 Top5 选股（使用全局IBKR客户端单例）"""
    from apps.rt_engine.top5_service import Top5Service

    global _ib_client_singleton
    
    if _ib_client_singleton is None:
        LOGGER.error("top5.manual_trigger.no_client", trade_date=str(trade_date))
        raise RuntimeError("Global IBKR client not initialized")
    
    LOGGER.info("top5.manual_trigger.start", trade_date=str(trade_date))
    try:
        top5_service = Top5Service(_ib_client_singleton, session_factory, settings)
        top5_service.run_for_today(trade_date)
        LOGGER.info("top5.manual_trigger.success", trade_date=str(trade_date))
    except Exception as exc:
        LOGGER.exception("top5.manual_trigger.failed", trade_date=str(trade_date), error=str(exc))


@app.post("/top5/trigger", response_model=Top5TriggerResponse)
async def trigger_top5_selection(
    background_tasks: BackgroundTasks,
    trade_date: str | None = None
) -> Top5TriggerResponse:
    """
    手动触发Top5选股
    
    - **trade_date**: 可选，格式YYYY-MM-DD，默认为今天（美东时间）
    """
    try:
        if trade_date:
            et_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
        else:
            et_date = datetime.now(EASTERN).date()
        
        # 在后台运行Top5选股，避免阻塞API响应
        background_tasks.add_task(_run_top5_selection, et_date)
        
        return Top5TriggerResponse(
            status="triggered",
            message=f"Top5选股已触发，将在后台运行",
            trade_date=str(et_date),
            timestamp=utc_now().isoformat(),
        )
    except Exception as exc:
        LOGGER.exception("top5.trigger_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))
