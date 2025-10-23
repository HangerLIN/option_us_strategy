from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterator, Optional
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from libs.core import (
    TraceContextMiddleware,
    configure_logging,
    get_settings,
    update_trace_context,
    utc_now,
)
from libs.db.dao import RiskEventDAO, StrategyPositionDAO
from libs.infra.db import get_session_factory
from libs.infra.redis_bus import RedisBus
from libs.schemas.common import ApiResult, ServiceHealth
from libs.infra.metrics import record_force_close
from libs.schemas.risk import (
    ExposureSnapshot,
    ForceCloseRequest,
    ForceCloseResult,
    RiskCheckRequest,
    RiskCheckResult,
    RiskLimits,
    RiskState,
)
from .service import RiskService
from .state import RiskStateAggregator
from .subscribers import RiskStreamConsumers
from .publisher import publish_force_close

LOGGER = logging.getLogger(__name__)

settings = get_settings()
configure_logging(settings)
session_factory = get_session_factory(settings)
redis_bus = RedisBus(settings.redis_url)
risk_service = RiskService(session_factory, redis_url=settings.redis_url)
_initial_limits = risk_service.current_limits()
_limits_snapshot: RiskLimits = _initial_limits.model_copy(deep=True)
_risk_notional_cap: Decimal = _limits_snapshot.notional_cap
_limits_updated_at = _limits_snapshot.updated_at
consumer_manager: Optional[RiskStreamConsumers] = None
consumer_tasks: list[asyncio.Task[Any]] = []

app = FastAPI(title="风控服务", version="0.1.0", description="风险管理与动态调整 API")
app.add_middleware(TraceContextMiddleware)


class RiskEventRecord(BaseModel):
    event_id: int
    event_ts: datetime
    created_at: datetime
    event_code: str
    severity: str
    symbol: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    model_config = {"from_attributes": True}


def _current_limits() -> RiskLimits:
    return _limits_snapshot.model_copy(deep=True)


def _set_limits(limits: RiskLimits) -> RiskLimits:
    global _limits_snapshot, _risk_notional_cap, _limits_updated_at
    _limits_snapshot = limits.model_copy(deep=True)
    _risk_notional_cap = _limits_snapshot.notional_cap
    _limits_updated_at = _limits_snapshot.updated_at
    return _current_limits()


def _handle_limits_reload(limits: RiskLimits) -> None:
    _set_limits(limits)


async def verify_dependencies() -> None:
    """Ensure downstream dependencies are reachable at startup."""
    try:
        session = session_factory()
        try:
            session.execute(text("SELECT 1"))
            session.commit()
        finally:
            session.close()
    except SQLAlchemyError as exc:
        LOGGER.exception("Database connectivity check failed")
        raise RuntimeError("Database connectivity check failed") from exc

    try:
        await redis_bus.ping()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Redis connectivity check failed")
        raise RuntimeError("Redis connectivity check failed") from exc


@app.on_event("startup")
async def startup_event() -> None:
    global consumer_manager
    if settings.app_env == "test":
        LOGGER.debug("Skipping dependency verification in test environment")
        return
    await verify_dependencies()
    state_aggregator = RiskStateAggregator(session_factory, risk_service, redis_bus)
    consumer_manager = RiskStreamConsumers(
        redis_bus,
        risk_service,
        session_factory=session_factory,
        on_limits_reloaded=_handle_limits_reload,
        state_aggregator=state_aggregator,
    )
    await consumer_manager.ensure_groups()
    tasks = list(consumer_manager.start())
    consumer_tasks.clear()
    consumer_tasks.extend(tasks)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global consumer_manager
    if consumer_manager is not None:
        await consumer_manager.stop()
        consumer_manager = None
    consumer_tasks.clear()
    await redis_bus.close()


def get_position_dao() -> Iterator[StrategyPositionDAO]:
    session: Session = session_factory()
    try:
        yield StrategyPositionDAO(session)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_risk_event_dao() -> Iterator[RiskEventDAO]:
    session: Session = session_factory()
    try:
        yield RiskEventDAO(session)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@app.get(
    "/healthz",
    response_model=ApiResult,
    summary="健康检查",
    description="探测风控服务状态。",
)
async def healthcheck() -> ApiResult:
    payload = ServiceHealth(status="ok", service="risk_svc", timestamp=utc_now())
    return ApiResult(ok=True, code="OK", message="服务正常", data=payload)


@app.get("/metrics", summary="Prometheus 指标")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get(
    "/risk/events",
    response_model=ApiResult,
    summary="风险事件查询",
    description="按时间区间、标的过滤风险事件审计记录。",
)
async def list_risk_events(
    from_ts: datetime = Query(..., alias="from"),
    to_ts: datetime | None = Query(None, alias="to"),
    symbol: str | None = Query(None),
    event_code: str | None = Query(None),
    page: int = Query(0, ge=0),
    page_size: int = Query(100, ge=1, le=500),
    dao: RiskEventDAO = Depends(get_risk_event_dao),
) -> ApiResult:
    symbol_filter = symbol.upper() if symbol else None
    event_filter = event_code.upper() if event_code else None
    if to_ts is not None and to_ts < from_ts:
        raise HTTPException(status_code=400, detail="结束时间必须晚于开始时间")
    offset = page * page_size
    events = dao.list_events(
        start_ts=from_ts,
        end_ts=to_ts,
        symbol=symbol_filter,
        event_code=event_filter,
        limit=page_size,
        offset=offset,
    )
    records = [RiskEventRecord.model_validate(event) for event in events]
    reason_stats: Dict[str, int] = {}
    serialized = []
    for record in records:
        dump = record.model_dump()
        serialized.append(dump)
        payload_map = dump.get("payload") or {}
        reason = payload_map.get("reason") or payload_map.get("limit_code")
        if reason:
            reason_stats[reason] = reason_stats.get(reason, 0) + 1
    payload = {
        "events": serialized,
        "count": len(records),
        "page": page,
        "page_size": page_size,
        "reason_stats": reason_stats,
    }
    return ApiResult(ok=True, code="OK", message="事件查询完成", data=payload)


def _compute_notional(positions: list) -> Decimal:
    option_multiplier = Decimal("100")
    return sum(
        Decimal(abs(position.open_quantity)) * position.mark_price * option_multiplier
        for position in positions
    )


def _build_exposure_snapshots(positions: list) -> list[ExposureSnapshot]:
    option_multiplier = Decimal("100")
    snapshots: list[ExposureSnapshot] = []
    for position in positions:
        notional = Decimal(abs(position.open_quantity)) * position.mark_price * option_multiplier
        snapshots.append(
            ExposureSnapshot(
                strategy_code=position.strategy_code,
                symbol=position.symbol,
                option_right=position.option_right,
                open_quantity=position.open_quantity,
                mark_price=position.mark_price,
                notional=notional,
                source_signal_code=position.source_signal_code,
                opened_at=position.opened_at,
                conid=position.conid,
                strike=position.strike,
                expiry=position.expiry,
                delta=position.delta,
            )
        )
    return snapshots


def _fetch_latest_metrics() -> tuple[Optional[datetime], Dict[str, Any]]:
    session = session_factory()
    try:
        latest_ts = session.execute(
            text("SELECT ts FROM risk_state WHERE symbol='GLOBAL' ORDER BY ts DESC LIMIT 1")
        ).scalar_one_or_none()
        if latest_ts is None:
            return None, {}
        rows = session.execute(
            text(
                """
                SELECT metric_code, metric_value, detail
                FROM risk_state
                WHERE symbol = 'GLOBAL' AND ts = :ts
                """
            ),
            {"ts": latest_ts},
        ).all()
        metrics: Dict[str, Any] = {}
        for code, value, detail in rows:
            metrics[code] = {
                "value": Decimal(str(value)) if value is not None else None,
                "detail": detail,
            }
        return latest_ts, metrics
    finally:
        session.close()


def _summarize_metrics(
    metrics: Dict[str, Any],
    metrics_ts: Optional[datetime],
    exposures: list[ExposureSnapshot],
) -> Dict[str, Any]:
    def _value(code: str) -> Optional[Decimal]:
        entry = metrics.get(code)
        value = entry.get("value") if entry else None
        if value is None:
            return None
        return Decimal(str(value))

    summary: Dict[str, Any] = {}
    kill_switch_val = _value("KILL_SWITCH")
    summary["kill_switch"] = bool(kill_switch_val) if kill_switch_val is not None else None
    used_r = _value("USED_R")
    drawdown_r = _value("DRAWDOWN_R")
    vix_last = _value("VIX_LAST")
    num_positions_metric = _value("NUM_POSITIONS")

    summary["used_r"] = str(used_r) if used_r is not None else None
    summary["drawdown_r"] = str(drawdown_r) if drawdown_r is not None else None
    summary["vix_last"] = str(vix_last) if vix_last is not None else None
    if num_positions_metric is not None:
        try:
            summary["num_positions"] = int(num_positions_metric)
        except (ValueError, TypeError):
            summary["num_positions"] = str(num_positions_metric)
    else:
        summary["num_positions"] = len(exposures)
    summary["timestamp"] = metrics_ts.isoformat() if metrics_ts else None
    return summary


@app.post(
    "/precheck",
    response_model=ApiResult,
    summary="下单预检",
    description="在提交执行前使用风控规则快速评估是否可下单。",
)
async def precheck(
    payload: RiskCheckRequest, dao: StrategyPositionDAO = Depends(get_position_dao)
) -> ApiResult:
    update_trace_context(symbol=payload.symbol, signal_code=payload.strategy_code)
    exposures = list(dao.list_positions(strategy_code=payload.strategy_code))
    current_notional = _compute_notional(exposures)
    limit = _risk_notional_cap
    pending = payload.notional
    utilisation = float((current_notional + pending) / limit) if limit else 1.0
    decision = risk_service.evaluate_order(
        payload,
        exposures=exposures,
        current_notional=current_notional,
        pending_notional=pending,
        utilisation=utilisation,
        record_state=False,
        record_events=True,
        redis_bus=redis_bus,
    )
    result = RiskCheckResult(
        strategy_code=payload.strategy_code,
        symbol=payload.symbol,
        approved=decision.approved,
        limit_utilisation=min(decision.utilisation, 1.0),
        detail=decision.detail,
        reasons=decision.reasons,
        timestamp=utc_now(),
    )

    code = "OK" if decision.approved else decision.code
    message = "预检通过" if decision.approved else "预检阻断"
    return ApiResult(ok=decision.approved, code=code, message=message, data=result.model_dump())


@app.post(
    "/force_close",
    response_model=ApiResult,
    summary="强制平仓",
    description="根据策略与标的指令平掉持仓，主要用于风控触发的清仓操作。",
)
async def force_close(
    payload: ForceCloseRequest,
    dao: StrategyPositionDAO = Depends(get_position_dao),
) -> ApiResult:
    update_trace_context(symbol=payload.symbol, signal_code=payload.strategy_code)
    position = dao.get_position(payload.strategy_code, payload.symbol, payload.option_right)
    if position is None:
        return ApiResult(ok=False, code="REJECT:POSITION_NOT_FOUND", message="未找到持仓")

    prior_quantity = position.open_quantity
    was_open = prior_quantity != 0
    if was_open:
        position.open_quantity = 0
        closed_quantity = abs(prior_quantity)
    else:
        closed_quantity = 0

    event_ts = utc_now()
    with session_factory() as session:
        event_dao = RiskEventDAO(session)
        event_dao.create_event(
            event_ts=event_ts,
            event_code="FORCE_CLOSE_MANUAL",
            severity="WARN",
            message="force_close_manual",
            symbol=payload.symbol,
            payload={
                "strategy_code": payload.strategy_code,
                "option_right": payload.option_right,
            },
        )
        session.commit()
    await publish_force_close(
        redis_bus,
        {
            "strategy_code": payload.strategy_code,
            "symbol": payload.symbol,
            "option_right": payload.option_right,
            "reason": "MANUAL",
            "triggered_at": event_ts,
        },
        trace_id=str(uuid4()),
    )
    record_force_close("MANUAL")

    result = ForceCloseResult(
        strategy_code=payload.strategy_code,
        symbol=payload.symbol,
        option_right=payload.option_right,
        was_open=was_open,
        closed_quantity=closed_quantity,
        timestamp=event_ts,
    )
    message = "强平执行完成" if was_open else "仓位已空"
    return ApiResult(ok=True, code="OK", message=message, data=result.model_dump())


@app.post(
    "/limits/reload",
    response_model=ApiResult,
    summary="重新加载风控限额",
    description="从配置中心刷新风控限额并立即生效。",
)
async def reload_limits() -> ApiResult:
    global settings
    try:
        get_settings.cache_clear()  # type: ignore[attr-defined]
        settings = get_settings()
    except RuntimeError as exc:  # pragma: no cover - defensive
        LOGGER.exception("Failed to reload settings")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    limits = risk_service.reload_limits(refresh_settings=True)
    limits = _set_limits(limits)
    return ApiResult(ok=True, code="OK", message="限额已刷新", data=limits.model_dump())


@app.get(
    "/state",
    response_model=ApiResult,
    summary="风控状态",
    description="返回当下风控限额与各标的风险快照。",
)
async def state(dao: StrategyPositionDAO = Depends(get_position_dao)) -> ApiResult:
    positions = list(dao.list_positions())
    exposures = _build_exposure_snapshots(positions)
    total_notional = _compute_notional(positions)
    metrics_ts, metrics = _fetch_latest_metrics()
    state_payload = RiskState(
        limits=_current_limits(),
        exposures=exposures,
        total_notional=total_notional,
        timestamp=metrics_ts or utc_now(),
        metrics=metrics,
    )
    summary = _summarize_metrics(metrics, metrics_ts, exposures)
    blocks = risk_service.list_blocks()
    concurrency = risk_service.concurrency_snapshot()
    data = {
        "state": state_payload.model_dump(),
        "metrics": summary,
        "blocks": blocks,
        "concurrency": concurrency,
    }
    return ApiResult(ok=True, code="OK", message="风控状态就绪", data=data)


@app.get(
    "/risk/blocks",
    response_model=ApiResult,
    summary="封禁状态",
    description="返回当前风控封禁列表与剩余 TTL。",
)
async def list_blocks() -> ApiResult:
    blocks = risk_service.list_blocks()
    payload = {"blocks": blocks, "count": len(blocks)}
    return ApiResult(ok=True, code="OK", message="封禁状态", data=payload)
