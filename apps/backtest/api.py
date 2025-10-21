from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict

from fastapi import FastAPI, HTTPException, Query

from apps.backtest.dao import BacktestDAO
from libs.core import configure_logging, get_settings, utc_now
from libs.infra.db import db_session, get_session_factory
from libs.schemas.common import ApiResult, ServiceHealth

settings = get_settings()
configure_logging(settings)
session_factory = get_session_factory(settings)

app = FastAPI(title="回测接口", version="0.1.0", description="回测运行与指标查询 API")


def _dao(session) -> BacktestDAO:
    return BacktestDAO(
        session,
        option_bar_table=settings.option_bar_table,
        option_chain_table=settings.option_chain_table,
    )


@app.get(
    "/healthz",
    response_model=ApiResult,
    summary="健康检查",
    description="探测回测服务状态。",
)
async def healthcheck() -> ApiResult:
    payload = ServiceHealth(status="ok", service="backtest_api", timestamp=utc_now())
    return ApiResult(ok=True, code="OK", message="服务正常", data=payload)


@app.get(
    "/runs",
    response_model=ApiResult,
    summary="回测运行列表",
    description="按时间范围筛选历史回测任务，默认倒序返回。",
)
def list_runs(
    symbol: str | None = Query(None),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> ApiResult:
    start_from = start or datetime(1970, 1, 1, tzinfo=timezone.utc)
    start_to = end or utc_now()

    with db_session(settings) as session:
        dao = _dao(session)
        result_rows = dao.fetch_runs(
            start_from=start_from, start_to=start_to, limit=limit, offset=offset
        )
        rows = [asdict(row) for row in result_rows]

    if symbol:
        symbol_upper = symbol.upper()
        rows = [
            row
            for row in rows
            if (row.get("parameters") or {}).get("symbol", "").upper() == symbol_upper
        ]

    return ApiResult(
        ok=True, code="OK", message="回测列表", data={"runs": rows, "count": len(rows)}
    )


@app.get(
    "/metrics/{run_id}",
    response_model=ApiResult,
    summary="回测指标",
    description="返回指定回测的总览指标及按交易日拆分的明细。",
)
def read_metrics(run_id: int) -> ApiResult:
    with db_session(settings) as session:
        dao = _dao(session)
        total_rows = [asdict(row) for row in dao.fetch_metrics_total(run_id)]
        if not total_rows:
            raise HTTPException(status_code=404, detail="回测任务不存在")
        daily_rows = [asdict(row) for row in dao.fetch_metrics_daily(run_id)]

    total: Dict[str, float] = {row["metric_code"]: float(row["metric_value"]) for row in total_rows}
    daily: Dict[str, Dict[str, float]] = defaultdict(dict)
    for row in daily_rows:
        trade_date = row["trade_date"].isoformat()
        daily[trade_date][row["metric_code"]] = float(row["metric_value"])

    return ApiResult(
        ok=True,
        code="OK",
        message="指标查询完成",
        data={
            "run_id": run_id,
            "total": total,
            "daily": daily,
        },
    )
