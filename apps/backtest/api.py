from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict

from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from apps.backtest.dao import BacktestDAO
from libs.core import configure_logging, get_settings, utc_now
from libs.infra.db import db_session, get_session_factory
from libs.schemas.common import ApiResult, ServiceHealth

settings = get_settings()
configure_logging(settings)
session_factory = get_session_factory(settings)

app = FastAPI(title="回测接口", version="0.1.0", description="回测运行与指标查询 API")

# ============================================================
# Prometheus 指标定义
# ============================================================

# API请求统计
_BACKTEST_API_REQUESTS_TOTAL = Counter(
    "backtest_api_requests_total",
    "Total API requests to backtest service",
    labelnames=("endpoint", "method", "status_code"),
)

_BACKTEST_API_LATENCY_SECONDS = Histogram(
    "backtest_api_latency_seconds",
    "API request latency in seconds",
    labelnames=("endpoint", "method"),
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# 回测运行统计
_BACKTEST_RUNS_QUERIED = Counter(
    "backtest_runs_queried_total",
    "Number of times backtest runs were queried",
)

_BACKTEST_METRICS_QUERIED = Counter(
    "backtest_metrics_queried_total",
    "Number of times backtest metrics were queried",
    labelnames=("found",),
)

# 数据库查询统计
_BACKTEST_DB_QUERY_SECONDS = Histogram(
    "backtest_db_query_seconds",
    "Database query latency for backtest operations",
    labelnames=("operation",),
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# 缓存的回测统计（从数据库读取的最新统计）
_BACKTEST_TOTAL_RUNS = Gauge(
    "backtest_total_runs",
    "Total number of backtest runs in database",
)

# ============================================================
# Prometheus 中间件
# ============================================================

@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    """记录每个请求的指标"""
    # 跳过 /metrics 端点本身，避免递归
    if request.url.path == "/metrics":
        return await call_next(request)
    
    start_time = time.time()
    
    # 处理请求
    response = await call_next(request)
    
    # 计算延迟
    duration = time.time() - start_time
    
    # 记录指标
    endpoint = request.url.path
    method = request.method
    status_code = str(response.status_code)
    
    _BACKTEST_API_REQUESTS_TOTAL.labels(
        endpoint=endpoint,
        method=method,
        status_code=status_code,
    ).inc()
    
    _BACKTEST_API_LATENCY_SECONDS.labels(
        endpoint=endpoint,
        method=method,
    ).observe(duration)
    
    return response


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

    query_start = time.time()
    with db_session(settings) as session:
        dao = _dao(session)
        result_rows = dao.fetch_runs(
            start_from=start_from, start_to=start_to, limit=limit, offset=offset
        )
        rows = [asdict(row) for row in result_rows]
    
    _BACKTEST_DB_QUERY_SECONDS.labels(operation="list_runs").observe(time.time() - query_start)
    _BACKTEST_RUNS_QUERIED.inc()
    
    # 更新总回测数（用于监控看板）
    _BACKTEST_TOTAL_RUNS.set(len(rows) if not offset else len(rows) + offset)

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
    "/backtest-metrics/{run_id}",
    response_model=ApiResult,
    summary="回测指标",
    description="返回指定回测的总览指标及按交易日拆分的明细。",
)
def read_metrics(run_id: int) -> ApiResult:
    query_start = time.time()
    with db_session(settings) as session:
        dao = _dao(session)
        total_rows = [asdict(row) for row in dao.fetch_metrics_total(run_id)]
        if not total_rows:
            _BACKTEST_METRICS_QUERIED.labels(found="false").inc()
            raise HTTPException(status_code=404, detail="回测任务不存在")
        daily_rows = [asdict(row) for row in dao.fetch_metrics_daily(run_id)]
    
    _BACKTEST_DB_QUERY_SECONDS.labels(operation="fetch_metrics").observe(time.time() - query_start)
    _BACKTEST_METRICS_QUERIED.labels(found="true").inc()

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


@app.get(
    "/metrics",
    summary="Prometheus指标",
    description="暴露Prometheus格式的监控指标",
    include_in_schema=False,
)
async def prometheus_metrics():
    """返回Prometheus格式的指标数据"""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
