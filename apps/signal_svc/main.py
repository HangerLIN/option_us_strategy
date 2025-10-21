from __future__ import annotations

import asyncio
import logging
from datetime import timezone

from fastapi import FastAPI, HTTPException, Response, WebSocket, WebSocketDisconnect
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from libs.core import (
    TraceContextMiddleware,
    configure_logging,
    get_settings,
    update_trace_context,
    utc_now,
)
from libs.infra import RedisBus
from libs.infra.db import get_session_factory
from libs.schemas.common import ApiResult, ServiceHealth
from libs.schemas.events import BarsClosed
from libs.schemas.signals import (
    SignalPreviewEntry,
    SignalPreviewRequest,
    SignalPreviewResponse,
    SignalPushItem,
    SignalPushRequest,
    SignalPushResponse,
    SignalPushResult,
)
from .engine import SignalEngine

LOGGER = logging.getLogger(__name__)

settings = get_settings()
configure_logging(settings)

app = FastAPI(title="信号服务", version="0.1.0", description="交易信号生成与预览接口")
app.add_middleware(TraceContextMiddleware)
redis_bus = RedisBus(settings.redis_url)
session_factory = get_session_factory(settings)
signal_engine = SignalEngine(session_factory, redis_bus)


class _SignalStreamBroker:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(websocket)

    async def broadcast(self, payload: dict) -> None:
        async with self._lock:
            connections = list(self._connections)
        for connection in connections:
            try:
                await connection.send_json(payload)
            except Exception:
                await self.disconnect(connection)


signal_stream = _SignalStreamBroker()


def _build_signal_message(signals, *, source: str) -> dict:
    return {
        "event": "signal",
        "source": source,
        "count": len(signals),
        "signals": [signal.model_dump(mode="json") for signal in signals],
        "timestamp": utc_now().isoformat(),
    }


@app.get(
    "/healthz",
    response_model=ApiResult,
    summary="健康检查",
    description="用于探测服务存活状态。",
)
async def healthcheck() -> ApiResult:
    payload = ServiceHealth(status="ok", service="signal_svc", timestamp=utc_now())
    return ApiResult(ok=True, code="OK", message="服务正常", data=payload)


@app.get("/metrics", summary="Prometheus 指标")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post(
    "/signals/preview",
    response_model=ApiResult,
    summary="信号复算预览",
    description="给定标的与时间区间，复算策略信号用于风控/执行前审查。",
)
async def preview_signals(payload: SignalPreviewRequest) -> ApiResult:
    if payload.start.tzinfo is None or payload.end.tzinfo is None:
        raise HTTPException(status_code=400, detail="开始与结束时间必须包含时区信息")
    if payload.end <= payload.start:
        raise HTTPException(status_code=400, detail="结束时间必须晚于开始时间")
    symbol = payload.symbol.upper()
    start_utc = payload.start.astimezone(timezone.utc)
    end_utc = payload.end.astimezone(timezone.utc)
    detected = signal_engine.preview_signals(symbol=symbol, start=start_utc, end=end_utc)
    entries = [
        SignalPreviewEntry(trace_id=item.trace_id, ts_end=item.ts_end, signal=item.signal)
        for item in detected
    ]
    response = SignalPreviewResponse(
        symbol=symbol,
        start=start_utc,
        end=end_utc,
        count=len(entries),
        signals=entries,
    )
    message = "复算完成" if entries else "区间内无有效信号"
    return ApiResult(ok=True, code="OK", message=message, data=response.model_dump())


@app.post(
    "/signals/push",
    response_model=ApiResult,
    summary="外部信号灌入",
    description="接收外部策略生成的信号并写入队列，返回逐条处理结果。",
)
async def push_signals(payload: SignalPushRequest) -> ApiResult:
    if not payload.signals:
        raise HTTPException(status_code=400, detail="至少需要提供一条有效信号")
    normalized_items: list[SignalPushItem] = []
    for item in payload.signals:
        generated_at = item.signal.generated_at
        if generated_at.tzinfo is None:
            raise HTTPException(status_code=400, detail="generated_at 必须包含时区信息")
        normalized_signal = item.signal.model_copy(
            update={"generated_at": generated_at.astimezone(timezone.utc)}
        )
        normalized_items.append(
            SignalPushItem(signal=normalized_signal, trace_id=item.trace_id, force=item.force)
        )
    results = signal_engine.push_signals(normalized_items)
    push_results = [
        SignalPushResult(
            trace_id=item["trace_id"],
            accepted=item["accepted"],
            reason=item["reason"],
            signal=item["signal"],
        )
        for item in results
    ]
    accepted_signals = [result.signal for result in push_results if result.accepted]
    if accepted_signals:
        await signal_stream.broadcast(
            _build_signal_message(accepted_signals, source="external_push")
        )
    accepted = sum(1 for item in push_results if item.accepted)
    response = SignalPushResponse(count=len(push_results), accepted=accepted, results=push_results)
    ok = accepted > 0
    code = "OK" if ok else "REJECT:NO_SIGNAL_ACCEPTED"
    message = "信号已转发" if ok else "全部信号因频控被拒绝"
    return ApiResult(ok=ok, code=code, message=message, data=response.model_dump())


@app.on_event("startup")
async def startup_event() -> None:
    if settings.app_env == "test":
        LOGGER.debug("Skipping redis ping in test environment")
        return
    await redis_bus.ping()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await redis_bus.close()


@app.post(
    "/engine/process",
    response_model=ApiResult,
    summary="实时信号处理",
    description="供数据管道调用，处理实时 bars_closed 事件并产出信号。",
)
async def process_bar(event: BarsClosed) -> ApiResult:
    update_trace_context(symbol=event.symbol)
    signals = signal_engine.process_bar(event)
    payload = [signal.model_dump() for signal in signals]
    if signals:
        await signal_stream.broadcast(_build_signal_message(signals, source="bars_closed"))
    return ApiResult(ok=True, code="OK", message="处理完成", data=payload)


@app.websocket("/ws/signals")
async def signals_ws(websocket: WebSocket) -> None:
    await signal_stream.connect(websocket)
    await websocket.send_json(
        {
            "event": "connected",
            "service": "signal_svc",
            "channel": "signals",
            "timestamp": utc_now().isoformat(),
        }
    )
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await signal_stream.disconnect(websocket)
