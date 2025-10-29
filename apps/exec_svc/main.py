from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from typing import Any, Awaitable, Callable, Dict, Optional, Set

import httpx
import structlog
from fastapi import FastAPI, HTTPException, Response, WebSocket, WebSocketDisconnect
from ibapi.contract import Contract
from ibapi.order import Order
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from libs.core import (
    TRACE_HEADER,
    TraceContextMiddleware,
    attach_trace_metadata,
    configure_logging,
    current_trace_context,
    get_settings,
    to_utc,
    update_trace_context,
    utc_now,
)
from libs.db.dao import StrategyPositionDAO
from libs.infra import IBClient, RedisBus, build_ibkr_client, get_next_order_id
from libs.infra.db import get_session_factory
from libs.infra.metrics import (
    observe_order_execution_latency,
    record_order_accept,
    record_order_block,
    record_order_submission,
    record_order_timeout,
    set_orders_by_status,
    set_pending_order_age,
)
from libs.schemas.common import ApiResult, ServiceHealth
from libs.schemas.events import ExecutionFill, ForceCloseEvent, RiskAlert, RiskBlock, RiskUnblock
from libs.schemas.exec import (
    ExecutionMode,
    ExecutionRequest,
    OrderCancelRequest,
    OrderSide,
    OrderState,
)

LOGGER = structlog.get_logger(__name__)

settings = get_settings()
configure_logging(settings)

redis_bus = RedisBus(settings.redis_url)
ib_client: Optional[IBClient] = None
session_factory = get_session_factory(settings)
orders_registry: dict[int, OrderState] = {}
orders_lock = asyncio.Lock()
trace_registry: Dict[str, int] = {}
METRICS_ENABLED = bool(settings.monitoring_enabled)


class _OrderStreamBroker:
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

    async def broadcast(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            connections = list(self._connections)
        for connection in connections:
            try:
                await connection.send_json(payload)
            except Exception:
                await self.disconnect(connection)


order_stream = _OrderStreamBroker()


def _emit_metric(func, *args, **kwargs) -> None:
    if not METRICS_ENABLED:
        return
    try:
        func(*args, **kwargs)
    except Exception:  # pragma: no cover - metrics best-effort
        pass


def _refresh_order_metrics() -> None:
    if not METRICS_ENABLED:
        return
    try:
        counts = Counter(state.status for state in orders_registry.values())
        for status, count in counts.items():
            _emit_metric(set_orders_by_status, status, count)
        # Ensure gauges fall back to zero for statuses not observed recently
        for status in {"submitted", "partial", "filled", "cancelled", "inactive", "pending", "cancelling"}:
            if status not in counts:
                _emit_metric(set_orders_by_status, status, 0)

        pending_states = [
            state for state in orders_registry.values()
            if state.status in {"submitted", "partial", "pending", "cancelling"}
        ]
        if pending_states:
            oldest = min(state.submitted_at for state in pending_states)
            age_seconds = max(0.0, (utc_now() - oldest).total_seconds())
        else:
            age_seconds = 0.0
        _emit_metric(set_pending_order_age, age_seconds)
    except Exception:  # pragma: no cover - defensive metrics bookkeeping
        pass


def _order_state_payload(event_type: str, state: OrderState) -> dict[str, Any]:
    return {
        "event": "order_state",
        "type": event_type,
        "order": state.model_dump(mode="json"),
        "trace_id": state.details.trace_id,
        "timestamp": utc_now().isoformat(),
    }


async def _broadcast_order_state(event_type: str, state: OrderState) -> None:
    await order_stream.broadcast(_order_state_payload(event_type, state))


@dataclass
class TraceMeta:
    payload: ExecutionRequest
    contract: Contract
    mode: ExecutionMode
    side: OrderSide
    min_tick: Decimal | None
    base_mid: Decimal | None
    spread: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    attempts: int = 0
    force_jumps: int = 0
    last_price: Decimal | None = None


trace_meta: Dict[str, TraceMeta] = {}
order_tasks: Dict[int, list[asyncio.Task[Any]]] = {}
blocked_symbols: Set[str] = set()
exit_priority: Set[str] = set()
risk_task: Optional[asyncio.Task] = None
signals_task: Optional[asyncio.Task] = None
risk_bus: Optional[RedisBus] = None
signals_bus: Optional[RedisBus] = None
ib_event_queue: Optional[asyncio.Queue[Dict[str, Any]]] = None
ib_events_task: Optional[asyncio.Task] = None
pending_commissions: Dict[str, Decimal] = {}

app = FastAPI(title="执行服务", version="0.1.0", description="执行与成本控制 API")
app.add_middleware(TraceContextMiddleware)


def _require_ib_client() -> IBClient:
    client = ib_client
    if client is None:
        raise RuntimeError("IBKR client not initialised")
    return client


@app.on_event("startup")
async def startup_event() -> None:
    global ib_client, risk_bus, signals_bus, risk_task, signals_task, ib_event_queue, ib_events_task
    if settings.app_env == "test":
        LOGGER.debug("Skipping IBKR connection in test environment")
        return
    ib_client = build_ibkr_client(settings)
    loop = asyncio.get_running_loop()
    ib_event_queue = asyncio.Queue()
    ib_client.register_event_queue(loop, ib_event_queue)
    # Request execution reports to get any fills that happened while disconnected
    ib_client.req_executions()
    ib_events_task = asyncio.create_task(_consume_ib_events())
    await redis_bus.ping()
    risk_bus = RedisBus(settings.redis_url)
    signals_bus = RedisBus(settings.redis_url)
    risk_task = asyncio.create_task(_consume_risk_events())
    signals_task = asyncio.create_task(_consume_signal_events())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global ib_event_queue, ib_events_task
    if ib_events_task is not None:
        ib_events_task.cancel()
        try:
            await ib_events_task
        except asyncio.CancelledError:  # pragma: no cover
            pass
        ib_events_task = None
    ib_event_queue = None
    if risk_task is not None:
        risk_task.cancel()
        try:
            await risk_task
        except asyncio.CancelledError:  # pragma: no cover
            pass
    if signals_task is not None:
        signals_task.cancel()
        try:
            await signals_task
        except asyncio.CancelledError:  # pragma: no cover
            pass
    if risk_bus is not None:
        await risk_bus.close()
    if signals_bus is not None:
        await signals_bus.close()
    await redis_bus.close()
    if ib_client is not None:
        ib_client.stop()


@app.get(
    "/healthz",
    response_model=ApiResult,
    summary="健康检查",
    description="探测执行服务状态。",
)
async def healthcheck() -> ApiResult:
    payload = ServiceHealth(status="ok", service="exec_svc", timestamp=utc_now())
    return ApiResult(ok=True, code="OK", message="服务正常", data=payload)


@app.get("/metrics", summary="Prometheus 指标")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.websocket("/ws/orders")
async def orders_ws(websocket: WebSocket) -> None:
    await order_stream.connect(websocket)
    await websocket.send_json(
        {
            "event": "connected",
            "service": "exec_svc",
            "channel": "orders",
            "timestamp": utc_now().isoformat(),
        }
    )
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await order_stream.disconnect(websocket)


def _build_contract(symbol: str, payload: ExecutionRequest | None = None) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.exchange = "SMART"
    contract.currency = "USD"
    
    # If option parameters are provided, build option contract
    if payload and payload.option_right and payload.option_strike and payload.option_expiry:
        contract.secType = "OPT"
        contract.right = payload.option_right
        contract.strike = float(payload.option_strike)
        contract.lastTradeDateOrContractMonth = payload.option_expiry
    else:
        # Default to stock
        contract.secType = "STK"
    
    return contract


def _build_order(payload: ExecutionRequest, price: Decimal, trace_id: str) -> Order:
    order = Order()
    order.action = payload.side.value
    order.totalQuantity = payload.quantity
    order.orderType = "LMT"
    order.lmtPrice = float(price)
    order.tif = payload.tif
    order.orderRef = trace_id
    order.eTradeOnly = False  # Explicitly disable eTradeOnly attribute
    order.firmQuoteOnly = False  # Explicitly disable firmQuoteOnly attribute
    return order


@app.post(
    "/orders/submit",
    response_model=ApiResult,
    summary="提交委托",
    description="执行前完成本地校验与风险检查，成功后进入委托簿。",
)
async def submit_order(payload: ExecutionRequest) -> ApiResult:
    try:
        _require_ib_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    update_trace_context(symbol=payload.symbol, signal_code=payload.strategy_code)
    mode_label = (
        payload.execution_mode.value
        if isinstance(payload.execution_mode, ExecutionMode)
        else str(payload.execution_mode)
    )
    _emit_metric(record_order_submission, mode_label)

    local_ok, local_code, local_message = _local_precheck(payload)
    if not local_ok:
        _emit_metric(record_order_block, "local", local_code)
        LOGGER.info(
            "order.precheck.local_block",
            symbol=payload.symbol,
            strategy=payload.strategy_code,
            code=local_code,
            detail=local_message,
        )
        return _reject_response(local_code, local_message, payload)

    notional = payload.limit_price * Decimal(payload.quantity) * OPTION_MULTIPLIER
    risk_ok, risk_code, risk_message = await _risk_precheck(payload, notional)
    if not risk_ok:
        _emit_metric(record_order_block, "risk", risk_code)
        LOGGER.info(
            "order.precheck.risk_block",
            symbol=payload.symbol,
            strategy=payload.strategy_code,
            code=risk_code,
            detail=risk_message,
        )
        return _reject_response(risk_code, risk_message, payload)

    trace_id = _ensure_trace(payload)
    async with orders_lock:
        existing_id = trace_registry.get(trace_id)
        if existing_id is not None:
            existing_state = orders_registry.get(existing_id)
            if existing_state is not None:
                LOGGER.info(
                    "order.idempotent_hit",
                    trace_id=trace_id,
                    order_id=existing_id,
                )
                return ApiResult(ok=True, code="OK", message="委托已存在", data=existing_state)

    contract = _build_contract(payload.symbol, payload)
    meta = _initialise_trace_meta(payload, contract)
    meta.payload = payload
    meta.base_mid = _current_mid(payload)
    meta.spread = _current_spread(payload)
    meta.bid = _to_decimal(payload.option_bid)
    meta.ask = _to_decimal(payload.option_ask)
    meta.min_tick = _to_decimal(payload.min_tick)
    meta.force_jumps = 0

    state, order_id = await _place_attempt(meta, trace_id, retry=False)
    _emit_metric(record_order_accept, mode_label)

    event_payload = attach_trace_metadata(meta.payload.model_dump())
    await redis_bus.publish(
        "execution-requests",
        event_payload,
        trace_id=str(event_payload["trace_id"]),
    )
    await _broadcast_order_state("submitted", state)

    return ApiResult(ok=True, code="OK", message="委托已受理", data=state)


@app.post(
    "/orders/cancel",
    response_model=ApiResult,
    summary="撤销委托",
    description="根据订单编号撤销未完成委托，并同步到 Redis 总线。",
)
async def cancel_order(request: OrderCancelRequest) -> ApiResult:
    try:
        client = _require_ib_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    async with orders_lock:
        state = orders_registry.get(request.order_id)
        if state is None:
            return ApiResult(ok=False, code="REJECT:ORDER_NOT_FOUND", message="委托不存在")

        update_trace_context(symbol=state.details.symbol, signal_code=state.details.strategy_code)

        if state.status == "cancelled":
            return ApiResult(ok=True, code="OK", message="委托已取消", data=state)

        updated_state = state.model_copy(
            update={
                "status": "cancelled",
                "updated_at": utc_now(),
                "reason_code": "CANCELLED_MANUAL",
            }
        )
        orders_registry[request.order_id] = updated_state
        _clear_tasks(request.order_id)
        trace_id = state.details.trace_id
        if trace_id and trace_registry.get(trace_id) == request.order_id:
            trace_registry.pop(trace_id, None)
        latency_seconds = max(
            0.0, (updated_state.updated_at - updated_state.submitted_at).total_seconds()
        )
        _emit_metric(observe_order_execution_latency, "cancel", latency_seconds)
        _refresh_order_metrics()

    client.cancelOrder(request.order_id)
    event_payload = attach_trace_metadata(updated_state.model_dump())
    await redis_bus.publish(
        "execution-cancels",
        event_payload,
        trace_id=str(event_payload["trace_id"]),
    )
    await _broadcast_order_state("cancelled", updated_state)
    return ApiResult(ok=True, code="OK", message="撤单成功", data=updated_state)


@app.get(
    "/orders/{order_id}",
    response_model=ApiResult,
    summary="查询委托状态",
    description="返回内存委托簿中的最新状态。",
)
async def get_order(order_id: int) -> ApiResult:
    async with orders_lock:
        state = orders_registry.get(order_id)
    if state is None:
        return ApiResult(ok=False, code="REJECT:ORDER_NOT_FOUND", message="委托不存在")
    update_trace_context(symbol=state.details.symbol, signal_code=state.details.strategy_code)
    return ApiResult(ok=True, code="OK", message="委托状态", data=state)


@app.post("/exec/place", include_in_schema=False)
async def legacy_place_order(payload: ExecutionRequest) -> ApiResult:
    return await submit_order(payload)


OPTION_MULTIPLIER = Decimal("100")


def _to_decimal(value: Optional[Decimal | float | int]) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _current_mid(payload: ExecutionRequest) -> Optional[Decimal]:
    if payload.option_mid is not None:
        return _to_decimal(payload.option_mid)
    if payload.option_bid is not None and payload.option_ask is not None:
        bid = _to_decimal(payload.option_bid)
        ask = _to_decimal(payload.option_ask)
        if bid is not None and ask is not None:
            return (bid + ask) / Decimal("2")
    return None


def _current_spread(payload: ExecutionRequest) -> Optional[Decimal]:
    if payload.option_spread is not None:
        return _to_decimal(payload.option_spread)
    if payload.option_bid is not None and payload.option_ask is not None:
        bid = _to_decimal(payload.option_bid)
        ask = _to_decimal(payload.option_ask)
        if bid is not None and ask is not None:
            return ask - bid
    return None


def _round_to_tick(price: Decimal, min_tick: Optional[Decimal], side: OrderSide) -> Decimal:
    if min_tick is None or min_tick <= 0:
        return price
    ticks = price / min_tick
    rounding = ROUND_UP if side == OrderSide.BUY else ROUND_DOWN
    ticks = ticks.quantize(Decimal("1"), rounding=rounding)
    return ticks * min_tick


def _local_precheck(payload: ExecutionRequest) -> tuple[bool, str, str]:
    if payload.is_exit:
        return True, "OK", "exit-order"

    symbol_upper = payload.symbol.upper()
    if symbol_upper in blocked_symbols and payload.side == OrderSide.BUY and not payload.is_exit:
        return False, "BLOCK:RISK_SYMBOL", "risk-blocked"

    if symbol_upper in exit_priority and payload.side == OrderSide.BUY and not payload.is_exit:
        return False, "BLOCK:EXIT_PENDING", "exit-priority"

    if payload.a1_gate is not None and payload.a1_gate is False:
        return False, "BLOCK:A1_GATE", "a1-gate-disabled"

    if payload.option_dte is not None and not (2 <= payload.option_dte <= 7):
        return False, "BLOCK:DTE", f"dte={payload.option_dte}"

    if payload.option_otm_steps is not None and not (2 <= payload.option_otm_steps <= 5):
        return False, "BLOCK:OTM_RANGE", f"otm_steps={payload.option_otm_steps}"

    open_interest = payload.option_open_interest
    volume = payload.option_volume
    mid = _current_mid(payload)
    spread = _current_spread(payload)
    if open_interest is not None and volume is not None and mid is not None and spread is not None:
        threshold = max(Decimal("0.10"), mid * Decimal("0.05"))
        if open_interest < 500 or volume < 100 or spread > threshold:
            return False, "BLOCK:LIQUIDITY", "liquidity-threshold"

    return True, "OK", "local-pass"


async def _risk_precheck(payload: ExecutionRequest, notional: Decimal) -> tuple[bool, str, str]:
    if payload.is_exit:
        return True, "OK", "exit-bypass"
    request_payload = {
        "strategy_code": payload.strategy_code,
        "symbol": payload.symbol,
        "notional": str(notional),
        "implied_vol": payload.implied_vol or 0.0,
        "timestamp": utc_now().isoformat(),
        "signal_code": payload.signal_code,
        "option_right": payload.option_right,
    }

    url = settings.risk_service_url.rstrip("/") + "/precheck"
    headers = {}
    trace_id = payload.trace_id or current_trace_context().get("trace_id")
    if trace_id:
        headers[TRACE_HEADER] = trace_id
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(url, json=request_payload, headers=headers)
        response.raise_for_status()
    except httpx.HTTPError as exc:  # noqa: BLE001
        LOGGER.exception("risk_precheck.failed", error=str(exc))
        return False, "BLOCK:RISK_UNAVAILABLE", "risk-precheck-unavailable"

    payload_json = response.json()
    ok = bool(payload_json.get("ok"))
    code = str(payload_json.get("code") or ("OK" if ok else "BLOCK:RISK_FAIL"))
    message = str(payload_json.get("message") or "")
    return ok, code, message


def _reject_response(code: str, message: str, payload: ExecutionRequest) -> ApiResult:
    now = utc_now()
    state = OrderState(
        order_id=-1,
        status="rejected",
        submitted_at=now,
        updated_at=now,
        reason_code=code,
        details=payload,
    )
    return ApiResult(ok=False, code=code, message=message, data=state)


def _ensure_trace(payload: ExecutionRequest) -> str:
    if payload.trace_id:
        return payload.trace_id
    raise HTTPException(status_code=400, detail="trace_id required for idempotent execution")


def _initialise_trace_meta(payload: ExecutionRequest, contract: Contract) -> TraceMeta:
    trace_id = _ensure_trace(payload)
    return trace_meta.setdefault(
        trace_id,
        TraceMeta(
            payload=payload,
            contract=contract,
            mode=payload.execution_mode,
            side=payload.side,
            min_tick=_to_decimal(payload.min_tick),
            base_mid=_current_mid(payload),
            spread=_current_spread(payload),
            bid=_to_decimal(payload.option_bid),
            ask=_to_decimal(payload.option_ask),
        ),
    )


def _clear_tasks(order_id: int) -> None:
    tasks = order_tasks.pop(order_id, [])
    for task in tasks:
        task.cancel()


async def _cancel_pending_buys(symbol: str, reason_code: str) -> None:
    symbol_upper = symbol.upper()
    order_ids: list[int] = []
    async with orders_lock:
        for order_id, state in list(orders_registry.items()):
            if (
                state.details.symbol.upper() == symbol_upper
                and state.status == "submitted"
                and state.details.side == OrderSide.BUY
            ):
                updated = state.model_copy(
                    update={
                        "status": "cancelling",
                        "updated_at": utc_now(),
                        "reason_code": reason_code,
                    }
                )
                orders_registry[order_id] = updated
                _clear_tasks(order_id)
                order_ids.append(order_id)
    try:
        client = _require_ib_client()
    except RuntimeError:
        return
    for order_id in order_ids:
        try:
            client.cancelOrder(order_id)
        except Exception:  # pragma: no cover - external interaction
            LOGGER.exception("order.cancel_failed", order_id=order_id)


async def _force_close_symbol(strategy_code: str, symbol: str, trace_id: str) -> None:
    symbol_upper = symbol.upper()
    exit_priority.add(symbol_upper)
    session = session_factory()
    try:
        dao = StrategyPositionDAO(session)
        positions = dao.list_positions(strategy_code=strategy_code, symbol=symbol)
        for position in positions:
            quantity = int(abs(position.open_quantity))
            if quantity == 0:
                continue
            side = OrderSide.SELL if position.open_quantity > 0 else OrderSide.BUY
            mark_price = position.mark_price if position.mark_price else Decimal("0.01")
            limit_price = Decimal(mark_price)
            request = ExecutionRequest(
                strategy_code=strategy_code,
                symbol=position.symbol,
                side=side,
                quantity=quantity,
                limit_price=limit_price,
                tif="DAY",
                signal_code="FORCE_CLOSE",
                option_right=position.option_right,
                option_dte=None,
                option_otm_steps=None,
                option_open_interest=None,
                option_volume=None,
                option_bid=None,
                option_ask=None,
                option_mid=None,
                option_spread=None,
                implied_vol=None,
                a1_gate=True,
                is_exit=True,
                min_tick=Decimal("0.01"),
                execution_mode=ExecutionMode.FORCE,
                trace_id=f"{trace_id}-force-{position.symbol}-{position.option_right or 'NA'}",
            )
            result = await submit_order(request)
            LOGGER.info(
                "force_close.submitted",
                symbol=symbol,
                strategy=strategy_code,
                side=side.value,
                qty=quantity,
                ok=result.ok,
            )
    finally:
        session.close()
        exit_priority.discard(symbol_upper)


async def _handle_risk_block(event: RiskBlock) -> None:
    symbol = (event.symbol or "").upper()
    if not symbol:
        return
    blocked_symbols.add(symbol)
    LOGGER.info(
        "risk.blocked",
        symbol=symbol,
        reason=event.reason,
        limit=event.limit_code,
    )
    await _cancel_pending_buys(symbol, "BLOCKED_BY_RISK")


async def _handle_risk_unblock(event: RiskUnblock) -> None:
    symbol = (event.symbol or "").upper()
    if not symbol:
        return
    blocked_symbols.discard(symbol)
    exit_priority.discard(symbol)
    LOGGER.info("risk.unblocked", symbol=symbol, reason=event.reason)


async def _handle_risk_alert(event: RiskAlert) -> None:
    if event.alert_code.upper() != "FORCE_CLOSE":
        return
    symbol = event.symbol or ""
    if not symbol:
        LOGGER.warning("risk.alert.force_close_missing_symbol", trace_id=event.trace_id)
        return
    strategy = event.strategy_code or "core-vol"
    await _force_close_symbol(strategy, symbol, event.trace_id)


async def _handle_force_close_event(event: ForceCloseEvent) -> None:
    symbol = (event.symbol or "").upper()
    if not symbol:
        LOGGER.warning("risk.force_close_missing_symbol", trace_id=event.trace_id)
        return
    strategy = event.strategy_code or "core-vol"
    LOGGER.info(
        "risk.force_close_received",
        symbol=symbol,
        strategy=strategy,
        reason=event.reason,
    )
    await _force_close_symbol(strategy, symbol, event.trace_id)


def _compute_initial_price(meta: TraceMeta) -> Decimal:
    payload = meta.payload
    mode = meta.mode
    side = meta.side
    mid = meta.base_mid
    spread = meta.spread
    bid = meta.bid
    ask = meta.ask

    if mode == ExecutionMode.PASSIVE and mid is not None:
        price = mid
    elif mode == ExecutionMode.FORCE:
        if side == OrderSide.BUY and ask is not None:
            price = ask
        elif side == OrderSide.SELL and bid is not None:
            price = bid
        else:
            price = payload.limit_price
    else:  # MARKETABLE
        if side == OrderSide.BUY:
            best = [payload.limit_price]
            if ask is not None:
                best.append(ask)
            if mid is not None and spread is not None:
                best.append(mid + spread * Decimal("0.6"))
            price = min(best)
        else:
            best = [payload.limit_price]
            if bid is not None:
                best.append(bid)
            if mid is not None and spread is not None:
                best.append(mid - spread * Decimal("0.6"))
            price = max(best)
    return _round_to_tick(price, meta.min_tick, side)


def _compute_passive_adjust(meta: TraceMeta) -> Optional[Decimal]:
    mid = meta.base_mid
    spread = meta.spread
    if mid is None or spread is None:
        return None
    if meta.side == OrderSide.BUY:
        price = mid + spread * Decimal("0.2")
    else:
        price = mid - spread * Decimal("0.2")
    return _round_to_tick(price, meta.min_tick, meta.side)


def _compute_force_jump(meta: TraceMeta) -> Optional[Decimal]:
    min_tick = meta.min_tick
    if min_tick is None:
        return None
    last_price = meta.last_price or _to_decimal(meta.payload.limit_price)
    if last_price is None:
        return None
    delta = min_tick * Decimal(meta.force_jumps + 1)
    if meta.side == OrderSide.BUY:
        price = last_price + delta
    else:
        price = last_price - delta
    return _round_to_tick(price, min_tick, meta.side)


def _compute_retry_price(meta: TraceMeta) -> Decimal:
    # 第二次尝试时，PASSIVE 升级为可成交价，其他保持原策略
    if meta.mode == ExecutionMode.PASSIVE:
        original_mode = meta.mode
        meta.mode = ExecutionMode.MARKETABLE
        price = _compute_initial_price(meta)
        meta.mode = original_mode
        return price
    return _compute_initial_price(meta)


async def _place_attempt(meta: TraceMeta, trace_id: str, *, retry: bool) -> tuple[OrderState, int]:
    client = _require_ib_client()
    async with orders_lock:
        meta.attempts += 1
        order_id = get_next_order_id(client)

    price = _compute_retry_price(meta) if retry else _compute_initial_price(meta)
    meta.last_price = price
    meta.payload.limit_price = price
    if retry:
        meta.force_jumps = 0

    order = _build_order(meta.payload, price, trace_id)

    LOGGER.info(
        "order.place_attempt",
        trace_id=trace_id,
        order_id=order_id,
        attempt=meta.attempts,
        price=str(price),
        mode=meta.mode.value,
    )
    client.placeOrder(order_id, meta.contract, order)

    now = utc_now()
    state = OrderState(
        order_id=order_id,
        status="submitted",
        submitted_at=now,
        updated_at=now,
        reason_code=None,
        details=meta.payload,
    )

    async with orders_lock:
        orders_registry[order_id] = state
        trace_registry[trace_id] = order_id
        _clear_tasks(order_id)
        _schedule_tasks(order_id, trace_id, meta)
        _refresh_order_metrics()

    return state, order_id


def _schedule_tasks(order_id: int, trace_id: str, meta: TraceMeta) -> None:
    tasks: list[asyncio.Task[Any]] = []
    if meta.mode == ExecutionMode.PASSIVE and meta.attempts == 1:
        tasks.append(asyncio.create_task(_passive_adjust(order_id, trace_id)))
    if meta.mode == ExecutionMode.FORCE and meta.attempts == 1:
        tasks.append(asyncio.create_task(_force_adjust(order_id, trace_id)))
    tasks.append(asyncio.create_task(_ttl_watch(order_id, trace_id)))
    order_tasks[order_id] = tasks


async def _passive_adjust(order_id: int, trace_id: str) -> None:
    try:
        await asyncio.sleep(15)
    except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
        return
    async with orders_lock:
        if trace_registry.get(trace_id) != order_id:
            return
        state = orders_registry.get(order_id)
        meta = trace_meta.get(trace_id)
        if state is None or meta is None or state.status != "submitted":
            return
        new_price = _compute_passive_adjust(meta)
        if new_price is None or new_price == meta.last_price:
            return
        meta.last_price = new_price
        meta.payload.limit_price = new_price
        order = _build_order(meta.payload, new_price, trace_id)
    client = _require_ib_client()
    client.placeOrder(order_id, meta.contract, order)
    async with orders_lock:
        updated = state.model_copy(update={"updated_at": utc_now()})
        orders_registry[order_id] = updated


async def _force_adjust(order_id: int, trace_id: str) -> None:
    for _ in range(3):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:  # pragma: no cover
            return
        async with orders_lock:
            if trace_registry.get(trace_id) != order_id:
                return
            state = orders_registry.get(order_id)
            meta = trace_meta.get(trace_id)
            if state is None or meta is None or state.status != "submitted":
                return
            if meta.force_jumps >= 3:
                return
            price = _compute_force_jump(meta)
            if price is None:
                continue
            meta.force_jumps += 1
            meta.last_price = price
            meta.payload.limit_price = price
            order = _build_order(meta.payload, price, trace_id)
        client = _require_ib_client()
        client.placeOrder(order_id, meta.contract, order)
        async with orders_lock:
            updated = state.model_copy(update={"updated_at": utc_now()})
            orders_registry[order_id] = updated


async def _ttl_watch(order_id: int, trace_id: str) -> None:
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:  # pragma: no cover
        return

    retry_needed = False
    meta: TraceMeta | None = None
    async with orders_lock:
        state = orders_registry.get(order_id)
        current_id = trace_registry.get(trace_id)
        meta = trace_meta.get(trace_id)
        if state is None or current_id != order_id or meta is None or state.status != "submitted":
            return
        if meta.attempts >= 2:
            updated = state.model_copy(
                update={
                    "status": "cancelled",
                    "updated_at": utc_now(),
                    "reason_code": "TTL_EXPIRED",
                }
            )
            orders_registry[order_id] = updated
            _clear_tasks(order_id)
            latency_seconds = max(
                0.0, (updated.updated_at - updated.submitted_at).total_seconds()
            )
            _emit_metric(record_order_timeout, "ttl_expired")
            _emit_metric(observe_order_execution_latency, "cancel", latency_seconds)
        else:
            retry_needed = True
            updated = state.model_copy(
                update={
                    "status": "cancelling",
                    "updated_at": utc_now(),
                    "reason_code": "TTL_RETRY",
                }
            )
            orders_registry[order_id] = updated
            _clear_tasks(order_id)
            _emit_metric(record_order_timeout, "ttl_retry")
        _refresh_order_metrics()

    client = _require_ib_client()
    client.cancelOrder(order_id)

    if not retry_needed or meta is None:
        return

    await asyncio.sleep(1)
    new_state, new_order_id = await _place_attempt(meta, trace_id, retry=True)
    async with orders_lock:
        if order_id in orders_registry:
            cancelled = orders_registry[order_id].model_copy(
                update={
                    "status": "cancelled",
                    "updated_at": utc_now(),
                    "reason_code": "TTL_RETRY_CANCELLED",
                }
            )
            orders_registry[order_id] = cancelled
            latency_seconds = max(
                0.0, (cancelled.updated_at - cancelled.submitted_at).total_seconds()
            )
            _emit_metric(observe_order_execution_latency, "cancel", latency_seconds)
            _refresh_order_metrics()
    LOGGER.info(
        "order.retry.success",
        previous_order_id=order_id,
        new_order_id=new_order_id,
        trace_id=trace_id,
    )


async def _consume_risk_events() -> None:
    assert risk_bus is not None
    streams: list[tuple[str, type[Any], Callable[[Any], Awaitable[None]]]] = [
        ("risk_block", RiskBlock, _handle_risk_block),
        ("risk_unblock", RiskUnblock, _handle_risk_unblock),
        ("risk_alert", RiskAlert, _handle_risk_alert),
        ("force_close", ForceCloseEvent, _handle_force_close_event),
    ]
    for stream, *_ in streams:
        await risk_bus.ensure_group(stream, "exec_svc", start_id="0")

    try:
        while True:
            for stream, model, handler in streams:
                entries = await risk_bus.consume(
                    stream, "exec_svc", "exec-risk", count=10, block_ms=1000
                )
                if not entries:
                    continue
                for entry in entries:
                    payload = entry.get("payload", {})
                    try:
                        event = model(**payload)
                    except Exception:
                        LOGGER.exception("risk_event.parse_failed", stream=stream, payload=payload)
                        continue
                    await handler(event)
    except asyncio.CancelledError:  # pragma: no cover - task cancelled on shutdown
        return
    except Exception:
        LOGGER.exception("risk_event.consumer_failed")


async def _consume_signal_events() -> None:
    assert signals_bus is not None
    await signals_bus.ensure_group("signals", "exec_svc", start_id="0")
    try:
        while True:
            entries = await signals_bus.consume(
                "signals", "exec_svc", "exec-signals", count=20, block_ms=5000
            )
            if not entries:
                continue
            for entry in entries:
                payload = entry.get("payload", {})
                await _handle_signal_payload(payload)
    except asyncio.CancelledError:  # pragma: no cover
        return
    except Exception:
        LOGGER.exception("signal.consumer_failed")


async def _consume_ib_events() -> None:
    if ib_event_queue is None:
        return
    try:
        while True:
            event = await ib_event_queue.get()
            event_type = str(event.get("type") or "")
            if event_type == "order_status":
                await _handle_order_status_event(event)
            elif event_type == "execution":
                await _handle_execution_event(event)
            elif event_type == "commission":
                _handle_commission_event(event)
    except asyncio.CancelledError:  # pragma: no cover
        return
    except Exception:
        LOGGER.exception("ib_event.consumer_failed")


def _handle_commission_event(event: Dict[str, Any]) -> None:
    exec_id = str(event.get("exec_id") or "")
    if not exec_id:
        return
    try:
        pending_commissions[exec_id] = Decimal(str(event.get("commission", "0")))
    except (InvalidOperation, TypeError, ValueError):  # pragma: no cover - defensive
        pending_commissions[exec_id] = Decimal("0")


async def _handle_order_status_event(event: Dict[str, Any]) -> None:
    order_id = int(event.get("order_id") or 0)
    status_raw = str(event.get("status") or "")
    remaining_raw = event.get("remaining")
    filled_raw = event.get("filled")
    remaining: Decimal | None = None
    filled: Decimal | None = None
    try:
        if remaining_raw is not None:
            remaining = Decimal(str(remaining_raw))
    except (InvalidOperation, TypeError):  # pragma: no cover - defensive
        remaining = None
    try:
        if filled_raw is not None:
            filled = Decimal(str(filled_raw))
    except (InvalidOperation, TypeError):  # pragma: no cover
        filled = None

    status = _map_ib_status(status_raw)
    async with orders_lock:
        state = orders_registry.get(order_id)
        if state is None:
            LOGGER.debug("order_status.unknown_order", order_id=order_id, status=status_raw)
            return
        if status == "submitted" and filled is not None and filled > 0 and (remaining is None or remaining > 0):
            status = "partial"
        updates = {
            "status": status,
            "updated_at": utc_now(),
            "reason_code": status_raw.upper() if status_raw else state.reason_code,
        }
        updated = state.model_copy(update=updates)
        orders_registry[order_id] = updated
        if status in {"filled", "cancelled", "apicancelled", "inactive"} or (
            remaining is not None and remaining <= 0
        ):
            _finalise_order_lifecycle(order_id, updated)
        if status in {"cancelled", "apicancelled", "inactive"}:
            latency_seconds = max(
                0.0, (updated.updated_at - updated.submitted_at).total_seconds()
            )
            _emit_metric(observe_order_execution_latency, "cancel", latency_seconds)
        _refresh_order_metrics()
        broadcast_state = updated
    await _broadcast_order_state("status", broadcast_state)


async def _handle_execution_event(event: Dict[str, Any]) -> None:
    order_id = int(event.get("order_id") or 0)
    exec_id = str(event.get("exec_id") or "")
    client_order_id = str(event.get("client_order_id") or "")
    shares = _decimal(event.get("shares"))
    price = _decimal(event.get("price"))
    cum_qty = _decimal(event.get("cum_qty"))
    filled_at = _parse_ib_time(str(event.get("time") or ""))
    contract_info: Dict[str, Any] = event.get("contract") or {}
    exchange = event.get("exchange")
    side_raw = str(event.get("side") or "BOT")

    latency_seconds: float | None = None
    async with orders_lock:
        state = orders_registry.get(order_id)
        if state is None:
            # Order not in registry (maybe from restart) - use event data directly
            LOGGER.warning("execution_event.unknown_order_fallback", order_id=order_id, exec_id=exec_id, client_order_id=client_order_id)
            trace_id = client_order_id if client_order_id else f"exec-{exec_id}"
            if not client_order_id:
                LOGGER.warning("execution_event.missing_client_order_id", order_id=order_id, exec_id=exec_id)
            # Publish fill event even without order state
            fees = pending_commissions.pop(exec_id, None)
            LOGGER.info("execution_event.building_fill", trace_id=trace_id, symbol=contract_info.get("symbol"), right=contract_info.get("right"))
            fill = ExecutionFill(
                trace_id=trace_id,
                order_id=order_id,
                client_order_id=trace_id,
                symbol=contract_info.get("symbol", "UNKNOWN"),
                side="BUY" if side_raw in {"BOT", "BUY"} else "SELL",
                fill_quantity=shares,
                fill_price=price,
                strategy_code="UNKNOWN",
                signal_code=None,
                option_right=_normalize_contract_field(contract_info.get("right")),
                conid=_int_or_none(contract_info.get("conid")),
                strike=_decimal_or_none(contract_info.get("strike")),
                expiry=_parse_contract_expiry(contract_info.get("expiry")),
                delta=None,
                fees=fees,
                execution_id=exec_id,
                venue=str(exchange) if exchange is not None else None,
                filled_at=filled_at,
            )
            payload = fill.model_dump(mode="json")
            await redis_bus.publish("execution_fills", payload, trace_id=fill.trace_id)
            LOGGER.info("execution_event.published_fallback_fill", trace_id=fill.trace_id, symbol=fill.symbol, option_right=fill.option_right)
            return
        
        request = state.details
        trace_id = client_order_id or (request.trace_id or "")
        if not trace_id:
            LOGGER.warning("execution_event.missing_trace", order_id=order_id, exec_id=exec_id)
            return
        total_quantity = Decimal(request.quantity)
        status = "filled" if cum_qty >= total_quantity and total_quantity > 0 else "partial"
        updated_state = state.model_copy(
            update={
                "status": status,
                "updated_at": utc_now(),
                "reason_code": "FILLED" if status == "filled" else "PARTIAL",
            }
        )
        orders_registry[order_id] = updated_state
        if status == "filled":
            _finalise_order_lifecycle(order_id, updated_state)
        if filled_at is not None:
            latency_seconds = max(0.0, (filled_at - state.submitted_at).total_seconds())
        else:
            latency_seconds = max(0.0, (updated_state.updated_at - state.submitted_at).total_seconds())
        _refresh_order_metrics()
        broadcast_state = updated_state
    await _broadcast_order_state("fill", broadcast_state)

    fees = pending_commissions.pop(exec_id, None)
    fill = ExecutionFill(
        trace_id=trace_id,
        order_id=order_id,
        client_order_id=trace_id,
        symbol=request.symbol,
        side=request.side.value,
        fill_quantity=shares,
        fill_price=price,
        strategy_code=request.strategy_code,
        signal_code=request.signal_code,
        option_right=request.option_right or _normalize_contract_field(contract_info.get("right")),
        conid=_int_or_none(contract_info.get("conid")),
        strike=_decimal_or_none(contract_info.get("strike")),
        expiry=_parse_contract_expiry(contract_info.get("expiry")),
        delta=None,
        fees=fees,
        execution_id=exec_id,
        venue=str(exchange) if exchange is not None else None,
        filled_at=filled_at,
    )
    payload = fill.model_dump(mode="json")
    await redis_bus.publish("execution_fills", payload, trace_id=fill.trace_id)

    if latency_seconds is not None:
        stage = "fill" if status == "filled" else "partial"
        _emit_metric(observe_order_execution_latency, stage, latency_seconds)


async def _handle_signal_payload(payload: Dict[str, object]) -> None:
    signal_code = str(payload.get("signal_code") or "")
    symbol = payload.get("symbol")
    if not symbol:
        return
    if signal_code.startswith("SIG_EXIT") or signal_code in {"SIG_TIME_CLEAR_12_14"}:
        strategy = str(payload.get("strategy_code") or "core-vol")
        trace_id = str(payload.get("trace_id") or f"exit-{symbol}")
        await _force_close_symbol(strategy, str(symbol), trace_id)


def _map_ib_status(status: str) -> str:
    mapping = {
        "Filled": "filled",
        "ApiCancelled": "apicancelled",
        "Cancelled": "cancelled",
        "PreSubmitted": "submitted",
        "Submitted": "submitted",
        "PendingSubmit": "pending",
        "PendingCancel": "cancelling",
        "Inactive": "inactive",
    }
    return mapping.get(status, status.lower())


def _finalise_order_lifecycle(order_id: int, state: OrderState) -> None:
    _clear_tasks(order_id)
    trace_id = state.details.trace_id
    if trace_id:
        trace_registry.pop(trace_id, None)
        trace_meta.pop(trace_id, None)


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_contract_field(value: Any) -> str | None:
    """Normalize IBKR contract fields to our schema format."""
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    # Convert IBKR option right format: C -> CALL, P -> PUT
    if text == 'C':
        return 'CALL'
    elif text == 'P':
        return 'PUT'
    return text


def _parse_contract_expiry(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if len(text) == 8:
            dt = datetime.strptime(text, "%Y%m%d")
        elif len(text) == 6:
            dt = datetime.strptime(text, "%Y%m")
        else:
            return None
        dt = dt.replace(hour=16, minute=0, second=0, microsecond=0)
        return to_utc(dt)
    except ValueError:
        return None


def _parse_ib_time(value: str) -> datetime:
    if not value:
        return utc_now()
    base, _, fraction = value.partition(".")
    try:
        dt = datetime.strptime(base, "%Y%m%d %H:%M:%S")
    except ValueError:
        return utc_now()
    if fraction:
        try:
            micro = int(float(f"0.{fraction}") * 1_000_000)
        except ValueError:  # pragma: no cover - defensive
            micro = 0
        dt = dt.replace(microsecond=min(max(micro, 0), 999_999))
    return to_utc(dt)
