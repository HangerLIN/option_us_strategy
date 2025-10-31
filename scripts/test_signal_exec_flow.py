#!/usr/bin/env python3
"""集成测试：模拟信号→执行→风控链路，验证期权字段与风控阻断逻辑。"""

import asyncio
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, time
from decimal import Decimal
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List, Tuple

# 配置最小运行环境
os.environ.setdefault("DATABASE_URL", "sqlite:///./test.db")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("IB_HOST", "127.0.0.1")
os.environ.setdefault("IB_PORT", "4002")
os.environ.setdefault("IB_CLIENT_ID", "16")
os.environ.setdefault("IB_ACCOUNT", "DU0000001")
os.environ.setdefault("PAPER", "1")
os.environ.setdefault("VIX_REQUIRED", "false")
os.environ.setdefault("RISK_NOTIONAL_CAP", "5000000")
os.environ.setdefault("RISK_SERVICE_URL", "http://localhost:8002")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if "libs.db.dim_trading_calendar" not in sys.modules:
    calendar_stub = ModuleType("libs.db.dim_trading_calendar")

    def _dummy_get_session(_trade_date):
        return SimpleNamespace(open_time=time(9, 30), close_time=time(16, 0))

    calendar_stub.get_trading_session = _dummy_get_session
    sys.modules["libs.db.dim_trading_calendar"] = calendar_stub

from ibapi.contract import Contract

from apps.exec_svc import main as exec_main
from libs.schemas.exec import ExecutionRequest
from libs.schemas.risk import RiskCheckRequest


class FakeOptionSelector:
    """可注入的 OptionSelector，返回预设报价。"""

    def __init__(self, selection) -> None:
        self._selection = selection

    def select_call(self, symbol: str, **_: Any):
        return self._selection


@contextmanager
def patched_execution_env(selection, *, risk_decision: Tuple[bool, str, str]):
    """替换 OptionSelector、风险预检与下单底层，捕获 ExecutionRequest 与风控请求。"""
    captured: List[ExecutionRequest] = []
    risk_payloads: List[Dict[str, Any]] = []

    async def fake_risk_precheck(payload: Dict[str, Any], notional: Decimal):
        risk_payloads.append({"payload": payload, "notional": notional})
        return risk_decision

    async def fake_place_attempt(meta, trace_id: str, retry: bool):
        from libs.core import utc_now

        order_id = len(exec_main.orders_registry) + 1
        state = exec_main.OrderState(
            order_id=order_id,
            status="submitted",
            submitted_at=utc_now(),
            updated_at=utc_now(),
            reason_code=None,
            details=meta.payload,
        )
        exec_main.orders_registry[order_id] = state
        exec_main.trace_registry[trace_id] = order_id
        captured.append(meta.payload)
        return state, order_id

    async def noop_publish(*args, **kwargs):
        return None

    original_selector = exec_main.option_selector
    original_risk = exec_main._risk_precheck
    original_place = exec_main._place_attempt
    original_publish = exec_main.redis_bus.publish
    original_require = exec_main._require_ib_client
    exec_main.option_selector = FakeOptionSelector(selection)
    exec_main._risk_precheck = fake_risk_precheck
    exec_main._place_attempt = fake_place_attempt
    exec_main.redis_bus.publish = noop_publish
    exec_main._require_ib_client = lambda: object()
    try:
        yield captured, risk_payloads
    finally:
        exec_main.option_selector = original_selector
        exec_main._risk_precheck = original_risk
        exec_main._place_attempt = original_place
        exec_main.redis_bus.publish = original_publish
        exec_main._require_ib_client = original_require


def build_selection(*, volume: int, open_interest: int, bid: Decimal, ask: Decimal):
    contract = Contract()
    contract.symbol = "AAPL"
    contract.secType = "OPT"
    contract.currency = "USD"
    contract.exchange = "SMART"
    contract.lastTradeDateOrContractMonth = (datetime.utcnow() + timedelta(days=5)).strftime("%Y%m%d")
    contract.strike = 185.0
    contract.right = "C"
    quote = SimpleNamespace(
        bid=bid,
        ask=ask,
        mid=(bid + ask) / Decimal("2"),
        spread=ask - bid,
        volume=volume,
        open_interest=open_interest,
        delta=Decimal("0.40"),
    )
    return SimpleNamespace(
        contract=contract,
        quote=quote,
        expiry=datetime.utcnow().date() + timedelta(days=5),
        dte=5,
        min_tick=Decimal("0.01"),
        market_rule_id=None,
    )


def build_signal_payload(symbol: str = "AAPL", *, liquidity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "side": "BUY",
        "strategy_code": "core-vol",
        "signal_code": "SIG_OPEN_CHASE_BUY",
        "generated_at": datetime.utcnow().isoformat(),
        "option_hint": {"liquidity": liquidity},
        "risk_hint": {"size": "2R"},
    }


def request_to_risk(req: ExecutionRequest) -> RiskCheckRequest:
    return RiskCheckRequest(
        strategy_code=req.strategy_code,
        symbol=req.symbol,
        notional=req.limit_price * Decimal(req.quantity) * Decimal(100),
        implied_vol=req.implied_vol or 0.0,
        timestamp=datetime.utcnow(),
        signal_code=req.signal_code,
        option_right=req.option_right,
        option_strike=req.option_strike,
        option_expiry=req.option_expiry,
        option_open_interest=req.option_open_interest,
        option_volume=req.option_volume,
        option_bid=req.option_bid,
        option_ask=req.option_ask,
        option_mid=req.option_mid,
        option_spread=req.option_spread,
        option_dte=req.option_dte,
        option_otm_steps=req.option_otm_steps,
    )


def risk_liquidity_result(risk_payload: RiskCheckRequest) -> Tuple[bool, Dict[str, Any]]:
    bid = risk_payload.option_bid
    ask = risk_payload.option_ask
    oi = risk_payload.option_open_interest
    volume = risk_payload.option_volume
    mid = risk_payload.option_mid
    spread = risk_payload.option_spread
    dte = risk_payload.option_dte
    snapshot = {
        "bid": float(bid) if bid is not None else None,
        "ask": float(ask) if ask is not None else None,
        "open_interest": oi,
        "volume": volume,
        "mid": float(mid) if mid is not None else None,
        "spread": float(spread) if spread is not None else None,
        "dte": dte,
    }
    if None in (bid, ask, oi, volume):
        snapshot["reason"] = "missing_fields"
        snapshot["pass"] = True
        return True, snapshot
    if mid is None:
        mid = (bid + ask) / Decimal("2")
        snapshot["mid"] = float(mid)
    if spread is None:
        spread = ask - bid
        snapshot["spread"] = float(spread)
    threshold = max(Decimal("0.10"), mid * Decimal("0.05"))
    snapshot["threshold"] = float(threshold)
    if oi < 500 or volume < 100 or spread > threshold:
        snapshot["pass"] = False
        snapshot["reason"] = "volume_or_spread"
        return False, snapshot
    if dte is not None and not (2 <= dte <= 7):
        snapshot["pass"] = False
        snapshot["reason"] = "dte_out_of_range"
        return False, snapshot
    snapshot["pass"] = True
    snapshot["reason"] = "ok"
    return True, snapshot


async def run_case(name: str, selection, risk_decision: Tuple[bool, str, str]) -> None:
    print(f"\n▶️ 用例: {name}")
    payload = build_signal_payload(liquidity={"pass": True})
    with patched_execution_env(selection, risk_decision=risk_decision) as (requests, risk_payloads):
        await exec_main._handle_signal_payload(payload)
    if requests:
        req = requests[0]
        print("✅ 捕获 ExecutionRequest")
        print(
            f"   strike={req.option_strike} expiry={req.option_expiry} "
            f"bid={req.option_bid} ask={req.option_ask} volume={req.option_volume} oi={req.option_open_interest}"
        )
        risk_request = request_to_risk(req)
        passed, snapshot = risk_liquidity_result(risk_request)
        print(f"   风控快照: pass={passed}, reason={snapshot.get('reason')}")
    elif risk_payloads:
        print("⚠️ 风控预检直接拒绝，未生成 ExecutionRequest")
        print(f"   risk_payload: {risk_payloads[0]}")
    else:
        print("❌ 未捕获到任何请求，请检查测试设定")


def main() -> None:
    print("🚀 集成测试：信号→执行→风控")
    good_selection = build_selection(volume=600, open_interest=1200, bid=Decimal("2.45"), ask=Decimal("2.55"))
    bad_selection = build_selection(volume=600, open_interest=1200, bid=Decimal("2.45"), ask=Decimal("2.55"))

    asyncio.run(run_case("流动性通过", good_selection, (True, "OK", "mock-pass")))
    asyncio.run(run_case("流动性不足", bad_selection, (False, "BLOCK:OPTION_LIQUIDITY", "mock-block")))


if __name__ == "__main__":
    main()
