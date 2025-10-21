from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable

import httpx
from sqlalchemy.orm import sessionmaker

from apps.risk_svc.service import RiskDecision as ServiceRiskDecision, RiskService, ExposureView
from libs.core import get_settings
from libs.schemas.risk import RiskCheckRequest
from libs.db import StrategyPositionDAO


@dataclass
class ExposureLike:
    symbol: str
    open_quantity: Decimal


@dataclass
class OrderIntent:
    strategy_code: str
    symbol: str
    signal_code: str
    side: str
    quantity: Decimal
    limit_price: Decimal
    option_right: str
    timestamp: datetime
    trace_id: str
    exposures: Iterable[ExposureLike]
    current_notional: Decimal
    pending_notional: Decimal
    utilisation: float
    implied_vol: float | None = None


@dataclass
class RiskResult:
    approved: bool
    reason_code: str
    detail: str | None = None
    utilisation: float | None = None


@dataclass
class RiskCtx:
    mode: str
    session_factory: sessionmaker
    base_url: str | None = None

    def __post_init__(self) -> None:
        self.mode = self.mode.lower()
        if self.mode not in {"inproc", "http"}:
            raise ValueError("risk mode must be 'inproc' or 'http'")
        self._risk_service: RiskService | None = None

    def get_service(self) -> RiskService:
        if self._risk_service is None:
            settings = get_settings()
            self._risk_service = RiskService(self.session_factory, redis_url=settings.redis_url)
        return self._risk_service


def build_risk_precheck(ctx: RiskCtx):
    if ctx.mode == "inproc":
        return lambda intent: _risk_precheck_inproc(ctx, intent)
    return lambda intent: _risk_precheck_http(ctx, intent)


def _risk_precheck_inproc(ctx: RiskCtx, intent: OrderIntent) -> RiskResult:
    service = ctx.get_service()
    with ctx.session_factory() as session:
        position_dao = StrategyPositionDAO(session)
        exposures: list[ExposureView] = list(
            position_dao.list_positions(strategy_code=intent.strategy_code)
        )

    request = RiskCheckRequest(
        strategy_code=intent.strategy_code,
        symbol=intent.symbol,
        notional=intent.pending_notional,
        implied_vol=float(intent.implied_vol or 0.0),
        timestamp=intent.timestamp,
        signal_code=intent.signal_code,
        option_right=intent.option_right,
    )

    decision: ServiceRiskDecision = service.evaluate_order(
        request,
        exposures=exposures,
        current_notional=intent.current_notional,
        pending_notional=intent.pending_notional,
        utilisation=intent.utilisation,
        record_state=False,
        record_events=True,
    )

    return RiskResult(
        approved=decision.approved,
        reason_code=decision.code,
        detail=decision.detail,
        utilisation=decision.utilisation,
    )


def _risk_precheck_http(ctx: RiskCtx, intent: OrderIntent) -> RiskResult:
    if not ctx.base_url:
        raise RuntimeError("risk-service base URL required for http mode")
    payload = {
        "strategy_code": intent.strategy_code,
        "symbol": intent.symbol,
        "notional": str(intent.pending_notional),
        "implied_vol": intent.implied_vol or 0.0,
        "timestamp": intent.timestamp.isoformat(),
        "signal_code": intent.signal_code,
        "option_right": intent.option_right,
    }
    url = ctx.base_url.rstrip("/") + "/precheck"
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            ok = bool(data.get("ok"))
            code = str(data.get("code") or ("OK" if ok else "BLOCK:RISK"))
            return RiskResult(approved=ok, reason_code=code, detail=str(data.get("message")))
        except Exception as exc:  # pragma: no cover - network failure path
            last_exc = exc
            continue
    raise RuntimeError(f"Risk precheck HTTP failed: {last_exc}")
