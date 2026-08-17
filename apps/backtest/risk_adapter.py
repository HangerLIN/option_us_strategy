from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable
from collections import defaultdict
import fnmatch

import httpx
from sqlalchemy.orm import sessionmaker

from apps.risk_svc.service import RiskDecision as ServiceRiskDecision, RiskService, ExposureView
from apps.risk_svc.block_store import RedisBlockStore
from libs.core import get_settings
from libs.db import StrategyPositionDAO
from libs.schemas.assets import AssetType
from libs.schemas.risk import RiskCheckRequest
from libs.schemas.signals import BUY_SIGNAL_CODES


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
    option_right: str | None
    timestamp: datetime
    trace_id: str
    exposures: Iterable[ExposureLike]
    current_notional: Decimal
    pending_notional: Decimal
    utilisation: float
    asset_type: AssetType = AssetType.OPTION
    implied_vol: float | None = None
    option_strike: Decimal | None = None
    option_expiry: str | datetime | None = None
    option_open_interest: int | None = None
    option_volume: int | None = None
    option_bid: Decimal | None = None
    option_ask: Decimal | None = None
    option_mid: Decimal | None = None
    option_spread: Decimal | None = None
    option_dte: int | None = None
    option_otm_steps: int | None = None
    option_quote_ts: datetime | None = None
    allow_missing_option_liquidity_metrics: bool = False


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
    allow_missing_option_liquidity_metrics: bool = False

    def __post_init__(self) -> None:
        self.mode = self.mode.lower()
        if self.mode not in {"inproc", "http"}:
            raise ValueError("risk mode must be 'inproc' or 'http'")
        self._risk_service: RiskService | None = None

    def get_service(self) -> RiskService:
        if self._risk_service is None:
            settings = get_settings()
            RiskService._BUY_SIGNALS = BUY_SIGNAL_CODES
            self._risk_service = RiskService(self.session_factory, redis_url=settings.redis_url)
            self._risk_service._BUY_SIGNALS = BUY_SIGNAL_CODES
            if self.mode == "inproc":
                fake_redis = _EphemeralRedis()
                self._risk_service._redis = None
                self._risk_service._blocks = RedisBlockStore(None, client=fake_redis)
        return self._risk_service


class _EphemeralRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = defaultdict(dict)
        self.ttl: dict[str, int] = {}

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value
        if ex is not None:
            self.ttl[key] = ex

    def setex(self, key: str, ttl: int, value: str) -> None:
        self.set(key, value, ex=ttl)

    def delete(self, key: str) -> None:
        self.store.pop(key, None)
        self.zsets.pop(key, None)

    def expire(self, key: str, ttl: int) -> None:
        self.ttl[key] = ttl

    def scan(
        self, cursor: int = 0, match: str | None = None, count: int | None = None
    ) -> tuple[int, list[str]]:
        keys = list(self.store.keys())
        if match is not None:
            keys = [key for key in keys if fnmatch.fnmatch(key, match)]
        return 0, keys


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
        asset_type=intent.asset_type,
        notional=intent.pending_notional,
        implied_vol=float(intent.implied_vol or 0.0),
        timestamp=intent.timestamp,
        signal_code=intent.signal_code,
        option_right=intent.option_right,
        option_strike=intent.option_strike,
        option_expiry=(
            intent.option_expiry.strftime("%Y%m%d")
            if isinstance(intent.option_expiry, datetime)
            else intent.option_expiry
        ),
        option_open_interest=intent.option_open_interest,
        option_volume=intent.option_volume,
        option_bid=intent.option_bid,
        option_ask=intent.option_ask,
        option_mid=intent.option_mid,
        option_spread=intent.option_spread,
        option_dte=intent.option_dte,
        option_otm_steps=intent.option_otm_steps,
        option_quote_ts=intent.option_quote_ts,
        allow_missing_option_liquidity_metrics=intent.allow_missing_option_liquidity_metrics,
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
        "asset_type": intent.asset_type.value,
        "notional": str(intent.pending_notional),
        "implied_vol": intent.implied_vol or 0.0,
        "timestamp": intent.timestamp.isoformat(),
        "signal_code": intent.signal_code,
        "option_right": intent.option_right,
    }
    extra_fields = {
        "option_strike": intent.option_strike,
        "option_expiry": (
            intent.option_expiry.strftime("%Y%m%d")
            if isinstance(intent.option_expiry, datetime)
            else intent.option_expiry
        ),
        "option_open_interest": intent.option_open_interest,
        "option_volume": intent.option_volume,
        "option_bid": intent.option_bid,
        "option_ask": intent.option_ask,
        "option_mid": intent.option_mid,
        "option_spread": intent.option_spread,
        "option_dte": intent.option_dte,
        "option_otm_steps": intent.option_otm_steps,
        "option_quote_ts": intent.option_quote_ts.isoformat() if intent.option_quote_ts else None,
        "allow_missing_option_liquidity_metrics": intent.allow_missing_option_liquidity_metrics,
    }
    for key, value in extra_fields.items():
        if value is None:
            continue
        payload[key] = str(value) if isinstance(value, Decimal) else value

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
