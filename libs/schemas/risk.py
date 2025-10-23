from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field


class RiskCheckRequest(BaseModel):
    strategy_code: str = Field(..., max_length=32)
    symbol: str = Field(..., max_length=32)
    notional: Decimal = Field(..., gt=0)
    implied_vol: float = Field(..., ge=0)
    timestamp: datetime
    signal_code: str | None = Field(default=None, max_length=64)
    option_right: str | None = Field(default=None, pattern="^(CALL|PUT)$")


class RiskCheckResult(BaseModel):
    strategy_code: str
    symbol: str
    approved: bool
    limit_utilisation: float = Field(..., ge=0, le=1)
    detail: str
    timestamp: datetime
    reasons: Optional[Dict[str, Any]] = None


class PreCheckSummary(BaseModel):
    strategy_code: str
    symbol: str
    approved: bool
    limit_utilisation: float = Field(..., ge=0)
    remaining_capacity: Decimal
    timestamp: datetime


class ForceCloseRequest(BaseModel):
    strategy_code: str = Field(..., max_length=32)
    symbol: str = Field(..., max_length=32)
    option_right: str = Field(..., pattern="^(CALL|PUT)$")


class ForceCloseResult(BaseModel):
    strategy_code: str
    symbol: str
    option_right: str
    was_open: bool
    closed_quantity: int = Field(..., ge=0)
    timestamp: datetime


class AmBottomLimits(BaseModel):
    pivot_w: int = Field(3, ge=1)
    neg_seq_min: int = Field(6, ge=1)
    pos_seq_min: int = Field(3, ge=1)
    allow_mid_filter: bool = True
    top_n: int = Field(3, ge=1)
    cooldown_s: int = Field(600, ge=0)


class AmSell1Limits(BaseModel):
    body_ratio_min: float = Field(0.6, ge=0.0)
    range_mult_min: float = Field(1.5, ge=0.0)
    consecutive: int = Field(2, ge=1)


class AmConfluenceBuyLimits(BaseModel):
    lookback_n: int = Field(5, ge=1)
    obv_slope_w: int = Field(5, ge=1)


class AmConfluenceSellLimits(BaseModel):
    lookback_n: int = Field(5, ge=1)


class AmConfluenceLimits(BaseModel):
    buy: AmConfluenceBuyLimits = Field(default_factory=AmConfluenceBuyLimits)
    sell: AmConfluenceSellLimits = Field(default_factory=AmConfluenceSellLimits)
    cooldown_s: int = Field(600, ge=0)


class OvernightBandwidthLimits(BaseModel):
    lookback: int = Field(120, ge=1)
    ma: int = Field(5, ge=1)
    ratio: float = Field(0.5, ge=0.0)
    hold_min: int = Field(10, ge=0)


class OvernightReboundLimits(BaseModel):
    max_pct: float = Field(0.005, ge=0.0)


class OvernightLimits(BaseModel):
    iv_max: float = Field(1.0, ge=0.0)
    bw: OvernightBandwidthLimits = Field(default_factory=OvernightBandwidthLimits)
    rebound: OvernightReboundLimits = Field(default_factory=OvernightReboundLimits)


class GateVixLimits(BaseModel):
    mode: Literal["enforce", "record", "ignore"] = "enforce"
    thresh: Decimal = Field(Decimal("20"), ge=Decimal("0"))


class GateLimits(BaseModel):
    vix: GateVixLimits = Field(default_factory=GateVixLimits)


class RiskLimits(BaseModel):
    notional_cap: Decimal
    updated_at: datetime
    am_bottom: AmBottomLimits = Field(default_factory=AmBottomLimits)
    am_sell1: AmSell1Limits = Field(default_factory=AmSell1Limits)
    am_conf: AmConfluenceLimits = Field(default_factory=AmConfluenceLimits)
    overnight: OvernightLimits = Field(default_factory=OvernightLimits)
    gate: GateLimits = Field(default_factory=GateLimits)


class ExposureSnapshot(BaseModel):
    strategy_code: str
    symbol: str
    option_right: str
    open_quantity: int
    mark_price: Decimal
    notional: Decimal
    source_signal_code: str | None = None
    opened_at: datetime | None = None
    conid: int | None = None
    strike: Decimal | None = None
    expiry: date | None = None
    delta: Decimal | None = None


class RiskState(BaseModel):
    limits: RiskLimits
    exposures: list[ExposureSnapshot]
    total_notional: Decimal
    timestamp: datetime
    metrics: Dict[str, Any] = Field(default_factory=dict)
