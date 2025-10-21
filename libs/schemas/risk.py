from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional

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


class RiskLimits(BaseModel):
    notional_cap: Decimal
    updated_at: datetime


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
