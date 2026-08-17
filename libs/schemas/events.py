from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from libs.schemas.assets import AssetType


class MarketDataEvent(BaseModel):
    """Top-of-book update received from the market data gateway."""

    symbol: str = Field(..., max_length=32)
    asset_type: AssetType = AssetType.EQUITY
    bid: Decimal = Field(..., gt=0)
    ask: Decimal = Field(..., gt=0)
    timestamp: datetime


class BarsClosed(BaseModel):
    """Normalized 1m bar event used for downstream strategies."""

    trace_id: str
    symbol: str = Field(..., max_length=32)
    asset_type: AssetType = AssetType.EQUITY
    bar_start: datetime
    bar_end: datetime
    timeframe: str = Field("1m", max_length=8)
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    vwap: Optional[Decimal] = None
    source: str = Field("md_gw")
    received_at: datetime


class Signal(BaseModel):
    trace_id: str
    strategy_code: str = Field(..., max_length=64)
    signal_code: str = Field(..., max_length=64)
    symbol: str = Field(..., max_length=32)
    asset_type: AssetType = AssetType.OPTION
    side: str = Field(..., pattern="^(BUY|SELL|FLAT)$")
    confidence: Decimal = Field(..., ge=0, le=1)
    reason: str = Field(..., max_length=256)
    generated_at: datetime


class ExecutionFill(BaseModel):
    trace_id: str
    order_id: int
    client_order_id: str
    symbol: str = Field(..., max_length=32)
    asset_type: AssetType = AssetType.OPTION
    side: str = Field(..., pattern="^(BUY|SELL)$")
    fill_quantity: Decimal
    fill_price: Decimal
    strategy_code: str | None = Field(default=None, max_length=64)
    signal_code: str | None = Field(default=None, max_length=64)
    option_right: str | None = Field(default=None, pattern="^(CALL|PUT)$")
    conid: int | None = Field(default=None)
    strike: Decimal | None = Field(default=None)
    expiry: datetime | None = Field(default=None)
    delta: float | None = Field(default=None)
    fees: Decimal | None = Field(default=None)
    execution_id: str
    venue: Optional[str] = Field(default=None, max_length=32)
    filled_at: datetime


class RiskBlock(BaseModel):
    trace_id: str
    strategy_code: str = Field(..., max_length=64)
    symbol: str = Field(..., max_length=32)
    metric_code: str = Field(..., max_length=64)
    metric_value: Decimal
    limit_code: str = Field(..., max_length=64)
    reason: str = Field(..., max_length=256)
    triggered_at: datetime


class RiskUnblock(BaseModel):
    trace_id: str
    strategy_code: str = Field(..., max_length=64)
    symbol: str = Field(..., max_length=32)
    reason: str = Field(..., max_length=256)
    triggered_at: datetime


class RiskAlert(BaseModel):
    trace_id: str
    alert_code: str = Field(..., max_length=64)
    severity: str = Field(..., pattern="^(INFO|WARN|ERROR)$")
    message: str = Field(..., max_length=512)
    symbol: Optional[str] = Field(default=None, max_length=32)
    strategy_code: Optional[str] = Field(default=None, max_length=64)
    created_at: datetime


class RiskParamReload(BaseModel):
    trace_id: str
    triggered_by: str = Field(..., max_length=64)
    reload_at: datetime


class ForceCloseEvent(BaseModel):
    trace_id: str
    strategy_code: str = Field(..., max_length=64)
    symbol: str = Field(..., max_length=32)
    asset_type: AssetType = AssetType.OPTION
    option_right: Optional[str] = Field(default=None, pattern="^(CALL|PUT)$")
    reason: str = Field(..., max_length=256)
    triggered_at: datetime
    detail: Dict[str, Any] = Field(default_factory=dict)
