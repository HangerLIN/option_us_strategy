from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field

from libs.schemas.assets import AssetType


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class ExecutionMode(str, Enum):
    ADAPTIVE = "ADAPTIVE"
    PASSIVE = "PASSIVE"
    MARKETABLE = "MARKETABLE"
    FORCE = "FORCE"


class ExecutionRequest(BaseModel):
    strategy_code: str = Field(..., max_length=32)
    symbol: str = Field(..., max_length=32)
    asset_type: AssetType = AssetType.OPTION
    side: OrderSide
    quantity: int = Field(..., gt=0)
    limit_price: Decimal = Field(..., gt=0)
    tif: str = Field(..., max_length=8, description="Time in force (e.g. IOC, DAY).")
    signal_code: str | None = Field(default=None, max_length=64)
    option_right: str | None = Field(default=None, pattern="^(CALL|PUT)$")
    option_dte: int | None = Field(default=None, ge=0)
    option_otm_steps: int | None = Field(default=None, ge=0)
    option_strike: Decimal | None = Field(default=None, gt=0, description="Option strike price")
    option_expiry: str | None = Field(default=None, max_length=8, description="Option expiry date YYYYMMDD")
    option_open_interest: int | None = Field(default=None, ge=0)
    option_volume: int | None = Field(default=None, ge=0)
    option_bid: Decimal | None = Field(default=None, ge=0)
    option_ask: Decimal | None = Field(default=None, ge=0)
    option_mid: Decimal | None = Field(default=None, ge=0)
    option_spread: Decimal | None = Field(default=None, ge=0)
    implied_vol: float | None = Field(default=None, ge=0)
    a1_gate: bool | None = None
    is_exit: bool | None = None
    min_tick: Decimal | None = Field(default=None, gt=0)
    execution_mode: ExecutionMode = ExecutionMode.MARKETABLE
    trace_id: str | None = Field(default=None, max_length=64)


class OrderCancelRequest(BaseModel):
    order_id: int = Field(..., ge=1)


class OrderState(BaseModel):
    order_id: int
    status: str
    submitted_at: datetime
    updated_at: datetime
    reason_code: str | None = None
    details: ExecutionRequest
