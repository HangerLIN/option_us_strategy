from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class AssetType(str, Enum):
    EQUITY = "EQUITY"
    ETF = "ETF"
    OPTION = "OPTION"


class InstrumentRef(BaseModel):
    asset_type: AssetType = AssetType.EQUITY
    symbol: str = Field(..., max_length=32)
    currency: str = Field("USD", max_length=8)
    venue: str | None = Field(default=None, max_length=32)
    option_right: str | None = Field(default=None, pattern="^(CALL|PUT)$")
    strike: Decimal | None = Field(default=None, gt=0)
    expiry: date | datetime | None = None
    conid: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_option_fields(self) -> "InstrumentRef":
        if self.asset_type == AssetType.OPTION:
            missing = [
                name
                for name, value in {
                    "option_right": self.option_right,
                    "strike": self.strike,
                    "expiry": self.expiry,
                }.items()
                if value is None
            ]
            if missing:
                raise ValueError(f"OPTION instrument missing fields: {', '.join(missing)}")
        return self


class MarketQuote(BaseModel):
    instrument: InstrumentRef
    bid: Decimal | None = Field(default=None, ge=0)
    ask: Decimal | None = Field(default=None, ge=0)
    mid: Decimal | None = Field(default=None, ge=0)
    last: Decimal | None = Field(default=None, ge=0)
    mark: Decimal | None = Field(default=None, ge=0)
    volume: int | None = Field(default=None, ge=0)
    open_interest: int | None = Field(default=None, ge=0)
    quote_ts: datetime
    source: str = Field("ibkr", max_length=32)

    @model_validator(mode="after")
    def derive_mid(self) -> "MarketQuote":
        if self.mid is None and self.bid is not None and self.ask is not None and self.ask >= self.bid:
            self.mid = (self.bid + self.ask) / Decimal("2")
        return self


class BarEvent(BaseModel):
    instrument: InstrumentRef
    bar_start: datetime
    bar_end: datetime
    timeframe: str = Field("1m", max_length=8)
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = Field(..., ge=0)
    vwap: Decimal | None = None
    source: str = Field("md_gw", max_length=32)


class PositionSnapshot(BaseModel):
    strategy_code: str = Field(..., max_length=64)
    instrument: InstrumentRef
    quantity: Decimal
    avg_open_price: Decimal
    mark_price: Decimal
    notional: Decimal
    source_signal_code: str | None = Field(default=None, max_length=64)
    opened_at: datetime | None = None
    updated_at: datetime | None = None


__all__ = [
    "AssetType",
    "InstrumentRef",
    "MarketQuote",
    "BarEvent",
    "PositionSnapshot",
]
