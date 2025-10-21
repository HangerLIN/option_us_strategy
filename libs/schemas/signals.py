from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class SignalSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class SignalInstruction(BaseModel):
    strategy_code: str = Field(..., max_length=32)
    symbol: str = Field(..., max_length=32)
    side: SignalSide
    confidence: float = Field(..., ge=0.0, le=1.0)
    reason: str = Field(..., max_length=256)
    engine: Literal["stat-arb", "vol-arb", "intraday"] = "stat-arb"


class SignalEnvelope(BaseModel):
    strategy_code: str = Field(..., max_length=32)
    symbol: str = Field(..., max_length=32)
    signal_code: str = Field(..., max_length=64)
    side: SignalSide
    confidence: float = Field(..., ge=0.0, le=1.0)
    reason: Dict[str, Any] = Field(default_factory=dict)
    risk_hint: Dict[str, Any] = Field(default_factory=dict)
    option_hint: Dict[str, Any] = Field(default_factory=dict)
    ttl_seconds: int = Field(180, ge=1)
    cooldown_seconds: int = Field(600, ge=1)
    generated_at: datetime


class SignalPreviewRequest(BaseModel):
    symbol: str = Field(..., max_length=32)
    start: datetime
    end: datetime


class SignalPreviewEntry(BaseModel):
    trace_id: str
    ts_end: datetime
    signal: SignalEnvelope


class SignalPreviewResponse(BaseModel):
    symbol: str
    start: datetime
    end: datetime
    count: int
    signals: List[SignalPreviewEntry]


class SignalPushItem(BaseModel):
    signal: SignalEnvelope
    trace_id: Optional[str] = Field(default=None, max_length=64)
    force: bool = False


class SignalPushRequest(BaseModel):
    signals: List[SignalPushItem]


class SignalPushResult(BaseModel):
    trace_id: str
    accepted: bool
    reason: Optional[str] = None
    signal: SignalEnvelope


class SignalPushResponse(BaseModel):
    count: int
    accepted: int
    results: List[SignalPushResult]
