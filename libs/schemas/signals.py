from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from libs.schemas.assets import AssetType


class SignalSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


BUY_SIGNAL_CODES = frozenset(
    {
        "SIG_OPEN_CHASE_BUY",
        "SIG_OPEN_CHASE_ORB_BUY",
        "SIG_1030_REVERSAL_CALL_BUY",
        "SIG_1030_V8A_LOW_REVERSAL_CALL_BUY",
        "SIG_1030_V8B_RECLAIM_CALL_BUY",
        "SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
        "SIG_REBOUND_BUY",
        "SIG_PM_BOTTOM_A2",
        "SIG_PM_BOTTOM_A3",
        "SIG_PM_BOTTOM_A4",
        "SIG_AM_BOTTOM_A1",
        "SIG_AM_CONFLUENCE_BUY_A2",
    }
)

SELL_SIGNAL_CODES = frozenset(
    {
        "SIG_EXIT_UPPER_TAP_X2",
        "SIG_EXIT_BOX2MID",
        "OPEN_CHASE_FAILURE_BELOW_VWAP",
        "OPEN_CHASE_FAILURE_BELOW_ORL",
        "OPEN_CHASE_FAILURE_BELOW_ENTRY_LOW",
        "OPEN_CHASE_FAILURE_NO_FOLLOW_THROUGH",
        "OPEN_CHASE_FAILURE_OPTION_LOSS",
        "OPEN_CHASE_TAKE_PROFIT_25",
        "OPEN_CHASE_TAKE_PROFIT_50",
        "OPEN_CHASE_UPPER_TAP_PROFIT_TAKE",
        "OPEN_CHASE_TIME_STOP_NO_PROFIT",
        "OPEN_CHASE_TIME_STOP_NO_TREND_CONFIRM",
        "OPEN_CHASE_EOD_EXIT",
        "SIG_1030_REVERSAL_BOLL_UP_EXIT",
        "SIG_1030_REVERSAL_BOLL_UP_TRUE_EXIT",
        "SIG_1030_REVERSAL_BOLL_UP_WEAK_EXIT",
        "SIG_1030_REVERSAL_BOLL_UP_REBOUND_FAILURE_EXIT",
        "SIG_1030_REVERSAL_CONTINUATION_TARGET_EXIT",
        "SIG_1030_REVERSAL_CONTINUATION_STRUCTURE_LOST_EXIT",
        "SIG_1030_REVERSAL_FAILURE_ENTRY_LOW_EXIT",
        "SIG_1030_REVERSAL_EARLY_NO_FOLLOW_THROUGH_EXIT",
        "SIG_1030_REVERSAL_FAILURE_RECLAIM_LOST_EXIT",
        "SIG_1030_REVERSAL_TIME_EXIT",
        "SIG_TIME_CLEAR_12_14",
        "SIG_OVERNIGHT_GAP_EXIT",
        "SIG_AM_SELL_C1",
        "SIG_AM_CONFLUENCE_SELL_S2",
    }
)

ENTRY_SIGNAL_CODES = BUY_SIGNAL_CODES
EXIT_SIGNAL_CODES = SELL_SIGNAL_CODES
ALL_SIGNAL_CODES = BUY_SIGNAL_CODES | SELL_SIGNAL_CODES


def is_buy_signal(signal_code: str | None) -> bool:
    return bool(signal_code and signal_code in BUY_SIGNAL_CODES)


def is_sell_signal(signal_code: str | None) -> bool:
    return bool(signal_code and signal_code in SELL_SIGNAL_CODES)


def signal_side_for_code(signal_code: str | None) -> SignalSide:
    if is_sell_signal(signal_code):
        return SignalSide.SELL
    return SignalSide.BUY


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
    asset_type: AssetType = AssetType.OPTION
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
