from __future__ import annotations

from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd

from apps.signal_svc.engine import SignalEngine, PositionState
from libs.schemas.risk import (
    AmBottomLimits,
    AmConfluenceBuyLimits,
    AmConfluenceLimits,
    AmConfluenceSellLimits,
    AmSell1Limits,
    GateLimits,
    GateVixLimits,
    OvernightBandwidthLimits,
    OvernightLimits,
    OvernightReboundLimits,
    RiskLimits,
)


class _DummyResult:
    def __init__(self, value: Decimal | None):
        self._value = value

    def fetchone(self):
        if self._value is None:
            return None
        return (self._value,)


class _DummySession:
    def __init__(self, values: dict[date, Decimal]):
        self._values = values

    def execute(self, *args, **kwargs):
        params = {}
        if args:
            maybe_params = args[-1]
            if isinstance(maybe_params, dict):
                params.update(maybe_params)
        params.update(kwargs)
        trade_date = params.get("trade_date")
        value = self._values.get(trade_date)
        return _DummyResult(value)


def _risk_limits() -> RiskLimits:
    now = datetime.now(timezone.utc)
    return RiskLimits(
        notional_cap=Decimal("1000000"),
        updated_at=now,
        am_bottom=AmBottomLimits(),
        am_sell1=AmSell1Limits(),
        am_conf=AmConfluenceLimits(
            buy=AmConfluenceBuyLimits(),
            sell=AmConfluenceSellLimits(),
            cooldown_s=600,
        ),
        overnight=OvernightLimits(
            iv_max=1.0,
            bw=OvernightBandwidthLimits(lookback=120, ma=5, ratio=0.5, hold_min=10),
            rebound=OvernightReboundLimits(max_pct=0.005),
        ),
        gate=GateLimits(vix=GateVixLimits(mode="enforce", thresh=Decimal("20"))),
    )


def _build_engine() -> SignalEngine:
    engine = SignalEngine.__new__(SignalEngine)
    engine._strategy_code = "core-vol"
    engine._ttl = 180
    engine._cooldown = 600
    engine._mfi_stoch_filter = False
    engine._positions = {}
    engine._upper_break = {}
    engine._last_signal_ts = {}
    engine._redis_bus = None
    engine._history_window = 180
    engine._top5_today = lambda *args, **kwargs: True
    engine._can_open_position = lambda *args, **kwargs: (True, False)
    limits = _risk_limits()
    engine._am_bottom_limits = limits.am_bottom
    engine._am_sell1_limits = limits.am_sell1
    engine._am_conf_limits = limits.am_conf
    engine._settings = SimpleNamespace(ttl_buy_seconds=180, cooldown_buy_seconds=600)
    return engine


def _make_df(prev_open: Decimal, today_open: Decimal, *, curr_time: str = "2025-09-22 09:31:00") -> pd.DataFrame:
    ts_prev = pd.Timestamp("2025-09-19 09:31:00", tz="US/Eastern").tz_convert("UTC")
    ts_curr = pd.Timestamp(curr_time, tz="US/Eastern").tz_convert("UTC")
    data = pd.DataFrame(
        {
            "ts_end": [ts_prev, ts_curr],
            "open": [float(prev_open), float(today_open)],
            "high": [float(prev_open), float(today_open)],
            "low": [float(prev_open), float(today_open)],
            "close": [float(prev_open), float(today_open)],
        }
    )
    data = data.sort_values("ts_end").reset_index(drop=True)
    data["et"] = data["ts_end"].dt.tz_convert("US/Eastern")
    return data


def test_overnight_gap_exit_triggers_on_flat_open():
    engine = _build_engine()
    opened_at = datetime(2025, 9, 19, 14, 21, tzinfo=timezone.utc)
    engine._positions["CRCL"] = PositionState(signal_code="SIG_PM_BOTTOM_A3", opened_at=opened_at)
    prev_trade_date = opened_at.date()
    trade_date = date(2025, 9, 22)
    session = _DummySession(
        {
            prev_trade_date: Decimal("100"),
            trade_date: Decimal("100.4"),
        }
    )
    df = _make_df(Decimal("100"), Decimal("100.4"), curr_time="2025-09-22 09:30:00")
    event = SimpleNamespace(symbol="CRCL", bar_end=df.iloc[-1]["ts_end"].to_pydatetime())
    trade_date = df.iloc[-1]["et"].date()

    signal = engine._detect_overnight_gap_exit(event, df, trade_date, None, session)
    assert signal is not None
    assert signal.signal_code == "SIG_OVERNIGHT_GAP_EXIT"
    assert Decimal(signal.reason["gap_pct"]) <= Decimal("0.005")


def test_overnight_gap_exit_skips_when_gap_positive():
    engine = _build_engine()
    opened_at = datetime(2025, 9, 19, 14, 21, tzinfo=timezone.utc)
    engine._positions["CRCL"] = PositionState(signal_code="SIG_PM_BOTTOM_A3", opened_at=opened_at)
    prev_trade_date = opened_at.date()
    trade_date = date(2025, 9, 22)
    session = _DummySession(
        {
            prev_trade_date: Decimal("100"),
            trade_date: Decimal("101.0"),
        }
    )
    df = _make_df(Decimal("100"), Decimal("101.0"), curr_time="2025-09-22 09:30:00")
    event = SimpleNamespace(symbol="CRCL", bar_end=df.iloc[-1]["ts_end"].to_pydatetime())
    trade_date = df.iloc[-1]["et"].date()

    signal = engine._detect_overnight_gap_exit(event, df, trade_date, None, session)
    assert signal is None
