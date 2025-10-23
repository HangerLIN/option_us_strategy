from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from apps.signal_svc.engine import SignalEngine, PositionState
from libs.schemas.signals import SignalSide
from libs.schemas.risk import (
    RiskLimits,
    AmBottomLimits,
    AmSell1Limits,
    AmConfluenceLimits,
    AmConfluenceBuyLimits,
    AmConfluenceSellLimits,
    OvernightLimits,
    OvernightBandwidthLimits,
    OvernightReboundLimits,
    GateLimits,
    GateVixLimits,
)


def _build_risk_limits() -> RiskLimits:
    updated = datetime.now(timezone.utc)
    return RiskLimits(
        notional_cap=Decimal("1000000"),
        updated_at=updated,
        am_bottom=AmBottomLimits(
            pivot_w=2,
            neg_seq_min=3,
            pos_seq_min=2,
            allow_mid_filter=True,
            top_n=3,
            cooldown_s=600,
        ),
        am_sell1=AmSell1Limits(
            body_ratio_min=0.6,
            range_mult_min=1.5,
            consecutive=2,
        ),
        am_conf=AmConfluenceLimits(
            buy=AmConfluenceBuyLimits(lookback_n=5, obv_slope_w=5),
            sell=AmConfluenceSellLimits(lookback_n=5),
            cooldown_s=600,
        ),
        overnight=OvernightLimits(
            iv_max=1.0,
            bw=OvernightBandwidthLimits(lookback=120, ma=5, ratio=0.5, hold_min=10),
            rebound=OvernightReboundLimits(max_pct=0.005),
        ),
        gate=GateLimits(vix=GateVixLimits(mode="enforce", thresh=Decimal("20"))),
    )


@pytest.fixture
def base_engine(monkeypatch):
    engine = SignalEngine.__new__(SignalEngine)
    engine._strategy_code = "core-vol"
    engine._ttl = 180
    engine._cooldown = 600
    engine._mfi_stoch_filter = False
    engine._top5_today = lambda trade_date, session, symbol: True
    engine._can_open_position = lambda *args, **kwargs: (True, False)
    engine._top5_recent = lambda *args, **kwargs: True
    engine._is_high_open = lambda *args, **kwargs: False
    engine._positions = {}
    engine._upper_break = {}
    engine._last_signal_ts = {}
    engine._redis_bus = None
    engine._history_window = 30
    limits = _build_risk_limits()
    engine._am_bottom_limits = limits.am_bottom
    engine._am_sell1_limits = limits.am_sell1
    engine._am_conf_limits = limits.am_conf
    engine._settings = SimpleNamespace(ttl_buy_seconds=180, cooldown_buy_seconds=600)
    return engine


def _make_dataframe(values: list[dict], tz: str = "UTC") -> pd.DataFrame:
    frame = pd.DataFrame(values)
    frame["ts_end"] = pd.to_datetime(frame["ts_end"]).tz_convert(tz)
    frame = frame.set_index("ts_end").sort_index()
    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "boll_mid",
        "boll_up",
        "boll_dn",
        "lr_m5_slope",
        "lr_boll_dn_slope",
        "lr_obv_slope",
        "stoch_rsi_k",
        "stoch_rsi_d",
        "rsi6",
        "obv",
        "obv_ma6",
    ]
    for col in numeric_cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame.reset_index()


def test_am_bottom_a1_triggers(base_engine):
    ts_start = datetime(2025, 1, 2, 14, 0, tzinfo=timezone.utc)
    rows = []
    lows = [100, 99.6, 99.2, 99.4, 99.1, 98.9, 98.5, 98.3, 98.1, 98.6]
    for idx in range(len(lows)):
        ts = ts_start + timedelta(minutes=idx)
        rows.append(
            {
                "ts_end": ts,
                "open": 100 + idx * 0.1,
                "high": 100.4 + idx * 0.1,
                "low": lows[idx],
                "close": lows[idx] + 0.3,
                "volume": 1000 + idx,
                "boll_mid": lows[idx] + 0.5,
                "boll_up": lows[idx] + 1.0,
                "boll_dn": lows[idx] - 0.5,
                "lr_m5_slope": -0.4 if idx < len(lows) - 2 else 0.6,
                "lr_boll_dn_slope": -0.3 if idx < len(lows) - 2 else 0.7,
                "lr_obv_slope": 0.5,
                "stoch_rsi_k": 25 + idx,
                "stoch_rsi_d": 25 + idx,
                "rsi6": 35 + idx,
                "obv": 100 + idx,
                "obv_ma6": 100,
            }
        )
    df = _make_dataframe(rows)
    base_engine._am_bottom_limits.allow_mid_filter = True
    event = SimpleNamespace(symbol="AAPL", bar_end=rows[-1]["ts_end"])
    signal = base_engine._detect_am_bottom_a1(event, df, event.bar_end.astimezone(timezone.utc).date(), None, None)
    assert signal is not None
    assert signal.signal_code == "SIG_AM_BOTTOM_A1"


def test_am_confluence_buy_requires_obv_slope_positive(base_engine):
    ts_start = datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc)
    rows = []
    for idx in range(6):
        ts = ts_start + timedelta(minutes=idx)
        rows.append(
            {
                "ts_end": ts,
                "open": 100 + 0.2 * idx,
                "high": 100.5 + 0.2 * idx,
                "low": 99.5 + 0.2 * idx,
                "close": 100.2 + 0.2 * idx,
                "volume": 1000 + idx,
                "boll_mid": 100 + 0.2 * idx,
                "boll_up": 100.7 + 0.2 * idx,
                "boll_dn": 99.5 + 0.2 * idx,
                "lr_obv_slope": 0.1 if idx < 5 else -0.2,
                "obv": 200 + idx,
                "obv_ma6": 180,
                "stoch_rsi_k": 15 + idx * 3,
                "stoch_rsi_d": 15 + idx * 3,
                "rsi6": 25 + idx * 2,
            }
        )
    df = _make_dataframe(rows)
    event = SimpleNamespace(symbol="AAPL", bar_end=rows[-1]["ts_end"])
    signal = base_engine._detect_am_confluence_buy_a2(
        event,
        df,
        event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )
    assert signal is None


def test_am_sell_c1_detection(base_engine):
    base_engine._positions = {
        "AAPL": PositionState(signal_code="SIG_OPEN_CHASE_BUY", opened_at=datetime.now(timezone.utc))
    }
    ts_start = datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc)
    rows = []
    for idx in range(4):
        ts = ts_start + timedelta(minutes=idx)
        open_px = 100 - idx * 0.5
        close_px = open_px - 0.5
        rows.append(
            {
                "ts_end": ts,
                "open": open_px,
                "high": open_px + 0.2,
                "low": open_px - 1.2,
                "close": close_px,
                "volume": 1000,
                "boll_mid": close_px + 1.5,
            }
        )
    df = _make_dataframe(rows)
    event = SimpleNamespace(symbol="AAPL", bar_end=rows[-1]["ts_end"])
    signal = base_engine._detect_am_sell_c1(
        event,
        df,
        event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )
    assert signal is not None
    assert signal.signal_code == "SIG_AM_SELL_C1"


def test_should_emit_cooldown(base_engine):
    ts = datetime(2025, 1, 2, 14, 0, tzinfo=timezone.utc)
    signal = base_engine._build_signal(
        symbol="AAPL",
        signal_code="SIG_AM_BOTTOM_A1",
        side=SignalSide.BUY,
        reason={"test": True},
        generated_at=ts,
    )
    assert base_engine._should_emit(signal, ts)
    base_engine._register_state(signal, ts)
    assert not base_engine._should_emit(signal, ts + timedelta(seconds=599))
    assert base_engine._should_emit(signal, ts + timedelta(seconds=600))


def test_evaluate_event_prioritises_sells(monkeypatch, base_engine):
    records = []

    def fake_history(session, symbol, ts_end, limit):
        ts = datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc)
        rows = []
        for idx in range(3):
            rows.append(
                {
                    "ts_end": ts + timedelta(minutes=idx),
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 1000,
                    "boll_mid": 100,
                    "boll_up": 101,
                    "boll_dn": 99,
                    "rsi6": 40,
                    "stoch_rsi_k": 50,
                    "stoch_rsi_d": 50,
                }
            )
        return _make_dataframe(rows)

    monkeypatch.setattr(base_engine, "_load_history", fake_history)

    def fake_sell(*args, **kwargs):
        return base_engine._build_signal(
            symbol="AAPL",
            signal_code="SIG_AM_SELL_C1",
            side=SignalSide.SELL,
            reason={},
            generated_at=kwargs["event"].bar_end,
        )

    def fake_buy(*args, **kwargs):
        return base_engine._build_signal(
            symbol="AAPL",
            signal_code="SIG_AM_BOTTOM_A1",
            side=SignalSide.BUY,
            reason={},
            generated_at=kwargs["event"].bar_end,
        )

    monkeypatch.setattr(base_engine, "_detect_am_sell_c1", fake_sell.__get__(base_engine))
    monkeypatch.setattr(base_engine, "_detect_am_confluence_sell_s2", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_s2", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_s1", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_time_clear", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_e1", fake_buy.__get__(base_engine))
    monkeypatch.setattr(base_engine, "_detect_e2", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_pm_a2", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_pm_a3", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_pm_a4", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_am_bottom_a1", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_detect_am_confluence_buy_a2", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_finalise_signal", lambda session, entry, **kw: records.append(entry.signal.signal_code))

    event = SimpleNamespace(symbol="AAPL", bar_end=datetime(2025, 1, 2, 10, 5, tzinfo=timezone.utc))
    detected = base_engine._evaluate_event(
        session=None,
        event=event,
        persist=False,
        publish=False,
        mutate_state=False,
        record_metric=False,
    )
    assert records[0] == "SIG_AM_SELL_C1"
    assert detected
