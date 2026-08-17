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
    engine._liquidity_snapshot = {}
    engine._option_liquidity_required = True
    engine._strategy_variant = "v1"
    engine._orb_entry_enabled = False
    engine._reversal_1030_enabled = False
    engine._reversal_1030_legacy_enabled = False
    engine._reversal_1030_v8_guard_filter_enabled = False
    engine._reversal_1030_v8_guard_followthrough_enabled = False
    engine._reversal_1030_v8_guard_rsi_floor_enabled = False
    engine._reversal_1030_v8_guard_continuation_enabled = False
    engine._reversal_1030_v8_guard_stop_enabled = False
    engine._reversal_1030_v8a_enabled = False
    engine._reversal_1030_v8b_enabled = False
    engine._reversal_1030_v8b2_enabled = False
    engine._reversal_1030_v8b2_loose_enabled = False
    engine._reversal_1030_v8b2_room040_enabled = False
    engine._reversal_1030_v8b2_strict_enabled = False
    engine._reversal_1030_v8b3_enabled = False
    engine._reversal_1030_v8_guard_core_room_025_enabled = False
    engine._reversal_1030_v8_guard_core_room_035_enabled = False
    engine._reversal_1030_v8_guard_continuation_v2_enabled = False
    engine._reversal_1030_v8_guard_scalp_observe_enabled = False
    engine._reversal_1030_entries = {}
    engine._delayed_entry_enabled = False
    engine._separated_exit_enabled = False
    engine._redis_bus = None
    engine._history_window = 30
    limits = _build_risk_limits()
    engine._am_bottom_limits = limits.am_bottom
    engine._am_sell1_limits = limits.am_sell1
    engine._am_conf_limits = limits.am_conf
    engine._settings = SimpleNamespace(
        ttl_buy_seconds=180,
        cooldown_buy_seconds=600,
        open_chase_earliest_entry_time="09:36",
        open_chase_opening_range_minutes=5,
        open_chase_max_upper_shadow_pct=Decimal("0.40"),
        open_chase_first_bar_blowoff_range_pct=Decimal("0.012"),
        open_chase_min_rvol=Decimal("1.0"),
        open_chase_require_market_confirmed=False,
        open_chase_min_hold_seconds_before_upper_tap_exit=300,
        open_chase_require_profit_for_upper_tap_exit=True,
        reversal_1030_symbols="TSLA,PLTR,TQQQ,GOOGL,AAPL",
        reversal_1030_start_time="10:25",
        reversal_1030_end_time="10:40",
        reversal_1030_exit_deadline="12:00",
        reversal_1030_max_vix=Decimal("20"),
        reversal_1030_max_abs_gap_pct=Decimal("0.06"),
        reversal_1030_min_morning_drop_pct=Decimal("0.003"),
        reversal_1030_lower_band_buffer_pct=Decimal("0.0015"),
        reversal_1030_rsi_reclaim=Decimal("30"),
        reversal_1030_max_boll_pos_low=Decimal("0.55"),
        reversal_1030_max_boll_pos_reclaim=Decimal("0.60"),
        reversal_1030_max_rsi6=Decimal("65"),
        reversal_1030_reclaim_max_distance_vwap_pct=Decimal("0.0025"),
        reversal_1030_mid_reclaim_max_distance_pct=Decimal("0.0010"),
        reversal_1030_mid_reclaim_min_upper_room_pct=Decimal("0.0035"),
        reversal_1030_mid_reclaim_loose_min_upper_room_pct=Decimal("0.0030"),
        reversal_1030_mid_reclaim_room040_min_upper_room_pct=Decimal("0.0040"),
        reversal_1030_mid_reclaim_strict_max_boll_pos=Decimal("0.55"),
        reversal_1030_mid_reclaim_strict_min_upper_room_pct=Decimal("0.0035"),
        reversal_1030_max_upper_shadow_pct=Decimal("0.40"),
        reversal_1030_failure_stop_entry_low_buffer_pct=Decimal("0.0005"),
        reversal_1030_reclaim_lost_buffer_pct=Decimal("0.0005"),
        reversal_1030_v8_guard_boll_pos_threshold=Decimal("0.30"),
        reversal_1030_v8_guard_rsi_floor_boll_pos_threshold=Decimal("0.20"),
        reversal_1030_v8_guard_rsi_floor_min_rsi6=Decimal("35"),
        reversal_1030_v8_guard_core_room_025_min_upper_room_pct=Decimal("0.0025"),
        reversal_1030_v8_guard_core_room_035_min_upper_room_pct=Decimal("0.0035"),
        reversal_1030_v8_guard_core_max_boll_pos=Decimal("0.80"),
        reversal_1030_v8_guard_continuation_min_boll_pos=Decimal("0.60"),
        reversal_1030_early_followthrough_check_minutes=3,
        reversal_1030_early_followthrough_min_mfe_pct=Decimal("0.0010"),
        reversal_1030_boll_up_true_profit_min_pct=Decimal("0"),
        reversal_1030_continuation_min_exit_profit_pct=Decimal("0.0030"),
        reversal_1030_continuation_max_vwap_distance_pct=Decimal("0.0075"),
        reversal_1030_continuation_fail_buffer_pct=Decimal("0.0003"),
        reversal_1030_thin_room_filter_pct=Decimal("0.0010"),
        reversal_1030_thin_room_filter_min_boll_pos=Decimal("0.80"),
    )
    return engine


def _make_dataframe(values: list[dict], tz: str = "UTC") -> pd.DataFrame:
    frame = pd.DataFrame(values)
    frame["ts_end"] = pd.to_datetime(frame["ts_end"]).dt.tz_convert(tz)
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


class _StubResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _StubSession:
    def __init__(self, responses):
        self._responses = list(responses)

    def execute(self, stmt, params):
        _ = (stmt, params)
        if not self._responses:
            raise AssertionError("unexpected execute call")
        return _StubResult(self._responses.pop(0))


def test_am_bottom_a1_triggers(base_engine):
    ts_start = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)
    rows = []
    lows = [
        100,
        99.8,
        99.6,
        99.4,
        99.2,
        99.0,
        99.4,
        99.6,
        99.8,
        100.0,
        99.8,
        99.6,
        99.4,
        99.2,
        99.0,
        98.8,
        98.6,
        98.4,
        98.2,
        98.7,
    ]
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
    base_engine._am_bottom_limits.allow_mid_filter = False
    base_engine._find_pivot_lows = lambda *args, **kwargs: [5, len(df) - 2]
    base_engine._check_slope_sequences = lambda *args, **kwargs: True
    event = SimpleNamespace(symbol="AAPL", bar_end=rows[-1]["ts_end"])
    signal = base_engine._detect_am_bottom_a1(event, df, event.bar_end.astimezone(timezone.utc).date(), None, None)
    assert signal is not None
    assert signal.signal_code == "SIG_AM_BOTTOM_A1"


def test_daily_close_price_falls_back_to_bars_when_daily_view_missing(base_engine):
    session = _StubSession([None, (Decimal("321.09"),)])

    value = base_engine._daily_close_price(session, "AAPL", datetime(2026, 6, 9, tzinfo=timezone.utc).date())

    assert value == Decimal("321.09")


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
    ts_start = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)
    rows = []
    for idx in range(7):
        ts = ts_start + timedelta(minutes=idx)
        if idx >= 5:
            open_px = 100 - idx * 0.1
            close_px = open_px - 1.5
            high_px = open_px + 0.1
            low_px = close_px - 0.1
            boll_mid = Decimal("101")
        else:
            open_px = 100 - idx * 0.1
            close_px = open_px - 0.1
            high_px = open_px + 0.1
            low_px = close_px - 0.1
            boll_mid = Decimal("101")
        rows.append(
            {
                "ts_end": ts,
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": 1000,
                "boll_mid": boll_mid,
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
        ts = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)
        rows = []
        for idx in range(5):
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
    monkeypatch.setattr(base_engine, "_current_vix", lambda *a, **k: None)
    monkeypatch.setattr(base_engine, "_passes_buy_priority", lambda *a, **k: True)

    def _event_arg(args, kwargs):
        if "event" in kwargs:
            return kwargs["event"]
        return next(arg for arg in args if hasattr(arg, "bar_end"))

    def fake_sell(*args, **kwargs):
        event = _event_arg(args, kwargs)
        return base_engine._build_signal(
            symbol="AAPL",
            signal_code="SIG_AM_SELL_C1",
            side=SignalSide.SELL,
            reason={},
            generated_at=event.bar_end,
        )

    def fake_buy(*args, **kwargs):
        event = _event_arg(args, kwargs)
        return base_engine._build_signal(
            symbol="AAPL",
            signal_code="SIG_AM_BOTTOM_A1",
            side=SignalSide.BUY,
            reason={},
            generated_at=event.bar_end,
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

    event = SimpleNamespace(
        symbol="AAPL",
        bar_end=datetime(2025, 1, 2, 15, 5, tzinfo=timezone.utc),
        trace_id="trace-test",
    )
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


def test_option_liquidity_pass_allows_quote_only_fallback_for_backtest(base_engine):
    base_engine._allow_missing_option_liquidity_metrics = True
    base_engine._option_symbol_column = lambda session: "underlying_symbol"
    ts_end = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)

    class _Result:
        def mappings(self):
            return self

        def all(self):
            return [
                {
                    "bid": Decimal("18.05"),
                    "ask": Decimal("18.65"),
                    "volume": 0,
                    "open_interest": 0,
                    "expiry": "2026-05-29",
                    "strike": Decimal("470"),
                    "ts_end": ts_end,
                    "underlying_price": Decimal("468.86"),
                }
            ]

    class _Session:
        def execute(self, stmt, params):
            return _Result()

    passed, snapshot = base_engine._option_liquidity_pass(_Session(), "AMD", ts_end)
    assert passed is True
    assert snapshot["missing_metrics_fallback"] is True
    assert snapshot["reason"] == "quote_only_missing_oi_volume"


def test_option_liquidity_pass_uses_latest_quote_per_conid(base_engine):
    base_engine._allow_missing_option_liquidity_metrics = True
    ts_end = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)

    base_engine._option_quote_candidates = lambda *args, **kwargs: [
        {
            "bid": Decimal("1.00"),
            "ask": Decimal("1.40"),
            "volume": 500,
            "open_interest": 800,
            "expiry": "2026-05-29",
            "strike": Decimal("470"),
            "ts_end": ts_end - timedelta(seconds=75),
            "underlying_price": Decimal("468.86"),
        },
        {
            "bid": Decimal("1.10"),
            "ask": Decimal("1.14"),
            "volume": 0,
            "open_interest": 0,
            "expiry": "2026-05-29",
            "strike": Decimal("472.5"),
            "ts_end": ts_end - timedelta(seconds=5),
            "underlying_price": Decimal("468.86"),
        },
    ]

    passed, snapshot = base_engine._option_liquidity_pass(object(), "AMD", ts_end)
    assert passed is True
    assert snapshot["reason"] == "quote_only_missing_oi_volume"
    assert snapshot["quote_age_seconds"] == 5.0


def _orb_rows(start: datetime, closes: list[float]) -> list[dict]:
    rows = []
    for idx, close in enumerate(closes):
        ts = start + timedelta(minutes=idx)
        open_px = close - 0.2
        high_px = close + 0.2
        low_px = close - 0.5
        rows.append(
            {
                "ts_end": ts,
                "et": pd.Timestamp(ts).tz_convert("US/Eastern"),
                "open": Decimal(str(open_px)),
                "high": Decimal(str(high_px)),
                "low": Decimal(str(low_px)),
                "close": Decimal(str(close)),
                "volume": Decimal("1000"),
                "rvol6": Decimal("1.5"),
            }
        )
    return rows


def test_open_chase_orb_rejects_early_first_wave(base_engine):
    base_engine._orb_entry_enabled = True
    start = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)
    df = pd.DataFrame(_orb_rows(start, [100.8, 101.5]))
    event = SimpleNamespace(symbol="AMD", bar_end=start + timedelta(minutes=1))

    signal = base_engine._detect_open_chase_orb(
        event, df, event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert signal is None


def test_open_chase_orb_allows_vwap_orh_breakout(base_engine):
    base_engine._orb_entry_enabled = True
    start = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)
    closes = [100.2, 100.5, 100.3, 100.4, 100.6, 101.4, 101.8, 102.2, 102.8, 103.1]
    df = pd.DataFrame(_orb_rows(start, closes))
    event = SimpleNamespace(symbol="AMD", bar_end=start + timedelta(minutes=9))

    signal = base_engine._detect_open_chase_orb(
        event, df, event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_OPEN_CHASE_ORB_BUY"


def _reversal_1030_rows(start: datetime) -> list[dict]:
    rows = []
    closes = [
        100.0,
        99.7,
        99.4,
        99.1,
        98.8,
        98.5,
        98.2,
        97.9,
        97.6,
        97.3,
        97.0,
        96.8,
        96.6,
        96.5,
        96.4,
        96.35,
        96.30,
        96.28,
        96.35,
        96.42,
        96.50,
        96.55,
        96.60,
        96.70,
        96.82,
        96.95,
        97.05,
        97.20,
        97.35,
        97.55,
        97.85,
        98.10,
    ]
    for idx, close in enumerate(closes):
        ts = start + timedelta(minutes=idx)
        open_px = close - 0.05
        high_px = close + 0.12
        low_px = close - 0.18
        if idx in {24, 25, 26, 27}:
            low_px = min(low_px, 96.20)
        rows.append(
            {
                "ts_end": ts,
                "et": pd.Timestamp(ts).tz_convert("US/Eastern"),
                "open": Decimal(str(open_px)),
                "high": Decimal(str(high_px)),
                "low": Decimal(str(low_px)),
                "close": Decimal(str(close)),
                "volume": Decimal("1000"),
                "boll_mid": Decimal("97.50"),
                "boll_up": Decimal("98.60"),
                "boll_dn": Decimal("96.55"),
                "rsi6": Decimal(str(22 + idx * 0.35 if idx < 29 else 31 + (idx - 29) * 1.2)),
                "rvol6": Decimal("1.2"),
            }
        )
    rows[-1]["open"] = Decimal("97.70")
    rows[-1]["close"] = Decimal("98.10")
    rows[-1]["high"] = Decimal("98.25")
    rows[-1]["low"] = Decimal("97.50")
    rows[-2]["rsi6"] = Decimal("29.5")
    rows[-1]["rsi6"] = Decimal("31.5")
    return rows


def test_1030_reversal_call_triggers_after_lower_band_reclaim(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _reversal_1030_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_CALL_BUY"
    assert signal.reason["entry_mode"] == "1030_reversal_call"
    assert signal.option_hint["strategy_family"] == "1030_reversal"


def test_1030_reversal_call_rejects_non_watchlist_symbol(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _reversal_1030_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="AMD", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is None


def test_1030_reversal_call_rejects_high_vix(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _reversal_1030_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("21"), None
    )

    assert signal is None


def _v8_guard_anomaly_rows(start: datetime) -> list[dict]:
    rows = _reversal_1030_rows(start)
    for row in rows:
        row["boll_dn"] = Decimal("96.00")
        row["boll_mid"] = Decimal("98.00")
        row["boll_up"] = Decimal("100.00")
        row["volume"] = Decimal("1000")
    rows[0]["open"] = Decimal("97.00")
    rows[0]["high"] = Decimal("97.10")
    rows[0]["low"] = Decimal("96.80")
    rows[0]["close"] = Decimal("97.00")
    for idx, row in enumerate(rows[1:-1], start=1):
        close = Decimal("96.70") - Decimal(idx) * Decimal("0.01")
        row["open"] = close + Decimal("0.04")
        row["high"] = close + Decimal("0.12")
        row["low"] = min(close - Decimal("0.18"), Decimal("96.10"))
        row["close"] = close
        row["rsi6"] = Decimal("28") + Decimal(idx) * Decimal("0.05")
        row["volume"] = Decimal("10000")
    for row in rows[-4:-1]:
        row["open"] = Decimal("96.16")
        row["close"] = Decimal("96.18")
        row["high"] = Decimal("96.30")
        row["low"] = Decimal("96.05")
    rows[-2]["rsi6"] = Decimal("29.5")
    rows[-1]["open"] = Decimal("96.45")
    rows[-1]["close"] = Decimal("96.70")
    rows[-1]["high"] = Decimal("96.85")
    rows[-1]["low"] = Decimal("96.10")
    rows[-1]["rsi6"] = Decimal("31.0")
    rows[0]["volume"] = Decimal("10")
    rows[-1]["volume"] = Decimal("1000")
    return rows


def test_1030_v8_guard_filter_rejects_above_vwap_low_boll_position(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8_guard_anomaly_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    baseline_signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )
    assert baseline_signal is not None
    assert baseline_signal.reason["v8_guard_triggered"] is True

    base_engine._reversal_1030_v8_guard_filter_enabled = True
    guarded_signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert guarded_signal is None


def test_1030_v8_guard_rsi_floor_rejects_below_vwap_deep_low_weak_rsi(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _reversal_1030_rows(start)
    for row in rows:
        row["boll_dn"] = Decimal("97.00")
        row["boll_mid"] = Decimal("98.00")
        row["boll_up"] = Decimal("100.00")
        row["volume"] = Decimal("1000")
    rows[0]["open"] = Decimal("100.20")
    rows[0]["close"] = Decimal("100.20")
    rows[0]["volume"] = Decimal("50000")
    for idx, row in enumerate(rows[1:-1], start=1):
        row["open"] = Decimal("97.70") - Decimal(idx) * Decimal("0.02")
        row["close"] = Decimal("97.55") - Decimal(idx) * Decimal("0.01")
        row["high"] = row["open"] + Decimal("0.08")
        row["low"] = Decimal("96.95")
        row["rsi6"] = Decimal("28") + Decimal(idx) * Decimal("0.05")
    for row in rows[-4:-1]:
        row["open"] = Decimal("97.06")
        row["close"] = Decimal("97.08")
        row["high"] = Decimal("97.20")
        row["low"] = Decimal("96.95")
    rows[-2]["rsi6"] = Decimal("32.0")
    rows[-1]["open"] = Decimal("97.10")
    rows[-1]["close"] = Decimal("97.50")
    rows[-1]["high"] = Decimal("97.65")
    rows[-1]["low"] = Decimal("96.95")
    rows[-1]["rsi6"] = Decimal("34.0")
    rows[-1]["volume"] = Decimal("1000")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    baseline_signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )
    assert baseline_signal is not None
    assert baseline_signal.reason["entry_mode"] == "1030_reversal_call"

    base_engine._reversal_1030_v8_guard_filter_enabled = True
    base_engine._reversal_1030_v8_guard_rsi_floor_enabled = True
    guarded_signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert guarded_signal is None


def test_1030_v8_guard_continuation_rejects_thin_room_near_upper_below_vwap(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    base_engine._reversal_1030_v8_guard_filter_enabled = True
    base_engine._reversal_1030_v8_guard_rsi_floor_enabled = True
    base_engine._reversal_1030_v8_guard_continuation_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _reversal_1030_rows(start)
    for row in rows:
        row["volume"] = Decimal("1000")
        row["boll_dn"] = Decimal("96.00")
        row["boll_mid"] = Decimal("98.00")
        row["boll_up"] = Decimal("100.04")
    rows[0]["open"] = Decimal("100.20")
    rows[0]["close"] = Decimal("100.20")
    rows[0]["volume"] = Decimal("100000")
    for row in rows[1:-1]:
        row["open"] = Decimal("97.80")
        row["close"] = Decimal("97.70")
        row["high"] = Decimal("98.00")
        row["low"] = Decimal("96.20")
        row["rsi6"] = Decimal("45")
        row["volume"] = Decimal("100")
    rows[-2]["close"] = Decimal("99.45")
    rows[-2]["high"] = Decimal("99.60")
    rows[-2]["low"] = Decimal("96.20")
    rows[-2]["rsi6"] = Decimal("60")
    rows[-1]["open"] = Decimal("99.55")
    rows[-1]["close"] = Decimal("100.00")
    rows[-1]["high"] = Decimal("100.02")
    rows[-1]["low"] = Decimal("99.30")
    rows[-1]["rsi6"] = Decimal("68")
    rows[-1]["volume"] = Decimal("100")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="AAPL", bar_end=rows[-1]["ts_end"])

    baseline_signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert baseline_signal is None


def test_1030_v8_guard_continuation_holds_first_upper_tap_then_targets(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 31, tzinfo=timezone.utc)
    base_engine._positions = {
        "AAPL": PositionState(
            signal_code="SIG_1030_REVERSAL_CALL_BUY",
            opened_at=opened_at,
        )
    }
    base_engine._reversal_1030_entries = {
        "AAPL": {
            "entry_mode": "v8_guard_continuation",
            "entry_bar_low": "99.60",
            "entry_close": "100.00",
            "entry_vwap": "99.80",
            "entry_boll_mid": "99.90",
            "had_positive_unrealized": False,
            "max_unrealized_pct": "0",
            "opened_at": opened_at,
        }
    }
    tap_rows = [
        {
            "ts_end": opened_at,
            "et": pd.Timestamp(opened_at).tz_convert("US/Eastern"),
            "open": Decimal("99.90"),
            "high": Decimal("100.05"),
            "low": Decimal("99.60"),
            "close": Decimal("100.00"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.90"),
            "boll_up": Decimal("100.12"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=3),
            "et": pd.Timestamp(opened_at + timedelta(minutes=3)).tz_convert("US/Eastern"),
            "open": Decimal("100.05"),
            "high": Decimal("100.15"),
            "low": Decimal("99.95"),
            "close": Decimal("100.08"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.95"),
            "boll_up": Decimal("100.12"),
        },
    ]
    tap_event = SimpleNamespace(symbol="AAPL", bar_end=tap_rows[-1]["ts_end"])

    first_tap = base_engine._detect_1030_reversal_exit(
        tap_event, pd.DataFrame(tap_rows), tap_event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert first_tap is None
    assert base_engine._reversal_1030_entries["AAPL"]["continuation_active"] is True

    target_rows = tap_rows + [
        {
            "ts_end": opened_at + timedelta(minutes=8),
            "et": pd.Timestamp(opened_at + timedelta(minutes=8)).tz_convert("US/Eastern"),
            "open": Decimal("100.20"),
            "high": Decimal("100.32"),
            "low": Decimal("100.10"),
            "close": Decimal("100.28"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("100.05"),
            "boll_up": Decimal("100.45"),
        }
    ]
    target_event = SimpleNamespace(symbol="AAPL", bar_end=target_rows[-1]["ts_end"])
    target_signal = base_engine._detect_1030_reversal_exit(
        target_event,
        pd.DataFrame(target_rows),
        target_event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )

    assert target_signal is not None
    assert target_signal.signal_code == "SIG_1030_REVERSAL_CONTINUATION_TARGET_EXIT"


def test_1030_v8_guard_stop_allows_entry_then_exits_below_entry_low(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    base_engine._reversal_1030_v8_guard_stop_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8_guard_anomaly_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_CALL_BUY"
    assert signal.reason["entry_mode"] == "v8_guard_stop"
    assert signal.reason["v8_guard_entry_low_stop_enabled"] is True

    base_engine._register_state(signal, event.bar_end)
    exit_rows = rows[-1:] + [
        {
            "ts_end": event.bar_end + timedelta(minutes=1),
            "et": pd.Timestamp(event.bar_end + timedelta(minutes=1)).tz_convert("US/Eastern"),
            "open": Decimal("96.35"),
            "high": Decimal("96.40"),
            "low": Decimal("96.00"),
            "close": Decimal("96.05"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("98.00"),
            "boll_up": Decimal("100.00"),
        }
    ]
    exit_event = SimpleNamespace(symbol="TSLA", bar_end=exit_rows[-1]["ts_end"])
    exit_signal = base_engine._detect_1030_reversal_exit(
        exit_event, pd.DataFrame(exit_rows), exit_event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert exit_signal is not None
    assert exit_signal.signal_code == "SIG_1030_REVERSAL_FAILURE_ENTRY_LOW_EXIT"
    assert exit_signal.reason["v8_guard_entry_low_stop_enabled"] is True


def test_1030_v8_guard_filter_followthrough_exits_when_reversal_stalls(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 31, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(
            signal_code="SIG_1030_REVERSAL_CALL_BUY",
            opened_at=opened_at,
        )
    }
    base_engine._reversal_1030_entries = {
        "TSLA": {
            "entry_mode": "v8_guard_filter_followthrough",
            "entry_bar_low": "99.00",
            "entry_close": "100.00",
            "entry_vwap": "100.20",
            "entry_boll_mid": "100.10",
            "had_positive_unrealized": False,
            "max_unrealized_pct": "0",
            "opened_at": opened_at,
        }
    }
    rows = [
        {
            "ts_end": opened_at,
            "et": pd.Timestamp(opened_at).tz_convert("US/Eastern"),
            "open": Decimal("99.80"),
            "high": Decimal("100.05"),
            "low": Decimal("99.50"),
            "close": Decimal("100.00"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("100.10"),
            "boll_up": Decimal("101.00"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=3),
            "et": pd.Timestamp(opened_at + timedelta(minutes=3)).tz_convert("US/Eastern"),
            "open": Decimal("100.00"),
            "high": Decimal("100.04"),
            "low": Decimal("99.30"),
            "close": Decimal("99.80"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("100.10"),
            "boll_up": Decimal("101.00"),
        },
    ]
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_exit(
        event, pd.DataFrame(rows), event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_EARLY_NO_FOLLOW_THROUGH_EXIT"
    assert signal.reason["entry_mode"] == "v8_guard_filter_followthrough"


def _v8a_rows(start: datetime) -> list[dict]:
    rows = _reversal_1030_rows(start)
    for row in rows:
        row["boll_dn"] = Decimal("96.00")
        row["boll_mid"] = Decimal("98.50")
        row["boll_up"] = Decimal("101.00")
    rows[0]["close"] = Decimal("101.00")
    rows[0]["open"] = Decimal("101.20")
    rows[0]["high"] = Decimal("101.30")
    rows[-2]["close"] = Decimal("95.90")
    rows[-2]["low"] = Decimal("95.80")
    rows[-2]["rsi6"] = Decimal("29.5")
    rows[-1]["open"] = Decimal("95.95")
    rows[-1]["close"] = Decimal("96.30")
    rows[-1]["high"] = Decimal("96.45")
    rows[-1]["low"] = Decimal("95.85")
    rows[-1]["rsi6"] = Decimal("34")
    return rows


def _v8b_rows(start: datetime) -> list[dict]:
    rows = _reversal_1030_rows(start)
    for row in rows:
        row["boll_dn"] = Decimal("96.50")
        row["boll_mid"] = Decimal("97.85")
        row["boll_up"] = Decimal("100.00")
    rows[-2]["close"] = Decimal("97.78")
    rows[-2]["rsi6"] = Decimal("45")
    rows[-1]["open"] = Decimal("97.80")
    rows[-1]["close"] = Decimal("97.92")
    rows[-1]["high"] = Decimal("97.96")
    rows[-1]["low"] = Decimal("97.75")
    rows[-1]["rsi6"] = Decimal("48")
    return rows

def _v8_guard_core_rows(start: datetime) -> list[dict]:
    rows = _reversal_1030_rows(start)
    for row in rows:
        row["boll_dn"] = Decimal("97.00")
        row["boll_mid"] = Decimal("97.85")
        row["boll_up"] = Decimal("98.40")
    rows[-2]["close"] = Decimal("96.70")
    rows[-2]["open"] = Decimal("96.85")
    rows[-2]["high"] = Decimal("97.10")
    rows[-2]["low"] = Decimal("96.70")
    rows[-2]["rsi6"] = Decimal("45")
    rows[-1]["open"] = Decimal("98.04")
    rows[-1]["close"] = Decimal("98.10")
    rows[-1]["high"] = Decimal("98.14")
    rows[-1]["low"] = Decimal("97.90")
    rows[-1]["rsi6"] = Decimal("48")
    return rows


def _v8_guard_continuation_rows(start: datetime) -> list[dict]:
    rows = _reversal_1030_rows(start)
    for row in rows:
        row["boll_dn"] = Decimal("96.20")
        row["boll_mid"] = Decimal("97.75")
        row["boll_up"] = Decimal("98.90")
    rows[-2]["close"] = Decimal("98.00")
    rows[-2]["open"] = Decimal("97.92")
    rows[-2]["high"] = Decimal("98.10")
    rows[-2]["low"] = Decimal("97.70")
    rows[-2]["rsi6"] = Decimal("55")
    rows[-1]["open"] = Decimal("98.08")
    rows[-1]["close"] = Decimal("98.18")
    rows[-1]["high"] = Decimal("98.25")
    rows[-1]["low"] = Decimal("97.96")
    rows[-1]["rsi6"] = Decimal("58")
    return rows


def _v8_guard_scalp_rows(start: datetime) -> list[dict]:
    rows = _reversal_1030_rows(start)
    for row in rows:
        row["boll_dn"] = Decimal("95.90")
        row["boll_mid"] = Decimal("97.50")
        row["boll_up"] = Decimal("98.16")
    for row in rows[-4:-1]:
        row["low"] = Decimal("95.85")
    rows[-2]["close"] = Decimal("97.96")
    rows[-2]["open"] = Decimal("97.88")
    rows[-2]["high"] = Decimal("98.02")
    rows[-2]["low"] = Decimal("97.72")
    rows[-2]["rsi6"] = Decimal("63")
    rows[-1]["open"] = Decimal("98.00")
    rows[-1]["close"] = Decimal("98.08")
    rows[-1]["high"] = Decimal("98.13")
    rows[-1]["low"] = Decimal("97.92")
    rows[-1]["rsi6"] = Decimal("64")
    return rows


def test_1030_v8a_accepts_below_vwap_low_reversal(base_engine):
    base_engine._reversal_1030_v8a_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8a_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8a_low_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_V8A_LOW_REVERSAL_CALL_BUY"
    assert signal.reason["entry_mode"] == "v8a_low_reversal"
    assert Decimal(signal.reason["boll_position"]) <= Decimal("0.55")


def test_1030_v8a_rejects_high_boll_position_above_vwap_and_hot_rsi(base_engine):
    base_engine._reversal_1030_v8a_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8a_rows(start)
    rows[-1]["close"] = Decimal("99.40")
    rows[-1]["high"] = Decimal("99.55")
    rows[-1]["rsi6"] = Decimal("66")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8a_low_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is None


def test_1030_v8b_accepts_tight_mid_reclaim(base_engine):
    base_engine._reversal_1030_v8b_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8b_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8b_reclaim_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_V8B_RECLAIM_CALL_BUY"
    assert signal.reason["entry_mode"] == "v8b_reclaim"
    assert signal.reason["reclaim_type"] == "boll_mid"


def test_1030_v8b_rejects_extended_reclaim_and_upper_shadow(base_engine):
    base_engine._reversal_1030_v8b_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8b_rows(start)
    rows[-1]["close"] = Decimal("99.30")
    rows[-1]["high"] = Decimal("100.40")
    rows[-1]["low"] = Decimal("97.80")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8b_reclaim_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is None


def test_1030_v8b2_accepts_mid_reclaim_with_upper_room(base_engine):
    base_engine._reversal_1030_v8b2_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8b_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8b2_mid_reclaim_quality_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_BOLL_MID_RECLAIM_CALL_BUY"
    assert signal.reason["entry_mode"] == "v8b2_mid_reclaim_quality"
    assert signal.reason["signal_type"] == "boll_mid_reclaim_only"
    assert signal.reason["reclaim_type"] == "boll_mid"
    assert signal.reason["quality_profile"] == "base"
    assert Decimal(signal.reason["upper_room_pct"]) >= Decimal("0.0035")
    assert signal.reason["min_upper_room_pct"] == "0.0035"


def test_1030_v8b3_marks_followthrough_profile(base_engine):
    base_engine._reversal_1030_v8b2_enabled = True
    base_engine._reversal_1030_v8b3_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8b_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8b2_mid_reclaim_quality_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_BOLL_MID_RECLAIM_CALL_BUY"
    assert signal.reason["entry_mode"] == "v8b3_mid_reclaim_followthrough"
    assert signal.reason["quality_profile"] == "followthrough"
    assert signal.reason["min_upper_room_pct"] == "0.0035"


def test_1030_v8b2_loose_accepts_room_between_030_and_035(base_engine):
    base_engine._reversal_1030_v8b2_enabled = True
    base_engine._reversal_1030_v8b2_loose_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8b_rows(start)
    rows[-1]["boll_dn"] = Decimal("97.46")
    rows[-1]["boll_up"] = Decimal("98.23")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8b2_mid_reclaim_quality_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.reason["quality_profile"] == "loose"
    assert Decimal("0.0030") <= Decimal(signal.reason["upper_room_pct"]) < Decimal("0.0035")
    assert signal.reason["min_upper_room_pct"] == "0.0030"


def test_1030_v8b2_room040_rejects_room_below_040(base_engine):
    base_engine._reversal_1030_v8b2_enabled = True
    base_engine._reversal_1030_v8b2_room040_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8b_rows(start)
    rows[-1]["boll_dn"] = Decimal("97.40")
    rows[-1]["boll_up"] = Decimal("98.29")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8b2_mid_reclaim_quality_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is None


def test_1030_v8b2_rejects_vwap_only_reclaim(base_engine):
    base_engine._reversal_1030_v8b2_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8b_rows(start)
    for row in rows:
        row["boll_mid"] = Decimal("98.50")
        row["boll_dn"] = Decimal("96.50")
        row["boll_up"] = Decimal("101.00")
    rows[-2]["close"] = Decimal("97.78")
    rows[-1]["open"] = Decimal("97.80")
    rows[-1]["close"] = Decimal("97.92")
    rows[-1]["high"] = Decimal("97.96")
    rows[-1]["low"] = Decimal("97.75")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_v8b2_mid_reclaim_quality_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is None


def test_1030_v8b2_rejects_small_upper_room_and_strict_profile(base_engine):
    base_engine._reversal_1030_v8b2_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8b_rows(start)
    rows[-1]["boll_dn"] = Decimal("97.46")
    rows[-1]["boll_up"] = Decimal("98.23")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    small_room_signal = base_engine._detect_1030_v8b2_mid_reclaim_quality_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert small_room_signal is None

    base_engine._reversal_1030_v8b2_strict_enabled = True
    rows = _v8b_rows(start)
    for row in rows:
        row["boll_dn"] = Decimal("96.00")
        row["boll_mid"] = Decimal("97.85")
        row["boll_up"] = Decimal("99.31")
    df = pd.DataFrame(rows)
    strict_signal = base_engine._detect_1030_v8b2_mid_reclaim_quality_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert strict_signal is None


def test_1030_v8_guard_core_room_025_accepts_balanced_core_trade(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    base_engine._reversal_1030_v8_guard_filter_enabled = True
    base_engine._reversal_1030_v8_guard_rsi_floor_enabled = True
    base_engine._reversal_1030_v8_guard_core_room_025_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8_guard_core_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_CALL_BUY"
    assert signal.reason["entry_mode"] == "v8_guard_rsi_floor_core_room_025"
    assert signal.reason["trade_type"] == "option_core_reversal"
    assert signal.reason["quality_profile"] == "core_room_025"
    assert Decimal(signal.reason["upper_room_pct"]) >= Decimal("0.0025")
    assert Decimal(signal.reason["boll_position"]) <= Decimal("0.80")


def test_1030_v8_guard_core_room_035_rejects_thin_room(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    base_engine._reversal_1030_v8_guard_filter_enabled = True
    base_engine._reversal_1030_v8_guard_rsi_floor_enabled = True
    base_engine._reversal_1030_v8_guard_core_room_035_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8_guard_core_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is None


def test_1030_v8_guard_core_room_035_accepts_strict_core_trade(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    base_engine._reversal_1030_v8_guard_filter_enabled = True
    base_engine._reversal_1030_v8_guard_rsi_floor_enabled = True
    base_engine._reversal_1030_v8_guard_core_room_035_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8_guard_core_rows(start)
    rows[-1]["boll_up"] = Decimal("98.55")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.reason["entry_mode"] == "v8_guard_rsi_floor_core_room_035"
    assert signal.reason["quality_profile"] == "core_room_035"
    assert Decimal(signal.reason["upper_room_pct"]) >= Decimal("0.0035")


def test_1030_v8_guard_continuation_v2_accepts_true_continuation(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    base_engine._reversal_1030_v8_guard_filter_enabled = True
    base_engine._reversal_1030_v8_guard_rsi_floor_enabled = True
    base_engine._reversal_1030_v8_guard_continuation_v2_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8_guard_continuation_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="PLTR", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.reason["entry_mode"] == "v8_guard_continuation_v2"
    assert signal.reason["trade_type"] == "continuation_candidate"
    assert signal.reason["is_continuation_candidate"] is True


def test_1030_v8_guard_scalp_observe_marks_thin_room_observation(base_engine):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_legacy_enabled = True
    base_engine._reversal_1030_v8_guard_filter_enabled = True
    base_engine._reversal_1030_v8_guard_rsi_floor_enabled = True
    base_engine._reversal_1030_v8_guard_scalp_observe_enabled = True
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8_guard_scalp_rows(start)
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="AAPL", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_call(
        event, df, event.bar_end.astimezone(timezone.utc).date(), Decimal("18"), None
    )

    assert signal is not None
    assert signal.reason["entry_mode"] == "v8_guard_scalp_observe"
    assert signal.reason["trade_type"] == "scalp_thin_room"
    assert signal.reason["is_scalp_thin_room"] is True


def test_1030_reversal_failure_entry_low_exit(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 32, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(
            signal_code="SIG_1030_V8A_LOW_REVERSAL_CALL_BUY",
            opened_at=opened_at,
        )
    }
    base_engine._reversal_1030_entries = {
        "TSLA": {
            "entry_mode": "v8a_low_reversal",
            "entry_bar_low": "97.50",
            "entry_close": "97.80",
            "entry_vwap": "98.20",
            "entry_boll_mid": "98.50",
            "had_positive_unrealized": False,
        }
    }
    rows = [
        {
            "ts_end": opened_at,
            "et": pd.Timestamp(opened_at).tz_convert("US/Eastern"),
            "open": Decimal("97.9"),
            "high": Decimal("98.0"),
            "low": Decimal("97.6"),
            "close": Decimal("97.8"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("98.5"),
            "boll_up": Decimal("99.5"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=1),
            "et": pd.Timestamp(opened_at + timedelta(minutes=1)).tz_convert("US/Eastern"),
            "open": Decimal("97.8"),
            "high": Decimal("97.9"),
            "low": Decimal("97.4"),
            "close": Decimal("97.45"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("98.5"),
            "boll_up": Decimal("99.5"),
        }
    ]
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_exit(
        event, df, event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_FAILURE_ENTRY_LOW_EXIT"


def test_1030_reversal_reclaim_lost_exit_only_after_profit(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 32, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(
            signal_code="SIG_1030_V8B_RECLAIM_CALL_BUY",
            opened_at=opened_at,
        )
    }
    base_engine._reversal_1030_entries = {
        "TSLA": {
            "entry_mode": "v8b_reclaim",
            "entry_bar_low": "97.50",
            "entry_close": "97.90",
            "entry_vwap": "97.85",
            "entry_boll_mid": "97.85",
            "reclaim_type": "boll_mid",
            "had_positive_unrealized": False,
        }
    }
    rows = [
        {
            "ts_end": opened_at,
            "et": pd.Timestamp(opened_at).tz_convert("US/Eastern"),
            "open": Decimal("97.8"),
            "high": Decimal("98.0"),
            "low": Decimal("97.6"),
            "close": Decimal("97.9"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.85"),
            "boll_up": Decimal("99.5"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=1),
            "et": pd.Timestamp(opened_at + timedelta(minutes=1)).tz_convert("US/Eastern"),
            "open": Decimal("97.9"),
            "high": Decimal("98.4"),
            "low": Decimal("97.85"),
            "close": Decimal("98.2"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.85"),
            "boll_up": Decimal("99.5"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=2),
            "et": pd.Timestamp(opened_at + timedelta(minutes=2)).tz_convert("US/Eastern"),
            "open": Decimal("98.1"),
            "high": Decimal("98.2"),
            "low": Decimal("97.70"),
            "close": Decimal("97.80"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.85"),
            "boll_up": Decimal("99.5"),
        },
    ]
    first_event = SimpleNamespace(symbol="TSLA", bar_end=rows[1]["ts_end"])
    first_signal = base_engine._detect_1030_reversal_exit(
        first_event, pd.DataFrame(rows[:2]), first_event.bar_end.astimezone(timezone.utc).date(), None, None
    )
    second_event = SimpleNamespace(symbol="TSLA", bar_end=rows[2]["ts_end"])
    second_signal = base_engine._detect_1030_reversal_exit(
        second_event, pd.DataFrame(rows), second_event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert first_signal is None
    assert second_signal is not None
    assert second_signal.signal_code == "SIG_1030_REVERSAL_FAILURE_RECLAIM_LOST_EXIT"


def test_1030_v8b2_reclaim_lost_uses_buffer_after_profit(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 32, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(
            signal_code="SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
            opened_at=opened_at,
        )
    }
    base_engine._reversal_1030_entries = {
        "TSLA": {
            "entry_mode": "v8b2_mid_reclaim_quality",
            "entry_bar_low": "97.50",
            "entry_close": "97.90",
            "entry_vwap": "97.85",
            "entry_boll_mid": "97.85",
            "reclaim_type": "boll_mid",
            "had_positive_unrealized": False,
        }
    }
    rows = [
        {
            "ts_end": opened_at,
            "et": pd.Timestamp(opened_at).tz_convert("US/Eastern"),
            "open": Decimal("97.8"),
            "high": Decimal("98.0"),
            "low": Decimal("97.6"),
            "close": Decimal("97.9"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.85"),
            "boll_up": Decimal("99.5"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=1),
            "et": pd.Timestamp(opened_at + timedelta(minutes=1)).tz_convert("US/Eastern"),
            "open": Decimal("97.9"),
            "high": Decimal("98.4"),
            "low": Decimal("97.85"),
            "close": Decimal("98.2"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.85"),
            "boll_up": Decimal("99.5"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=2),
            "et": pd.Timestamp(opened_at + timedelta(minutes=2)).tz_convert("US/Eastern"),
            "open": Decimal("98.1"),
            "high": Decimal("98.2"),
            "low": Decimal("97.78"),
            "close": Decimal("97.82"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.85"),
            "boll_up": Decimal("99.5"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=3),
            "et": pd.Timestamp(opened_at + timedelta(minutes=3)).tz_convert("US/Eastern"),
            "open": Decimal("97.82"),
            "high": Decimal("97.84"),
            "low": Decimal("97.75"),
            "close": Decimal("97.79"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.85"),
            "boll_up": Decimal("99.5"),
        },
    ]
    profit_event = SimpleNamespace(symbol="TSLA", bar_end=rows[1]["ts_end"])
    profit_signal = base_engine._detect_1030_reversal_exit(
        profit_event,
        pd.DataFrame(rows[:2]),
        profit_event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )
    shallow_event = SimpleNamespace(symbol="TSLA", bar_end=rows[2]["ts_end"])
    shallow_signal = base_engine._detect_1030_reversal_exit(
        shallow_event,
        pd.DataFrame(rows[:3]),
        shallow_event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )
    deep_event = SimpleNamespace(symbol="TSLA", bar_end=rows[3]["ts_end"])
    deep_signal = base_engine._detect_1030_reversal_exit(
        deep_event,
        pd.DataFrame(rows),
        deep_event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )

    assert profit_signal is None
    assert shallow_signal is None
    assert deep_signal is not None
    assert deep_signal.signal_code == "SIG_1030_REVERSAL_FAILURE_RECLAIM_LOST_EXIT"
    assert deep_signal.reason["lost_buffer_pct"] == "0.0005"


def test_1030_v8b3_early_no_followthrough_requires_mid_loss(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 32, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(
            signal_code="SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
            opened_at=opened_at,
        )
    }
    base_engine._reversal_1030_entries = {
        "TSLA": {
            "entry_mode": "v8b3_mid_reclaim_followthrough",
            "entry_bar_low": "97.50",
            "entry_close": "100.00",
            "entry_vwap": "99.80",
            "entry_boll_mid": "99.90",
            "reclaim_type": "boll_mid",
            "opened_at": opened_at,
            "had_positive_unrealized": False,
            "max_unrealized_pct": "0.0006",
        }
    }
    rows = [
        {
            "ts_end": opened_at,
            "et": pd.Timestamp(opened_at).tz_convert("US/Eastern"),
            "open": Decimal("99.90"),
            "high": Decimal("100.03"),
            "low": Decimal("99.85"),
            "close": Decimal("100.00"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.90"),
            "boll_up": Decimal("101.00"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=1),
            "et": pd.Timestamp(opened_at + timedelta(minutes=1)).tz_convert("US/Eastern"),
            "open": Decimal("100.00"),
            "high": Decimal("100.05"),
            "low": Decimal("99.90"),
            "close": Decimal("99.96"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.92"),
            "boll_up": Decimal("101.00"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=2),
            "et": pd.Timestamp(opened_at + timedelta(minutes=2)).tz_convert("US/Eastern"),
            "open": Decimal("99.96"),
            "high": Decimal("100.06"),
            "low": Decimal("99.91"),
            "close": Decimal("99.95"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.94"),
            "boll_up": Decimal("101.00"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=3),
            "et": pd.Timestamp(opened_at + timedelta(minutes=3)).tz_convert("US/Eastern"),
            "open": Decimal("99.95"),
            "high": Decimal("100.04"),
            "low": Decimal("99.82"),
            "close": Decimal("99.90"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.95"),
            "boll_up": Decimal("101.00"),
        },
    ]
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_exit(
        event,
        pd.DataFrame(rows),
        event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_EARLY_NO_FOLLOW_THROUGH_EXIT"
    assert signal.reason["max_unrealized_pct"] == "0.0006"


def test_1030_v8b3_no_early_exit_when_mid_holds(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 32, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(
            signal_code="SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
            opened_at=opened_at,
        )
    }
    base_engine._reversal_1030_entries = {
        "TSLA": {
            "entry_mode": "v8b3_mid_reclaim_followthrough",
            "entry_bar_low": "97.50",
            "entry_close": "100.00",
            "entry_vwap": "99.80",
            "entry_boll_mid": "99.90",
            "reclaim_type": "boll_mid",
            "opened_at": opened_at,
            "had_positive_unrealized": False,
            "max_unrealized_pct": "0",
        }
    }
    rows = [
        {
            "ts_end": opened_at,
            "et": pd.Timestamp(opened_at).tz_convert("US/Eastern"),
            "open": Decimal("99.90"),
            "high": Decimal("100.03"),
            "low": Decimal("99.85"),
            "close": Decimal("100.00"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.90"),
            "boll_up": Decimal("101.00"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=3),
            "et": pd.Timestamp(opened_at + timedelta(minutes=3)).tz_convert("US/Eastern"),
            "open": Decimal("99.98"),
            "high": Decimal("100.06"),
            "low": Decimal("99.90"),
            "close": Decimal("99.98"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.95"),
            "boll_up": Decimal("101.00"),
        },
    ]
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_exit(
        event,
        pd.DataFrame(rows),
        event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )

    assert signal is None


def test_1030_v8b3_splits_boll_upper_rebound_failure(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 32, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(
            signal_code="SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
            opened_at=opened_at,
        )
    }
    base_engine._reversal_1030_entries = {
        "TSLA": {
            "entry_mode": "v8b3_mid_reclaim_followthrough",
            "entry_bar_low": "97.50",
            "entry_close": "100.00",
            "entry_vwap": "99.80",
            "entry_boll_mid": "99.90",
            "reclaim_type": "boll_mid",
            "opened_at": opened_at,
            "had_positive_unrealized": False,
            "max_unrealized_pct": "0",
        }
    }
    rows = [
        {
            "ts_end": opened_at,
            "et": pd.Timestamp(opened_at).tz_convert("US/Eastern"),
            "open": Decimal("99.90"),
            "high": Decimal("100.10"),
            "low": Decimal("99.80"),
            "close": Decimal("100.00"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.90"),
            "boll_up": Decimal("101.00"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=5),
            "et": pd.Timestamp(opened_at + timedelta(minutes=5)).tz_convert("US/Eastern"),
            "open": Decimal("99.70"),
            "high": Decimal("99.90"),
            "low": Decimal("99.60"),
            "close": Decimal("99.80"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("99.30"),
            "boll_up": Decimal("99.85"),
        },
    ]
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_exit(
        event,
        pd.DataFrame(rows),
        event.bar_end.astimezone(timezone.utc).date(),
        None,
        None,
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_BOLL_UP_REBOUND_FAILURE_EXIT"


def test_1030_v8_ab_does_not_emit_duplicate_same_symbol_entries(base_engine, monkeypatch):
    base_engine._reversal_1030_enabled = True
    base_engine._reversal_1030_v8a_enabled = True
    base_engine._reversal_1030_v8b_enabled = True
    base_engine._metrics_enabled = False
    base_engine._buy_priority_slots = 0
    base_engine._current_vix = lambda session, ts_end: Decimal("18")
    base_engine._should_emit = lambda signal, ts_end: True
    base_engine._finalise_signal = (
        lambda session, detected, **kwargs: base_engine._register_state(detected.signal, detected.ts_end)
    )
    start = datetime(2026, 6, 1, 13, 55, tzinfo=timezone.utc)
    rows = _v8a_rows(start)
    rows[-2]["close"] = Decimal("94.95")
    rows[-2]["low"] = Decimal("94.90")
    rows[-2]["boll_dn"] = Decimal("95.00")
    rows[-2]["boll_mid"] = Decimal("96.20")
    rows[-2]["boll_up"] = Decimal("100.00")
    rows[-1]["close"] = Decimal("96.30")
    rows[-1]["high"] = Decimal("96.45")
    rows[-1]["boll_dn"] = Decimal("95.00")
    rows[-1]["boll_mid"] = Decimal("96.20")
    rows[-1]["boll_up"] = Decimal("100.00")
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"], trace_id="trace")
    monkeypatch.setattr(base_engine, "_load_history", lambda *args, **kwargs: df)

    detected = base_engine._evaluate_event(
        None,
        event,
        persist=False,
        publish=False,
        mutate_state=True,
        record_metric=False,
    )

    entry_codes = [item.signal.signal_code for item in detected if item.signal.side == SignalSide.BUY]
    assert len(entry_codes) == 1
    assert entry_codes[0] == "SIG_1030_V8A_LOW_REVERSAL_CALL_BUY"


def test_1030_reversal_exit_on_boll_upper_tap(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 32, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(signal_code="SIG_1030_REVERSAL_CALL_BUY", opened_at=opened_at)
    }
    rows = [
        {
            "ts_end": opened_at + timedelta(minutes=1),
            "et": pd.Timestamp(opened_at + timedelta(minutes=1)).tz_convert("US/Eastern"),
            "open": Decimal("98.0"),
            "high": Decimal("98.4"),
            "low": Decimal("97.8"),
            "close": Decimal("98.2"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.5"),
            "boll_up": Decimal("99.0"),
        },
        {
            "ts_end": opened_at + timedelta(minutes=2),
            "et": pd.Timestamp(opened_at + timedelta(minutes=2)).tz_convert("US/Eastern"),
            "open": Decimal("98.4"),
            "high": Decimal("99.2"),
            "low": Decimal("98.3"),
            "close": Decimal("99.0"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.8"),
            "boll_up": Decimal("99.1"),
        },
    ]
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_exit(
        event, df, event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_BOLL_UP_EXIT"


def test_1030_reversal_exit_at_deadline(base_engine):
    opened_at = datetime(2026, 6, 1, 14, 32, tzinfo=timezone.utc)
    bar_end = datetime(2026, 6, 1, 16, 0, tzinfo=timezone.utc)
    base_engine._positions = {
        "TSLA": PositionState(signal_code="SIG_1030_REVERSAL_CALL_BUY", opened_at=opened_at)
    }
    rows = [
        {
            "ts_end": bar_end,
            "et": pd.Timestamp(bar_end).tz_convert("US/Eastern"),
            "open": Decimal("98.0"),
            "high": Decimal("98.4"),
            "low": Decimal("97.8"),
            "close": Decimal("98.2"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.5"),
            "boll_up": Decimal("99.0"),
        },
        {
            "ts_end": bar_end + timedelta(minutes=1),
            "et": pd.Timestamp(bar_end + timedelta(minutes=1)).tz_convert("US/Eastern"),
            "open": Decimal("98.1"),
            "high": Decimal("98.5"),
            "low": Decimal("98.0"),
            "close": Decimal("98.3"),
            "volume": Decimal("1000"),
            "boll_mid": Decimal("97.6"),
            "boll_up": Decimal("99.2"),
        },
    ]
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="TSLA", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_1030_reversal_exit(
        event, df, event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert signal is not None
    assert signal.signal_code == "SIG_1030_REVERSAL_TIME_EXIT"


def test_upper_tap_requires_profit_when_separated_exit_enabled(base_engine):
    base_engine._separated_exit_enabled = True
    opened_at = datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc)
    base_engine._positions = {
        "AMD": PositionState(signal_code="SIG_OPEN_CHASE_BUY", opened_at=opened_at)
    }
    rows = []
    for idx, (close, up) in enumerate([(100, 99), (99, 101), (98, 101)]):
        ts = opened_at + timedelta(minutes=6 + idx)
        rows.append(
            {
                "ts_end": ts,
                "et": pd.Timestamp(ts).tz_convert("US/Eastern"),
                "open": Decimal(str(close)),
                "high": Decimal(str(close + 1)),
                "low": Decimal(str(close - 1)),
                "close": Decimal(str(close)),
                "volume": Decimal("1000"),
                "boll_up": Decimal(str(up)),
            }
        )
    df = pd.DataFrame(rows)
    event = SimpleNamespace(symbol="AMD", bar_end=rows[-1]["ts_end"])

    signal = base_engine._detect_s2(
        event, df, event.bar_end.astimezone(timezone.utc).date(), None, None
    )

    assert signal is None
