#!/usr/bin/env python3
"""Equity-only research scanner for early opening chase lifecycle variants.

The scanner is read-only. It replays 1m equity bars and tests whether an
opening-range breakout with lifecycle exits has enough trade count and
underlying return thickness to become a new main research line.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from statistics import median
from typing import Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.core import EASTERN  # noqa: E402
from libs.core.config import get_settings  # noqa: E402
from scripts.opening_momentum_research import (  # noqa: E402
    RunInfo,
    _bar_shadow_ratio,
    _daily_groups,
    _deadline_ts,
    _et_str,
    _last_available_ts,
    _load_bars,
    _load_runs,
    _markdown_table,
    _parse_hhmm,
    _pct,
    _prev_rth_close,
    _safe_float,
    _safe_ratio,
)


@dataclass(frozen=True)
class ChaseVariant:
    name: str
    entry_style: str
    entry_start: time
    entry_end: time
    min_gap_or_or_move_pct: float
    min_or_move_pct: float
    min_or_break_pct: float
    max_first_range_pct: float
    min_close_position: float
    max_close_position: float | None
    max_upper_shadow_ratio: float
    min_rel_volume: float
    min_rsi6: float | None = None
    max_rsi6: float | None = None
    max_vwap_dist_pct: float | None = None
    require_green: bool = True
    require_above_ma60: bool = False
    max_reclaim_dist_pct: float | None = None
    min_gap_pct: float | None = None


@dataclass(frozen=True)
class ChaseTradeRecord:
    version_name: str
    symbol: str
    trade_date: str
    entry_time: str
    exit_time: str
    entry_ts_utc: str
    exit_ts_utc: str
    entry_price: float
    exit_price: float
    return_pct: float
    MFE: float
    MAE: float
    holding_minutes: float
    exit_reason: str
    entry_style: str
    gap_pct: float
    opening_range_move_pct: float
    first_range_pct: float
    or_break_pct: float
    reclaim_dist_pct: float
    vwap_dist_pct: float
    close_position: float
    upper_shadow_ratio: float
    rsi6: float
    rel_volume: float
    close_above_vwap: bool
    close_above_orh: bool
    close_above_ma60: bool


VARIANTS = [
    ChaseVariant(
        name="opening_chase_v2_orb_loose",
        entry_style="orb_break",
        entry_start=time(9, 36),
        entry_end=time(9, 55),
        min_gap_or_or_move_pct=0.35,
        min_or_move_pct=0.35,
        min_or_break_pct=0.00,
        max_first_range_pct=1.80,
        min_close_position=0.55,
        max_close_position=None,
        max_upper_shadow_ratio=0.45,
        min_rel_volume=1.00,
        min_rsi6=50.0,
        max_rsi6=82.0,
        max_vwap_dist_pct=1.80,
    ),
    ChaseVariant(
        name="opening_chase_v2_orb_balanced",
        entry_style="orb_break",
        entry_start=time(9, 36),
        entry_end=time(9, 52),
        min_gap_or_or_move_pct=0.50,
        min_or_move_pct=0.50,
        min_or_break_pct=0.03,
        max_first_range_pct=1.50,
        min_close_position=0.60,
        max_close_position=None,
        max_upper_shadow_ratio=0.35,
        min_rel_volume=1.10,
        min_rsi6=55.0,
        max_rsi6=80.0,
        max_vwap_dist_pct=1.45,
    ),
    ChaseVariant(
        name="opening_chase_v2_orb_quality",
        entry_style="orb_break",
        entry_start=time(9, 36),
        entry_end=time(9, 48),
        min_gap_or_or_move_pct=0.70,
        min_or_move_pct=0.65,
        min_or_break_pct=0.05,
        max_first_range_pct=1.25,
        min_close_position=0.65,
        max_close_position=None,
        max_upper_shadow_ratio=0.28,
        min_rel_volume=1.20,
        min_rsi6=58.0,
        max_rsi6=78.0,
        max_vwap_dist_pct=1.20,
    ),
    ChaseVariant(
        name="opening_chase_v2_gap_balanced",
        entry_style="orb_break",
        entry_start=time(9, 40),
        entry_end=time(9, 52),
        min_gap_or_or_move_pct=0.50,
        min_or_move_pct=0.50,
        min_or_break_pct=0.03,
        max_first_range_pct=1.50,
        min_close_position=0.60,
        max_close_position=None,
        max_upper_shadow_ratio=0.35,
        min_rel_volume=1.10,
        min_rsi6=55.0,
        max_rsi6=80.0,
        max_vwap_dist_pct=0.55,
        min_gap_pct=0.0,
    ),
    ChaseVariant(
        name="opening_chase_v2_gap_quality",
        entry_style="orb_break",
        entry_start=time(9, 40),
        entry_end=time(9, 48),
        min_gap_or_or_move_pct=0.70,
        min_or_move_pct=0.65,
        min_or_break_pct=0.05,
        max_first_range_pct=1.25,
        min_close_position=0.65,
        max_close_position=None,
        max_upper_shadow_ratio=0.28,
        min_rel_volume=1.20,
        min_rsi6=58.0,
        max_rsi6=78.0,
        max_vwap_dist_pct=0.55,
        min_gap_pct=0.0,
    ),
    ChaseVariant(
        name="opening_chase_v2_gap_quality_break030",
        entry_style="orb_break",
        entry_start=time(9, 40),
        entry_end=time(9, 48),
        min_gap_or_or_move_pct=0.70,
        min_or_move_pct=0.65,
        min_or_break_pct=0.30,
        max_first_range_pct=1.25,
        min_close_position=0.65,
        max_close_position=None,
        max_upper_shadow_ratio=0.28,
        min_rel_volume=1.20,
        min_rsi6=58.0,
        max_rsi6=78.0,
        max_vwap_dist_pct=0.55,
        min_gap_pct=0.0,
    ),
    ChaseVariant(
        name="opening_chase_v2_gap_quality_core",
        entry_style="orb_break",
        entry_start=time(9, 40),
        entry_end=time(9, 48),
        min_gap_or_or_move_pct=0.70,
        min_or_move_pct=0.65,
        min_or_break_pct=0.30,
        max_first_range_pct=1.25,
        min_close_position=0.65,
        max_close_position=0.95,
        max_upper_shadow_ratio=0.28,
        min_rel_volume=1.20,
        min_rsi6=58.0,
        max_rsi6=78.0,
        max_vwap_dist_pct=0.55,
        min_gap_pct=0.0,
    ),
    ChaseVariant(
        name="opening_chase_v2_orb_strict_option_core",
        entry_style="orb_break",
        entry_start=time(9, 36),
        entry_end=time(9, 45),
        min_gap_or_or_move_pct=0.85,
        min_or_move_pct=0.80,
        min_or_break_pct=0.08,
        max_first_range_pct=1.10,
        min_close_position=0.70,
        max_close_position=None,
        max_upper_shadow_ratio=0.20,
        min_rel_volume=1.30,
        min_rsi6=60.0,
        max_rsi6=76.0,
        max_vwap_dist_pct=1.05,
    ),
    ChaseVariant(
        name="opening_chase_v2_pullback_reclaim",
        entry_style="pullback_reclaim",
        entry_start=time(9, 40),
        entry_end=time(10, 0),
        min_gap_or_or_move_pct=0.50,
        min_or_move_pct=0.45,
        min_or_break_pct=0.03,
        max_first_range_pct=1.60,
        min_close_position=0.58,
        max_close_position=None,
        max_upper_shadow_ratio=0.35,
        min_rel_volume=1.05,
        min_rsi6=50.0,
        max_rsi6=78.0,
        max_vwap_dist_pct=1.20,
        max_reclaim_dist_pct=0.25,
    ),
]

DEFAULT_DIRECT_SYMBOLS = [
    "TSLA",
    "PLTR",
    "TQQQ",
    "GOOGL",
    "AAPL",
    "NVDA",
    "AMD",
    "MSFT",
    "META",
    "AMZN",
    "NFLX",
    "AVGO",
    "COIN",
    "SMCI",
    "MSTR",
    "QQQ",
    "SPY",
]


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan opening chase lifecycle variants.")
    parser.add_argument("--batch-id", default=None, help="Baseline batch id to infer scope.")
    parser.add_argument("--run-ids", nargs="*", type=int, default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--start", default=None, help="Direct scan start date (YYYY-MM-DD ET).")
    parser.add_argument("--end", default=None, help="Direct scan end date (YYYY-MM-DD ET).")
    parser.add_argument(
        "--versions",
        nargs="*",
        default=None,
        help="Optional variant whitelist for direct or baseline scans.",
    )
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--deadline", default="12:00")
    parser.add_argument("--target-pct", type=float, default=1.25)
    parser.add_argument("--hard-stop-buffer-pct", type=float, default=0.05)
    parser.add_argument("--protect-trigger-pct", type=float, default=0.65)
    parser.add_argument("--protect-floor-pct", type=float, default=0.20)
    parser.add_argument("--no-new-high-minutes", type=int, default=8)
    parser.add_argument("--no-profit-exit-minutes", type=int, default=10)
    parser.add_argument("--no-profit-min-mfe-pct", type=float, default=0.20)
    parser.add_argument("--trend-confirm-time", default="10:00")
    parser.add_argument("--trend-confirm-mfe-pct", type=float, default=0.35)
    return parser.parse_args(list(argv) if argv is not None else None)


def _parse_ymd(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _opening_range(day: pd.DataFrame, minutes: int = 5) -> tuple[pd.DataFrame, float, float]:
    if day.empty:
        return day, math.nan, math.nan
    end_hour = 9
    end_minute = 30 + max(1, minutes) - 1
    if end_minute >= 60:
        end_hour += end_minute // 60
        end_minute %= 60
    window = day[(day["time_et"] >= time(9, 30)) & (day["time_et"] <= time(end_hour, end_minute))]
    if window.empty:
        return window, math.nan, math.nan
    return window, _safe_float(window["high"].max()), _safe_float(window["low"].min())


def _close_position(row: pd.Series) -> float:
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    close = _safe_float(row.get("close"))
    return _safe_ratio(close - low, high - low)


def _relative_volume(rth: pd.DataFrame, ts: pd.Timestamp, current_volume: float) -> float:
    prev = rth[rth.index < ts].tail(5)
    if len(prev) < 3:
        return math.nan
    base = _safe_float(prev["volume"].median())
    return _safe_ratio(current_volume, base)


def _base_features(
    *,
    full_symbol_frame: pd.DataFrame,
    day: pd.DataFrame,
    rth: pd.DataFrame,
    trade_date: str,
    or_high: float,
    or_low: float,
) -> dict[str, float]:
    open_price = _safe_float(rth.iloc[0]["open"])
    prev_close = _prev_rth_close(full_symbol_frame, trade_date)
    gap_pct = _pct(_safe_ratio(open_price - prev_close, prev_close))
    opening_range_move_pct = _pct(_safe_ratio(or_high - open_price, open_price))
    first_range_pct = _pct(_safe_ratio(or_high - or_low, open_price))
    return {
        "gap_pct": gap_pct,
        "opening_range_move_pct": opening_range_move_pct,
        "first_range_pct": first_range_pct,
    }


def _passes_static_filters(variant: ChaseVariant, features: Mapping[str, float]) -> bool:
    gap_pct = float(features.get("gap_pct", math.nan))
    or_move = float(features.get("opening_range_move_pct", math.nan))
    first_range = float(features.get("first_range_pct", math.nan))
    if not math.isfinite(or_move) or or_move < variant.min_or_move_pct:
        return False
    if max(gap_pct if math.isfinite(gap_pct) else -math.inf, or_move) < variant.min_gap_or_or_move_pct:
        return False
    if not math.isfinite(first_range) or first_range <= 0 or first_range > variant.max_first_range_pct:
        return False
    if variant.min_gap_pct is not None and (not math.isfinite(gap_pct) or gap_pct < variant.min_gap_pct):
        return False
    return True


def _candidate_features(
    *,
    variant: ChaseVariant,
    rth: pd.DataFrame,
    ts: pd.Timestamp,
    row: pd.Series,
    previous_row: pd.Series | None,
    or_high: float,
) -> dict[str, float | bool] | None:
    open_px = _safe_float(row.get("open"))
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    close = _safe_float(row.get("close"))
    vwap = _safe_float(row.get("VWAP"))
    ma60 = _safe_float(row.get("ma60"))
    rsi6 = _safe_float(row.get("rsi6"))
    volume = _safe_float(row.get("volume"))
    if not all(math.isfinite(value) for value in [open_px, high, low, close, vwap, volume]):
        return None
    if close <= 0 or high <= low:
        return None
    if variant.require_green and close <= open_px:
        return None
    close_above_vwap = close > vwap
    close_above_orh = close > or_high
    if not close_above_vwap or not close_above_orh:
        return None

    close_pos = _close_position(row)
    if not math.isfinite(close_pos) or close_pos < variant.min_close_position:
        return None
    if variant.max_close_position is not None and close_pos > variant.max_close_position:
        return None
    upper_shadow_ratio = _bar_shadow_ratio(row)
    if math.isfinite(upper_shadow_ratio) and upper_shadow_ratio > variant.max_upper_shadow_ratio:
        return None
    rel_volume = _relative_volume(rth, ts, volume)
    if not math.isfinite(rel_volume) or rel_volume < variant.min_rel_volume:
        return None
    if variant.min_rsi6 is not None and (not math.isfinite(rsi6) or rsi6 < variant.min_rsi6):
        return None
    if variant.max_rsi6 is not None and math.isfinite(rsi6) and rsi6 > variant.max_rsi6:
        return None
    vwap_dist_pct = _pct(_safe_ratio(close - vwap, close))
    if not math.isfinite(vwap_dist_pct):
        return None
    if variant.max_vwap_dist_pct is not None and vwap_dist_pct > variant.max_vwap_dist_pct:
        return None
    if variant.require_above_ma60 and (not math.isfinite(ma60) or close < ma60):
        return None

    or_break_pct = _pct(_safe_ratio(close - or_high, or_high))
    if not math.isfinite(or_break_pct) or or_break_pct < variant.min_or_break_pct:
        return None

    reclaim_dist_pct = math.nan
    if variant.entry_style == "pullback_reclaim":
        if previous_row is None:
            return None
        prev_close = _safe_float(previous_row.get("close"))
        level = max(or_high, vwap)
        if not math.isfinite(prev_close) or not math.isfinite(level) or level <= 0:
            return None
        if prev_close > level or close < level:
            return None
        reclaim_dist_pct = _pct(_safe_ratio(close - level, level))
        if (
            variant.max_reclaim_dist_pct is not None
            and (not math.isfinite(reclaim_dist_pct) or reclaim_dist_pct > variant.max_reclaim_dist_pct)
        ):
            return None

    return {
        "or_break_pct": or_break_pct,
        "reclaim_dist_pct": reclaim_dist_pct,
        "vwap_dist_pct": vwap_dist_pct,
        "close_position": close_pos,
        "upper_shadow_ratio": upper_shadow_ratio,
        "rsi6": rsi6,
        "rel_volume": rel_volume,
        "close_above_vwap": close_above_vwap,
        "close_above_orh": close_above_orh,
        "close_above_ma60": bool(math.isfinite(ma60) and close >= ma60),
    }


def _find_entry(
    *,
    variant: ChaseVariant,
    day: pd.DataFrame,
    full_symbol_frame: pd.DataFrame,
    trade_date: str,
) -> tuple[pd.Timestamp, dict[str, float | bool]] | None:
    rth = day[(day["time_et"] >= time(9, 30)) & (day["time_et"] <= time(12, 0))]
    if len(rth) < 100:
        return None
    or_window, or_high, or_low = _opening_range(rth)
    if len(or_window) < 5 or not math.isfinite(or_high) or not math.isfinite(or_low):
        return None
    base_features = _base_features(
        full_symbol_frame=full_symbol_frame,
        day=day,
        rth=rth,
        trade_date=trade_date,
        or_high=or_high,
        or_low=or_low,
    )
    if not _passes_static_filters(variant, base_features):
        return None

    candidates = rth[(rth["time_et"] >= variant.entry_start) & (rth["time_et"] <= variant.entry_end)]
    if candidates.empty:
        return None
    for ts, row in candidates.iterrows():
        prev_window = rth[rth.index < ts]
        previous_row = prev_window.iloc[-1] if not prev_window.empty else None
        if variant.entry_style == "pullback_reclaim":
            prior_after_or = rth[(rth.index < ts) & (rth["time_et"] >= time(9, 36))]
            if prior_after_or.empty:
                continue
            prior_break_pct = _pct(_safe_ratio(_safe_float(prior_after_or["close"].max()) - or_high, or_high))
            if not math.isfinite(prior_break_pct) or prior_break_pct < variant.min_or_break_pct:
                continue
        features = _candidate_features(
            variant=variant,
            rth=rth,
            ts=ts,
            row=row,
            previous_row=previous_row,
            or_high=or_high,
        )
        if features is None:
            continue
        merged = {
            **base_features,
            **features,
            "opening_range_high": or_high,
            "opening_range_low": or_low,
        }
        return ts, merged
    return None


def _simulate_exit(
    day: pd.DataFrame,
    entry_ts: pd.Timestamp,
    *,
    opening_range_high: float,
    opening_range_low: float,
    deadline: time,
    target_pct: float,
    hard_stop_buffer_pct: float,
    protect_trigger_pct: float,
    protect_floor_pct: float,
    no_new_high_minutes: int,
    no_profit_exit_minutes: int,
    no_profit_min_mfe_pct: float,
    trend_confirm_time: time,
    trend_confirm_mfe_pct: float,
) -> tuple[pd.Timestamp, str]:
    entry = day.loc[entry_ts]
    entry_price = _safe_float(entry.get("close"))
    entry_low = _safe_float(entry.get("low"))
    entry_high = _safe_float(entry.get("high"))
    if not math.isfinite(entry_price) or entry_price <= 0:
        return entry_ts, "DATA_BAD_ENTRY"
    if not math.isfinite(entry_low):
        entry_low = entry_price
    if not math.isfinite(entry_high):
        entry_high = entry_price
    end_ts = _last_available_ts(day, _deadline_ts(entry_ts, deadline))
    if end_ts is None:
        return entry_ts, "DATA_NO_DEADLINE"

    window = day[(day.index >= entry_ts) & (day.index <= end_ts)]
    hard_stop = entry_low * (1 - hard_stop_buffer_pct / 100.0)
    max_unrealized = 0.0
    high_since_entry = entry_high
    below_structure_count = 0
    trend_checked = False

    for ts, row in window.iloc[1:].iterrows():
        high = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        close = _safe_float(row.get("close"))
        vwap = _safe_float(row.get("VWAP"))
        if math.isfinite(high):
            high_since_entry = max(high_since_entry, high)
            max_unrealized = max(max_unrealized, _safe_ratio(high - entry_price, entry_price))
        if math.isfinite(low) and low <= hard_stop:
            return ts, "HARD_STOP_ENTRY_LOW"
        if math.isfinite(opening_range_low) and math.isfinite(close) and close < opening_range_low:
            return ts, "FAILURE_BELOW_ORL"
        if not math.isfinite(close):
            continue

        current = _safe_ratio(close - entry_price, entry_price)
        elapsed_minutes = max(0.0, (ts - entry_ts).total_seconds() / 60.0)
        if current >= target_pct / 100.0:
            return ts, "TARGET_RETURN"
        if math.isfinite(vwap) and close < vwap and current < 0:
            return ts, "FAILURE_BELOW_VWAP_NEG"
        if max_unrealized >= protect_trigger_pct / 100.0 and current <= protect_floor_pct / 100.0:
            return ts, "PROTECT_FLOOR"

        structure_level = opening_range_high
        if math.isfinite(vwap):
            structure_level = max(structure_level, vwap)
        if max_unrealized >= protect_trigger_pct / 100.0 and math.isfinite(structure_level):
            if close < structure_level:
                below_structure_count += 1
            else:
                below_structure_count = 0
            if below_structure_count >= 2:
                return ts, "STRUCTURE_LOST_2BAR"

        if (
            elapsed_minutes >= no_new_high_minutes
            and high_since_entry <= entry_high
            and current <= 0
        ):
            return ts, "FAILURE_NO_FOLLOW_THROUGH"
        if (
            elapsed_minutes >= no_profit_exit_minutes
            and current <= 0
            and max_unrealized < no_profit_min_mfe_pct / 100.0
        ):
            return ts, "TIME_STOP_NO_PROFIT"

        current_et_time = ts.tz_convert(EASTERN).time()
        if not trend_checked and current_et_time >= trend_confirm_time:
            trend_checked = True
            if max_unrealized < trend_confirm_mfe_pct / 100.0 and close < opening_range_high:
                return ts, "TIME_STOP_NO_TREND_CONFIRM"

    return end_ts, "TIME_EXIT"


def _make_record(
    *,
    variant: ChaseVariant,
    symbol: str,
    trade_date: str,
    day: pd.DataFrame,
    entry_ts: pd.Timestamp,
    exit_ts: pd.Timestamp,
    exit_reason: str,
    features: Mapping[str, float | bool],
) -> ChaseTradeRecord | None:
    entry = day.loc[entry_ts]
    exit_bar = day.loc[exit_ts]
    entry_price = _safe_float(entry.get("close"))
    exit_price = _safe_float(exit_bar.get("close"))
    if not math.isfinite(entry_price) or entry_price <= 0 or not math.isfinite(exit_price):
        return None
    window = day[(day.index >= entry_ts) & (day.index <= exit_ts)]
    mfe = _safe_ratio(_safe_float(window["high"].max()) - entry_price, entry_price)
    mae = _safe_ratio(_safe_float(window["low"].min()) - entry_price, entry_price)
    return_frac = _safe_ratio(exit_price - entry_price, entry_price)
    return ChaseTradeRecord(
        version_name=variant.name,
        symbol=symbol,
        trade_date=trade_date,
        entry_time=_et_str(entry_ts),
        exit_time=_et_str(exit_ts),
        entry_ts_utc=entry_ts.isoformat(),
        exit_ts_utc=exit_ts.isoformat(),
        entry_price=entry_price,
        exit_price=exit_price,
        return_pct=_pct(return_frac),
        MFE=_pct(mfe),
        MAE=_pct(mae),
        holding_minutes=max(0.0, (exit_ts - entry_ts).total_seconds() / 60.0),
        exit_reason=exit_reason,
        entry_style=variant.entry_style,
        gap_pct=float(features.get("gap_pct", math.nan)),
        opening_range_move_pct=float(features.get("opening_range_move_pct", math.nan)),
        first_range_pct=float(features.get("first_range_pct", math.nan)),
        or_break_pct=float(features.get("or_break_pct", math.nan)),
        reclaim_dist_pct=float(features.get("reclaim_dist_pct", math.nan)),
        vwap_dist_pct=float(features.get("vwap_dist_pct", math.nan)),
        close_position=float(features.get("close_position", math.nan)),
        upper_shadow_ratio=float(features.get("upper_shadow_ratio", math.nan)),
        rsi6=float(features.get("rsi6", math.nan)),
        rel_volume=float(features.get("rel_volume", math.nan)),
        close_above_vwap=bool(features.get("close_above_vwap", False)),
        close_above_orh=bool(features.get("close_above_orh", False)),
        close_above_ma60=bool(features.get("close_above_ma60", False)),
    )


def _scan_symbol(
    *,
    symbol: str,
    frame: pd.DataFrame,
    variants: Sequence[ChaseVariant] | None = None,
    scan_start_date: date | None = None,
    scan_end_date: date | None = None,
    deadline: time,
    target_pct: float,
    hard_stop_buffer_pct: float,
    protect_trigger_pct: float,
    protect_floor_pct: float,
    no_new_high_minutes: int,
    no_profit_exit_minutes: int,
    no_profit_min_mfe_pct: float,
    trend_confirm_time: time,
    trend_confirm_mfe_pct: float,
) -> list[ChaseTradeRecord]:
    records: list[ChaseTradeRecord] = []
    selected_variants = list(variants) if variants is not None else VARIANTS
    for trade_date, day in _daily_groups(frame):
        current_date = _parse_ymd(str(trade_date))
        if scan_start_date is not None and current_date < scan_start_date:
            continue
        if scan_end_date is not None and current_date > scan_end_date:
            continue
        rth = day[(day["time_et"] >= time(9, 30)) & (day["time_et"] <= time(12, 0))]
        if len(rth) < 100:
            continue
        for variant in selected_variants:
            found = _find_entry(
                variant=variant,
                day=day,
                full_symbol_frame=frame,
                trade_date=str(trade_date),
            )
            if found is None:
                continue
            entry_ts, features = found
            exit_ts, exit_reason = _simulate_exit(
                day,
                entry_ts,
                opening_range_high=float(features.get("opening_range_high", math.nan)),
                opening_range_low=float(features.get("opening_range_low", math.nan)),
                deadline=deadline,
                target_pct=target_pct,
                hard_stop_buffer_pct=hard_stop_buffer_pct,
                protect_trigger_pct=protect_trigger_pct,
                protect_floor_pct=protect_floor_pct,
                no_new_high_minutes=no_new_high_minutes,
                no_profit_exit_minutes=no_profit_exit_minutes,
                no_profit_min_mfe_pct=no_profit_min_mfe_pct,
                trend_confirm_time=trend_confirm_time,
                trend_confirm_mfe_pct=trend_confirm_mfe_pct,
            )
            record = _make_record(
                variant=variant,
                symbol=symbol,
                trade_date=str(trade_date),
                day=day,
                entry_ts=entry_ts,
                exit_ts=exit_ts,
                exit_reason=exit_reason,
                features=features,
            )
            if record is not None:
                records.append(record)
    return records


def _summarize(records: Sequence[ChaseTradeRecord]) -> list[dict[str, object]]:
    groups: dict[str, list[ChaseTradeRecord]] = {}
    for record in records:
        groups.setdefault(record.version_name, []).append(record)
    rows: list[dict[str, object]] = []
    for version, items in sorted(groups.items()):
        returns = [r.return_pct for r in items]
        capture = [r.return_pct / r.MFE for r in items if math.isfinite(r.MFE) and r.MFE > 0]
        rows.append(
            {
                "version_name": version,
                "trade_count": len(items),
                "win_rate": sum(1 for value in returns if value > 0) / len(items),
                "avg_return": sum(returns) / len(items),
                "median_return": median(returns),
                "total_return": sum(returns),
                "max_loss": min(returns),
                "max_win": max(returns),
                "avg_MFE": sum(r.MFE for r in items) / len(items),
                "avg_MAE": sum(r.MAE for r in items) / len(items),
                "MFE_capture_ratio": sum(capture) / len(capture) if capture else math.nan,
                "avg_holding_minutes": sum(r.holding_minutes for r in items) / len(items),
            }
        )
    return rows


def _summary_by_symbol(records: Sequence[ChaseTradeRecord]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[ChaseTradeRecord]] = {}
    for record in records:
        groups.setdefault((record.version_name, record.symbol), []).append(record)
    rows: list[dict[str, object]] = []
    for (version, symbol), items in sorted(groups.items()):
        returns = [r.return_pct for r in items]
        rows.append(
            {
                "version_name": version,
                "symbol": symbol,
                "trade_count": len(items),
                "win_rate": sum(1 for value in returns if value > 0) / len(items),
                "avg_return": sum(returns) / len(items),
                "total_return": sum(returns),
                "max_loss": min(returns),
                "max_win": max(returns),
            }
        )
    return rows


def _exit_summary(records: Sequence[ChaseTradeRecord]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[ChaseTradeRecord]] = {}
    for record in records:
        groups.setdefault((record.version_name, record.exit_reason), []).append(record)
    rows: list[dict[str, object]] = []
    for (version, reason), items in sorted(groups.items()):
        rows.append(
            {
                "version_name": version,
                "exit_reason": reason,
                "trade_count": len(items),
                "avg_return": sum(r.return_pct for r in items) / len(items),
                "avg_holding_minutes": sum(r.holding_minutes for r in items) / len(items),
            }
        )
    return rows


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_report(
    path: Path,
    *,
    runs: Sequence[RunInfo],
    records: Sequence[ChaseTradeRecord],
    summary_rows: Sequence[Mapping[str, object]],
    symbol_rows: Sequence[Mapping[str, object]],
    exit_rows: Sequence[Mapping[str, object]],
    coverage_rows: Sequence[Mapping[str, object]],
    output_dir: Path,
    args: argparse.Namespace,
) -> None:
    lines = [
        "# Opening Chase Lifecycle Research Report",
        "",
        f"Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "## Scope",
        "",
        f"- Source baseline batch: `{runs[0].batch_id}`",
        f"- Symbols: `{', '.join(run.symbol for run in runs)}`",
        f"- Output directory: `{output_dir}`",
        "- Equity-only replay; option PnL is not estimated here.",
        "",
        "## Version Summary",
        "",
        _markdown_table(
            summary_rows,
            [
                "version_name",
                "trade_count",
                "win_rate",
                "avg_return",
                "median_return",
                "total_return",
                "max_loss",
                "max_win",
                "avg_MFE",
                "avg_MAE",
                "MFE_capture_ratio",
                "avg_holding_minutes",
            ],
        ),
        "",
        "## Symbol Summary",
        "",
        _markdown_table(
            symbol_rows,
            ["version_name", "symbol", "trade_count", "win_rate", "avg_return", "total_return", "max_loss", "max_win"],
        ),
        "",
        "## Exit Summary",
        "",
        _markdown_table(
            exit_rows,
            ["version_name", "exit_reason", "trade_count", "avg_return", "avg_holding_minutes"],
        ),
        "",
        "## Data Coverage",
        "",
        _markdown_table(coverage_rows, ["symbol", "bar_count", "first_ts", "last_ts", "trade_dates"]),
        "",
        "## Parameters",
        "",
        f"- deadline: `{args.deadline} ET`",
        f"- target_pct: `{args.target_pct:.2f}%`",
        f"- hard_stop_buffer_pct: `{args.hard_stop_buffer_pct:.2f}%`",
        f"- protect_trigger_pct: `{args.protect_trigger_pct:.2f}%`",
        f"- protect_floor_pct: `{args.protect_floor_pct:.2f}%`",
        f"- no_new_high_minutes: `{args.no_new_high_minutes}`",
        f"- no_profit_exit_minutes: `{args.no_profit_exit_minutes}`",
        f"- no_profit_min_mfe_pct: `{args.no_profit_min_mfe_pct:.2f}%`",
        f"- trend_confirm_time: `{args.trend_confirm_time} ET`",
        f"- trend_confirm_mfe_pct: `{args.trend_confirm_mfe_pct:.2f}%`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path("research_notes") / f"opening_chase_lifecycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)
    settings = get_settings()
    engine = create_engine(settings.database_url)
    deadline = _parse_hhmm(args.deadline)
    trend_confirm_time = _parse_hhmm(args.trend_confirm_time)
    selected_variants = VARIANTS
    if args.versions:
        wanted = {name.strip() for name in args.versions if name.strip()}
        selected_variants = [variant for variant in VARIANTS if variant.name in wanted]
        if not selected_variants:
            raise SystemExit("No variants matched --versions.")

    all_records: list[ChaseTradeRecord] = []
    coverage_rows: list[dict[str, object]] = []
    with engine.connect() as conn:
        direct_mode = bool(args.start and args.end)
        scan_start_date = None
        scan_end_date = None
        if direct_mode:
            scan_start_date = _parse_ymd(args.start)
            scan_end_date = _parse_ymd(args.end)
            symbols = [symbol.upper() for symbol in (args.symbols or DEFAULT_DIRECT_SYMBOLS)]
            batch_id = f"direct-{scan_start_date.isoformat()}-{scan_end_date.isoformat()}"
            runs = [
                RunInfo(
                    run_id=index + 1,
                    symbol=symbol,
                    batch_id=batch_id,
                    start=pd.Timestamp(
                        datetime.combine(scan_start_date - timedelta(days=7), datetime.min.time(), tzinfo=EASTERN)
                    ).tz_convert("UTC").to_pydatetime(),
                    end=pd.Timestamp(
                        datetime.combine(scan_end_date + timedelta(days=1), datetime.min.time(), tzinfo=EASTERN)
                    ).tz_convert("UTC").to_pydatetime(),
                )
                for index, symbol in enumerate(symbols)
            ]
        else:
            runs = _load_runs(conn, batch_id=args.batch_id, run_ids=args.run_ids)
            if args.symbols:
                allowed = {symbol.upper() for symbol in args.symbols}
                runs = [run for run in runs if run.symbol in allowed]
            if not runs:
                raise SystemExit("No runs left after symbol filtering.")
        for run in runs:
            frame = _load_bars(
                conn,
                symbol=run.symbol,
                start=run.start - timedelta(days=7),
                end=run.end + timedelta(hours=4),
            )
            coverage_rows.append(
                {
                    "symbol": run.symbol,
                    "bar_count": len(frame),
                    "first_ts": frame.index[0].isoformat() if not frame.empty else "",
                    "last_ts": frame.index[-1].isoformat() if not frame.empty else "",
                    "trade_dates": frame["trade_date_et"].nunique() if not frame.empty else 0,
                }
            )
            if frame.empty:
                continue
            all_records.extend(
                _scan_symbol(
                    symbol=run.symbol,
                    frame=frame,
                    variants=selected_variants,
                    scan_start_date=scan_start_date,
                    scan_end_date=scan_end_date,
                    deadline=deadline,
                    target_pct=args.target_pct,
                    hard_stop_buffer_pct=args.hard_stop_buffer_pct,
                    protect_trigger_pct=args.protect_trigger_pct,
                    protect_floor_pct=args.protect_floor_pct,
                    no_new_high_minutes=args.no_new_high_minutes,
                    no_profit_exit_minutes=args.no_profit_exit_minutes,
                    no_profit_min_mfe_pct=args.no_profit_min_mfe_pct,
                    trend_confirm_time=trend_confirm_time,
                    trend_confirm_mfe_pct=args.trend_confirm_mfe_pct,
                )
            )

    trade_fields = list(ChaseTradeRecord.__dataclass_fields__.keys())
    trade_rows = [dict(record.__dict__) for record in all_records]
    summary_rows = _summarize(all_records)
    symbol_rows = _summary_by_symbol(all_records)
    exit_rows = _exit_summary(all_records)
    _write_csv(output_dir / "trade_level.csv", trade_rows, trade_fields)
    _write_csv(
        output_dir / "summary_by_version.csv",
        summary_rows,
        [
            "version_name",
            "trade_count",
            "win_rate",
            "avg_return",
            "median_return",
            "total_return",
            "max_loss",
            "max_win",
            "avg_MFE",
            "avg_MAE",
            "MFE_capture_ratio",
            "avg_holding_minutes",
        ],
    )
    _write_csv(
        output_dir / "summary_by_symbol.csv",
        symbol_rows,
        ["version_name", "symbol", "trade_count", "win_rate", "avg_return", "total_return", "max_loss", "max_win"],
    )
    _write_csv(
        output_dir / "exit_summary.csv",
        exit_rows,
        ["version_name", "exit_reason", "trade_count", "avg_return", "avg_holding_minutes"],
    )
    _write_csv(
        output_dir / "coverage.csv",
        coverage_rows,
        ["symbol", "bar_count", "first_ts", "last_ts", "trade_dates"],
    )
    _write_report(
        output_dir / "report.md",
        runs=runs,
        records=all_records,
        summary_rows=summary_rows,
        symbol_rows=symbol_rows,
        exit_rows=exit_rows,
        coverage_rows=coverage_rows,
        output_dir=output_dir,
        args=args,
    )
    print(f"wrote {output_dir}")
    print(f"trades={len(all_records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
