#!/usr/bin/env python3
"""Offline research scanner for opening momentum continuation trades.

This is intentionally read-only. It scans equity 1m bars for strong-opening
continuation setups and writes CSV/Markdown reports for comparison with v8
1030 reversal research.
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
from sqlalchemy import bindparam, create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from libs.core import EASTERN
from libs.core.config import get_settings


BASELINE_VERSION = "v8_guard_rsi_floor"


@dataclass(frozen=True)
class RunInfo:
    run_id: int
    symbol: str
    batch_id: str | None
    start: datetime
    end: datetime


@dataclass(frozen=True)
class Variant:
    name: str
    min_gap_pct: float
    min_0930_1000_ret_pct: float
    min_0930_1000_high_pct: float
    max_pullback_from_high_pct: float
    min_entry_break_pct: float
    require_volume_expansion: bool
    require_morning_high_break: bool
    max_upper_shadow_ratio: float
    min_vwap_dist_pct: float = 0.0
    min_rel_volume: float = 1.05
    min_rsi6: float | None = None
    max_rsi6: float | None = None
    symbols: tuple[str, ...] | None = None


@dataclass(frozen=True)
class TradeRecord:
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
    gap_pct: float
    morning_ret_pct: float
    morning_high_pct: float
    pullback_from_high_pct: float
    entry_break_pct: float
    vwap_dist_pct: float
    boll_pos: float
    upper_shadow_ratio: float
    rsi6: float
    rel_volume: float
    morning_high_break: bool
    close_above_vwap: bool
    close_above_boll_mid: bool


VARIANTS = [
    Variant(
        name="opening_momentum_loose",
        min_gap_pct=0.30,
        min_0930_1000_ret_pct=0.35,
        min_0930_1000_high_pct=0.80,
        max_pullback_from_high_pct=1.80,
        min_entry_break_pct=0.03,
        require_volume_expansion=False,
        require_morning_high_break=False,
        max_upper_shadow_ratio=0.65,
        max_rsi6=82.0,
    ),
    Variant(
        name="opening_momentum_quality",
        min_gap_pct=0.50,
        min_0930_1000_ret_pct=0.50,
        min_0930_1000_high_pct=1.00,
        max_pullback_from_high_pct=1.40,
        min_entry_break_pct=0.05,
        require_volume_expansion=True,
        require_morning_high_break=False,
        max_upper_shadow_ratio=0.50,
        min_rsi6=50.0,
        max_rsi6=78.0,
    ),
    Variant(
        name="opening_momentum_breakout",
        min_gap_pct=0.50,
        min_0930_1000_ret_pct=0.60,
        min_0930_1000_high_pct=1.20,
        max_pullback_from_high_pct=1.20,
        min_entry_break_pct=0.05,
        require_volume_expansion=True,
        require_morning_high_break=True,
        max_upper_shadow_ratio=0.45,
        min_rsi6=55.0,
        max_rsi6=80.0,
    ),
    Variant(
        name="opening_momentum_breakout_v2",
        min_gap_pct=0.0,
        min_0930_1000_ret_pct=1.50,
        min_0930_1000_high_pct=1.80,
        max_pullback_from_high_pct=0.70,
        min_entry_break_pct=0.12,
        require_volume_expansion=True,
        require_morning_high_break=True,
        max_upper_shadow_ratio=0.08,
        min_vwap_dist_pct=0.90,
        min_rel_volume=1.30,
        min_rsi6=65.0,
        max_rsi6=80.0,
    ),
    Variant(
        name="opening_momentum_breakout_v3_strict",
        min_gap_pct=0.0,
        min_0930_1000_ret_pct=1.80,
        min_0930_1000_high_pct=2.00,
        max_pullback_from_high_pct=0.65,
        min_entry_break_pct=0.15,
        require_volume_expansion=True,
        require_morning_high_break=True,
        max_upper_shadow_ratio=0.06,
        min_vwap_dist_pct=1.20,
        min_rel_volume=1.35,
        min_rsi6=65.0,
        max_rsi6=80.0,
    ),
    Variant(
        name="opening_momentum_tqqq_breakout_v2",
        min_gap_pct=0.0,
        min_0930_1000_ret_pct=1.20,
        min_0930_1000_high_pct=1.50,
        max_pullback_from_high_pct=0.80,
        min_entry_break_pct=0.10,
        require_volume_expansion=True,
        require_morning_high_break=True,
        max_upper_shadow_ratio=0.08,
        min_vwap_dist_pct=0.80,
        min_rel_volume=1.25,
        min_rsi6=65.0,
        max_rsi6=80.0,
        symbols=("TQQQ",),
    ),
]


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan opening momentum continuation variants.")
    parser.add_argument("--batch-id", default=None, help="Baseline batch id to infer scope.")
    parser.add_argument("--run-ids", nargs="*", type=int, default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--deadline", default="12:00")
    parser.add_argument("--target-pct", type=float, default=1.00)
    parser.add_argument("--protect-trigger-pct", type=float, default=0.55)
    parser.add_argument("--protect-floor-pct", type=float, default=0.20)
    parser.add_argument("--hard-stop-buffer-pct", type=float, default=0.10)
    return parser.parse_args(list(argv) if argv is not None else None)


def _utc_timestamp(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _safe_float(value: object) -> float:
    if value is None:
        return math.nan
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    return result


def _safe_ratio(numerator: float, denominator: float) -> float:
    if not math.isfinite(numerator) or not math.isfinite(denominator) or denominator == 0:
        return math.nan
    return numerator / denominator


def _pct(value: float) -> float:
    return value * 100 if math.isfinite(value) else math.nan


def _parse_hhmm(raw: str) -> time:
    hour, minute = raw.split(":", 1)
    return time(int(hour), int(minute))


def _et_str(ts: pd.Timestamp) -> str:
    return _utc_timestamp(ts).tz_convert(EASTERN).strftime("%Y-%m-%d %H:%M")


def _latest_baseline_batch(conn) -> str:
    row = conn.execute(
        text(
            """
            SELECT parameters->>'batch_id' AS batch_id
            FROM bt_runs
            WHERE strategy_version = :version
              AND status = 'COMPLETED'
              AND parameters ? 'batch_id'
            GROUP BY parameters->>'batch_id'
            ORDER BY MAX(run_id) DESC
            LIMIT 1
            """
        ),
        {"version": BASELINE_VERSION},
    ).fetchone()
    if row is None or not row.batch_id:
        raise SystemExit(f"No completed {BASELINE_VERSION} batch found.")
    return str(row.batch_id)


def _load_runs(conn, *, batch_id: str | None, run_ids: Sequence[int] | None) -> list[RunInfo]:
    if run_ids:
        stmt = (
            text(
                """
                SELECT run_id, data_window_start, data_window_end, parameters
                FROM bt_runs
                WHERE run_id IN :run_ids
                ORDER BY run_id
                """
            )
            .bindparams(bindparam("run_ids", expanding=True))
        )
        rows = conn.execute(stmt, {"run_ids": list(run_ids)}).mappings().all()
    else:
        if not batch_id:
            batch_id = _latest_baseline_batch(conn)
        rows = conn.execute(
            text(
                """
                SELECT run_id, data_window_start, data_window_end, parameters
                FROM bt_runs
                WHERE parameters->>'batch_id' = :batch_id
                  AND strategy_version = :version
                  AND status = 'COMPLETED'
                ORDER BY run_id
                """
            ),
            {"batch_id": batch_id, "version": BASELINE_VERSION},
        ).mappings().all()
    runs: list[RunInfo] = []
    for row in rows:
        params = dict(row["parameters"] or {})
        symbol = str(params.get("symbol") or "").upper()
        if not symbol:
            continue
        runs.append(
            RunInfo(
                run_id=int(row["run_id"]),
                symbol=symbol,
                batch_id=params.get("batch_id"),
                start=_utc_timestamp(row["data_window_start"]).to_pydatetime(),
                end=_utc_timestamp(row["data_window_end"]).to_pydatetime(),
            )
        )
    if not runs:
        raise SystemExit("No baseline runs matched the requested scope.")
    return runs


def _load_bars(conn, *, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    rows = conn.execute(
        text(
            """
            SELECT
                b.ts_end,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                i.boll_mid,
                i.boll_up,
                i.boll_dn,
                i.rsi6,
                ma.sma60 AS ma60
            FROM bars1m_equity b
            LEFT JOIN indicators_eq_1m i
              ON b.symbol = i.symbol AND b.ts_end = i.ts_end
            LEFT JOIN LATERAL (
                SELECT sma60
                FROM v_daily_ma60
                WHERE symbol = b.symbol
                  AND trade_date_et <= (b.ts_end AT TIME ZONE 'America/New_York')::date
                  AND sma60 IS NOT NULL
                ORDER BY trade_date_et DESC
                LIMIT 1
            ) ma ON true
            WHERE b.symbol = :symbol
              AND b.ts_end >= :start_ts
              AND b.ts_end <= :end_ts
            ORDER BY b.ts_end
            """
        ),
        {"symbol": symbol, "start_ts": start, "end_ts": end},
    ).mappings().all()
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["ts_end"] = pd.to_datetime(frame["ts_end"], utc=True)
    for column in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "boll_mid",
        "boll_up",
        "boll_dn",
        "rsi6",
        "ma60",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.set_index("ts_end").sort_index()
    return _with_session_vwap(frame)


def _with_session_vwap(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    frame = frame.copy()
    et_index = frame.index.tz_convert(EASTERN)
    frame["trade_date_et"] = [ts.date().isoformat() for ts in et_index]
    frame["time_et"] = [ts.time() for ts in et_index]
    frame["VWAP"] = math.nan
    for _, idx in frame.groupby("trade_date_et").groups.items():
        cumulative_notional = 0.0
        cumulative_volume = 0.0
        for ts in frame.loc[idx].index:
            current_time = ts.tz_convert(EASTERN).time()
            if current_time < time(9, 30) or current_time > time(16, 0):
                continue
            close = _safe_float(frame.at[ts, "close"])
            volume = _safe_float(frame.at[ts, "volume"])
            if math.isfinite(close) and math.isfinite(volume) and volume > 0:
                cumulative_notional += close * volume
                cumulative_volume += volume
            if cumulative_volume > 0:
                frame.at[ts, "VWAP"] = cumulative_notional / cumulative_volume
    return frame


def _daily_groups(frame: pd.DataFrame) -> Iterable[tuple[str, pd.DataFrame]]:
    if frame.empty:
        return []
    return frame.groupby("trade_date_et")


def _prev_rth_close(frame: pd.DataFrame, trade_date: str) -> float:
    prior = frame[frame["trade_date_et"] < trade_date]
    prior = prior[(prior["time_et"] >= time(9, 30)) & (prior["time_et"] <= time(16, 0))]
    if prior.empty:
        return math.nan
    return _safe_float(prior.iloc[-1]["close"])


def _bar_shadow_ratio(row: pd.Series) -> float:
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    close = _safe_float(row.get("close"))
    open_ = _safe_float(row.get("open"))
    rng = high - low
    if not math.isfinite(rng) or rng <= 0:
        return math.nan
    return max(0.0, high - max(open_, close)) / rng


def _boll_pos(row: pd.Series) -> float:
    close = _safe_float(row.get("close"))
    up = _safe_float(row.get("boll_up"))
    dn = _safe_float(row.get("boll_dn"))
    return _safe_ratio(close - dn, up - dn)


def _deadline_ts(entry_ts: pd.Timestamp, deadline: time) -> pd.Timestamp:
    entry_et = entry_ts.tz_convert(EASTERN)
    deadline_et = datetime.combine(entry_et.date(), deadline, tzinfo=EASTERN)
    return pd.Timestamp(deadline_et).tz_convert("UTC")


def _last_available_ts(bars: pd.DataFrame, desired: pd.Timestamp) -> pd.Timestamp | None:
    subset = bars[bars.index <= desired]
    if subset.empty:
        return None
    return subset.index[-1]


def _find_entry(
    *,
    variant: Variant,
    symbol: str,
    day: pd.DataFrame,
    full_symbol_frame: pd.DataFrame,
    trade_date: str,
) -> tuple[pd.Timestamp, dict[str, float | bool]] | None:
    if variant.symbols is not None and symbol.upper() not in variant.symbols:
        return None
    rth = day[(day["time_et"] >= time(9, 30)) & (day["time_et"] <= time(12, 0))]
    if rth.empty:
        return None
    early = rth[(rth["time_et"] >= time(9, 30)) & (rth["time_et"] <= time(10, 0))]
    if len(early) < 20:
        return None
    pullback = rth[(rth["time_et"] > time(10, 0)) & (rth["time_et"] <= time(10, 30))]
    candidates = rth[(rth["time_et"] >= time(10, 25)) & (rth["time_et"] <= time(10, 45))]
    if pullback.empty or candidates.empty:
        return None

    open_price = _safe_float(early.iloc[0]["open"])
    close_1000 = _safe_float(early.iloc[-1]["close"])
    high_1000 = _safe_float(early["high"].max())
    prev_close = _prev_rth_close(full_symbol_frame, trade_date)
    if not math.isfinite(open_price) or open_price <= 0:
        return None

    gap_pct = _pct(_safe_ratio(open_price - prev_close, prev_close))
    morning_ret_pct = _pct(_safe_ratio(close_1000 - open_price, open_price))
    morning_high_pct = _pct(_safe_ratio(high_1000 - open_price, open_price))
    if max(gap_pct, morning_ret_pct) < variant.min_gap_pct:
        return None
    if morning_ret_pct < variant.min_0930_1000_ret_pct:
        return None
    if morning_high_pct < variant.min_0930_1000_high_pct:
        return None

    pullback_low = _safe_float(pullback["low"].min())
    pullback_from_high_pct = _pct(_safe_ratio(high_1000 - pullback_low, high_1000))
    if not math.isfinite(pullback_from_high_pct) or pullback_from_high_pct > variant.max_pullback_from_high_pct:
        return None

    for ts, row in candidates.iterrows():
        close = _safe_float(row.get("close"))
        high = _safe_float(row.get("high"))
        vwap = _safe_float(row.get("VWAP"))
        boll_mid = _safe_float(row.get("boll_mid"))
        rsi6 = _safe_float(row.get("rsi6"))
        if not math.isfinite(close) or close <= 0:
            continue
        prev_window = rth[(rth.index < ts)].tail(10)
        if len(prev_window) < 5:
            continue
        prev_high = _safe_float(prev_window["high"].max())
        entry_break_pct = _pct(_safe_ratio(close - prev_high, prev_high))
        morning_high_break = bool(math.isfinite(high) and high >= high_1000)
        close_above_vwap = bool(math.isfinite(vwap) and close >= vwap)
        close_above_boll_mid = bool(math.isfinite(boll_mid) and close >= boll_mid)
        vwap_dist_pct = _pct(_safe_ratio(close - vwap, close))
        if entry_break_pct < variant.min_entry_break_pct:
            continue
        if variant.require_morning_high_break and not morning_high_break:
            continue
        if not close_above_vwap or not close_above_boll_mid:
            continue
        if not math.isfinite(vwap_dist_pct) or vwap_dist_pct < variant.min_vwap_dist_pct:
            continue
        if variant.min_rsi6 is not None and (not math.isfinite(rsi6) or rsi6 < variant.min_rsi6):
            continue
        if variant.max_rsi6 is not None and math.isfinite(rsi6) and rsi6 > variant.max_rsi6:
            continue
        upper_shadow_ratio = _bar_shadow_ratio(row)
        if math.isfinite(upper_shadow_ratio) and upper_shadow_ratio > variant.max_upper_shadow_ratio:
            continue
        recent_vol = _safe_float(prev_window["volume"].median())
        current_vol = _safe_float(row.get("volume"))
        rel_volume = _safe_ratio(current_vol, recent_vol)
        if variant.require_volume_expansion and (
            not math.isfinite(rel_volume) or rel_volume < variant.min_rel_volume
        ):
            continue
        features = {
            "gap_pct": gap_pct,
            "morning_ret_pct": morning_ret_pct,
            "morning_high_pct": morning_high_pct,
            "pullback_from_high_pct": pullback_from_high_pct,
            "entry_break_pct": entry_break_pct,
            "vwap_dist_pct": vwap_dist_pct,
            "boll_pos": _boll_pos(row),
            "upper_shadow_ratio": upper_shadow_ratio,
            "rsi6": rsi6,
            "rel_volume": rel_volume,
            "morning_high_break": morning_high_break,
            "close_above_vwap": close_above_vwap,
            "close_above_boll_mid": close_above_boll_mid,
        }
        return ts, features
    return None


def _simulate_exit(
    day: pd.DataFrame,
    entry_ts: pd.Timestamp,
    *,
    deadline: time,
    target_pct: float,
    protect_trigger_pct: float,
    protect_floor_pct: float,
    hard_stop_buffer_pct: float,
) -> tuple[pd.Timestamp, str]:
    entry = day.loc[entry_ts]
    entry_price = _safe_float(entry["close"])
    entry_low = _safe_float(entry["low"])
    if not math.isfinite(entry_price) or entry_price <= 0:
        return entry_ts, "DATA_BAD_ENTRY"
    if not math.isfinite(entry_low):
        entry_low = entry_price
    end_ts = _last_available_ts(day, _deadline_ts(entry_ts, deadline))
    if end_ts is None:
        return entry_ts, "DATA_NO_DEADLINE"
    window = day[(day.index >= entry_ts) & (day.index <= end_ts)]
    max_unrealized = 0.0
    below_structure_count = 0
    hard_stop = entry_low * (1 - hard_stop_buffer_pct / 100.0)
    for ts, row in window.iloc[1:].iterrows():
        high = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        close = _safe_float(row.get("close"))
        vwap = _safe_float(row.get("VWAP"))
        boll_mid = _safe_float(row.get("boll_mid"))
        if math.isfinite(high):
            max_unrealized = max(max_unrealized, _safe_ratio(high - entry_price, entry_price))
        if math.isfinite(low) and low <= hard_stop:
            return ts, "HARD_STOP_ENTRY_LOW"
        if math.isfinite(close):
            current = _safe_ratio(close - entry_price, entry_price)
            if current >= target_pct / 100.0:
                return ts, "TARGET_RETURN"
            if max_unrealized >= protect_trigger_pct / 100.0 and current <= protect_floor_pct / 100.0:
                return ts, "PROTECT_FLOOR"
            structure_level = math.nan
            if math.isfinite(vwap) and math.isfinite(boll_mid):
                structure_level = max(vwap, boll_mid)
            elif math.isfinite(vwap):
                structure_level = vwap
            elif math.isfinite(boll_mid):
                structure_level = boll_mid
            if math.isfinite(structure_level) and close < structure_level:
                below_structure_count += 1
            else:
                below_structure_count = 0
            if max_unrealized >= 0.0030 and below_structure_count >= 2:
                return ts, "STRUCTURE_LOST_2BAR"
    return end_ts, "TIME_EXIT"


def _make_record(
    *,
    variant_name: str,
    symbol: str,
    trade_date: str,
    day: pd.DataFrame,
    entry_ts: pd.Timestamp,
    exit_ts: pd.Timestamp,
    exit_reason: str,
    features: Mapping[str, float | bool],
) -> TradeRecord | None:
    entry = day.loc[entry_ts]
    exit_bar = day.loc[exit_ts]
    entry_price = _safe_float(entry["close"])
    exit_price = _safe_float(exit_bar["close"])
    if not math.isfinite(entry_price) or entry_price <= 0 or not math.isfinite(exit_price):
        return None
    window = day[(day.index >= entry_ts) & (day.index <= exit_ts)]
    mfe = _safe_ratio(_safe_float(window["high"].max()) - entry_price, entry_price)
    mae = _safe_ratio(_safe_float(window["low"].min()) - entry_price, entry_price)
    return_frac = _safe_ratio(exit_price - entry_price, entry_price)
    return TradeRecord(
        version_name=variant_name,
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
        gap_pct=float(features.get("gap_pct", math.nan)),
        morning_ret_pct=float(features.get("morning_ret_pct", math.nan)),
        morning_high_pct=float(features.get("morning_high_pct", math.nan)),
        pullback_from_high_pct=float(features.get("pullback_from_high_pct", math.nan)),
        entry_break_pct=float(features.get("entry_break_pct", math.nan)),
        vwap_dist_pct=float(features.get("vwap_dist_pct", math.nan)),
        boll_pos=float(features.get("boll_pos", math.nan)),
        upper_shadow_ratio=float(features.get("upper_shadow_ratio", math.nan)),
        rsi6=float(features.get("rsi6", math.nan)),
        rel_volume=float(features.get("rel_volume", math.nan)),
        morning_high_break=bool(features.get("morning_high_break", False)),
        close_above_vwap=bool(features.get("close_above_vwap", False)),
        close_above_boll_mid=bool(features.get("close_above_boll_mid", False)),
    )


def _scan_symbol(
    *,
    symbol: str,
    frame: pd.DataFrame,
    deadline: time,
    target_pct: float,
    protect_trigger_pct: float,
    protect_floor_pct: float,
    hard_stop_buffer_pct: float,
) -> list[TradeRecord]:
    records: list[TradeRecord] = []
    for trade_date, day in _daily_groups(frame):
        rth = day[(day["time_et"] >= time(9, 30)) & (day["time_et"] <= time(12, 0))]
        if len(rth) < 100:
            continue
        for variant in VARIANTS:
            found = _find_entry(
                variant=variant,
                symbol=symbol,
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
                deadline=deadline,
                target_pct=target_pct,
                protect_trigger_pct=protect_trigger_pct,
                protect_floor_pct=protect_floor_pct,
                hard_stop_buffer_pct=hard_stop_buffer_pct,
            )
            record = _make_record(
                variant_name=variant.name,
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


def _summarize(records: Sequence[TradeRecord]) -> list[dict[str, object]]:
    groups: dict[str, list[TradeRecord]] = {}
    for record in records:
        groups.setdefault(record.version_name, []).append(record)
    rows: list[dict[str, object]] = []
    for version, items in sorted(groups.items()):
        returns = [r.return_pct for r in items]
        ratios = [r.return_pct / r.MFE for r in items if math.isfinite(r.MFE) and r.MFE > 0]
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
                "MFE_capture_ratio": sum(ratios) / len(ratios) if ratios else math.nan,
                "avg_holding_minutes": sum(r.holding_minutes for r in items) / len(items),
            }
        )
    return rows


def _summary_by_symbol(records: Sequence[TradeRecord]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[TradeRecord]] = {}
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


def _exit_summary(records: Sequence[TradeRecord]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[TradeRecord]] = {}
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


def _fmt(value: object, digits: int = 4) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return ""
    return f"{number:.{digits}f}"


def _markdown_table(rows: Sequence[Mapping[str, object]], columns: Sequence[str]) -> str:
    if not rows:
        return "_No rows._"
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        values = []
        for column in columns:
            value = row.get(column, "")
            values.append(_fmt(value) if isinstance(value, float) else str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _write_report(
    path: Path,
    *,
    runs: Sequence[RunInfo],
    records: Sequence[TradeRecord],
    summary_rows: Sequence[Mapping[str, object]],
    symbol_rows: Sequence[Mapping[str, object]],
    exit_rows: Sequence[Mapping[str, object]],
    coverage_rows: Sequence[Mapping[str, object]],
    output_dir: Path,
    args: argparse.Namespace,
) -> None:
    lines = [
        "# Opening Momentum Research Report",
        "",
        f"Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "## Scope",
        "",
        f"- Source baseline batch: `{runs[0].batch_id}`",
        f"- Symbols: `{', '.join(run.symbol for run in runs)}`",
        f"- Output directory: `{output_dir}`",
        "- This is an equity-only offline scan; no synthetic option pricing is used.",
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
        f"- entry_window: `10:25-10:45 ET`",
        f"- deadline: `{args.deadline} ET`",
        f"- target_pct: `{args.target_pct:.2f}%`",
        f"- protect_trigger_pct: `{args.protect_trigger_pct:.2f}%`",
        f"- protect_floor_pct: `{args.protect_floor_pct:.2f}%`",
        f"- hard_stop_buffer_pct: `{args.hard_stop_buffer_pct:.2f}%`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path("research_notes") / f"opening_momentum_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)
    settings = get_settings()
    engine = create_engine(settings.database_url)
    deadline = _parse_hhmm(args.deadline)

    all_records: list[TradeRecord] = []
    coverage_rows: list[dict[str, object]] = []
    with engine.connect() as conn:
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
                    deadline=deadline,
                    target_pct=args.target_pct,
                    protect_trigger_pct=args.protect_trigger_pct,
                    protect_floor_pct=args.protect_floor_pct,
                    hard_stop_buffer_pct=args.hard_stop_buffer_pct,
                )
            )

    trade_fields = list(TradeRecord.__dataclass_fields__.keys())
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
