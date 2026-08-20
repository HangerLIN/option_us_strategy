#!/usr/bin/env python3
"""Generate trade-level research reports for v8_guard_rsi_floor experiments.

The script is intentionally read-only for backtest runs: it consumes an existing
baseline run set, reconstructs 1030 reversal trades from bt_signals, replays
independent research variants from equity bars, and writes CSV/Markdown outputs.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta
from pathlib import Path
from statistics import median
from typing import Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import bindparam, create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from libs.core import EASTERN
from libs.core.config import get_settings


BASELINE_VERSION = "v8_guard_rsi_floor"
ENTRY_CODE = "SIG_1030_REVERSAL_CALL_BUY"
EXIT_CODES = {
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
}

VERSION_BASELINE = BASELINE_VERSION
VERSION_DELAY_BOLL_UP = "v8_guard_exit_delay_boll_up"
VERSION_TARGET_PROTECT = "v8_guard_exit_target_protect"
VERSION_EARLY_PROOF = "v8_guard_early_proof_exit"
ROOM_VERSIONS = {
    "v8_guard_rsi_floor_core_room_020": 0.0020,
    "v8_guard_rsi_floor_core_room_025": 0.0025,
    "v8_guard_rsi_floor_core_room_030": 0.0030,
    "v8_guard_rsi_floor_core_room_035": 0.0035,
}


@dataclass(frozen=True)
class RunInfo:
    run_id: int
    symbol: str
    batch_id: str | None
    strategy_version: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class SignalPoint:
    run_id: int
    symbol: str
    ts: pd.Timestamp
    signal_code: str


@dataclass(frozen=True)
class BaselineTrade:
    source_run_id: int
    symbol: str
    entry_ts: pd.Timestamp
    exit_ts: pd.Timestamp
    entry_signal: str
    exit_signal: str


@dataclass(frozen=True)
class TradeRecord:
    version_name: str
    source_run_id: int
    symbol: str
    date: str
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
    trade_type: str
    is_continuation_candidate: bool
    is_option_core: bool
    is_scalp_thin_room: bool
    upper_room: float
    boll_pos: float
    vwap_dist: float
    VWAP: float
    boll_mid: float
    boll_up: float
    MA60: float
    RSI6: float
    entry_bar_low: float
    exit_reason: str
    baseline_exit_signal: str
    return_frac: float
    MFE_frac: float
    MAE_frac: float
    upper_room_frac: float
    vwap_dist_frac: float


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay v8_guard_rsi_floor trade tagging and exit experiments."
    )
    parser.add_argument("--batch-id", default=None, help="Baseline batch id to consume.")
    parser.add_argument("--run-ids", nargs="*", type=int, default=None, help="Explicit run ids.")
    parser.add_argument("--symbols", nargs="*", default=None, help="Optional symbol filter.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory. Defaults to research_notes/v8_guard_experiments_<timestamp>.",
    )
    parser.add_argument("--target-return-pct", type=float, default=0.50)
    parser.add_argument("--protect-trigger-pct", type=float, default=0.20)
    parser.add_argument("--protect-floor-pct", type=float, default=0.10)
    parser.add_argument("--early-proof-minutes", type=int, default=3)
    parser.add_argument("--early-proof-high-pct", type=float, default=0.10)
    parser.add_argument("--large-loss-threshold-pct", type=float, default=0.20)
    parser.add_argument("--deadline", default="12:00", help="ET deadline HH:MM.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _utc_timestamp(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _et_str(ts: pd.Timestamp) -> str:
    return _utc_timestamp(ts).tz_convert(EASTERN).strftime("%Y-%m-%d %H:%M")


def _date_str(ts: pd.Timestamp) -> str:
    return _utc_timestamp(ts).tz_convert(EASTERN).date().isoformat()


def _deadline_ts(entry_ts: pd.Timestamp, deadline: time) -> pd.Timestamp:
    entry_et = _utc_timestamp(entry_ts).tz_convert(EASTERN)
    deadline_et = datetime.combine(entry_et.date(), deadline, tzinfo=EASTERN)
    return pd.Timestamp(deadline_et).tz_convert("UTC")


def _parse_hhmm(raw: str) -> time:
    hour, minute = raw.split(":", 1)
    return time(int(hour), int(minute))


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
                SELECT run_id, strategy_version, data_window_start, data_window_end, parameters
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
                SELECT run_id, strategy_version, data_window_start, data_window_end, parameters
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
        runs.append(
            RunInfo(
                run_id=int(row["run_id"]),
                symbol=str(params.get("symbol") or "").upper(),
                batch_id=params.get("batch_id"),
                strategy_version=str(row["strategy_version"]),
                start=_utc_timestamp(row["data_window_start"]).to_pydatetime(),
                end=_utc_timestamp(row["data_window_end"]).to_pydatetime(),
            )
        )
    if not runs:
        raise SystemExit("No baseline runs matched the requested scope.")
    return runs


def _load_signals(conn, run_ids: Sequence[int]) -> list[SignalPoint]:
    stmt = (
        text(
            """
            SELECT run_id, symbol, ts_end, signal_code
            FROM bt_signals
            WHERE run_id IN :run_ids
              AND accepted = true
              AND (signal_code = :entry_code OR signal_code IN :exit_codes)
            ORDER BY run_id, symbol, ts_end, signal_code
            """
        )
        .bindparams(bindparam("run_ids", expanding=True), bindparam("exit_codes", expanding=True))
    )
    rows = conn.execute(
        stmt,
        {
            "run_ids": list(run_ids),
            "entry_code": ENTRY_CODE,
            "exit_codes": sorted(EXIT_CODES),
        },
    ).mappings().all()
    return [
        SignalPoint(
            run_id=int(row["run_id"]),
            symbol=str(row["symbol"]).upper(),
            ts=_utc_timestamp(row["ts_end"]),
            signal_code=str(row["signal_code"]),
        )
        for row in rows
    ]


def _pair_baseline_trades(signals: Sequence[SignalPoint]) -> list[BaselineTrade]:
    trades: list[BaselineTrade] = []
    open_entries: dict[tuple[int, str], SignalPoint] = {}
    for sig in signals:
        key = (sig.run_id, sig.symbol)
        if sig.signal_code == ENTRY_CODE:
            if key not in open_entries:
                open_entries[key] = sig
            continue
        if sig.signal_code in EXIT_CODES and key in open_entries:
            entry = open_entries.pop(key)
            if sig.ts > entry.ts:
                trades.append(
                    BaselineTrade(
                        source_run_id=entry.run_id,
                        symbol=entry.symbol,
                        entry_ts=entry.ts,
                        exit_ts=sig.ts,
                        entry_signal=entry.signal_code,
                        exit_signal=sig.signal_code,
                    )
                )
    return trades


def _load_bars(conn, *, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    rows = conn.execute(
        text(
            """
            SELECT
                b.ts_end,
                b.symbol,
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


def _bar_at(frame: pd.DataFrame, ts: pd.Timestamp) -> pd.Series | None:
    ts = _utc_timestamp(ts)
    if frame.empty or ts not in frame.index:
        return None
    return frame.loc[ts]


def _slice(frame: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if frame.empty:
        return frame
    start = _utc_timestamp(start)
    end = _utc_timestamp(end)
    return frame[(frame.index >= start) & (frame.index <= end)].copy()


def _entry_features(entry_bar: pd.Series) -> dict[str, float | bool | str]:
    close = _safe_float(entry_bar.get("close"))
    vwap = _safe_float(entry_bar.get("VWAP"))
    boll_mid = _safe_float(entry_bar.get("boll_mid"))
    boll_up = _safe_float(entry_bar.get("boll_up"))
    boll_dn = _safe_float(entry_bar.get("boll_dn"))
    upper_room_frac = _safe_ratio(boll_up - close, close)
    boll_pos = _safe_ratio(close - boll_dn, boll_up - boll_dn)
    vwap_dist_frac = _safe_ratio(close - vwap, close)

    is_scalp = bool(
        math.isfinite(close)
        and math.isfinite(vwap)
        and math.isfinite(boll_pos)
        and math.isfinite(upper_room_frac)
        and close >= vwap
        and boll_pos > 0.80
        and upper_room_frac < 0.0010
    )
    is_continuation = bool(
        math.isfinite(close)
        and math.isfinite(vwap)
        and math.isfinite(boll_mid)
        and math.isfinite(boll_pos)
        and close >= vwap
        and close >= boll_mid
        and boll_pos >= 0.60
    )
    is_option_core = bool(
        math.isfinite(upper_room_frac)
        and math.isfinite(boll_pos)
        and upper_room_frac >= 0.0020
        and boll_pos <= 0.80
    )
    if is_scalp:
        trade_type = "scalp_thin_room"
    elif is_continuation:
        trade_type = "continuation_candidate"
    elif is_option_core:
        trade_type = "option_core_reversal"
    else:
        trade_type = "uncategorized_reversal"
    return {
        "upper_room_frac": upper_room_frac,
        "boll_pos": boll_pos,
        "vwap_dist_frac": vwap_dist_frac,
        "is_scalp_thin_room": is_scalp,
        "is_continuation_candidate": is_continuation,
        "is_option_core": is_option_core,
        "trade_type": trade_type,
    }


def _make_trade_record(
    *,
    version_name: str,
    base: BaselineTrade,
    bars: pd.DataFrame,
    exit_ts: pd.Timestamp,
    exit_reason: str,
) -> TradeRecord | None:
    entry_bar = _bar_at(bars, base.entry_ts)
    exit_bar = _bar_at(bars, exit_ts)
    if entry_bar is None or exit_bar is None:
        return None
    entry_price = _safe_float(entry_bar.get("close"))
    exit_price = _safe_float(exit_bar.get("close"))
    if not math.isfinite(entry_price) or entry_price <= 0 or not math.isfinite(exit_price):
        return None
    window = _slice(bars, base.entry_ts, exit_ts)
    if window.empty:
        return None
    mfe_frac = _safe_ratio(float(window["high"].max()) - entry_price, entry_price)
    mae_frac = _safe_ratio(float(window["low"].min()) - entry_price, entry_price)
    return_frac = (exit_price - entry_price) / entry_price
    features = _entry_features(entry_bar)
    holding_minutes = max(0.0, (_utc_timestamp(exit_ts) - base.entry_ts).total_seconds() / 60.0)
    return TradeRecord(
        version_name=version_name,
        source_run_id=base.source_run_id,
        symbol=base.symbol,
        date=_date_str(base.entry_ts),
        trade_date=_date_str(base.entry_ts),
        entry_time=_et_str(base.entry_ts),
        exit_time=_et_str(exit_ts),
        entry_ts_utc=base.entry_ts.isoformat(),
        exit_ts_utc=_utc_timestamp(exit_ts).isoformat(),
        entry_price=entry_price,
        exit_price=exit_price,
        return_pct=_pct(return_frac),
        MFE=_pct(mfe_frac),
        MAE=_pct(mae_frac),
        holding_minutes=holding_minutes,
        trade_type=str(features["trade_type"]),
        is_continuation_candidate=bool(features["is_continuation_candidate"]),
        is_option_core=bool(features["is_option_core"]),
        is_scalp_thin_room=bool(features["is_scalp_thin_room"]),
        upper_room=_pct(float(features["upper_room_frac"])),
        boll_pos=float(features["boll_pos"]),
        vwap_dist=_pct(float(features["vwap_dist_frac"])),
        VWAP=_safe_float(entry_bar.get("VWAP")),
        boll_mid=_safe_float(entry_bar.get("boll_mid")),
        boll_up=_safe_float(entry_bar.get("boll_up")),
        MA60=_safe_float(entry_bar.get("ma60")),
        RSI6=_safe_float(entry_bar.get("rsi6")),
        entry_bar_low=_safe_float(entry_bar.get("low")),
        exit_reason=exit_reason,
        baseline_exit_signal=base.exit_signal,
        return_frac=return_frac,
        MFE_frac=mfe_frac,
        MAE_frac=mae_frac,
        upper_room_frac=float(features["upper_room_frac"]),
        vwap_dist_frac=float(features["vwap_dist_frac"]),
    )


def _last_available_ts(bars: pd.DataFrame, desired: pd.Timestamp) -> pd.Timestamp | None:
    if bars.empty:
        return None
    subset = bars[bars.index <= _utc_timestamp(desired)]
    if subset.empty:
        return None
    return subset.index[-1]


def _simulate_delay_boll_up(
    base: BaselineTrade,
    bars: pd.DataFrame,
    deadline: time,
) -> tuple[pd.Timestamp, str]:
    end_ts = _last_available_ts(bars, _deadline_ts(base.entry_ts, deadline))
    if end_ts is None:
        return base.exit_ts, "DATA_MISSING_FALLBACK_BASELINE"
    window = _slice(bars, base.entry_ts, end_ts)
    continuation_active = False
    for ts, row in window.iloc[1:].iterrows():
        high = _safe_float(row.get("high"))
        close = _safe_float(row.get("close"))
        boll_up = _safe_float(row.get("boll_up"))
        boll_mid = _safe_float(row.get("boll_mid"))
        vwap = _safe_float(row.get("VWAP"))
        holds_structure = bool(
            (math.isfinite(vwap) and close >= vwap)
            or (math.isfinite(boll_mid) and close >= boll_mid)
        )
        if not continuation_active and math.isfinite(high) and math.isfinite(boll_up) and high >= boll_up:
            if holds_structure:
                continuation_active = True
                continue
            return ts, "DELAY_BOLL_UP_TOUCH_NO_STRUCTURE"
        if continuation_active and not holds_structure:
            return ts, "DELAY_BOLL_UP_STRUCTURE_LOST"
    return end_ts, "DELAY_BOLL_UP_TIME_EXIT"


def _simulate_target_protect(
    base: BaselineTrade,
    bars: pd.DataFrame,
    deadline: time,
    *,
    target_return_pct: float,
    protect_trigger_pct: float,
    protect_floor_pct: float,
) -> tuple[pd.Timestamp, str]:
    entry_bar = _bar_at(bars, base.entry_ts)
    if entry_bar is None:
        return base.exit_ts, "DATA_MISSING_FALLBACK_BASELINE"
    entry_price = _safe_float(entry_bar.get("close"))
    end_ts = _last_available_ts(bars, _deadline_ts(base.entry_ts, deadline))
    if not math.isfinite(entry_price) or entry_price <= 0 or end_ts is None:
        return base.exit_ts, "DATA_MISSING_FALLBACK_BASELINE"
    target_frac = target_return_pct / 100.0
    trigger_frac = protect_trigger_pct / 100.0
    floor_frac = protect_floor_pct / 100.0
    max_unrealized = 0.0
    window = _slice(bars, base.entry_ts, end_ts)
    for ts, row in window.iloc[1:].iterrows():
        high = _safe_float(row.get("high"))
        close = _safe_float(row.get("close"))
        if math.isfinite(high):
            max_unrealized = max(max_unrealized, (high - entry_price) / entry_price)
        if math.isfinite(close):
            current_return = (close - entry_price) / entry_price
            if current_return >= target_frac:
                return ts, "TARGET_PROTECT_TARGET_RETURN"
            if max_unrealized >= trigger_frac and current_return <= floor_frac:
                return ts, "TARGET_PROTECT_FLOOR_EXIT"
    return end_ts, "TARGET_PROTECT_TIME_EXIT"


def _simulate_early_proof(
    base: BaselineTrade,
    bars: pd.DataFrame,
    *,
    early_minutes: int,
    proof_high_pct: float,
) -> tuple[pd.Timestamp, str]:
    entry_bar = _bar_at(bars, base.entry_ts)
    if entry_bar is None:
        return base.exit_ts, "DATA_MISSING_FALLBACK_BASELINE"
    entry_price = _safe_float(entry_bar.get("close"))
    entry_low = _safe_float(entry_bar.get("low"))
    if not math.isfinite(entry_price) or entry_price <= 0 or not math.isfinite(entry_low):
        return base.exit_ts, "DATA_MISSING_FALLBACK_BASELINE"
    check_end = base.entry_ts + pd.Timedelta(minutes=early_minutes)
    check_window = _slice(bars, base.entry_ts + pd.Timedelta(minutes=1), check_end)
    proof_high = False
    proof_reclaim = False
    for ts, row in check_window.iterrows():
        low = _safe_float(row.get("low"))
        high = _safe_float(row.get("high"))
        close = _safe_float(row.get("close"))
        vwap = _safe_float(row.get("VWAP"))
        boll_mid = _safe_float(row.get("boll_mid"))
        if math.isfinite(low) and low < entry_low:
            return ts, "EARLY_FAILURE_BREAK_ENTRY_LOW"
        if math.isfinite(high) and high >= entry_price * (1 + proof_high_pct / 100.0):
            proof_high = True
        if (math.isfinite(vwap) and close >= vwap) or (
            math.isfinite(boll_mid) and close >= boll_mid
        ):
            proof_reclaim = True
    if base.exit_ts <= check_end:
        return base.exit_ts, _normal_exit_reason(base.exit_signal)
    if not proof_high and not proof_reclaim:
        exit_ts = _last_available_ts(bars, check_end)
        if exit_ts is not None:
            return exit_ts, "EARLY_NO_PROOF"
    return base.exit_ts, _normal_exit_reason(base.exit_signal)


def _normal_exit_reason(signal_code: str) -> str:
    if signal_code == "SIG_1030_REVERSAL_BOLL_UP_EXIT":
        return "NORMAL_BOLL_UP_EXIT"
    if signal_code == "SIG_1030_REVERSAL_TIME_EXIT":
        return "NORMAL_TIME_EXIT"
    return "NORMAL_OTHER_EXIT"


def _build_all_records(
    trades: Sequence[BaselineTrade],
    bars_by_symbol: Mapping[str, pd.DataFrame],
    *,
    deadline: time,
    target_return_pct: float,
    protect_trigger_pct: float,
    protect_floor_pct: float,
    early_minutes: int,
    proof_high_pct: float,
) -> list[TradeRecord]:
    records: list[TradeRecord] = []
    for trade in trades:
        bars = bars_by_symbol.get(trade.symbol)
        if bars is None or bars.empty:
            continue
        baseline = _make_trade_record(
            version_name=VERSION_BASELINE,
            base=trade,
            bars=bars,
            exit_ts=trade.exit_ts,
            exit_reason=_normal_exit_reason(trade.exit_signal),
        )
        if baseline is None:
            continue
        records.append(baseline)

        delay_ts, delay_reason = _simulate_delay_boll_up(trade, bars, deadline)
        delay_record = _make_trade_record(
            version_name=VERSION_DELAY_BOLL_UP,
            base=trade,
            bars=bars,
            exit_ts=delay_ts,
            exit_reason=delay_reason,
        )
        if delay_record is not None:
            records.append(delay_record)

        target_ts, target_reason = _simulate_target_protect(
            trade,
            bars,
            deadline,
            target_return_pct=target_return_pct,
            protect_trigger_pct=protect_trigger_pct,
            protect_floor_pct=protect_floor_pct,
        )
        target_record = _make_trade_record(
            version_name=VERSION_TARGET_PROTECT,
            base=trade,
            bars=bars,
            exit_ts=target_ts,
            exit_reason=target_reason,
        )
        if target_record is not None:
            records.append(target_record)

        early_ts, early_reason = _simulate_early_proof(
            trade,
            bars,
            early_minutes=early_minutes,
            proof_high_pct=proof_high_pct,
        )
        early_record = _make_trade_record(
            version_name=VERSION_EARLY_PROOF,
            base=trade,
            bars=bars,
            exit_ts=early_ts,
            exit_reason=early_reason,
        )
        if early_record is not None:
            records.append(early_record)

        for version_name, threshold in ROOM_VERSIONS.items():
            if baseline.is_option_core and baseline.upper_room_frac >= threshold:
                records.append(replace(baseline, version_name=version_name))
    return records


def _record_to_dict(record: TradeRecord) -> dict[str, object]:
    return dict(record.__dict__)


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _summarize(records: Sequence[TradeRecord], *, large_loss_threshold_pct: float) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[TradeRecord]] = {}
    for record in records:
        groups.setdefault((record.version_name, record.trade_type), []).append(record)
    summaries: list[dict[str, object]] = []
    for (version_name, trade_type), rows in sorted(groups.items()):
        returns = [r.return_pct for r in rows]
        mfes = [r.MFE for r in rows]
        maes = [r.MAE for r in rows]
        holding = [r.holding_minutes for r in rows]
        ratios = [r.return_pct / r.MFE for r in rows if math.isfinite(r.MFE) and r.MFE > 0]
        summaries.append(
            {
                "version_name": version_name,
                "trade_type": trade_type,
                "trade_count": len(rows),
                "win_rate": _safe_ratio(sum(1 for value in returns if value > 0), len(rows)),
                "avg_return": sum(returns) / len(rows),
                "median_return": median(returns),
                "total_return": sum(returns),
                "avg_MFE": sum(mfes) / len(rows),
                "avg_MAE": sum(maes) / len(rows),
                "MFE_capture_ratio": sum(ratios) / len(ratios) if ratios else math.nan,
                "small_win_ratio": _safe_ratio(
                    sum(1 for value in returns if 0 < value < 0.10),
                    len(rows),
                ),
                "large_loss_count": sum(1 for value in returns if value <= -large_loss_threshold_pct),
                "avg_holding_minutes": sum(holding) / len(rows),
                "max_holding_minutes": max(holding),
            }
        )
    return summaries


def _version_summary(records: Sequence[TradeRecord], *, large_loss_threshold_pct: float) -> list[dict[str, object]]:
    groups: dict[str, list[TradeRecord]] = {}
    for record in records:
        groups.setdefault(record.version_name, []).append(record)
    rows: list[dict[str, object]] = []
    for version_name, items in sorted(groups.items()):
        returns = [r.return_pct for r in items]
        ratios = [r.return_pct / r.MFE for r in items if math.isfinite(r.MFE) and r.MFE > 0]
        rows.append(
            {
                "version_name": version_name,
                "trade_count": len(items),
                "win_rate": _safe_ratio(sum(1 for value in returns if value > 0), len(items)),
                "avg_return": sum(returns) / len(items),
                "median_return": median(returns),
                "total_return": sum(returns),
                "max_loss": min(returns),
                "MFE_capture_ratio": sum(ratios) / len(ratios) if ratios else math.nan,
                "small_win_ratio": _safe_ratio(
                    sum(1 for value in returns if 0 < value < 0.10),
                    len(items),
                ),
                "large_loss_count": sum(1 for value in returns if value <= -large_loss_threshold_pct),
                "avg_MAE": sum(r.MAE for r in items) / len(items),
                "avg_holding_minutes": sum(r.holding_minutes for r in items) / len(items),
                "max_holding_minutes": max(r.holding_minutes for r in items),
            }
        )
    return rows


def _exit_summary(records: Sequence[TradeRecord]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[TradeRecord]] = {}
    for record in records:
        groups.setdefault((record.version_name, record.exit_reason), []).append(record)
    return [
        {
            "version_name": version,
            "exit_reason": reason,
            "trade_count": len(rows),
            "avg_return": sum(r.return_pct for r in rows) / len(rows),
            "avg_holding_minutes": sum(r.holding_minutes for r in rows) / len(rows),
        }
        for (version, reason), rows in sorted(groups.items())
    ]


def _final_branch_records(records: Sequence[TradeRecord]) -> list[TradeRecord]:
    """Build the three final non-mixed branches requested by the research plan."""
    branch_records: list[TradeRecord] = []
    for record in records:
        if (
            record.version_name == "v8_guard_rsi_floor_core_room_025"
            and record.trade_type == "option_core_reversal"
        ):
            branch_records.append(record)
        elif (
            record.version_name == "v8_guard_rsi_floor_core_room_035"
            and record.trade_type == "option_core_reversal"
        ):
            branch_records.append(record)
        elif (
            record.version_name == VERSION_TARGET_PROTECT
            and record.trade_type == "continuation_candidate"
        ):
            branch_records.append(replace(record, version_name="v8_guard_continuation_v2"))
        elif record.version_name == VERSION_BASELINE and record.trade_type == "scalp_thin_room":
            branch_records.append(replace(record, version_name="v8_guard_scalp_observe"))
    return branch_records


def _case_rows(records: Sequence[TradeRecord]) -> list[dict[str, object]]:
    wanted = {
        ("AAPL", "2026-05-14"),
        ("AAPL", "2026-05-15"),
        ("PLTR", "2026-05-27"),
        ("PLTR", "2026-05-18"),
    }
    rows = [
        _record_to_dict(record)
        for record in records
        if (record.symbol, record.trade_date) in wanted
    ]
    present = {(row["symbol"], row["trade_date"]) for row in rows}
    for symbol, trade_date in sorted(wanted - present):
        rows.append(
            {
                "version_name": "case_missing",
                "symbol": symbol,
                "date": trade_date,
                "trade_date": trade_date,
                "entry_time": "",
                "exit_time": "",
                "entry_price": "",
                "exit_price": "",
                "return_pct": "",
                "MFE": "",
                "MAE": "",
                "trade_type": "not_found",
                "exit_reason": "NO_BASELINE_TRADE",
                "upper_room": "",
                "boll_pos": "",
                "vwap_dist": "",
            }
        )
    return rows


def _fmt(value: object, digits: int = 4) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return ""
    return f"{number:.{digits}f}"


def _markdown_table(rows: Sequence[Mapping[str, object]], columns: Sequence[str], limit: int | None = None) -> str:
    selected = list(rows[:limit] if limit else rows)
    if not selected:
        return "_No rows._"
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in selected:
        values = []
        for column in columns:
            value = row.get(column, "")
            if isinstance(value, float):
                values.append(_fmt(value))
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _write_report(
    path: Path,
    *,
    runs: Sequence[RunInfo],
    records: Sequence[TradeRecord],
    summary_rows: Sequence[Mapping[str, object]],
    version_rows: Sequence[Mapping[str, object]],
    exit_rows: Sequence[Mapping[str, object]],
    branch_rows: Sequence[Mapping[str, object]],
    branch_summary_rows: Sequence[Mapping[str, object]],
    cases: Sequence[Mapping[str, object]],
    output_dir: Path,
    args: argparse.Namespace,
) -> None:
    baseline_rows = [r for r in records if r.version_name == VERSION_BASELINE]
    baseline_version_rows = [row for row in version_rows if row["version_name"] == VERSION_BASELINE]
    core_versions = [
        row for row in version_rows if str(row["version_name"]).startswith("v8_guard_rsi_floor_core_room_")
    ]
    continuation_rows = [
        row
        for row in version_rows
        if str(row["version_name"])
        in {VERSION_DELAY_BOLL_UP, VERSION_TARGET_PROTECT, VERSION_EARLY_PROOF}
    ]
    trade_type_counts: dict[str, int] = {}
    for row in baseline_rows:
        trade_type_counts[row.trade_type] = trade_type_counts.get(row.trade_type, 0) + 1
    trade_type_table = [
        {"trade_type": key, "trade_count": value}
        for key, value in sorted(trade_type_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    text_parts = [
        "# v8 Guard RSI Floor Experiment Report",
        "",
        f"Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "## Scope",
        "",
        f"- Baseline version: `{BASELINE_VERSION}`",
        f"- Source batch: `{runs[0].batch_id}`",
        f"- Source run_ids: `{', '.join(str(r.run_id) for r in runs)}`",
        f"- Symbols: `{', '.join(r.symbol for r in runs)}`",
        f"- Output directory: `{output_dir}`",
        "",
        "This report is a read-only research replay. It does not overwrite baseline runs.",
        "",
        "## Baseline Overview",
        "",
        _markdown_table(
            baseline_version_rows,
            [
                "version_name",
                "trade_count",
                "win_rate",
                "avg_return",
                "median_return",
                "total_return",
                "max_loss",
                "MFE_capture_ratio",
                "small_win_ratio",
                "large_loss_count",
                "avg_holding_minutes",
            ],
        ),
        "",
        "## Baseline Trade Type Distribution",
        "",
        "Single `trade_type` precedence is `scalp_thin_room` > `continuation_candidate` > `option_core_reversal` > `uncategorized_reversal`; boolean columns keep overlapping labels.",
        "",
        _markdown_table(trade_type_table, ["trade_type", "trade_count"]),
        "",
        "## Version Comparison",
        "",
        _markdown_table(
            version_rows,
            [
                "version_name",
                "trade_count",
                "win_rate",
                "avg_return",
                "total_return",
                "max_loss",
                "MFE_capture_ratio",
                "small_win_ratio",
                "large_loss_count",
                "avg_holding_minutes",
            ],
        ),
        "",
        "## Branch Recommendation",
        "",
        _branch_recommendation_text(baseline_version_rows, core_versions, continuation_rows),
        "",
        "## Final Branches",
        "",
        "These are split outputs, not a mixed strategy.",
        "",
        _markdown_table(
            branch_summary_rows,
            [
                "version_name",
                "trade_count",
                "win_rate",
                "avg_return",
                "total_return",
                "max_loss",
                "MFE_capture_ratio",
                "small_win_ratio",
                "large_loss_count",
                "avg_holding_minutes",
            ],
        ),
        "",
        "## Summary By Trade Type",
        "",
        _markdown_table(
            summary_rows,
            [
                "version_name",
                "trade_type",
                "trade_count",
                "win_rate",
                "avg_return",
                "total_return",
                "avg_MFE",
                "avg_MAE",
                "MFE_capture_ratio",
                "small_win_ratio",
                "large_loss_count",
            ],
        ),
        "",
        "## Exit Breakdown",
        "",
        _markdown_table(
            exit_rows,
            ["version_name", "exit_reason", "trade_count", "avg_return", "avg_holding_minutes"],
        ),
        "",
        "## Key Cases",
        "",
        _markdown_table(
            cases,
            [
                "version_name",
                "symbol",
                "trade_date",
                "entry_time",
                "exit_time",
                "return_pct",
                "MFE",
                "MAE",
                "trade_type",
                "exit_reason",
                "upper_room",
                "boll_pos",
                "vwap_dist",
            ],
        ),
        "",
        "## Experiment Parameters",
        "",
        f"- target_return: `{args.target_return_pct:.2f}%`",
        f"- protect_trigger: `{args.protect_trigger_pct:.2f}%`",
        f"- protect_floor: `{args.protect_floor_pct:.2f}%`",
        f"- early_proof_minutes: `{args.early_proof_minutes}`",
        f"- early_proof_high: `{args.early_proof_high_pct:.2f}%`",
        f"- large_loss_threshold: `-{args.large_loss_threshold_pct:.2f}%`",
        "",
        "## Recommendation Framework",
        "",
        "- Keep `v8_guard_rsi_floor` as the preserved baseline unless an experiment improves average return, MFE capture, small-win ratio, and max loss together.",
        "- Keep `v8_guard_rsi_floor_core_room_025` as the balanced baseline.",
        "- Promote `v8_guard_rsi_floor_core_room_035` only if the stricter room cut still leaves enough trades for short-DTE execution.",
        "- Promote `v8_guard_continuation_v2` only if delayed/target exits reduce small wins without expanding large losses.",
        "- Keep `v8_guard_scalp_observe` out of short-DTE option PnL unless real bid/ask viability later proves otherwise.",
    ]
    path.write_text("\n".join(text_parts) + "\n", encoding="utf-8")


def _branch_recommendation_text(
    baseline_rows: Sequence[Mapping[str, object]],
    core_versions: Sequence[Mapping[str, object]],
    continuation_rows: Sequence[Mapping[str, object]],
) -> str:
    lines = [
        f"- Preserve baseline: `{VERSION_BASELINE}`.",
        "",
    ]
    if baseline_rows:
        lines.append(
            f"- Baseline overview: {baseline_rows[0]['trade_count']} trades, "
            f"avg return {_fmt(baseline_rows[0]['avg_return'])}%, "
            f"win rate {_fmt(baseline_rows[0]['win_rate'] * 100 if baseline_rows[0]['win_rate'] <= 1 else baseline_rows[0]['win_rate'])}%."
        )
    if core_versions:
        ranked = sorted(
            core_versions,
            key=lambda row: (
                _safe_float(row.get("MFE_capture_ratio")),
                _safe_float(row.get("avg_return")),
                _safe_float(row.get("trade_count")),
            ),
            reverse=True,
        )
        best = ranked[0]
        lines.extend(
            [
                "",
                f"- Best core-room candidate: `{best['version_name']}`.",
                f"  - trade_count: {best['trade_count']}",
                f"  - avg_return: {_fmt(best['avg_return'])}%",
                f"  - MFE_capture_ratio: {_fmt(best['MFE_capture_ratio'])}",
                f"  - small_win_ratio: {_fmt(best['small_win_ratio'] * 100 if best['small_win_ratio'] <= 1 else best['small_win_ratio'])}%",
                f"  - max_loss: {_fmt(best['max_loss'])}%",
                "  - Reason: it improves capture and removes small-win noise without over-shrinking the sample as aggressively as the strictest room cut.",
            ]
        )
    if continuation_rows:
        lines.extend(
            [
                "",
                "- Continuation exit experiments are not ready as a global baseline.",
                "  - The delayed Bollinger hold improves the obvious runners, but it also expands losses on thin-room / scalp cases.",
                "  - The target-protect variant is too blunt globally and needs a continuation-only gate before it can be trusted.",
                "  - The continuation branch should be restricted to true continuation candidates, not every v8 trade.",
            ]
        )
    lines.extend(
        [
            "",
            "- Scalp-thin-room trades remain observational only.",
            "  - They can be profitable in equity,",
            "  - but they are not a reliable short-DTE option core without stronger premium-room viability.",
        ]
    )
    return "\n".join(lines)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path("research_notes")
        / f"v8_guard_experiments_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    settings = get_settings()
    engine = create_engine(settings.database_url)
    with engine.connect() as conn:
        runs = _load_runs(conn, batch_id=args.batch_id, run_ids=args.run_ids)
        if args.symbols:
            allowed = {symbol.upper() for symbol in args.symbols}
            runs = [run for run in runs if run.symbol in allowed]
        if not runs:
            raise SystemExit("No runs left after symbol filtering.")
        signals = _load_signals(conn, [run.run_id for run in runs])
        trades = _pair_baseline_trades(signals)
        if not trades:
            raise SystemExit("No baseline 1030 reversal trades reconstructed.")

        bars_by_symbol: dict[str, pd.DataFrame] = {}
        for run in runs:
            # Load through 12:00 ET of the end date plus one minute for deadline exits.
            bars_by_symbol[run.symbol] = _load_bars(
                conn,
                symbol=run.symbol,
                start=run.start - timedelta(minutes=1),
                end=run.end + timedelta(hours=4),
            )

    records = _build_all_records(
        trades,
        bars_by_symbol,
        deadline=_parse_hhmm(args.deadline),
        target_return_pct=args.target_return_pct,
        protect_trigger_pct=args.protect_trigger_pct,
        protect_floor_pct=args.protect_floor_pct,
        early_minutes=args.early_proof_minutes,
        proof_high_pct=args.early_proof_high_pct,
    )
    if not records:
        raise SystemExit("No trade records generated.")

    trade_rows = [_record_to_dict(record) for record in records]
    summary_rows = _summarize(records, large_loss_threshold_pct=args.large_loss_threshold_pct)
    version_rows = _version_summary(records, large_loss_threshold_pct=args.large_loss_threshold_pct)
    exit_rows = _exit_summary(records)
    branch_records = _final_branch_records(records)
    branch_rows = [_record_to_dict(record) for record in branch_records]
    branch_summary_rows = _version_summary(
        branch_records, large_loss_threshold_pct=args.large_loss_threshold_pct
    )
    case_rows = _case_rows(records)

    trade_fields = list(TradeRecord.__dataclass_fields__.keys())
    summary_fields = [
        "version_name",
        "trade_type",
        "trade_count",
        "win_rate",
        "avg_return",
        "median_return",
        "total_return",
        "avg_MFE",
        "avg_MAE",
        "MFE_capture_ratio",
        "small_win_ratio",
        "large_loss_count",
        "avg_holding_minutes",
        "max_holding_minutes",
    ]
    version_fields = [
        "version_name",
        "trade_count",
        "win_rate",
        "avg_return",
        "median_return",
        "total_return",
        "max_loss",
        "MFE_capture_ratio",
        "small_win_ratio",
        "large_loss_count",
        "avg_MAE",
        "avg_holding_minutes",
        "max_holding_minutes",
    ]
    exit_fields = ["version_name", "exit_reason", "trade_count", "avg_return", "avg_holding_minutes"]

    _write_csv(output_dir / "trade_level.csv", trade_rows, trade_fields)
    _write_csv(output_dir / "summary_by_trade_type.csv", summary_rows, summary_fields)
    _write_csv(output_dir / "summary_by_version.csv", version_rows, version_fields)
    _write_csv(output_dir / "exit_breakdown.csv", exit_rows, exit_fields)
    _write_csv(output_dir / "final_branch_trade_level.csv", branch_rows, trade_fields)
    _write_csv(output_dir / "final_branch_summary.csv", branch_summary_rows, version_fields)
    _write_csv(
        output_dir / "case_review.csv",
        case_rows,
        [
            "version_name",
            "source_run_id",
            "symbol",
            "date",
            "trade_date",
            "entry_time",
            "exit_time",
            "entry_price",
            "exit_price",
            "return_pct",
            "MFE",
            "MAE",
            "holding_minutes",
            "trade_type",
            "exit_reason",
            "upper_room",
            "boll_pos",
            "vwap_dist",
            "VWAP",
            "boll_mid",
            "boll_up",
            "MA60",
            "RSI6",
        ],
    )
    _write_report(
        output_dir / "report.md",
        runs=runs,
        records=records,
        summary_rows=summary_rows,
        version_rows=version_rows,
        exit_rows=exit_rows,
        branch_rows=branch_rows,
        branch_summary_rows=branch_summary_rows,
        cases=case_rows,
        output_dir=output_dir,
        args=args,
    )

    print(f"wrote {output_dir}")
    print(f"baseline_trades={sum(1 for r in records if r.version_name == VERSION_BASELINE)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
