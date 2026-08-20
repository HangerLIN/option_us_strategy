#!/usr/bin/env python3
"""Split equity bt_trades canonical ledger into sleeves by entry reason_code.

Reads the round-trip ledger written by the equity track (one BUY + one SELL row
per trace_id), attributes each round-trip to the entry signal's sleeve, and
reports per-sleeve trade count, win rate, mean/median return, max single loss,
and cumulative-equity-curve max drawdown.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Iterable, Mapping, Sequence

from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from libs.core.config import get_settings


# Entry reason_code -> sleeve. Unmapped entry codes fall into "other".
SLEEVE_MAP: dict[str, str] = {
    "SIG_1030_REVERSAL_CALL_BUY": "v8_reversal",
    "SIG_1030_V8A_LOW_REVERSAL_CALL_BUY": "v8_reversal",
    "SIG_1030_V8B_RECLAIM_CALL_BUY": "v8_reversal",
    "SIG_1030_BOLL_MID_RECLAIM_CALL_BUY": "v8_reversal",
    "SIG_OPEN_CHASE_BUY": "opening_chase",
    "SIG_OPEN_CHASE_ORB_BUY": "opening_chase",
    "SIG_PM_BOTTOM_A2": "pm_bottom",
    "SIG_PM_BOTTOM_A3": "pm_bottom",
    "SIG_PM_BOTTOM_A4": "pm_bottom",
}

SLEEVE_ORDER = ["v8_reversal", "opening_chase", "pm_bottom", "other"]

SLEEVE_LABEL = {
    "v8_reversal": "v8 1030 反转",
    "opening_chase": "opening chase",
    "pm_bottom": "PM_BOTTOM",
    "other": "其他",
}


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-id",
        default="bt-20260817-eq-full-v8rsi-bttrades",
        help="bt_runs.parameters.batch_id to consume (default newest equity v8 batch)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory. Defaults to research_notes/equity_sleeve_<timestamp>.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _sleeve_for(reason_code: str | None) -> str:
    return SLEEVE_MAP.get(str(reason_code or "").upper(), "other")


def _load_legs(engine, batch_id: str) -> list[dict]:
    """Fetch equity BUY/SELL legs for a batch, ordered by trade time."""
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT t.trade_id, t.symbol, t.side, t.quantity, t.price,
                       t.trade_ts, t.trace_id, t.reason_code, t.fees, t.slippage
                FROM bt_trades t
                JOIN bt_runs r ON t.run_id = r.run_id
                WHERE r.parameters->>'batch_id' = :batch_id
                  AND t.asset_type = 'EQUITY'
                ORDER BY t.trade_ts ASC, t.side ASC
                """
            ),
            {"batch_id": batch_id},
        ).mappings().all()
    return [dict(r) for r in rows]


def _pair_round_trips(legs: Sequence[Mapping]) -> list[dict]:
    """Pair BUY/SELL legs by trace_id into round-trips."""
    by_trace: dict[str, dict[str, dict]] = defaultdict(dict)
    for leg in legs:
        side = str(leg.get("side") or "").upper()
        trace = str(leg.get("trace_id") or "")
        if side in {"BUY", "SELL"} and trace:
            by_trace[trace][side] = leg

    round_trips: list[dict] = []
    for trace, legs_by_side in by_trace.items():
        buy = legs_by_side.get("BUY")
        sell = legs_by_side.get("SELL")
        if buy is None or sell is None:
            continue
        entry_px = float(buy["price"] or 0)
        exit_px = float(sell["price"] or 0)
        if entry_px <= 0:
            continue
        entry_ts = buy["trade_ts"]
        exit_ts = sell["trade_ts"]
        holding_min = 0.0
        if isinstance(entry_ts, datetime) and isinstance(exit_ts, datetime):
            holding_min = (exit_ts - entry_ts).total_seconds() / 60.0
        entry_code = str(buy.get("reason_code") or "")
        round_trips.append(
            {
                "symbol": str(buy.get("symbol") or "").upper(),
                "sleeve": _sleeve_for(entry_code),
                "entry_signal": entry_code,
                "exit_signal": str(sell.get("reason_code") or ""),
                "entry_time": entry_ts,
                "exit_time": exit_ts,
                "entry_px": entry_px,
                "exit_px": exit_px,
                "return_pct": (exit_px - entry_px) / entry_px * 100.0,
                "holding_min": holding_min,
            }
        )
    round_trips.sort(key=lambda r: (r["sleeve"], str(r["entry_time"]), r["symbol"]))
    return round_trips


def _equity_curve_max_drawdown(returns: Sequence[float]) -> float:
    """Max peak-to-trough drawdown of cumulative return (%) series."""
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in returns:
        cumulative += value
        peak = max(peak, cumulative)
        max_dd = max(max_dd, peak - cumulative)
    return max_dd


def _summarize(round_trips: Sequence[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for rt in round_trips:
        grouped[rt["sleeve"]].append(rt)

    rows: list[dict] = []
    for sleeve in SLEEVE_ORDER:
        if sleeve not in grouped:
            continue
        items = grouped[sleeve]
        returns = [rt["return_pct"] for rt in items]
        wins = [v for v in returns if v > 0]
        holding = [rt["holding_min"] for rt in items]
        count = len(returns)
        rows.append(
            {
                "sleeve": sleeve,
                "sleeve_label": SLEEVE_LABEL[sleeve],
                "trade_count": count,
                "win_rate": len(wins) / count if count else 0.0,
                "avg_return_pct": mean(returns) if count else 0.0,
                "median_return_pct": median(returns) if count else 0.0,
                "total_return_pct": sum(returns) if count else 0.0,
                "max_single_loss_pct": min(returns) if count else 0.0,
                "max_single_win_pct": max(returns) if count else 0.0,
                "max_drawdown_pct": _equity_curve_max_drawdown(returns),
                "avg_holding_min": mean(holding) if holding else 0.0,
                "symbols": sorted({rt["symbol"] for rt in items}),
            }
        )
    return rows


def _write_csv(path: Path, rows: Sequence[Mapping], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})


def _fmt(value: object, digits: int = 2) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return ""
    return f"{number:.{digits}f}"


def _pct(value: object, digits: int = 2) -> str:
    return _fmt(value, digits) + "%"


def _markdown_table(rows: Sequence[Mapping], columns: Sequence[tuple[str, str]]) -> str:
    if not rows:
        return "_No rows._"
    headers = [label for _, label in columns]
    keys = [key for key, _ in columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        values = []
        for key in keys:
            value = row.get(key, "")
            values.append(_fmt(value) if isinstance(value, float) else str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _write_report(path: Path, *, batch_id: str, summary: Sequence[Mapping], round_trips: Sequence[Mapping]) -> None:
    summary_columns = [
        ("sleeve_label", "Sleeve"),
        ("trade_count", "笔数"),
        ("win_rate", "胜率"),
        ("avg_return_pct", "平均收益"),
        ("median_return_pct", "中位收益"),
        ("total_return_pct", "累计收益"),
        ("max_single_loss_pct", "最大单笔亏损"),
        ("max_single_win_pct", "最大单笔盈利"),
        ("max_drawdown_pct", "累计曲线最大回撤"),
        ("avg_holding_min", "平均持仓(分)"),
    ]
    total_count = sum(r["trade_count"] for r in summary)
    lines = [
        "# Equity Sleeve 分析（v8_guard_rsi_floor 逐笔账）",
        "",
        f"Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "## Scope",
        "",
        f"- batch_id: `{batch_id}`",
        "- 数据源: `bt_trades`（equity track 逐笔账，BUY/SELL 按 trace_id 配对）",
        f"- 总笔数: {total_count}",
        "",
        "## Sleeve 对比",
        "",
        _markdown_table(summary, summary_columns),
        "",
        "## 说明",
        "",
        "- `return_pct` = (exit_px - entry_px) / entry_px * 100，equity 1 股口径。",
        "- `max_drawdown_pct` 为 sleeve 内按 entry 时间累积收益曲线的峰值回撤。",
        "- `v8 反转` 包含 `SIG_1030_REVERSAL_CALL_BUY` 及 V8A/V8B/BOLL_MID 变体（本次仅 CALL_BUY）。",
        "- `opening chase` 包含 `SIG_OPEN_CHASE_BUY` / `SIG_OPEN_CHASE_ORB_BUY`。",
        "- `PM_BOTTOM` 包含 `SIG_PM_BOTTOM_A2/A3/A4`。",
        "",
        "## 逐笔明细（按 sleeve）",
        "",
        _markdown_table(
            round_trips,
            [
                ("sleeve", "Sleeve"),
                ("symbol", "Symbol"),
                ("entry_signal", "Entry Signal"),
                ("exit_signal", "Exit Signal"),
                ("entry_time", "Entry Time"),
                ("exit_time", "Exit Time"),
                ("entry_px", "Entry Px"),
                ("exit_px", "Exit Px"),
                ("return_pct", "Return %"),
                ("holding_min", "Holding Min"),
            ],
        ),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path("research_notes") / f"equity_sleeve_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    settings = get_settings()
    engine = create_engine(settings.database_url)
    legs = _load_legs(engine, args.batch_id)
    if not legs:
        raise SystemExit(f"No equity bt_trades rows found for batch {args.batch_id}")

    round_trips = _pair_round_trips(legs)
    if not round_trips:
        raise SystemExit("No round-trips paired from ledger")

    summary = _summarize(round_trips)

    detail_fields = [
        "sleeve",
        "symbol",
        "entry_signal",
        "exit_signal",
        "entry_time",
        "exit_time",
        "entry_px",
        "exit_px",
        "return_pct",
        "holding_min",
    ]
    summary_fields = list(summary[0].keys()) if summary else []
    _write_csv(output_dir / "sleeve_summary.csv", summary, summary_fields)
    _write_csv(output_dir / "trade_level.csv", round_trips, detail_fields)
    _write_report(
        output_dir / "report.md",
        batch_id=args.batch_id,
        summary=summary,
        round_trips=round_trips,
    )

    print(f"wrote {output_dir}")
    print(f"round_trips={len(round_trips)}")
    for row in summary:
        print(
            f"  {row['sleeve_label']:<16} n={row['trade_count']:<3} "
            f"win={row['win_rate']*100:.1f}% avg={row['avg_return_pct']:+.2f}% "
            f"med={row['median_return_pct']:+.2f}% dd={row['max_drawdown_pct']:.2f}%"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
