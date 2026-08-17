#!/usr/bin/env python3
"""Combine independent equity research branches into branch-priority portfolios."""

from __future__ import annotations

import argparse
import csv
import math
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Iterable, Mapping, Sequence

import pandas as pd


DEFAULT_V8_VERSIONS = (
    "v8_guard_rsi_floor",
    "v8_guard_rsi_floor_core_room_025",
    "v8_guard_rsi_floor_core_room_035",
)


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Combine v8 reversal and opening chase research trades.")
    parser.add_argument(
        "--v8-trades",
        default="research_notes/v8_guard_experiments_20260614_v2/trade_level.csv",
    )
    parser.add_argument(
        "--opening-trades",
        default="research_notes/opening_chase_lifecycle_20260615_v3_t100/trade_level.csv",
    )
    parser.add_argument("--opening-version", default="opening_chase_v2_gap_quality_core")
    parser.add_argument("--v8-versions", nargs="*", default=list(DEFAULT_V8_VERSIONS))
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Defaults to research_notes/combined_branch_<timestamp>.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _finite(value: object) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    return result


def _normalize_opening(opening: pd.DataFrame, version: str) -> pd.DataFrame:
    selected = opening[opening["version_name"] == version].copy()
    if selected.empty:
        return selected
    selected["branch"] = "opening_chase"
    selected["branch_version"] = selected["version_name"]
    selected["source_run_id"] = ""
    selected["trade_type"] = "opening_chase_gap_quality_core"
    selected["is_option_core"] = True
    selected["baseline_exit_signal"] = ""
    selected["upper_room"] = math.nan
    selected["boll_pos"] = math.nan
    selected["vwap_dist"] = selected.get("vwap_dist_pct", math.nan)
    selected["RSI6"] = selected.get("rsi6", math.nan)
    selected["date"] = selected["trade_date"]
    keep = [
        "branch",
        "branch_version",
        "source_run_id",
        "symbol",
        "date",
        "trade_date",
        "entry_time",
        "exit_time",
        "entry_ts_utc",
        "exit_ts_utc",
        "entry_price",
        "exit_price",
        "return_pct",
        "MFE",
        "MAE",
        "holding_minutes",
        "trade_type",
        "is_option_core",
        "upper_room",
        "boll_pos",
        "vwap_dist",
        "RSI6",
        "exit_reason",
        "baseline_exit_signal",
    ]
    return selected[keep]


def _normalize_v8(v8: pd.DataFrame, version: str, blocked_keys: set[tuple[str, str]]) -> pd.DataFrame:
    selected = v8[v8["version_name"] == version].copy()
    if selected.empty:
        return selected
    selected = selected[
        ~selected.apply(lambda row: (str(row["symbol"]).upper(), str(row["trade_date"])) in blocked_keys, axis=1)
    ].copy()
    selected["branch"] = "v8_reversal"
    selected["branch_version"] = selected["version_name"]
    keep = [
        "branch",
        "branch_version",
        "source_run_id",
        "symbol",
        "date",
        "trade_date",
        "entry_time",
        "exit_time",
        "entry_ts_utc",
        "exit_ts_utc",
        "entry_price",
        "exit_price",
        "return_pct",
        "MFE",
        "MAE",
        "holding_minutes",
        "trade_type",
        "is_option_core",
        "upper_room",
        "boll_pos",
        "vwap_dist",
        "RSI6",
        "exit_reason",
        "baseline_exit_signal",
    ]
    return selected[keep]


def _summary(rows: pd.DataFrame, group_cols: Sequence[str]) -> list[dict[str, object]]:
    if rows.empty:
        return []
    out: list[dict[str, object]] = []
    grouped = rows.groupby(list(group_cols), dropna=False)
    for key, items in grouped:
        if not isinstance(key, tuple):
            key = (key,)
        returns = [_finite(value) for value in items["return_pct"]]
        returns = [value for value in returns if math.isfinite(value)]
        mfe = [_finite(value) for value in items["MFE"] if math.isfinite(_finite(value))]
        mae = [_finite(value) for value in items["MAE"] if math.isfinite(_finite(value))]
        if not returns:
            continue
        row = {column: value for column, value in zip(group_cols, key)}
        row.update(
            {
                "trade_count": len(returns),
                "win_rate": sum(1 for value in returns if value > 0) / len(returns),
                "avg_return": sum(returns) / len(returns),
                "median_return": median(returns),
                "total_return": sum(returns),
                "max_loss": min(returns),
                "max_win": max(returns),
                "avg_MFE": sum(mfe) / len(mfe) if mfe else math.nan,
                "avg_MAE": sum(mae) / len(mae) if mae else math.nan,
                "avg_holding_minutes": sum(_finite(v) for v in items["holding_minutes"]) / len(items),
            }
        )
        out.append(row)
    return out


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
    output_dir: Path,
    opening_version: str,
    summary_rows: Sequence[Mapping[str, object]],
    branch_rows: Sequence[Mapping[str, object]],
    symbol_rows: Sequence[Mapping[str, object]],
    skipped_rows: Sequence[Mapping[str, object]],
) -> None:
    summary_columns = [
        "combined_version",
        "trade_count",
        "win_rate",
        "avg_return",
        "median_return",
        "total_return",
        "max_loss",
        "max_win",
        "avg_MFE",
        "avg_MAE",
        "avg_holding_minutes",
    ]
    lines = [
        "# Combined Branch Research Report",
        "",
        f"Generated at: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "## Rule",
        "",
        f"- Opening branch: `{opening_version}`",
        "- Combination priority: opening branch first; skip same-symbol same-date v8 reversal if opening branch traded.",
        "- Equity-only arithmetic; no option pricing or synthetic fills.",
        f"- Output directory: `{output_dir}`",
        "",
        "## Combined Summary",
        "",
        _markdown_table(summary_rows, summary_columns),
        "",
        "## Branch Contribution",
        "",
        _markdown_table(
            branch_rows,
            [
                "combined_version",
                "branch",
                "trade_count",
                "win_rate",
                "avg_return",
                "total_return",
                "max_loss",
                "max_win",
            ],
        ),
        "",
        "## Symbol Summary",
        "",
        _markdown_table(
            symbol_rows,
            [
                "combined_version",
                "symbol",
                "trade_count",
                "win_rate",
                "avg_return",
                "total_return",
                "max_loss",
                "max_win",
            ],
        ),
        "",
        "## Skipped V8 Same-Symbol Same-Date Trades",
        "",
        _markdown_table(skipped_rows, ["combined_version", "skipped_count"]),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path("research_notes") / f"combined_branch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    v8 = pd.read_csv(args.v8_trades)
    opening = pd.read_csv(args.opening_trades)
    opening_norm = _normalize_opening(opening, args.opening_version)
    if opening_norm.empty:
        raise SystemExit(f"No opening trades found for version={args.opening_version}")
    blocked_keys = {
        (str(row["symbol"]).upper(), str(row["trade_date"]))
        for _, row in opening_norm.iterrows()
    }

    combined_frames: list[pd.DataFrame] = []
    skipped_rows: list[dict[str, object]] = []
    for v8_version in args.v8_versions:
        v8_selected_all = v8[v8["version_name"] == v8_version].copy()
        skipped = v8_selected_all[
            v8_selected_all.apply(
                lambda row: (str(row["symbol"]).upper(), str(row["trade_date"])) in blocked_keys,
                axis=1,
            )
        ]
        v8_norm = _normalize_v8(v8, v8_version, blocked_keys)
        combo_name = f"{v8_version}__plus__{args.opening_version}"
        combo = pd.concat([opening_norm, v8_norm], ignore_index=True)
        combo["combined_version"] = combo_name
        combo = combo.sort_values(["entry_ts_utc", "symbol"]).reset_index(drop=True)
        combined_frames.append(combo)
        skipped_rows.append({"combined_version": combo_name, "skipped_count": len(skipped)})

    combined = pd.concat(combined_frames, ignore_index=True)
    trade_fields = list(combined.columns)
    combined.to_csv(output_dir / "combined_trade_level.csv", index=False)

    summary_rows = _summary(combined, ["combined_version"])
    branch_rows = _summary(combined, ["combined_version", "branch"])
    symbol_rows = _summary(combined, ["combined_version", "symbol"])
    summary_fields = [
        "combined_version",
        "trade_count",
        "win_rate",
        "avg_return",
        "median_return",
        "total_return",
        "max_loss",
        "max_win",
        "avg_MFE",
        "avg_MAE",
        "avg_holding_minutes",
    ]
    _write_csv(output_dir / "summary_by_version.csv", summary_rows, summary_fields)
    _write_csv(
        output_dir / "summary_by_branch.csv",
        branch_rows,
        [
            "combined_version",
            "branch",
            "trade_count",
            "win_rate",
            "avg_return",
            "median_return",
            "total_return",
            "max_loss",
            "max_win",
            "avg_MFE",
            "avg_MAE",
            "avg_holding_minutes",
        ],
    )
    _write_csv(
        output_dir / "summary_by_symbol.csv",
        symbol_rows,
        [
            "combined_version",
            "symbol",
            "trade_count",
            "win_rate",
            "avg_return",
            "median_return",
            "total_return",
            "max_loss",
            "max_win",
            "avg_MFE",
            "avg_MAE",
            "avg_holding_minutes",
        ],
    )
    _write_csv(output_dir / "skipped_v8_trades.csv", skipped_rows, ["combined_version", "skipped_count"])
    _write_report(
        output_dir / "report.md",
        output_dir=output_dir,
        opening_version=args.opening_version,
        summary_rows=summary_rows,
        branch_rows=branch_rows,
        symbol_rows=symbol_rows,
        skipped_rows=skipped_rows,
    )
    print(f"wrote {output_dir}")
    print(f"trades={len(combined)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
