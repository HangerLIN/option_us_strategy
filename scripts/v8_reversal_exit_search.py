#!/usr/bin/env python3
"""v8 反转（核心白名单标的）退出规则参数搜索 —— equity 层回放。

基于 6 个月窗口核心 4 标的（AAPL/GOOGL/PLTR/TSLA）的 23 笔 v8 反转 entry 逐笔账，
在 equity 1m bar 上回放不同退出规则，对比 笔数/胜率/平均/中位/累计/最大回撤。

不依赖 IBKR、不重跑回测：entry 点已固定，纯读 bars1m_equity + indicators_eq_1m。
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Iterable, Sequence

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from libs.core.config import get_settings
from libs.core.timeutil import EASTERN


# ---------------------------------------------------------------------------
# 退出规则定义
# ---------------------------------------------------------------------------
@dataclass
class ExitRule:
    name: str
    label: str
    # 退出条件开关
    boll_up: bool = False          # high >= 当日 boll_up 触碰退出（baseline）
    time_min: int | None = None    # entry 后固定 N 分钟退出
    tp_pct: float | None = None    # 目标止盈 high >= entry*(1+tp)
    sl_pct: float | None = None    # 保护止损 low <= entry*(1-sl)
    # 未命中任何条件时，用 deadline 前最后一根 bar 退出
    use_boll_up_fallback: bool = False  # tp/sl 未命中时回退到 boll_up（而非持有到 deadline）


def _build_rules() -> list[ExitRule]:
    rules: list[ExitRule] = [
        ExitRule("baseline_boll_up", "baseline (boll_up 触碰)", boll_up=True),
    ]
    for m in (5, 10, 15, 20, 30, 45, 60):
        rules.append(ExitRule(f"time_{m}m", f"固定 {m} 分钟", time_min=m))
    for p in (0.003, 0.005, 0.008):
        rules.append(
            ExitRule(f"tp_{int(p*1000)/10:.1f}pct", f"止盈 {p*100:.1f}%", tp_pct=p, use_boll_up_fallback=True)
        )
    for p in (0.003, 0.005):
        rules.append(
            ExitRule(f"sl_{int(p*1000)/10:.1f}pct", f"止损 {p*100:.1f}% + boll_up", boll_up=True, sl_pct=p)
        )
    # 组合：止盈 + 止损 + boll_up
    rules.append(
        ExitRule("tp0.5_sl0.3", "止盈0.5% + 止损0.3% + boll_up", boll_up=True, tp_pct=0.005, sl_pct=0.003)
    )
    rules.append(
        ExitRule("tp0.8_sl0.5", "止盈0.8% + 止损0.5% + boll_up", boll_up=True, tp_pct=0.008, sl_pct=0.005)
    )
    return rules


# ---------------------------------------------------------------------------
# 数据加载
# ---------------------------------------------------------------------------
def _load_entries(engine, batch_id: str) -> list[dict]:
    rows = engine.connect().execute(
        text(
            """
            SELECT t.symbol, t.price AS entry_px, t.trade_ts AS entry_ts
            FROM bt_trades t JOIN bt_runs r ON t.run_id = r.run_id
            WHERE r.parameters->>'batch_id' = :b AND t.asset_type = 'EQUITY'
              AND t.side = 'BUY' AND t.reason_code = 'SIG_1030_REVERSAL_CALL_BUY'
            ORDER BY t.trade_ts
            """
        ),
        {"b": batch_id},
    ).mappings().all()
    entries: list[dict] = []
    for r in rows:
        ts = r["entry_ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        entries.append(
            {"symbol": str(r["symbol"]).upper(), "entry_px": float(r["entry_px"]), "entry_ts": ts}
        )
    return entries


def _load_bars(engine, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    rows = engine.connect().execute(
        text(
            """
            SELECT b.ts_end, b.open, b.high, b.low, b.close, b.volume,
                   i.boll_up, i.boll_mid, i.boll_dn, i.rsi6
            FROM bars1m_equity b
            LEFT JOIN indicators_eq_1m i ON b.symbol = i.symbol AND b.ts_end = i.ts_end
            WHERE b.symbol = :symbol AND b.ts_end > :start AND b.ts_end <= :end
            ORDER BY b.ts_end
            """
        ),
        {"symbol": symbol, "start": start, "end": end},
    ).mappings().all()
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["ts_end"] = pd.to_datetime(frame["ts_end"], utc=True)
    for col in ["open", "high", "low", "close", "boll_up", "boll_mid", "boll_dn", "rsi6"]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame.set_index("ts_end").sort_index()


# ---------------------------------------------------------------------------
# 退出模拟
# ---------------------------------------------------------------------------
@dataclass
class SimResult:
    rule: str
    symbol: str
    entry_ts: datetime
    exit_ts: datetime
    entry_px: float
    exit_px: float
    return_pct: float
    holding_min: float
    exit_reason: str


def _simulate_entry(entry: dict, bars_after: pd.DataFrame, rule: ExitRule, deadline: datetime) -> SimResult | None:
    """回放单笔 entry 在给定规则下的退出。bars_after 为 entry 之后的 bar（按时间升序）。"""
    entry_px = entry["entry_px"]
    if entry_px <= 0 or bars_after.empty:
        return None

    # 限定到 deadline（含）之前的 bar
    window = bars_after[bars_after.index <= deadline]
    if window.empty:
        window = bars_after.head(1)

    exit_reason = "deadline"
    exit_bar = window.iloc[-1]

    for i, (ts, bar) in enumerate(window.iterrows()):
        high = bar["high"]
        low = bar["low"]
        close = bar["close"]
        boll_up = bar["boll_up"]
        if pd.isna(close) or close <= 0:
            continue
        # 优先级：止损 > 止盈 > boll_up > 时间
        if rule.sl_pct is not None and not pd.isna(low) and low <= entry_px * (1 - rule.sl_pct):
            exit_bar, exit_reason = bar, "stop"
            break
        if rule.tp_pct is not None and not pd.isna(high) and high >= entry_px * (1 + rule.tp_pct):
            exit_bar, exit_reason = bar, "target"
            break
        if rule.boll_up and not pd.isna(boll_up) and not pd.isna(high) and high >= boll_up:
            exit_bar, exit_reason = bar, "boll_up"
            break
        if rule.time_min is not None and (i + 1) >= rule.time_min:
            exit_bar, exit_reason = bar, "time"
            break

    # tp/sl 未命中且启用 boll_up 回退时，重新走 boll_up 触碰（在 window 内）
    if rule.use_boll_up_fallback and exit_reason == "deadline":
        for i, (ts, bar) in enumerate(window.iterrows()):
            high = bar["high"]
            boll_up = bar["boll_up"]
            if not pd.isna(boll_up) and not pd.isna(high) and high >= boll_up:
                exit_bar, exit_reason = bar, "boll_up"
                break

    exit_px = float(exit_bar["close"]) if not pd.isna(exit_bar["close"]) else entry_px
    exit_ts = exit_bar.name
    return SimResult(
        rule=rule.name,
        symbol=entry["symbol"],
        entry_ts=entry["entry_ts"],
        exit_ts=exit_ts,
        entry_px=entry_px,
        exit_px=exit_px,
        return_pct=(exit_px - entry_px) / entry_px * 100.0,
        holding_min=(exit_ts - entry["entry_ts"]).total_seconds() / 60.0,
        exit_reason=exit_reason,
    )


# ---------------------------------------------------------------------------
# 汇总
# ---------------------------------------------------------------------------
def _max_drawdown(returns: Sequence[float]) -> float:
    cum = 0.0
    peak = 0.0
    dd = 0.0
    for v in returns:
        cum += v
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
    return dd


def _summarize(results: Sequence[SimResult], rule: ExitRule) -> dict:
    returns = [r.return_pct for r in results]
    n = len(returns)
    wins = [v for v in returns if v > 0]
    holding = [r.holding_min for r in results]
    return {
        "rule": rule.name,
        "label": rule.label,
        "n": n,
        "win_rate": len(wins) / n if n else 0.0,
        "avg_return": mean(returns) if n else 0.0,
        "median_return": median(returns) if n else 0.0,
        "total_return": sum(returns) if n else 0.0,
        "max_loss": min(returns) if n else 0.0,
        "max_win": max(returns) if n else 0.0,
        "max_drawdown": _max_drawdown(returns),
        "avg_holding": mean(holding) if holding else 0.0,
    }


def _fmt(value: object, digits: int = 2) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(v):
        return ""
    return f"{v:.{digits}f}"


def _print_table(rows: Sequence[dict]) -> None:
    header = f"{'规则':<34}{'笔数':>4}{'胜率':>7}{'平均':>8}{'中位':>8}{'累计':>8}{'最大亏':>8}{'最大回撤':>8}{'持仓':>7}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['label']:<34}{r['n']:>4}{r['win_rate']*100:>6.1f}%{r['avg_return']:>+8.2f}"
            f"{r['median_return']:>+8.2f}{r['total_return']:>+8.2f}{r['max_loss']:>+8.2f}"
            f"{r['max_drawdown']:>8.2f}{r['avg_holding']:>7.1f}"
        )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", default="bt-20260817-core5-v8rsi-6m-bttrades")
    parser.add_argument("--deadline", default="12:00", help="ET 退出截止时间 HH:MM")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Defaults to research_notes/v8_exit_search_<timestamp>",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path("research_notes") / f"v8_exit_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    settings = get_settings()
    engine = create_engine(settings.database_url)
    entries = _load_entries(engine, args.batch_id)
    if not entries:
        raise SystemExit("No v8 reversal entries found")

    hour, minute = args.deadline.split(":")
    rules = _build_rules()

    # 预加载每笔 entry 之后的 bars（到 deadline + 15min buffer）
    print(f"loading bars for {len(entries)} entries ...")
    bars_cache: dict[tuple, pd.DataFrame] = {}
    for e in entries:
        deadline = datetime(
            e["entry_ts"].year, e["entry_ts"].month, e["entry_ts"].day,
            int(hour), int(minute), tzinfo=EASTERN,
        ).astimezone(timezone.utc)
        end = deadline + timedelta(minutes=15)
        key = (e["symbol"], e["entry_ts"])
        bars_cache[key] = _load_bars(engine, e["symbol"], e["entry_ts"], end)

    # 对每个规则回放全部 entry
    all_results: dict[str, list[SimResult]] = {}
    for rule in rules:
        results: list[SimResult] = []
        for e in entries:
            deadline = datetime(
                e["entry_ts"].year, e["entry_ts"].month, e["entry_ts"].day,
                int(hour), int(minute), tzinfo=EASTERN,
            ).astimezone(timezone.utc)
            bars_after = bars_cache[(e["symbol"], e["entry_ts"])]
            res = _simulate_entry(e, bars_after, rule, deadline)
            if res is not None:
                results.append(res)
        all_results[rule.name] = results

    # 汇总
    summary_rows = [_summarize(all_results[r.name], r) for r in rules]

    # 打印
    print(f"\n=== v8 反转退出规则参数搜索（{len(entries)} 笔, 核心4标的 6m）===\n")
    _print_table(summary_rows)

    # 排序：按 avg_return 降序展示 top 规则
    ranked = sorted(summary_rows, key=lambda r: r["avg_return"], reverse=True)
    print("\n=== 按平均收益排序 Top ===")
    _print_table(ranked)

    # 写 CSV
    summary_fields = [
        "rule", "label", "n", "win_rate", "avg_return", "median_return",
        "total_return", "max_loss", "max_win", "max_drawdown", "avg_holding",
    ]
    with (output_dir / "summary.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=summary_fields, extrasaction="ignore")
        w.writeheader()
        for r in summary_rows:
            w.writerow(r)

    # 写逐笔明细（baseline 对比规则：baseline / tp_0.5 / sl_0.3 / time_30m）
    detail_rules = ["baseline_boll_up", "tp_0.5pct", "sl_0.3pct", "time_30m", "tp0.5_sl0.3"]
    detail_fields = ["rule", "symbol", "entry_ts", "exit_ts", "entry_px", "exit_px", "return_pct", "holding_min", "exit_reason"]
    with (output_dir / "detail.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=detail_fields, extrasaction="ignore")
        w.writeheader()
        for rule_name in detail_rules:
            for res in all_results.get(rule_name, []):
                w.writerow(
                    {
                        "rule": res.rule,
                        "symbol": res.symbol,
                        "entry_ts": res.entry_ts.isoformat(),
                        "exit_ts": res.exit_ts.isoformat(),
                        "entry_px": res.entry_px,
                        "exit_px": res.exit_px,
                        "return_pct": res.return_pct,
                        "holding_min": res.holding_min,
                        "exit_reason": res.exit_reason,
                    }
                )

    print(f"\nwrote {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
