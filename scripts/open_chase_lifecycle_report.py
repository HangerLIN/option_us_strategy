from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.core import get_settings  # noqa: E402


TARGET_SYMBOLS = {"MSFT", "AMD", "NOW", "SHOP"}


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _f(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def _near_opening_high(price: Any, opening_range_high: Any) -> bool | None:
    px = _decimal(price)
    high = _decimal(opening_range_high)
    if px is None or high is None or high <= 0:
        return None
    return px >= high * Decimal("0.995")


def _pair_trades(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buys: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    pairs: list[dict[str, Any]] = []
    for row in rows:
        key = (
            row["run_id"],
            row["symbol"],
            row.get("option_right"),
            str(row.get("strike")),
            str(row.get("expiry")),
        )
        side = str(row.get("side") or "").upper()
        if side == "BUY":
            buys[key].append(row)
            continue
        if side != "SELL":
            continue
        buy = buys[key].pop(0) if buys[key] else None
        pairs.append({"buy": buy, "sell": row})
    for remaining in buys.values():
        for buy in remaining:
            pairs.append({"buy": buy, "sell": None})
    return pairs


def _event_index(events: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        trace_id = event.get("trace_id")
        if trace_id:
            index[str(trace_id)].append(event)
    return index


def _has_event(events: list[dict[str, Any]], *types: str) -> bool:
    wanted = set(types)
    return any(str(event.get("event_type")) in wanted for event in events)


def _count_event(events: list[dict[str, Any]], *types: str) -> int:
    wanted = set(types)
    return sum(1 for event in events if str(event.get("event_type")) in wanted)


def _first_fill(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    for event in events:
        if event.get("event_type") == "ORDER_FILLED":
            return event
    return None


def _is_timeout_stale(event: dict[str, Any]) -> bool:
    reason = event.get("stale_reject_reason")
    if reason:
        return str(reason).startswith("signal_to_fill_seconds>")
    return bool(event.get("is_stale_fill"))


def build_report(batch_id: str, output_dir: Path) -> tuple[Path, Path]:
    engine = create_engine(get_settings().database_url)
    with engine.connect() as conn:
        runs = conn.execute(
            text(
                """
                SELECT run_id, parameters
                FROM bt_runs
                WHERE parameters->>'batch_id' = :batch_id
                ORDER BY run_id
                """
            ),
            {"batch_id": batch_id},
        ).mappings().all()
        run_ids = [row["run_id"] for row in runs]
        if not run_ids:
            raise SystemExit(f"No runs found for batch_id={batch_id}")
        trades = conn.execute(
            text(
                """
                SELECT t.*, t.trade_ts AT TIME ZONE 'US/Eastern' AS trade_time_et
                FROM bt_trades t
                WHERE t.run_id = ANY(:run_ids)
                ORDER BY t.trade_ts, t.trade_id
                """
            ),
            {"run_ids": run_ids},
        ).mappings().all()
        events = conn.execute(
            text(
                """
                SELECT e.*, e.signal_time AT TIME ZONE 'US/Eastern' AS signal_time_et,
                       e.order_submit_time AT TIME ZONE 'US/Eastern' AS order_submit_time_et,
                       e.fill_time AT TIME ZONE 'US/Eastern' AS fill_time_et
                FROM bt_order_events e
                WHERE e.run_id = ANY(:run_ids)
                ORDER BY e.created_at, e.event_id
                """
            ),
            {"run_ids": run_ids},
        ).mappings().all()

    trade_rows = [dict(row) for row in trades]
    event_rows = [dict(row) for row in events]
    events_by_trace = _event_index(event_rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{batch_id}_open_chase_lifecycle.csv"
    md_path = output_dir / f"{batch_id}_open_chase_lifecycle.md"
    report_rows: list[dict[str, Any]] = []

    for pair in _pair_trades(trade_rows):
        buy = pair["buy"]
        sell = pair["sell"]
        if buy is None:
            continue
        symbol = str(buy.get("symbol") or "").upper()
        buy_events = events_by_trace.get(str(buy.get("trace_id")), [])
        sell_events = events_by_trace.get(str(sell.get("trace_id")), []) if sell else []
        buy_fill = _first_fill(buy_events)
        buy_price = _decimal(buy.get("price")) or Decimal("0")
        sell_price = _decimal(sell.get("price")) if sell else None
        qty = abs(_decimal(buy.get("quantity")) or Decimal("0"))
        buy_fee = _decimal(buy.get("fees")) or Decimal("0")
        sell_fee = _decimal(sell.get("fees")) if sell else Decimal("0")
        realized = None
        if sell_price is not None:
            realized = (sell_price - buy_price) * qty * Decimal("100") - buy_fee - sell_fee
        fill_event = buy_fill or {}
        spread_pct = _decimal(fill_event.get("option_spread_pct_at_fill"))
        stale = bool(fill_event.get("is_stale_fill")) or _has_event(
            buy_events, "STALE_FILL_REJECTED", "INVALID_FILL_DETECTED"
        )
        rejected = _has_event(buy_events, "STALE_FILL_REJECTED", "INVALID_FILL_DETECTED")
        stale = _is_timeout_stale(fill_event)
        ttl_failure = _has_event(
            buy_events + sell_events,
            "ORDER_TTL_EXPIRED",
            "ORDER_TTL_FALLBACK",
            "EXIT_TTL_FALLBACK",
            "EXIT_FORCED_LIQUIDATION",
        )
        eod_forced = _has_event(
            buy_events + sell_events,
            "EXIT_NO_QUOTE_CONSERVATIVE_MARK",
        ) or (sell and str(sell.get("reason_code")) in {"OPEN_CHASE_EOD_EXIT", "EXIT_NO_QUOTE_CONSERVATIVE_MARK"})
        wide_spread = spread_pct is not None and spread_pct > Decimal("0.10")
        unresolved = sell is None
        clean = not (stale or rejected or wide_spread or ttl_failure or unresolved or eod_forced)
        report_rows.append(
            {
                "run_id": buy.get("run_id"),
                "symbol": symbol,
                "buy_signal": buy.get("reason_code"),
                "sell_signal": sell.get("reason_code") if sell else "",
                "signal_time": fill_event.get("signal_time_et"),
                "fill_time": buy.get("trade_time_et"),
                "signal_to_fill_seconds": fill_event.get("signal_to_fill_seconds"),
                "is_stale_fill": stale,
                "stale_reject_reason": fill_event.get("stale_reject_reason"),
                "underlying_price_at_signal": fill_event.get("underlying_price_at_signal"),
                "underlying_price_at_fill": fill_event.get("underlying_price_at_fill"),
                "vwap_at_signal": fill_event.get("vwap_at_signal"),
                "vwap_at_fill": fill_event.get("vwap_at_fill"),
                "above_vwap_at_fill": fill_event.get("above_vwap_at_fill"),
                "above_orh_at_fill": fill_event.get("above_orh_at_fill"),
                "opening_range_high": fill_event.get("opening_range_high"),
                "opening_range_low": fill_event.get("opening_range_low"),
                "near_opening_high_at_fill": _near_opening_high(
                    fill_event.get("underlying_price_at_fill"),
                    fill_event.get("opening_range_high"),
                ),
                "option_spread_pct_at_fill": spread_pct,
                "exit_ttl_failure": ttl_failure,
                "eod_forced": eod_forced,
                "unresolved_position": unresolved,
                "realized_pnl": realized,
                "clean_trade": clean,
                "pollution_tags": ",".join(
                    tag
                    for tag, flag in [
                        ("stale_fill", stale),
                        ("guard_rejected", rejected),
                        ("wide_spread", wide_spread),
                        ("ttl_failure", ttl_failure),
                        ("unresolved", unresolved),
                        ("eod_forced", eod_forced),
                    ]
                    if flag
                ),
            }
        )

    trade_trace_ids = {str(row.get("trace_id")) for row in trade_rows if row.get("trace_id")}
    for event in event_rows:
        trace_id = event.get("trace_id")
        if not trace_id or str(trace_id) in trade_trace_ids:
            continue
        event_type = str(event.get("event_type") or "")
        if event_type not in {
            "STALE_FILL_REJECTED",
            "ORDER_TTL_EXPIRED",
            "EXIT_TTL_FALLBACK",
            "EXIT_FORCED_LIQUIDATION",
            "EXIT_NO_QUOTE_CONSERVATIVE_MARK",
            "ORDER_REJECTED",
            "ORDER_CANCELLED",
        }:
            continue
        spread_pct = _decimal(event.get("option_spread_pct_at_fill"))
        wide_spread = spread_pct is not None and spread_pct > Decimal("0.10")
        stale = _is_timeout_stale(event)
        rejected = event_type in {
            "STALE_FILL_REJECTED",
            "ORDER_TTL_EXPIRED",
            "EXIT_TTL_FALLBACK",
            "EXIT_FORCED_LIQUIDATION",
            "EXIT_NO_QUOTE_CONSERVATIVE_MARK",
            "ORDER_REJECTED",
            "ORDER_CANCELLED",
        }
        ttl_failure = event_type in {
            "ORDER_TTL_EXPIRED",
            "EXIT_TTL_FALLBACK",
            "EXIT_FORCED_LIQUIDATION",
        }
        eod_forced = event_type == "EXIT_NO_QUOTE_CONSERVATIVE_MARK"
        report_rows.append(
            {
                "run_id": event.get("run_id"),
                "symbol": str(event.get("symbol") or "").upper(),
                "buy_signal": event.get("signal_code"),
                "sell_signal": "",
                "signal_time": event.get("signal_time_et"),
                "fill_time": event.get("fill_time_et"),
                "signal_to_fill_seconds": event.get("signal_to_fill_seconds"),
                "is_stale_fill": stale,
                "stale_reject_reason": event.get("stale_reject_reason"),
                "underlying_price_at_signal": event.get("underlying_price_at_signal"),
                "underlying_price_at_fill": event.get("underlying_price_at_fill"),
                "vwap_at_signal": event.get("vwap_at_signal"),
                "vwap_at_fill": event.get("vwap_at_fill"),
                "above_vwap_at_fill": event.get("above_vwap_at_fill"),
                "above_orh_at_fill": event.get("above_orh_at_fill"),
                "opening_range_high": event.get("opening_range_high"),
                "opening_range_low": event.get("opening_range_low"),
                "near_opening_high_at_fill": _near_opening_high(
                    event.get("underlying_price_at_fill"),
                    event.get("opening_range_high"),
                ),
                "option_spread_pct_at_fill": spread_pct,
                "exit_ttl_failure": ttl_failure,
                "eod_forced": eod_forced,
                "unresolved_position": False,
                "realized_pnl": None,
                "clean_trade": False,
                "pollution_tags": ",".join(
                    tag
                    for tag, flag in [
                        ("stale_fill", stale),
                        ("wide_spread", wide_spread),
                        ("ttl_failure", ttl_failure),
                        ("eod_forced", eod_forced),
                        ("guard_rejected", event_type == "STALE_FILL_REJECTED"),
                        ("rejected", rejected),
                    ]
                    if flag
                ),
            }
        )

    fieldnames = [
        "run_id",
        "symbol",
        "buy_signal",
        "sell_signal",
        "signal_time",
        "fill_time",
        "signal_to_fill_seconds",
        "is_stale_fill",
        "stale_reject_reason",
        "underlying_price_at_signal",
        "underlying_price_at_fill",
        "vwap_at_signal",
        "vwap_at_fill",
        "above_vwap_at_fill",
        "above_orh_at_fill",
        "opening_range_high",
        "opening_range_low",
        "near_opening_high_at_fill",
        "option_spread_pct_at_fill",
        "exit_ttl_failure",
        "eod_forced",
        "unresolved_position",
        "realized_pnl",
        "clean_trade",
        "pollution_tags",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report_rows:
            writer.writerow({key: _f(row.get(key)) for key in fieldnames})

    closed_rows = [row for row in report_rows if row["realized_pnl"] is not None]
    pnl_values = [row["realized_pnl"] for row in closed_rows]
    wins = [pnl for pnl in pnl_values if pnl > 0]
    losses = [pnl for pnl in pnl_values if pnl < 0]
    clean_count = sum(1 for row in report_rows if row["clean_trade"])
    stale_count = sum(1 for row in report_rows if row["is_stale_fill"])
    ttl_count = sum(1 for row in report_rows if row["exit_ttl_failure"])
    spread_count = sum(
        1
        for row in report_rows
        if (row["option_spread_pct_at_fill"] is not None and row["option_spread_pct_at_fill"] > Decimal("0.10"))
    )
    forced_exit_count = sum(1 for row in report_rows if row["eod_forced"])
    forced_exit_pnl = sum(
        (row["realized_pnl"] for row in report_rows if row["eod_forced"] and row["realized_pnl"] is not None),
        Decimal("0"),
    )
    vwap_filtered_count = sum(
        1
        for row in report_rows
        if row.get("stale_reject_reason") and "below_vwap" in str(row["stale_reject_reason"])
    )
    orb_filtered_count = sum(
        1
        for row in report_rows
        if row.get("stale_reject_reason") and "below_orh" in str(row["stale_reject_reason"])
    )
    realized_pnl = sum(pnl_values, Decimal("0"))
    win_rate = (Decimal(len(wins)) / Decimal(len(closed_rows))) if closed_rows else None
    avg_win = (sum(wins, Decimal("0")) / Decimal(len(wins))) if wins else None
    avg_loss = (sum(losses, Decimal("0")) / Decimal(len(losses))) if losses else None
    gross_win = sum(wins, Decimal("0"))
    gross_loss = abs(sum(losses, Decimal("0")))
    profit_factor = (gross_win / gross_loss) if gross_loss else None
    max_loss = min(losses) if losses else None
    event_counts = {
        "spread filtered count": spread_count,
        "VWAP filtered count": vwap_filtered_count,
        "ORB filtered count": orb_filtered_count,
        "forced exit count": forced_exit_count,
        "ORDER_TTL_EXPIRED": _count_event(event_rows, "ORDER_TTL_EXPIRED"),
        "EXIT_TTL_FALLBACK": _count_event(event_rows, "EXIT_TTL_FALLBACK"),
        "EXIT_FORCED_LIQUIDATION": _count_event(event_rows, "EXIT_FORCED_LIQUIDATION"),
    }
    with md_path.open("w", encoding="utf-8") as handle:
        handle.write(f"# Open Chase Lifecycle Report: {batch_id}\n\n")
        handle.write(f"- trade count: {len(report_rows)}\n")
        handle.write(f"- clean trade count: {clean_count}\n")
        handle.write(f"- win rate: {_f(win_rate)}\n")
        handle.write(f"- avg win: {_f(avg_win)}\n")
        handle.write(f"- avg loss: {_f(avg_loss)}\n")
        handle.write(f"- profit factor: {_f(profit_factor)}\n")
        handle.write(f"- max loss: {_f(max_loss)}\n")
        handle.write(f"- stale fill count: {stale_count}\n")
        handle.write(f"- TTL failure count: {ttl_count}\n")
        handle.write(f"- spread filtered/flagged count: {spread_count}\n")
        handle.write(f"- forced exit PnL: {_f(forced_exit_pnl)}\n")
        handle.write("- unrealized / MTM PnL: 0 (EOD forced exit/mark rows are included when present)\n")
        handle.write(f"- realized PnL: {_f(realized_pnl)}\n")
        handle.write("- event counts:\n")
        for key, value in event_counts.items():
            handle.write(f"  - {key}: {value}\n")
        handle.write("\n")
        handle.write("| run | symbol | signal->fill | reject reason | above VWAP | near OR high | spread pct | tags | pnl | clean |\n")
        handle.write("|---:|---|---:|---|---|---|---:|---|---:|---|\n")
        for row in report_rows:
            handle.write(
                f"| {row['run_id']} | {row['symbol']} | {_f(row['signal_to_fill_seconds'])} | "
                f"{_f(row['stale_reject_reason'])} | {_f(row['above_vwap_at_fill'])} | "
                f"{_f(row['near_opening_high_at_fill'])} | {_f(row['option_spread_pct_at_fill'])} | "
                f"{row['pollution_tags']} | "
                f"{_f(row['realized_pnl'])} | {row['clean_trade']} |\n"
            )
    return csv_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate open-chase lifecycle report")
    parser.add_argument("batch_id")
    parser.add_argument("--output-dir", default="reports")
    args = parser.parse_args()
    csv_path, md_path = build_report(args.batch_id, Path(args.output_dir))
    print(csv_path)
    print(md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
