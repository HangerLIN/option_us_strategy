from __future__ import annotations

import argparse
from datetime import date, datetime, time, timezone
from typing import Iterable, Sequence

from sqlalchemy.orm import sessionmaker

from apps.backtest.dao import BacktestDAO
from apps.backtest.universe import UniverseResolver
from libs.core import EASTERN, configure_logging, get_settings
from libs.infra import build_ibkr_client, get_session_factory
from scripts.ingest_equity_1m_ibkr import (
    _bars_to_records,
    _recompute_indicators,
    _store_equity_rows,
)


def _parse_time(value: str) -> time:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid time {value!r}, expected HH:MM") from exc


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill premarket minute bars (e.g. 09:25 ET) for equity symbols."
    )
    parser.add_argument(
        "--start",
        required=True,
        type=_parse_date,
        help="Start trade date (inclusive, YYYY-MM-DD ET).",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=_parse_date,
        help="End trade date (inclusive, YYYY-MM-DD ET).",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Explicit symbols to backfill (overrides --universe).",
    )
    parser.add_argument(
        "--universe",
        default="ref_market_cap:GLOBAL",
        help="Universe spec used when --symbols not provided.",
    )
    parser.add_argument(
        "--window-start",
        default="09:15",
        type=_parse_time,
        help="Premarket window start time in ET (default 09:15).",
    )
    parser.add_argument(
        "--window-end",
        default="09:31",
        type=_parse_time,
        help="Premarket window end time in ET, exclusive (default 09:31).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of symbols processed per IB connection batch (default 10).",
    )
    return parser.parse_args()


def _iter_trade_dates(start_date: date, end_date: date, dao: BacktestDAO) -> list[date]:
    if end_date < start_date:
        raise SystemExit("--end must not be earlier than --start")
    return dao.fetch_trade_dates_between(start_date=start_date, end_date=end_date)


def _resolve_symbols(
    args: argparse.Namespace,
    session_factory: sessionmaker,
) -> list[str]:
    if args.symbols:
        return sorted({symbol.upper() for symbol in args.symbols})
    with session_factory() as session:
        resolver = UniverseResolver(session)
        universe = resolver.resolve(args.universe)
        return universe.symbols


def _backfill_window(
    *,
    session_factory: sessionmaker,
    client,
    symbol: str,
    trade_date: date,
    start_time: time,
    end_time: time,
) -> int:
    from datetime import timedelta
    start_et = datetime.combine(trade_date, start_time, tzinfo=EASTERN)
    end_et = datetime.combine(trade_date, end_time, tzinfo=EASTERN)
    # IBKR endDateTime 包含该分钟，所以需要减1分钟以获取正确的范围
    # 例如：要获取 [09:25, 09:30]，需要 endDateTime = 09:30（而不是 09:31）
    end_et = end_et - timedelta(minutes=1)
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = end_et.astimezone(timezone.utc)

    bars: Sequence[dict[str, object]] = client.req_historical_1m(
        symbol=symbol,
        start=start_utc,
        end=end_utc,
        use_rth=False,
    )
    records = _bars_to_records(bars)
    if not records:
        return 0
    # Filter once more inside the window (IB may return trailing minute equal to end_ts)
    filtered = [row for row in records if start_utc <= row["ts_end"] < end_utc]
    if not filtered:
        return 0
    _store_equity_rows(session_factory, symbol, filtered)
    _recompute_indicators(session_factory, symbol, trade_date)
    return len(filtered)


def _batched(iterable: Sequence[str], batch_size: int) -> Iterable[Sequence[str]]:
    if batch_size <= 0:
        batch_size = len(iterable)
    for idx in range(0, len(iterable), batch_size):
        yield iterable[idx : idx + batch_size]


def main() -> int:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings)
    session_factory = get_session_factory(settings)

    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        trade_dates = _iter_trade_dates(args.start, args.end, dao)
        if not trade_dates:
            raise SystemExit("No trading dates found in requested window.")

    symbols = _resolve_symbols(args, session_factory)
    if not symbols:
        raise SystemExit("No symbols resolved for backfill.")

    total_records = 0
    for batch in _batched(symbols, args.batch_size):
        client = build_ibkr_client(settings)
        try:
            for symbol in batch:
                batch_total = 0
                for trade_date in trade_dates:
                    inserted = _backfill_window(
                        session_factory=session_factory,
                        client=client,
                        symbol=symbol,
                        trade_date=trade_date,
                        start_time=args.window_start,
                        end_time=args.window_end,
                    )
                    batch_total += inserted
                total_records += batch_total
        finally:
            client.disconnect()
    print(f"Backfill complete. Total records inserted: {total_records}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
