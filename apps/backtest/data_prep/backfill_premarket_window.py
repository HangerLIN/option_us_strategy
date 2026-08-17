from __future__ import annotations

import argparse
import logging
import time as time_module
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, Sequence

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from apps.backtest.dao import BacktestDAO
from apps.backtest.universe import UniverseResolver
from libs.core import EASTERN, configure_logging, get_settings
from libs.infra import build_ibkr_client, get_session_factory
from apps.backtest.data_prep.ingest_equity_1m_ibkr import (
    _bars_to_records,
    _recompute_indicators,
    _store_equity_rows,
)

LOGGER = logging.getLogger("backfill_window")


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
        help="Premarket window end time in ET, inclusive (default 09:31).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of symbols processed per IB connection batch (default 10).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch the window even when bars and indicators already look complete.",
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


def _window_bounds_utc(trade_date: date, start_time: time, end_time: time) -> tuple[datetime, datetime]:
    start_et = datetime.combine(trade_date, start_time, tzinfo=EASTERN)
    end_et = datetime.combine(trade_date, end_time, tzinfo=EASTERN)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


def _expected_minutes(start_utc: datetime, end_utc: datetime) -> int:
    return max(1, int((end_utc - start_utc).total_seconds() // 60))


def _count_rows(
    session_factory: sessionmaker,
    table_name: str,
    symbol: str,
    start_utc: datetime,
    end_utc: datetime,
) -> int:
    with session_factory() as session:
        return int(
            session.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE symbol = :symbol
                      AND ts_end > :start_ts
                      AND ts_end <= :end_ts
                    """
                ),
                {"symbol": symbol, "start_ts": start_utc, "end_ts": end_utc},
            ).scalar()
            or 0
        )


def _backfill_window(
    *,
    session_factory: sessionmaker,
    client,
    symbol: str,
    trade_date: date,
    start_time: time,
    end_time: time,
    force: bool,
) -> int:
    start_utc, end_utc = _window_bounds_utc(trade_date, start_time, end_time)
    expected = _expected_minutes(start_utc, end_utc)

    existing_bars = _count_rows(session_factory, "bars1m_equity", symbol, start_utc, end_utc)
    existing_indicators = _count_rows(
        session_factory, "indicators_eq_1m", symbol, start_utc, end_utc
    )
    if not force and existing_bars >= expected:
        if existing_indicators < expected:
            LOGGER.info(
                "recompute indicators symbol=%s trade_date=%s indicators=%d expected=%d",
                symbol,
                trade_date.isoformat(),
                existing_indicators,
                expected,
            )
            _recompute_indicators(session_factory, symbol, trade_date)
        else:
            LOGGER.info(
                "skip complete symbol=%s trade_date=%s bars=%d expected=%d",
                symbol,
                trade_date.isoformat(),
                existing_bars,
                expected,
            )
        return 0

    bars: Sequence[dict[str, object]] = client.req_historical_1m(
        symbol=symbol,
        start=start_utc,
        end=end_utc,
        use_rth=False,
    )
    records = _bars_to_records(bars)
    if not records:
        return 0
    start_et_naive = start_utc.astimezone(EASTERN).replace(tzinfo=None)
    end_et_naive = end_utc.astimezone(EASTERN).replace(tzinfo=None)
    filtered = [row for row in records if start_et_naive < row["ts_end"] <= end_et_naive]
    if not filtered:
        return 0
    _store_equity_rows(session_factory, symbol, filtered)
    _recompute_indicators(session_factory, symbol, trade_date)
    LOGGER.info(
        "backfilled symbol=%s trade_date=%s records=%d",
        symbol,
        trade_date.isoformat(),
        len(filtered),
    )
    return len(filtered)


def _backfill_window_with_retry(
    *,
    session_factory: sessionmaker,
    client,
    symbol: str,
    trade_date: date,
    start_time: time,
    end_time: time,
    force: bool,
    max_attempts: int = 4,
) -> int:
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return _backfill_window(
                session_factory=session_factory,
                client=client,
                symbol=symbol,
                trade_date=trade_date,
                start_time=start_time,
                end_time=end_time,
                force=force,
            )
        except Exception as exc:
            last_exc = exc
            wait_seconds = 5 * attempt
            LOGGER.warning(
                "backfill retry symbol=%s trade_date=%s attempt=%d/%d wait=%ds error=%s",
                symbol,
                trade_date.isoformat(),
                attempt,
                max_attempts,
                wait_seconds,
                exc,
            )
            if attempt == max_attempts:
                break
            time_module.sleep(wait_seconds)
    raise RuntimeError(f"Failed to backfill {symbol} {trade_date}: {last_exc}")


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
                    inserted = _backfill_window_with_retry(
                        session_factory=session_factory,
                        client=client,
                        symbol=symbol,
                        trade_date=trade_date,
                        start_time=args.window_start,
                        end_time=args.window_end,
                        force=args.force,
                    )
                    batch_total += inserted
                total_records += batch_total
        finally:
            client.disconnect_and_stop()
    print(f"Backfill complete. Total records inserted: {total_records}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
