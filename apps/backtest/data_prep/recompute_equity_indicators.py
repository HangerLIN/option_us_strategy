from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone

import pandas as pd
from sqlalchemy import text

from apps.backtest.dao import BacktestDAO
from apps.backtest.universe import UniverseResolver
from apps.rt_engine.indicators import compute_indicators
from libs.core import EASTERN, configure_logging, get_settings
from libs.infra import get_session_factory


LOOKBACK_DAYS = 7


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute indicators_eq_1m from existing bars1m_equity rows."
    )
    parser.add_argument("--start", required=True, type=_parse_date, help="Start trade date.")
    parser.add_argument("--end", required=True, type=_parse_date, help="End trade date.")
    parser.add_argument("--symbols", nargs="+", help="Explicit symbol list.")
    parser.add_argument(
        "--universe",
        default="ref_market_cap:GLOBAL",
        help="Universe spec used when --symbols not provided.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Number of symbols to process before committing progress.",
    )
    return parser.parse_args()


def _resolve_symbols(args: argparse.Namespace, session_factory) -> list[str]:
    if args.symbols:
        return sorted({symbol.upper() for symbol in args.symbols})
    with session_factory() as session:
        resolver = UniverseResolver(session)
        return resolver.resolve(args.universe).symbols


def _load_prices(session, symbol: str, trade_date: date) -> pd.DataFrame:
    start_et = datetime.combine(trade_date, time(0, 0), tzinfo=EASTERN) - timedelta(
        days=LOOKBACK_DAYS
    )
    end_et = datetime.combine(trade_date + timedelta(days=1), time(0, 0), tzinfo=EASTERN)
    rows = session.execute(
        text(
            """
        SELECT ts_end, open, high, low, close, volume
        FROM bars1m_equity
        WHERE symbol = :symbol
          AND ts_end >= :start_ts
          AND ts_end < :end_ts
        ORDER BY ts_end ASC
        """
        ),
        {
            "symbol": symbol,
            "start_ts": start_et.astimezone(timezone.utc),
            "end_ts": end_et.astimezone(timezone.utc),
        },
    ).all()
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows, columns=["ts_end", "open", "high", "low", "close", "volume"])
    df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)
    df.set_index("ts_end", inplace=True)
    return df


def _target_window_mask(index: pd.Index, trade_date: date) -> pd.Series:
    start_et = datetime.combine(trade_date, time(0, 0), tzinfo=EASTERN)
    end_et = start_et + timedelta(days=1)
    start_utc = pd.Timestamp(start_et.astimezone(timezone.utc))
    end_utc = pd.Timestamp(end_et.astimezone(timezone.utc))
    return (index >= start_utc) & (index < end_utc)


def _batches(items: list[str], size: int):
    batch_size = max(1, int(size or 1))
    for index in range(0, len(items), batch_size):
        yield items[index : index + batch_size]


def main() -> int:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings)
    session_factory = get_session_factory(settings)

    symbols = _resolve_symbols(args, session_factory)
    if not symbols:
        raise SystemExit("No symbols resolved for recompute.")

    total_rows = 0
    total_symbol_days = 0
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        trade_dates = dao.fetch_trade_dates_between(start_date=args.start, end_date=args.end)
        if not trade_dates:
            raise SystemExit("No trading dates found in requested window.")

        for trade_date in trade_dates:
            day_rows = 0
            day_symbol_days = 0
            for batch in _batches(symbols, args.batch_size):
                for symbol in batch:
                    df_prices = _load_prices(session, symbol, trade_date)
                    if df_prices.empty:
                        continue
                    indicators = compute_indicators(
                        df_prices,
                        baseline_map=dao.fetch_rvol_baseline(symbol=symbol),
                    )
                    target = indicators.loc[_target_window_mask(indicators.index, trade_date)]
                    if target.empty:
                        continue
                    dao.upsert_equity_indicators_from_df(symbol=symbol, df=target)
                    day_rows += len(target)
                    day_symbol_days += 1
                session.commit()
                print(
                    f"{trade_date.isoformat()} processed batch of {len(batch)} symbols; "
                    f"day rows={day_rows}",
                    flush=True,
                )
            total_rows += day_rows
            total_symbol_days += day_symbol_days

    print(
        f"Recomputed indicators for {total_symbol_days} symbol-days, "
        f"upserted {total_rows} rows."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
