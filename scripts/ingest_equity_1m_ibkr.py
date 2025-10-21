# path: scripts/ingest_equity_1m_ibkr.py
from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Mapping, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from apps.rt_engine.indicators import compute_indicator_row
from apps.backtest.dao import BacktestDAO
from apps.backtest.universe import UniverseResolver
from libs.core import EASTERN, configure_logging, get_settings
from libs.infra import build_ibkr_client, get_session_factory

LOGGER = logging.getLogger("ingest_equity_1m")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest 1m equity bars from IBKR into TimescaleDB")
    parser.add_argument("--date", help="Single trade date in YYYY-MM-DD (Eastern)")
    parser.add_argument("--start", help="Start trade date (inclusive, YYYY-MM-DD)")
    parser.add_argument("--end", help="End trade date (inclusive, YYYY-MM-DD)")
    parser.add_argument("--symbols", nargs="+", help="Explicit equity symbols to ingest")
    parser.add_argument(
        "--universe",
        default=None,
        help="Universe specification (default ref_market_cap:GLOBAL when --symbols omitted)",
    )
    parser.add_argument(
        "--symbols-offset",
        type=int,
        default=0,
        help="Skip the first N symbols after universe resolution (default 0)",
    )
    parser.add_argument(
        "--symbols-limit",
        type=int,
        help="Limit number of symbols processed after applying offset",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Process symbols in batches of N; defaults to all symbols in one batch",
    )
    parser.add_argument(
        "--rth-only",
        type=int,
        default=1,
        choices=[0, 1],
        help="When set to 1 (default) only request Regular Trading Hours data",
    )
    args = parser.parse_args()
    if not args.date:
        if not (args.start and args.end):
            parser.error("either --date or both --start/--end must be provided")
    if args.date and (args.start or args.end):
        parser.error("--date cannot be combined with --start/--end")
    return args


def _to_trade_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid --date value {value!r}: must be YYYY-MM-DD") from exc


def _ib_epoch_to_ts_end(epoch: int) -> datetime:
    dt_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
    dt_et = dt_utc.astimezone(EASTERN).replace(second=0, microsecond=0)
    minute_end = dt_et + timedelta(minutes=1)
    return minute_end.astimezone(timezone.utc)


def _bars_to_records(bars: Sequence[Mapping[str, object]]) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for bar in bars:
        raw_time = bar.get("time")
        if raw_time is None:
            continue
        epoch = int(raw_time)
        ts_end = _ib_epoch_to_ts_end(epoch)
        record = {
            "ts_end": ts_end,
            "open": float(bar.get("open") or 0.0),
            "high": float(bar.get("high") or 0.0),
            "low": float(bar.get("low") or 0.0),
            "close": float(bar.get("close") or 0.0),
            "volume": int(bar.get("volume") or 0),
        }
        records.append(record)
    records.sort(key=lambda row: row["ts_end"])
    return records


def _expected_rth_minutes(trade_date: date) -> int:
    start = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
        hour=9, minute=30
    )
    end = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
        hour=16, minute=0
    )
    return int((end - start).total_seconds() / 60)


def _load_baseline_map(session: Session, symbol: str) -> Dict[int, Decimal]:
    rows = session.execute(
        text(
            "SELECT minute_index, mean_vol_20d FROM rvol_baseline_eq WHERE symbol = :symbol"
        ),
        {"symbol": symbol},
    ).all()
    return {int(row[0]): Decimal(str(row[1])) for row in rows}


def _upsert_indicator_row(
    session: Session,
    symbol: str,
    ts_end: datetime,
    df: pd.DataFrame,
    baseline_map: Mapping[int, Decimal],
) -> None:
    metrics = compute_indicator_row(df, baseline_map)
    if not metrics:
        return
    payload = {
        "symbol": symbol,
        "ts_end": ts_end,
        **{key: Decimal(str(value)) if value is not None else None for key, value in metrics.items()},
    }
    session.execute(
        text(
            """
            INSERT INTO indicators_eq_1m (
                symbol, ts_end,
                rsi6, rsi12, rsi24,
                boll_mid, boll_up, boll_dn,
                atr14, ao,
                stoch_k, stoch_d,
                cci14, cci6,
                obv, obv_ema20,
                mfi14, rvol6
            ) VALUES (
                :symbol, :ts_end,
                :rsi6, :rsi12, :rsi24,
                :boll_mid, :boll_up, :boll_dn,
                :atr14, :ao,
                :stoch_k, :stoch_d,
                :cci14, :cci6,
                :obv, :obv_ema20,
                :mfi14, :rvol6
            )
            ON CONFLICT (symbol, ts_end) DO UPDATE SET
                rsi6 = EXCLUDED.rsi6,
                rsi12 = EXCLUDED.rsi12,
                rsi24 = EXCLUDED.rsi24,
                boll_mid = EXCLUDED.boll_mid,
                boll_up = EXCLUDED.boll_up,
                boll_dn = EXCLUDED.boll_dn,
                atr14 = EXCLUDED.atr14,
                ao = EXCLUDED.ao,
                stoch_k = EXCLUDED.stoch_k,
                stoch_d = EXCLUDED.stoch_d,
                cci14 = EXCLUDED.cci14,
                cci6 = EXCLUDED.cci6,
                obv = EXCLUDED.obv,
                obv_ema20 = EXCLUDED.obv_ema20,
                mfi14 = EXCLUDED.mfi14,
                rvol6 = EXCLUDED.rvol6
            """
        ),
        payload,
    )


def _store_equity_rows(
    session_factory: sessionmaker[Session],
    symbol: str,
    records: Sequence[Mapping[str, object]],
) -> None:
    if not records:
        raise RuntimeError(f"No bars to persist for {symbol}")
    insert_sql = text(
        """
        INSERT INTO bars1m_equity (ts_end, symbol, open, high, low, close, volume)
        VALUES (:ts_end, :symbol, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, ts_end) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
    )
    with session_factory() as session:
        for row in records:
            session.execute(
                insert_sql,
                {
                    "ts_end": row["ts_end"],
                    "symbol": symbol,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                },
            )
        session.commit()


def _recompute_indicators(
    session_factory: sessionmaker[Session],
    symbol: str,
    trade_date: date,
) -> None:
    start_window = datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc) - timedelta(
        days=7
    )
    with session_factory() as session:
        rows = session.execute(
            text(
                """
                SELECT ts_end, open, high, low, close, volume
                FROM bars1m_equity
                WHERE symbol = :symbol
                  AND ts_end >= :start_window
                ORDER BY ts_end ASC
                """
            ),
            {
                "symbol": symbol,
                "start_window": start_window,
            },
        ).all()
        if not rows:
            return
        df = pd.DataFrame(rows, columns=["ts_end", "open", "high", "low", "close", "volume"])
        df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)
        df.set_index("ts_end", inplace=True)
        baseline_map = _load_baseline_map(session, symbol)
        for idx, (ts_end, _) in enumerate(df.iterrows(), start=1):
            window_df = df.iloc[:idx]
            _upsert_indicator_row(session, symbol, ts_end, window_df, baseline_map)
        session.commit()


def _refresh_materialized_views(session_factory: sessionmaker[Session]) -> None:
    matviews = ("mv_daily_ohlcv_bounds", "mv_daily_ind_last")
    with session_factory() as session:
        for mv_name in matviews:
            exists = session.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_matviews
                        WHERE schemaname = 'public' AND matviewname = :name
                    )
                    """
                ),
                {"name": mv_name},
            ).scalar()
            if not exists:
                continue
            LOGGER.info("Refreshing materialized view %s", mv_name)
            session.execute(text(f"REFRESH MATERIALIZED VIEW {mv_name}"))
        session.commit()


def _validate_rth_count(
    session_factory: sessionmaker[Session],
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    expected: int,
) -> None:
    with session_factory() as session:
        count = session.execute(
            text(
                """
                SELECT COUNT(*) FROM bars1m_equity
                WHERE symbol = :symbol
                  AND ts_end >= :start_ts
                  AND ts_end < :end_ts
                """
            ),
            {
                "symbol": symbol,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        ).scalar()
    if count is None or count < expected:
        raise RuntimeError(
            f"{symbol} RTH minute count insufficient ({count or 0}/{expected}) "
            f"for window {start_ts.isoformat()} – {end_ts.isoformat()}"
        )


def _request_equity_bars_with_retry(
    client,
    *,
    symbol: str,
    trade_date: date,
    rth_only: bool,
    max_attempts: int = 4,
    backoff_seconds: float = 5.0,
):
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return client.req_historical_1m_equity(symbol, trade_date, rth_only=rth_only)
        except Exception as exc:  # pragma: no cover - network/ib failure
            last_exc = exc
            wait_time = backoff_seconds * attempt
            LOGGER.warning(
                "ingest_equity.retry symbol=%s trade_date=%s attempt=%d/%d wait=%ds error=%s",
                symbol,
                trade_date.isoformat(),
                attempt,
                max_attempts,
                wait_time,
                str(exc),
            )
            if attempt == max_attempts:
                break
            time.sleep(wait_time)
    raise RuntimeError(f"Failed to fetch historical data for {symbol}: {last_exc}")


def ingest_equity(
    client,
    session_factory: sessionmaker[Session],
    trade_date: date,
    symbol: str,
    *,
    rth_only: bool,
) -> None:
    LOGGER.info("Fetching 1m bars symbol=%s trade_date=%s", symbol, trade_date.isoformat())
    bars = _request_equity_bars_with_retry(
        client,
        symbol=symbol,
        trade_date=trade_date,
        rth_only=rth_only,
    )
    records = _bars_to_records(bars)
    if not records:
        raise RuntimeError(f"IBKR returned no bars for {symbol} on {trade_date}")
    _store_equity_rows(session_factory, symbol, records)
    _recompute_indicators(session_factory, symbol, trade_date)

    start_et = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
        hour=9, minute=30
    )
    end_et = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
        hour=16, minute=0
    )
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
    expected = max(380, _expected_rth_minutes(trade_date))
    _validate_rth_count(session_factory, symbol, start_utc, end_utc, expected)
    LOGGER.info("Ingest completed symbol=%s records=%s", symbol, len(records))


def main() -> int:
    args = _parse_args()

    settings = get_settings()
    configure_logging(settings)
    session_factory = get_session_factory(settings)
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        if args.date:
            trade_dates = [_to_trade_date(args.date)]
        else:
            start_date = _to_trade_date(args.start)
            end_date = _to_trade_date(args.end)
            if end_date < start_date:
                raise SystemExit("--end must not be earlier than --start")
            trade_dates = dao.fetch_trade_dates_between(start_date=start_date, end_date=end_date)
        if not trade_dates:
            raise SystemExit("No trading dates available in the requested window")

        if args.symbols:
            symbols = [symbol.upper() for symbol in args.symbols]
        else:
            resolver = UniverseResolver(session)
            universe = resolver.resolve(args.universe)
            symbols = universe.symbols
        if not symbols:
            raise SystemExit("Symbol universe resolved to zero entries")
        offset = max(0, int(args.symbols_offset))
        if offset:
            symbols = symbols[offset:]
        if args.symbols_limit is not None:
            limit = max(0, int(args.symbols_limit))
            symbols = symbols[:limit]
        if not symbols:
            raise SystemExit("No symbols remain after applying offset/limit")

    LOGGER.info(
        "ingest_equity.schedule trade_dates=%s symbols=%d offset=%d limit=%s batch_size=%s",
        [d.isoformat() for d in trade_dates],
        len(symbols),
        args.symbols_offset,
        args.symbols_limit,
        args.batch_size,
    )
    client = build_ibkr_client(settings)
    try:
        batches: list[list[str]]
        if args.batch_size and int(args.batch_size) > 0:
            size = max(1, int(args.batch_size))
            batches = [
                symbols[index : index + size] for index in range(0, len(symbols), size)
            ]
        else:
            batches = [symbols]

        for batch_index, batch_symbols in enumerate(batches, start=1):
            LOGGER.info(
                "ingest_equity.batch_start index=%d/%d size=%d symbols=%s",
                batch_index,
                len(batches),
                len(batch_symbols),
                batch_symbols,
            )
            for trade_date in trade_dates:
                for symbol in batch_symbols:
                    try:
                        ingest_equity(
                            client=client,
                            session_factory=session_factory,
                            trade_date=trade_date,
                            symbol=symbol,
                            rth_only=bool(args.rth_only),
                        )
                    except Exception as exc:  # pragma: no cover - network failure path
                        LOGGER.error(
                            "ingest_equity.symbol_failed symbol=%s trade_date=%s error=%s",
                            symbol,
                            trade_date.isoformat(),
                            str(exc),
                        )
                        continue
            LOGGER.info(
                "ingest_equity.batch_complete index=%d/%d processed=%d",
                batch_index,
                len(batches),
                len(batch_symbols),
            )
        _refresh_materialized_views(session_factory)
    finally:
        client.disconnect_and_stop()
    LOGGER.info("All symbols ingested successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
