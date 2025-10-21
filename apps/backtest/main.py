from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import Iterable, List
from uuid import uuid4

import structlog
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.backtest.bt_runner import run_backtest
from apps.backtest.dao import BacktestDAO
from apps.backtest.pipeline.premarket import PremarketTop5Builder
from apps.backtest.universe import UniverseResolver
from apps.signal_svc.top5_source import BacktestTop5Source
from libs.core import EASTERN, configure_logging, get_settings

LOGGER = structlog.get_logger(__name__)


def _parse_datetime(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            raise ValueError("timestamp must include timezone offset")
        return dt
    except ValueError as exc:  # pragma: no cover - validation
        raise argparse.ArgumentTypeError(f"Invalid ISO datetime: {value}") from exc


def _deduplicate(symbols: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in symbols:
        key = symbol.upper()
        if key and key not in seen:
            ordered.append(key)
            seen.add(key)
    return ordered


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run backtests using TimescaleDB data")
    parser.add_argument(
        "--start", type=_parse_datetime, required=True, help="Inclusive start time (ISO-8601)"
    )
    parser.add_argument(
        "--end", type=_parse_datetime, required=True, help="Exclusive end time (ISO-8601)"
    )
    parser.add_argument(
        "--universe",
        default=None,
        help="Universe specification (default ref_market_cap:GLOBAL)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Optional debug override: restrict universe to these symbols",
    )
    parser.add_argument("--batch-id", default=None, help="Optional batch identifier override")
    parser.add_argument("--mode", default="logic", help="Reserved for future strategy variants")
    parser.add_argument(
        "--signal-mode",
        default="recompute",
        choices=["recompute", "db"],
        help="Signal source mode",
    )
    parser.add_argument(
        "--track",
        default="option",
        choices=["option", "equity"],
        help="Execution track: option (default) or equity-only (Track A)",
    )
    parser.add_argument(
        "--metrics-mode",
        default="fixed",
        choices=["fixed", "tfe", "both"],
        help="Equity metrics aggregation: fixed horizon, entry→first-exit (tfe), or both",
    )
    parser.add_argument(
        "--risk-mode",
        default="inproc",
        choices=["inproc", "http"],
        help="Risk precheck backend",
    )
    parser.add_argument(
        "--paper", type=int, choices=[0, 1], default=1, help="Reserved flag for paper/live modes"
    )
    args_list = list(argv) if argv is not None else None
    return parser.parse_args(args_list)


def _derive_batch_id(args: argparse.Namespace) -> str:
    if args.batch_id:
        return args.batch_id
    start_tag = args.start.astimezone(EASTERN).strftime("%Y%m%d")
    end_tag = (args.end - timedelta(seconds=1)).astimezone(EASTERN).strftime("%Y%m%d")
    return f"bt-{start_tag}-{end_tag}-{uuid4().hex[:6]}"


def _date_range_bounds(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    if end <= start:
        raise SystemExit("--end must be after --start")
    start_et = start.astimezone(EASTERN)
    end_adjusted = (end - timedelta(minutes=1)).astimezone(EASTERN)
    if end_adjusted < start_et:
        end_adjusted = start_et
    return start_et, end_adjusted


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    configure_logging(settings)

    batch_id = _derive_batch_id(args)
    start_et, end_et = _date_range_bounds(args.start, args.end)

    LOGGER.info(
        "backtest.start",
        batch=batch_id,
        start=args.start.isoformat(),
        end=args.end.isoformat(),
        signal_mode=args.signal_mode,
        risk_mode=args.risk_mode,
        universe=args.universe or "ref_market_cap:GLOBAL",
        track=args.track,
        metrics_mode=args.metrics_mode,
    )

    engine = create_engine(settings.database_url)
    SessionLocal = sessionmaker(bind=engine)

    trade_dates: list[date]
    universe_symbols: list[str]
    universe_code: str

    with SessionLocal() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        trade_dates = dao.fetch_trade_dates_between(
            start_date=start_et.date(), end_date=end_et.date()
        )
        if not trade_dates:
            raise SystemExit("No trading dates found in the requested window")

        resolver = UniverseResolver(session)
        universe = resolver.resolve(args.universe)
        universe_code = universe.code
        if args.symbols:
            universe_symbols = _deduplicate(args.symbols)
            universe_code = f"{universe_code}|manual"
        else:
            universe_symbols = universe.symbols

        if not universe_symbols:
            raise SystemExit("Universe resolved to zero symbols")

        builder = PremarketTop5Builder(
            session,
            preearn_days_min=settings.preearn_days_min,
            preearn_days_max=settings.preearn_days_max,
            preearn_atr_pct_max=settings.preearn_atr_pct_max,
        )

        observed_symbols: list[str] = []
        observed_set: set[str] = set()
        try:
            for trade_date in trade_dates:
                rows = builder.build_for_date(
                    batch_id=batch_id,
                    trade_date=trade_date,
                    symbols=universe_symbols,
                    universe_code=universe_code,
                )
                for row in rows:
                    symbol = str(row["symbol"]).upper()
                    if symbol not in observed_set:
                        observed_set.add(symbol)
                        observed_symbols.append(symbol)
                LOGGER.info(
                    "backtest.top5_cached",
                    trade_date=trade_date.isoformat(),
                    count=len(rows),
                )
            session.commit()
        except Exception:
            session.rollback()
            raise

    requested_symbols = _deduplicate(args.symbols or [])
    if requested_symbols:
        final_symbols = requested_symbols[:]
        seen = set(final_symbols)
        for symbol in observed_symbols:
            if symbol not in seen:
                final_symbols.append(symbol)
                seen.add(symbol)
    else:
        final_symbols = observed_symbols[:]

    if not final_symbols:
        LOGGER.warning("backtest.no_symbols", reason="top5_empty", batch=batch_id)
        return 0

    LOGGER.info(
        "backtest.symbols_resolved",
        batch=batch_id,
        symbols=final_symbols,
        trade_dates=[d.isoformat() for d in trade_dates],
        track=args.track,
    )

    top5_source = BacktestTop5Source(batch_id=batch_id)

    run_ids: List[int] = []
    for symbol in final_symbols:
        run_id = run_backtest(
            symbol=symbol,
            start=args.start,
            end=args.end,
            signal_mode=args.signal_mode,
            risk_mode=args.risk_mode,
            track=args.track,
            top5_source=top5_source,
            batch_id=batch_id,
            universe_code=universe_code,
            trade_dates=trade_dates,
        )
        run_ids.append(run_id)
        LOGGER.info("backtest.completed", symbol=symbol, run_id=run_id, batch=batch_id)

    LOGGER.info("backtest.all_completed", run_ids=run_ids, batch=batch_id)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
