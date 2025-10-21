# path: scripts/ingest_option_chain_meta_ibkr.py
from __future__ import annotations

import argparse
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from queue import Empty
from typing import Iterable, List, Mapping, Sequence, Tuple

import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from apps.backtest.dao import BacktestDAO
from apps.backtest.universe import UniverseResolver
from libs.core import EASTERN, configure_logging, get_settings, trading_session_window
from libs.infra import build_ibkr_client, get_session_factory
from libs.infra.ibkr_client import IBClient

LOGGER = structlog.get_logger(__name__)


@dataclass(frozen=True)
class OptionQuote:
    conid: int
    symbol: str
    option_right: str
    expiry: datetime
    strike: Decimal
    dte: int
    bid: Decimal
    ask: Decimal
    mid: Decimal
    open_interest: int
    volume: int
    min_tick: Decimal
    delta: float | None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest option chain metadata via IBKR API")
    parser.add_argument("--date", help="Trade date (YYYY-MM-DD, Eastern)")
    parser.add_argument("--start", help="Start trade date (inclusive, YYYY-MM-DD)")
    parser.add_argument("--end", help="End trade date (inclusive, YYYY-MM-DD)")
    parser.add_argument("--symbols", nargs="+", help="Underlying symbols (e.g. AAPL SPY)")
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
    parser.add_argument("--dte-min", type=int, default=2, help="Minimum days to expiry (inclusive)")
    parser.add_argument("--dte-max", type=int, default=7, help="Maximum days to expiry (inclusive)")
    parser.add_argument(
        "--otm-min",
        type=float,
        default=2.0,
        help="Minimum OTM percentage (e.g. 2 means +2%% for call, -2%% for put)",
    )
    parser.add_argument(
        "--otm-max",
        type=float,
        default=5.0,
        help="Maximum OTM percentage (e.g. 5 means +5%% / -5%%)",
    )
    parser.add_argument(
        "--max-per-side",
        type=int,
        default=2,
        help="Maximum number of contracts to store per option side (CALL/PUT)",
    )
    parser.add_argument(
        "--market-data-type",
        type=int,
        default=4,
        choices=[1, 2, 3, 4],
        help="IBKR market data type (1=live, 2=frozen, 3=delayed, 4=delayed frozen). Default 4.",
    )
    args = parser.parse_args()
    if not args.date:
        if not (args.start and args.end):
            parser.error("either --date or both --start/--end must be provided")
    if args.date and (args.start or args.end):
        parser.error("--date cannot be combined with --start/--end")
    return args


def _parse_trade_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid --date {value!r}. Expected YYYY-MM-DD") from exc


def _parse_expiry(raw: str) -> datetime:
    token = raw.split(":")[0]
    dt = datetime.strptime(token, "%Y%m%d")
    return dt.replace(tzinfo=EASTERN)


def _market_snapshot(client: IBClient, contract, *, generic_ticks: str = "100,101,104,106"):
    return client.market_data_snapshot(contract, generic_ticks=generic_ticks, timeout=10.0)


def _resolve_spot(
    session_factory: sessionmaker[Session],
    client: IBClient,
    symbol: str,
    trade_date: date,
) -> float:
    start_et, end_et = trading_session_window(trade_date, tz=EASTERN)
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
    with session_factory() as session:
        row = session.execute(
            text(
                """
                SELECT close
                FROM bars1m_equity
                WHERE symbol = :symbol
                  AND ts_end >= :start_ts
                  AND ts_end < :end_ts
                ORDER BY ts_end DESC
                LIMIT 1
                """
            ),
            {"symbol": symbol, "start_ts": start_utc, "end_ts": end_utc},
        ).scalar()
        if row is not None:
            return float(row)
    try:
        bars = client.req_historical_1m_equity(symbol, trade_date, rth_only=True)
        if bars:
            return float(bars[-1]["close"])
    except Exception as exc:
        LOGGER.warning("spot.historical_failed symbol=%s error=%s", symbol, str(exc))
    stock_contract = client.stock_contract(symbol)
    snapshot = _market_snapshot(client, stock_contract, generic_ticks="")
    bid = snapshot.get("bid")
    ask = snapshot.get("ask")
    last = snapshot.get("last")
    if bid is not None and ask is not None and ask > 0:
        return float((bid + ask) / 2.0)
    if last:
        return float(last)
    raise RuntimeError(f"Unable to resolve spot price for {symbol}")


def _session_bounds(trade_date: date) -> Tuple[datetime, datetime]:
    start_et, end_et = trading_session_window(trade_date, tz=EASTERN)
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
    return start_utc, end_utc


def _stream_bid_ask(
    client: IBClient,
    contract,
    symbol: str,
    option_right: str,
    strike: float,
    *,
    timeout: float = 5.0,
) -> Tuple[float | None, float | None]:
    alias = f"SMOKE:{contract.conId}"
    req_id, queue = client.subscribe_l1(
        symbol,
        sec_type="OPT",
        contract=contract,
        generic_ticks="100,101,104,106",
        alias=alias,
    )
    bid = ask = None
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                payload = queue.get(timeout=min(0.5, remaining))
            except Empty:
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("type") == "price":
                field = payload.get("field")
                if field == 1:  # Bid price
                    bid = float(payload.get("price"))
                elif field == 2:  # Ask price
                    ask = float(payload.get("price"))
                if bid is not None and ask is not None:
                    break
    finally:
        client.unsubscribe_l1(alias)
    if bid is None or ask is None:
        LOGGER.debug(
            "option_stream.incomplete",
            symbol=symbol,
            right=option_right,
            strike=strike,
            bid=bid,
            ask=ask,
        )
    return bid, ask


def _choose_strikes(strikes: Iterable[float], spot: float, *, right: str, otm_min: float, otm_max: float) -> List[float]:
    strikes_sorted = sorted(set(strikes))
    if not strikes_sorted:
        return []
    lower_pct = otm_min / 100.0
    upper_pct = otm_max / 100.0
    chosen: List[float] = []
    for strike in strikes_sorted:
        if right == "CALL":
            if strike >= spot * (1 + lower_pct) and strike <= spot * (1 + upper_pct):
                chosen.append(strike)
        else:
            if strike <= spot * (1 - lower_pct) and strike >= spot * (1 - upper_pct):
                chosen.append(strike)
    if not chosen:
        # fallback to closest strikes if filters too strict
        if right == "CALL":
            chosen = [strike for strike in strikes_sorted if strike >= spot]
        else:
            chosen = [strike for strike in strikes_sorted if strike <= spot]
    return chosen


def _build_option_quotes(
    client: IBClient,
    *,
    symbol: str,
    trade_date: date,
    spot_price: float,
    expirations: Sequence[str],
    strikes: Sequence[float],
    exchange: str,
    trading_class: str | None,
    multiplier: str | None,
    dte_min: int,
    dte_max: int,
    otm_min: float,
    otm_max: float,
    max_per_side: int,
) -> List[OptionQuote]:
    quotes: List[OptionQuote] = []
    for expiry_raw in expirations:
        expiry_dt = _parse_expiry(expiry_raw)
        dte = (expiry_dt.date() - trade_date).days
        if dte < dte_min or dte > dte_max:
            continue
        for right in ("CALL", "PUT"):
            selected_strikes = _choose_strikes(strikes, spot_price, right=right, otm_min=otm_min, otm_max=otm_max)
            if not selected_strikes:
                continue
            if right == "CALL":
                selected_strikes = sorted(selected_strikes)
            else:
                selected_strikes = sorted(selected_strikes, reverse=True)
            selected_strikes = selected_strikes[:max_per_side]
            for strike in selected_strikes:
                contract = client.option_contract(
                    symbol=symbol,
                    expiry=expiry_dt.strftime("%Y%m%d"),
                    strike=strike,
                    right=right,
                    exchange=exchange or "SMART",
                    multiplier=multiplier or "100",
                )
                if trading_class:
                    contract.tradingClass = trading_class
                detail = client.req_contract_details(contract, timeout=15.0)
                if not detail:
                    LOGGER.warning("contract_details.empty symbol=%s expiry=%s strike=%s right=%s", symbol, expiry_dt.isoformat(), strike, right)
                    continue
                resolved = detail.contract
                conid = int(resolved.conId)
                min_tick = Decimal(str(detail.minTick or 0.01))
            snapshot = _market_snapshot(client, resolved, generic_ticks="")
            bid = snapshot.get("bid")
            ask = snapshot.get("ask")
            open_interest = snapshot.get("open_interest")
            volume = snapshot.get("option_volume")
            delta = snapshot.get("delta")
            if bid is None or ask is None:
                start_utc, end_utc = _session_bounds(trade_date)
                try:
                    l1_records = client.req_option_bid_ask_1m(
                        resolved,
                        start=start_utc,
                        end=end_utc,
                        use_rth=True,
                    )
                except Exception as exc:
                    LOGGER.warning(
                        "option_snapshot.no_liquidity",
                        symbol=symbol,
                        conid=conid,
                        right=right,
                        strike=strike,
                        reason=str(exc),
                    )
                    continue
                if not l1_records:
                    LOGGER.warning(
                        "option_snapshot.no_liquidity",
                        symbol=symbol,
                        conid=conid,
                        right=right,
                        strike=strike,
                        reason="empty_bid_ask_history",
                    )
                    continue
                bid = l1_records[0]["bid"]
                ask = l1_records[0]["ask"]
            bid_dec = Decimal(str(bid))
            ask_dec = Decimal(str(ask))
            if ask_dec <= 0 or bid_dec <= 0 or ask_dec <= bid_dec:
                continue
                mid_dec = (bid_dec + ask_dec) / Decimal("2")
                oi_val = int(open_interest) if open_interest is not None and not math.isnan(open_interest) else 0
                vol_val = int(volume) if volume is not None and not math.isnan(volume) else 0
                if oi_val <= 0 and vol_val <= 0:
                    LOGGER.warning("option_snapshot.no_liquidity symbol=%s conid=%s", symbol, conid)
                    continue
                quotes.append(
                    OptionQuote(
                        conid=conid,
                        symbol=symbol,
                        option_right=right,
                        expiry=expiry_dt,
                        strike=Decimal(str(strike)),
                        dte=dte,
                        bid=bid_dec,
                        ask=ask_dec,
                        mid=mid_dec,
                        open_interest=oi_val,
                        volume=vol_val,
                        min_tick=min_tick,
                        delta=float(delta) if delta is not None and not math.isnan(delta) else None,
                    )
                )
    return quotes


def _insert_quotes(session_factory: sessionmaker[Session], trade_date: date, quotes: Sequence[OptionQuote]) -> None:
    if not quotes:
        return
    insert_sql = text(
        """
        INSERT INTO option_chain_meta (
            conid,
            trade_date,
            underlying_symbol,
            expiry,
            strike,
            right,
            dte,
            delta,
            bid,
            ask,
            mid,
            open_interest,
            volume,
            min_tick
        ) VALUES (
            :conid,
            :trade_date,
            :symbol,
            :expiry,
            :strike,
            :right,
            :dte,
            :delta,
            :bid,
            :ask,
            :mid,
            :open_interest,
            :volume,
            :min_tick
        )
        ON CONFLICT (conid) DO UPDATE SET
            trade_date = EXCLUDED.trade_date,
            underlying_symbol = EXCLUDED.underlying_symbol,
            expiry = EXCLUDED.expiry,
            strike = EXCLUDED.strike,
            right = EXCLUDED.right,
            dte = EXCLUDED.dte,
            delta = EXCLUDED.delta,
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            mid = EXCLUDED.mid,
            open_interest = EXCLUDED.open_interest,
            volume = EXCLUDED.volume,
            min_tick = EXCLUDED.min_tick
        """
    )
    with session_factory() as session:
        for quote in quotes:
            session.execute(
                insert_sql,
                {
                    "conid": quote.conid,
                    "trade_date": trade_date,
                    "symbol": quote.symbol,
                    "expiry": quote.expiry,
                    "strike": quote.strike,
                    "right": quote.option_right,
                    "dte": quote.dte,
                    "delta": quote.delta,
                    "bid": quote.bid,
                    "ask": quote.ask,
                    "mid": quote.mid,
                    "open_interest": quote.open_interest,
                    "volume": quote.volume,
                    "min_tick": quote.min_tick,
                },
            )
        session.commit()


def ingest_option_chain_meta(
    client: IBClient,
    session_factory: sessionmaker[Session],
    *,
    trade_date: date,
    symbol: str,
    dte_min: int,
    dte_max: int,
    otm_min: float,
    otm_max: float,
    max_per_side: int,
) -> List[OptionQuote]:
    LOGGER.info("option_chain.start symbol=%s trade_date=%s", symbol, str(trade_date))
    stock_contract = client.stock_contract(symbol)
    stock_details = client.req_contract_details(stock_contract, timeout=15.0)
    if not stock_details:
        raise RuntimeError(f"No contract details available for {symbol}")
    underlying_conid = int(stock_details.contract.conId)
    spot_price = _resolve_spot(session_factory, client, symbol, trade_date)
    LOGGER.info("option_chain.spot symbol=%s spot=%s", symbol, spot_price)
    params = client.req_opt_params(symbol, underlying_conid=underlying_conid, exchange="", timeout=15.0)
    option_entries = [entry for entry in params if isinstance(entry, Mapping) and entry.get("expirations")]
    if not option_entries:
        raise RuntimeError(f"No option parameters returned for {symbol}")
    base_entry = option_entries[0]
    expirations = sorted(set(base_entry.get("expirations", [])))
    strikes = sorted(set(float(x) for x in base_entry.get("strikes", [])))
    exchange = base_entry.get("exchange") or "SMART"
    trading_class = base_entry.get("trading_class") or base_entry.get("tradingClass")
    multiplier = base_entry.get("multiplier")
    quotes = _build_option_quotes(
        client,
        symbol=symbol,
        trade_date=trade_date,
        spot_price=spot_price,
        expirations=expirations,
        strikes=strikes,
        exchange=exchange,
        trading_class=trading_class,
        multiplier=multiplier,
        dte_min=dte_min,
        dte_max=dte_max,
        otm_min=otm_min,
        otm_max=otm_max,
        max_per_side=max_per_side,
    )
    if not quotes:
        LOGGER.warning("option_chain.no_quotes symbol=%s", symbol)
        return []
    _insert_quotes(session_factory, trade_date, quotes)
    LOGGER.info("option_chain.saved symbol=%s contracts=%d", symbol, len(quotes))
    return quotes


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
            trade_dates = [_parse_trade_date(args.date)]
        else:
            start_date = _parse_trade_date(args.start)
            end_date = _parse_trade_date(args.end)
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
        "option_chain.schedule trade_dates=%s symbols=%d offset=%d limit=%s batch_size=%s",
        trade_dates=[d.isoformat() for d in trade_dates],
        symbols=len(symbols),
        offset=args.symbols_offset,
        limit=args.symbols_limit,
        batch_size=args.batch_size,
    )
    client = build_ibkr_client(settings)
    client.reqMarketDataType(args.market_data_type)
    try:
        total_quotes = 0
        if args.batch_size and int(args.batch_size) > 0:
            size = max(1, int(args.batch_size))
            batches = [
                symbols[index : index + size] for index in range(0, len(symbols), size)
            ]
        else:
            batches = [symbols]

        for batch_index, batch_symbols in enumerate(batches, start=1):
            LOGGER.info(
                "option_chain.batch_start index=%d/%d size=%d symbols=%s",
                batch_index,
                len(batches),
                len(batch_symbols),
                batch_symbols,
            )
            for trade_date in trade_dates:
                for symbol in batch_symbols:
                    try:
                        quotes = ingest_option_chain_meta(
                            client,
                            session_factory,
                            trade_date=trade_date,
                            symbol=symbol,
                            dte_min=args.dte_min,
                            dte_max=args.dte_max,
                            otm_min=args.otm_min,
                            otm_max=args.otm_max,
                            max_per_side=args.max_per_side,
                        )
                        total_quotes += len(quotes)
                    except Exception as exc:
                        LOGGER.error(
                            "option_chain.failed symbol=%s trade_date=%s error=%s",
                            symbol,
                            str(trade_date),
                            str(exc),
                        )
            LOGGER.info(
                "option_chain.batch_complete index=%d/%d processed=%d",
                batch_index,
                len(batches),
                len(batch_symbols),
            )
        LOGGER.info("option_chain.completed total=%d", total_quotes)
    finally:
        client.disconnect_and_stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
