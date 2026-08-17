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
    bid: Decimal | None
    ask: Decimal | None
    mid: Decimal | None
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
        "--rights",
        nargs="+",
        choices=["CALL", "PUT"],
        default=["CALL", "PUT"],
        help="Option rights to store (default CALL PUT)",
    )
    parser.add_argument(
        "--delta-min",
        type=float,
        default=None,
        help="Minimum absolute delta when delta is available",
    )
    parser.add_argument(
        "--delta-max",
        type=float,
        default=None,
        help="Maximum absolute delta when delta is available",
    )
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


def _safe_int(value: object | None) -> int:
    if value is None:
        return 0
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0
    if math.isnan(numeric):
        return 0
    return int(numeric)


def _safe_float(value: object | None) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


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


def _quote_probe_window(trade_date: date) -> Tuple[datetime, datetime]:
    _, end_et = trading_session_window(trade_date, tz=EASTERN)
    end_et = end_et + timedelta(minutes=1)
    start_et = end_et - timedelta(minutes=30)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


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


def _decimal_or_none(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        numeric = Decimal(str(value))
    except Exception:
        return None
    if not numeric.is_finite():
        return None
    return numeric


def _quote_from_values(
    bid_raw: object | None,
    ask_raw: object | None,
) -> Tuple[Decimal | None, Decimal | None, Decimal | None]:
    bid = _decimal_or_none(bid_raw)
    ask = _decimal_or_none(ask_raw)
    if bid is None or ask is None or ask <= 0 or ask < bid:
        return None, None, None
    return bid, ask, (bid + ask) / Decimal("2")


def _resolve_option_quote(
    client: IBClient,
    contract,
    trade_date: date,
    symbol: str,
    option_right: str,
    strike: float,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, int, int, float | None]:
    start_utc, end_utc = _quote_probe_window(trade_date)
    try:
        bars = client.req_option_bid_ask_1m(
            contract,
            start=start_utc,
            end=end_utc,
            use_rth=True,
        )
        for bar in reversed(bars):
            bid, ask, mid = _quote_from_values(bar.get("bid"), bar.get("ask"))
            if mid is not None:
                volume = _safe_int(bar.get("bid_size")) + _safe_int(bar.get("ask_size"))
                return bid, ask, mid, 0, volume, None
    except Exception as exc:
        LOGGER.warning(
            "option_quote.history_failed",
            symbol=symbol,
            right=option_right,
            strike=strike,
            error=str(exc),
        )

    try:
        snapshot = _market_snapshot(client, contract)
        bid, ask, mid = _quote_from_values(snapshot.get("bid"), snapshot.get("ask"))
        if mid is not None:
            return (
                bid,
                ask,
                mid,
                _safe_int(snapshot.get("open_interest")),
                _safe_int(snapshot.get("option_volume") or snapshot.get("volume")),
                _safe_float(snapshot.get("delta")),
            )
    except Exception as exc:
        LOGGER.warning(
            "option_quote.snapshot_failed",
            symbol=symbol,
            right=option_right,
            strike=strike,
            error=str(exc),
        )

    bid_float, ask_float = _stream_bid_ask(client, contract, symbol, option_right, strike)
    bid, ask, mid = _quote_from_values(bid_float, ask_float)
    return bid, ask, mid, 0, 0, None


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


def _select_option_entry(params: Sequence[Mapping], symbol: str) -> Mapping:
    symbol_up = symbol.upper()

    def score(entry: Mapping) -> tuple[int, int, int, int]:
        trading_class = str(entry.get("trading_class") or entry.get("tradingClass") or "").upper()
        exchange = str(entry.get("exchange") or "").upper()
        expirations = entry.get("expirations") or []
        strikes = entry.get("strikes") or []
        return (
            1 if trading_class == symbol_up else 0,
            1 if exchange == "SMART" else 0,
            len(expirations),
            len(strikes),
        )

    return max(params, key=score)


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
    rights: Sequence[str] = ("CALL", "PUT"),
    delta_min: float | None = None,
    delta_max: float | None = None,
    max_per_side: int,
) -> List[OptionQuote]:
    quotes: List[OptionQuote] = []
    for expiry_raw in expirations:
        expiry_dt = _parse_expiry(expiry_raw)
        dte = (expiry_dt.date() - trade_date).days
        if dte < dte_min or dte > dte_max:
            continue
        for right in rights:
            selected_strikes = _choose_strikes(strikes, spot_price, right=right, otm_min=otm_min, otm_max=otm_max)
            if not selected_strikes:
                continue
            if right == "CALL":
                selected_strikes = sorted(selected_strikes)
            else:
                selected_strikes = sorted(selected_strikes, reverse=True)
            saved_for_side = 0
            for strike in selected_strikes:
                if saved_for_side >= max_per_side:
                    break
                contract = client.option_contract(
                    symbol=symbol,
                    expiry=expiry_dt.strftime("%Y%m%d"),
                    strike=strike,
                    right=right,
                    exchange=exchange or "SMART",
                    multiplier=multiplier or "100",
                    include_expired=True,
                )
                if trading_class:
                    contract.tradingClass = trading_class
                try:
                    detail = client.req_contract_details(contract, timeout=15.0)
                except Exception as exc:
                    LOGGER.warning(
                        "contract_details.failed",
                        symbol=symbol,
                        expiry=expiry_dt.isoformat(),
                        strike=strike,
                        right=right,
                        reason=str(exc),
                    )
                    continue
                if not detail:
                    LOGGER.warning("contract_details.empty symbol=%s expiry=%s strike=%s right=%s", symbol, expiry_dt.isoformat(), strike, right)
                    continue
                resolved = detail.contract
                conid = int(resolved.conId)
                min_tick = Decimal(str(detail.minTick or 0.01))
                bid, ask, mid, open_interest, volume, delta = _resolve_option_quote(
                    client,
                    resolved,
                    trade_date,
                    symbol,
                    right,
                    strike,
                )
                if delta is not None:
                    abs_delta = abs(float(delta))
                    if delta_min is not None and abs_delta < delta_min:
                        continue
                    if delta_max is not None and abs_delta > delta_max:
                        continue
                quotes.append(
                    OptionQuote(
                        conid=conid,
                        symbol=symbol,
                        option_right=right,
                        expiry=expiry_dt,
                        strike=Decimal(str(strike)),
                        dte=dte,
                        bid=bid,
                        ask=ask,
                        mid=mid,
                        open_interest=open_interest,
                        volume=volume,
                        min_tick=min_tick,
                        delta=delta,
                    )
                )
                saved_for_side += 1
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
            "right",
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
        ON CONFLICT (trade_date, conid) DO UPDATE SET
            underlying_symbol = EXCLUDED.underlying_symbol,
            expiry = EXCLUDED.expiry,
            strike = EXCLUDED.strike,
            "right" = EXCLUDED."right",
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
    rights: Sequence[str],
    delta_min: float | None,
    delta_max: float | None,
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
    base_entry = _select_option_entry(option_entries, symbol)
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
        rights=rights,
        delta_min=delta_min,
        delta_max=delta_max,
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
                            rights=[right.upper() for right in args.rights],
                            delta_min=args.delta_min,
                            delta_max=args.delta_max,
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
