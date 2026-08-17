# path: scripts/ingest_option_l1_ibkr.py
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Mapping, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from apps.backtest.dao import BacktestDAO, OptionCandidateRow
from libs.core import EASTERN, configure_logging, get_settings, trading_session_window
from libs.infra import build_ibkr_client, get_session_factory
from libs.infra.ibkr_client import IBClient

LOGGER = logging.getLogger("ingest_option_l1")


@dataclass
class ContractDescriptor:
    symbol: str
    option_right: str
    expiry: str
    strike: float
    conid: int | None
    min_tick: float
    open_interest: int | None = None
    volume: int | None = None

    @classmethod
    def from_candidate(cls, candidate: OptionCandidateRow) -> "ContractDescriptor":
        expiry = candidate.expiry
        if isinstance(expiry, datetime):
            expiry_str = expiry.strftime("%Y%m%d")
        elif expiry is not None:
            expiry_str = datetime.combine(expiry, datetime.min.time()).strftime("%Y%m%d")
        else:
            raise ValueError("Candidate missing expiry")
        return cls(
            symbol=candidate.underlying_symbol,
            option_right=(candidate.option_right or "").upper(),
            expiry=expiry_str,
            strike=float(candidate.strike),
            conid=int(candidate.conid) if candidate.conid is not None else None,
            min_tick=float(candidate.min_tick),
            open_interest=int(candidate.open_interest or 0),
            volume=int(candidate.volume or 0),
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest option L1 BID/ASK 1m bars from IBKR into TimescaleDB"
    )
    parser.add_argument("--date", required=True, help="Trade date in YYYY-MM-DD (Eastern)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--underlyings", nargs="+", help="Underlying symbols to auto-select contracts")
    group.add_argument(
        "--contracts-file",
        help="Path to JSON file containing pre-selected contracts (list of descriptors)",
    )
    parser.add_argument("--dte-min", type=int, default=2, help="Minimum DTE inclusive (default 2)")
    parser.add_argument("--dte-max", type=int, default=7, help="Maximum DTE inclusive (default 7)")
    parser.add_argument(
        "--rights",
        nargs="+",
        choices=["CALL", "PUT"],
        default=["CALL", "PUT"],
        help="Option rights to auto-select when using --underlyings (default CALL PUT)",
    )
    parser.add_argument(
        "--delta-min",
        type=float,
        default=None,
        help="Minimum absolute delta when candidate delta is available",
    )
    parser.add_argument(
        "--delta-max",
        type=float,
        default=None,
        help="Maximum absolute delta when candidate delta is available",
    )
    parser.add_argument(
        "--otm-min",
        type=float,
        default=2.0,
        help="Minimum OTM percentage (calls strike/spot - 1) * 100, puts (spot-strike)/spot * 100",
    )
    parser.add_argument(
        "--otm-max",
        type=float,
        default=5.0,
        help="Maximum OTM percentage boundary",
    )
    parser.add_argument(
        "--output-contracts",
        help="Optional path to dump the selected contracts JSON",
    )
    parser.add_argument(
        "--market-data-type",
        type=int,
        choices=[1, 2, 3, 4],
        default=4,
        help="IBKR market data type (1=live,2=frozen,3=delayed,4=delayed frozen)",
    )
    parser.add_argument(
        "--min-minutes",
        type=int,
        default=300,
        help="Minimum number of 1m L1 candles required per option (default 300)",
    )
    parser.add_argument(
        "--window-start-et",
        help="Optional explicit start time in HH:MM Eastern for the L1 request window.",
    )
    parser.add_argument(
        "--window-end-et",
        help="Optional explicit end time in HH:MM Eastern for the L1 request window.",
    )
    return parser.parse_args()


def _parse_trade_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid --date {value!r}, expected YYYY-MM-DD") from exc


def _parse_hhmm(value: str) -> tuple[int, int]:
    try:
        parsed = datetime.strptime(value, "%H:%M").time()
    except ValueError as exc:
        raise SystemExit(f"Invalid time {value!r}, expected HH:MM") from exc
    return parsed.hour, parsed.minute


def _load_contracts_from_file(path: Path) -> List[ContractDescriptor]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SystemExit(f"Contracts file {path} not found") from exc
    if not isinstance(raw, list):
        raise SystemExit("Contracts file must contain a JSON list of contract descriptors")
    descriptors: List[ContractDescriptor] = []
    for item in raw:
        try:
            descriptors.append(
                ContractDescriptor(
                    symbol=str(item["symbol"]).upper(),
                    option_right=str(item["option_right"]).upper(),
                    expiry=str(item["expiry"]),
                    strike=float(item["strike"]),
                    conid=int(item["conid"]) if item.get("conid") is not None else None,
                    min_tick=float(item["min_tick"]),
                    open_interest=int(item["open_interest"]) if item.get("open_interest") is not None else None,
                    volume=int(item["volume"]) if item.get("volume") is not None else None,
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SystemExit(f"Invalid contract descriptor: {item!r}") from exc
    return descriptors


def _spot_close_price(session: Session, symbol: str, start: datetime, end: datetime) -> float:
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
        {
            "symbol": symbol,
            "start_ts": start,
            "end_ts": end,
        },
    ).first()
    if row is None or row[0] is None:
        raise RuntimeError(f"Missing equity close price for {symbol}")
    return float(row[0])


def _spread_ok(bid: Decimal, ask: Decimal, mid: Decimal, min_tick: Decimal) -> bool:
    if ask <= bid:
        return False
    spread = ask - bid
    threshold = max(Decimal("0.10"), mid * Decimal("0.05")) + min_tick
    return spread <= threshold


def _otm_percentage(option_right: str, strike: Decimal, spot: Decimal) -> float:
    if spot <= 0:
        return 0.0
    if option_right == "CALL":
        return float((strike - spot) / spot * Decimal("100"))
    return float((spot - strike) / spot * Decimal("100"))


def _filter_candidates(
    candidates: Sequence[OptionCandidateRow],
    *,
    option_right: str,
    spot_price: float,
    otm_min: float,
    otm_max: float,
    delta_min: Decimal | None = None,
    delta_max: Decimal | None = None,
    require_activity: bool = True,
) -> List[ContractDescriptor]:
    filtered: List[ContractDescriptor] = []
    spot_dec = Decimal(str(spot_price))
    for cand in candidates:
        bid = cand.bid
        ask = cand.ask
        mid = cand.mid
        min_tick = cand.min_tick
        strike = cand.strike
        oi = cand.open_interest
        vol = cand.volume
        if None in (min_tick, strike):
            continue
        has_quote = None not in (bid, ask, mid)
        if has_quote:
            bid_d = Decimal(str(bid))
            ask_d = Decimal(str(ask))
            mid_d = Decimal(str(mid))
        else:
            bid_d = ask_d = mid_d = Decimal("0")
        min_tick_d = Decimal(str(min_tick))
        strike_d = Decimal(str(strike))
        oi_d = Decimal(str(oi or 0))
        vol_d = Decimal(str(vol or 0))
        if require_activity and (oi_d < Decimal("500") or vol_d < Decimal("100")):
            continue
        if has_quote and not _spread_ok(bid_d, ask_d, mid_d, min_tick_d):
            continue
        otm_pct = _otm_percentage(option_right, strike_d, spot_dec)
        if otm_pct < otm_min or otm_pct > otm_max:
            continue
        if cand.delta is not None:
            delta_abs = abs(Decimal(str(cand.delta)))
            if delta_min is not None and delta_abs < delta_min:
                continue
            if delta_max is not None and delta_abs > delta_max:
                continue
        desc = ContractDescriptor.from_candidate(cand)
        filtered.append(desc)
    return filtered


def _select_contracts_for_symbol(
    dao: BacktestDAO,
    session: Session,
    symbol: str,
    trade_date: date,
    *,
    dte_min: int,
    dte_max: int,
    otm_min: float,
    otm_max: float,
    rights: Sequence[str],
    delta_min: Decimal | None,
    delta_max: Decimal | None,
) -> List[ContractDescriptor]:
    start_et, end_et = trading_session_window(trade_date, tz=EASTERN)
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
    spot = _spot_close_price(session, symbol, start_utc, end_utc)

    selections: List[ContractDescriptor] = []
    for option_right in rights:
        candidates = dao.fetch_option_candidates(
            trade_date=trade_date,
            underlying_symbol=symbol,
            option_right=option_right,
            dte_min=dte_min,
            dte_max=dte_max,
        )
        filtered = _filter_candidates(
            candidates,
            option_right=option_right,
            spot_price=spot,
            otm_min=otm_min,
            otm_max=otm_max,
            delta_min=delta_min,
            delta_max=delta_max,
        )
        if not filtered:
            filtered = _filter_candidates(
                candidates,
                option_right=option_right,
                spot_price=spot,
                otm_min=otm_min,
                otm_max=otm_max,
                delta_min=delta_min,
                delta_max=delta_max,
                require_activity=False,
            )
            if filtered:
                LOGGER.warning(
                    "Using relaxed option activity filter",
                    extra={"symbol": symbol, "right": option_right},
                )
        if not filtered:
            LOGGER.warning(
                "No contracts passed filters",
                extra={"symbol": symbol, "right": option_right},
            )
            continue
        spot_dec = Decimal(str(spot))
        filtered.sort(
            key=lambda item: (
                -(item.open_interest or 0),
                -(item.volume or 0),
                item.min_tick,
                abs(Decimal(str(item.strike)) - spot_dec),
            ),
        )
        selections.append(filtered[0])
    if not selections:
        raise RuntimeError(f"{symbol} no option contracts meet the criteria")
    return selections


def _aggregate_contracts(
    args: argparse.Namespace,
    session_factory: sessionmaker[Session],
    settings,
    trade_date: date,
) -> List[ContractDescriptor]:
    if args.contracts_file:
        return _load_contracts_from_file(Path(args.contracts_file))

    targets: List[ContractDescriptor] = []
    delta_min = Decimal(str(args.delta_min)) if args.delta_min is not None else None
    delta_max = Decimal(str(args.delta_max)) if args.delta_max is not None else None
    rights = [right.upper() for right in (args.rights or ["CALL", "PUT"])]
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        for symbol in (args.underlyings or []):
            symbol_up = symbol.upper()
            targets.extend(
                _select_contracts_for_symbol(
                    dao,
                    session,
                    symbol_up,
                    trade_date,
                    dte_min=args.dte_min,
                    dte_max=args.dte_max,
                    otm_min=args.otm_min,
                    otm_max=args.otm_max,
                    rights=rights,
                    delta_min=delta_min,
                    delta_max=delta_max,
                )
            )
    seen: Dict[tuple, ContractDescriptor] = {}
    for desc in targets:
        key = (desc.conid, desc.symbol, desc.option_right, desc.expiry, desc.strike)
        seen[key] = desc
    return list(seen.values())


def _store_option_minutes(
    session_factory: sessionmaker[Session],
    descriptor: ContractDescriptor,
    records: Sequence[Mapping[str, float]],
) -> None:
    if not records:
        raise RuntimeError(f"No option L1 records generated for {descriptor}")
    if descriptor.conid is None:
        raise RuntimeError(f"Descriptor missing conid for {descriptor.symbol} {descriptor.option_right}")
    insert_sql = text(
        """
        INSERT INTO bars1m_option (
            conid,
            underlying_symbol,
            expiry,
            "right",
            strike,
            ts_end,
            bid,
            ask,
            mid,
            volume,
            open_interest,
            implied_vol,
            delta,
            gamma,
            theta,
            vega,
            underlying_price
        ) VALUES (
            :conid,
            :underlying_symbol,
            :expiry,
            :right,
            :strike,
            :ts_end,
            :bid,
            :ask,
            :mid,
            :volume,
            :open_interest,
            :implied_vol,
            :delta,
            :gamma,
            :theta,
            :vega,
            :underlying_price
        )
        ON CONFLICT (conid, ts_end) DO UPDATE SET
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            mid = EXCLUDED.mid,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            implied_vol = EXCLUDED.implied_vol,
            delta = EXCLUDED.delta,
            gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta,
            vega = EXCLUDED.vega,
            underlying_price = EXCLUDED.underlying_price,
            updated_at = now()
        """
    )
    with session_factory() as session:
        for row in records:
            session.execute(
                insert_sql,
                {
                    "conid": descriptor.conid,
                    "underlying_symbol": descriptor.symbol,
                    "expiry": datetime.strptime(descriptor.expiry, "%Y%m%d").date(),
                    "right": descriptor.option_right,
                    "strike": descriptor.strike,
                    "ts_end": row["ts_end"],
                    "bid": row["bid"],
                    "ask": row["ask"],
                    "mid": row["mid"],
                    "volume": descriptor.volume,
                    "open_interest": descriptor.open_interest,
                    "implied_vol": None,
                    "delta": None,
                    "gamma": None,
                    "theta": None,
                    "vega": None,
                    "underlying_price": row.get("und_price"),
                },
            )
        session.commit()


def _backfill_chain_quote(
    session_factory: sessionmaker[Session],
    trade_date: date,
    descriptor: ContractDescriptor,
    records: Sequence[Mapping[str, float]],
) -> None:
    if not records or descriptor.conid is None:
        return
    first = records[0]
    update_sql = text(
        """
        UPDATE option_chain_meta
        SET
            bid = :bid,
            ask = :ask,
            mid = :mid,
            volume = COALESCE(volume, 0),
            open_interest = COALESCE(open_interest, 0),
            updated_at = now()
        WHERE trade_date = :trade_date
          AND conid = :conid
        """
    )
    with session_factory() as session:
        session.execute(
            update_sql,
            {
                "trade_date": trade_date,
                "conid": descriptor.conid,
                "bid": first["bid"],
                "ask": first["ask"],
                "mid": first["mid"],
            },
        )
        session.commit()


def _merge_minute_records(
    primary: Sequence[Mapping[str, float]],
    secondary: Sequence[Mapping[str, float]],
) -> List[Mapping[str, float]]:
    merged: Dict[datetime, Mapping[str, float]] = {}
    for record in primary:
        ts = record["ts_end"]
        merged[ts] = record
    for record in secondary:
        ts = record["ts_end"]
        merged.setdefault(ts, record)
    return [merged[ts] for ts in sorted(merged)]


def _ingest_contract(
    client: IBClient,
    session_factory: sessionmaker[Session],
    descriptor: ContractDescriptor,
    trade_date: date,
    *,
    min_minutes: int,
    window_start_et: str | None,
    window_end_et: str | None,
) -> None:
    if descriptor.conid is None:
        LOGGER.info("Resolving contract detail for %s", descriptor)
    contract = client.option_contract(
        symbol=descriptor.symbol,
        expiry=descriptor.expiry,
        strike=descriptor.strike,
        right=descriptor.option_right,
        conid=descriptor.conid,
        include_expired=True,
    )
    start_et, end_et = trading_session_window(trade_date, tz=EASTERN)
    request_start_et = start_et
    request_end_et = end_et + timedelta(minutes=1)
    if window_start_et or window_end_et:
        if not (window_start_et and window_end_et):
            raise RuntimeError("--window-start-et and --window-end-et must be provided together")
        start_hour, start_minute = _parse_hhmm(window_start_et)
        end_hour, end_minute = _parse_hhmm(window_end_et)
        request_start_et = datetime(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            start_hour,
            start_minute,
            tzinfo=EASTERN,
        )
        request_end_et = datetime(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            end_hour,
            end_minute,
            tzinfo=EASTERN,
        )
        if request_end_et <= request_start_et:
            raise RuntimeError("--window-end-et must be later than --window-start-et")
    elif min_minutes > 0:
        request_start_et = max(start_et, end_et - timedelta(minutes=min_minutes))
    start_utc = request_start_et.astimezone(timezone.utc)
    end_utc = request_end_et.astimezone(timezone.utc)
    records = client.req_option_bid_ask_1m(contract, start=start_utc, end=end_utc, use_rth=True)
    required = max(0, min_minutes)
    if required and len(records) < required:
        LOGGER.warning(
            "option_l1.rth_insufficient",
            extra={
                "symbol": descriptor.symbol,
                "right": descriptor.option_right,
                "conid": descriptor.conid,
                "count": len(records),
                "required": required,
            },
        )
        try:
            extended_records = client.req_option_bid_ask_1m(
                contract,
                start=start_utc,
                end=end_utc,
                use_rth=False,
            )
        except Exception as exc:
            LOGGER.warning(
                "option_l1.extended_failed",
                extra={
                    "symbol": descriptor.symbol,
                    "right": descriptor.option_right,
                    "conid": descriptor.conid,
                    "error": str(exc),
                },
            )
            extended_records = []
        if extended_records:
            records = _merge_minute_records(records, extended_records)
    if required and len(records) < required:
        raise RuntimeError(
            f"{descriptor.symbol} {descriptor.option_right} conid={descriptor.conid} insufficient minutes "
            f"({len(records)}/{required})"
        )
    _store_option_minutes(session_factory, descriptor, records)
    _backfill_chain_quote(session_factory, trade_date, descriptor, records)
    LOGGER.info(
        "Stored option L1 minutes symbol=%s right=%s strike=%s expiry=%s count=%s",
        descriptor.symbol,
        descriptor.option_right,
        descriptor.strike,
        descriptor.expiry,
        len(records),
    )


def main() -> int:
    args = _parse_args()
    trade_date = _parse_trade_date(args.date)
    settings = get_settings()
    configure_logging(settings)
    session_factory = get_session_factory(settings)

    contracts = _aggregate_contracts(args, session_factory, settings, trade_date)
    if not contracts:
        raise SystemExit("No option contracts determined for ingestion")

    output_path: Path | None = None
    if args.output_contracts:
        output_path = Path(args.output_contracts)
    elif args.contracts_file:
        output_path = Path(args.contracts_file)
    else:
        output_path = Path(".cache") / f"contracts_{trade_date.isoformat()}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps([asdict(item) for item in contracts], indent=2, default=str), encoding="utf-8")
    LOGGER.info("Contract list written to %s", output_path)

    client = build_ibkr_client(settings)
    client.reqMarketDataType(args.market_data_type)
    try:
        for contract in contracts:
            _ingest_contract(
                client,
                session_factory,
                contract,
                trade_date,
                min_minutes=args.min_minutes,
                window_start_et=args.window_start_et,
                window_end_et=args.window_end_et,
            )
    finally:
        client.disconnect_and_stop()
    LOGGER.info("Option L1 ingestion completed contracts=%s", len(contracts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
