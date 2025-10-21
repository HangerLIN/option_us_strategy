# path: scripts/probe_ibkr_features.py
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional

from ibapi.contract import Contract

from libs.core import EASTERN, configure_logging, get_settings, trading_session_window
from libs.infra import build_ibkr_client
from libs.infra.ibkr_client import IBClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quick probe for IBKR client helpers")
    parser.add_argument("--symbol", default="SPY", help="Underlying symbol for equity requests")
    parser.add_argument(
        "--date",
        required=True,
        help="Session date (YYYY-MM-DD) for 1m equity bars, e.g. 2024-01-03",
    )
    parser.add_argument(
        "--option-expiry",
        help="Option expiry in YYYYMMDD. Provide along with --option-right/--option-strike to fetch BID/ASK ticks.",
    )
    parser.add_argument(
        "--option-strike",
        type=float,
        help="Option strike price. Must be used with --option-expiry and --option-right.",
    )
    parser.add_argument(
        "--option-right",
        choices=["CALL", "PUT"],
        help="Option right. Must be used with --option-expiry and --option-strike.",
    )
    parser.add_argument(
        "--option-conid",
        type=int,
        help="Option contract ID (optional). When omitted the script lets IBKR derive conid from contract details.",
    )
    return parser.parse_args()


def parse_session_date(value: str) -> datetime.date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid --date value {value!r}, expected YYYY-MM-DD") from exc


def summary(title: str, payload: object) -> None:
    print(f"[probe] {title}: {payload}")


def build_option_contract(client: IBClient, args: argparse.Namespace) -> Optional[Contract]:
    if not (args.option_expiry and args.option_right and args.option_strike):
        return None
    contract = client.option_contract(
        symbol=args.symbol.upper(),
        expiry=args.option_expiry,
        strike=args.option_strike,
        right=args.option_right,
        conid=args.option_conid,
    )
    return contract


def main() -> int:
    args = parse_args()
    trade_date = parse_session_date(args.date)
    settings = get_settings()
    configure_logging(settings)

    client = build_ibkr_client(settings)
    try:
        summary("connected", True)

        # Request equity 1m bars twice in the same session to observe pacing behaviour
        for attempt in range(1, 3):
            bars = client.req_historical_1m_equity(args.symbol.upper(), trade_date)
            summary(
                f"equity_attempt_{attempt}",
                {
                    "bars": len(bars),
                    "first_ts": bars[0]["time"] if bars else None,
                    "last_ts": bars[-1]["time"] if bars else None,
                },
            )

        # Option L1 aggregation (optional)
        contract = build_option_contract(client, args)
        if contract is not None:
            start_et, end_et = trading_session_window(trade_date, tz=EASTERN)
            start_utc = start_et.astimezone(timezone.utc)
            end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
            bid_ask_minutes = client.req_option_bid_ask_1m(
                contract,
                start=start_utc,
                end=end_utc,
            )
            summary("option_bid_ask_minutes", len(bid_ask_minutes))
    finally:
        client.disconnect_and_stop()
        summary("disconnected", True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
