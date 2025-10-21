#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from libs.db import Base, RefMarketCap


def load_snapshot(path: str, source: str, session: Session) -> None:
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    now = datetime.now(timezone.utc)
    for row in rows:
        symbol = row["symbol"].strip().upper()
        market_cap = Decimal(row["market_cap_usd"])
        updated_at = row.get("updated_at")
        updated = datetime.fromisoformat(updated_at) if updated_at else now

        existing = session.get(RefMarketCap, symbol)
        if existing:
            existing.market_cap_usd = market_cap
            existing.source = source
            existing.updated_at = updated
        else:
            session.add(
                RefMarketCap(
                    symbol=symbol,
                    market_cap_usd=market_cap,
                    source=source,
                    updated_at=updated,
                )
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Load market cap snapshot into ref_market_cap")
    parser.add_argument("csv", help="CSV file with columns symbol,market_cap_usd,[updated_at]")
    parser.add_argument("--source", default="compliance_snapshot", help="Snapshot source tag")
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL environment variable is required")

    engine = create_engine(database_url, future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        load_snapshot(args.csv, args.source, session)
        session.commit()


if __name__ == "__main__":
    main()
