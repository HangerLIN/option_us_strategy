from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol, Set

from sqlalchemy import text
from sqlalchemy.orm import Session


class Top5Source(Protocol):
    """Abstraction for retrieving Top5 symbol membership."""

    def symbols_for_date(self, session: Session, trade_date: date) -> Set[str]:
        ...

    def symbols_between(
        self, session: Session, start_date: date, end_date: date
    ) -> Set[str]:
        ...


@dataclass(frozen=True)
class PremarketTop5Source:
    table_name: str = "premarket_top5"

    def symbols_for_date(self, session: Session, trade_date: date) -> Set[str]:
        stmt = text(
            f"""
            SELECT symbol
            FROM {self.table_name}
            WHERE trade_date = :trade_date
            """
        )
        rows = session.execute(stmt, {"trade_date": trade_date}).scalars().all()
        return {str(symbol).upper() for symbol in rows if symbol}

    def symbols_between(
        self, session: Session, start_date: date, end_date: date
    ) -> Set[str]:
        stmt = text(
            f"""
            SELECT DISTINCT symbol
            FROM {self.table_name}
            WHERE trade_date BETWEEN :start_date AND :end_date
            """
        )
        rows = session.execute(
            stmt, {"start_date": start_date, "end_date": end_date}
        ).scalars().all()
        return {str(symbol).upper() for symbol in rows if symbol}


@dataclass(frozen=True)
class BacktestTop5Source:
    batch_id: str
    table_name: str = "bt_top5"

    def symbols_for_date(self, session: Session, trade_date: date) -> Set[str]:
        stmt = text(
            f"""
            SELECT symbol
            FROM {self.table_name}
            WHERE batch_id = :batch_id
              AND trade_date = :trade_date
            """
        )
        rows = session.execute(
            stmt, {"batch_id": self.batch_id, "trade_date": trade_date}
        ).scalars().all()
        return {str(symbol).upper() for symbol in rows if symbol}

    def symbols_between(
        self, session: Session, start_date: date, end_date: date
    ) -> Set[str]:
        stmt = text(
            f"""
            SELECT DISTINCT symbol
            FROM {self.table_name}
            WHERE batch_id = :batch_id
              AND trade_date BETWEEN :start_date AND :end_date
            """
        )
        rows = session.execute(
            stmt,
            {
                "batch_id": self.batch_id,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).scalars().all()
        return {str(symbol).upper() for symbol in rows if symbol}


__all__ = ["Top5Source", "PremarketTop5Source", "BacktestTop5Source"]
