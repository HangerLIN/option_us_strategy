from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from functools import lru_cache
from typing import Iterable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from libs.core.config import get_settings
from libs.infra.db import get_session_factory


@dataclass(frozen=True)
class TradingSession:
    session_date: date
    open_time: time
    close_time: time
    session_type: str  # "REGULAR" | "HALF_DAY"

    @property
    def is_half_day(self) -> bool:
        return self.session_type.upper() == "HALF_DAY"


def _open_session() -> Session:
    settings = get_settings()
    factory = get_session_factory(settings)
    return factory()


@lru_cache(maxsize=4096)
def get_trading_session(session_date: date) -> TradingSession:
    session: Session = _open_session()
    try:
        row = session.execute(
            text(
                """
                SELECT open_et, close_et, is_half_day
                FROM dim_trading_calendar
                WHERE trade_date = :d
                """
            ),
            {"d": session_date},
        ).one_or_none()
        if not row:
            raise KeyError(f"No trading session configured for {session_date}")
        open_et, close_et, is_half = row
        return TradingSession(
            session_date=session_date,
            open_time=open_et,
            close_time=close_et,
            session_type="HALF_DAY" if bool(is_half) else "REGULAR",
        )
    finally:
        session.close()


def iter_trading_sessions() -> Iterable[TradingSession]:
    session: Session = _open_session()
    try:
        rows = session.execute(
            text(
                """
                SELECT trade_date, open_et, close_et, is_half_day
                FROM dim_trading_calendar
                ORDER BY trade_date
                """
            )
        ).all()
        for trade_date, open_et, close_et, is_half in rows:
            yield TradingSession(
                session_date=trade_date,
                open_time=open_et,
                close_time=close_et,
                session_type="HALF_DAY" if bool(is_half) else "REGULAR",
            )
    finally:
        session.close()


def get_prev_trading_date(current_date: date) -> Optional[date]:
    session: Session = _open_session()
    try:
        row = session.execute(
            text(
                """
                SELECT max(trade_date)
                FROM dim_trading_calendar
                WHERE trade_date < :d
                """
            ),
            {"d": current_date},
        ).scalar_one_or_none()
        return row
    finally:
        session.close()
