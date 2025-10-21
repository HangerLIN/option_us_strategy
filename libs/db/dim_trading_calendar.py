from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, time
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable


@dataclass(frozen=True)
class TradingSession:
    session_date: date
    open_time: time
    close_time: time
    session_type: str

    @property
    def is_half_day(self) -> bool:
        return self.session_type.upper() == "HALF_DAY"


def _calendar_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "dim_trading_calendar.csv"


@lru_cache(maxsize=1)
def _load_calendar() -> Dict[date, TradingSession]:
    calendar: Dict[date, TradingSession] = {}
    path = _calendar_path()
    if not path.exists():
        raise FileNotFoundError(f"Trading calendar file not found at {path}")

    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            session_date = date.fromisoformat(row["session_date"])
            open_time = time.fromisoformat(row["open_time"])
            close_time = time.fromisoformat(row["close_time"])
            session_type = row["session_type"].strip().upper()
            calendar[session_date] = TradingSession(
                session_date=session_date,
                open_time=open_time,
                close_time=close_time,
                session_type=session_type,
            )
    return calendar


def get_trading_session(session_date: date) -> TradingSession:
    calendar = _load_calendar()
    try:
        return calendar[session_date]
    except KeyError as exc:
        raise KeyError(f"No trading session configured for {session_date}") from exc


def iter_trading_sessions() -> Iterable[TradingSession]:
    return _load_calendar().values()
