from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time as time_cls, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
from sqlalchemy import text
from sqlalchemy.orm import Session

from libs.core.config import get_settings
from libs.db.dim_trading_calendar import get_trading_session
from libs.infra.db import get_session_factory

LOGGER = logging.getLogger(__name__)
EASTERN = ZoneInfo("America/New_York")
_CALENDAR_CODE = "XNYS"
_LOCK_KEY = 0x0D1DC4AE

def _open_session() -> Session:
    settings = get_settings()
    factory = get_session_factory(settings)
    return factory()


def _try_lock(session: Session) -> bool:
    row = session.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": _LOCK_KEY}).scalar()
    return bool(row)


def _unlock(session: Session) -> None:
    session.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _LOCK_KEY})


def _to_et_time(ts: datetime) -> time_cls:
    return ts.astimezone(EASTERN).time()


def ensure_future_calendar(days_ahead: int = 90) -> int:
    """
    Refresh the trading calendar table to cover today through days_ahead into the future.

    Returns the number of sessions inserted or updated.
    """
    session: Session = _open_session()
    locked = False
    try:
        if not _try_lock(session):
            session.rollback()
            LOGGER.debug("calendar_refresher.skip_locked")
            return 0
        locked = True

        today = datetime.now(EASTERN).date()
        start = today
        end = today + timedelta(days=days_ahead)

        calendar = xcals.get_calendar(_CALENDAR_CODE)
        
        # 获取交易日列表
        sessions = calendar.sessions_in_range(start, end)
        
        # 获取半日市日期集合
        early_closes = calendar.early_closes
        early_dates = {ts.date() for ts in early_closes if start <= ts.date() <= end}

        upsert = text(
            """
            INSERT INTO dim_trading_calendar (trade_date, is_half_day, open_et, close_et)
            VALUES (:d, :half, :open_et, :close_et)
            ON CONFLICT (trade_date)
            DO UPDATE
            SET is_half_day = EXCLUDED.is_half_day,
                open_et = EXCLUDED.open_et,
                close_et = EXCLUDED.close_et
            """
        )

        total = 0
        for session_ts in sessions:
            session_date = session_ts.date()
            
            # 获取该交易日的开盘/收盘时间（UTC）
            open_dt = calendar.session_open(session_ts).to_pydatetime()
            close_dt = calendar.session_close(session_ts).to_pydatetime()
            
            # 转换为东部时间
            open_et = _to_et_time(open_dt)
            close_et = _to_et_time(close_dt)
            is_half = session_date in early_dates
            
            session.execute(
                upsert,
                {
                    "d": session_date,
                    "half": bool(is_half),
                    "open_et": open_et,
                    "close_et": close_et,
                },
            )
            total += 1

        session.commit()
        get_trading_session.cache_clear()
        LOGGER.info(
            "calendar_refresher.refreshed: start=%s end=%s sessions=%d",
            start, end, total
        )
        return total
    except Exception:
        session.rollback()
        LOGGER.exception("calendar_refresher.failed")
        raise
    finally:
        if locked:
            try:
                _unlock(session)
                session.commit()
            except Exception:
                session.rollback()
            finally:
                session.close()
        else:
            session.close()


async def schedule_daily_refresh(days_ahead: int = 90, at_minute: int = 5) -> None:
    """
    Background coroutine that refreshes the trading calendar once per day at 00:MM ET.
    """
    try:
        while True:
            now = datetime.now(EASTERN)
            target = datetime.combine(now.date(), time_cls(0, at_minute), tzinfo=EASTERN)
            if now >= target:
                target += timedelta(days=1)
            sleep_seconds = max(1.0, (target - now).total_seconds())
            await asyncio.sleep(sleep_seconds)
            try:
                refreshed = await asyncio.to_thread(ensure_future_calendar, days_ahead)
                LOGGER.debug(
                    "calendar_refresher.daily_refresh_completed: sessions=%d next=%s",
                    refreshed, target.isoformat()
                )
            except Exception:
                LOGGER.exception("calendar_refresher.daily_refresh_failed")
    except asyncio.CancelledError:
        LOGGER.info("calendar_refresher.scheduler_stopped")
        raise
