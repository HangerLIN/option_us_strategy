from __future__ import annotations

from datetime import date, datetime, timezone, tzinfo

from libs.core.constants import EASTERN
from libs.db.dim_trading_calendar import get_trading_session


def utc_now() -> datetime:
    """Return the current UTC time with timezone information."""
    return datetime.now(timezone.utc)


def epoch_ms(ts: datetime | None = None) -> int:
    """Return milliseconds since epoch for the provided UTC datetime."""
    target = ts or utc_now()
    return int(target.timestamp() * 1000)


def to_utc(dt: datetime) -> datetime:
    """Convert any datetime to UTC, assuming Eastern when naive."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EASTERN)
    return dt.astimezone(timezone.utc)


def to_et(dt: datetime) -> datetime:
    """Convert any datetime to US/Eastern, assuming UTC when naive."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(EASTERN)


def _ensure_zone(dt: datetime, default_zone: tzinfo) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=default_zone)
    return dt


def ts_end(ts: datetime, *, clamp_to_session: bool = True) -> datetime:
    """
    Return the minute-close timestamp for a given datetime.

    For example 09:30:12 -> 09:30:59.999999. Timezone information, if present,
    is preserved.
    """
    tz = ts.tzinfo or EASTERN
    ts = _ensure_zone(ts, tz)
    base = ts.replace(second=0, microsecond=0)
    minute_close = base.replace(second=59, microsecond=999_999)
    if not clamp_to_session:
        return minute_close
    session_close = trading_session_window(ts.date(), tz=tz)[1]
    if minute_close > session_close:
        return session_close
    return minute_close


def trading_session_window(
    session_date: date,
    *,
    tz: tzinfo | None = None,
) -> tuple[datetime, datetime]:
    """
    Return the start/end datetimes for the requested trading session.

    Session times are sourced from dim_trading_calendar; by default they are returned
    in US/Eastern unless a different ZoneInfo is supplied.
    """
    session = get_trading_session(session_date)
    base_start = datetime.combine(session.session_date, session.open_time, EASTERN)
    base_end = datetime.combine(session.session_date, session.close_time, EASTERN)
    if tz is None or tz is EASTERN:
        return base_start, base_end
    return base_start.astimezone(tz), base_end.astimezone(tz)


def is_half_day(session_date: date) -> bool:
    """Return True if the session is a designated half-day."""
    session = get_trading_session(session_date)
    return session.is_half_day


def session_close_utc(session_date: date) -> datetime:
    """Convenience helper returning the trading session close in UTC."""
    _, close_et = trading_session_window(session_date, tz=EASTERN)
    return close_et.astimezone(timezone.utc)
