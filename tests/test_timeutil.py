from datetime import date, datetime, time, timezone

import pytest

import libs.core.timeutil as timeutil
from libs.core.timeutil import (
    EASTERN,
    is_half_day,
    to_et,
    to_utc,
    trading_session_window,
    ts_end,
)
from libs.db.dim_trading_calendar import TradingSession


@pytest.fixture(autouse=True)
def _stub_trading_calendar(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_trading_session(session_date: date) -> TradingSession:
        close_time = time(13, 0) if session_date == date(2024, 7, 3) else time(16, 0)
        return TradingSession(
            session_date=session_date,
            open_time=time(9, 30),
            close_time=close_time,
            session_type="HALF_DAY" if close_time == time(13, 0) else "REGULAR",
        )

    monkeypatch.setattr(timeutil, "get_trading_session", fake_get_trading_session)


def test_to_utc_and_back_roundtrip() -> None:
    et_dt = datetime(2024, 7, 1, 9, 30, tzinfo=EASTERN)
    utc_dt = to_utc(et_dt)
    assert utc_dt.tzinfo is timezone.utc
    assert utc_dt.hour == 13
    assert to_et(utc_dt) == et_dt


def test_trading_session_window_regular_day() -> None:
    start, end = trading_session_window(date(2024, 7, 1))
    assert start.tzinfo == EASTERN
    assert start.hour == 9 and start.minute == 30
    assert end.hour == 16 and end.minute == 0
    start_utc, end_utc = trading_session_window(date(2024, 7, 1), tz=timezone.utc)
    assert start_utc.hour == 13 and end_utc.hour == 20


def test_trading_session_window_half_day() -> None:
    session = date(2024, 7, 3)
    assert is_half_day(session)
    start, end = trading_session_window(session)
    assert start.hour == 9 and end.hour == 13


def test_ts_end_clamps_to_session_close() -> None:
    ts = datetime(2024, 7, 3, 13, 5, tzinfo=EASTERN)
    closed = ts_end(ts)
    assert closed.hour == 13 and closed.minute == 0
    assert closed.tzinfo == EASTERN


def test_ts_end_minute_close_alignment() -> None:
    ts = datetime(2024, 7, 1, 10, 15, 12, tzinfo=EASTERN)
    close = ts_end(ts, clamp_to_session=False)
    assert close.minute == 15
    assert close.second == 59 and close.microsecond == 999_999
