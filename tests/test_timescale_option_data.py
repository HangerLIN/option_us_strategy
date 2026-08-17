from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

from apps.backtest.datafeed.timescale_option import TimescaleOptionData
from apps.backtest.dao import OptionBarRow


@dataclass
class _StubDAO:
    rows: list[OptionBarRow]

    def fetch_option_bars_by_conid(self, conid: int, start_ts: datetime, end_ts: datetime):
        return self.rows

    def fetch_option_bars_by_attributes(self, **kwargs):
        return self.rows


def test_timescale_option_data_builds_synthetic_ohlc_from_mid() -> None:
    rows = [
        OptionBarRow(
            ts_end=datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc),
            conid=123,
            symbol="NOW",
            expiry=date(2026, 5, 29),
            strike=Decimal("103"),
            right="CALL",
            trading_class=None,
            multiplier=None,
            exchange=None,
            bid=Decimal("2.55"),
            ask=Decimal("3.60"),
            mid=Decimal("3.075"),
            last=None,
            volume=100,
            open_interest=500,
            implied_vol=None,
            delta=None,
            gamma=None,
            theta=None,
            vega=None,
            underlying_price=None,
        ),
        OptionBarRow(
            ts_end=datetime(2026, 5, 22, 13, 32, tzinfo=timezone.utc),
            conid=123,
            symbol="NOW",
            expiry=date(2026, 5, 29),
            strike=Decimal("103"),
            right="CALL",
            trading_class=None,
            multiplier=None,
            exchange=None,
            bid=Decimal("2.60"),
            ask=Decimal("3.20"),
            mid=Decimal("2.90"),
            last=None,
            volume=120,
            open_interest=550,
            implied_vol=None,
            delta=None,
            gamma=None,
            theta=None,
            vega=None,
            underlying_price=None,
        ),
    ]

    data = TimescaleOptionData.from_timescale(
        dao=_StubDAO(rows),
        contract={
            "conid": 123,
            "symbol": "NOW",
            "expiry": "2026-05-29",
            "strike": 103,
            "right": "CALL",
        },
        start=datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc),
        end=datetime(2026, 5, 22, 13, 32, tzinfo=timezone.utc),
    )

    frame = data.p.dataname
    assert list(frame.columns[:6]) == [
        "conid",
        "symbol",
        "expiry",
        "strike",
        "right",
        "trading_class",
    ]
    assert frame.iloc[0]["open"] == 3.075
    assert frame.iloc[0]["high"] == 3.075
    assert frame.iloc[0]["low"] == 3.075
    assert frame.iloc[0]["close"] == 3.075
    assert frame.iloc[0]["openinterest"] == 500
    assert frame.iloc[0]["quote_age_seconds"] == 0


def test_timescale_option_data_forward_fills_missing_minutes() -> None:
    rows = [
        OptionBarRow(
            ts_end=datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc),
            conid=123,
            symbol="NOW",
            expiry=date(2026, 5, 29),
            strike=Decimal("103"),
            right="CALL",
            trading_class=None,
            multiplier=None,
            exchange=None,
            bid=Decimal("2.55"),
            ask=Decimal("3.60"),
            mid=Decimal("3.075"),
            last=None,
            volume=100,
            open_interest=500,
            implied_vol=None,
            delta=None,
            gamma=None,
            theta=None,
            vega=None,
            underlying_price=None,
        ),
        OptionBarRow(
            ts_end=datetime(2026, 5, 22, 13, 33, tzinfo=timezone.utc),
            conid=123,
            symbol="NOW",
            expiry=date(2026, 5, 29),
            strike=Decimal("103"),
            right="CALL",
            trading_class=None,
            multiplier=None,
            exchange=None,
            bid=Decimal("2.60"),
            ask=Decimal("3.20"),
            mid=Decimal("2.90"),
            last=None,
            volume=120,
            open_interest=550,
            implied_vol=None,
            delta=None,
            gamma=None,
            theta=None,
            vega=None,
            underlying_price=None,
        ),
    ]

    data = TimescaleOptionData.from_timescale(
        dao=_StubDAO(rows),
        contract={
            "conid": 123,
            "symbol": "NOW",
            "expiry": "2026-05-29",
            "strike": 103,
            "right": "CALL",
        },
        start=datetime(2026, 5, 22, 13, 31, tzinfo=timezone.utc),
        end=datetime(2026, 5, 22, 13, 33, tzinfo=timezone.utc),
    )

    frame = data.p.dataname
    assert len(frame) == 3
    assert frame.index[1].minute == 32
    assert frame.iloc[1]["bid"] == 2.55
    assert frame.iloc[1]["ask"] == 3.60
    assert frame.iloc[1]["close"] == 3.075
    assert frame.iloc[1]["quote_age_seconds"] == 60
    assert frame.iloc[2]["quote_age_seconds"] == 0
    assert frame.iloc[1]["source_quote_ts"] == frame.index[0]
