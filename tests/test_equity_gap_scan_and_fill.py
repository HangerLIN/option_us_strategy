from __future__ import annotations

from datetime import date, time
from decimal import Decimal
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from apps.backtest.dao import EquityBarRow
from apps.backtest.datafeed.timescale_equity import (
    _INDICATOR_COLS,
    _fill_equity_gaps_via_ibkr,
    _scan_equity_gaps,
)
from libs.db.dim_trading_calendar import TradingSession


@pytest.fixture(autouse=True)
def _stub_trading_calendar(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_trading_session(session_date: date) -> TradingSession:
        return TradingSession(
            session_date=session_date,
            open_time=time(9, 30),
            close_time=time(16, 0),
            session_type="REGULAR",
        )

    monkeypatch.setattr(
        "apps.backtest.datafeed.timescale_equity.get_trading_session",
        fake_get_trading_session,
    )


class FakeDAO:
    def __init__(self, price_df: pd.DataFrame | None = None) -> None:
        self.price_df = price_df
        self.bars_calls: List[pd.DataFrame] = []
        self.indicator_calls: List[pd.DataFrame] = []
        self.events: List[Dict[str, Any]] = []
        self.baseline_requests: List[str] = []

    def upsert_equity_bars_from_df(self, *, symbol: str, df: pd.DataFrame) -> None:
        self.bars_calls.append(df.copy())

    def upsert_equity_indicators_from_df(self, *, symbol: str, df: pd.DataFrame) -> None:
        self.indicator_calls.append(df.copy())

    def insert_risk_event(
        self,
        *,
        symbol: str,
        event_code: str,
        severity: str,
        action: str,
        payload_json: str,
    ) -> None:
        self.events.append(
            {
                "symbol": symbol,
                "event_code": event_code,
                "severity": severity,
                "action": action,
                "payload_json": payload_json,
            }
        )

    def fetch_rvol_baseline(self, *, symbol: str) -> Dict[int, Decimal]:
        self.baseline_requests.append(symbol)
        return {}

    def fetch_equity_bars(self, *, symbol: str, start_ts, end_ts) -> List[EquityBarRow]:
        if self.price_df is None:
            return []
        rows: List[EquityBarRow] = []
        for ts_et, row in self.price_df.sort_index().iterrows():
            ts_utc = ts_et.tz_convert("UTC").to_pydatetime()
            if start_ts and ts_utc < start_ts:
                continue
            if end_ts and ts_utc >= end_ts:
                continue
            rows.append(
                EquityBarRow(
                    ts_end=ts_utc,
                    symbol=symbol,
                    open=Decimal(str(row["open"])),
                    high=Decimal(str(row["high"])),
                    low=Decimal(str(row["low"])),
                    close=Decimal(str(row["close"])),
                    volume=int(row["volume"]),
                    rsi6=None,
                    rsi12=None,
                    rsi24=None,
                    atr14=None,
                    ao=None,
                    stoch_k=None,
                    stoch_d=None,
                    cci14=None,
                    cci6=None,
                    obv=None,
                    obv_ema20=None,
                    mfi14=None,
                    rvol6=None,
                )
            )
        return rows


TEST_TRADE_DATE = "2025-10-02"


def _build_base_dataframe() -> pd.DataFrame:
    trade_dates = [
        pd.Timestamp(TEST_TRADE_DATE, tz="US/Eastern") - pd.Timedelta(days=1),
        pd.Timestamp(TEST_TRADE_DATE, tz="US/Eastern"),
    ]
    frames: List[pd.DataFrame] = []
    offset = 0
    for day in trade_dates:
        start = day.replace(hour=9, minute=25, second=0, microsecond=0)
        end = day.replace(hour=16, minute=0, second=0, microsecond=0)
        index = pd.date_range(start, end, freq="1min")
        data = {
            "open": [100 + (offset + i) * 0.1 for i in range(len(index))],
            "high": [100.2 + (offset + i) * 0.1 for i in range(len(index))],
            "low": [99.8 + (offset + i) * 0.1 for i in range(len(index))],
            "close": [100.1 + (offset + i) * 0.1 for i in range(len(index))],
            "volume": [1_000 + (offset + i) * 5 for i in range(len(index))],
        }
        frames.append(pd.DataFrame(data, index=index))
        offset += len(index)
    df = pd.concat(frames).sort_index()
    for col in _INDICATOR_COLS:
        df[col] = 1.0
    return df


def _update_dataframe_with_calls(
    base: pd.DataFrame, bars_calls: List[pd.DataFrame], indicator_calls: List[pd.DataFrame]
) -> pd.DataFrame:
    result = base.copy()
    for df_call in bars_calls:
        for ts, row in df_call.iterrows():
            for col in ["open", "high", "low", "close", "volume"]:
                result.loc[ts, col] = row[col]
    for df_call in indicator_calls:
        for ts, row in df_call.iterrows():
            for col in _INDICATOR_COLS:
                if col in row:
                    result.loc[ts, col] = row[col]
    result = result.sort_index()
    return result


def test_price_gap_scan_and_fill() -> None:
    df = _build_base_dataframe()
    df = df.drop(
        index=[
            pd.Timestamp(f"{TEST_TRADE_DATE} 09:40", tz="US/Eastern"),
            pd.Timestamp(f"{TEST_TRADE_DATE} 09:41", tz="US/Eastern"),
        ]
    )
    symbol = "AAPL"
    report = _scan_equity_gaps(df=df, symbol=symbol)
    report.prev_close_missing.clear()
    assert report.total_missing_minutes() == 2

    fake_dao = FakeDAO()
    mock_ib = Mock()
    bars = []
    for minute in ("2025-01-06 09:40", "2025-01-06 09:41"):
        ts_utc = pd.Timestamp(
            minute.replace("2025-01-06", TEST_TRADE_DATE), tz="US/Eastern"
        ).tz_convert("UTC")
        bars.append(
            {
                "ts_end": ts_utc.isoformat(),
                "open": 110.0,
                "high": 110.2,
                "low": 109.8,
                "close": 110.1,
                "volume": 1234,
            }
        )
    mock_ib.req_historical_1m.return_value = bars

    with patch(
        "apps.backtest.datafeed.timescale_equity.indcalc.compute_indicators",
        return_value=pd.DataFrame(
            {col: [2.0, 2.1] for col in _INDICATOR_COLS},
            index=[
                pd.Timestamp(f"{TEST_TRADE_DATE} 09:40", tz="US/Eastern"),
                pd.Timestamp(f"{TEST_TRADE_DATE} 09:41", tz="US/Eastern"),
            ],
        ),
    ):
        _fill_equity_gaps_via_ibkr(dao=fake_dao, ib=mock_ib, symbol=symbol, gap_report=report)

    assert mock_ib.req_historical_1m.called
    assert fake_dao.bars_calls, "Expected bars upsert to be invoked"
    assert fake_dao.indicator_calls, "Expected indicator upsert to be invoked"

    filled = _update_dataframe_with_calls(df, fake_dao.bars_calls, fake_dao.indicator_calls)
    post_report = _scan_equity_gaps(df=filled, symbol=symbol)
    assert not post_report.price_gaps
    assert not post_report.indicator_gaps


def test_indicator_gap_only_triggers_indicator_recompute() -> None:
    df = _build_base_dataframe()
    problematic_minutes = [
        pd.Timestamp(f"{TEST_TRADE_DATE} 09:35", tz="US/Eastern"),
        pd.Timestamp(f"{TEST_TRADE_DATE} 09:36", tz="US/Eastern"),
    ]
    for ts in problematic_minutes:
        for col in _INDICATOR_COLS:
            df.loc[ts, col] = pd.NA

    symbol = "MSFT"
    report = _scan_equity_gaps(df=df, symbol=symbol)
    report.prev_close_missing.clear()
    assert report.price_gaps == []
    assert report.indicator_gaps

    fake_dao = FakeDAO(price_df=df[["open", "high", "low", "close", "volume"]])
    mock_ib = Mock()
    mock_ib.req_historical_1m.side_effect = AssertionError("req_historical_1m should not be called")

    indicator_patch = pd.DataFrame(
        {col: [3.0, 3.1] for col in _INDICATOR_COLS},
        index=problematic_minutes,
    )

    with patch(
        "apps.backtest.datafeed.timescale_equity.indcalc.compute_indicators",
        return_value=indicator_patch,
    ):
        _fill_equity_gaps_via_ibkr(dao=fake_dao, ib=mock_ib, symbol=symbol, gap_report=report)

    mock_ib.req_historical_1m.assert_not_called()
    assert fake_dao.indicator_calls, "Indicators should be recomputed for NaN windows"

    filled = _update_dataframe_with_calls(df, fake_dao.bars_calls, fake_dao.indicator_calls)
    post_report = _scan_equity_gaps(df=filled, symbol=symbol)
    assert not post_report.indicator_gaps
