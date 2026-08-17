from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from libs.core.config import Settings
from libs.infra.ibkr_client import IBClient
from apps.backtest.bt_runner import (
    _SimulatedSignalTrade,
    _price_event_time_option_trade,
    _select_event_time_contract,
    discover_recoverable_trade_dates,
)
from apps.backtest.dao import OptionBarRow, OptionCandidateRow


def _candidate(
    *,
    conid: int = 101,
    expiry: date = date(2026, 6, 12),
    delta: str = "0.45",
) -> OptionCandidateRow:
    bid = Decimal("2.00")
    ask = Decimal("2.20")
    return OptionCandidateRow(
        conid=conid,
        underlying_symbol="AAPL",
        expiry=expiry,
        strike=Decimal("205"),
        option_right="CALL",
        dte=2,
        delta=Decimal(delta),
        gamma=None,
        theta=None,
        vega=None,
        bid=bid,
        ask=ask,
        mid=(bid + ask) / Decimal("2"),
        open_interest=200,
        volume=50,
        min_tick=Decimal("0.01"),
        trading_class=None,
        multiplier=None,
        exchange=None,
        implied_vol=None,
        underlying_price=Decimal("204"),
    )


def _quote(
    *,
    conid: int = 101,
    ts_end: datetime,
    bid: str = "2.00",
    ask: str = "2.20",
    delta: str = "0.45",
) -> OptionBarRow:
    bid_dec = Decimal(bid)
    ask_dec = Decimal(ask)
    return OptionBarRow(
        ts_end=ts_end,
        conid=conid,
        symbol="AAPL",
        expiry=date(2026, 6, 12),
        strike=Decimal("205"),
        right="CALL",
        trading_class=None,
        multiplier=None,
        exchange=None,
        bid=bid_dec,
        ask=ask_dec,
        mid=(bid_dec + ask_dec) / Decimal("2"),
        last=None,
        volume=100,
        open_interest=500,
        implied_vol=None,
        delta=Decimal(delta),
        gamma=None,
        theta=None,
        vega=None,
        underlying_price=Decimal("204"),
    )


def test_discover_recoverable_trade_dates_marks_expired_candidates_unrecoverable() -> None:
    class DummyDAO:
        def fetch_recent_trade_dates(self, *, limit, as_of):
            assert limit == 5
            assert as_of == date(2026, 6, 11)
            return [date(2026, 6, 5), date(2026, 6, 4)]

        def fetch_option_candidates(self, *, trade_date, underlying_symbol, option_right, dte_min, dte_max):
            assert underlying_symbol == "AAPL"
            assert option_right == "CALL"
            assert (dte_min, dte_max) == (0, 7)
            if trade_date == date(2026, 6, 5):
                return [_candidate(expiry=date(2026, 6, 12))]
            return [_candidate(expiry=date(2026, 6, 10))]

    statuses = discover_recoverable_trade_dates(
        DummyDAO(),
        symbols=["AAPL"],
        as_of_date=date(2026, 6, 11),
        lookback_trade_days=5,
    )

    assert statuses[("AAPL", date(2026, 6, 5))] == "recoverable"
    assert statuses[("AAPL", date(2026, 6, 4))] == "option-unrecoverable"


def test_select_event_time_contract_prefers_fresh_valid_quote() -> None:
    ts = datetime(2026, 6, 11, 14, 35, tzinfo=timezone.utc)

    class DummyDAO:
        def fetch_latest_option_quotes_for_symbol(self, *, underlying_symbol, option_right, start_ts, end_ts):
            assert underlying_symbol == "AAPL"
            assert option_right == "CALL"
            assert end_ts == ts
            return [
                _quote(conid=101, ts_end=ts - timedelta(seconds=25), delta="0.45"),
                _quote(conid=202, ts_end=ts - timedelta(seconds=75), delta="0.42"),
            ]

    selected = _select_event_time_contract(
        dao=DummyDAO(),
        symbol="AAPL",
        ts=ts,
        candidates=[_candidate(conid=101), _candidate(conid=202)],
        delta_low=Decimal("0.35"),
        delta_high=Decimal("0.55"),
    )

    assert selected is not None
    assert selected.conid == 101
    assert selected.quote_ts == ts - timedelta(seconds=25)


def test_select_event_time_contract_allows_missing_delta_with_valid_quote_fallback() -> None:
    ts = datetime(2026, 6, 11, 14, 35, tzinfo=timezone.utc)

    class DummyDAO:
        def fetch_latest_option_quotes_for_symbol(self, *, underlying_symbol, option_right, start_ts, end_ts):
            assert underlying_symbol == "AAPL"
            assert option_right == "CALL"
            return [
                _quote(conid=101, ts_end=ts - timedelta(seconds=10), bid="2.00", ask="2.08", delta="0.45"),
                OptionBarRow(
                    ts_end=ts - timedelta(seconds=5),
                    conid=202,
                    symbol="AAPL",
                    expiry=date(2026, 6, 12),
                    strike=Decimal("204"),
                    right="CALL",
                    trading_class=None,
                    multiplier=None,
                    exchange=None,
                    bid=Decimal("2.10"),
                    ask=Decimal("2.15"),
                    mid=Decimal("2.125"),
                    last=None,
                    volume=200,
                    open_interest=800,
                    implied_vol=None,
                    delta=None,
                    gamma=None,
                    theta=None,
                    vega=None,
                    underlying_price=Decimal("204"),
                ),
            ]

    selected = _select_event_time_contract(
        dao=DummyDAO(),
        symbol="AAPL",
        ts=ts,
        candidates=[_candidate(conid=101), _candidate(conid=202, delta="0.45")],
        delta_low=Decimal("0.35"),
        delta_high=Decimal("0.55"),
    )

    assert selected is not None
    assert selected.conid == 202
    assert selected.delta == Decimal("0.45")


def test_price_event_time_trade_marks_missing_exit_quote_as_data_skipped() -> None:
    entry_ts = datetime(2026, 6, 11, 14, 35, tzinfo=timezone.utc)
    exit_ts = datetime(2026, 6, 11, 14, 40, tzinfo=timezone.utc)
    trade = _SimulatedSignalTrade(
        symbol="AAPL",
        signal_code="SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
        entry_ts=entry_ts,
        exit_ts=exit_ts,
        entry_price=Decimal("204"),
        exit_price=Decimal("205"),
        option_hint={"dte": [0, 7], "delta": [0.35, 0.55]},
        exit_signal_code="SIG_1030_REVERSAL_BOLL_UP_TRUE_EXIT",
    )

    class DummyDAO:
        def fetch_latest_option_quotes_for_symbol(self, *, underlying_symbol, option_right, start_ts, end_ts):
            return [_quote(conid=101, ts_end=entry_ts - timedelta(seconds=5))]

        def fetch_latest_option_quote_by_conid(self, *, conid, start_ts, end_ts):
            return None

    outcome = _price_event_time_option_trade(
        dao=DummyDAO(),
        trade=trade,
        candidates=[_candidate(conid=101)],
        as_of_date=date(2026, 6, 11),
    )

    assert outcome.status == "data-skipped"
    assert outcome.contract is not None
    assert outcome.skip_reason == "missing_exit_quote"


def test_ibkr_tick_option_computation_ignores_none_greeks() -> None:
    settings = Settings(
        DATABASE_URL="postgresql://user:pass@localhost/db",
        REDIS_URL="redis://localhost:6379/0",
        IB_HOST="127.0.0.1",
        IB_PORT=7497,
        IB_CLIENT_ID=99,
        IB_ACCOUNT="DU1234567",
    )
    client = IBClient(settings)
    client._req_symbol[123] = "SMOKE:OPT"

    client.tickOptionComputation(
        123,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )

    queue = client._symbol_queues.get("SMOKE:OPT")
    assert queue is None
