from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

import apps.backtest.bt_runner as bt_runner
from apps.backtest.bt_runner import _prepare_option_feeds, _select_best_contract
from apps.backtest.dao import OptionCandidateRow
from libs.core.config import get_settings
from libs.schemas.signals import SignalSide


def _candidate(
    *,
    conid: int = 881644421,
    bid: str = "8.05",
    ask: str = "9.15",
    delta: str | None = None,
) -> OptionCandidateRow:
    return OptionCandidateRow(
        conid=conid,
        underlying_symbol="AMD",
        expiry=date(2026, 5, 29),
        strike=Decimal("505"),
        option_right="CALL",
        dte=3,
        delta=Decimal(delta) if delta is not None else None,
        gamma=None,
        theta=None,
        vega=None,
        bid=Decimal(bid),
        ask=Decimal(ask),
        mid=(Decimal(bid) + Decimal(ask)) / Decimal("2"),
        open_interest=0,
        volume=2,
        min_tick=Decimal("0.01"),
        trading_class=None,
        multiplier=None,
        exchange=None,
        implied_vol=None,
        underlying_price=None,
    )


def test_backtest_option_selection_allows_wide_spread_when_liquidity_disabled(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OPTION_LIQUIDITY_REQUIRED", "false")
    get_settings.cache_clear()

    selected = _select_best_contract([_candidate()], "CALL", "AMD")

    assert selected is not None
    assert selected.conid == 881644421


def test_backtest_option_selection_filters_wide_spread_when_liquidity_required(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OPTION_LIQUIDITY_REQUIRED", "true")
    get_settings.cache_clear()

    selected = _select_best_contract([_candidate()], "CALL", "AMD")

    assert selected is None


def test_prepare_option_feeds_selects_contracts_by_signal_trade_date(monkeypatch) -> None:
    calls: list[tuple[date, int, int]] = []

    class DummyDAO:
        def fetch_option_candidates(self, *, trade_date, underlying_symbol, option_right, dte_min, dte_max):
            calls.append((trade_date, dte_min, dte_max))
            return [_candidate()]

        def count_option_minutes(self, *, conid, start_ts, end_ts):
            return 91

    class DummyCerebro:
        def __init__(self):
            self.feeds = []

        def adddata(self, feed):
            self.feeds.append(feed)

    def fake_from_timescale(**kwargs):
        return SimpleNamespace(p=SimpleNamespace(contract=kwargs["contract"]))

    monkeypatch.setattr(bt_runner.TimescaleOptionData, "from_timescale", fake_from_timescale)
    signal = SimpleNamespace(
        symbol="AMD",
        side=SignalSide.BUY,
        ts_end=datetime(2026, 5, 20, 14, 30, tzinfo=timezone.utc),
        option_hint={"dte": [0, 7]},
    )
    cerebro = DummyCerebro()

    contracts, feeds = _prepare_option_feeds(
        cerebro=cerebro,
        dao=DummyDAO(),
        symbol="AMD",
        start=datetime(2026, 5, 18, 13, 30, tzinfo=timezone.utc),
        end=datetime(2026, 5, 26, 16, 0, tzinfo=timezone.utc),
        provided_contracts=None,
        signals=[signal],
    )

    assert calls == [(date(2026, 5, 20), 0, 7)]
    assert contracts
    assert feeds
    assert len(cerebro.feeds) == 1


def test_prepare_option_feeds_filters_available_delta_by_signal_hint(monkeypatch) -> None:
    seen_conids: list[int] = []

    class DummyDAO:
        def fetch_option_candidates(self, *, trade_date, underlying_symbol, option_right, dte_min, dte_max):
            return [
                _candidate(conid=1, delta="0.20"),
                _candidate(conid=2, delta="0.45"),
                _candidate(conid=3, delta=None),
            ]

        def count_option_minutes(self, *, conid, start_ts, end_ts):
            seen_conids.append(conid)
            return 91

    class DummyCerebro:
        def __init__(self):
            self.feeds = []

        def adddata(self, feed):
            self.feeds.append(feed)

    def fake_from_timescale(**kwargs):
        return SimpleNamespace(p=SimpleNamespace(contract=kwargs["contract"]))

    monkeypatch.setattr(bt_runner.TimescaleOptionData, "from_timescale", fake_from_timescale)
    signal = SimpleNamespace(
        symbol="AMD",
        side=SignalSide.BUY,
        ts_end=datetime(2026, 5, 20, 14, 30, tzinfo=timezone.utc),
        option_hint={"dte": [0, 7], "delta": [0.35, 0.55]},
    )
    cerebro = DummyCerebro()

    contracts, feeds = _prepare_option_feeds(
        cerebro=cerebro,
        dao=DummyDAO(),
        symbol="AMD",
        start=datetime(2026, 5, 18, 13, 30, tzinfo=timezone.utc),
        end=datetime(2026, 5, 26, 16, 0, tzinfo=timezone.utc),
        provided_contracts=None,
        signals=[signal],
    )

    assert seen_conids == [2, 3]
    assert {contract.conid for contract in contracts.values()} == {2, 3}
    assert feeds


def test_prepare_option_feeds_skips_missing_l1_and_fails_without_real_quotes(monkeypatch) -> None:
    class DummyDAO:
        def fetch_option_candidates(self, *, trade_date, underlying_symbol, option_right, dte_min, dte_max):
            return [_candidate()]

        def count_option_minutes(self, *, conid, start_ts, end_ts):
            return 0

    class DummyCerebro:
        def adddata(self, feed):
            raise AssertionError("no feed should be added")

    signal = SimpleNamespace(
        symbol="AMD",
        side=SignalSide.BUY,
        ts_end=datetime(2026, 5, 20, 14, 30, tzinfo=timezone.utc),
        option_hint={"dte": [0, 7]},
    )

    with pytest.raises(RuntimeError, match="No option contracts available"):
        _prepare_option_feeds(
            cerebro=DummyCerebro(),
            dao=DummyDAO(),
            symbol="AMD",
            start=datetime(2026, 5, 18, 13, 30, tzinfo=timezone.utc),
            end=datetime(2026, 5, 26, 16, 0, tzinfo=timezone.utc),
            provided_contracts=None,
            signals=[signal],
        )
