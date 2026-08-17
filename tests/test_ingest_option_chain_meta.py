from __future__ import annotations

from datetime import date, datetime, time
from types import SimpleNamespace

import scripts.ingest_option_chain_meta_ibkr as option_chain_meta
import scripts.ingest_option_l1_ibkr as option_l1
from apps.backtest.dao import OptionCandidateRow
from libs.core import EASTERN


class _FakeOptionClient:
    def option_contract(
        self,
        *,
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
        exchange: str,
        multiplier: str,
        include_expired: bool,
    ):
        return SimpleNamespace(
            symbol=symbol,
            lastTradeDateOrContractMonth=expiry,
            strike=strike,
            right=right,
            exchange=exchange,
            multiplier=multiplier,
            include_expired=include_expired,
            tradingClass="",
            conId=0,
        )

    def req_contract_details(self, contract, timeout: float = 15.0):
        conid = int(contract.strike * 1000) + (1 if contract.right == "CALL" else 2)
        return SimpleNamespace(
            contract=SimpleNamespace(
                symbol=contract.symbol,
                lastTradeDateOrContractMonth=contract.lastTradeDateOrContractMonth,
                strike=contract.strike,
                right=contract.right,
                exchange=contract.exchange,
                multiplier=contract.multiplier,
                tradingClass=contract.tradingClass,
                conId=conid,
            ),
            minTick=0.05,
        )

    def req_option_bid_ask_1m(self, contract, *, start, end, use_rth: bool = True):
        return [
            {"bid": 1.2, "ask": 1.4, "bid_size": 3, "ask_size": 4},
        ]


def test_build_option_quotes_populates_historical_bid_ask(monkeypatch) -> None:
    def fake_session_window(session_date: date, *, tz=None):
        zone = tz or EASTERN
        start = datetime.combine(session_date, time(9, 30), tzinfo=EASTERN)
        end = datetime.combine(session_date, time(16, 0), tzinfo=EASTERN)
        return start.astimezone(zone), end.astimezone(zone)

    monkeypatch.setattr(option_chain_meta, "trading_session_window", fake_session_window)

    quotes = option_chain_meta._build_option_quotes(
        _FakeOptionClient(),
        symbol="NVDA",
        trade_date=date(2026, 5, 26),
        spot_price=214.84,
        expirations=["20260529"],
        strikes=[210.0, 212.5, 215.0, 217.5],
        exchange="SMART",
        trading_class="NVDA",
        multiplier="100",
        dte_min=2,
        dte_max=7,
        otm_min=0,
        otm_max=5,
        max_per_side=1,
    )

    assert len(quotes) == 2
    assert {quote.option_right for quote in quotes} == {"CALL", "PUT"}
    assert all(quote.bid is not None for quote in quotes)
    assert all(quote.ask is not None for quote in quotes)
    assert all(quote.mid is not None for quote in quotes)
    assert all(quote.volume == 7 for quote in quotes)


def test_build_option_quotes_respects_rights_and_available_delta(monkeypatch) -> None:
    class DeltaClient(_FakeOptionClient):
        def req_option_bid_ask_1m(self, contract, *, start, end, use_rth: bool = True):
            raise RuntimeError("force snapshot")

    def fake_session_window(session_date: date, *, tz=None):
        zone = tz or EASTERN
        start = datetime.combine(session_date, time(9, 30), tzinfo=EASTERN)
        end = datetime.combine(session_date, time(16, 0), tzinfo=EASTERN)
        return start.astimezone(zone), end.astimezone(zone)

    def fake_snapshot(client, contract, *, generic_ticks="100,101,104,106"):
        return {
            "bid": 1.0,
            "ask": 1.2,
            "delta": 0.45 if contract.strike == 217.5 else 0.20,
            "volume": 10,
        }

    monkeypatch.setattr(option_chain_meta, "trading_session_window", fake_session_window)
    monkeypatch.setattr(option_chain_meta, "_market_snapshot", fake_snapshot)

    quotes = option_chain_meta._build_option_quotes(
        DeltaClient(),
        symbol="NVDA",
        trade_date=date(2026, 5, 26),
        spot_price=214.84,
        expirations=["20260529"],
        strikes=[215.0, 217.5],
        exchange="SMART",
        trading_class="NVDA",
        multiplier="100",
        dte_min=0,
        dte_max=7,
        otm_min=0,
        otm_max=5,
        rights=["CALL"],
        delta_min=0.35,
        delta_max=0.55,
        max_per_side=2,
    )

    assert [quote.option_right for quote in quotes] == ["CALL"]
    assert [quote.strike for quote in quotes] == [option_chain_meta.Decimal("217.5")]


def test_l1_candidate_filter_respects_available_delta() -> None:
    def candidate(conid: int, delta: str | None) -> OptionCandidateRow:
        return OptionCandidateRow(
            conid=conid,
            underlying_symbol="NVDA",
            expiry=date(2026, 5, 29),
            strike=option_chain_meta.Decimal("220"),
            option_right="CALL",
            dte=3,
            delta=option_chain_meta.Decimal(delta) if delta is not None else None,
            gamma=None,
            theta=None,
            vega=None,
            bid=option_chain_meta.Decimal("1.0"),
            ask=option_chain_meta.Decimal("1.1"),
            mid=option_chain_meta.Decimal("1.05"),
            open_interest=1000,
            volume=200,
            min_tick=option_chain_meta.Decimal("0.01"),
            trading_class=None,
            multiplier=None,
            exchange=None,
            implied_vol=None,
            underlying_price=None,
        )

    filtered = option_l1._filter_candidates(
        [candidate(1, "0.20"), candidate(2, "0.45"), candidate(3, None)],
        option_right="CALL",
        spot_price=214.84,
        otm_min=0,
        otm_max=5,
        delta_min=option_chain_meta.Decimal("0.35"),
        delta_max=option_chain_meta.Decimal("0.55"),
    )

    assert [item.conid for item in filtered] == [2, 3]
