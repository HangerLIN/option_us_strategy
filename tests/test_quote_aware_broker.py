from __future__ import annotations

from types import SimpleNamespace

from apps.backtest.broker.quote_aware import QuoteAwareOptionBroker


class _Line:
    def __init__(self, value: float) -> None:
        self.value = value

    def __getitem__(self, index: int) -> float:
        assert index == 0
        return self.value


class _Data:
    def __init__(self, *, bid: float, ask: float, quote_age_seconds: float = 0) -> None:
        self.bid = _Line(bid)
        self.ask = _Line(ask)
        self.quote_age_seconds = _Line(quote_age_seconds)


class _Order:
    def __init__(
        self,
        *,
        buy: bool,
        bid: float,
        ask: float,
        quote_age_seconds: float = 0,
        reject_reason: str | None = None,
    ) -> None:
        self.data = _Data(
            bid=bid,
            ask=ask,
            quote_age_seconds=quote_age_seconds,
        )
        self._buy = buy
        self.info: dict[str, object] = {}
        self.rejected = False
        self.owner = (
            SimpleNamespace(
                _validate_order_fill=lambda order, price: reject_reason,
            )
            if reject_reason is not None
            else SimpleNamespace()
        )

    def isbuy(self) -> bool:
        return self._buy

    def addinfo(self, **fields: object) -> None:
        self.info.update(fields)

    def reject(self, broker) -> None:
        self.rejected = True


def test_quote_aware_broker_executes_buy_at_ask_when_limit_crosses() -> None:
    broker = QuoteAwareOptionBroker()
    executions: list[float] = []
    broker._execute = lambda order, ago, price: executions.append(price)  # type: ignore[method-assign]

    broker._try_exec_limit(_Order(buy=True, bid=2.9, ask=3.2), 0, 0, 0, 3.25)

    assert executions == [3.2]


def test_quote_aware_broker_executes_sell_at_bid_when_limit_crosses() -> None:
    broker = QuoteAwareOptionBroker()
    executions: list[float] = []
    broker._execute = lambda order, ago, price: executions.append(price)  # type: ignore[method-assign]

    broker._try_exec_limit(_Order(buy=False, bid=5.8, ask=6.2), 0, 0, 0, 5.75)

    assert executions == [5.8]


def test_quote_aware_broker_does_not_execute_uncrossed_buy() -> None:
    broker = QuoteAwareOptionBroker()
    executions: list[float] = []
    broker._execute = lambda order, ago, price: executions.append(price)  # type: ignore[method-assign]

    broker._try_exec_limit(_Order(buy=True, bid=2.9, ask=3.2), 0, 0, 0, 3.10)

    assert executions == []


def test_quote_aware_broker_does_not_fallback_to_ohlc_for_stale_l1() -> None:
    broker = QuoteAwareOptionBroker()
    executions: list[float] = []
    broker._execute = lambda order, ago, price: executions.append(price)  # type: ignore[method-assign]

    broker._try_exec_limit(
        _Order(buy=True, bid=2.9, ask=3.2, quote_age_seconds=120),
        3.1,
        3.3,
        3.0,
        3.25,
    )

    assert executions == []


def test_quote_aware_broker_rejects_strategy_invalid_fill_before_execution() -> None:
    broker = QuoteAwareOptionBroker()
    executions: list[float] = []
    notifications: list[_Order] = []
    broker._execute = lambda order, ago, price: executions.append(price)  # type: ignore[method-assign]
    broker.notify = notifications.append  # type: ignore[method-assign]
    order = _Order(
        buy=True,
        bid=2.9,
        ask=3.2,
        reject_reason="signal_to_fill_seconds>90",
    )

    broker._try_exec_limit(order, 0, 0, 0, 3.25)

    assert executions == []
    assert order.rejected is True
    assert order.info["fill_reject_reason"] == "signal_to_fill_seconds>90"
    assert notifications == [order]
