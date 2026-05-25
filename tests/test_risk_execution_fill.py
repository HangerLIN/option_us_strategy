from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from collections import defaultdict
import fnmatch
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker

from risk_svc.block_store import RedisBlockStore  # type: ignore[import-not-found]
from risk_svc.subscribers import RiskStreamConsumers  # type: ignore[import-not-found]
from risk_svc.service import RiskService  # type: ignore[import-not-found]
from libs.core.config import get_settings
from libs.db import Base
from libs.db.models import PnLIntraday, RiskEvent, RiskState, StrategyPosition
from libs.schemas.events import ExecutionFill


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = defaultdict(dict)
        self.ttl: dict[str, int] = {}

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value
        if ex is not None:
            self.ttl[key] = ex

    def setex(self, key: str, ttl: int, value: str) -> None:
        self.set(key, value, ex=ttl)

    def delete(self, key: str) -> None:
        self.store.pop(key, None)
        self.zsets.pop(key, None)

    def expire(self, key: str, ttl: int) -> None:
        self.ttl[key] = ttl

    def scan(
        self, cursor: int = 0, match: str | None = None, count: int | None = None
    ) -> tuple[int, list[str]]:
        keys = list(self.store.keys())
        if match is not None:
            keys = [key for key in keys if fnmatch.fnmatch(key, match)]
        return 0, keys

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        zset = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            zset[member] = float(score)

    def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        zset = self.zsets.get(key)
        if not zset:
            return 0
        min_value = float(min_score)
        max_value = float(max_score)
        to_remove = [member for member, score in zset.items() if min_value <= score <= max_value]
        for member in to_remove:
            zset.pop(member, None)
        return len(to_remove)

    def zrange(self, key: str, start: int, end: int, withscores: bool = False) -> list:
        zset = self.zsets.get(key, {})
        ordered_items = sorted(zset.items(), key=lambda item: item[1])
        if end == -1:
            sliced = ordered_items[start:]
        else:
            sliced = ordered_items[start : end + 1]
        if withscores:
            return [(member, score) for member, score in sliced]
        return [member for member, _ in sliced]

    def zcard(self, key: str) -> int:
        return len(self.zsets.get(key, {}))

    def zrem(self, key: str, *members: str) -> int:
        zset = self.zsets.get(key)
        if not zset:
            return 0
        removed = 0
        for member in members:
            if zset.pop(member, None) is not None:
                removed += 1
        return removed


class StubRedisBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict, str]] = []

    async def publish(
        self, event: str, payload: dict, *, trace_id: str, maxlen: int | None = None
    ) -> str:
        self.published.append((event, payload, trace_id))
        return "1-0"


class StubBlockStore:
    def __init__(self, reasons: dict[str, str] | None = None) -> None:
        self._reasons = {symbol.upper(): reason for symbol, reason in (reasons or {}).items()}
        self.unblocked: list[str] = []

    def reason(self, symbol: str) -> str | None:
        return self._reasons.get(symbol.upper())

    def unblock(self, symbol: str) -> None:
        self._reasons.pop(symbol.upper(), None)
        self.unblocked.append(symbol.upper())


def _sqlite_engine():
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _register_date_trunc(dbapi_connection, connection_record):
        dbapi_connection.create_function("date_trunc", 2, lambda _part, value: value)

    return engine


@pytest.fixture(autouse=True)
def _settings_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("IB_HOST", "127.0.0.1")
    monkeypatch.setenv("IB_PORT", "4002")
    monkeypatch.setenv("IB_CLIENT_ID", "1")
    monkeypatch.setenv("IB_ACCOUNT", "DU123456")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_handle_execution_fill_updates_state_and_unblocks() -> None:
    engine = _sqlite_engine()
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, future=True)

    risk_service = RiskService(session_factory, redis_url="redis://localhost:6379/0")
    risk_service._redis = FakeRedis()  # type: ignore[attr-defined]
    risk_service._blocks = StubBlockStore({"AAPL": "MISSED_ENTRY"})  # type: ignore[attr-defined]

    bus = StubRedisBus()
    consumers = RiskStreamConsumers(
        bus,
        risk_service,
        session_factory=session_factory,
    )

    filled_at = datetime(2024, 7, 1, 14, 30, tzinfo=timezone.utc)
    payload = ExecutionFill(
        trace_id="trace-1",
        order_id=1,
        client_order_id="cl-1",
        symbol="AAPL",
        side="BUY",
        fill_quantity=Decimal("1"),
        fill_price=Decimal("2.5"),
        strategy_code="core-vol",
        signal_code="SIG-OPEN",
        option_right="CALL",
        conid=123456,
        strike=Decimal("100"),
        expiry=datetime(2024, 7, 19, tzinfo=timezone.utc),
        delta=0.45,
        fees=Decimal("1.25"),
        execution_id="exec-1",
        filled_at=filled_at,
    ).model_dump()

    await consumers._handle_execution_fill({"payload": payload, "trace_id": "trace-1"})

    with session_factory() as session:
        position = session.execute(select(StrategyPosition)).scalar_one()
        assert position.strategy_code == "core-vol"
        assert position.symbol == "AAPL"
        assert position.option_right == "CALL"
        assert position.open_quantity == 1
        assert position.source_signal_code == "SIG-OPEN"
        assert position.conid == 123456
        assert position.strike == Decimal("100")
        assert str(position.expiry) == "2024-07-19"
        assert position.delta == Decimal("0.45")

        pnl_entry = session.execute(select(PnLIntraday)).scalar_one()
        assert pnl_entry.realized == Decimal("0")
        assert pnl_entry.fees == Decimal("1.25")

        risk_rows = session.execute(
            select(RiskState.metric_code, RiskState.metric_value, RiskState.detail).where(
                RiskState.symbol == "AAPL"
            )
        ).all()
        metrics = {code: value for code, value, _ in risk_rows}
        assert metrics["NET_QTY"] == Decimal("1")
        assert metrics["NOTIONAL"] == Decimal("250")
        assert metrics["REALIZED_PNL"] == Decimal("-1.25")

        event = session.execute(select(RiskEvent)).scalar_one()
        assert event.event_code == "UNBLOCK"
        assert event.payload["blocked_reason"] == "MISSED_ENTRY"

    assert bus.published == [
        (
            "risk_unblock",
            {"strategy_code": "core-vol", "symbol": "AAPL", "reason": "signal_filled"},
            "trace-1",
        )
    ]
    assert "AAPL" in risk_service._blocks.unblocked  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_update_vix_gate_persists_and_blocks() -> None:
    engine = _sqlite_engine()
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, future=True)

    risk_service = RiskService(session_factory, redis_url="redis://localhost:6379/0")
    fake_redis = FakeRedis()
    risk_service._redis = fake_redis  # type: ignore[attr-defined]

    bus = StubRedisBus()
    now = datetime(2024, 7, 1, 14, 30, tzinfo=timezone.utc)

    with session_factory() as session:
        await risk_service.update_vix_gate(
            session, now, Decimal("25"), redis_bus=bus, trace_id="gate"
        )
        session.commit()

    gate_state = fake_redis.get("risk:gate:vix")
    assert gate_state is not None
    state_payload = json.loads(gate_state)
    assert state_payload["open"] is False
    assert any(event == "risk_block" for event, _, _ in bus.published)

    bus.published.clear()

    with session_factory() as session:
        await risk_service.update_vix_gate(
            session, now + timedelta(minutes=1), Decimal("18"), redis_bus=bus, trace_id="gate"
        )
        session.commit()

    gate_state = fake_redis.get("risk:gate:vix")
    assert gate_state is not None
    state_payload = json.loads(gate_state)
    assert state_payload["open"] is True
    assert any(event == "risk_unblock" for event, _, _ in bus.published)


def test_get_vix_value_prefers_redis_snapshot() -> None:
    engine = _sqlite_engine()
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, future=True)

    risk_service = RiskService(session_factory, redis_url="redis://localhost:6379/0")
    fake_redis = FakeRedis()
    risk_service._redis = fake_redis  # type: ignore[attr-defined]

    snapshot_time = datetime(2024, 7, 1, 13, 0, tzinfo=timezone.utc)
    fake_redis.set(
        "risk:vix:last",
        json.dumps({"value": "21.5", "ts": snapshot_time.isoformat()}),
    )

    snapshot = risk_service._read_vix_snapshot()  # type: ignore[attr-defined]
    assert snapshot is not None
    _, snapshot_ts = snapshot
    diff_seconds = (snapshot_time + timedelta(seconds=30) - snapshot_ts).total_seconds()
    assert diff_seconds == 30

    with session_factory() as session:
        value = risk_service.get_vix_value(session, snapshot_time + timedelta(seconds=30))

    assert value == Decimal("21.5")


@pytest.mark.asyncio
async def test_signal_concurrency_tracking() -> None:
    engine = _sqlite_engine()
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, future=True)

    risk_service = RiskService(session_factory, redis_url="redis://localhost:6379/0")
    fake_redis = FakeRedis()
    risk_service._redis = fake_redis  # type: ignore[attr-defined]

    bus = StubRedisBus()
    consumers = RiskStreamConsumers(
        bus,
        risk_service,
        session_factory=session_factory,
    )

    generated_at = "2024-07-01T13:31:00+00:00"
    await consumers._handle_signal(
        {
            "trace_id": "sig-1",
            "payload": {
                "status": "emitted",
                "symbol": "AAPL",
                "strategy_code": "core-vol",
                "signal_code": "SIG_OPEN_CHASE_BUY",
                "generated_at": generated_at,
            },
        }
    )

    now_et = risk_service._to_eastern(datetime.fromisoformat(generated_at))  # type: ignore[attr-defined]
    symbols = risk_service._fetch_concurrency_symbols(now_et)
    assert symbols is not None and "AAPL" in symbols
    snapshot_before = risk_service.concurrency_snapshot()
    assert snapshot_before["count"] >= 1

    await consumers._handle_signal(
        {
            "trace_id": "sig-2",
            "payload": {
                "status": "filled",
                "symbol": "AAPL",
                "strategy_code": "core-vol",
                "signal_code": "SIG_OPEN_CHASE_BUY",
                "filled_at": "2024-07-01T13:32:00+00:00",
            },
        }
    )

    symbols_after = risk_service._fetch_concurrency_symbols(now_et)
    assert symbols_after is not None and "AAPL" not in symbols_after
    snapshot_after = risk_service.concurrency_snapshot()
    assert snapshot_after["count"] == 0


def test_list_blocks_snapshot() -> None:
    engine = _sqlite_engine()
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, future=True)

    risk_service = RiskService(session_factory, redis_url="redis://localhost:6379/0")
    fake_redis = FakeRedis()
    risk_service._redis = fake_redis  # type: ignore[attr-defined]
    risk_service._blocks = RedisBlockStore(None, client=fake_redis)  # type: ignore[attr-defined]

    until = datetime(2024, 7, 1, 20, 0, tzinfo=timezone.utc)
    risk_service._blocks.block("AAPL", until, "TEST")

    blocks = risk_service.list_blocks()
    assert blocks
    assert blocks[0]["symbol"] == "AAPL"
    assert blocks[0]["reason"] == "TEST"
