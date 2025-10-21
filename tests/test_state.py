from __future__ import annotations

from decimal import Decimal
from typing import Tuple

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.risk_svc.state import RiskStateAggregator
from apps.risk_svc.service import RiskService
from libs.db import Base


class StubRedisBus:
    def __init__(self) -> None:
        self.published: list[Tuple[str, dict, str]] = []

    async def publish(
        self, event: str, payload: dict, *, trace_id: str, maxlen: int | None = None
    ) -> str:
        self.published.append((event, payload, trace_id))
        return "1-0"


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(self, key: str, value: str) -> None:
        self.store[key] = value


@pytest.mark.asyncio
async def test_kill_switch_broadcasts_block_and_unblock() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, future=True)

    risk_service = RiskService(session_factory, redis_url="redis://localhost:6379/0")
    risk_service._redis = FakeRedis()  # type: ignore[attr-defined]

    bus = StubRedisBus()
    aggregator = RiskStateAggregator(session_factory, risk_service, bus)

    await aggregator._emit_kill_switch_alert(True, Decimal("-1000"), Decimal("-900"), "trace")
    assert any(event == "risk_alert" for event, _, _ in bus.published)
    assert any(event == "risk_block" for event, _, _ in bus.published)

    bus.published.clear()

    await aggregator._emit_kill_switch_alert(False, Decimal("-800"), Decimal("-900"), "trace")
    assert any(event == "risk_unblock" for event, _, _ in bus.published)
