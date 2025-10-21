from __future__ import annotations
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from libs.core import attach_trace_metadata, bind_trace_context, clear_trace_context
from libs.infra.redis_bus import RedisBus
from libs.schemas.events import BarsClosed


@pytest.mark.asyncio
async def test_redis_stream_publish_consume_bars_closed() -> None:
    bus = RedisBus("redis://localhost:6379/0")
    try:
        await bus.ping()
    except Exception:  # pragma: no cover - skip when redis unavailable
        await bus.close()
        pytest.skip("Redis server not available for stream test")

    clear_trace_context()
    trace_id = str(uuid.uuid4())
    bind_trace_context(trace_id=trace_id, symbol="SPY")

    bar = BarsClosed(
        trace_id=trace_id,
        symbol="SPY",
        bar_start=datetime(2024, 7, 1, 13, 59, tzinfo=timezone.utc),
        bar_end=datetime(2024, 7, 1, 14, 0, tzinfo=timezone.utc),
        timeframe="1m",
        open=Decimal("551.25"),
        high=Decimal("551.60"),
        low=Decimal("551.10"),
        close=Decimal("551.45"),
        volume=125000,
        vwap=Decimal("551.35"),
        source="selfcheck",
        received_at=datetime.now(timezone.utc),
    )
    payload = attach_trace_metadata(bar.model_dump())
    entry_id = await bus.publish("bars.closed", payload, trace_id=trace_id)
    assert entry_id

    await bus.ensure_group("bars.closed", "test-cg")
    events = await bus.consume("bars.closed", "test-cg", "consumer-1", count=1, block_ms=1000)
    assert events
    event = events[-1]
    assert event["trace_id"] == trace_id
    assert event["payload"]["symbol"] == "SPY"
    await bus.close()
