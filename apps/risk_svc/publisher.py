from __future__ import annotations

from typing import Any, Mapping

import structlog

from libs.core import utc_now

from libs.infra.redis_bus import RedisBus

LOGGER = structlog.get_logger(__name__)


async def publish_risk_block(bus: RedisBus, payload: Mapping[str, Any], *, trace_id: str) -> None:
    await _safe_publish(bus, "risk_block", payload, trace_id=trace_id)


async def publish_risk_unblock(bus: RedisBus, payload: Mapping[str, Any], *, trace_id: str) -> None:
    await _safe_publish(bus, "risk_unblock", payload, trace_id=trace_id)


async def publish_risk_alert(bus: RedisBus, payload: Mapping[str, Any], *, trace_id: str) -> None:
    await _safe_publish(bus, "risk_alert", payload, trace_id=trace_id)


async def publish_force_close(bus: RedisBus, payload: Mapping[str, Any], *, trace_id: str) -> None:
    enriched = dict(payload)
    enriched.setdefault("triggered_at", enriched.get("triggered_at") or utc_now())
    await _safe_publish(bus, "force_close", enriched, trace_id=trace_id)


async def _safe_publish(
    bus: RedisBus, event: str, payload: Mapping[str, Any], *, trace_id: str
) -> None:
    try:
        await bus.publish(event, dict(payload), trace_id=trace_id)
        LOGGER.debug("risk_publisher.event_sent", channel=event, trace_id=trace_id)
    except Exception:  # pragma: no cover - external dependency
        LOGGER.exception("risk_publisher.event_failed", channel=event, trace_id=trace_id)
