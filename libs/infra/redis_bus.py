from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Mapping

from redis.asyncio import Redis
from redis.exceptions import ResponseError

LOGGER = logging.getLogger(__name__)


class RedisBus:
    """Redis Streams based event bus with trace-aware idempotency."""

    _STREAM_PREFIX = "stream:"
    _TRACE_PREFIX = "trace:"

    def __init__(self, url: str, *, dedupe_ttl: int = 3600, maxlen: int = 10000) -> None:
        self._redis = Redis.from_url(url, encoding="utf-8", decode_responses=True)
        self._dedupe_ttl = dedupe_ttl
        self._maxlen = maxlen

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _stream_name(self, event: str) -> str:
        return f"{self._STREAM_PREFIX}{event}"

    def _trace_key(self, event: str, trace_id: str) -> str:
        return f"{self._TRACE_PREFIX}{event}:{trace_id}"

    async def _ensure_group(self, stream: str, group: str, *, start_id: str = "0") -> None:
        try:
            await self._redis.xgroup_create(stream, group, id=start_id, mkstream=True)
            LOGGER.debug("Created consumer group stream=%s group=%s", stream, group)
        except ResponseError as exc:  # pragma: no cover - expected when group exists
            if "BUSYGROUP" not in str(exc):
                raise

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------
    async def publish(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        trace_id: str | None = None,
        maxlen: int | None = None,
    ) -> str:
        trace = trace_id or str(payload.get("trace_id") or "")
        if not trace:
            raise ValueError("trace_id required for event publication")

        dedupe_key = self._trace_key(event, trace)
        inserted = await self._redis.set(dedupe_key, "1", nx=True, ex=self._dedupe_ttl)
        if not inserted:
            LOGGER.debug("Skipping duplicate event=%s trace_id=%s", event, trace)
            return ""

        stream = self._stream_name(event)
        entry_id = await self._redis.xadd(
            stream,
            {
                "trace_id": trace,
                "event": event,
                "payload": json.dumps(payload, default=str),
            },
            maxlen=maxlen or self._maxlen,
            approximate=True,
        )
        LOGGER.debug("Published event=%s trace_id=%s entry_id=%s", event, trace, entry_id)
        return entry_id

    async def ensure_group(self, event: str, group: str, *, start_id: str = "0") -> None:
        await self._ensure_group(self._stream_name(event), group, start_id=start_id)

    async def consume(
        self,
        event: str,
        group: str,
        consumer: str,
        *,
        count: int = 10,
        block_ms: int = 5000,
        ack: bool = True,
    ) -> list[dict[str, Any]]:
        stream = self._stream_name(event)
        await self._ensure_group(stream, group, start_id="0")
        response = await self._redis.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=count,
            block=block_ms,
        )
        if not response:
            return []

        entries: list[dict[str, Any]] = []
        for _stream_name, messages in response:
            for message_id, fields in messages:
                trace_id = fields.get("trace_id", "")
                payload_raw = fields.get("payload") or "{}"
                try:
                    payload = json.loads(payload_raw)
                except json.JSONDecodeError:  # pragma: no cover - defensive
                    payload = {"raw": payload_raw}
                entry = {"id": message_id, "trace_id": trace_id, "payload": payload}
                entries.append(entry)
                if ack:
                    await self._redis.xack(stream, group, message_id)
        return entries

    async def ack(self, event: str, group: str, ids: Iterable[str]) -> None:
        stream = self._stream_name(event)
        await self._redis.xack(stream, group, *ids)

    async def ping(self) -> bool:
        return await self._redis.ping()

    async def close(self) -> None:
        await self._redis.close()
