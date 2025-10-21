from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, cast

import redis


class RedisBlockStore:
    """Durable symbol block registry backed by Redis."""

    def __init__(
        self, url: str | None, *, prefix: str = "risk:block:", client: redis.Redis | None = None
    ) -> None:
        if client is not None:
            self._redis: redis.Redis = client
        else:
            if url is None:
                raise ValueError("RedisBlockStore requires a redis client or url")
            self._redis = redis.Redis.from_url(url, decode_responses=True)
        self._prefix = prefix

    def block(self, symbol: str, until: datetime, reason: str) -> None:
        symbol_key = symbol.upper()
        ttl_seconds = int(max((until - self._utc_now()).total_seconds(), 1))
        payload = {"until": until.isoformat(), "reason": reason}
        self._redis.set(self._key(symbol_key), json.dumps(payload), ex=ttl_seconds)

    def unblock(self, symbol: str) -> None:
        self._redis.delete(self._key(symbol.upper()))

    def reason(self, symbol: str) -> Optional[str]:
        data = self._load(symbol.upper())
        if data is None:
            return None
        return data[1]

    def is_blocked(self, symbol: str, now_utc: datetime) -> bool:
        data = self._load(symbol.upper())
        if data is None:
            return False
        until, _ = data
        if now_utc >= until:
            self.unblock(symbol)
            return False
        return True

    def snapshot(self) -> List[Dict[str, object]]:
        now = self._utc_now()
        pattern = f"{self._prefix}*"
        cursor: int = 0
        results: List[Dict[str, object]] = []
        while True:
            scan_result = cast(Tuple[int, List[str]], self._redis.scan(cursor=cursor, match=pattern, count=100))
            cursor, keys = scan_result
            for key in keys:
                raw = self._redis.get(key)
                if not raw:
                    continue
                try:
                    data = json.loads(cast(str, raw))
                    until = datetime.fromisoformat(data["until"])
                    if until.tzinfo is None:
                        until = until.replace(tzinfo=timezone.utc)
                    reason = str(data.get("reason") or "SYMBOL")
                    ttl = max(0, int((until - now).total_seconds()))
                    symbol = key[len(self._prefix) :].upper()
                    results.append(
                        {
                            "symbol": symbol,
                            "reason": reason,
                            "until": until.isoformat(),
                            "ttl": ttl,
                        }
                    )
                except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                    continue
            if cursor == 0:
                break
        results.sort(key=lambda item: cast(str, item["symbol"]))
        return results

    def prune(self, now_utc: Optional[datetime] = None) -> None:
        """Remove expired blocks eagerly to keep Redis tidy."""

        now = now_utc or self._utc_now()
        cursor: int = 0
        pattern = f"{self._prefix}*"
        while True:
            scan_result = cast(Tuple[int, List[str]], self._redis.scan(cursor=cursor, match=pattern, count=100))
            cursor, keys = scan_result
            for key in keys:
                raw = self._redis.get(key)
                if not raw:
                    continue
                try:
                    data = json.loads(cast(str, raw))
                    until = datetime.fromisoformat(data["until"])
                    if until.tzinfo is None:
                        until = until.replace(tzinfo=timezone.utc)
                    if now >= until:
                        self._redis.delete(key)
                except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                    continue
            if cursor == 0:
                break

    def _load(self, symbol_key: str) -> Optional[Tuple[datetime, str]]:
        raw = self._redis.get(self._key(symbol_key))
        if not raw:
            return None
        try:
            data = json.loads(cast(str, raw))
            until = datetime.fromisoformat(data["until"])
            if until.tzinfo is None:
                until = until.replace(tzinfo=timezone.utc)
            reason = str(data.get("reason") or "SYMBOL")
            return until, reason
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def _key(self, symbol_key: str) -> str:
        return f"{self._prefix}{symbol_key}"

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)
