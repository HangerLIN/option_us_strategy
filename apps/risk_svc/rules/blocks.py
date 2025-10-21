from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional, Tuple


class BlockStore:
    """In-memory symbol block registry with TTL support."""

    def __init__(self) -> None:
        self._blocks: Dict[str, Tuple[datetime, str]] = {}

    def block(self, symbol: str, until: datetime, reason: str) -> None:
        self._blocks[symbol.upper()] = (until, reason)

    def unblock(self, symbol: str) -> None:
        self._blocks.pop(symbol.upper(), None)

    def reason(self, symbol: str) -> Optional[str]:
        entry = self._blocks.get(symbol.upper())
        if entry is None:
            return None
        return entry[1]

    def is_blocked(self, symbol: str, now: datetime) -> bool:
        symbol_key = symbol.upper()
        entry = self._blocks.get(symbol_key)
        if entry is None:
            return False
        expiry, _ = entry
        if now >= expiry:
            self._blocks.pop(symbol_key, None)
            return False
        return True

    def prune(self, now: datetime) -> None:
        expired = [symbol for symbol, (expiry, _) in self._blocks.items() if expiry <= now]
        for symbol in expired:
            self._blocks.pop(symbol, None)

    def snapshot(self) -> Dict[str, Tuple[datetime, str]]:
        return dict(self._blocks)
