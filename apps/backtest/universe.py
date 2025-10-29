from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session

from libs.core import get_settings

LOGGER = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class UniverseSpec:
    code: str
    symbols: List[str]
    metadata: dict[str, object]


class UniverseResolver:
    """Resolve universe specifications into symbol lists from database."""

    DEFAULT_SPEC = "stock_universe"

    def __init__(self, session: Session) -> None:
        self._session = session

    def resolve(self, spec: str | None) -> UniverseSpec:
        """
        解析股票池规范，统一从数据库表读取。
        
        支持的格式：
        - "stock_universe" 或 None: 从stock_universe表读取所有active股票
        - "stock_universe:超大盘股": 按market_cap_tier筛选
        - "ref_market_cap": 兼容旧格式，从ref_market_cap表读取
        """
        spec_value = (spec or self.DEFAULT_SPEC).strip()
        if not spec_value:
            spec_value = self.DEFAULT_SPEC

        provider, _, target = spec_value.partition(":")
        provider = provider.lower()
        target_value = target.strip() if target else ""

        # 优先使用stock_universe表
        if provider in {"stock_universe", "universe"}:
            return self._resolve_stock_universe(target_value or None, original=spec_value)
        
        # 兼容旧的ref_market_cap格式
        if provider in {"ref_market_cap", "market_cap"}:
            return self._resolve_market_cap(target_value or None, original=spec_value)

        raise ValueError(
            f"Unsupported universe provider: {provider}. "
            f"Use 'stock_universe' or 'ref_market_cap'"
        )

    # ------------------------------------------------------------------
    def _resolve_stock_universe(self, tier: str | None, *, original: str) -> UniverseSpec:
        """
        从stock_universe表读取股票池。
        
        Args:
            tier: 市值分类筛选，如"超大盘股"、"大盘科技股"等，None表示全部
            original: 原始规范字符串
        """
        if tier:
            stmt = text(
                """
                SELECT symbol
                FROM stock_universe
                WHERE active = true
                  AND market_cap_tier = :tier
                ORDER BY sort_order
                """
            )
            rows = self._session.execute(stmt, {"tier": tier}).scalars().all()
            metadata = {"provider": "stock_universe", "tier": tier, "table": "stock_universe"}
        else:
            stmt = text(
                """
                SELECT symbol
                FROM stock_universe
                WHERE active = true
                ORDER BY sort_order
                """
            )
            rows = self._session.execute(stmt).scalars().all()
            metadata = {"provider": "stock_universe", "tier": "all", "table": "stock_universe"}
        
        symbols = _normalise_symbols(rows)
        if not symbols:
            raise ValueError(f"stock_universe table returned no symbols for tier={tier}")
        
        metadata["count"] = len(symbols)
        return UniverseSpec(code=original, symbols=symbols, metadata=metadata)

    def _resolve_market_cap(self, source: str | None, *, original: str) -> UniverseSpec:
        stmt = text(
            """
            SELECT mc.symbol
            FROM ref_market_cap AS mc
            JOIN compliance_whitelist_largecap AS wl
              ON wl.symbol = mc.symbol
            WHERE (:source IS NULL OR mc.source = :source)
              AND (
                wl.min_market_cap_usd IS NULL
                OR mc.market_cap_usd >= wl.min_market_cap_usd
              )
            ORDER BY mc.market_cap_usd DESC NULLS LAST
            """
        )
        rows = self._session.execute(stmt, {"source": source}).scalars().all()
        symbols = _normalise_symbols(rows)
        metadata: dict[str, object] = {"provider": "ref_market_cap", "source": source or "*"}
        if not symbols:
            settings = get_settings()
            fallback = [tok.strip().upper() for tok in settings.top5_fixed_pool.split(",") if tok.strip()]
            symbols = _deduplicate(fallback)
            metadata.update({"fallback": "settings.top5_fixed_pool"})
            LOGGER.warning(
                "universe.fallback_top5_pool",
                original=original,
                fallback_count=len(symbols),
            )
        return UniverseSpec(code=original, symbols=symbols, metadata=metadata)


def _normalise_symbols(values: Iterable[str]) -> list[str]:
    return _deduplicate([value.upper() for value in values if value])


def _deduplicate(symbols: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in symbols:
        if symbol not in seen:
            ordered.append(symbol)
            seen.add(symbol)
    return ordered
