from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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
    """Resolve universe specifications into symbol lists."""

    DEFAULT_SPEC = "ref_market_cap:GLOBAL"

    def __init__(self, session: Session) -> None:
        self._session = session

    def resolve(self, spec: str | None) -> UniverseSpec:
        spec_value = (spec or self.DEFAULT_SPEC).strip()
        if not spec_value:
            spec_value = self.DEFAULT_SPEC

        provider, _, target = spec_value.partition(":")
        provider = provider.lower()
        target_value = target.strip() if target else ""

        if provider in {"ref_market_cap", "market_cap"}:
            return self._resolve_market_cap(target_value or None, original=spec_value)
        if provider == "file":
            return self._resolve_file(target_value, original=spec_value)
        if provider == "sql":
            return self._resolve_sql(target_value, original=spec_value)
        if provider == "list":
            return self._resolve_inline_list(target_value, original=spec_value)

        raise ValueError(f"Unsupported universe provider: {provider}")

    # ------------------------------------------------------------------
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

    def _resolve_file(self, path_str: str, *, original: str) -> UniverseSpec:
        if not path_str:
            raise ValueError("Universe file path must not be empty")
        path = Path(path_str).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Universe file not found: {path}")
        symbols: list[str] = []
        for raw in path.read_text(encoding="utf-8").splitlines():
            token = raw.strip()
            if not token or token.startswith("#"):
                continue
            symbols.append(token.upper())
        if not symbols:
            raise ValueError(f"Universe file produced no symbols: {path}")
        return UniverseSpec(
            code=original,
            symbols=_deduplicate(symbols),
            metadata={"provider": "file", "path": str(path)},
        )

    def _resolve_sql(self, target: str, *, original: str) -> UniverseSpec:
        if not target:
            raise ValueError("Universe SQL target must not be empty")
        sql_text: str
        candidate_path = Path(target).expanduser()
        if candidate_path.exists():
            sql_text = candidate_path.read_text(encoding="utf-8")
            metadata = {"provider": "sql", "path": str(candidate_path)}
        else:
            sql_text = target
            metadata = {"provider": "sql", "inline": True}

        stmt = text(sql_text)
        rows = self._session.execute(stmt).fetchall()
        if not rows:
            raise ValueError("Universe SQL returned no rows")

        symbols = []
        for row in rows:
            if isinstance(row, dict):
                value = row.get("symbol") or next(iter(row.values()))
            else:
                try:
                    value = row[0]
                except (TypeError, IndexError):
                    value = getattr(row, "symbol", None)
            if value is None:
                continue
            symbols.append(str(value).upper())
        if not symbols:
            raise ValueError("Universe SQL did not produce any symbols")
        metadata["rowcount"] = len(symbols)
        return UniverseSpec(code=original, symbols=_deduplicate(symbols), metadata=metadata)

    def _resolve_inline_list(self, payload: str, *, original: str) -> UniverseSpec:
        tokens = [token.strip().upper() for token in payload.split(",") if token.strip()]
        if not tokens:
            raise ValueError("Universe list is empty")
        return UniverseSpec(
            code=original,
            symbols=_deduplicate(tokens),
            metadata={"provider": "list", "count": len(tokens)},
        )


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
