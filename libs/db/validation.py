from __future__ import annotations

from typing import Iterable

from sqlalchemy.engine import Engine
from sqlalchemy import text


_DEFAULT_REQUIRED_COLUMNS: tuple[str, ...] = ("mfi14", "stoch_k", "stoch_d")


def ensure_indicator_columns(engine: Engine, required: Iterable[str] | None = None) -> None:
    """
    确认 indicators_eq_1m 表包含关键指标列。

    如果缺失将抛出 RuntimeError，提示先运行最新的数据库迁移。
    """
    required_cols = tuple(required) if required is not None else _DEFAULT_REQUIRED_COLUMNS
    if not required_cols:
        return

    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'indicators_eq_1m'
                """
            )
        )
        existing = {row[0] for row in result}

    missing = [col for col in required_cols if col not in existing]
    if missing:
        raise RuntimeError(
            "indicators_eq_1m 缺少必须的指标列: "
            f"{', '.join(missing)}。请先执行最新的 Alembic 迁移。"
        )
