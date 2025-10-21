from __future__ import annotations

import argparse
import csv
import logging
from datetime import time
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

from libs.core import configure_logging, get_settings
from libs.infra.db import get_session_factory

LOGGER = logging.getLogger("scripts.seed_calendar")


def _parse_bool(session_type: str) -> bool:
    return session_type.strip().upper() in {"HALF_DAY", "HALF", "SHORT"}


def main() -> int:
    parser = argparse.ArgumentParser(description="刷新 dim_trading_calendar 表")
    parser.add_argument(
        "path", type=Path, help="CSV 文件，包含 session_date,open_time,close_time,session_type"
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="导入前先清空 dim_trading_calendar",
    )
    args = parser.parse_args()

    if not args.path.exists():
        raise SystemExit(f"文件不存在: {args.path}")

    settings = get_settings()
    configure_logging(settings)
    session_factory = get_session_factory(settings)

    rows = []
    with args.path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"session_date", "open_time", "close_time", "session_type"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV 缺少必要字段: {', '.join(sorted(missing))}")
        for row in reader:
            trade_date = row["session_date"].strip()
            open_time = time.fromisoformat(row["open_time"].strip())
            close_time = time.fromisoformat(row["close_time"].strip())
            session_type = row["session_type"].strip()
            rows.append(
                {
                    "trade_date": trade_date,
                    "is_half_day": _parse_bool(session_type),
                    "open_et": open_time,
                    "close_et": close_time,
                }
            )

    if not rows:
        LOGGER.warning("未读取到任何交易日，操作终止")
        return 0

    session: Session = session_factory()
    try:
        if args.truncate:
            session.execute(text("TRUNCATE TABLE dim_trading_calendar"))
            LOGGER.info("已清空 dim_trading_calendar 表")

        upsert_sql = text(
            """
            INSERT INTO dim_trading_calendar (trade_date, is_half_day, open_et, close_et)
            VALUES (:trade_date, :is_half_day, :open_et, :close_et)
            ON CONFLICT (trade_date)
            DO UPDATE SET
                is_half_day = EXCLUDED.is_half_day,
                open_et = EXCLUDED.open_et,
                close_et = EXCLUDED.close_et
            """
        )
        for row in rows:
            session.execute(upsert_sql, row)

        session.commit()
        LOGGER.info("交易日历导入完成，共处理 %s 条记录", len(rows))
    except Exception:
        session.rollback()
        LOGGER.exception("刷新交易日历失败")
        raise
    finally:
        session.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
