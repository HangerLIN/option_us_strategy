from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Mapping, Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore[assignment]

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from libs.core import configure_logging, get_settings
from libs.db.models import RiskLimit
from libs.infra.db import get_session_factory

LOGGER = logging.getLogger("scripts.load_risk_limits")


def _load_payload(path: Path) -> Iterable[Mapping[str, object]]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML 未安装，无法解析 YAML 文件")
        data = yaml.safe_load(raw)
    else:
        data = json.loads(raw)
    if isinstance(data, dict):
        # 支持 {"global": {"RISK_NOTIONAL_CAP": 6000000, ...}}
        entries: List[Mapping[str, object]] = []
        for scope, limits in data.items():
            if not isinstance(limits, Mapping):
                raise ValueError(f"Expect mapping for scope '{scope}'")
            for key, value in limits.items():
                entries.append({"scope": scope, "key": key, "value": value})
        return entries
    if isinstance(data, list):
        return data
    raise ValueError("配置格式必须为列表或字典")


def _normalise(entry: Mapping[str, object]) -> Mapping[str, object]:
    data: dict[str, Any] = dict(entry)
    key = data.pop("key", data.pop("limit_code", None))
    if not key:
        raise ValueError("缺少 key/limit_code 字段")
    key_str = str(key).upper()

    scope_raw = data.pop("scope", None)
    symbol_hint = data.get("symbol")
    scope = str(scope_raw).lower() if scope_raw is not None else None
    if scope is None:
        symbol_probe = str(symbol_hint).upper() if symbol_hint is not None else ""
        if symbol_probe in {"GLOBAL", "ALL", ""}:
            scope = "global"
        elif symbol_probe.startswith("BUCKET:"):
            scope = "symbol_bucket"
            data.setdefault("bucket", symbol_probe.split(":", 1)[1])
            data["symbol"] = ""
        elif symbol_probe:
            scope = "symbol"
        else:
            scope = "global"
    if scope not in {"global", "symbol", "symbol_bucket"}:
        raise ValueError(f"不支持的 scope: {scope}")
    if scope not in {"global", "symbol", "symbol_bucket"}:
        raise ValueError(f"不支持的 scope: {scope}")

    raw_value = data.pop("value", data.pop("limit_value", None))
    if raw_value is None:
        raise ValueError(f"{key_str} 缺少 value")
    value_str = str(raw_value)

    symbol = data.pop("symbol", None)
    bucket = data.pop("bucket", None)
    if scope == "symbol":
        if not symbol:
            raise ValueError(f"{key_str} scope=symbol 时必须提供 symbol")
        symbol_str = str(symbol).upper()
        bucket_str = ""
    elif scope == "symbol_bucket":
        if not bucket:
            raise ValueError(f"{key_str} scope=symbol_bucket 时必须提供 bucket")
        symbol_str = ""
        bucket_str = str(bucket).upper()
    else:
        symbol_str = ""
        bucket_str = ""

    effective_from = data.pop("effective_from", None)
    if effective_from is None:
        effective_dt = datetime.now(timezone.utc)
    else:
        if isinstance(effective_from, datetime):
            effective_dt = effective_from
        else:
            effective_dt = datetime.fromisoformat(str(effective_from))
        if effective_dt.tzinfo is None:
            effective_dt = effective_dt.replace(tzinfo=timezone.utc)
        else:
            effective_dt = effective_dt.astimezone(timezone.utc)

    updated_by = data.pop("updated_by", None) or "script"

    return {
        "key": key_str,
        "value": value_str,
        "scope": scope,
        "symbol": symbol_str,
        "bucket": bucket_str,
        "effective_from": effective_dt,
        "updated_by": str(updated_by),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="导入 risk_limits 热更新配置")
    parser.add_argument("path", type=Path, help="配置文件路径（JSON 或 YAML）")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="导入前清空 risk_limits 表",
    )
    args = parser.parse_args()

    if not args.path.exists():
        raise SystemExit(f"文件不存在: {args.path}")

    settings = get_settings()
    configure_logging(settings)
    session_factory = get_session_factory(settings)

    payload = [_normalise(entry) for entry in _load_payload(args.path)]
    if not payload:
        LOGGER.warning("未解析到任何限值，操作终止")
        return 0

    session: Session = session_factory()
    try:
        if args.truncate:
            deleted = session.query(RiskLimit).delete()
            LOGGER.info("已清空 risk_limits 表，删除记录 %s 条", deleted)

        for entry in payload:
            stmt = (
                insert(RiskLimit)
                .values(entry)
                .on_conflict_do_update(
                    index_elements=["key", "scope", "symbol", "bucket", "effective_from"],
                    set_={
                        "value": entry["value"],
                        "updated_at": func.now(),
                        "updated_by": entry["updated_by"],
                    },
                )
            )
            session.execute(stmt)

        session.commit()
        LOGGER.info("风险限值导入完成，共处理 %s 条记录", len(payload))
    except Exception:
        session.rollback()
        LOGGER.exception("导入风险限值失败")
        raise
    finally:
        session.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
