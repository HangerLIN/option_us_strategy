from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from typing import Optional

from libs.core import configure_logging, get_settings, utc_now
from libs.infra.redis_bus import RedisBus
from libs.schemas.events import RiskParamReload

LOGGER = logging.getLogger("scripts.send_risk_param_reload")


async def _publish(triggered_by: str, trace_id: Optional[str]) -> None:
    settings = get_settings()
    configure_logging(settings)
    bus = RedisBus(settings.redis_url)
    event = RiskParamReload(
        trace_id=trace_id or f"risk-reload-{int(datetime.utcnow().timestamp())}",
        triggered_by=triggered_by,
        reload_at=utc_now(),
    )
    entry_id = await bus.publish(
        "risk_param_reload",
        event.model_dump(mode="json"),
        trace_id=event.trace_id,
    )
    await bus.close()
    if not entry_id:
        LOGGER.warning("风险参数热更新事件已存在 trace_id=%s，未重复发送", event.trace_id)
    else:
        LOGGER.info("风险参数热更新事件已发布 trace_id=%s entry_id=%s", event.trace_id, entry_id)


def main() -> int:
    parser = argparse.ArgumentParser(description="发布 risk_param_reload 事件触发风控热更新")
    parser.add_argument(
        "--triggered-by",
        default="manual-cli",
        help="触发人或任务标识",
    )
    parser.add_argument(
        "--trace-id",
        default=None,
        help="自定义 trace_id，留空自动生成",
    )
    args = parser.parse_args()
    asyncio.run(_publish(args.triggered_by, args.trace_id))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
