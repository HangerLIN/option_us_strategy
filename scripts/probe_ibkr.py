from __future__ import annotations

import argparse
import logging
import time

from libs.core import configure_logging, get_settings
from libs.infra import build_ibkr_client

LOGGER = logging.getLogger("scripts.probe_ibkr")


def main() -> int:
    parser = argparse.ArgumentParser(description="IBKR 连通性与 pacing 探针")
    parser.add_argument(
        "--max-seconds",
        type=float,
        default=15.0,
        help="允许的最大连接耗时，超出即视为失败",
    )
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings)

    start = time.perf_counter()
    try:
        client = build_ibkr_client(settings)
    except Exception:
        LOGGER.exception("IBKR 连接失败")
        raise SystemExit(1)

    elapsed = time.perf_counter() - start
    LOGGER.info("IBKR 连接成功，耗时 %.2fs", elapsed)
    if elapsed > args.max_seconds:
        LOGGER.error("连接耗时超出阈值 %.2fs > %.2fs", elapsed, args.max_seconds)
        client.disconnect_and_stop()
        raise SystemExit(2)

    client.disconnect_and_stop()
    LOGGER.info("IBKR 探针完成，连接与节流状态正常")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
