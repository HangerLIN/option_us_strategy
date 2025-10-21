#!/usr/bin/env python
"""手动运行盘前Top5选股"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path

# 设置独立的client ID避免冲突
os.environ['IB_CLIENT_ID'] = '98'

import structlog

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from libs.core import EASTERN, configure_logging
from libs.core.config import get_settings
from libs.infra import get_session_factory, build_ibkr_client
from apps.rt_engine.top5_service import Top5Service

LOGGER = structlog.get_logger(__name__)


def main() -> int:
    import argparse
    
    parser = argparse.ArgumentParser(description="运行盘前Top5选股")
    parser.add_argument(
        "--date",
        type=str,
        help="交易日期 (YYYY-MM-DD格式)，默认为今天",
    )
    args = parser.parse_args()
    
    settings = get_settings()
    configure_logging(settings)
    
    # 确定交易日期
    if args.date:
        et_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        et_date = datetime.now(EASTERN).date()
    
    LOGGER.info("top5_manual.start", trade_date=str(et_date))
    
    # 准备服务
    session_factory = get_session_factory(settings)
    ib_client = build_ibkr_client(settings)
    
    try:
        # 连接IBKR
        LOGGER.info("ibkr.connecting", client_id=settings.ib_client_id)
        ib_client.connect_and_wait(timeout=30.0)
        
        # 运行Top5选股
        top5_service = Top5Service(ib_client, session_factory, settings)
        top5_service.run_for_today(et_date)
        
        LOGGER.info("top5_manual.success", trade_date=str(et_date))
        
        # 查询结果
        with session_factory() as session:
            from libs.db import PremarketTop5DAO
            dao = PremarketTop5DAO(session)
            results = dao.fetch(et_date)
            
            if results:
                print(f"\n📊 {et_date} 盘前Top5选股结果：\n")
                for r in sorted(results, key=lambda x: x.rank):
                    print(f"  {r.rank}. {r.symbol:6s} - 收益率: {r.ret_0925_0930:>7.2%}")
            else:
                print(f"\n⚠️  {et_date} 无Top5结果")
        
        return 0
        
    except Exception as exc:
        LOGGER.exception("top5_manual.failed", error=str(exc))
        return 1
    finally:
        ib_client.disconnect_and_stop()


if __name__ == "__main__":
    sys.exit(main())

