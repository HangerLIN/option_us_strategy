#!/usr/bin/env python
"""获取盘前Top5股票的期权链数据"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

# 设置独立的client ID避免冲突
os.environ['IB_CLIENT_ID'] = '99'

import structlog
from sqlalchemy.orm import sessionmaker

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from libs.core import EASTERN, configure_logging
from libs.core.config import get_settings
from libs.db import PremarketTop5DAO
from libs.infra import get_session_factory, IBClient

LOGGER = structlog.get_logger(__name__)


def get_top5_symbols(session_factory: sessionmaker, et_date: date | None = None) -> List[str]:
    """从数据库获取盘前Top5股票列表"""
    if et_date is None:
        et_date = datetime.now(EASTERN).date()
    
    with session_factory() as session:
        dao = PremarketTop5DAO(session)
        records = dao.fetch(et_date)
        
        if not records:
            LOGGER.warning("top5.not_found", trade_date=str(et_date))
            return []
        
        # 按rank排序
        records.sort(key=lambda r: r.rank)
        symbols = [r.symbol for r in records]
        
        LOGGER.info(
            "top5.loaded",
            trade_date=str(et_date),
            symbols=symbols,
            entries=[
                {"rank": r.rank, "symbol": r.symbol, "ret": str(r.ret_0925_0930)}
                for r in records
            ],
        )
        return symbols


def get_option_chains(
    ib_client: IBClient, symbols: List[str]
) -> Dict[str, Dict[str, Any]]:
    """为每个股票获取期权链参数"""
    chains: Dict[str, Dict[str, Any]] = {}
    
    for symbol in symbols:
        try:
            # 先获取股票合约详情以得到conId
            LOGGER.info("contract.requesting", symbol=symbol)
            stock_contract = ib_client.stock_contract(symbol)
            contract_details = ib_client.req_contract_details(stock_contract, timeout=10.0)
            underlying_conid = contract_details.contract.conId
            
            LOGGER.info("option_chain.requesting", symbol=symbol, underlying_conid=underlying_conid)
            params = ib_client.req_opt_params(symbol, underlying_conid=underlying_conid)
            
            # 解析期权参数 (params是字典列表)
            expirations = []
            strikes = []
            exchange = "SMART"
            multiplier = "100"
            trading_class = None
            
            if params:
                # params是list，每个元素是dict
                for param in params:
                    if isinstance(param, dict):
                        if 'expirations' in param:
                            expirations.extend(param['expirations'])
                        if 'strikes' in param:
                            strikes.extend(param['strikes'])
                        if 'exchange' in param and not exchange:
                            exchange = param['exchange']
                        if 'multiplier' in param and param['multiplier']:
                            multiplier = param['multiplier']
                        if 'tradingClass' in param and param['tradingClass']:
                            trading_class = param['tradingClass']
                
                # 去重并排序
                expirations = sorted(set(expirations))
                strikes = sorted(set(strikes))
                
                chains[symbol] = {
                    "symbol": symbol,
                    "expirations": expirations[:10],  # 只保留前10个到期日
                    "strikes": strikes,
                    "exchange": exchange,
                    "underlying_conid": underlying_conid,
                    "multiplier": multiplier,
                    "trading_class": trading_class,
                }
                
                LOGGER.info(
                    "option_chain.success",
                    symbol=symbol,
                    expirations_count=len(expirations),
                    strikes_count=len(strikes),
                )
            else:
                LOGGER.warning("option_chain.empty", symbol=symbol)
                chains[symbol] = {"symbol": symbol, "error": "No option parameters returned"}
                
        except Exception as exc:
            LOGGER.error("option_chain.failed", symbol=symbol, error=str(exc))
            chains[symbol] = {"symbol": symbol, "error": str(exc)}
    
    return chains


def main() -> int:
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="获取盘前Top5股票的期权链")
    parser.add_argument(
        "--date",
        type=str,
        help="交易日期 (YYYY-MM-DD格式)，默认为今天",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        help="逗号分隔的股票代码，如果指定则忽略Top5",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="option_chains.json",
        help="输出JSON文件路径",
    )
    args = parser.parse_args()
    
    settings = get_settings()
    configure_logging(settings)
    
    # 准备数据库
    session_factory = get_session_factory(settings)
    
    # 获取股票列表
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
        LOGGER.info("manual_symbols", symbols=symbols)
    else:
        et_date = None
        if args.date:
            et_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        symbols = get_top5_symbols(session_factory, et_date)
        
        if not symbols:
            LOGGER.error("no_symbols_found")
            return 1
    
    # 连接IBKR
    ib_client = IBClient(settings)
    try:
        LOGGER.info("ibkr.connecting", client_id=settings.ib_client_id)
        ib_client.connect_and_wait(timeout=30.0)
        
        # 获取期权链
        chains = get_option_chains(ib_client, symbols)
        
        # 输出结果
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, "w") as f:
            json.dump(chains, f, indent=2, default=str)
        
        LOGGER.info("output.saved", path=str(output_path), symbols_count=len(chains))
        
        # 打印摘要
        print(f"\n📊 期权链数据已保存到: {output_path}\n")
        for symbol, data in chains.items():
            if "error" in data:
                print(f"❌ {symbol}: {data['error']}")
            else:
                print(f"✅ {symbol}:")
                print(f"   - 到期日数量: {len(data.get('expirations', []))}")
                print(f"   - 行权价数量: {len(data.get('strikes', []))}")
                if data.get('expirations'):
                    print(f"   - 最近到期: {data['expirations'][0]}")
        
        return 0
        
    except Exception as exc:
        LOGGER.exception("script.failed", error=str(exc))
        return 1
    finally:
        ib_client.disconnect_and_stop()


if __name__ == "__main__":
    sys.exit(main())

