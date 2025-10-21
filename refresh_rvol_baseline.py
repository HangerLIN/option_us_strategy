#!/usr/bin/env python3
"""
刷新 rvol_baseline_eq 表 - 计算历史分时均量
用于回测前准备基线数据
"""
from __future__ import annotations

import sys
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Dict, List

import pandas as pd
import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from libs.core import EASTERN, get_settings
from libs.infra import build_ibkr_client

LOGGER = structlog.get_logger(__name__)


def refresh_rvol_baseline(symbols: List[str], as_of_date: date, lookback_days: int = 40):
    """
    刷新指定股票的RVOL基线数据
    
    Args:
        symbols: 股票列表
        as_of_date: 截止日期
        lookback_days: 回溯天数（默认40天，确保有足够交易日）
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    SessionLocal = sessionmaker(bind=engine)
    
    # 连接IBKR
    ib_client = build_ibkr_client(settings)
    if not ib_client.is_connected():
        LOGGER.error("ibkr.connection_failed")
        return 1
    
    LOGGER.info(
        "rvol_baseline.start",
        symbols=symbols,
        as_of_date=str(as_of_date),
        lookback_days=lookback_days,
    )
    
    with SessionLocal() as session:
        for symbol in symbols:
            try:
                LOGGER.info("rvol_baseline.processing", symbol=symbol)
                
                # 计算时间范围
                end_et = datetime.combine(as_of_date, time(16, 0), EASTERN)
                start_et = end_et - timedelta(days=lookback_days)
                
                # 从IBKR获取历史数据
                try:
                    bars = ib_client.req_historical_1m(
                        symbol=symbol,
                        start=start_et,
                        end=end_et,
                        use_rth=True,
                    )
                except Exception as exc:
                    LOGGER.error(
                        "rvol_baseline.ibkr_error",
                        symbol=symbol,
                        error=str(exc),
                    )
                    continue
                
                if not bars:
                    LOGGER.warning("rvol_baseline.no_bars", symbol=symbol)
                    continue
                
                LOGGER.info("rvol_baseline.bars_fetched", symbol=symbol, count=len(bars))
                
                # 按日期和分钟索引组织数据
                per_day: Dict[date, Dict[int, Decimal]] = {}
                for raw in bars:
                    dt_et = raw.get("datetime")
                    if not dt_et:
                        continue
                    
                    if dt_et.tzinfo is None:
                        dt_et = EASTERN.localize(dt_et)
                    else:
                        dt_et = dt_et.astimezone(EASTERN)
                    
                    trade_date = dt_et.date()
                    
                    # 计算分钟索引 (09:30 = 1, 16:00 = 390)
                    market_open = datetime.combine(trade_date, time(9, 30), EASTERN)
                    if dt_et < market_open:
                        continue
                    
                    minutes_since_open = int((dt_et - market_open).total_seconds() / 60)
                    minute_index = minutes_since_open + 1
                    
                    if minute_index < 1 or minute_index > 390:
                        continue
                    
                    volume = raw.get("volume", 0)
                    if trade_date not in per_day:
                        per_day[trade_date] = {}
                    per_day[trade_date][minute_index] = Decimal(str(volume))
                
                if not per_day:
                    LOGGER.warning("rvol_baseline.no_valid_days", symbol=symbol)
                    continue
                
                LOGGER.info("rvol_baseline.days_processed", symbol=symbol, days=len(per_day))
                
                # 计算每个分钟索引的均量
                minute_volumes: Dict[int, List[Decimal]] = {}
                for day_data in per_day.values():
                    for minute_index, volume in day_data.items():
                        if minute_index not in minute_volumes:
                            minute_volumes[minute_index] = []
                        minute_volumes[minute_index].append(volume)
                
                # 计算平均值并写入数据库
                inserted = 0
                for minute_index in range(1, 391):
                    volumes = minute_volumes.get(minute_index, [])
                    if len(volumes) < 5:  # 至少需要5天数据
                        continue
                    
                    mean_vol = float(sum(volumes) / len(volumes))
                    
                    # Upsert到数据库
                    session.execute(
                        text("""
                            INSERT INTO rvol_baseline_eq (symbol, minute_index, mean_vol_20d, updated_at)
                            VALUES (:symbol, :minute_index, :mean_vol_20d, now())
                            ON CONFLICT (symbol, minute_index)
                            DO UPDATE SET
                                mean_vol_20d = EXCLUDED.mean_vol_20d,
                                updated_at = EXCLUDED.updated_at
                        """),
                        {
                            "symbol": symbol,
                            "minute_index": minute_index,
                            "mean_vol_20d": mean_vol,
                        }
                    )
                    inserted += 1
                
                session.commit()
                LOGGER.info(
                    "rvol_baseline.completed",
                    symbol=symbol,
                    rows_inserted=inserted,
                    days_used=len(per_day),
                )
                
            except Exception as exc:
                LOGGER.exception("rvol_baseline.symbol_failed", symbol=symbol)
                session.rollback()
                continue
    
    # 断开IBKR连接
    try:
        ib_client.disconnect_and_stop()
    except Exception:
        pass
    
    LOGGER.info("rvol_baseline.all_completed", symbols=len(symbols))
    return 0


def main():
    settings = get_settings()
    
    # 回测股票列表
    symbols = [
        "AAPL", "AMD", "AMZN", "ASML", "AVGO",
        "CRM", "GOOGL", "MSFT", "NVDA", "ORCL",
        "PLTR", "TSLA", "TSM"
    ]
    
    # 使用回测结束日期作为截止日期
    as_of_date = date(2025, 10, 7)
    
    return refresh_rvol_baseline(symbols=symbols, as_of_date=as_of_date, lookback_days=40)


if __name__ == "__main__":
    sys.exit(main())

