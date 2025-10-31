#!/usr/bin/env python3
"""
从config/stock_universe.txt加载股票数据到stock_universe表
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 添加项目根目录到sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from libs.core import configure_logging, get_settings

LOGGER = structlog.get_logger(__name__)


def load_symbols_from_file(file_path: Path) -> list[str]:
    """从文件读取股票代码"""
    if not file_path.exists():
        raise FileNotFoundError(f"Stock universe file not found: {file_path}")
    
    symbols = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            # 跳过注释和空行
            if not line or line.startswith('#'):
                continue
            # 提取股票代码（如果行包含其他信息，只取第一个单词）
            symbol = line.split()[0].upper()
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    
    return symbols


def load_to_database(symbols: list[str], session_factory, clear_existing: bool = False) -> int:
    """将股票代码加载到数据库"""
    with session_factory() as session:
        try:
            if clear_existing:
                LOGGER.info("清空现有stock_universe数据")
                session.execute(text("DELETE FROM stock_universe"))
            
            # 使用INSERT ... ON CONFLICT DO UPDATE
            stmt = text("""
                INSERT INTO stock_universe (symbol, category, market_cap_tier, sort_order, active)
                VALUES (:symbol, :category, :tier, :sort_order, true)
                ON CONFLICT (symbol) DO UPDATE SET
                    active = true,
                    sort_order = EXCLUDED.sort_order,
                    updated_at = now()
            """)
            
            for idx, symbol in enumerate(symbols):
                session.execute(
                    stmt,
                    {
                        "symbol": symbol,
                        "category": "科技股",  # 默认分类
                        "tier": "大盘科技股",  # 默认市值分类
                        "sort_order": idx + 1,
                    }
                )
            
            session.commit()
            LOGGER.info(
                "stock_universe加载完成",
                count=len(symbols),
                symbols=symbols[:10] + (['...'] if len(symbols) > 10 else [])
            )
            return len(symbols)
            
        except Exception:
            session.rollback()
            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从config/stock_universe.txt加载股票数据到stock_universe表"
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=Path(__file__).parent.parent / "config" / "stock_universe.txt",
        help="股票池配置文件路径（默认: config/stock_universe.txt）",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="清空现有数据后加载",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings)
    
    LOGGER.info(
        "开始加载stock_universe",
        file=str(args.file),
        clear_existing=args.clear
    )
    
    # 读取股票代码
    symbols = load_symbols_from_file(args.file)
    if not symbols:
        LOGGER.error("未找到任何股票代码")
        return 1
    
    LOGGER.info(f"从文件读取到 {len(symbols)} 个股票代码")
    
    # 加载到数据库
    engine = create_engine(settings.database_url)
    session_factory = sessionmaker(bind=engine)
    
    count = load_to_database(symbols, session_factory, clear_existing=args.clear)
    
    LOGGER.info("✅ 加载完成", count=count)
    return 0


if __name__ == "__main__":
    sys.exit(main())









