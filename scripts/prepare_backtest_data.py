#!/usr/bin/env python3
"""
一键准备回测所需的全部数据

用途: 自动拉取回测所需的所有数据（ref_market_cap, bars1m_equity, indicators等）
用法: python scripts/prepare_backtest_data.py --start 2025-10-02 --end 2025-10-06 --universe ref_market_cap:GLOBAL
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="一键准备回测所需的全部数据（ref_market_cap, bars1m_equity, indicators等）"
    )
    parser.add_argument(
        "--start",
        required=True,
        type=_parse_date,
        help="开始日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=_parse_date,
        help="结束日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--universe",
        default="ref_market_cap:GLOBAL",
        help="股票宇宙 (默认 ref_market_cap:GLOBAL)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="每批处理的股票数（默认10）",
    )
    parser.add_argument(
        "--skip-market-cap",
        action="store_true",
        help="跳过 ref_market_cap 加载（如果已有数据）",
    )
    parser.add_argument(
        "--skip-calendar",
        action="store_true",
        help="跳过交易日历加载（如果已有数据）",
    )
    parser.add_argument(
        "--skip-premarket",
        action="store_true",
        help="跳过盘前数据回填（如果已有数据）",
    )
    return parser.parse_args()


def run_command(cmd: list[str], description: str) -> int:
    """运行命令并显示进度"""
    print(f"\n{'='*70}")
    print(f"  {description}")
    print(f"{'='*70}")
    print(f"执行: {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd)
    
    if result.returncode != 0:
        print(f"\n❌ 失败: {description}")
        return result.returncode
    
    print(f"\n✅ 完成: {description}")
    return 0


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).parent.parent
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  📊 回测数据准备工具                                          ║
║                                                              ║
║  时间范围: {args.start} 至 {args.end}                       ║
║  股票宇宙: {args.universe}                                   ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    # 步骤1: 加载 ref_market_cap
    if not args.skip_market_cap:
        csv_path = project_root / "data" / "ref_market_cap_snapshot.csv"
        if csv_path.exists():
            ret = run_command(
                ["python", "-m", "scripts.load_marketcap_snapshot", str(csv_path), "--source", "compliance_snapshot"],
                "1️⃣ 加载 ref_market_cap（市值数据）"
            )
            if ret != 0:
                return ret
        else:
            print(f"⚠️ 警告: {csv_path} 不存在，跳过 ref_market_cap 加载")
    else:
        print("⏭️  跳过 ref_market_cap 加载")
    
    # 步骤2: 加载交易日历
    if not args.skip_calendar:
        csv_path = project_root / "data" / "dim_trading_calendar.csv"
        if csv_path.exists():
            ret = run_command(
                ["python", "-m", "scripts.seed_calendar"],
                "2️⃣ 加载 dim_trading_calendar（交易日历）"
            )
            if ret != 0:
                return ret
        else:
            print(f"⚠️ 警告: {csv_path} 不存在，跳过日历加载")
    else:
        print("⏭️  跳过交易日历加载")
    
    # 步骤3: 拉取盘前数据（09:25-09:30）+ 指标
    if not args.skip_premarket:
        # 需要前一天的数据来计算 prev_close_rth
        from datetime import timedelta
        start_with_prev = args.start - timedelta(days=3)  # 往前推3天以确保包含前一交易日
        
        ret = run_command(
            [
                "python", "-m", "scripts.backfill_premarket_window",
                "--start", start_with_prev.strftime("%Y-%m-%d"),
                "--end", args.end.strftime("%Y-%m-%d"),
                "--universe", args.universe,
                "--window-start", "09:24",
                "--window-end", "09:30",
                "--batch-size", str(args.batch_size),
            ],
            "3️⃣ 回填盘前数据（09:25-09:30）+ 指标计算"
        )
        if ret != 0:
            print("⚠️ 警告: 盘前数据回填失败，但继续执行...")
    else:
        print("⏭️  跳过盘前数据回填")
    
    # 步骤4: 拉取全天RTH数据（09:30-16:00）+ 指标
    ret = run_command(
        [
            "python", "-m", "scripts.ingest_equity_1m_ibkr",
            "--start", args.start.strftime("%Y-%m-%d"),
            "--end", args.end.strftime("%Y-%m-%d"),
            "--universe", args.universe,
            "--batch-size", str(args.batch_size),
            "--rth-only", "1",
        ],
        "4️⃣ 拉取全天RTH数据（09:30-16:00）+ 指标计算"
    )
    if ret != 0:
        return ret
    
    print(f"""
{'='*70}
  ✅ 数据准备完成！
{'='*70}

已准备的数据:
  ✓ ref_market_cap: 股票池定义
  ✓ dim_trading_calendar: 交易日历
  ✓ bars1m_equity: 分钟K线（盘前09:25-09:30 + RTH 09:30-16:00）
  ✓ indicators_eq_1m: 技术指标（自动计算）
  ✓ v_daily_ohlcv_enriched: 每日OHLCV视图（自动生成）

下一步: 运行回测
  python -m apps.backtest.main \\
    --start {args.start}T09:30:00-04:00 \\
    --end {args.end.replace(day=args.end.day+1) if args.end.day < 28 else args.end}T16:00:00-04:00 \\
    --track equity \\
    --signal-mode recompute \\
    --risk-mode inproc \\
    --universe {args.universe}

{'='*70}
    """)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())


