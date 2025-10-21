#!/usr/bin/env python3
"""测试股票池配置"""

from pathlib import Path

def test_universe_file():
    """测试股票池文件"""
    universe_file = Path("config/stock_universe.txt")
    
    print("=" * 70)
    print("股票池配置验证")
    print("=" * 70)
    
    # 检查文件是否存在
    if not universe_file.exists():
        print(f"❌ 错误：股票池文件不存在 - {universe_file}")
        return False
    
    print(f"✓ 股票池文件: {universe_file.absolute()}")
    
    # 读取股票池
    symbols = []
    lines = universe_file.read_text(encoding="utf-8").splitlines()
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.append(line.upper())
    
    print(f"✓ 总标的数量: {len(symbols)}")
    print()
    
    # 按市值分类统计
    categories = {
        "超大盘股": ["NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "GOOG"],
        "大盘科技股": ["AVGO", "TSM", "TSLA", "ORCL", "NFLX"],
        "中大盘成长股": [s for s in symbols if s not in ["NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "GOOG", "AVGO", "TSM", "TSLA", "ORCL", "NFLX"]][:30],
        "中盘成长股": [s for s in symbols if s not in ["NVDA", "MSFT", "AAPL", "GOOGL", "AMZN", "META", "GOOG", "AVGO", "TSM", "TSLA", "ORCL", "NFLX"]][30:],
    }
    
    print("【股票池分类】")
    for category, stocks in categories.items():
        if stocks:
            print(f"\n{category} ({len(stocks)}个):")
            # 每行显示10个
            for i in range(0, len(stocks), 10):
                batch = stocks[i:i+10]
                print(f"  {', '.join(batch)}")
    
    print("\n" + "=" * 70)
    print("【使用方式】")
    print("=" * 70)
    
    print("\n1️⃣ 默认模式（从配置文件读取股票池）：")
    print("   python -m apps.ingest.ingest_option_chain \\")
    print("     --date-from 2025-10-01 \\")
    print("     --date-to 2025-10-07")
    
    print("\n2️⃣ 指定其他配置文件：")
    print("   python -m apps.ingest.ingest_option_chain \\")
    print("     --date-from 2025-10-01 \\")
    print("     --date-to 2025-10-07 \\")
    print("     --symbols-from file \\")
    print("     --universe-file my_custom_universe.txt")
    
    print("\n3️⃣ 手动指定少量标的（测试用）：")
    print("   python -m apps.ingest.ingest_option_chain \\")
    print("     --date-from 2025-10-01 \\")
    print("     --date-to 2025-10-01 \\")
    print("     --symbols-from list \\")
    print("     --symbol-list AAPL,NVDA,MSFT")
    
    print("\n4️⃣ 从数据库ref_market_cap表读取：")
    print("   python -m apps.ingest.ingest_option_chain \\")
    print("     --date-from 2025-10-01 \\")
    print("     --date-to 2025-10-07 \\")
    print("     --symbols-from ref_universe")
    
    print("\n" + "=" * 70)
    print("【配置说明】")
    print("=" * 70)
    print("• DTE范围: 2-7天（短期期权策略）")
    print("• OTM档位: 第2-7档虚值期权")
    print("• 每个标的最多80个合约（可通过--max-contracts-per-underlying调整）")
    print("• 默认使用MIDPOINT价格")
    print("• 默认只抓取RTH（常规交易时段）数据")
    
    print("\n✅ 配置验证完成！")
    return True

if __name__ == "__main__":
    test_universe_file()

