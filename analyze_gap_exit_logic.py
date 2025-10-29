#!/usr/bin/env python
"""分析隔夜缺口退出逻辑问题"""

import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="option_us",
    user="option_user",
    password="option_pass"
)

cursor = conn.cursor()

print("=" * 80)
print("隔夜缺口退出逻辑错误分析")
print("=" * 80)
print()

# 获取所有隔夜缺口退出的交易
cursor.execute("""
    WITH buy_signals AS (
        SELECT 
            s.symbol,
            s.ts_end,
            b.close as buy_price
        FROM bt_signals s
        LEFT JOIN bars1m_equity b ON s.symbol = b.symbol AND s.ts_end = b.ts_end
        WHERE s.run_id BETWEEN 481 AND 511
            AND s.signal_code IN (
                'SIG_OPEN_CHASE_BUY',
                'SIG_PM_BOTTOM_A2', 
                'SIG_PM_BOTTOM_A3',
                'SIG_PM_BOTTOM_A4'
            )
    ),
    gap_exits AS (
        SELECT 
            s.symbol,
            s.ts_end as exit_time,
            b.open as exit_open,
            b.close as exit_close
        FROM bt_signals s
        LEFT JOIN bars1m_equity b ON s.symbol = b.symbol AND s.ts_end = b.ts_end
        WHERE s.run_id BETWEEN 481 AND 511
            AND s.signal_code = 'SIG_OVERNIGHT_GAP_EXIT'
    )
    SELECT 
        buy.symbol,
        buy.ts_end AT TIME ZONE 'America/New_York' as buy_time,
        buy.buy_price,
        gap.exit_time AT TIME ZONE 'America/New_York' as exit_time,
        gap.exit_open,
        gap.exit_close
    FROM buy_signals buy
    INNER JOIN gap_exits gap ON buy.symbol = gap.symbol
    WHERE gap.exit_time > buy.ts_end
        AND DATE(gap.exit_time AT TIME ZONE 'America/New_York') > DATE(buy.ts_end AT TIME ZONE 'America/New_York')
    ORDER BY buy.ts_end
""")

trades = cursor.fetchall()

print(f"发现 {len(trades)} 笔隔夜缺口退出交易\n")
print("-" * 80)

problem_count = 0

for symbol, buy_time, buy_price, exit_time, exit_open, exit_close in trades:
    if buy_price is None or exit_open is None:
        continue
        
    buy_price = float(buy_price)
    exit_open = float(exit_open)
    exit_close = float(exit_close)
    
    gap_pct = (exit_open - buy_price) / buy_price * 100
    actual_pnl = exit_close - buy_price
    
    print(f"\n股票: {symbol}")
    print(f"  买入: {buy_time.strftime('%m-%d %H:%M')} @ ${buy_price:.2f}")
    print(f"  退出: {exit_time.strftime('%m-%d %H:%M')} @ ${exit_close:.2f}")
    print(f"  开盘: ${exit_open:.2f}")
    print(f"  相对买入价缺口: {gap_pct:+.2f}%")
    print(f"  实际盈亏: ${actual_pnl:+.2f}")
    
    if gap_pct > 0.5:
        print(f"  ⚠️  问题：高开{gap_pct:.2f}% > 0.5%，不应该退出！")
        problem_count += 1
    elif gap_pct > 0:
        print(f"  ⚡ 小幅高开{gap_pct:.2f}%，可能过早退出")
    else:
        print(f"  ✅ 低开{gap_pct:.2f}%，退出合理")

print("\n" + "=" * 80)
print(f"总结：")
print(f"  - 总交易数: {len(trades)}笔")
print(f"  - 明显错误（高开>0.5%仍退出）: {problem_count}笔")
print(f"  - 错误率: {problem_count/len(trades)*100:.1f}%")
print("=" * 80)

conn.close()
