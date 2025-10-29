#!/usr/bin/env python
"""生成9月2-15日回测分析报告（基于17只成功完成的股票）"""

from datetime import datetime
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="option_us",
    user="option_user",
    password="option_pass"
)

def generate_report():
    cursor = conn.cursor()
    
    # 获取买入信号
    cursor.execute("""
        SELECT 
            ts_end AT TIME ZONE 'America/New_York' as ts_et,
            symbol,
            signal_code,
            accepted
        FROM bt_signals
        WHERE run_id BETWEEN 463 AND 479
            AND (signal_code LIKE '%BUY%' OR signal_code LIKE '%BOTTOM%')
        ORDER BY ts_end
    """)
    buy_signals = cursor.fetchall()
    
    # 获取卖出信号统计
    cursor.execute("""
        SELECT signal_code, COUNT(*) as count
        FROM bt_signals
        WHERE run_id BETWEEN 463 AND 479
            AND (signal_code LIKE '%EXIT%' OR signal_code LIKE '%SELL%' OR signal_code LIKE '%CLEAR%')
        GROUP BY signal_code
        ORDER BY count DESC
    """)
    sell_signals = cursor.fetchall()
    
    # 按日期分组
    daily_signals = {}
    for ts, symbol, code, accepted in buy_signals:
        date_str = ts.strftime('%Y-%m-%d')
        if date_str not in daily_signals:
            daily_signals[date_str] = []
        daily_signals[date_str].append({
            'time': ts.strftime('%H:%M:%S'),
            'symbol': symbol,
            'signal_code': code,
            'accepted': accepted
        })
    
    # 生成报告
    report = f"""# 9月2-15日回测分析报告（部分完成）

## 📋 报告概览

- **批次ID**: `bt-20250902-20250915-f6ad60`
- **回测模式**: Equity Tracking (TFE指标)
- **时间范围**: 2025-09-02 至 2025-09-15 (10个交易日)
- **MFI/Stoch过滤**: ✅ 已启用
- **报告生成**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## ⚠️ 重要说明

本次回测**部分完成**：
- ✅ **成功完成**: 17只股票
- ❌ **失败**: 1只股票（TEAM - decimal.InvalidOperation错误）
- 📊 **数据基础**: 基于成功完成的17只股票生成报告

失败股票列表：
- **TEAM**: 指标NULL值导致的decimal异常（需进一步修复）

---

## 🎯 执行摘要

### 关键数据

| 指标 | 数值 |
|------|------|
| **分析股票数** | 17只（成功）+ 1只（失败）|
| **总买入信号** | {len(buy_signals)}次 |
| **总卖出信号** | {sum(s[1] for s in sell_signals)}次 |
| **日均买入机会** | {len(buy_signals)/10:.1f}次/天 |

### 买入信号类型分布

| 信号类型 | 数量 | 涉及股票 |
|---------|------|----------|
"""
    
    # 统计买入信号类型
    buy_type_stats = {}
    buy_type_symbols = {}
    for _, symbol, code, _ in buy_signals:
        buy_type_stats[code] = buy_type_stats.get(code, 0) + 1
        if code not in buy_type_symbols:
            buy_type_symbols[code] = set()
        buy_type_symbols[code].add(symbol)
    
    signal_names = {
        'SIG_OPEN_CHASE_BUY': '开盘追涨(E1)',
        'SIG_PM_BOTTOM_A3': '盘前底部A3',
        'SIG_PM_BOTTOM_A2': '盘前底部A2',
        'SIG_PM_BOTTOM_A4': '盘前底部A4',
    }
    
    for code in sorted(buy_type_stats.keys()):
        name = signal_names.get(code, code)
        count = buy_type_stats[code]
        symbols = len(buy_type_symbols[code])
        report += f"| **{name}** | {count} | {symbols}只 |\n"
    
    # 卖出信号分布
    report += "\n### 卖出信号类型分布\n\n| 信号类型 | 数量 | 说明 |\n|---------|------|------|\n"
    sell_names = {
        'SIG_EXIT_BOX2MID': '回归布林中轨',
        'SIG_EXIT_UPPER_TAP_X2': '触及上轨',
        'SIG_TIME_CLEAR_12_14': '时间清仓(12-14点)',
        'SIG_OVERNIGHT_GAP_EXIT': '隔夜跳空退出'
    }
    for code, count in sell_signals:
        name = sell_names.get(code, code)
        report += f"| **{name}** | {count} | - |\n"
    
    # 每日信号明细
    report += "\n---\n\n## 📅 每日买入信号明细\n\n"
    for date in sorted(daily_signals.keys()):
        sigs = daily_signals[date]
        report += f"### {date} ({len(sigs)}笔)\n\n"
        report += "| 时间 | 股票 | 信号类型 | 状态 |\n|------|------|----------|------|\n"
        for sig in sigs:
            status = "✅ 接受" if sig['accepted'] else "❌ 拒绝"
            name = signal_names.get(sig['signal_code'], sig['signal_code'])
            report += f"| {sig['time']} | {sig['symbol']} | {name} | {status} |\n"
        report += "\n"
    
    # 总结
    avg_signals = len(buy_signals) / 10
    report += f"""---

## 🔍 MFI/Stoch过滤效果评估

### 过滤设置

```
买入过滤条件:
- MFI > 50 且斜率 > 0 (动量向上)
- 或 Stoch金叉 (K>D) 且 K<80 (未超买)
```

### 实际影响

- 日均买入机会: **{avg_signals:.1f}次** (目标2-5次)
- 10天共{len(buy_signals)}次买入信号，涉及{len(set(s[1] for s in buy_signals))}只股票
"""
    
    if avg_signals < 2:
        report += "- **结论: 过滤偏严格**，可考虑放宽条件\n"
    elif avg_signals > 5:
        report += "- **结论: 信号较多**，过滤效果适中\n"
    else:
        report += "- **结论: 信号数量符合预期**\n"
    
    report += """
---

## 🎯 策略表现总结

### 优势

1. **大部分股票运行稳定**:
   - 17只股票成功完成回测
   - MFI/Stoch过滤正常工作
   - 信号生成逻辑基本正常

2. **信号类型分布合理**:
   - E1开盘追涨信号出现
   - A3盘前底部信号是主力
   - 卖出以BOX2MID为主（正常回归）

### 待改进

1. **TEAM股票失败问题**:
   - 依然存在decimal.InvalidOperation错误
   - 需要进一步检查是否有其他信号检测函数也有类似问题
   - 建议全面审查所有指标比较逻辑

2. **信号数量评估**:
   - 日均{avg_signals:.1f}次买入机会
   - 如果目标是2-5次/天，当前{"偏少" if avg_signals < 2 else "偏多" if avg_signals > 5 else "合适"}

---

## 📌 下一步行动

### 立即修复

1. **[ ] 修复TEAM的decimal错误**:
   - 全面检查所有信号检测函数
   - 确保所有指标比较都有NULL检查
   - 重点检查E2、A2、A4等尚未触发的信号

2. **[ ] 重新运行完整回测**:
   - 修复后重新运行TEAM
   - 完成剩余11只股票的回测
   - 生成完整的29只股票报告

### 长期优化

1. **参数调优**: 根据完整数据评估MFI/Stoch过滤参数
2. **信号多样性**: 观察为何E2、A2、A4等信号未出现
3. **性能分析**: 基于TFE指标评估策略收益

---

**报告生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**数据范围**: run_id 463-479 (17只成功股票)  
**失败股票**: TEAM (run_id 480)
"""
    
    return report

if __name__ == "__main__":
    report = generate_report()
    filename = "回测分析报告_bt-20250902-20250915-部分完成.md"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"✅ 报告已生成: {filename}")
    conn.close()
