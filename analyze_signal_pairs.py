#!/usr/bin/env python3
"""
分析9月15-30日回测的信号配对和TFE收益
"""
from sqlalchemy import create_engine, text
from libs.core import get_settings
import pandas as pd

settings = get_settings()
engine = create_engine(settings.database_url)

# 查询所有买入信号
buy_signals_query = text("""
SELECT 
    r.run_id,
    r.parameters->>'symbol' as symbol,
    s.ts_end,
    s.signal_code,
    s.reason
FROM bt_signals s
JOIN bt_runs r ON s.run_id = r.run_id
WHERE r.parameters->>'batch_id' = 'bt-20250915-20250930-ffc67e'
    AND s.signal_code IN ('SIG_OPEN_CHASE_BUY', 'SIG_PM_BOTTOM_A3')
    AND s.accepted = true
ORDER BY r.parameters->>'symbol', s.ts_end
""")

# 查询所有卖出信号
sell_signals_query = text("""
SELECT 
    r.run_id,
    r.parameters->>'symbol' as symbol,
    s.ts_end,
    s.signal_code,
    s.reason
FROM bt_signals s
JOIN bt_runs r ON s.run_id = r.run_id
WHERE r.parameters->>'batch_id' = 'bt-20250915-20250930-ffc67e'
    AND s.signal_code IN ('SIG_EXIT_BOX2MID', 'SIG_EXIT_UPPER_TAP_X2', 'SIG_AM_SELL_C1')
    AND s.accepted = true
ORDER BY r.parameters->>'symbol', s.ts_end
""")

# 查询TFE指标
tfe_metrics_query = text("""
SELECT 
    r.parameters->>'symbol' as symbol,
    m.metric_code,
    m.metric_value
FROM bt_metrics_total m
JOIN bt_runs r ON m.run_id = r.run_id
WHERE r.parameters->>'batch_id' = 'bt-20250915-20250930-ffc67e'
    AND m.metric_code LIKE 'RET_SIG_%_TFE_MEAN'
ORDER BY r.parameters->>'symbol'
""")

with engine.connect() as conn:
    buy_signals_df = pd.read_sql(buy_signals_query, conn)
    sell_signals_df = pd.read_sql(sell_signals_query, conn)
    tfe_metrics_df = pd.read_sql(tfe_metrics_query, conn)

# 转换时间为东部时间
buy_signals_df['ts_end'] = pd.to_datetime(buy_signals_df['ts_end']).dt.tz_convert('America/New_York')
sell_signals_df['ts_end'] = pd.to_datetime(sell_signals_df['ts_end']).dt.tz_convert('America/New_York')

print("=" * 100)
print("9月15-30日回测：信号配对与TFE收益分析")
print("=" * 100)
print()

# 按股票分组分析
for symbol in sorted(buy_signals_df['symbol'].unique()):
    symbol_buys = buy_signals_df[buy_signals_df['symbol'] == symbol].copy()
    symbol_sells = sell_signals_df[sell_signals_df['symbol'] == symbol].copy()
    symbol_tfe = tfe_metrics_df[tfe_metrics_df['symbol'] == symbol].copy()
    
    if len(symbol_buys) == 0:
        continue
    
    print(f"\n{'='*100}")
    print(f"股票: {symbol}")
    print(f"{'='*100}")
    
    # 买入信号
    print(f"\n买入信号 ({len(symbol_buys)}笔):")
    print("-" * 100)
    for idx, row in symbol_buys.iterrows():
        print(f"  {row['ts_end'].strftime('%m-%d %H:%M')} | {row['signal_code']}")
    
    # 卖出信号（只显示前10个）
    print(f"\n卖出信号 (共{len(symbol_sells)}个，显示前10个):")
    print("-" * 100)
    for idx, row in symbol_sells.head(10).iterrows():
        print(f"  {row['ts_end'].strftime('%m-%d %H:%M')} | {row['signal_code']}")
    if len(symbol_sells) > 10:
        print(f"  ... 还有 {len(symbol_sells) - 10} 个卖出信号")
    
    # TFE指标
    if len(symbol_tfe) > 0:
        print(f"\nTFE收益指标:")
        print("-" * 100)
        for idx, row in symbol_tfe.iterrows():
            signal_type = row['metric_code'].replace('RET_SIG_', '').replace('_TFE_MEAN', '')
            tfe_value = float(row['metric_value']) * 100
            status = "✅ 正收益" if tfe_value > 0 else "❌ 负收益" if tfe_value < 0 else "⚪ 持平"
            print(f"  {signal_type}: {tfe_value:.2f}% {status}")
    else:
        print(f"\n⚠️ 无TFE数据（可能信号未实际执行或被过滤）")

# 总结
print(f"\n\n{'='*100}")
print("总体统计")
print(f"{'='*100}")

buy_signal_counts = buy_signals_df.groupby('signal_code').size()
print(f"\n买入信号分布:")
for signal, count in buy_signal_counts.items():
    print(f"  {signal}: {count}次")

print(f"\n有TFE数据的股票: {len(tfe_metrics_df['symbol'].unique())}只")
print(f"触发买入信号的股票: {len(buy_signals_df['symbol'].unique())}只")

# TFE收益汇总
print(f"\nTFE收益汇总（按信号类型）:")
print("-" * 100)
for signal_type in ['SIG_OPEN_CHASE_BUY', 'SIG_PM_BOTTOM_A3']:
    tfe_rows = tfe_metrics_df[tfe_metrics_df['metric_code'].str.contains(signal_type)]
    if len(tfe_rows) > 0:
        avg_tfe = tfe_rows['metric_value'].mean() * 100
        max_tfe = tfe_rows['metric_value'].max() * 100
        min_tfe = tfe_rows['metric_value'].min() * 100
        count = len(tfe_rows)
        status = "✅" if avg_tfe > 0 else "❌"
        print(f"  {signal_type}:")
        print(f"    平均TFE: {avg_tfe:.2f}% {status}")
        print(f"    最佳: {max_tfe:.2f}% | 最差: {min_tfe:.2f}%")
        print(f"    样本数: {count}只股票")

print("\n" + "=" * 100)
print("注：TFE (Transaction Fee Equivalent) 是假设交易的收益，不包含实际交易成本和滑点")
print("=" * 100)







