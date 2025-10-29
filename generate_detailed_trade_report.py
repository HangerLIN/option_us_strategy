#!/usr/bin/env python
"""生成详细的交易配对和盈亏分析报告"""

from datetime import datetime
from decimal import Decimal
import psycopg2
from collections import defaultdict

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="option_us",
    user="option_user",
    password="option_pass"
)

def get_all_signals_with_prices():
    """获取所有信号及其价格"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            s.ts_end AT TIME ZONE 'America/New_York' as ts_et,
            s.symbol,
            s.signal_code,
            s.accepted,
            b.close as price,
            b.open as open_price,
            b.high as high_price,
            b.low as low_price,
            b.volume
        FROM bt_signals s
        LEFT JOIN bars1m_equity b ON s.symbol = b.symbol AND s.ts_end = b.ts_end
        WHERE s.run_id BETWEEN 463 AND 479
            AND s.accepted = true
        ORDER BY s.symbol, s.ts_end
    """)
    return cursor.fetchall()

def categorize_signal(signal_code):
    """判断信号类型"""
    buy_signals = ['SIG_OPEN_CHASE_BUY', 'SIG_REBOUND_BUY', 'SIG_PM_BOTTOM_A2', 
                   'SIG_PM_BOTTOM_A3', 'SIG_PM_BOTTOM_A4', 'SIG_AM_BOTTOM_A1',
                   'SIG_AM_CONFLUENCE_BUY_A2']
    return 'BUY' if signal_code in buy_signals else 'SELL'

def pair_signals(signals_by_symbol):
    """配对买入和卖出信号"""
    trades = []
    
    for symbol, signals in signals_by_symbol.items():
        position = None
        
        for sig in signals:
            ts, sym, code, accepted, price, open_p, high_p, low_p, vol = sig
            sig_type = categorize_signal(code)
            
            if sig_type == 'BUY':
                if position is None:  # 开新仓
                    position = {
                        'symbol': sym,
                        'buy_time': ts,
                        'buy_signal': code,
                        'buy_price': float(price) if price else None,
                        'buy_volume': int(vol) if vol else 0
                    }
            elif sig_type == 'SELL':
                if position is not None:  # 平仓
                    sell_price = float(price) if price else None
                    if position['buy_price'] and sell_price:
                        pnl = sell_price - position['buy_price']
                        pnl_pct = (pnl / position['buy_price']) * 100
                        
                        hold_minutes = int((ts - position['buy_time']).total_seconds() / 60)
                        
                        trades.append({
                            'symbol': sym,
                            'buy_time': position['buy_time'],
                            'sell_time': ts,
                            'buy_signal': position['buy_signal'],
                            'sell_signal': code,
                            'buy_price': position['buy_price'],
                            'sell_price': sell_price,
                            'pnl': pnl,
                            'pnl_pct': pnl_pct,
                            'hold_minutes': hold_minutes,
                            'buy_volume': position['buy_volume'],
                            'sell_volume': int(vol) if vol else 0
                        })
                    
                    position = None  # 清空持仓
    
    return trades

def generate_report():
    """生成完整报告"""
    
    # 获取所有信号
    all_signals = get_all_signals_with_prices()
    
    # 按股票分组
    signals_by_symbol = defaultdict(list)
    for sig in all_signals:
        signals_by_symbol[sig[1]].append(sig)
    
    # 配对交易
    trades = pair_signals(signals_by_symbol)
    
    # 生成报告
    report = f"""# 9月2-15日回测：详细交易配对与盈亏分析

## 📋 报告概览

- **批次ID**: `bt-20250902-20250915-f6ad60`
- **时间范围**: 2025-09-02 至 2025-09-15 (10个交易日)
- **分析基础**: 17只成功完成的股票
- **总信号数**: {len(all_signals)}条
- **配对交易数**: {len(trades)}笔
- **报告生成**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 💰 整体盈亏统计

"""
    
    if trades:
        # 计算统计数据
        total_pnl = sum(t['pnl'] for t in trades)
        winning_trades = [t for t in trades if t['pnl'] > 0]
        losing_trades = [t for t in trades if t['pnl'] < 0]
        breakeven_trades = [t for t in trades if t['pnl'] == 0]
        
        win_rate = (len(winning_trades) / len(trades)) * 100 if trades else 0
        avg_win = sum(t['pnl'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
        avg_loss = sum(t['pnl'] for t in losing_trades) / len(losing_trades) if losing_trades else 0
        avg_hold = sum(t['hold_minutes'] for t in trades) / len(trades)
        
        report += f"""### 核心指标

| 指标 | 数值 |
|------|------|
| **总交易笔数** | {len(trades)}笔 |
| **盈利交易** | {len(winning_trades)}笔 ({len(winning_trades)/len(trades)*100:.1f}%) |
| **亏损交易** | {len(losing_trades)}笔 ({len(losing_trades)/len(trades)*100:.1f}%) |
| **平盘交易** | {len(breakeven_trades)}笔 |
| **胜率** | {win_rate:.1f}% |
| **平均盈利** | ${avg_win:.2f} ({avg_win/trades[0]['buy_price']*100 if trades else 0:.2f}%) |
| **平均亏损** | ${avg_loss:.2f} ({avg_loss/trades[0]['buy_price']*100 if trades and avg_loss else 0:.2f}%) |
| **盈亏比** | {abs(avg_win/avg_loss) if avg_loss != 0 else 'N/A':.2f} |
| **平均持仓时间** | {int(avg_hold)}分钟 ({avg_hold/60:.1f}小时) |

---

## 📊 交易明细表

### 所有交易配对（按时间排序）

| # | 股票 | 买入时间 | 买入信号 | 买入价 | 卖出时间 | 卖出信号 | 卖出价 | 盈亏$ | 盈亏% | 持仓时间 |
|---|------|----------|----------|--------|----------|----------|--------|-------|-------|----------|
"""
        
        # 按时间排序
        trades_sorted = sorted(trades, key=lambda x: x['buy_time'])
        
        for i, trade in enumerate(trades_sorted, 1):
            buy_sig_name = {
                'SIG_OPEN_CHASE_BUY': 'E1-开盘追涨',
                'SIG_PM_BOTTOM_A3': 'A3-盘前底部'
            }.get(trade['buy_signal'], trade['buy_signal'])
            
            sell_sig_name = {
                'SIG_EXIT_BOX2MID': 'BOX2MID',
                'SIG_EXIT_UPPER_TAP_X2': 'UPPER_TAP'
            }.get(trade['sell_signal'], trade['sell_signal'])
            
            pnl_symbol = "✅" if trade['pnl'] > 0 else "❌" if trade['pnl'] < 0 else "⚪"
            
            buy_time_str = trade['buy_time'].strftime('%m-%d %H:%M')
            sell_time_str = trade['sell_time'].strftime('%m-%d %H:%M')
            hold_str = f"{trade['hold_minutes']}分" if trade['hold_minutes'] < 120 else f"{trade['hold_minutes']/60:.1f}小时"
            
            report += f"| {i} | **{trade['symbol']}** | {buy_time_str} | {buy_sig_name} | ${trade['buy_price']:.2f} | {sell_time_str} | {sell_sig_name} | ${trade['sell_price']:.2f} | {pnl_symbol} ${trade['pnl']:.2f} | {trade['pnl_pct']:+.2f}% | {hold_str} |\n"
        
        # 按股票分组统计
        report += "\n---\n\n## 📈 按股票分组统计\n\n"
        
        trades_by_symbol = defaultdict(list)
        for trade in trades:
            trades_by_symbol[trade['symbol']].append(trade)
        
        for symbol in sorted(trades_by_symbol.keys()):
            symbol_trades = trades_by_symbol[symbol]
            symbol_wins = [t for t in symbol_trades if t['pnl'] > 0]
            symbol_losses = [t for t in symbol_trades if t['pnl'] < 0]
            symbol_total_pnl = sum(t['pnl'] for t in symbol_trades)
            symbol_avg_pnl = symbol_total_pnl / len(symbol_trades)
            
            report += f"### {symbol} ({len(symbol_trades)}笔交易)\n\n"
            report += f"- **胜率**: {len(symbol_wins)}/{len(symbol_trades)} ({len(symbol_wins)/len(symbol_trades)*100:.1f}%)\n"
            report += f"- **总盈亏**: ${symbol_total_pnl:.2f}\n"
            report += f"- **平均盈亏**: ${symbol_avg_pnl:.2f}\n"
            report += f"- **最大单笔盈利**: ${max(t['pnl'] for t in symbol_trades):.2f}\n"
            report += f"- **最大单笔亏损**: ${min(t['pnl'] for t in symbol_trades):.2f}\n\n"
            
            report += "| 买入时间 | 买入信号 | 买入价 | 卖出时间 | 卖出信号 | 卖出价 | 盈亏 | 持仓 |\n"
            report += "|----------|----------|--------|----------|----------|--------|------|------|\n"
            
            for trade in symbol_trades:
                buy_sig_name = {
                    'SIG_OPEN_CHASE_BUY': 'E1',
                    'SIG_PM_BOTTOM_A3': 'A3'
                }.get(trade['buy_signal'], trade['buy_signal'][:10])
                
                sell_sig_name = {
                    'SIG_EXIT_BOX2MID': 'BOX2MID',
                    'SIG_EXIT_UPPER_TAP_X2': 'UPPER_TAP'
                }.get(trade['sell_signal'], trade['sell_signal'][:10])
                
                pnl_str = f"✅ ${trade['pnl']:.2f}" if trade['pnl'] > 0 else f"❌ ${trade['pnl']:.2f}"
                hold_str = f"{trade['hold_minutes']}分" if trade['hold_minutes'] < 120 else f"{trade['hold_minutes']/60:.1f}h"
                
                report += f"| {trade['buy_time'].strftime('%m-%d %H:%M')} | {buy_sig_name} | ${trade['buy_price']:.2f} | {trade['sell_time'].strftime('%m-%d %H:%M')} | {sell_sig_name} | ${trade['sell_price']:.2f} | {pnl_str} | {hold_str} |\n"
            
            report += "\n"
        
        # 按买入信号类型统计
        report += "---\n\n## 🎯 按买入信号类型统计\n\n"
        
        trades_by_buy_signal = defaultdict(list)
        for trade in trades:
            trades_by_buy_signal[trade['buy_signal']].append(trade)
        
        report += "| 买入信号 | 交易数 | 胜率 | 平均盈亏 | 总盈亏 |\n"
        report += "|---------|--------|------|----------|--------|\n"
        
        for signal_code in sorted(trades_by_buy_signal.keys()):
            sig_trades = trades_by_buy_signal[signal_code]
            sig_wins = [t for t in sig_trades if t['pnl'] > 0]
            sig_total_pnl = sum(t['pnl'] for t in sig_trades)
            sig_avg_pnl = sig_total_pnl / len(sig_trades)
            sig_win_rate = len(sig_wins) / len(sig_trades) * 100
            
            sig_name = {
                'SIG_OPEN_CHASE_BUY': 'E1-开盘追涨',
                'SIG_PM_BOTTOM_A3': 'A3-盘前底部'
            }.get(signal_code, signal_code)
            
            report += f"| **{sig_name}** | {len(sig_trades)}笔 | {sig_win_rate:.1f}% | ${sig_avg_pnl:.2f} | ${sig_total_pnl:.2f} |\n"
        
        # 按卖出信号类型统计
        report += "\n---\n\n## 🚪 按卖出信号类型统计\n\n"
        
        trades_by_sell_signal = defaultdict(list)
        for trade in trades:
            trades_by_sell_signal[trade['sell_signal']].append(trade)
        
        report += "| 卖出信号 | 交易数 | 平均持仓时间 | 平均盈亏 |\n"
        report += "|---------|--------|--------------|----------|\n"
        
        for signal_code in sorted(trades_by_sell_signal.keys()):
            sig_trades = trades_by_sell_signal[signal_code]
            sig_avg_hold = sum(t['hold_minutes'] for t in sig_trades) / len(sig_trades)
            sig_avg_pnl = sum(t['pnl'] for t in sig_trades) / len(sig_trades)
            
            sig_name = {
                'SIG_EXIT_BOX2MID': 'BOX2MID-回归中轨',
                'SIG_EXIT_UPPER_TAP_X2': 'UPPER_TAP-触及上轨'
            }.get(signal_code, signal_code)
            
            hold_str = f"{int(sig_avg_hold)}分钟" if sig_avg_hold < 120 else f"{sig_avg_hold/60:.1f}小时"
            
            report += f"| **{sig_name}** | {len(sig_trades)}笔 | {hold_str} | ${sig_avg_pnl:.2f} |\n"
    
    else:
        report += "\n⚠️ **没有找到配对的交易**\n\n"
    
    report += f"""
---

## 📌 关键发现

### 1. 信号配对情况

- **总信号数**: {len(all_signals)}条（买入+卖出）
- **成功配对**: {len(trades)}笔交易
- **未配对信号**: {len(all_signals) - len(trades)*2}条

### 2. 交易特征

"""
    
    if trades:
        # 最佳和最差交易
        best_trade = max(trades, key=lambda x: x['pnl'])
        worst_trade = min(trades, key=lambda x: x['pnl'])
        longest_hold = max(trades, key=lambda x: x['hold_minutes'])
        shortest_hold = min(trades, key=lambda x: x['hold_minutes'])
        
        report += f"""- **最佳交易**: {best_trade['symbol']} ({best_trade['buy_time'].strftime('%m-%d')}) 
  - 买入: ${best_trade['buy_price']:.2f} ({best_trade['buy_signal']})
  - 卖出: ${best_trade['sell_price']:.2f} ({best_trade['sell_signal']})
  - 盈亏: **${best_trade['pnl']:.2f}** ({best_trade['pnl_pct']:+.2f}%)

- **最差交易**: {worst_trade['symbol']} ({worst_trade['buy_time'].strftime('%m-%d')})
  - 买入: ${worst_trade['buy_price']:.2f} ({worst_trade['buy_signal']})
  - 卖出: ${worst_trade['sell_price']:.2f} ({worst_trade['sell_signal']})
  - 盈亏: **${worst_trade['pnl']:.2f}** ({worst_trade['pnl_pct']:+.2f}%)

- **最长持仓**: {longest_hold['symbol']} - {longest_hold['hold_minutes']}分钟 ({longest_hold['hold_minutes']/60:.1f}小时)
- **最短持仓**: {shortest_hold['symbol']} - {shortest_hold['hold_minutes']}分钟

### 3. 策略表现评估

- **整体胜率**: {win_rate:.1f}%
- **盈亏比**: {abs(avg_win/avg_loss) if avg_loss != 0 else 'N/A':.2f}:1
- **平均持仓**: {int(avg_hold)}分钟
- **交易频率**: {len(trades)/10:.1f}笔/天
"""
    
    report += """
---

**报告生成时间**: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """  
**数据来源**: run_id 463-479 (17只成功完成的股票)  
**分析方法**: 基于信号时间戳的买卖配对，使用K线收盘价计算盈亏
"""
    
    return report

if __name__ == "__main__":
    report = generate_report()
    filename = "回测分析报告_bt-20250902-20250915-完整版.md"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"✅ 详细交易报告已生成: {filename}")
    conn.close()



