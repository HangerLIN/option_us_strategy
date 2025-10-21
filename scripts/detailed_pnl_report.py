#!/usr/bin/env python3
"""
详细盈亏报告 - 列出所有标的的每一笔信号对的盈亏
"""
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from libs.core.config import get_settings


def generate_detailed_pnl_report(batch_id: str):
    """生成详细的盈亏报告"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print(f"\n{'='*120}")
    print(f"💰 详细盈亏报告")
    print(f"批次: {batch_id}")
    print(f"{'='*120}\n")
    
    with engine.connect() as conn:
        # 获取所有run
        run_query = text("""
            SELECT run_id, parameters
            FROM bt_runs
            WHERE parameters->>'batch_id' = :batch_id
            ORDER BY run_id
        """)
        
        runs = conn.execute(run_query, {"batch_id": batch_id}).fetchall()
        
        if not runs:
            print("❌ 未找到数据")
            return
        
        all_symbol_data = []
        
        for run in runs:
            run_id = run.run_id
            symbol = run.parameters.get('symbol')
            
            # 获取该标的的所有信号
            signals_query = text("""
                SELECT ts_end, signal_code, accepted
                FROM bt_signals
                WHERE run_id = :run_id
                  AND accepted = true
                ORDER BY ts_end
            """)
            
            signals = conn.execute(signals_query, {"run_id": run_id}).fetchall()
            
            # 分类信号
            buy_signals = []
            sell_signals = []
            
            for sig in signals:
                if 'BUY' in sig.signal_code or 'BOTTOM' in sig.signal_code:
                    buy_signals.append(sig)
                elif 'EXIT' in sig.signal_code or 'CLEAR' in sig.signal_code:
                    sell_signals.append(sig)
            
            # 配对买入卖出信号
            signal_pairs = []
            used_sells = set()
            
            for buy_sig in buy_signals:
                # 找最近的未使用的卖出信号
                for i, sell_sig in enumerate(sell_signals):
                    if sell_sig.ts_end > buy_sig.ts_end and i not in used_sells:
                        # 获取价格
                        buy_price_query = text("""
                            SELECT close FROM bars1m_equity
                            WHERE symbol = :symbol AND ts_end = :ts
                        """)
                        
                        buy_price_result = conn.execute(buy_price_query, 
                            {"symbol": symbol, "ts": buy_sig.ts_end}).fetchone()
                        sell_price_result = conn.execute(buy_price_query,
                            {"symbol": symbol, "ts": sell_sig.ts_end}).fetchone()
                        
                        if buy_price_result and sell_price_result:
                            buy_px = float(buy_price_result.close)
                            sell_px = float(sell_price_result.close)
                            pnl_pct = (sell_px - buy_px) / buy_px * 100
                            hold_minutes = (sell_sig.ts_end - buy_sig.ts_end).total_seconds() / 60
                            
                            signal_pairs.append({
                                'buy_time': buy_sig.ts_end,
                                'sell_time': sell_sig.ts_end,
                                'buy_signal': buy_sig.signal_code,
                                'sell_signal': sell_sig.signal_code,
                                'buy_price': buy_px,
                                'sell_price': sell_px,
                                'pnl_pct': pnl_pct,
                                'hold_minutes': hold_minutes
                            })
                            
                            used_sells.add(i)
                            break
            
            # 计算总收益
            total_pnl = sum(p['pnl_pct'] for p in signal_pairs)
            avg_pnl = total_pnl / len(signal_pairs) if signal_pairs else 0
            win_count = sum(1 for p in signal_pairs if p['pnl_pct'] > 0)
            win_rate = win_count / len(signal_pairs) if signal_pairs else 0
            
            all_symbol_data.append({
                'symbol': symbol,
                'signal_pairs': signal_pairs,
                'total_pnl': total_pnl,
                'avg_pnl': avg_pnl,
                'win_count': win_count,
                'total_count': len(signal_pairs),
                'win_rate': win_rate
            })
        
        # 按总收益排序
        all_symbol_data.sort(key=lambda x: x['total_pnl'], reverse=True)
        
        # 打印详细报告
        print(f"{'='*120}")
        print("📊 总览表")
        print(f"{'='*120}\n")
        
        print(f"{'排名':<6} {'标的':<8} {'信号对数':<10} {'总收益':<12} {'平均收益':<12} "
              f"{'胜率':<10} {'盈利笔数':<10}")
        print(f"{'-'*90}")
        
        total_pairs = 0
        total_win = 0
        weighted_pnl = 0
        
        for idx, data in enumerate(all_symbol_data, 1):
            icon = '🟢' if data['total_pnl'] > 0 else '🔴' if data['total_pnl'] < 0 else '⚪'
            rank_icon = '🥇' if idx == 1 else '🥈' if idx == 2 else '🥉' if idx == 3 else '  '
            
            print(f"{rank_icon} {idx:<3} {data['symbol']:<8} "
                  f"{data['total_count']:<10} "
                  f"{icon} {data['total_pnl']:>9.2f}% "
                  f"{icon} {data['avg_pnl']:>9.2f}% "
                  f"{data['win_rate']:>9.1%} "
                  f"{data['win_count']}/{data['total_count']}")
            
            total_pairs += data['total_count']
            total_win += data['win_count']
            weighted_pnl += data['total_pnl']
        
        overall_avg = weighted_pnl / len(all_symbol_data) if all_symbol_data else 0
        overall_win_rate = total_win / total_pairs if total_pairs > 0 else 0
        
        print(f"{'-'*90}")
        print(f"{'总计':<6} {len(all_symbol_data)}标的   "
              f"{total_pairs:<10} "
              f"{'  '} {weighted_pnl:>9.2f}% "
              f"{'  '} {overall_avg:>9.2f}% "
              f"{overall_win_rate:>9.1%} "
              f"{total_win}/{total_pairs}")
        
        # 详细信号对
        print(f"\n{'='*120}")
        print("📝 详细交易记录")
        print(f"{'='*120}\n")
        
        for data in all_symbol_data:
            if not data['signal_pairs']:
                continue
            
            print(f"\n{'▼'*60}")
            print(f"📌 {data['symbol']} - 总收益: {data['total_pnl']:+.2f}% "
                  f"(平均: {data['avg_pnl']:+.2f}%, 胜率: {data['win_rate']:.1%})")
            print(f"{'▼'*60}\n")
            
            for idx, pair in enumerate(data['signal_pairs'], 1):
                pnl_icon = '🟢' if pair['pnl_pct'] > 0 else '🔴' if pair['pnl_pct'] < 0 else '⚪'
                hours = int(pair['hold_minutes'] // 60)
                mins = int(pair['hold_minutes'] % 60)
                
                print(f"  #{idx}. {pnl_icon} 收益: {pair['pnl_pct']:+7.2f}%")
                print(f"      买入: {pair['buy_time']} @ ${pair['buy_price']:.2f}  [{pair['buy_signal']}]")
                print(f"      卖出: {pair['sell_time']} @ ${pair['sell_price']:.2f}  [{pair['sell_signal']}]")
                print(f"      持有: {pair['hold_minutes']:.0f} 分钟 ({hours}小时{mins}分钟)")
                print()
        
        # 统计分析
        print(f"{'='*120}")
        print("📈 统计分析")
        print(f"{'='*120}\n")
        
        # 按信号类型统计
        buy_signal_stats = defaultdict(lambda: {'count': 0, 'total_pnl': 0, 'win': 0})
        
        for data in all_symbol_data:
            for pair in data['signal_pairs']:
                sig_type = pair['buy_signal']
                buy_signal_stats[sig_type]['count'] += 1
                buy_signal_stats[sig_type]['total_pnl'] += pair['pnl_pct']
                if pair['pnl_pct'] > 0:
                    buy_signal_stats[sig_type]['win'] += 1
        
        print("按买入信号类型统计:\n")
        print(f"{'信号类型':<35} {'数量':<8} {'平均收益':<15} {'胜率':<10}")
        print(f"{'-'*70}")
        
        for sig_type, stats in sorted(buy_signal_stats.items()):
            avg_pnl = stats['total_pnl'] / stats['count']
            win_rate = stats['win'] / stats['count']
            icon = '🟢' if avg_pnl > 0 else '🔴'
            
            print(f"{sig_type:<35} {stats['count']:<8} {icon} {avg_pnl:>12.2f}% {win_rate:>9.1%}")
        
        # 最佳和最差交易
        all_trades = []
        for data in all_symbol_data:
            for pair in data['signal_pairs']:
                all_trades.append({
                    'symbol': data['symbol'],
                    **pair
                })
        
        all_trades.sort(key=lambda x: x['pnl_pct'], reverse=True)
        
        print(f"\n{'='*120}")
        print("🏆 Top 5 最佳交易")
        print(f"{'='*120}\n")
        
        for idx, trade in enumerate(all_trades[:5], 1):
            hours = int(trade['hold_minutes'] // 60)
            mins = int(trade['hold_minutes'] % 60)
            
            print(f"{idx}. {trade['symbol']}: {trade['pnl_pct']:+.2f}% "
                  f"(${trade['buy_price']:.2f} → ${trade['sell_price']:.2f}, "
                  f"持有{hours}h{mins}m)")
            print(f"   买入: {trade['buy_time']} [{trade['buy_signal']}]")
            print(f"   卖出: {trade['sell_time']} [{trade['sell_signal']}]")
            print()
        
        print(f"{'='*120}")
        print("🥊 Top 5 最差交易")
        print(f"{'='*120}\n")
        
        for idx, trade in enumerate(all_trades[-5:][::-1], 1):
            hours = int(trade['hold_minutes'] // 60)
            mins = int(trade['hold_minutes'] % 60)
            
            print(f"{idx}. {trade['symbol']}: {trade['pnl_pct']:+.2f}% "
                  f"(${trade['buy_price']:.2f} → ${trade['sell_price']:.2f}, "
                  f"持有{hours}h{mins}m)")
            print(f"   买入: {trade['buy_time']} [{trade['buy_signal']}]")
            print(f"   卖出: {trade['sell_time']} [{trade['sell_signal']}]")
            print()
        
        print(f"{'='*120}")
        print("✅ 报告生成完成")
        print(f"{'='*120}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        batch_id = sys.argv[1]
    else:
        batch_id = "bt-20251002-20251007-3d379d"
    
    generate_detailed_pnl_report(batch_id)

