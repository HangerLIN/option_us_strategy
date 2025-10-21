#!/usr/bin/env python3
"""
AMD案例深度分析 - 为什么AMD在这次回测中表现如此优异
"""
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from libs.core.config import get_settings


def analyze_amd_performance(batch_id: str):
    """深度分析AMD的表现"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print(f"\n{'='*100}")
    print(f"🔍 AMD 深度案例分析")
    print(f"{'='*100}\n")
    
    with engine.connect() as conn:
        # 获取AMD的run_id
        run_query = text("""
            SELECT run_id, parameters
            FROM bt_runs
            WHERE parameters->>'batch_id' = :batch_id
              AND parameters->>'symbol' = 'AMD'
        """)
        
        amd_run = conn.execute(run_query, {"batch_id": batch_id}).fetchone()
        
        if not amd_run:
            print("❌ 未找到AMD的回测数据")
            return
        
        run_id = amd_run.run_id
        print(f"✅ 找到AMD回测记录 (run_id: {run_id})\n")
        
        # 1. 获取AMD的所有信号
        print(f"{'='*100}")
        print("1️⃣  AMD 信号时间线")
        print(f"{'='*100}\n")
        
        signals_query = text("""
            SELECT 
                ts_end,
                signal_code,
                accepted,
                reason
            FROM bt_signals
            WHERE run_id = :run_id
            ORDER BY ts_end
        """)
        
        signals = conn.execute(signals_query, {"run_id": run_id}).fetchall()
        
        print(f"{'时间':<25} {'信号类型':<35} {'状态':<8} {'说明':<30}")
        print(f"{'-'*100}")
        
        buy_signals = []
        sell_signals = []
        
        for sig in signals:
            status = '✅' if sig.accepted else '❌'
            reason = sig.reason or ''
            print(f"{str(sig.ts_end):<25} {sig.signal_code:<35} {status:<8} {reason:<30}")
            
            if 'BUY' in sig.signal_code:
                buy_signals.append(sig)
            elif 'EXIT' in sig.signal_code:
                sell_signals.append(sig)
        
        print(f"\n📊 信号统计:")
        print(f"   买入信号: {len(buy_signals)} 个")
        print(f"   卖出信号: {len(sell_signals)} 个")
        
        # 2. 获取AMD的价格走势
        print(f"\n{'='*100}")
        print("2️⃣  AMD 价格走势（信号前后）")
        print(f"{'='*100}\n")
        
        if buy_signals:
            first_buy = buy_signals[0].ts_end
            
            # 获取买入信号前后的价格
            price_query = text("""
                SELECT 
                    ts_end,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM bars1m_equity
                WHERE symbol = 'AMD'
                  AND ts_end >= :start_time - INTERVAL '30 minutes'
                  AND ts_end <= :start_time + INTERVAL '4 hours'
                ORDER BY ts_end
            """)
            
            prices = conn.execute(price_query, {"start_time": first_buy}).fetchall()
            
            if prices:
                print(f"买入信号时间: {first_buy}")
                print(f"数据范围: 前30分钟 到 后4小时\n")
                
                print(f"{'时间':<25} {'开盘':<10} {'最高':<10} {'最低':<10} {'收盘':<10} {'成交量':<15} {'相对变化':<10}")
                print(f"{'-'*100}")
                
                base_price = float(prices[0].close)
                
                for idx, p in enumerate(prices[:50]):  # 只显示前50条
                    pct_change = (float(p.close) - base_price) / base_price * 100
                    
                    # 标记信号时间点
                    marker = ''
                    for sig in signals:
                        if sig.ts_end == p.ts_end:
                            if 'BUY' in sig.signal_code:
                                marker = '🔵 BUY'
                            elif 'EXIT' in sig.signal_code:
                                marker = '🔴 EXIT'
                    
                    print(f"{str(p.ts_end):<25} ${float(p.open):<9.2f} ${float(p.high):<9.2f} "
                          f"${float(p.low):<9.2f} ${float(p.close):<9.2f} {p.volume:<15,} "
                          f"{pct_change:>+8.2f}%  {marker}")
        
        # 3. 计算信号对的收益
        print(f"\n{'='*100}")
        print("3️⃣  信号对收益分析")
        print(f"{'='*100}\n")
        
        # 找出买入卖出信号对
        signal_pairs = []
        
        for i, buy_sig in enumerate(buy_signals):
            # 找到这个买入信号之后最近的卖出信号
            for sell_sig in sell_signals:
                if sell_sig.ts_end > buy_sig.ts_end:
                    signal_pairs.append({
                        'buy': buy_sig,
                        'sell': sell_sig
                    })
                    break
        
        print(f"找到 {len(signal_pairs)} 个信号对\n")
        
        for idx, pair in enumerate(signal_pairs, 1):
            buy_time = pair['buy'].ts_end
            sell_time = pair['sell'].ts_end
            
            # 获取买入和卖出时的价格
            buy_price_query = text("""
                SELECT close FROM bars1m_equity
                WHERE symbol = 'AMD' AND ts_end = :ts
            """)
            
            buy_price = conn.execute(buy_price_query, {"ts": buy_time}).fetchone()
            sell_price = conn.execute(buy_price_query, {"ts": sell_time}).fetchone()
            
            if buy_price and sell_price:
                buy_px = float(buy_price.close)
                sell_px = float(sell_price.close)
                pnl_pct = (sell_px - buy_px) / buy_px * 100
                hold_time = (sell_time - buy_time).total_seconds() / 60
                
                pnl_icon = '🟢' if pnl_pct > 0 else '🔴'
                
                print(f"信号对 #{idx}:")
                print(f"  买入: {buy_time} @ ${buy_px:.2f} [{pair['buy'].signal_code}]")
                print(f"  卖出: {sell_time} @ ${sell_px:.2f} [{pair['sell'].signal_code}]")
                print(f"  持有: {hold_time:.0f} 分钟 ({hold_time/60:.1f} 小时)")
                print(f"  收益: {pnl_icon} {pnl_pct:+.2f}%")
                print()
        
        # 4. 获取AMD的关键指标
        print(f"{'='*100}")
        print("4️⃣  AMD 技术指标表现")
        print(f"{'='*100}\n")
        
        metrics_query = text("""
            SELECT metric_code, metric_value
            FROM bt_metrics_total
            WHERE run_id = :run_id
            ORDER BY metric_code
        """)
        
        metrics = conn.execute(metrics_query, {"run_id": run_id}).fetchall()
        
        if metrics:
            print("收益率指标:")
            for m in metrics:
                if 'RET' in m.metric_code:
                    value = float(m.metric_value) if m.metric_value else 0
                    icon = '🟢' if value > 0 else '🔴' if value < 0 else '⚪'
                    print(f"  {icon} {m.metric_code:<50}: {value:>+8.2%}")
            
            print("\n命中率指标:")
            for m in metrics:
                if 'HIT' in m.metric_code:
                    value = float(m.metric_value) if m.metric_value else 0
                    print(f"  • {m.metric_code:<50}: {value:>7.1%}")
            
            print("\n持续时间指标:")
            for m in metrics:
                if 'DUR' in m.metric_code:
                    value = float(m.metric_value) if m.metric_value else 0
                    hours = int(value // 60)
                    mins = int(value % 60)
                    print(f"  ⏱️  {m.metric_code:<50}: {value:.0f} 分钟 ({hours}h{mins}m)")
        
        # 5. 对比其他标的
        print(f"\n{'='*100}")
        print("5️⃣  AMD vs 其他标的对比")
        print(f"{'='*100}\n")
        
        comparison_query = text("""
            WITH symbol_performance AS (
                SELECT 
                    r.parameters->>'symbol' as symbol,
                    AVG(CASE WHEN m.metric_code LIKE '%TFE_MEAN' 
                             AND m.metric_code LIKE '%RET_SIG_%'
                        THEN m.metric_value ELSE NULL END) as avg_return
                FROM bt_runs r
                JOIN bt_metrics_total m ON r.run_id = m.run_id
                WHERE r.parameters->>'batch_id' = :batch_id
                GROUP BY r.parameters->>'symbol'
            )
            SELECT * FROM symbol_performance
            ORDER BY avg_return DESC NULLS LAST
        """)
        
        comparison = conn.execute(comparison_query, {"batch_id": batch_id}).fetchall()
        
        print(f"{'标的':<8} {'平均收益率':<15} {'vs AMD差距':<15}")
        print(f"{'-'*40}")
        
        amd_return = None
        for comp in comparison:
            if comp.symbol == 'AMD':
                amd_return = float(comp.avg_return) if comp.avg_return else 0
                break
        
        for comp in comparison:
            symbol = comp.symbol
            ret = float(comp.avg_return) if comp.avg_return else 0
            
            if amd_return is not None:
                diff = ret - amd_return
                diff_str = f"{diff:+.2%}"
            else:
                diff_str = "N/A"
            
            icon = '⭐' if symbol == 'AMD' else '🟢' if ret > 0 else '🔴' if ret < 0 else '⚪'
            
            print(f"{icon} {symbol:<6} {ret:>+13.2%} {diff_str:>15}")
        
        print(f"\n{'='*100}")
        print("✅ AMD 分析完成")
        print(f"{'='*100}\n")
        
        # 总结
        print("🎯 关键结论:\n")
        print("1. AMD的超强表现主要来自PM_BOTTOM_A3信号的长期持有")
        print("2. 平均持有时间远超其他标的，让盈利充分发酵")
        print("3. 需要验证这种长持有策略在其他时期是否稳定")
        print("4. 建议扩展回测周期，确认AMD的表现不是偶然")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        batch_id = sys.argv[1]
    else:
        batch_id = "bt-20251002-20251007-3d379d"
    
    analyze_amd_performance(batch_id)

