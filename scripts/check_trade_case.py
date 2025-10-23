#!/usr/bin/env python3
"""
检查具体交易案例的数据合理性
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from libs.core.config import get_settings


def check_trade_case(symbol: str, trade_date: str, entry_time: str):
    """
    检查具体交易案例
    
    Args:
        symbol: 股票代码，如 "TSLA"
        trade_date: 交易日期，如 "2025-10-03"
        entry_time: 买入时间（ET），如 "09:31"
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print(f"\n{'='*100}")
    print(f"🔍 交易案例数据检查")
    print(f"{'='*100}\n")
    print(f"📊 标的: {symbol}")
    print(f"📅 日期: {trade_date}")
    print(f"⏰ 买入时间: {entry_time} ET\n")
    
    # 将日期和时间转换为 UTC（假设 ET 是 EDT，即 UTC-4）
    entry_dt = datetime.strptime(f"{trade_date} {entry_time}", "%Y-%m-%d %H:%M")
    # 美东时间转 UTC（夏令时 +4 小时）
    entry_utc = entry_dt + timedelta(hours=4)
    
    # 查询前后 10 分钟的数据
    start_utc = entry_utc - timedelta(minutes=5)
    end_utc = entry_utc + timedelta(minutes=5)
    
    with engine.connect() as conn:
        # =================================================================
        # 1. 查询 K 线数据和指标
        # =================================================================
        bars_query = text("""
            SELECT 
                b.ts_end,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                i.boll_mid,
                i.boll_up,
                i.boll_dn,
                i.rsi6,
                i.ao,
                i.cci14,
                i.obv,
                i.obv_ema20,
                i.rvol6
            FROM bars1m_equity b
            LEFT JOIN indicators_eq_1m i ON b.ts_end = i.ts_end AND b.symbol = i.symbol
            WHERE b.symbol = :symbol
              AND b.ts_end >= :start_utc
              AND b.ts_end <= :end_utc
            ORDER BY b.ts_end
        """)
        
        bars = conn.execute(bars_query, {
            "symbol": symbol,
            "start_utc": start_utc,
            "end_utc": end_utc
        }).fetchall()
        
        if not bars:
            print(f"❌ 未找到 {symbol} 在 {trade_date} 的数据")
            return
        
        print(f"✅ 找到 {len(bars)} 根 K 线数据\n")
        print(f"{'='*100}")
        print("📊 K 线与指标数据")
        print(f"{'='*100}\n")
        
        # 表头
        print(f"{'时间(UTC)':<20} {'时间(ET)':<10} | {'开':<8} {'高':<8} {'低':<8} {'收':<8} | {'中轨':<8} {'上轨':<8} {'下轨':<8} | {'RSI6':<6} {'AO':<8} | {'收盘位置':<12}")
        print(f"{'-'*160}")
        
        # 找到买入时刻的索引
        entry_idx = None
        for idx, bar in enumerate(bars):
            bar_et = bar.ts_end - timedelta(hours=4)
            bar_et_str = bar_et.strftime("%H:%M")
            
            # 判断收盘价相对于布林带的位置
            close_pos = ""
            if bar.close and bar.boll_up and bar.boll_mid and bar.boll_dn:
                if bar.close > bar.boll_up:
                    close_pos = "🔴 上轨外"
                elif bar.close < bar.boll_dn:
                    close_pos = "🟢 下轨外"
                elif bar.close >= bar.boll_mid:
                    close_pos = "🟡 中轨-上轨"
                else:
                    close_pos = "🟠 下轨-中轨"
                
                # 检查是否"回踩上轨"
                if bar.close < bar.boll_up and bar.high >= bar.boll_up:
                    close_pos += " 👈回踩"
                elif bar.close < bar.boll_up:
                    close_pos += " ↓上轨下"
            
            # 高亮显示买入时刻
            marker = ""
            if bar_et_str == entry_time:
                marker = "🔵 买入 → "
                entry_idx = idx
            elif entry_idx is not None and idx == entry_idx + 1:
                marker = "🔴 卖出 → "
            
            print(f"{marker}{str(bar.ts_end):<20} {bar_et_str:<10} | "
                  f"{bar.open or 0:<8.2f} {bar.high or 0:<8.2f} {bar.low or 0:<8.2f} {bar.close or 0:<8.2f} | "
                  f"{bar.boll_mid or 0:<8.2f} {bar.boll_up or 0:<8.2f} {bar.boll_dn or 0:<8.2f} | "
                  f"{bar.rsi6 or 0:<6.1f} {bar.ao or 0:<8.2f} | "
                  f"{close_pos:<12}")
        
        # =================================================================
        # 2. 分析买入和卖出的合理性
        # =================================================================
        print(f"\n{'='*100}")
        print("🔍 交易逻辑分析")
        print(f"{'='*100}\n")
        
        if entry_idx is None:
            print(f"⚠️  未找到买入时刻的数据")
            return
        
        if entry_idx >= len(bars):
            print(f"⚠️  没有卖出时刻的数据")
            return
        
        entry_bar = bars[entry_idx]
        entry_bar_et = entry_bar.ts_end - timedelta(hours=4)
        
        print(f"📌 买入时刻: {entry_bar_et.strftime('%H:%M')} ET")
        print(f"   价格: ${entry_bar.close:.2f}")
        print(f"   布林上轨: ${entry_bar.boll_up:.2f}")
        print(f"   收盘价与上轨距离: ${entry_bar.close - entry_bar.boll_up:.2f} ({(entry_bar.close - entry_bar.boll_up) / entry_bar.boll_up * 100:.2f}%)")
        
        # 检查买入条件（E1: 开盘追高）
        print(f"\n🔍 买入信号 SIG_OPEN_CHASE_BUY 检查:")
        
        # 需要检查 09:30 和 09:31 两根K线（但我们的数据可能从09:26开始）
        # 找到09:30和09:31
        bars_0930_0931 = []
        for bar in bars:
            bar_et = bar.ts_end - timedelta(hours=4)
            bar_et_str = bar_et.strftime("%H:%M")
            if bar_et_str in ["09:30", "09:31"]:
                bars_0930_0931.append((bar_et_str, bar))
        
        if len(bars_0930_0931) >= 2:
            for time_str, bar in bars_0930_0931:
                is_green = bar.close > bar.open if (bar.close and bar.open) else False
                print(f"   {time_str}: {'🟢 阳线' if is_green else '🔴 阴线'} "
                      f"(开: ${bar.open:.2f}, 收: ${bar.close:.2f}, 涨幅: {(bar.close - bar.open) / bar.open * 100:.2f}%)")
        
        # 检查卖出条件
        if entry_idx + 1 < len(bars):
            exit_bar = bars[entry_idx + 1]
            exit_bar_et = exit_bar.ts_end - timedelta(hours=4)
            
            print(f"\n📌 卖出时刻: {exit_bar_et.strftime('%H:%M')} ET")
            print(f"   价格: ${exit_bar.close:.2f}")
            print(f"   布林上轨: ${exit_bar.boll_up:.2f}")
            print(f"   收盘价与上轨距离: ${exit_bar.close - exit_bar.boll_up:.2f} ({(exit_bar.close - exit_bar.boll_up) / exit_bar.boll_up * 100:.2f}%)")
            
            print(f"\n🔍 卖出信号 SIG_EXIT_UPPER_TAP_X2 检查:")
            print(f"   条件: 连续两根K线收盘价都在布林上轨下方")
            
            entry_below_upper = entry_bar.close < entry_bar.boll_up
            exit_below_upper = exit_bar.close < exit_bar.boll_up
            
            print(f"   • 买入K线 ({entry_bar_et.strftime('%H:%M')}): 收盘 ${entry_bar.close:.2f} {'<' if entry_below_upper else '>='} 上轨 ${entry_bar.boll_up:.2f} → {'✅ 满足' if entry_below_upper else '❌ 不满足'}")
            print(f"   • 卖出K线 ({exit_bar_et.strftime('%H:%M')}): 收盘 ${exit_bar.close:.2f} {'<' if exit_below_upper else '>='} 上轨 ${exit_bar.boll_up:.2f} → {'✅ 满足' if exit_below_upper else '❌ 不满足'}")
            
            if entry_below_upper and exit_below_upper:
                print(f"\n✅ 卖出信号触发条件满足")
                print(f"\n⚠️  问题分析:")
                print(f"   1. 买入时（{entry_bar_et.strftime('%H:%M')}）收盘价 ${entry_bar.close:.2f} 已经在上轨 ${entry_bar.boll_up:.2f} 下方")
                print(f"   2. 这意味着买入信号触发时，价格可能在上轨附近或略低于上轨")
                print(f"   3. 如果买入后价格没有突破上轨，而是继续在上轨下方，就会立即触发卖出")
                print(f"\n💡 可能的原因:")
                print(f"   • E1（开盘追高）信号是基于09:30的两根阳线，但不要求价格突破上轨")
                print(f"   • 买入时机可能在价格冲高回落后，已经回到上轨下方")
                print(f"   • S2（回踩上轨）信号过于敏感，建议调整为「突破上轨后再回踩」")
                
                # 检查价格是否曾突破上轨
                if entry_bar.high > entry_bar.boll_up:
                    print(f"\n   📊 注意: 买入K线的最高价 ${entry_bar.high:.2f} 曾触及上轨 ${entry_bar.boll_up:.2f}")
                    print(f"        但收盘价回落到上轨下方，这是典型的「冲高回落」形态")
            else:
                print(f"\n❌ 卖出信号触发条件不满足，可能是回测逻辑有误")
            
            # 计算实际收益
            pnl_pct = (exit_bar.close - entry_bar.close) / entry_bar.close * 100
            pnl_icon = "🟢" if pnl_pct > 0 else "🔴" if pnl_pct < 0 else "⚪"
            
            print(f"\n💰 交易结果:")
            print(f"   买入: ${entry_bar.close:.2f}")
            print(f"   卖出: ${exit_bar.close:.2f}")
            print(f"   收益: {pnl_icon} {pnl_pct:+.2f}%")
            print(f"   持有时间: 1 分钟")
        
        # =================================================================
        # 3. 查询回测记录
        # =================================================================
        print(f"\n{'='*100}")
        print("📋 回测记录查询")
        print(f"{'='*100}\n")
        
        # 查询这个标的在这个日期的回测信号
        signals_query = text("""
            SELECT 
                s.ts_end,
                s.signal_code,
                s.accepted,
                s.reason
            FROM bt_signals s
            JOIN bt_runs r ON s.run_id = r.run_id
            WHERE r.parameters->>'symbol' = :symbol
              AND DATE(s.ts_end AT TIME ZONE 'America/New_York') = :trade_date
            ORDER BY s.ts_end
        """)
        
        signals = conn.execute(signals_query, {
            "symbol": symbol,
            "trade_date": trade_date
        }).fetchall()
        
        if signals:
            print(f"✅ 找到 {len(signals)} 条信号记录\n")
            print(f"{'时间(ET)':<20} {'信号类型':<30} {'接受':<8} {'原因':<50}")
            print(f"{'-'*120}")
            
            for sig in signals:
                sig_et = sig.ts_end - timedelta(hours=4)
                sig_et_str = sig_et.strftime("%Y-%m-%d %H:%M")
                status = '✅' if sig.accepted else '❌'
                reason_str = str(sig.reason)[:47] + '...' if len(str(sig.reason)) > 50 else str(sig.reason)
                print(f"{sig_et_str:<20} {sig.signal_code:<30} {status:<8} {reason_str:<50}")
        else:
            print(f"⚠️  未找到回测信号记录")
        
        print(f"\n{'='*100}")
        print("✅ 检查完成")
        print(f"{'='*100}\n")


if __name__ == "__main__":
    if len(sys.argv) >= 4:
        symbol = sys.argv[1]
        trade_date = sys.argv[2]
        entry_time = sys.argv[3]
    else:
        # 默认参数：根据用户提供的信息
        print("用法: python check_trade_case.py <symbol> <trade_date> <entry_time>")
        print("例如: python check_trade_case.py TSLA 2025-10-03 09:31")
        print("\n使用默认参数进行演示...")
        symbol = input("请输入股票代码 (如 TSLA): ").strip() or "TSLA"
        trade_date = input("请输入交易日期 (如 2025-10-03): ").strip() or "2025-10-03"
        entry_time = input("请输入买入时间 (如 09:31): ").strip() or "09:31"
    
    check_trade_case(symbol, trade_date, entry_time)



