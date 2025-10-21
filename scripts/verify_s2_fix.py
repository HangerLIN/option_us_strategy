#!/usr/bin/env python3
"""
验证 S2 信号修复效果

对比修复前后的行为：
- 修复前：只要连续两根 < 上轨就触发
- 修复后：必须先在上轨之上，然后连续两根回到上轨内
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from libs.core.config import get_settings


def verify_s2_logic_old(df_bars):
    """
    旧逻辑：只检查连续两根 < 上轨
    """
    if len(df_bars) < 2:
        return False
    prev = df_bars.iloc[-2]
    curr = df_bars.iloc[-1]
    return bool(prev["close"] < prev["boll_up"] and curr["close"] < curr["boll_up"])


def verify_s2_logic_new(df_bars, eps=1e-6):
    """
    新逻辑：严格三根形态（上 → 下 → 下）
    """
    if len(df_bars) < 3:
        return False
    
    prev2 = df_bars.iloc[-3]
    prev1 = df_bars.iloc[-2]
    curr = df_bars.iloc[-1]
    
    was_above = bool(prev2["close"] > prev2["boll_up"] + eps)
    tap_1 = bool(prev1["close"] < prev1["boll_up"] - eps)
    tap_2 = bool(curr["close"] < curr["boll_up"] - eps)
    
    return was_above and tap_1 and tap_2


def analyze_tsm_case():
    """分析 TSM 2025-10-07 09:31 案例"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print("\n" + "="*100)
    print("🔍 TSM 案例验证 (2025-10-07 09:31)")
    print("="*100 + "\n")
    
    with engine.connect() as conn:
        # 查询 TSM 数据
        query = text("""
            SELECT 
                b.ts_end,
                b.close,
                i.boll_up
            FROM bars1m_equity b
            LEFT JOIN indicators_eq_1m i ON b.ts_end = i.ts_end AND b.symbol = i.symbol
            WHERE b.symbol = :symbol
              AND b.ts_end >= '2025-10-07 21:28:00+08:00'::timestamptz
              AND b.ts_end <= '2025-10-07 21:32:00+08:00'::timestamptz
            ORDER BY b.ts_end
        """)
        
        result = conn.execute(query, {"symbol": "TSM"}).fetchall()
        
        if not result:
            print("❌ 未找到数据")
            return
        
        # 转换为 DataFrame
        df = pd.DataFrame([{
            "ts_end": r.ts_end,
            "close": float(r.close),
            "boll_up": float(r.boll_up)
        } for r in result])
        
        print("📊 K线数据:")
        print(df.to_string(index=False))
        print()
        
        # 找到 09:31 和 09:32 (对应 21:31 和 21:32 +08:00)
        idx_0931 = df[df['ts_end'].astype(str).str.contains('21:31')].index
        idx_0932 = df[df['ts_end'].astype(str).str.contains('21:32')].index
        
        if len(idx_0931) == 0 or len(idx_0932) == 0:
            print("❌ 未找到 09:31 或 09:32 数据")
            return
        
        idx_0932 = idx_0932[0]
        
        # 在 09:32 时刻检查信号
        df_at_0932 = df.iloc[:idx_0932+1]
        
        old_trigger = verify_s2_logic_old(df_at_0932)
        new_trigger = verify_s2_logic_new(df_at_0932)
        
        print("="*100)
        print("🔬 信号触发检查 (09:32 时刻)")
        print("="*100 + "\n")
        
        print(f"旧逻辑 (只检查连续两根 < 上轨): {'✅ 触发' if old_trigger else '❌ 不触发'}")
        print(f"新逻辑 (严格三根形态): {'✅ 触发' if new_trigger else '❌ 不触发'}")
        
        if old_trigger != new_trigger:
            print(f"\n🎯 修复生效！")
            print(f"   • 旧逻辑会错误触发")
            print(f"   • 新逻辑正确阻止了误触发")
        else:
            print(f"\n⚠️  两种逻辑结果相同")
        
        # 详细分析
        print(f"\n📋 详细分析:")
        if len(df_at_0932) >= 3:
            prev2 = df_at_0932.iloc[-3]
            prev1 = df_at_0932.iloc[-2]
            curr = df_at_0932.iloc[-1]
            
            print(f"\n   t-2 (09:30): close=${prev2['close']:.2f}, upper=${prev2['boll_up']:.2f}")
            print(f"                {'✅ 在上轨之上' if prev2['close'] > prev2['boll_up'] else '❌ 不在上轨之上'}")
            
            print(f"\n   t-1 (09:31): close=${prev1['close']:.2f}, upper=${prev1['boll_up']:.2f}")
            print(f"                {'✅ 在上轨之下' if prev1['close'] < prev1['boll_up'] else '❌ 不在上轨之下'}")
            
            print(f"\n   t   (09:32): close=${curr['close']:.2f}, upper=${curr['boll_up']:.2f}")
            print(f"                {'✅ 在上轨之下' if curr['close'] < curr['boll_up'] else '❌ 不在上轨之下'}")
            
            print(f"\n   💡 结论:")
            if prev2['close'] <= prev2['boll_up']:
                print(f"      09:30 (t-2) 不在上轨之上 → 不符合「从上轨上方回落」的语义")
                print(f"      新逻辑正确阻止触发")
            else:
                print(f"      09:30 (t-2) 在上轨之上 → 符合「回踩」语义")
                print(f"      两种逻辑都会触发（这是合理的）")


def count_affected_signals():
    """统计受影响的信号数量"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print("\n" + "="*100)
    print("📊 受影响信号统计")
    print("="*100 + "\n")
    
    with engine.connect() as conn:
        # 统计所有 S2 信号
        query = text("""
            SELECT COUNT(*) as total
            FROM bt_signals
            WHERE signal_code = 'SIG_EXIT_UPPER_TAP_X2'
              AND accepted = true
        """)
        
        result = conn.execute(query).fetchone()
        total_s2 = result.total
        
        print(f"✅ 历史 S2 信号总数: {total_s2}")
        
        # 统计持有时间 <= 2 分钟的
        query2 = text("""
            WITH signal_pairs AS (
                SELECT 
                    symbol,
                    ts_end,
                    signal_code,
                    LAG(ts_end) OVER (PARTITION BY run_id, symbol ORDER BY ts_end) as prev_ts,
                    LAG(signal_code) OVER (PARTITION BY run_id, symbol ORDER BY ts_end) as prev_code
                FROM bt_signals
                WHERE accepted = true
            )
            SELECT 
                COUNT(*) as quick_exit_count,
                AVG(EXTRACT(EPOCH FROM (ts_end - prev_ts))/60) as avg_hold_minutes
            FROM signal_pairs
            WHERE prev_code = 'SIG_OPEN_CHASE_BUY'
              AND signal_code = 'SIG_EXIT_UPPER_TAP_X2'
              AND EXTRACT(EPOCH FROM (ts_end - prev_ts))/60 <= 2
        """)
        
        result2 = conn.execute(query2).fetchone()
        quick_exit_count = result2.quick_exit_count
        avg_hold = result2.avg_hold_minutes
        
        print(f"⚠️  持有 ≤2 分钟的 S2 信号: {quick_exit_count} ({quick_exit_count/total_s2*100:.1f}%)")
        print(f"   平均持有时间: {avg_hold:.1f} 分钟")
        
        print(f"\n💡 预期修复效果:")
        print(f"   • 这 {quick_exit_count} 个快速退出的信号，大部分会被新逻辑过滤")
        print(f"   • 修复后 S2 信号数量预计减少 {quick_exit_count/total_s2*100:.0f}%")
        print(f"   • 剩余信号的质量会更高，持有时间会更合理")


def batch_validate_signals():
    """批量验证所有历史信号"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print("\n" + "="*100)
    print("🔬 批量验证历史 S2 信号")
    print("="*100 + "\n")
    
    with engine.connect() as conn:
        # 获取所有 S2 信号及其前后的 K 线数据
        query = text("""
            WITH s2_signals AS (
                SELECT 
                    s.run_id,
                    s.symbol,
                    s.ts_end as signal_ts,
                    LAG(s.ts_end) OVER (PARTITION BY s.run_id, s.symbol ORDER BY s.ts_end) as prev_signal_ts
                FROM bt_signals s
                WHERE s.signal_code = 'SIG_EXIT_UPPER_TAP_X2'
                  AND s.accepted = true
                LIMIT 50
            )
            SELECT 
                s.symbol,
                s.signal_ts,
                b.ts_end,
                b.close,
                i.boll_up
            FROM s2_signals s
            JOIN bars1m_equity b ON b.symbol = s.symbol
            LEFT JOIN indicators_eq_1m i ON i.ts_end = b.ts_end AND i.symbol = b.symbol
            WHERE b.ts_end >= s.signal_ts - interval '5 minutes'
              AND b.ts_end <= s.signal_ts
            ORDER BY s.symbol, s.signal_ts, b.ts_end
        """)
        
        results = conn.execute(query).fetchall()
        
        if not results:
            print("❌ 未找到历史信号数据")
            return
        
        # 按信号分组
        signals_data = {}
        for r in results:
            key = (r.symbol, r.signal_ts)
            if key not in signals_data:
                signals_data[key] = []
            signals_data[key].append({
                "ts_end": r.ts_end,
                "close": float(r.close) if r.close else 0,
                "boll_up": float(r.boll_up) if r.boll_up else 0
            })
        
        # 验证每个信号
        old_triggered = 0
        new_triggered = 0
        
        print(f"📋 验证 {len(signals_data)} 个历史 S2 信号...\n")
        
        mismatch_cases = []
        
        for (symbol, signal_ts), bars_data in signals_data.items():
            df = pd.DataFrame(bars_data)
            
            if len(df) >= 3:
                old_result = verify_s2_logic_old(df)
                new_result = verify_s2_logic_new(df)
                
                if old_result:
                    old_triggered += 1
                if new_result:
                    new_triggered += 1
                
                if old_result != new_result:
                    mismatch_cases.append({
                        "symbol": symbol,
                        "signal_ts": signal_ts,
                        "old": old_result,
                        "new": new_result,
                        "df": df
                    })
        
        print(f"📊 统计结果:")
        print(f"   旧逻辑触发: {old_triggered}/{len(signals_data)} ({old_triggered/len(signals_data)*100:.1f}%)")
        print(f"   新逻辑触发: {new_triggered}/{len(signals_data)} ({new_triggered/len(signals_data)*100:.1f}%)")
        print(f"   差异案例: {len(mismatch_cases)} ({len(mismatch_cases)/len(signals_data)*100:.1f}%)")
        
        if mismatch_cases:
            print(f"\n📋 差异案例详情 (前5个):\n")
            for case in mismatch_cases[:5]:
                print(f"   {case['symbol']} @ {case['signal_ts']}")
                print(f"      旧逻辑: {'✅ 触发' if case['old'] else '❌ 不触发'}")
                print(f"      新逻辑: {'✅ 触发' if case['new'] else '❌ 不触发'}")
                
                df = case['df']
                if len(df) >= 3:
                    prev2 = df.iloc[-3]
                    print(f"      t-2: close=${prev2['close']:.2f}, upper=${prev2['boll_up']:.2f} "
                          f"({'上轨上' if prev2['close'] > prev2['boll_up'] else '上轨下'})")
                print()


if __name__ == "__main__":
    print("\n" + "="*100)
    print("🔧 S2 信号修复验证工具")
    print("="*100)
    
    # 1. 分析 TSM 案例
    analyze_tsm_case()
    
    # 2. 统计受影响的信号
    count_affected_signals()
    
    # 3. 批量验证历史信号
    batch_validate_signals()
    
    print("\n" + "="*100)
    print("✅ 验证完成")
    print("="*100 + "\n")


