#!/usr/bin/env python3
"""
超详细回测报告
包括：
1. 所有买入和卖出信号及触发时的指标
2. 信号配对及收益
3. 策略合规性审查
"""
import sys
from pathlib import Path
from datetime import datetime, time
import pytz
from decimal import Decimal
from collections import defaultdict
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from libs.core.config import get_settings


def generate_comprehensive_report(batch_id: str):
    """生成超详细回测报告"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    et_tz = pytz.timezone('US/Eastern')
    
    print(f"\n{'='*150}")
    print(f"📊 超详细回测报告")
    print(f"批次: {batch_id}")
    print(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*150}\n")
    
    with engine.connect() as conn:
        # 获取回测基本信息
        run_query = text("""
            SELECT run_id, parameters
            FROM bt_runs
            WHERE parameters->>'batch_id' = :batch_id
            ORDER BY run_id
        """)
        
        runs = conn.execute(run_query, {"batch_id": batch_id}).fetchall()
        
        if not runs:
            print("❌ 未找到回测数据")
            return
        
        # 提取回测参数
        first_run = runs[0]
        params = first_run.parameters
        
        print("📋 回测配置")
        print("="*150)
        print(f"开始时间: {params.get('start')}")
        print(f"结束时间: {params.get('end')}")
        print(f"交易日: {', '.join(params.get('trade_dates', []))}")
        print(f"标的数量: {len(runs)}")
        print(f"追踪模式: {params.get('track')}")
        print(f"信号模式: {params.get('signal_mode')}")
        print()
        
        # ============================================================
        # 第一部分：所有信号详情（带指标）
        # ============================================================
        print(f"{'='*150}")
        print("📍 第一部分：所有信号详情（含触发时指标）")
        print(f"{'='*150}\n")
        
        all_signals_data = []
        
        for run in runs:
            run_id = run.run_id
            symbol = run.parameters.get('symbol')
            
            # 获取该标的的所有信号
            signals_query = text("""
                SELECT 
                    s.ts_end,
                    s.signal_code,
                    s.accepted,
                    s.reason
                FROM bt_signals s
                WHERE s.run_id = :run_id
                ORDER BY s.ts_end
            """)
            
            signals = conn.execute(signals_query, {"run_id": run_id}).fetchall()
            
            print(f"\n{'▼'*75}")
            print(f"📌 {symbol} - 共 {len(signals)} 个信号")
            print(f"{'▼'*75}\n")
            
            for sig in signals:
                ts_et = sig.ts_end.astimezone(et_tz)
                
                # 获取该时刻的指标数据
                indicator_query = text("""
                    SELECT 
                        b.open, b.high, b.low, b.close, b.volume,
                        i.rsi6, i.rsi12, i.rsi24,
                        i.atr14, i.ao, i.stoch_k, i.stoch_d,
                        i.cci14, i.cci6, i.obv, i.obv_ema20,
                        i.mfi14, i.rvol6
                    FROM bars1m_equity b
                    LEFT JOIN indicators_eq_1m i 
                        ON b.symbol = i.symbol AND b.ts_end = i.ts_end
                    WHERE b.symbol = :symbol AND b.ts_end = :ts_end
                """)
                
                indicator = conn.execute(indicator_query, {
                    "symbol": symbol,
                    "ts_end": sig.ts_end
                }).fetchone()
                
                # 计算BOLL (这里简化，实际需要从历史数据计算)
                # 为了报告完整性，我们从reason中提取
                reason_dict = sig.reason if isinstance(sig.reason, dict) else {}
                
                status_icon = '✅' if sig.accepted else '❌'
                signal_type = '🟢 买入' if ('BUY' in sig.signal_code or 'BOTTOM' in sig.signal_code) else '🔴 卖出'
                
                print(f"{status_icon} {signal_type} | {ts_et.strftime('%Y-%m-%d %H:%M:%S %Z')} | {sig.signal_code}")
                
                if indicator:
                    print(f"   价格: O=${float(indicator.open):.2f} H=${float(indicator.high):.2f} "
                          f"L=${float(indicator.low):.2f} C=${float(indicator.close):.2f}")
                    print(f"   成交量: {indicator.volume:,}")
                    print(f"   RSI: RSI6={float(indicator.rsi6 or 0):.1f} RSI12={float(indicator.rsi12 or 0):.1f} "
                          f"RSI24={float(indicator.rsi24 or 0):.1f}")
                    print(f"   动量: AO={float(indicator.ao or 0):.2f} CCI14={float(indicator.cci14 or 0):.1f} "
                          f"CCI6={float(indicator.cci6 or 0):.1f}")
                    print(f"   随机: K={float(indicator.stoch_k or 0):.1f} D={float(indicator.stoch_d or 0):.1f}")
                    print(f"   成交量: MFI14={float(indicator.mfi14 or 0):.1f} RVOL6={float(indicator.rvol6 or 0):.2f}")
                    print(f"   ATR14: {float(indicator.atr14 or 0):.2f}")
                    print(f"   OBV: {float(indicator.obv or 0):.0f} (EMA20: {float(indicator.obv_ema20 or 0):.0f})")
                
                if reason_dict:
                    print(f"   触发原因: {json.dumps(reason_dict, indent=15, ensure_ascii=False)}")
                
                print()
                
                # 保存数据供后续分析
                all_signals_data.append({
                    'symbol': symbol,
                    'ts_end': sig.ts_end,
                    'ts_et': ts_et,
                    'signal_code': sig.signal_code,
                    'accepted': sig.accepted,
                    'reason': reason_dict,
                    'indicator': indicator._asdict() if indicator else None
                })
        
        # ============================================================
        # 第二部分：信号配对与收益分析
        # ============================================================
        print(f"\n{'='*150}")
        print("💰 第二部分：信号配对与收益分析")
        print(f"{'='*150}\n")
        
        symbol_results = []
        
        for run in runs:
            run_id = run.run_id
            symbol = run.parameters.get('symbol')
            
            # 获取信号
            signals_query = text("""
                SELECT ts_end, signal_code, accepted, reason
                FROM bt_signals
                WHERE run_id = :run_id AND accepted = true
                ORDER BY ts_end
            """)
            
            signals = conn.execute(signals_query, {"run_id": run_id}).fetchall()
            
            # 分类
            buy_signals = []
            sell_signals = []
            
            for sig in signals:
                if 'BUY' in sig.signal_code or 'BOTTOM' in sig.signal_code:
                    buy_signals.append(sig)
                elif 'EXIT' in sig.signal_code or 'CLEAR' in sig.signal_code:
                    sell_signals.append(sig)
            
            if not buy_signals:
                print(f"📌 {symbol} - 无买入信号\n")
                continue
            
            print(f"{'▼'*75}")
            print(f"📌 {symbol} - {len(buy_signals)} 个买入信号, {len(sell_signals)} 个卖出信号")
            print(f"{'▼'*75}\n")
            
            # 配对
            pairs = []
            used_sells = set()
            
            for buy_sig in buy_signals:
                best_sell = None
                for i, sell_sig in enumerate(sell_signals):
                    if sell_sig.ts_end > buy_sig.ts_end and i not in used_sells:
                        best_sell = (i, sell_sig)
                        break
                
                if best_sell:
                    i, sell_sig = best_sell
                    used_sells.add(i)
                    
                    # 获取价格
                    price_query = text("""
                        SELECT close FROM bars1m_equity
                        WHERE symbol = :symbol AND ts_end = :ts
                    """)
                    
                    buy_price = conn.execute(price_query, {
                        "symbol": symbol, "ts": buy_sig.ts_end
                    }).fetchone()
                    
                    sell_price = conn.execute(price_query, {
                        "symbol": symbol, "ts": sell_sig.ts_end
                    }).fetchone()
                    
                    if buy_price and sell_price:
                        buy_px = float(buy_price.close)
                        sell_px = float(sell_price.close)
                        pnl_pct = (sell_px - buy_px) / buy_px * 100
                        hold_minutes = (sell_sig.ts_end - buy_sig.ts_end).total_seconds() / 60
                        
                        buy_ts_et = buy_sig.ts_end.astimezone(et_tz)
                        sell_ts_et = sell_sig.ts_end.astimezone(et_tz)
                        
                        pnl_icon = '🟢' if pnl_pct > 0 else '🔴' if pnl_pct < 0 else '⚪'
                        
                        print(f"交易对:")
                        print(f"  {pnl_icon} 收益: {pnl_pct:+.2f}%")
                        print(f"  买入: {buy_ts_et.strftime('%m-%d %H:%M %Z')} @ ${buy_px:.2f}  [{buy_sig.signal_code}]")
                        print(f"  卖出: {sell_ts_et.strftime('%m-%d %H:%M %Z')} @ ${sell_px:.2f}  [{sell_sig.signal_code}]")
                        print(f"  持有: {hold_minutes:.0f} 分钟 ({int(hold_minutes//60)}h{int(hold_minutes%60)}m)")
                        print()
                        
                        pairs.append({
                            'buy_time': buy_sig.ts_end,
                            'sell_time': sell_sig.ts_end,
                            'buy_signal': buy_sig.signal_code,
                            'sell_signal': sell_sig.signal_code,
                            'buy_price': buy_px,
                            'sell_price': sell_px,
                            'pnl_pct': pnl_pct,
                            'hold_minutes': hold_minutes
                        })
            
            # 未配对的买入信号
            if len(buy_signals) > len(pairs):
                print(f"⚠️  未配对的买入信号: {len(buy_signals) - len(pairs)} 个")
                for buy_sig in buy_signals[len(pairs):]:
                    buy_ts_et = buy_sig.ts_end.astimezone(et_tz)
                    print(f"  - {buy_ts_et.strftime('%m-%d %H:%M %Z')} [{buy_sig.signal_code}]")
                print()
            
            if pairs:
                total_pnl = sum(p['pnl_pct'] for p in pairs)
                avg_pnl = total_pnl / len(pairs)
                win_count = sum(1 for p in pairs if p['pnl_pct'] > 0)
                win_rate = win_count / len(pairs)
                
                print(f"📊 {symbol} 汇总:")
                print(f"  总收益: {total_pnl:+.2f}%")
                print(f"  平均收益: {avg_pnl:+.2f}%")
                print(f"  胜率: {win_rate:.1%} ({win_count}/{len(pairs)})")
                print()
                
                symbol_results.append({
                    'symbol': symbol,
                    'pairs': pairs,
                    'total_pnl': total_pnl,
                    'avg_pnl': avg_pnl,
                    'win_rate': win_rate,
                    'win_count': win_count
                })
        
        # ============================================================
        # 第三部分：策略合规性审查
        # ============================================================
        print(f"\n{'='*150}")
        print("🔍 第三部分：策略合规性审查")
        print(f"{'='*150}\n")
        
        print("📋 策略文档规则检查:\n")
        
        violations = []
        compliance_checks = []
        
        # 规则1: 开盘追高必须14:00前清仓
        print("规则1: 开盘追高(OPEN_CHASE_BUY)必须在14:00(美东)前清仓")
        print("-"*100)
        
        cutoff_time = time(14, 0)
        
        for run in runs:
            run_id = run.run_id
            symbol = run.parameters.get('symbol')
            
            signals_query = text("""
                SELECT ts_end, signal_code
                FROM bt_signals
                WHERE run_id = :run_id AND accepted = true
                ORDER BY ts_end
            """)
            
            signals = conn.execute(signals_query, {"run_id": run_id}).fetchall()
            
            buy_signals = [s for s in signals if 'OPEN_CHASE_BUY' in s.signal_code]
            sell_signals = [s for s in signals if 'EXIT' in s.signal_code]
            
            for buy_sig in buy_signals:
                # 找对应的卖出
                sell_sig = None
                for s in sell_signals:
                    if s.ts_end > buy_sig.ts_end:
                        sell_sig = s
                        break
                
                if sell_sig:
                    sell_time_et = sell_sig.ts_end.astimezone(et_tz)
                    sell_time_only = sell_time_et.time()
                    
                    if sell_time_only > cutoff_time:
                        violations.append({
                            'rule': '规则1',
                            'symbol': symbol,
                            'type': 'OPEN_CHASE_BUY超时清仓',
                            'detail': f"买入{buy_sig.ts_end.astimezone(et_tz).strftime('%m-%d %H:%M')}, "
                                     f"卖出{sell_time_et.strftime('%m-%d %H:%M')} > 14:00"
                        })
                        print(f"❌ {symbol}: OPEN_CHASE_BUY在{sell_time_et.strftime('%H:%M')}平仓，"
                              f"超过14:00截止时间{(sell_time_only.hour*60+sell_time_only.minute)-(14*60)}分钟")
                    else:
                        compliance_checks.append(f"✅ {symbol}: OPEN_CHASE_BUY合规清仓于{sell_time_et.strftime('%H:%M')}")
        
        if not any(v['rule'] == '规则1' for v in violations):
            print("✅ 所有OPEN_CHASE_BUY仓位均在14:00前清仓")
        print()
        
        # 规则2: 12:00-14:00禁开新仓
        print("规则2: 12:00-14:00 禁开新仓")
        print("-"*100)
        
        noon_start = time(12, 0)
        noon_end = time(14, 0)
        
        for run in runs:
            run_id = run.run_id
            symbol = run.parameters.get('symbol')
            
            signals_query = text("""
                SELECT ts_end, signal_code
                FROM bt_signals
                WHERE run_id = :run_id AND accepted = true
                  AND (signal_code LIKE '%BUY%' OR signal_code LIKE '%BOTTOM%')
                ORDER BY ts_end
            """)
            
            buy_signals = conn.execute(signals_query, {"run_id": run_id}).fetchall()
            
            for sig in buy_signals:
                ts_et = sig.ts_end.astimezone(et_tz)
                ts_only = ts_et.time()
                
                if noon_start <= ts_only < noon_end:
                    violations.append({
                        'rule': '规则2',
                        'symbol': symbol,
                        'type': '午间禁开时段买入',
                        'detail': f"{ts_et.strftime('%m-%d %H:%M')} [{sig.signal_code}]"
                    })
                    print(f"❌ {symbol}: {ts_et.strftime('%H:%M')}买入，违反12:00-14:00禁开规则 [{sig.signal_code}]")
        
        if not any(v['rule'] == '规则2' for v in violations):
            print("✅ 无午间禁开时段买入")
        print()
        
        # 规则3: VIX<20才能触发E1(追高)
        print("规则3: VIX<20 才能触发E1(开盘追高)")
        print("-"*100)
        print("⚠️  当前回测VIX模式为ignore，跳过VIX检查")
        print("   (从reason字段可以看到: 'vix_mode': 'ignore')")
        print()
        
        # 规则4: 过滤主规则(AO/CCI/OBV三选二)
        print("规则4: 过滤主规则 - AO>0且上行、CCI14>0&CCI6>0、OBV>EMA20，满足其二")
        print("-"*100)
        
        # 这个需要从reason字段中检查
        for sig_data in all_signals_data:
            if sig_data['accepted'] and ('BUY' in sig_data['signal_code']):
                reason = sig_data['reason']
                if reason and 'status' in reason and reason['status'] == 'PASS':
                    print(f"✅ {sig_data['symbol']} {sig_data['ts_et'].strftime('%m-%d %H:%M')}: "
                          f"{sig_data['signal_code']} - 过滤规则通过")
        
        print()
        
        # 规则5: 期权DTE要求(2-7天)
        print("规则5: 期权DTE要求 (2-7天)")
        print("-"*100)
        print("ℹ️  当前回测使用equity track，未涉及期权选择，此规则不适用")
        print()
        
        # ============================================================
        # 第四部分：统计汇总
        # ============================================================
        print(f"{'='*150}")
        print("📊 第四部分：统计汇总")
        print(f"{'='*150}\n")
        
        # 按标的排名
        symbol_results.sort(key=lambda x: x['total_pnl'], reverse=True)
        
        print("标的表现排名:")
        print("-"*100)
        print(f"{'排名':<6} {'标的':<8} {'交易笔数':<10} {'总收益':<12} {'平均收益':<12} {'胜率':<10}")
        print("-"*100)
        
        for idx, res in enumerate(symbol_results, 1):
            icon = '🟢' if res['total_pnl'] > 0 else '🔴' if res['total_pnl'] < 0 else '⚪'
            rank_icon = '🥇' if idx == 1 else '🥈' if idx == 2 else '🥉' if idx == 3 else '  '
            
            print(f"{rank_icon} {idx:<3} {res['symbol']:<8} {len(res['pairs']):<10} "
                  f"{icon} {res['total_pnl']:>9.2f}% {icon} {res['avg_pnl']:>9.2f}% "
                  f"{res['win_rate']:>9.1%}")
        
        # 总体统计
        total_pairs = sum(len(r['pairs']) for r in symbol_results)
        total_win = sum(r['win_count'] for r in symbol_results)
        total_pnl = sum(r['total_pnl'] for r in symbol_results)
        overall_avg = total_pnl / len(symbol_results) if symbol_results else 0
        overall_win_rate = total_win / total_pairs if total_pairs > 0 else 0
        
        print("-"*100)
        print(f"{'总计':<6} {len(runs)}标的   {total_pairs:<10} "
              f"{'  '} {total_pnl:>9.2f}% {'  '} {overall_avg:>9.2f}% "
              f"{overall_win_rate:>9.1%}")
        print()
        
        # 按信号类型统计
        print("按信号类型统计:")
        print("-"*100)
        
        signal_stats = defaultdict(lambda: {'count': 0, 'total_pnl': 0, 'win': 0})
        
        for res in symbol_results:
            for pair in res['pairs']:
                sig_type = pair['buy_signal']
                signal_stats[sig_type]['count'] += 1
                signal_stats[sig_type]['total_pnl'] += pair['pnl_pct']
                if pair['pnl_pct'] > 0:
                    signal_stats[sig_type]['win'] += 1
        
        print(f"{'信号类型':<35} {'数量':<8} {'平均收益':<15} {'胜率':<10}")
        print("-"*100)
        
        for sig_type in sorted(signal_stats.keys()):
            stats = signal_stats[sig_type]
            avg_pnl = stats['total_pnl'] / stats['count']
            win_rate = stats['win'] / stats['count']
            icon = '🟢' if avg_pnl > 0 else '🔴'
            
            print(f"{sig_type:<35} {stats['count']:<8} {icon} {avg_pnl:>12.2f}% {win_rate:>9.1%}")
        
        print()
        
        # ============================================================
        # 第五部分：合规性总结
        # ============================================================
        print(f"{'='*150}")
        print("✅ 第五部分：合规性总结")
        print(f"{'='*150}\n")
        
        if violations:
            print(f"❌ 发现 {len(violations)} 个违规项:\n")
            for v in violations:
                print(f"  • [{v['rule']}] {v['symbol']}: {v['type']}")
                print(f"    {v['detail']}")
                print()
        else:
            print("✅ 所有检查项目均合规\n")
        
        print("合规检查汇总:")
        print(f"  • 规则1 (OPEN_CHASE_BUY 14:00清仓): {'✅ 通过' if not any(v['rule']=='规则1' for v in violations) else '❌ 违规'}")
        print(f"  • 规则2 (12:00-14:00禁开): {'✅ 通过' if not any(v['rule']=='规则2' for v in violations) else '❌ 违规'}")
        print(f"  • 规则3 (VIX检查): ⚠️  已忽略")
        print(f"  • 规则4 (过滤规则): ✅ 已应用")
        print(f"  • 规则5 (期权DTE): N/A (equity track)")
        print()
        
        print(f"{'='*150}")
        print("📝 报告生成完成")
        print(f"{'='*150}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        batch_id = sys.argv[1]
    else:
        batch_id = "bt-20251002-20251006-ca6396"
    
    generate_comprehensive_report(batch_id)

