#!/usr/bin/env python3
"""
检查回测中的时间规则违规
"""
import sys
from pathlib import Path
from datetime import datetime, time
import pytz

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from libs.core.config import get_settings


def check_time_violations(batch_id: str):
    """检查时间规则违规"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print(f"\n{'='*120}")
    print(f"⏰ 时间规则违规检查")
    print(f"{'='*120}\n")
    
    # 时间规则
    print("📋 策略时间规则:")
    print("   1. 开盘追高(OPEN_CHASE_BUY)必须在14:00(美东)前清仓")
    print("   2. 期权交易时间：09:30-16:00 (美东)")
    print("   3. 12:00-14:00 禁开新仓")
    print()
    
    et_tz = pytz.timezone('US/Eastern')
    
    with engine.connect() as conn:
        # 获取所有交易对
        run_query = text("""
            SELECT run_id, parameters
            FROM bt_runs
            WHERE parameters->>'batch_id' = :batch_id
            ORDER BY run_id
        """)
        
        runs = conn.execute(run_query, {"batch_id": batch_id}).fetchall()
        
        violations = []
        total_trades = 0
        
        for run in runs:
            run_id = run.run_id
            symbol = run.parameters.get('symbol')
            
            # 获取信号
            signals_query = text("""
                SELECT ts_end, signal_code, accepted
                FROM bt_signals
                WHERE run_id = :run_id
                  AND accepted = true
                ORDER BY ts_end
            """)
            
            signals = conn.execute(signals_query, {"run_id": run_id}).fetchall()
            
            # 分类
            buy_signals = []
            sell_signals = []
            
            for sig in signals:
                if 'BUY' in sig.signal_code or 'BOTTOM' in sig.signal_code:
                    buy_signals.append(sig)
                elif 'EXIT' in sig.signal_code:
                    sell_signals.append(sig)
            
            # 配对
            used_sells = set()
            for buy_sig in buy_signals:
                for i, sell_sig in enumerate(sell_signals):
                    if sell_sig.ts_end > buy_sig.ts_end and i not in used_sells:
                        total_trades += 1
                        
                        # 转换为美东时间
                        buy_time_et = buy_sig.ts_end.astimezone(et_tz)
                        sell_time_et = sell_sig.ts_end.astimezone(et_tz)
                        
                        buy_time_only = buy_time_et.time()
                        sell_time_only = sell_time_et.time()
                        
                        violation_reasons = []
                        
                        # 检查规则1: OPEN_CHASE_BUY 必须14:00前平仓
                        if 'OPEN_CHASE_BUY' in buy_sig.signal_code:
                            cutoff_time = time(14, 0)  # 14:00
                            if sell_time_only > cutoff_time:
                                violation_reasons.append(
                                    f"开盘追高仓位在{sell_time_only.strftime('%H:%M')}平仓，超过14:00截止时间"
                                )
                        
                        # 检查规则2: 卖出时间必须在16:00前
                        market_close = time(16, 0)  # 16:00
                        if sell_time_only > market_close:
                            violation_reasons.append(
                                f"卖出时间{sell_time_only.strftime('%H:%M')}超过期权交易时间(16:00)"
                            )
                        
                        # 检查规则3: 09:30前不能开仓
                        market_open = time(9, 30)
                        if buy_time_only < market_open:
                            violation_reasons.append(
                                f"买入时间{buy_time_only.strftime('%H:%M')}早于开盘时间(09:30)"
                            )
                        
                        # 检查规则4: 12:00-14:00禁开新仓
                        noon_start = time(12, 0)
                        noon_end = time(14, 0)
                        if noon_start <= buy_time_only < noon_end:
                            violation_reasons.append(
                                f"买入时间{buy_time_only.strftime('%H:%M')}在午间禁开时段(12:00-14:00)"
                            )
                        
                        if violation_reasons:
                            violations.append({
                                'symbol': symbol,
                                'buy_signal': buy_sig.signal_code,
                                'sell_signal': sell_sig.signal_code,
                                'buy_time_et': buy_time_et,
                                'sell_time_et': sell_time_et,
                                'reasons': violation_reasons
                            })
                        
                        used_sells.add(i)
                        break
        
        # 报告
        print(f"{'='*120}")
        print(f"📊 检查结果")
        print(f"{'='*120}\n")
        
        print(f"总交易笔数: {total_trades}")
        print(f"违规交易: {len(violations)} 笔")
        print(f"违规率: {len(violations)/total_trades*100 if total_trades > 0 else 0:.1f}%\n")
        
        if violations:
            print(f"{'='*120}")
            print(f"❌ 违规交易详情")
            print(f"{'='*120}\n")
            
            for idx, v in enumerate(violations, 1):
                print(f"#{idx}. {v['symbol']}")
                print(f"   买入: {v['buy_time_et'].strftime('%Y-%m-%d %H:%M:%S %Z')}  [{v['buy_signal']}]")
                print(f"   卖出: {v['sell_time_et'].strftime('%Y-%m-%d %H:%M:%S %Z')}  [{v['sell_signal']}]")
                print(f"   违规原因:")
                for reason in v['reasons']:
                    print(f"      ⚠️  {reason}")
                print()
        else:
            print("✅ 未发现时间规则违规")
        
        print(f"{'='*120}")
        print("🔍 问题分析")
        print(f"{'='*120}\n")
        
        if violations:
            print("问题根源:")
            print("1. 回测代码未正确实现时间纪律规则")
            print("2. SIG_EXIT_BOX2MID 和 SIG_EXIT_UPPER_TAP_X2 没有检查时间限制")
            print("3. 应该在14:00强制清空OPEN_CHASE_BUY仓位（SIG_TIME_CLEAR_14）")
            print()
            print("影响:")
            print("1. 实际期权交易无法在16:00后执行")
            print("2. 开盘追高仓位风险敞口超过允许时间")
            print("3. 回测结果不能反映真实可执行的策略")
            print()
            print("修复方案:")
            print("1. 在信号生成时增加时间窗口检查")
            print("2. 在14:00触发强制清仓信号（SIG_TIME_CLEAR_14）")
            print("3. 过滤掉所有16:00后的退出信号")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        batch_id = sys.argv[1]
    else:
        batch_id = "bt-20251002-20251007-3d379d"
    
    check_time_violations(batch_id)

