#!/usr/bin/env python3
"""
回测信号分析脚本 - 基于信号收益率评估
分析买入信号到卖出信号之间的价格变化
"""
import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from collections import defaultdict
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from libs.core.config import get_settings


def analyze_signal_performance(batch_id: str):
    """分析信号表现"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    print(f"\n{'='*100}")
    print(f"📊 回测信号表现分析")
    print(f"批次: {batch_id}")
    print(f"{'='*100}\n")
    
    with engine.connect() as conn:
        # 获取所有相关的 run_id
        run_query = text("""
            SELECT 
                run_id,
                strategy_code,
                started_at,
                completed_at,
                status,
                parameters
            FROM bt_runs
            WHERE parameters->>'batch_id' = :batch_id
            ORDER BY run_id
        """)
        
        runs = conn.execute(run_query, {"batch_id": batch_id}).fetchall()
        
        if not runs:
            print(f"❌ 未找到批次 {batch_id} 的回测数据")
            return
        
        run_ids = [r.run_id for r in runs]
        symbols = [r.parameters.get('symbol', 'UNKNOWN') for r in runs]
        
        print(f"✅ 找到 {len(runs)} 个标的的回测数据")
        print(f"📅 回测期间: {runs[0].parameters.get('start')} ~ {runs[0].parameters.get('end')}")
        print(f"📈 标的列表: {', '.join(symbols)}\n")
        
        # =================================================================
        # 1. 总体信号统计
        # =================================================================
        print(f"{'='*100}")
        print("1️⃣  总体信号统计")
        print(f"{'='*100}\n")
        
        signal_summary_query = text("""
            SELECT 
                COUNT(*) as total_signals,
                SUM(CASE WHEN accepted THEN 1 ELSE 0 END) as accepted,
                SUM(CASE WHEN NOT accepted THEN 1 ELSE 0 END) as rejected,
                COUNT(DISTINCT symbol) as symbol_count,
                COUNT(DISTINCT signal_code) as signal_types
            FROM bt_signals
            WHERE run_id = ANY(:run_ids)
        """)
        
        summary = conn.execute(signal_summary_query, {"run_ids": run_ids}).fetchone()
        
        print(f"📊 信号总数: {summary.total_signals}")
        print(f"   ✅ 接受: {summary.accepted} ({summary.accepted/summary.total_signals*100:.1f}%)")
        print(f"   ❌ 拒绝: {summary.rejected} ({summary.rejected/summary.total_signals*100:.1f}%)")
        print(f"   📌 涉及标的: {summary.symbol_count}")
        print(f"   🎯 信号类型: {summary.signal_types}\n")
        
        # 按信号类型统计
        signal_type_query = text("""
            SELECT 
                signal_code,
                COUNT(*) as count,
                SUM(CASE WHEN accepted THEN 1 ELSE 0 END) as accepted
            FROM bt_signals
            WHERE run_id = ANY(:run_ids)
            GROUP BY signal_code
            ORDER BY count DESC
        """)
        
        signal_types = conn.execute(signal_type_query, {"run_ids": run_ids}).fetchall()
        
        print("📋 信号类型分布:")
        for st in signal_types:
            print(f"   • {st.signal_code:30s}: {st.count:3d} 个 (接受: {st.accepted:3d}, {st.accepted/st.count*100:5.1f}%)")
        
        # =================================================================
        # 2. 信号表现分析 - 按信号类型
        # =================================================================
        print(f"\n{'='*100}")
        print("2️⃣  信号表现分析 - 各时间窗口收益率")
        print(f"{'='*100}\n")
        
        # 获取所有信号相关的指标
        metrics_by_signal_query = text("""
            SELECT 
                metric_code,
                AVG(metric_value) as avg_val,
                MIN(metric_value) as min_val,
                MAX(metric_value) as max_val,
                STDDEV(metric_value) as std_val,
                COUNT(*) as sample_count
            FROM bt_metrics_total
            WHERE run_id = ANY(:run_ids)
              AND metric_code LIKE '%RET_SIG_%'
            GROUP BY metric_code
            ORDER BY metric_code
        """)
        
        metrics = conn.execute(metrics_by_signal_query, {"run_ids": run_ids}).fetchall()
        
        # 按信号类型组织数据
        signal_metrics = defaultdict(lambda: defaultdict(dict))
        
        for m in metrics:
            # 解析 metric_code: RET_SIG_OPEN_CHASE_BUY_5M
            parts = m.metric_code.split('_')
            if 'SIG_SIG' in m.metric_code:
                # RET_SIG_SIG_OPEN_CHASE_BUY_TFE_MEAN
                sig_name = '_'.join(parts[3:-2])  # OPEN_CHASE_BUY
                time_window = parts[-2] + '_' + parts[-1]  # TFE_MEAN
            else:
                # RET_SIG_OPEN_CHASE_BUY_5M
                sig_name = '_'.join(parts[2:-1])  # OPEN_CHASE_BUY
                time_window = parts[-1]  # 5M
            
            signal_metrics[sig_name][time_window] = {
                'avg': float(m.avg_val) if m.avg_val else 0,
                'min': float(m.min_val) if m.min_val else 0,
                'max': float(m.max_val) if m.max_val else 0,
                'std': float(m.std_val) if m.std_val else 0,
                'count': m.sample_count
            }
        
        # 获取命中率数据
        hit_rate_query = text("""
            SELECT 
                metric_code,
                AVG(metric_value) as avg_val,
                COUNT(*) as sample_count
            FROM bt_metrics_total
            WHERE run_id = ANY(:run_ids)
              AND metric_code LIKE '%HIT_SIG_%'
            GROUP BY metric_code
            ORDER BY metric_code
        """)
        
        hit_rates = conn.execute(hit_rate_query, {"run_ids": run_ids}).fetchall()
        
        for h in hit_rates:
            parts = h.metric_code.split('_')
            if 'SIG_SIG' in h.metric_code:
                sig_name = '_'.join(parts[3:-2])
                time_window = parts[-2] + '_' + parts[-1]
            else:
                sig_name = '_'.join(parts[2:-1])
                time_window = parts[-1]
            
            if time_window not in signal_metrics[sig_name]:
                signal_metrics[sig_name][time_window] = {}
            signal_metrics[sig_name][time_window]['hit_rate'] = float(h.avg_val) if h.avg_val else 0
            signal_metrics[sig_name][time_window]['hit_count'] = h.sample_count
        
        # 显示各信号类型的表现
        for sig_name in sorted(signal_metrics.keys()):
            print(f"{'─'*100}")
            print(f"🎯 {sig_name} 信号表现")
            print(f"{'─'*100}")
            
            metrics_data = signal_metrics[sig_name]
            
            # 获取信号数量
            count_metric = f"COUNT_EXEC_SIG_{sig_name}"
            count_query = text("""
                SELECT AVG(metric_value) as avg_count
                FROM bt_metrics_total
                WHERE run_id = ANY(:run_ids)
                  AND metric_code = :metric_code
            """)
            count_result = conn.execute(count_query, {
                "run_ids": run_ids,
                "metric_code": count_metric
            }).fetchone()
            
            if count_result and count_result.avg_count:
                print(f"📊 平均信号数: {float(count_result.avg_count):.1f} 个/标的\n")
            
            # 时间窗口表现表格
            time_windows = ['5M', '15M', '30M', '60M']
            
            print(f"{'时间窗口':<10} {'平均收益率':>12} {'胜率':>10} {'最大收益':>12} {'最小收益':>12} {'标准差':>10}")
            print(f"{'-'*80}")
            
            for tw in time_windows:
                if tw in metrics_data:
                    data = metrics_data[tw]
                    avg_ret = data.get('avg', 0)
                    hit = data.get('hit_rate', 0)
                    max_ret = data.get('max', 0)
                    min_ret = data.get('min', 0)
                    std = data.get('std', 0)
                    
                    # 用颜色标记正负收益
                    ret_symbol = '🟢' if avg_ret > 0 else '🔴' if avg_ret < 0 else '⚪'
                    
                    print(f"{tw:<10} {ret_symbol} {avg_ret:>10.2%} {hit:>10.1%} {max_ret:>11.2%} {min_ret:>11.2%} {std:>10.2%}")
            
            # 信号到平仓（TFE）表现
            tfe_metrics = ['TFE_MEAN', 'TFE_P50', 'TFE_P90']
            has_tfe = any(tm in metrics_data for tm in tfe_metrics)
            
            if has_tfe:
                print(f"\n💰 信号持有期表现 (从买入到卖出):")
                print(f"{'指标':<15} {'收益率':>12} {'胜率':>10}")
                print(f"{'-'*40}")
                
                for tm in tfe_metrics:
                    if tm in metrics_data:
                        data = metrics_data[tm]
                        avg_ret = data.get('avg', 0)
                        hit = data.get('hit_rate', 0)
                        ret_symbol = '🟢' if avg_ret > 0 else '🔴' if avg_ret < 0 else '⚪'
                        print(f"{tm:<15} {ret_symbol} {avg_ret:>10.2%} {hit:>10.1%}")
                
                # 显示持有时长
                dur_metric = f"DUR_SIG_SIG_{sig_name}_TFE_MIN_MEAN"
                dur_query = text("""
                    SELECT AVG(metric_value) as avg_dur
                    FROM bt_metrics_total
                    WHERE run_id = ANY(:run_ids)
                      AND metric_code = :metric_code
                """)
                dur_result = conn.execute(dur_query, {
                    "run_ids": run_ids,
                    "metric_code": dur_metric
                }).fetchone()
                
                if dur_result and dur_result.avg_dur:
                    avg_dur = float(dur_result.avg_dur)
                    hours = int(avg_dur // 60)
                    mins = int(avg_dur % 60)
                    print(f"\n⏱️  平均持有时长: {avg_dur:.0f} 分钟 ({hours}小时{mins}分钟)")
            
            print()
        
        # =================================================================
        # 3. 标的排名
        # =================================================================
        print(f"{'='*100}")
        print("3️⃣  标的表现排名")
        print(f"{'='*100}\n")
        
        symbol_performance = []
        
        for run in runs:
            symbol = run.parameters.get('symbol')
            run_id = run.run_id
            
            # 获取该标的的所有 TFE 收益指标
            perf_query = text("""
                SELECT metric_code, metric_value
                FROM bt_metrics_total
                WHERE run_id = :run_id
                  AND metric_code LIKE '%TFE_MEAN'
                  AND metric_code LIKE '%RET_SIG_%'
            """)
            
            perf_metrics = conn.execute(perf_query, {"run_id": run_id}).fetchall()
            
            total_return = 0
            signal_count = 0
            
            for pm in perf_metrics:
                if pm.metric_value:
                    total_return += float(pm.metric_value)
                    signal_count += 1
            
            avg_return = total_return / signal_count if signal_count > 0 else 0
            
            # 获取信号总数
            sig_count_query = text("""
                SELECT COUNT(*) as cnt
                FROM bt_signals
                WHERE run_id = :run_id
                  AND accepted = true
            """)
            sig_cnt = conn.execute(sig_count_query, {"run_id": run_id}).fetchone()
            
            symbol_performance.append({
                'symbol': symbol,
                'avg_return': avg_return,
                'signal_count': sig_cnt.cnt,
                'total_return': total_return
            })
        
        # 按平均收益排序
        symbol_performance.sort(key=lambda x: x['avg_return'], reverse=True)
        
        print(f"{'排名':<6} {'标的':<8} {'平均信号收益率':>15} {'信号数':>10} {'累计收益率':>15}")
        print(f"{'-'*70}")
        
        for idx, sp in enumerate(symbol_performance, 1):
            symbol_icon = '🥇' if idx == 1 else '🥈' if idx == 2 else '🥉' if idx == 3 else '  '
            ret_icon = '🟢' if sp['avg_return'] > 0 else '🔴' if sp['avg_return'] < 0 else '⚪'
            
            print(f"{symbol_icon} {idx:<3} {sp['symbol']:<8} {ret_icon} {sp['avg_return']:>13.2%} {sp['signal_count']:>10} {sp['total_return']:>14.2%}")
        
        # =================================================================
        # 4. 最佳/最差信号案例
        # =================================================================
        print(f"\n{'='*100}")
        print("4️⃣  信号案例分析")
        print(f"{'='*100}\n")
        
        # 查看具体的信号数据
        signal_details_query = text("""
            SELECT 
                s.symbol,
                s.ts_end,
                s.signal_code,
                s.accepted,
                s.reason
            FROM bt_signals s
            WHERE s.run_id = ANY(:run_ids)
              AND s.accepted = true
            ORDER BY s.ts_end
            LIMIT 20
        """)
        
        signal_details = conn.execute(signal_details_query, {"run_ids": run_ids}).fetchall()
        
        print("📋 信号时间线 (前20条):\n")
        print(f"{'时间':<20} {'标的':<8} {'信号类型':<30} {'状态':<8}")
        print(f"{'-'*80}")
        
        for sd in signal_details:
            status = '✅' if sd.accepted else '❌'
            print(f"{str(sd.ts_end):<20} {sd.symbol:<8} {sd.signal_code:<30} {status:<8}")
        
        # =================================================================
        # 5. 策略建议
        # =================================================================
        print(f"\n{'='*100}")
        print("5️⃣  策略优化建议")
        print(f"{'='*100}\n")
        
        # 分析最优时间窗口
        best_windows = {}
        for sig_name, metrics_data in signal_metrics.items():
            best_ret = -999
            best_window = None
            
            for tw in ['5M', '15M', '30M', '60M']:
                if tw in metrics_data:
                    ret = metrics_data[tw].get('avg', 0)
                    if ret > best_ret:
                        best_ret = ret
                        best_window = tw
            
            if best_window:
                best_windows[sig_name] = (best_window, best_ret)
        
        print("⏱️  最佳持有时间窗口:")
        for sig_name, (window, ret) in best_windows.items():
            ret_icon = '🟢' if ret > 0 else '🔴'
            print(f"   • {sig_name:30s}: {window:5s} ({ret_icon} {ret:>7.2%})")
        
        # 整体表现评估
        print(f"\n📊 整体策略评估:")
        
        total_avg_return = sum(sp['avg_return'] for sp in symbol_performance) / len(symbol_performance)
        win_rate = sum(1 for sp in symbol_performance if sp['avg_return'] > 0) / len(symbol_performance)
        
        print(f"   • 平均收益率: {total_avg_return:>7.2%}")
        print(f"   • 盈利标的占比: {win_rate:>7.1%}")
        print(f"   • 盈利标的数: {sum(1 for sp in symbol_performance if sp['avg_return'] > 0)}/{len(symbol_performance)}")
        
        if total_avg_return > 0:
            print(f"\n✅ 策略整体表现为正，建议继续优化")
        else:
            print(f"\n⚠️  策略整体表现为负，需要重大调整")
        
        print(f"\n💡 优化方向:")
        print(f"   1. 关注表现优异的标的特征（如 {symbol_performance[0]['symbol']}）")
        print(f"   2. 优化持有时间窗口，选择最佳退出点")
        print(f"   3. 分析亏损标的的共同特征，加强过滤条件")
        print(f"   4. 考虑动态止盈止损策略")
        
        print(f"\n{'='*100}")
        print("✅ 分析完成")
        print(f"{'='*100}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        batch_id = sys.argv[1]
    else:
        batch_id = "bt-20251002-20251007-3d379d"
    
    analyze_signal_performance(batch_id)

