#!/usr/bin/env python3
"""
真实数据环境的回测 Smoke 测试脚本
使用 TimescaleDB 中的真实 IBKR 数据进行回测验证
"""

import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import structlog
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from apps.backtest.api import app as backtest_api_app
from apps.backtest.bt_runner import run_backtest
from apps.backtest.dao import BacktestDAO
from apps.backtest.strategy.option_signal_strategy import ContractSelection
from libs.core import configure_logging, get_settings
from libs.infra.db import get_session_factory
from libs.schemas.signals import SignalSide

LOGGER = structlog.get_logger(__name__)


@dataclass
class DataDiagnosis:
    """数据诊断结果"""
    has_equity_data: bool
    has_indicator_data: bool  
    has_option_chain_data: bool
    has_option_bar_data: bool
    equity_symbols: List[str]
    equity_date_range: Optional[Tuple[date, date]]
    option_trade_dates: List[date]
    total_equity_bars: int
    total_option_chains: int
    diagnostic_message: str


def _validate_environment() -> None:
    """验证环境变量"""
    required_env = [
        "DATABASE_URL",
        "REDIS_URL", 
        "IB_HOST",
        "IB_PORT",
        "IB_CLIENT_ID",
        "IB_ACCOUNT",
    ]
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"❌ 缺少必要环境变量: {', '.join(sorted(missing))}")
    
    LOGGER.info("✅ 环境变量验证通过")


def _diagnose_data_availability(session: Session, settings) -> DataDiagnosis:
    """诊断数据可用性"""
    LOGGER.info("🔍 开始诊断 TimescaleDB 数据可用性...")
    
    # 检查股票数据
    equity_count_sql = text("SELECT COUNT(*) FROM bars1m_equity")
    total_equity_bars = session.execute(equity_count_sql).scalar() or 0
    
    # 检查技术指标数据
    indicator_count_sql = text("SELECT COUNT(*) FROM indicators_eq_1m")  
    total_indicators = session.execute(indicator_count_sql).scalar() or 0
    
    # 检查期权链数据
    option_chain_count_sql = text(f"SELECT COUNT(*) FROM {settings.option_chain_table}")
    total_option_chains = session.execute(option_chain_count_sql).scalar() or 0
    
    # 检查期权Bar数据
    option_bar_count_sql = text(f"SELECT COUNT(*) FROM {settings.option_bar_table}")
    total_option_bars = session.execute(option_bar_count_sql).scalar() or 0
    
    # 获取可用股票列表
    equity_symbols = []
    if total_equity_bars > 0:
        symbols_sql = text("SELECT DISTINCT symbol FROM bars1m_equity ORDER BY symbol LIMIT 20")
        equity_symbols = [row[0] for row in session.execute(symbols_sql).fetchall()]
    
    # 获取股票数据日期范围
    equity_date_range = None
    if total_equity_bars > 0:
        date_range_sql = text("""
            SELECT MIN(DATE(ts_end)) as min_date, MAX(DATE(ts_end)) as max_date 
            FROM bars1m_equity
        """)
        result = session.execute(date_range_sql).fetchone()
        if result and result[0]:
            equity_date_range = (result[0], result[1])
    
    # 获取期权链交易日
    option_trade_dates = []
    if total_option_chains > 0:
        dates_sql = text(f"""
            SELECT DISTINCT trade_date 
            FROM {settings.option_chain_table} 
            ORDER BY trade_date DESC 
            LIMIT 10
        """)
        option_trade_dates = [row[0] for row in session.execute(dates_sql).fetchall()]
    
    # 生成诊断信息
    diagnosis = DataDiagnosis(
        has_equity_data=total_equity_bars > 0,
        has_indicator_data=total_indicators > 0,
        has_option_chain_data=total_option_chains > 0, 
        has_option_bar_data=total_option_bars > 0,
        equity_symbols=equity_symbols,
        equity_date_range=equity_date_range,
        option_trade_dates=option_trade_dates,
        total_equity_bars=total_equity_bars,
        total_option_chains=total_option_chains,
        diagnostic_message=""
    )
    
    # 生成诊断报告
    if not diagnosis.has_equity_data:
        diagnosis.diagnostic_message = (
            "❌ 未发现股票1分钟数据 (bars1m_equity)\n"
            "   可能原因:\n"
            "   1. IBKR Market Data Gateway 未运行\n"
            "   2. 数据源未配置或连接失败\n" 
            "   3. 数据落库服务未启动\n"
            "   建议: 检查 md_gw 服务状态和日志"
        )
    elif not diagnosis.has_indicator_data:
        diagnosis.diagnostic_message = (
            "❌ 未发现技术指标数据 (indicators_eq_1m)\n"
            "   股票数据存在但缺少技术指标\n"
            "   建议: 检查指标计算服务状态"
        )
    elif not diagnosis.has_option_chain_data:
        diagnosis.diagnostic_message = (
            f"❌ 未发现期权链数据 ({settings.option_chain_table})\n"
            "   可能原因:\n"
            "   1. 期权链获取服务未运行\n"
            "   2. IBKR 权限不足（需要期权数据权限）\n"
            "   建议: 检查期权数据获取服务和IBKR权限"
        )
    elif not diagnosis.has_option_bar_data:
        diagnosis.diagnostic_message = (
            f"❌ 未发现期权Bar数据 ({settings.option_bar_table})\n"
            "   期权链存在但缺少价格数据\n" 
            "   建议: 检查期权实时数据订阅服务"
        )
    else:
        diagnosis.diagnostic_message = (
            f"✅ 数据完整性良好\n"
            f"   股票数据: {total_equity_bars:,} 条\n"
            f"   期权链: {total_option_chains:,} 条\n"
            f"   可用股票: {len(equity_symbols)} 个\n"
            f"   日期范围: {diagnosis.equity_date_range}"
        )
    
    return diagnosis


def _find_viable_backtest_window(
    session: Session, 
    settings, 
    diagnosis: DataDiagnosis
) -> Optional[Tuple[str, datetime, datetime]]:
    """寻找可行的回测窗口"""
    if not diagnosis.has_equity_data or not diagnosis.has_option_chain_data:
        return None
        
    LOGGER.info("🔍 寻找可行的回测窗口...")
    
    # 寻找同时有股票数据和期权链数据的交易日
    sql = text(f"""
        WITH equity_dates AS (
            SELECT DISTINCT DATE(ts_end) as trade_date, symbol
            FROM bars1m_equity 
            WHERE DATE(ts_end) >= CURRENT_DATE - INTERVAL '30 days'
        ),
        option_dates AS (
            SELECT DISTINCT trade_date, underlying_symbol
            FROM {settings.option_chain_table}
            WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
            AND bid IS NOT NULL AND ask IS NOT NULL
            AND open_interest >= 100 AND volume >= 50
        )
        SELECT 
            e.trade_date,
            e.symbol,
            COUNT(*) as equity_bars,
            COUNT(DISTINCT o.underlying_symbol) as option_symbols
        FROM equity_dates e
        INNER JOIN option_dates o ON e.trade_date = o.trade_date AND e.symbol = o.underlying_symbol
        INNER JOIN bars1m_equity be ON e.symbol = be.symbol AND DATE(be.ts_end) = e.trade_date
        GROUP BY e.trade_date, e.symbol
        HAVING COUNT(*) >= 300  -- 至少5小时的数据
        ORDER BY e.trade_date DESC, COUNT(*) DESC
        LIMIT 5
    """)
    
    results = session.execute(sql).fetchall()
    
    if not results:
        LOGGER.warning("❌ 未找到可行的回测窗口")
        return None
    
    # 选择第一个结果
    trade_date, symbol, equity_bars, option_symbols = results[0]
    
    # 构建回测时间窗口 (使用ETF市场时间)
    start_dt = datetime.combine(trade_date, datetime.min.time().replace(hour=9, minute=30))
    end_dt = datetime.combine(trade_date, datetime.min.time().replace(hour=16, minute=0))
    
    LOGGER.info(
        "✅ 找到可行回测窗口",
        symbol=symbol,
        trade_date=trade_date.isoformat(),
        equity_bars=equity_bars,
        start_time=start_dt.isoformat(),
        end_time=end_dt.isoformat(),
    )
    
    return symbol, start_dt, end_dt


def _get_viable_contracts(
    dao: BacktestDAO,
    symbol: str, 
    trade_date: date
) -> List[ContractSelection]:
    """获取可行的期权合约"""
    LOGGER.info("🔍 获取期权合约", symbol=symbol, trade_date=trade_date.isoformat())
    
    contracts = []
    for option_right in ("CALL", "PUT"):
        candidates = dao.fetch_option_candidates(
            trade_date=trade_date,
            underlying_symbol=symbol,
            option_right=option_right, 
            dte_min=1,  # 放宽条件
            dte_max=14,
        )
        
        # 过滤并选择最佳合约
        viable = []
        for candidate in candidates:
            if None in (candidate.bid, candidate.ask, candidate.mid, candidate.min_tick):
                continue
            if candidate.bid <= 0 or candidate.ask <= candidate.bid:
                continue
            if (candidate.open_interest or 0) < 50:  # 降低门槛
                continue
                
            spread = Decimal(str(candidate.ask)) - Decimal(str(candidate.bid))
            mid = Decimal(str(candidate.mid))
            if spread > mid * Decimal("0.10"):  # 10%以内的价差
                continue
                
            viable.append(candidate)
        
        if viable:
            # 选择流动性最好的合约（按OI+Volume排序）
            best = max(viable, key=lambda x: (x.open_interest or 0) + (x.volume or 0))
            
            expiry = best.expiry
            if not isinstance(expiry, datetime):
                expiry = datetime.combine(expiry, datetime.min.time())
                
            contracts.append(ContractSelection(
                conid=best.conid,
                symbol=symbol,
                expiry=expiry,
                strike=Decimal(str(best.strike)),
                right=best.option_right, 
                bid=Decimal(str(best.bid)),
                ask=Decimal(str(best.ask)),
                mid=Decimal(str(best.mid)),
                min_tick=Decimal(str(best.min_tick)),
                option_right=best.option_right,
                open_interest=int(best.open_interest or 0),
                volume=int(best.volume or 0),
                dte=int(best.dte or 0),
            ))
    
    LOGGER.info("✅ 获取到合约", count=len(contracts))
    for contract in contracts:
        LOGGER.info(
            "合约详情", 
            right=contract.option_right,
            strike=float(contract.strike),
            dte=contract.dte,
            bid=float(contract.bid),
            ask=float(contract.ask),
            oi=contract.open_interest,
            volume=contract.volume,
        )
    
    return contracts


def _run_backtest_with_diagnostics(
    symbol: str,
    start_dt: datetime, 
    end_dt: datetime,
    contracts: List[ContractSelection]
) -> int:
    """运行回测并提供诊断信息"""
    LOGGER.info("🚀 开始执行回测", symbol=symbol, start=start_dt.isoformat(), end=end_dt.isoformat())
    
    # 创建预选合约的option_selector
    def create_preselected_option_selector(contracts: List[ContractSelection]):
        contract_map = {contract.option_right: contract for contract in contracts}
        
        def preselected_selector(signal, ts: datetime):
            option_right = "CALL" if signal.side == SignalSide.BUY else "PUT"
            selected = contract_map.get(option_right)
            if selected:
                LOGGER.info("选择合约", right=option_right, strike=float(selected.strike))
            return selected
        
        return preselected_selector
    
    option_selector = create_preselected_option_selector(contracts)
    
    try:
        run_id = run_backtest(
            symbol=symbol,
            start=start_dt,
            end=end_dt,
            signal_mode="recompute",
            risk_mode="inproc", 
            option_selector=option_selector,
        )
        LOGGER.info("✅ 回测执行成功", run_id=run_id)
        return run_id
        
    except Exception as e:
        LOGGER.error("❌ 回测执行失败", error=str(e), error_type=type(e).__name__)
        raise


def _verify_results(run_id: int, session: Session) -> Dict:
    """验证回测结果"""
    LOGGER.info("🔍 验证回测结果", run_id=run_id)
    
    # 检查回测记录
    run_sql = text("SELECT * FROM bt_runs WHERE run_id = :run_id")
    run_record = session.execute(run_sql, {"run_id": run_id}).fetchone()
    
    if not run_record:
        raise RuntimeError(f"❌ 未找到回测记录 run_id={run_id}")
    
    # 检查交易记录
    trades_sql = text("SELECT COUNT(*) FROM bt_trades WHERE run_id = :run_id")
    trade_count = session.execute(trades_sql, {"run_id": run_id}).scalar() or 0
    
    # 检查信号记录  
    signals_sql = text("SELECT COUNT(*) FROM bt_signals WHERE run_id = :run_id")
    signal_count = session.execute(signals_sql, {"run_id": run_id}).scalar() or 0
    
    # 检查指标记录
    metrics_sql = text("SELECT COUNT(*) FROM bt_metrics_total WHERE run_id = :run_id")
    metrics_count = session.execute(metrics_sql, {"run_id": run_id}).scalar() or 0
    
    results = {
        "run_id": run_id,
        "status": run_record.status,
        "trade_count": trade_count,
        "signal_count": signal_count, 
        "metrics_count": metrics_count,
    }
    
    LOGGER.info("✅ 结果验证完成", **results)
    return results


def _test_api_access(run_id: int) -> None:
    """测试API访问"""
    LOGGER.info("🔍 测试回测API访问", run_id=run_id)
    
    client = TestClient(backtest_api_app)
    
    # 测试运行列表API
    runs_resp = client.get("/runs", params={"limit": 10})
    if runs_resp.status_code != 200:
        raise RuntimeError(f"❌ /runs API失败: {runs_resp.status_code}")
    
    # 测试指标API
    metrics_resp = client.get(f"/metrics/{run_id}")  
    if metrics_resp.status_code != 200:
        raise RuntimeError(f"❌ /metrics/{run_id} API失败: {metrics_resp.status_code}")
    
    metrics_data = metrics_resp.json()
    if not metrics_data.get("ok"):
        raise RuntimeError(f"❌ /metrics/{run_id} 返回错误: {metrics_data}")
    
    LOGGER.info("✅ API访问测试通过")


def main() -> int:
    """主函数"""
    settings = get_settings()
    configure_logging(settings)
    
    LOGGER.info("🚀 开始真实数据环境回测 Smoke 测试")
    LOGGER.info("=" * 60)
    
    try:
        # 1. 环境验证
        _validate_environment()
        
        # 2. 数据诊断
        session_factory = get_session_factory(settings)
        with session_factory() as session:
            dao = BacktestDAO(
                session,
                option_bar_table=settings.option_bar_table,
                option_chain_table=settings.option_chain_table,
            )
            
            diagnosis = _diagnose_data_availability(session, settings)
            
            print("\n📊 数据可用性诊断:")
            print(diagnosis.diagnostic_message)
            
            if not (diagnosis.has_equity_data and diagnosis.has_option_chain_data):
                print("\n💡 解决建议:")
                print("1. 启动 IBKR Gateway 或 TWS")
                print("2. 运行市场数据服务: make run-rt") 
                print("3. 运行期权链获取: python scripts/get_top5_option_chains.py")
                print("4. 等待数据落库后重新运行测试")
                return 1
            
            # 3. 寻找可行回测窗口
            window = _find_viable_backtest_window(session, settings, diagnosis)
            if not window:
                print("\n❌ 未找到可行的回测窗口")
                print("   需要同时具备股票数据和期权链数据的交易日")
                return 1
                
            symbol, start_dt, end_dt = window
            
            # 4. 获取期权合约
            contracts = _get_viable_contracts(dao, symbol, start_dt.date())
            if not contracts:
                print(f"\n❌ 未找到 {symbol} 的可行期权合约")
                return 1
        
        # 5. 执行回测
        run_id = _run_backtest_with_diagnostics(symbol, start_dt, end_dt, contracts)
        
        # 6. 验证结果
        with session_factory() as session:
            results = _verify_results(run_id, session)
        
        # 7. 测试API
        _test_api_access(run_id)
        
        # 8. 输出成功总结
        print("\n" + "=" * 60)
        print("🎉 Smoke 测试成功完成!")
        print(f"✅ 回测运行ID: {run_id}")
        print(f"✅ 测试标的: {symbol}")
        print(f"✅ 测试窗口: {start_dt.strftime('%Y-%m-%d %H:%M')} - {end_dt.strftime('%H:%M')}")
        print(f"✅ 交易记录: {results['trade_count']} 笔")
        print(f"✅ 信号记录: {results['signal_count']} 个")
        print(f"✅ 性能指标: {results['metrics_count']} 项")
        print("✅ API查询: 正常")
        
        return 0
        
    except Exception as e:
        LOGGER.error("❌ Smoke测试失败", error=str(e), error_type=type(e).__name__)
        print(f"\n❌ 测试失败: {e}")
        
        import traceback
        print("\n🔍 详细错误信息:")
        traceback.print_exc()
        
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


