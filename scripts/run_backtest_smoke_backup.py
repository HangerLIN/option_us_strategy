# path: scripts/run_backtest_smoke.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Sequence

import structlog
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from apps.backtest.api import app as backtest_api_app
from apps.backtest.bt_runner import run_backtest
from apps.backtest.dao import BacktestDAO, OptionCandidateRow
from apps.backtest.datafeed.timescale_option import TimescaleOptionData
from apps.backtest.strategy.option_signal_strategy import ContractSelection
from libs.core import configure_logging, get_settings, to_utc, trading_session_window
from libs.schemas.signals import SignalSide
from libs.infra.db import get_session_factory

LOGGER = structlog.get_logger(__name__)


@dataclass(frozen=True)
class TradeSession:
    trade_date: date
    start_et: datetime
    end_et: datetime
    start_utc: datetime
    end_utc: datetime


@dataclass
class SymbolSelection:
    symbol: str
    contracts: List[ContractSelection]
    run_id: int | None = None


def _validate_environment() -> None:
    required_env = [
            "DATABASE_URL",
            "REDIS_URL",
        "IB_ACCOUNT",
        "IB_CLIENT_ID",
        "IB_HOST",
        "IB_PORT",
    ]
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"缺少必要环境变量: {', '.join(sorted(missing))}")


def _ensure_tables(dao: BacktestDAO, option_tables: Sequence[str]) -> None:
    required = [
        "bars1m_equity",
        "indicators_eq_1m",
        "bt_runs",
        "bt_trades",
        "bt_signals",
        "bt_metrics_daily",
        "bt_metrics_total",
        "premarket_top5",
        "dim_trading_calendar",
    ]
    required.extend(option_tables)
    dao.ensure_tables(required)


def _latest_trade_session(session: Session, option_chain_table: str) -> TradeSession:
    sql = text(
        f"""
        SELECT trade_date
        FROM {option_chain_table}
        WHERE bid IS NOT NULL
          AND ask IS NOT NULL
          AND min_tick IS NOT NULL
          AND open_interest >= 500
          AND volume >= 100
          AND dte BETWEEN 2 AND 7
        ORDER BY trade_date DESC
        LIMIT 1
        """
    )
    trade_date = session.execute(sql).scalar()
    if trade_date is None:
        raise RuntimeError("未找到满足条件的期权链交易日")
    rth_start_et, rth_end_et = trading_session_window(trade_date)
    start_utc = to_utc(rth_start_et)
    end_utc = to_utc(rth_end_et)
    return TradeSession(
        trade_date=trade_date,
        start_et=rth_start_et,
        end_et=rth_end_et,
        start_utc=start_utc,
        end_utc=end_utc,
    )


def _symbol_rth_coverage(
    session: Session,
    symbol: str,
    start_utc: datetime,
    end_utc: datetime,
) -> bool:
    sql = text(
        """
        SELECT COUNT(*) AS bars
        FROM bars1m_equity
        WHERE symbol = :symbol
          AND ts_end >= :start_utc
          AND ts_end < :end_utc
        """
    )
    row = session.execute(
        sql,
        {
            "symbol": symbol,
            "start_utc": start_utc,
            "end_utc": end_utc,
        },
    ).one()
    total_minutes = int((end_utc - start_utc).total_seconds() / 60)
    return row.bars >= total_minutes


def _top_symbols_from_premarket(session: Session, trade_date: date) -> List[str]:
    sql = text(
                """
                SELECT symbol
                FROM premarket_top5
                WHERE trade_date = :trade_date
        ORDER BY rank ASC
        """
    )
    rows = session.execute(sql, {"trade_date": trade_date}).scalars().all()
    return [row.upper() for row in rows]


def _top_symbols_by_volume(
    session: Session, start_utc: datetime, end_utc: datetime, limit: int
) -> List[str]:
    sql = text(
                """
                SELECT symbol
        FROM (
            SELECT symbol, SUM(volume) AS vol
            FROM bars1m_equity
            WHERE ts_end >= :start_utc
              AND ts_end < :end_utc
            GROUP BY symbol
        ) AS agg
        ORDER BY agg.vol DESC
        LIMIT :limit
        """
    )
    rows = session.execute(
        sql,
        {"start_utc": start_utc, "end_utc": end_utc, "limit": limit},
    ).scalars()
    return [row.upper() for row in rows]


def _filter_option_candidates(
    candidates: Sequence[OptionCandidateRow],
    *,
    symbol: str,
    option_right: str,
) -> List[ContractSelection]:
    filtered: List[ContractSelection] = []
    for candidate in candidates:
        bid = candidate.bid
        ask = candidate.ask
        mid = candidate.mid
        oi = candidate.open_interest
        vol = candidate.volume
        min_tick = candidate.min_tick
        strike_value = candidate.strike
        expiry = candidate.expiry
        if None in (bid, ask, mid, oi, vol, min_tick, strike_value, expiry):
            continue
        bid_d = Decimal(str(bid))
        ask_d = Decimal(str(ask))
        mid_d = Decimal(str(mid))
        spread = ask_d - bid_d
        if ask_d <= bid_d or spread <= 0:
            continue
        if Decimal(str(oi)) < Decimal("500") or Decimal(str(vol)) < Decimal("100"):
            continue
        threshold = max(Decimal("0.10"), mid_d * Decimal("0.05"))
        if spread > threshold:
            continue
        if not isinstance(expiry, datetime):
            expiry = datetime.combine(expiry, datetime.min.time(), tzinfo=timezone.utc)
        try:
            conid = int(candidate.conid) if candidate.conid is not None else None
            oi_int = int(candidate.open_interest or 0)
            vol_int = int(candidate.volume or 0)
            dte_int = int(candidate.dte or 0)
        except (TypeError, ValueError):
            continue
        filtered.append(
            ContractSelection(
                conid=conid,
                symbol=symbol,
                expiry=expiry,
                strike=Decimal(str(strike_value)),
                right=option_right,
                bid=bid_d,
                ask=ask_d,
                mid=mid_d,
                min_tick=Decimal(str(min_tick)),
                option_right=option_right,
                open_interest=oi_int,
                volume=vol_int,
                dte=dte_int,
            )
        )
    filtered.sort(key=lambda item: (item.ask - item.bid, item.mid))
    return filtered


def _select_contracts_for_symbol(
    dao: BacktestDAO,
    symbol: str,
    session: TradeSession,
) -> List[ContractSelection]:
    contracts: List[ContractSelection] = []
    for option_right in ("CALL", "PUT"):
        candidates = dao.fetch_option_candidates(
            trade_date=session.trade_date,
            underlying_symbol=symbol,
            option_right=option_right,
            dte_min=2,
            dte_max=7,
        )
        filtered = _filter_option_candidates(
            candidates,
            symbol=symbol,
            option_right=option_right,
        )
        if not filtered:
            continue
        contract = filtered[0]
        TimescaleOptionData.from_timescale(
            dao=dao,
            contract={
                "conid": contract.conid,
                "symbol": symbol,
                "expiry": contract.expiry,
                "strike": contract.strike,
                "right": contract.option_right,
            },
            start=session.start_utc,
            end=session.end_utc + timedelta(minutes=1),
        )
        contracts.append(contract)
    if not contracts:
        raise RuntimeError(f"{symbol} 在 {session.trade_date} 未匹配到期权合约")
    return contracts


def _pick_symbols(
    raw_symbols: Sequence[str],
    *,
    session: Session,
    trade_session: TradeSession,
    dao: BacktestDAO,
    max_symbols: int,
) -> List[SymbolSelection]:
    selections: List[SymbolSelection] = []
    for symbol in raw_symbols:
        if len(selections) >= max_symbols:
            break
        if not _symbol_rth_coverage(
            session,
            symbol,
            trade_session.start_utc,
            trade_session.end_utc + timedelta(minutes=1),
        ):
            LOGGER.warning("symbol.rth_incomplete", symbol=symbol)
            continue
        try:
            contracts = _select_contracts_for_symbol(dao, symbol, trade_session)
        except Exception as exc:
            LOGGER.warning("symbol.option_unavailable", symbol=symbol, error=str(exc))
            continue
        selections.append(SymbolSelection(symbol=symbol, contracts=contracts))
    if not selections:
        raise RuntimeError("未找到可用于回测的标的")
    return selections


def _collect_signal_stats(session: Session, run_id: int) -> Dict[str, int]:
    sql = text(
                """
                SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE accepted IS TRUE) AS accepted,
            COUNT(*) FILTER (WHERE accepted IS FALSE) AS blocked
                FROM bt_signals
                WHERE run_id = :run_id
                """
            )
    row = session.execute(sql, {"run_id": run_id}).one()
    return {
        "total": int(row.total or 0),
        "accepted": int(row.accepted or 0),
        "blocked": int(row.blocked or 0),
    }


def _collect_trade_stats(session: Session, run_id: int) -> Dict[str, float]:
    sql = text(
                """
                SELECT
            COUNT(*) AS trades,
            COALESCE(SUM(fees), 0) AS fees_total,
            COALESCE(SUM(ABS(price * quantity * 100)), 0) AS gross_notional,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY slippage) AS slip_p50,
            percentile_cont(0.9) WITHIN GROUP (ORDER BY slippage) AS slip_p90
                FROM bt_trades
                WHERE run_id = :run_id
                """
            )
    row = session.execute(sql, {"run_id": run_id}).one()
    gross_notional = Decimal(row.gross_notional or 0)
    fees_total = Decimal(row.fees_total or 0)
    fee_ratio = (fees_total / gross_notional) if gross_notional > 0 else Decimal("0")
    return {
        "trades": float(row.trades or 0),
        "fees_total": float(fees_total),
        "gross_notional": float(gross_notional),
        "fee_ratio": float(fee_ratio),
        "slip_p50": float(row.slip_p50 or 0),
        "slip_p90": float(row.slip_p90 or 0),
    }


def _collect_metrics(session: Session, run_id: int, dao: BacktestDAO) -> Dict[str, float]:
    metrics = {row.metric_code: float(row.metric_value) for row in dao.fetch_metrics_total(run_id)}
    return metrics


def _run_api_checks(run_ids: Iterable[int], trade_session: TradeSession) -> None:
    client = TestClient(backtest_api_app)
    start_iso = trade_session.start_utc.isoformat()
    end_iso = (trade_session.end_utc + timedelta(minutes=1)).isoformat()
    runs_resp = client.get("/runs", params={"start": start_iso, "end": end_iso, "limit": 10})
    if runs_resp.status_code != 200:
        raise RuntimeError(f"/runs 调用失败: {runs_resp.text}")
    runs_payload = runs_resp.json()
    if not runs_payload.get("ok"):
        raise RuntimeError("/runs 返回状态异常")
    available_ids = {item["run_id"] for item in runs_payload.get("data", {}).get("runs", [])}
    missing = [run_id for run_id in run_ids if run_id not in available_ids]
    if missing:
        raise RuntimeError(f"/runs 未找到 run_id: {missing}")
    for run_id in run_ids:
        metrics_resp = client.get(f"/metrics/{run_id}")
        if metrics_resp.status_code != 200:
            raise RuntimeError(f"/metrics/{run_id} 调用失败: {metrics_resp.text}")
        body = metrics_resp.json()
        if not body.get("ok"):
            raise RuntimeError(f"/metrics/{run_id} 返回状态异常")


def _format_decimal(value: float, precision: int = 4) -> str:
    return f"{value:.{precision}f}"


def _format_percentage(value: float) -> str:
    return f"{value * 100:.2f}%"


def main() -> int:
    settings = get_settings()
    configure_logging(settings)
    LOGGER.info("smoke.start")
    _validate_environment()

    session_factory: sessionmaker[Session] = get_session_factory(settings)

    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        _ensure_tables(
            dao,
            option_tables=[settings.option_bar_table, settings.option_chain_table],
        )
        trade_session = _latest_trade_session(session, settings.option_chain_table)

        candidates = _top_symbols_from_premarket(session, trade_session.trade_date)
        if not candidates:
            candidates = _top_symbols_by_volume(
                session,
                trade_session.start_utc,
                trade_session.end_utc + timedelta(minutes=1),
                limit=10,
            )
        selections = _pick_symbols(
            candidates,
            session=session,
            trade_session=trade_session,
            dao=dao,
            max_symbols=2,
        )

    execution_plan: List[SymbolSelection] = selections
    run_ids: List[int] = []
    start_run = trade_session.start_utc
    end_run = trade_session.end_utc + timedelta(minutes=1)

    for item in execution_plan:
        LOGGER.info(
            "smoke.run_backtest",
            symbol=item.symbol,
            contracts=len(item.contracts),
            start=start_run.isoformat(),
            end=end_run.isoformat(),
        )
        
        # 创建使用预选合约的 option_selector
        def create_preselected_option_selector(contracts: List[ContractSelection]):
            contract_map = {contract.option_right: contract for contract in contracts}
            
            def preselected_selector(signal, ts: datetime):
                option_right = "CALL" if signal.side == SignalSide.BUY else "PUT"
                return contract_map.get(option_right)
            
            return preselected_selector
        
        option_selector = create_preselected_option_selector(item.contracts)
        
        run_id = run_backtest(
            symbol=item.symbol,
            start=start_run,
            end=end_run,
            signal_mode="recompute",
            risk_mode="inproc",
            option_selector=option_selector,
        )
        item.run_id = run_id
        run_ids.append(run_id)
        LOGGER.info("smoke.run_backtest_ok", symbol=item.symbol, run_id=run_id)

    with session_factory() as session:
        dao = BacktestDAO(
            session,
        option_bar_table=settings.option_bar_table,
        option_chain_table=settings.option_chain_table,
        )
        summaries: Dict[str, Dict[str, float | int]] = {}
        for item in execution_plan:
            if item.run_id is None:
                continue
            signal_stats = _collect_signal_stats(session, item.run_id)
            trade_stats = _collect_trade_stats(session, item.run_id)
            metrics = _collect_metrics(session, item.run_id, dao)
            summaries[item.symbol] = {
                "run_id": item.run_id,
                "signals": signal_stats["total"],
                "accepted": signal_stats["accepted"],
                "blocked": signal_stats["blocked"],
                "trades": trade_stats["trades"],
                "fees_total": trade_stats["fees_total"],
                "fee_ratio": trade_stats["fee_ratio"],
                "slip_p50": trade_stats["slip_p50"],
                "slip_p90": trade_stats["slip_p90"],
                "sharpe": metrics.get("sharpe", 0.0),
                "max_drawdown": metrics.get("max_drawdown", 0.0),
                "total_pnl": metrics.get("total_pnl", 0.0),
                "contracts": len(item.contracts),
            }

    _run_api_checks(run_ids, trade_session)

    header = (
        f"回测窗口 {trade_session.trade_date} "
        f"[{trade_session.start_et:%H:%M} - {trade_session.end_et:%H:%M} ET] "
        f"({trade_session.start_utc.isoformat()} → {trade_session.end_utc.isoformat()} UTC)"
    )
    print(header)
    for symbol, stats in summaries.items():
        summary_line = (
            f"{symbol}: 合约 {int(stats['contracts'])} "
            f"| 信号 {int(stats['signals'])} "
            f"(通过 {int(stats['accepted'])}, 拒绝 {int(stats['blocked'])}) "
            f"| 成交 {int(stats['trades'])} "
            f"| Sharpe { _format_decimal(float(stats['sharpe'])) } "
            f"| MaxDD { _format_decimal(float(stats['max_drawdown'])) } "
            f"| 滑点P50 { _format_decimal(float(stats['slip_p50'])) },"
            f" P90 { _format_decimal(float(stats['slip_p90'])) } "
            f"| 费用 { _format_percentage(float(stats['fee_ratio'])) } "
            f"| PnL { _format_decimal(float(stats['total_pnl']), 2) } "
            f"| run_id {int(stats['run_id'])}"
        )
        print(summary_line)

    LOGGER.info("smoke.completed", run_ids=run_ids)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
