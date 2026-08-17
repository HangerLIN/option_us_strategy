from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import backtrader as bt
import pandas as pd
import structlog
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.backtest.analyzers.metrics_writer import MetricsWriter
from apps.backtest.broker.commission_ib import IBOptionCommissionInfo
from apps.backtest.broker.quote_aware import QuoteAwareOptionBroker
from apps.backtest.dao import BacktestDAO
from apps.backtest.datafeed.timescale_equity import TimescaleEquityData
from apps.backtest.datafeed.timescale_option import TimescaleOptionData
from apps.backtest.lifecycle_log import BacktestLifecycleLogger, resolve_lifecycle_log_path
from apps.backtest.risk_adapter import RiskCtx, build_risk_precheck
from apps.backtest.signal_source import load_signals
from apps.backtest.strategy.option_signal_strategy import (
    ContractSelection,
    OptionSignalStrategy,
    OptionSelector,
)
from apps.signal_svc.top5_source import Top5Source
from libs.core import EASTERN, get_settings
from libs.schemas.signals import SignalSide

LOGGER = structlog.get_logger(__name__)


def run_backtest(
    *,
    symbol: str,
    start: datetime,
    end: datetime,
    signal_mode: str,
    risk_mode: str,
    track: str = "option",
    option_selector: OptionSelector | None = None,
    option_contracts: Mapping[str, ContractSelection] | None = None,
    top5_source: Top5Source | None = None,
    batch_id: str | None = None,
    universe_code: str | None = None,
    trade_dates: Sequence[date] | None = None,
    strategy_variant: str | None = None,
    option_pricing_mode: str = "full_feed",
    event_log_path: str | Path | None = None,
) -> int:
    settings = get_settings()
    track_mode = (track or "option").lower()
    if track_mode not in {"option", "equity"}:
        raise ValueError(f"Unsupported track '{track}'. Expected 'option' or 'equity'.")

    engine = create_engine(settings.database_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    dao = BacktestDAO(
        session,
        option_bar_table=settings.option_bar_table,
        option_chain_table=settings.option_chain_table,
    )

    required_tables = [
        "bars1m_equity",
        "indicators_eq_1m",
        "bt_runs",
        "bt_trades",
        "bt_signals",
        "bt_order_events",
        "bt_metrics_daily",
        "bt_metrics_total",
        "bt_top5",
    ]
    if track_mode == "option":
        required_tables.append(settings.option_bar_table)
    else:
        required_tables.append("risk_state")
    dao.ensure_tables(required_tables)

    run_params = {
        "symbol": symbol,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "signal_mode": signal_mode,
        "track": track_mode,
        "option_pricing_mode": option_pricing_mode,
        "strategy_variant": strategy_variant or settings.backtest_strategy_variant,
        "calibration_version": settings.calibration_version,
    }
    if batch_id:
        run_params["batch_id"] = batch_id
    if universe_code:
        run_params["universe"] = universe_code
    if trade_dates:
        run_params["trade_dates"] = [d.isoformat() for d in trade_dates]

    strategy_code = "core-vol" if track_mode == "option" else "track-a-equity"
    run_id = dao.create_run(
        strategy_code=strategy_code,
        started_at=datetime.utcnow(),
        status="RUNNING",
        strategy_version=strategy_variant or settings.backtest_strategy_variant,
        calibration_version=settings.calibration_version,
        data_window_start=start,
        data_window_end=end,
        parameters=run_params,
        notes=None,
    )
    resolved_log_path = resolve_lifecycle_log_path(
        event_log_path,
        batch_id=batch_id,
        run_id=run_id,
    )
    lifecycle_log = BacktestLifecycleLogger(
        resolved_log_path,
        context={
            "batch_id": batch_id,
            "run_id": run_id,
            "symbol": symbol.upper(),
            "track": track_mode,
            "strategy_variant": strategy_variant or settings.backtest_strategy_variant,
        },
    )
    lifecycle_log.emit(
        "BACKTEST_RUN_STARTED",
        start=start,
        end=end,
        signal_mode=signal_mode,
        risk_mode=risk_mode,
        option_pricing_mode=option_pricing_mode,
        event_log_file=str(resolved_log_path),
    )

    if track_mode == "equity":
        try:
            _run_equity_track(
                dao=dao,
                symbol=symbol,
                start=start,
                end=end,
                session_factory=SessionLocal,
                run_id=run_id,
                signal_mode=signal_mode,
                top5_source=top5_source,
                strategy_variant=strategy_variant or settings.backtest_strategy_variant,
                lifecycle_log=lifecycle_log,
            )
            dao.update_run_status(
                run_id=run_id, status="COMPLETED", completed_at=datetime.utcnow(), notes=None
            )
            lifecycle_log.emit(
                "BACKTEST_RUN_COMPLETED",
                event_counts=lifecycle_log.snapshot_counts(),
            )
        except BaseException as exc:
            dao.update_run_status(
                run_id=run_id,
                status="FAILED",
                completed_at=datetime.utcnow(),
                notes=str(exc),
            )
            lifecycle_log.emit(
                "BACKTEST_RUN_FAILED",
                error_type=type(exc).__name__,
                error=str(exc),
                event_counts=lifecycle_log.snapshot_counts(),
            )
            raise
        finally:
            session.commit()
            session.close()
        return run_id

    if (option_pricing_mode or "full_feed").lower() == "event_time":
        try:
            _run_option_event_time_track(
                dao=dao,
                symbol=symbol,
                start=start,
                end=end,
                session_factory=SessionLocal,
                run_id=run_id,
                signal_mode=signal_mode,
                top5_source=top5_source,
                strategy_variant=strategy_variant or settings.backtest_strategy_variant,
                lifecycle_log=lifecycle_log,
            )
            dao.update_run_status(
                run_id=run_id, status="COMPLETED", completed_at=datetime.utcnow(), notes=None
            )
            lifecycle_log.emit(
                "BACKTEST_RUN_COMPLETED",
                event_counts=lifecycle_log.snapshot_counts(),
            )
        except BaseException as exc:
            dao.update_run_status(
                run_id=run_id,
                status="FAILED",
                completed_at=datetime.utcnow(),
                notes=str(exc),
            )
            lifecycle_log.emit(
                "BACKTEST_RUN_FAILED",
                error_type=type(exc).__name__,
                error=str(exc),
                event_counts=lifecycle_log.snapshot_counts(),
            )
            raise
        finally:
            session.commit()
            session.close()
        return run_id

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.setbroker(QuoteAwareOptionBroker())
    equity_data = TimescaleEquityData.from_timescale(
        session=session, dao=dao, symbol=symbol, start=start, end=end
    )
    cerebro.adddata(equity_data)
    signals = load_signals(
        mode=signal_mode,
        session_factory=SessionLocal,
        dao=dao,
        symbols=[symbol],
        start=start,
        end=end,
        top5_source=top5_source,
        strategy_variant=strategy_variant or settings.backtest_strategy_variant,
    )
    signals = [event for event in signals if event.symbol.upper() == symbol.upper()]
    contracts_map, option_feeds = _prepare_option_feeds(
        cerebro=cerebro,
        dao=dao,
        symbol=symbol,
        start=start,
        end=end,
        provided_contracts=option_contracts,
        signals=signals,
    )
    if option_selector is None and option_contracts:
        option_selector = _option_selector_from_contracts(symbol, contracts_map)
    cerebro.broker.addcommissioninfo(IBOptionCommissionInfo())

    risk_ctx = RiskCtx(
        mode=risk_mode,
        session_factory=SessionLocal,
        base_url=settings.risk_service_url,
        allow_missing_option_liquidity_metrics=True,
    )
    risk_prechecker = build_risk_precheck(risk_ctx)

    strategy = OptionSignalStrategy
    cerebro.addstrategy(
        strategy,
        dao=dao,
        signals=signals,
        run_id=run_id,
        option_selector=option_selector,
        risk_ctx=risk_ctx,
        risk_prechecker=risk_prechecker,
        option_feeds=option_feeds,
        strategy_variant=strategy_variant or settings.backtest_strategy_variant,
        lifecycle_log=lifecycle_log,
    )
    cerebro.addanalyzer(MetricsWriter, _name="metrics", dao=dao, run_id=run_id)

    try:
        cerebro.run()
        dao.update_run_status(
            run_id=run_id, status="COMPLETED", completed_at=datetime.utcnow(), notes=None
        )
        lifecycle_log.emit(
            "BACKTEST_RUN_COMPLETED",
            event_counts=lifecycle_log.snapshot_counts(),
        )
    except BaseException as exc:
        dao.update_run_status(
            run_id=run_id, status="FAILED", completed_at=datetime.utcnow(), notes=str(exc)
        )
        lifecycle_log.emit(
            "BACKTEST_RUN_FAILED",
            error_type=type(exc).__name__,
            error=str(exc),
            event_counts=lifecycle_log.snapshot_counts(),
        )
        raise
    finally:
        session.commit()
        session.close()

    return run_id


def _run_equity_track(
    *,
    dao: BacktestDAO,
    symbol: str,
    start: datetime,
    end: datetime,
    session_factory,
    run_id: int,
    signal_mode: str,
    top5_source: Top5Source | None,
    strategy_variant: str | None,
    lifecycle_log: BacktestLifecycleLogger | None = None,
) -> None:
    signals = load_signals(
        mode=signal_mode,
        session_factory=session_factory,
        dao=dao,
        symbols=[symbol],
        start=start,
        end=end,
        top5_source=top5_source,
        strategy_variant=strategy_variant,
    )
    signals = [event for event in signals if event.symbol.upper() == symbol.upper()]
    frame = _load_equity_frame(dao=dao, symbol=symbol, start=start, end=end)
    accumulator = _EquityMetricsAccumulator()
    trades = _simulate_signal_lifecycle(
        dao=dao,
        symbol=symbol,
        frame=frame,
        signals=signals,
        run_id=run_id,
        signal_asset_type="EQUITY",
        accumulator=accumulator,
        lifecycle_log=lifecycle_log,
    )
    for trade in trades:
        _record_equity_trade_ledger(dao=dao, run_id=run_id, trade=trade)
    accumulator.flush(dao, run_id)


@dataclass
class _SimulatedSignalTrade:
    symbol: str
    signal_code: str
    entry_ts: datetime
    exit_ts: datetime
    entry_price: Decimal
    exit_price: Decimal
    option_hint: Mapping[str, Any]
    exit_signal_code: str


def _record_equity_trade_ledger(
    *,
    dao: BacktestDAO,
    run_id: int,
    trade: _SimulatedSignalTrade,
) -> None:
    """Persist an equity round-trip as BUY/SELL executions in bt_trades.

    The option tracks already write one row per execution (a BUY row and a SELL row
    per closed leg). Equity mirrors that canonical ledger shape so the three tracks
    can be reconciled through the same bt_trades acceptance query.
    """
    trace_id = f"equity:{run_id}:{trade.symbol}:{trade.entry_ts.isoformat()}"
    common = {
        "run_id": run_id,
        "symbol": trade.symbol,
        "asset_type": "EQUITY",
        "trace_id": trace_id,
        "option_right": None,
        "strike": None,
        "expiry": None,
        "fees": Decimal("0"),
        "slippage": Decimal("0"),
    }
    dao.record_trade(
        {
            **common,
            "side": "BUY",
            "quantity": Decimal("1"),
            "price": trade.entry_price,
            "trade_ts": trade.entry_ts,
            "reason_code": trade.signal_code,
        }
    )
    dao.record_trade(
        {
            **common,
            "side": "SELL",
            "quantity": Decimal("1"),
            "price": trade.exit_price,
            "trade_ts": trade.exit_ts,
            "reason_code": trade.exit_signal_code,
        }
    )


@dataclass
class _EventTimeOptionOutcome:
    trade: _SimulatedSignalTrade
    status: str
    contract: ContractSelection | None = None
    entry_quote_ts: datetime | None = None
    exit_quote_ts: datetime | None = None
    entry_ask: Decimal | None = None
    exit_bid: Decimal | None = None
    entry_spread_pct: Decimal | None = None
    return_pct: Decimal | None = None
    skip_reason: str | None = None


def _run_option_event_time_track(
    *,
    dao: BacktestDAO,
    symbol: str,
    start: datetime,
    end: datetime,
    session_factory,
    run_id: int,
    signal_mode: str,
    top5_source: Top5Source | None,
    strategy_variant: str | None,
    lifecycle_log: BacktestLifecycleLogger | None = None,
) -> None:
    signals = load_signals(
        mode=signal_mode,
        session_factory=session_factory,
        dao=dao,
        symbols=[symbol],
        start=start,
        end=end,
        top5_source=top5_source,
        strategy_variant=strategy_variant,
    )
    signals = [event for event in signals if event.symbol.upper() == symbol.upper()]
    frame = _load_equity_frame(dao=dao, symbol=symbol, start=start, end=end)
    trades = _simulate_signal_lifecycle(
        dao=dao,
        symbol=symbol,
        frame=frame,
        signals=signals,
        run_id=run_id,
        signal_asset_type="OPTION",
        accumulator=None,
        lifecycle_log=lifecycle_log,
    )
    as_of_date = _today_eastern_date()
    grouped = _group_trades_by_trade_date(trades)
    outcomes: list[_EventTimeOptionOutcome] = []

    for trade_date, day_trades in grouped.items():
        day_candidates = dao.fetch_option_candidates(
            trade_date=trade_date,
            underlying_symbol=symbol,
            option_right="CALL",
            dte_min=0,
            dte_max=7,
        )
        recoverable_candidates = [
            candidate
            for candidate in day_candidates
            if _candidate_expiry_date(candidate) is not None
            and _candidate_expiry_date(candidate) >= as_of_date
        ]
        if recoverable_candidates:
            _ensure_event_time_option_data(
                session_factory=session_factory,
                trade_date=trade_date,
                candidates=recoverable_candidates,
                trades=day_trades,
            )
        for trade in day_trades:
            outcome = _price_event_time_option_trade(
                dao=dao,
                trade=trade,
                candidates=day_candidates,
                as_of_date=as_of_date,
            )
            outcomes.append(outcome)
            trace_id = f"event-time:{run_id}:{symbol}:{trade.entry_ts.isoformat()}"
            if outcome.status != "option-priced" or outcome.contract is None:
                if lifecycle_log is not None:
                    lifecycle_log.emit(
                        "OPTION_TRADE_SKIPPED",
                        trace_id=trace_id,
                        event_time=trade.entry_ts,
                        signal_code=trade.signal_code,
                        exit_signal_code=trade.exit_signal_code,
                        status=outcome.status,
                        reason=outcome.skip_reason,
                        underlying_entry_price=trade.entry_price,
                        underlying_exit_price=trade.exit_price,
                        execution_model="event_time_quote_cross_no_order_book",
                    )
                continue
            contract_fields = {
                "option_right": outcome.contract.option_right,
                "strike": outcome.contract.strike,
                "expiry": outcome.contract.expiry,
                "conid": outcome.contract.conid,
            }
            if lifecycle_log is not None:
                lifecycle_log.emit(
                    "ORDER_SUBMITTED",
                    trace_id=trace_id,
                    event_time=trade.entry_ts,
                    signal_code=trade.signal_code,
                    side="BUY",
                    quantity=1,
                    limit_price=outcome.entry_ask,
                    simulated=True,
                    execution_model="event_time_quote_cross_no_order_book",
                    **contract_fields,
                )
                lifecycle_log.emit(
                    "ORDER_FILLED",
                    trace_id=trace_id,
                    event_time=outcome.entry_quote_ts or trade.entry_ts,
                    signal_code=trade.signal_code,
                    side="BUY",
                    quantity=1,
                    fill_price=outcome.entry_ask,
                    spread_pct=outcome.entry_spread_pct,
                    simulated=True,
                    execution_model="event_time_quote_cross_no_order_book",
                    **contract_fields,
                )
                lifecycle_log.emit(
                    "POSITION_OPENED",
                    trace_id=trace_id,
                    event_time=outcome.entry_quote_ts or trade.entry_ts,
                    quantity=1,
                    entry_price=outcome.entry_ask,
                    **contract_fields,
                )
            dao.record_trade(
                {
                    "run_id": run_id,
                    "symbol": symbol,
                    "asset_type": "OPTION",
                    "side": "BUY",
                    "quantity": Decimal("1"),
                    "price": outcome.entry_ask,
                    "trade_ts": trade.entry_ts,
                    "trace_id": trace_id,
                    "option_right": outcome.contract.option_right,
                    "strike": outcome.contract.strike,
                    "expiry": outcome.contract.expiry.date()
                    if isinstance(outcome.contract.expiry, datetime)
                    else outcome.contract.expiry,
                    "fees": Decimal("0"),
                    "slippage": Decimal("0"),
                    "reason_code": trade.signal_code,
                }
            )
            if lifecycle_log is not None:
                gross_pnl = None
                if outcome.entry_ask is not None and outcome.exit_bid is not None:
                    gross_pnl = (outcome.exit_bid - outcome.entry_ask) * Decimal("100")
                lifecycle_log.emit(
                    "EXIT_TRIGGERED",
                    trace_id=trace_id,
                    event_time=trade.exit_ts,
                    signal_code=trade.exit_signal_code,
                    side="SELL",
                    quantity=1,
                    **contract_fields,
                )
                lifecycle_log.emit(
                    "ORDER_SUBMITTED",
                    trace_id=trace_id,
                    event_time=trade.exit_ts,
                    signal_code=trade.exit_signal_code,
                    side="SELL",
                    quantity=1,
                    limit_price=outcome.exit_bid,
                    simulated=True,
                    execution_model="event_time_quote_cross_no_order_book",
                    **contract_fields,
                )
                lifecycle_log.emit(
                    "ORDER_FILLED",
                    trace_id=trace_id,
                    event_time=outcome.exit_quote_ts or trade.exit_ts,
                    signal_code=trade.exit_signal_code,
                    side="SELL",
                    quantity=1,
                    fill_price=outcome.exit_bid,
                    return_pct=outcome.return_pct,
                    gross_pnl=gross_pnl,
                    simulated=True,
                    execution_model="event_time_quote_cross_no_order_book",
                    **contract_fields,
                )
                lifecycle_log.emit(
                    "POSITION_CLOSED",
                    trace_id=trace_id,
                    event_time=outcome.exit_quote_ts or trade.exit_ts,
                    quantity=0,
                    entry_price=outcome.entry_ask,
                    exit_price=outcome.exit_bid,
                    return_pct=outcome.return_pct,
                    gross_pnl=gross_pnl,
                    holding_seconds=(trade.exit_ts - trade.entry_ts).total_seconds(),
                    **contract_fields,
                )
            dao.record_trade(
                {
                    "run_id": run_id,
                    "symbol": symbol,
                    "asset_type": "OPTION",
                    "side": "SELL",
                    "quantity": Decimal("1"),
                    "price": outcome.exit_bid,
                    "trade_ts": trade.exit_ts,
                    "trace_id": trace_id,
                    "option_right": outcome.contract.option_right,
                    "strike": outcome.contract.strike,
                    "expiry": outcome.contract.expiry.date()
                    if isinstance(outcome.contract.expiry, datetime)
                    else outcome.contract.expiry,
                    "fees": Decimal("0"),
                    "slippage": Decimal("0"),
                    "reason_code": trade.exit_signal_code,
                }
            )

    _record_event_time_metrics(
        dao=dao,
        run_id=run_id,
        outcomes=outcomes,
    )


def discover_recoverable_trade_dates(
    dao: BacktestDAO,
    *,
    symbols: Sequence[str],
    as_of_date: date,
    lookback_trade_days: int = 5,
) -> dict[tuple[str, date], str]:
    trade_dates = dao.fetch_recent_trade_dates(limit=max(1, lookback_trade_days), as_of=as_of_date)
    statuses: dict[tuple[str, date], str] = {}
    for symbol in symbols:
        for trade_date in trade_dates:
            candidates = dao.fetch_option_candidates(
                trade_date=trade_date,
                underlying_symbol=symbol.upper(),
                option_right="CALL",
                dte_min=0,
                dte_max=7,
            )
            recoverable = any(
                expiry_date is not None and expiry_date >= as_of_date
                for expiry_date in (_candidate_expiry_date(candidate) for candidate in candidates)
            )
            statuses[(symbol.upper(), trade_date)] = "recoverable" if recoverable else "option-unrecoverable"
    return statuses


def _load_equity_frame(
    *,
    dao: BacktestDAO,
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    rows = dao.fetch_equity_bars(symbol=symbol, start_ts=start, end_ts=end)
    if not rows:
        raise RuntimeError(f"No equity data available for {symbol} between {start} and {end}")

    frame = pd.DataFrame([asdict(row) for row in rows])
    if frame.empty:
        raise RuntimeError(f"Failed to materialise equity dataset for {symbol}")

    frame["ts_end"] = pd.to_datetime(frame["ts_end"], utc=True)
    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "boll_mid",
        "boll_up",
        "boll_dn",
        "rsi6",
        "rsi12",
        "rsi24",
        "atr14",
        "ao",
        "stoch_k",
        "stoch_d",
        "stoch_rsi_k",
        "stoch_rsi_d",
        "cci14",
        "cci6",
        "obv",
        "obv_ma6",
        "obv_ema20",
        "sma5",
        "lr_m5_slope",
        "lr_boll_dn_slope",
        "lr_obv_slope",
        "mfi14",
        "rvol6",
    ]
    for column in numeric_cols:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.set_index("ts_end").sort_index()


def _simulate_signal_lifecycle(
    *,
    dao: BacktestDAO,
    symbol: str,
    frame: pd.DataFrame,
    signals: Sequence[object],
    run_id: int,
    signal_asset_type: str,
    accumulator: "_EquityMetricsAccumulator | None",
    lifecycle_log: BacktestLifecycleLogger | None = None,
) -> list[_SimulatedSignalTrade]:
    settings = get_settings()
    trades: list[_SimulatedSignalTrade] = []
    vix_mode = settings.vix_gate_mode.lower()
    vix_threshold = settings.vix_gate
    open_entries: List[Dict[str, Any]] = []

    for event in signals:
        ts_utc = _ensure_utc(event.ts_end)
        trace_id = f"sim:{run_id}:{symbol}:{event.signal_code}:{ts_utc.isoformat()}"
        if lifecycle_log is not None:
            lifecycle_log.emit(
                "SIGNAL_RECEIVED",
                trace_id=trace_id,
                event_time=ts_utc,
                asset_type=signal_asset_type,
                signal_code=event.signal_code,
                side=getattr(event.side, "value", str(event.side)),
                execution_model=(
                    "equity_close_no_order_book"
                    if signal_asset_type == "EQUITY"
                    else "event_time_option_projection"
                ),
            )

        if event.side == SignalSide.SELL:
            if not open_entries:
                reason_payload = {
                    "track": signal_asset_type.lower(),
                    "status": "BLOCK",
                    "reason": "BLOCK:EXIT_POSITION_NOT_FOUND",
                    "side": getattr(event.side, "value", str(event.side)),
                }
                dao.record_signal(
                    {
                        "run_id": run_id,
                        "ts_end": ts_utc,
                        "symbol": symbol,
                        "asset_type": signal_asset_type,
                        "signal_code": event.signal_code,
                        "accepted": False,
                        "reason": json.dumps(reason_payload, default=str),
                    }
                )
                if lifecycle_log is not None:
                    lifecycle_log.emit(
                        "SIGNAL_BLOCKED",
                        trace_id=trace_id,
                        event_time=ts_utc,
                        asset_type=signal_asset_type,
                        signal_code=event.signal_code,
                        side="SELL",
                        reason="BLOCK:EXIT_POSITION_NOT_FOUND",
                        open_entries=0,
                    )
                    lifecycle_log.emit(
                        "EXIT_SKIPPED",
                        trace_id=trace_id,
                        event_time=ts_utc,
                        asset_type=signal_asset_type,
                        signal_code=event.signal_code,
                        reason="no_open_position",
                        execution_model=(
                            "equity_close_no_order_book"
                            if signal_asset_type == "EQUITY"
                            else "event_time_option_projection"
                        ),
                    )
                continue

            reason_payload = {
                "track": signal_asset_type.lower(),
                "status": "PASS",
                "side": getattr(event.side, "value", str(event.side)),
            }
            dao.record_signal(
                {
                    "run_id": run_id,
                    "ts_end": ts_utc,
                    "symbol": symbol,
                    "asset_type": signal_asset_type,
                    "signal_code": event.signal_code,
                    "accepted": True,
                    "reason": json.dumps(reason_payload, default=str),
                }
            )
            if lifecycle_log is not None:
                lifecycle_log.emit(
                    "SIGNAL_ACCEPTED",
                    trace_id=trace_id,
                    event_time=ts_utc,
                    asset_type=signal_asset_type,
                    signal_code=event.signal_code,
                    side="SELL",
                    reason="PASS",
                    open_entries=len(open_entries),
                )
            exit_bar = _lookup_bar(frame, ts_utc)
            if exit_bar is not None and not pd.isna(exit_bar.get("close")):
                exit_price = Decimal(str(exit_bar["close"]))
                if exit_price > 0:
                    for entry in open_entries:
                        tfe_ret = (exit_price - entry["entry_price"]) / entry["entry_price"]
                        if accumulator is not None:
                            accumulator.add_tfe(entry["signal_code"], tfe_ret)
                        trades.append(
                            _SimulatedSignalTrade(
                                symbol=symbol,
                                signal_code=entry["signal_code"],
                                entry_ts=entry["entry_ts"],
                                exit_ts=ts_utc,
                                entry_price=entry["entry_price"],
                                exit_price=exit_price,
                                option_hint=dict(entry["option_hint"]),
                                exit_signal_code=event.signal_code,
                            )
                        )
                        if lifecycle_log is not None:
                            lifecycle_log.emit(
                                (
                                    "EXIT_FILLED"
                                    if signal_asset_type == "EQUITY"
                                    else "SIGNAL_LIFECYCLE_EXIT_MARKED"
                                ),
                                trace_id=trace_id,
                                event_time=ts_utc,
                                asset_type=signal_asset_type,
                                signal_code=event.signal_code,
                                entry_signal_code=entry["signal_code"],
                                entry_time=entry["entry_ts"],
                                entry_price=entry["entry_price"],
                                exit_price=exit_price,
                                return_pct=tfe_ret,
                                holding_seconds=(ts_utc - entry["entry_ts"]).total_seconds(),
                                execution_model=(
                                    "equity_close_no_order_book"
                                    if signal_asset_type == "EQUITY"
                                    else "event_time_option_projection"
                                ),
                            )
                    open_entries.clear()
            elif lifecycle_log is not None:
                lifecycle_log.emit(
                    "EXIT_SKIPPED",
                    trace_id=trace_id,
                    event_time=ts_utc,
                    asset_type=signal_asset_type,
                    signal_code=event.signal_code,
                    reason="missing_or_invalid_exit_close",
                    open_entries=len(open_entries),
                )
            continue

        if event.side != SignalSide.BUY:
            reason_payload = {
                "track": signal_asset_type.lower(),
                "status": "PASS",
                "side": getattr(event.side, "value", str(event.side)),
            }
            dao.record_signal(
                {
                    "run_id": run_id,
                    "ts_end": ts_utc,
                    "symbol": symbol,
                    "asset_type": signal_asset_type,
                    "signal_code": event.signal_code,
                    "accepted": True,
                    "reason": json.dumps(reason_payload, default=str),
                }
            )
            if lifecycle_log is not None:
                lifecycle_log.emit(
                    "SIGNAL_ACCEPTED",
                    trace_id=trace_id,
                    event_time=ts_utc,
                    asset_type=signal_asset_type,
                    signal_code=event.signal_code,
                    side=getattr(event.side, "value", str(event.side)),
                    reason="non_entry_signal",
                )
            continue

        bar = _lookup_bar(frame, ts_utc)
        if bar is None or pd.isna(bar.get("close")):
            raise RuntimeError(f"Missing close price for {symbol} at {ts_utc.isoformat()}")

        entry_price = Decimal(str(bar["close"]))
        vix_decision = _evaluate_vix_gate(
            dao=dao,
            ts=ts_utc,
            threshold=vix_threshold,
            mode=vix_mode,
        )
        opportunity_reason = None
        if vix_decision.triggered and vix_mode != "ignore":
            opportunity_reason = "VIX_GATE"

        accepted = True
        block_reason = None

        if vix_decision.triggered and vix_decision.blocked:
            accepted = False
            block_reason = "BLOCK:VIX_GATE"

        if accepted and _in_midday_block(ts_utc):
            accepted = False
            block_reason = "BLOCK:TIME_WINDOW"
            if opportunity_reason is None:
                opportunity_reason = "TIME_WINDOW"

        reason_payload = {
            "track": signal_asset_type.lower(),
            "status": "PASS" if accepted else "BLOCK",
            "reason": block_reason or "PASS",
            "vix_mode": vix_mode,
        }
        if vix_decision.vix is not None:
            reason_payload["vix_value"] = str(vix_decision.vix)

        dao.record_signal(
            {
                "run_id": run_id,
                "ts_end": ts_utc,
                "symbol": symbol,
                "asset_type": signal_asset_type,
                "signal_code": event.signal_code,
                "accepted": accepted,
                "reason": json.dumps(reason_payload, default=str),
            }
        )

        if lifecycle_log is not None:
            lifecycle_log.emit(
                "SIGNAL_ACCEPTED" if accepted else "SIGNAL_BLOCKED",
                trace_id=trace_id,
                event_time=ts_utc,
                asset_type=signal_asset_type,
                signal_code=event.signal_code,
                side="BUY",
                reason=block_reason or "PASS",
                vix_mode=vix_mode,
                vix_value=vix_decision.vix,
                entry_reference_price=entry_price,
            )

        if accepted:
            if accumulator is not None:
                accumulator.add_execution(signal_code=event.signal_code)
            open_entries.append(
                {
                    "signal_code": event.signal_code,
                    "entry_price": entry_price,
                    "entry_ts": ts_utc,
                    "option_hint": dict(getattr(event, "option_hint", {}) or {}),
                }
            )
            if lifecycle_log is not None:
                lifecycle_log.emit(
                    (
                        "ENTRY_FILLED"
                        if signal_asset_type == "EQUITY"
                        else "SIGNAL_LIFECYCLE_ENTRY_MARKED"
                    ),
                    trace_id=trace_id,
                    event_time=ts_utc,
                    asset_type=signal_asset_type,
                    signal_code=event.signal_code,
                    side="BUY",
                    price=entry_price,
                    quantity=1,
                    position_entries=len(open_entries),
                    execution_model=(
                        "equity_close_no_order_book"
                        if signal_asset_type == "EQUITY"
                        else "event_time_option_projection"
                    ),
                )
        else:
            if accumulator is not None and block_reason:
                accumulator.add_block_reason(block_reason)
            if accumulator is not None and opportunity_reason:
                accumulator.add_opportunity(event.signal_code, opportunity_reason)

    if open_entries:
        tail_close = frame["close"].dropna()
        if not tail_close.empty:
            exit_price = Decimal(str(tail_close.iloc[-1]))
            exit_ts = _ensure_utc(tail_close.index[-1].to_pydatetime())
            if exit_price > 0:
                for entry in open_entries:
                    tfe_ret = (exit_price - entry["entry_price"]) / entry["entry_price"]
                    if accumulator is not None:
                        accumulator.add_tfe(entry["signal_code"], tfe_ret)
                    trades.append(
                        _SimulatedSignalTrade(
                            symbol=symbol,
                            signal_code=entry["signal_code"],
                            entry_ts=entry["entry_ts"],
                            exit_ts=exit_ts,
                            entry_price=entry["entry_price"],
                            exit_price=exit_price,
                            option_hint=dict(entry["option_hint"]),
                            exit_signal_code="TAIL_CLOSE",
                        )
                    )
                    if lifecycle_log is not None:
                        lifecycle_log.emit(
                            (
                                "EXIT_FILLED"
                                if signal_asset_type == "EQUITY"
                                else "SIGNAL_LIFECYCLE_EXIT_MARKED"
                            ),
                            trace_id=(
                                f"sim:{run_id}:{symbol}:TAIL_CLOSE:{exit_ts.isoformat()}"
                            ),
                            event_time=exit_ts,
                            asset_type=signal_asset_type,
                            signal_code="TAIL_CLOSE",
                            entry_signal_code=entry["signal_code"],
                            entry_time=entry["entry_ts"],
                            entry_price=entry["entry_price"],
                            exit_price=exit_price,
                            return_pct=tfe_ret,
                            holding_seconds=(exit_ts - entry["entry_ts"]).total_seconds(),
                            execution_model="terminal_bar_mark",
                        )
        open_entries.clear()

    return trades


def _group_trades_by_trade_date(
    trades: Sequence[_SimulatedSignalTrade],
) -> dict[date, list[_SimulatedSignalTrade]]:
    grouped: dict[date, list[_SimulatedSignalTrade]] = defaultdict(list)
    for trade in trades:
        grouped[trade.entry_ts.astimezone(EASTERN).date()].append(trade)
    return grouped


def _price_event_time_option_trade(
    *,
    dao: BacktestDAO,
    trade: _SimulatedSignalTrade,
    candidates: Sequence[object],
    as_of_date: date,
) -> _EventTimeOptionOutcome:
    dte_min, dte_max = _hint_int_range(trade.option_hint.get("dte"), default=(0, 7))
    delta_low, delta_high = _hint_decimal_range(
        trade.option_hint.get("delta"), default=(Decimal("0.35"), Decimal("0.55"))
    )
    recoverable_candidates = [
        candidate
        for candidate in candidates
        if candidate.conid is not None
        and candidate.option_right == "CALL"
        and candidate.dte is not None
        and dte_min <= int(candidate.dte) <= dte_max
        and (_candidate_expiry_date(candidate) or date.min) >= as_of_date
    ]
    if not recoverable_candidates:
        return _EventTimeOptionOutcome(
            trade=trade,
            status="option-unrecoverable",
            skip_reason="no_recoverable_candidates",
        )

    contract = _select_event_time_contract(
        dao=dao,
        symbol=trade.symbol,
        ts=trade.entry_ts,
        candidates=recoverable_candidates,
        delta_low=delta_low,
        delta_high=delta_high,
    )
    if contract is None:
        return _EventTimeOptionOutcome(
            trade=trade,
            status="selector-no-contract",
            skip_reason="no_fresh_entry_quote",
        )

    exit_quote = _fetch_event_time_quote(
        dao=dao,
        conid=int(contract.conid),
        ts=trade.exit_ts,
    )
    if exit_quote is None:
        return _EventTimeOptionOutcome(
            trade=trade,
            status="data-skipped",
            contract=contract,
            entry_quote_ts=contract.quote_ts,
            entry_ask=contract.ask,
            entry_spread_pct=_spread_pct(contract.bid, contract.ask, contract.mid),
            skip_reason="missing_exit_quote",
        )

    exit_bid = Decimal(str(exit_quote.bid))
    return_pct = (exit_bid - contract.ask) / contract.ask if contract.ask > 0 else None
    return _EventTimeOptionOutcome(
        trade=trade,
        status="option-priced",
        contract=contract,
        entry_quote_ts=contract.quote_ts,
        exit_quote_ts=_ensure_utc(exit_quote.ts_end),
        entry_ask=contract.ask,
        exit_bid=exit_bid,
        entry_spread_pct=_spread_pct(contract.bid, contract.ask, contract.mid),
        return_pct=return_pct,
    )


def _select_event_time_contract(
    *,
    dao: BacktestDAO,
    symbol: str,
    ts: datetime,
    candidates: Sequence[object],
    delta_low: Decimal,
    delta_high: Decimal,
) -> ContractSelection | None:
    window_start = ts - timedelta(minutes=15)
    quotes = dao.fetch_latest_option_quotes_for_symbol(
        underlying_symbol=symbol,
        option_right="CALL",
        start_ts=window_start,
        end_ts=ts,
    )
    candidates_by_conid = {int(candidate.conid): candidate for candidate in candidates if candidate.conid is not None}
    ranked: list[tuple[tuple[object, ...], ContractSelection]] = []
    for quote in quotes:
        if quote.conid is None or int(quote.conid) not in candidates_by_conid:
            continue
        candidate = candidates_by_conid[int(quote.conid)]
        contract = _event_time_contract_from_quote(
            candidate=candidate,
            quote=quote,
            ts=ts,
            delta_low=delta_low,
            delta_high=delta_high,
        )
        if contract is None:
            continue
        strike_distance = (
            abs(contract.strike - contract.underlying_price)
            if contract.underlying_price is not None
            else Decimal("0")
        )
        quote_age = max(0.0, (ts - contract.quote_ts).total_seconds()) if contract.quote_ts else 0.0
        ranked.append(
            (
                (
                    quote_age,
                    contract.ask - contract.bid,
                    _delta_penalty_value(contract.delta, delta_low, delta_high),
                    _missing_underlying_penalty(contract.underlying_price),
                    -contract.open_interest,
                    -contract.volume,
                    strike_distance,
                ),
                contract,
            )
        )
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[0])
    return ranked[0][1]


def _event_time_contract_from_quote(
    *,
    candidate,
    quote,
    ts: datetime,
    delta_low: Decimal,
    delta_high: Decimal,
) -> ContractSelection | None:
    quote_ts = _ensure_utc(quote.ts_end)
    if max(0.0, (ts - quote_ts).total_seconds()) > 60:
        return None
    try:
        bid = Decimal(str(quote.bid))
        ask = Decimal(str(quote.ask))
        mid = Decimal(str(quote.mid if quote.mid is not None else (bid + ask) / Decimal("2")))
        strike = Decimal(str(quote.strike if quote.strike is not None else candidate.strike))
    except Exception:
        return None
    if bid <= 0 or ask <= bid or mid <= 0:
        return None
    expiry = quote.expiry or candidate.expiry
    expiry_dt = _normalise_expiry_datetime(expiry)
    if expiry_dt is None:
        return None
    dte = (expiry_dt.date() - ts.astimezone(EASTERN).date()).days
    if dte < 0 or dte > 7:
        return None
    delta_value = quote.delta if quote.delta is not None else candidate.delta
    delta_dec: Decimal | None = None
    if delta_value is not None:
        delta_dec = Decimal(str(delta_value))
        if not (delta_low <= abs(delta_dec) <= delta_high):
            return None
    underlying_price = None
    if quote.underlying_price is not None:
        underlying_price = Decimal(str(quote.underlying_price))
    elif candidate.underlying_price is not None:
        underlying_price = Decimal(str(candidate.underlying_price))
    return ContractSelection(
        conid=int(candidate.conid),
        symbol=str(candidate.underlying_symbol or quote.symbol).upper(),
        expiry=expiry_dt,
        strike=strike,
        right="CALL",
        bid=bid,
        ask=ask,
        mid=mid,
        min_tick=Decimal(str(candidate.min_tick or "0.01")),
        option_right="CALL",
        open_interest=int(quote.open_interest or candidate.open_interest or 0),
        volume=int(quote.volume or candidate.volume or 0),
        dte=dte,
        delta=delta_dec,
        underlying_price=underlying_price,
        quote_ts=quote_ts,
    )


def _fetch_event_time_quote(
    *,
    dao: BacktestDAO,
    conid: int,
    ts: datetime,
) -> object | None:
    window_start = ts - timedelta(minutes=1)
    quote = dao.fetch_latest_option_quote_by_conid(conid=conid, start_ts=window_start, end_ts=ts)
    if quote is None:
        return None
    quote_ts = _ensure_utc(quote.ts_end)
    if max(0.0, (ts - quote_ts).total_seconds()) > 60:
        return None
    if quote.bid is None or quote.ask is None:
        return None
    if Decimal(str(quote.ask)) <= Decimal(str(quote.bid)):
        return None
    return quote


def _ensure_event_time_option_data(
    *,
    session_factory,
    trade_date: date,
    candidates: Sequence[object],
    trades: Sequence[_SimulatedSignalTrade],
) -> None:
    if not candidates or not trades:
        return
    window_start = min(trade.entry_ts - timedelta(minutes=15) for trade in trades).astimezone(EASTERN)
    window_end = max(trade.exit_ts + timedelta(minutes=1) for trade in trades).astimezone(EASTERN)
    if window_end <= window_start:
        return
    from libs.core import get_settings as _get_settings
    from libs.infra import build_ibkr_client
    from scripts.ingest_option_l1_ibkr import ContractDescriptor, _ingest_contract

    settings = _get_settings()
    client = build_ibkr_client(settings)
    client.reqMarketDataType(4)
    try:
        for candidate in candidates:
            if candidate.conid is None:
                continue
            descriptor = ContractDescriptor.from_candidate(candidate)
            try:
                _ingest_contract(
                    client,
                    session_factory,
                    descriptor,
                    trade_date,
                    min_minutes=0,
                    window_start_et=window_start.strftime("%H:%M"),
                    window_end_et=window_end.strftime("%H:%M"),
                )
            except Exception as exc:
                LOGGER.warning(
                    "backtest.event_time_option_ingest_failed",
                    symbol=descriptor.symbol,
                    trade_date=trade_date.isoformat(),
                    conid=descriptor.conid,
                    error=str(exc),
                )
    finally:
        client.disconnect_and_stop()


def _record_event_time_metrics(
    *,
    dao: BacktestDAO,
    run_id: int,
    outcomes: Sequence[_EventTimeOptionOutcome],
) -> None:
    priced = [outcome for outcome in outcomes if outcome.status == "option-priced" and outcome.return_pct is not None]
    skipped = [outcome for outcome in outcomes if outcome.status == "data-skipped"]
    unrecoverable = [outcome for outcome in outcomes if outcome.status == "option-unrecoverable"]
    selector_misses = [outcome for outcome in outcomes if outcome.status == "selector-no-contract"]
    returns = [float(outcome.return_pct) for outcome in priced if outcome.return_pct is not None]
    spreads = [
        float(outcome.entry_spread_pct)
        for outcome in priced
        if outcome.entry_spread_pct is not None
    ]
    deltas = [
        float(abs(outcome.contract.delta))
        for outcome in priced
        if outcome.contract is not None and outcome.contract.delta is not None
    ]
    dtes = [
        float(outcome.contract.dte)
        for outcome in priced
        if outcome.contract is not None and outcome.contract.dte is not None
    ]
    total_pnl = 0.0
    gross_wins = 0.0
    gross_losses = 0.0
    for outcome in priced:
        if outcome.entry_ask is None or outcome.exit_bid is None:
            continue
        pnl = float((outcome.exit_bid - outcome.entry_ask) * Decimal("100"))
        total_pnl += pnl
        if pnl > 0:
            gross_wins += pnl
        elif pnl < 0:
            gross_losses += abs(pnl)
    win_rate = (sum(1 for value in returns if value > 0) / len(returns)) if returns else 0.0
    metrics = [
        {"run_id": run_id, "metric_code": "num_trades", "metric_value": float(len(priced))},
        {"run_id": run_id, "metric_code": "win_rate", "metric_value": win_rate},
        {"run_id": run_id, "metric_code": "total_pnl", "metric_value": total_pnl},
        {"run_id": run_id, "metric_code": "gross_wins", "metric_value": gross_wins},
        {"run_id": run_id, "metric_code": "gross_losses", "metric_value": gross_losses},
        {
            "run_id": run_id,
            "metric_code": "option_priced_count",
            "metric_value": float(len(priced)),
        },
        {
            "run_id": run_id,
            "metric_code": "option_data_skipped_count",
            "metric_value": float(len(skipped)),
        },
        {
            "run_id": run_id,
            "metric_code": "option_unrecoverable_count",
            "metric_value": float(len(unrecoverable)),
        },
        {
            "run_id": run_id,
            "metric_code": "option_selector_no_contract_count",
            "metric_value": float(len(selector_misses)),
        },
        {
            "run_id": run_id,
            "metric_code": "option_avg_return",
            "metric_value": (sum(returns) / len(returns)) if returns else 0.0,
        },
        {
            "run_id": run_id,
            "metric_code": "option_total_return",
            "metric_value": sum(returns) if returns else 0.0,
        },
        {
            "run_id": run_id,
            "metric_code": "option_avg_spread_pct",
            "metric_value": (sum(spreads) / len(spreads)) if spreads else 0.0,
        },
        {
            "run_id": run_id,
            "metric_code": "option_avg_delta",
            "metric_value": (sum(deltas) / len(deltas)) if deltas else 0.0,
        },
        {
            "run_id": run_id,
            "metric_code": "option_avg_dte",
            "metric_value": (sum(dtes) / len(dtes)) if dtes else 0.0,
        },
    ]
    if metrics:
        dao.record_metrics_total(metrics)


def _candidate_expiry_date(candidate) -> date | None:
    expiry = getattr(candidate, "expiry", None)
    if expiry is None:
        return None
    if isinstance(expiry, datetime):
        return expiry.date()
    if isinstance(expiry, date):
        return expiry
    return None


def _normalise_expiry_datetime(expiry: date | datetime | None) -> datetime | None:
    if expiry is None:
        return None
    if isinstance(expiry, datetime):
        return _ensure_utc(expiry)
    return datetime.combine(expiry, datetime.min.time(), tzinfo=timezone.utc)


def _spread_pct(bid: Decimal, ask: Decimal, mid: Decimal) -> Decimal | None:
    if mid <= 0:
        return None
    return (ask - bid) / mid


def _delta_penalty_value(
    delta: Decimal | None,
    low: Decimal,
    high: Decimal,
) -> Decimal:
    if delta is None:
        return Decimal("50")
    abs_delta = abs(delta)
    if low <= abs_delta <= high:
        return Decimal("0")
    if abs_delta < low:
        return low - abs_delta
    return abs_delta - high


def _missing_underlying_penalty(underlying_price: Decimal | None) -> int:
    return 1 if underlying_price is None else 0


def _today_eastern_date() -> date:
    return datetime.now(timezone.utc).astimezone(EASTERN).date()


@dataclass
class _VixGateDecision:
    triggered: bool
    blocked: bool
    vix: Decimal | None
    mode: str


class _EquityMetricsAccumulator:
    def __init__(self) -> None:
        self._tfe_returns: Dict[str, list[float]] = defaultdict(list)
        self._exec_counts: Counter[str] = Counter()
        self._opp_counts: Counter[str] = Counter()
        self._opp_reason_counts: Counter[str] = Counter()
        self._block_counts: Counter[str] = Counter()

    def add_execution(self, *, signal_code: str) -> None:
        self._exec_counts[signal_code] += 1

    def add_opportunity(self, signal_code: str, reason: Optional[str]) -> None:
        self._opp_counts[signal_code] += 1
        if reason:
            self._opp_reason_counts[_normalise_reason(reason)] += 1

    def add_block_reason(self, reason: str) -> None:
        self._block_counts[_normalise_reason(reason)] += 1

    def add_tfe(self, signal_code: str, value: Decimal) -> None:
        self._tfe_returns[signal_code].append(float(value))

    def flush(self, dao: BacktestDAO, run_id: int) -> None:
        metrics: list[dict[str, object]] = []

        for signal_code, values in self._tfe_returns.items():
            if not values:
                continue
            signal_suffix = signal_code.removeprefix("SIG_")
            values_sorted = sorted(values)
            mean_val = sum(values_sorted) / len(values_sorted)
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"RET_SIG_{signal_suffix}_TFE_MEAN",
                    "metric_value": mean_val,
                }
            )
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"RET_SIG_{signal_suffix}_TFE_P50",
                    "metric_value": _percentile(values_sorted, 0.50),
                }
            )
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"RET_SIG_{signal_suffix}_TFE_P90",
                    "metric_value": _percentile(values_sorted, 0.90),
                }
            )

        for signal_code, count in self._exec_counts.items():
            signal_suffix = signal_code.removeprefix("SIG_")
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"COUNT_EXEC_SIG_{signal_suffix}",
                    "metric_value": float(count),
                }
            )

        for signal_code, count in self._opp_counts.items():
            signal_suffix = signal_code.removeprefix("SIG_")
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"COUNT_OPP_SIG_{signal_suffix}",
                    "metric_value": float(count),
                }
            )

        for reason, count in self._opp_reason_counts.items():
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"COUNT_OPP_REASON_{reason}",
                    "metric_value": float(count),
                }
            )

        for reason, count in self._block_counts.items():
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"COUNT_BLOCK_{reason}",
                    "metric_value": float(count),
                }
            )

        if metrics:
            dao.record_metrics_total(metrics)


def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _lookup_bar(frame: pd.DataFrame, ts_utc: datetime):
    if ts_utc in frame.index:
        row = frame.loc[ts_utc]
    else:
        try:
            indexer = frame.index.get_indexer([ts_utc], method="nearest")
        except Exception:
            return None
        if not indexer.size or indexer[0] == -1:
            return None
        candidate = frame.index[indexer[0]]
        if abs((ts_utc - candidate).total_seconds()) > 60:
            return None
        row = frame.loc[candidate]
    if isinstance(row, pd.DataFrame):
        return row.iloc[0]
    return row


def _evaluate_vix_gate(
    *,
    dao: BacktestDAO,
    ts: datetime,
    threshold: Decimal,
    mode: str,
) -> _VixGateDecision:
    vix_value = dao.fetch_vix_value(ts=ts)
    if vix_value is None:
        # Use a default VIX value (20.0) when data is missing, assume not triggered
        vix_value = Decimal("20.0")
        LOGGER.warning("vix.missing", ts=ts.isoformat(), default_value=str(vix_value))
    triggered = vix_value >= threshold
    mode_normalised = mode.lower()
    blocked = triggered and mode_normalised == "enforce"
    return _VixGateDecision(triggered=triggered, blocked=blocked, vix=vix_value, mode=mode_normalised)


def _in_midday_block(ts_utc: datetime) -> bool:
    local_time = ts_utc.astimezone(EASTERN).time()
    return time(12, 0) <= local_time < time(14, 0)


def _normalise_reason(reason: str | None) -> str:
    if not reason:
        return "PASS"
    value = reason
    if ":" in value:
        value = value.split(":", 1)[1]
    return value.strip().replace("-", "_").upper()


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        return 0.0
    quantile = min(max(quantile, 0.0), 1.0)
    if len(values) == 1:
        return values[0]
    idx = quantile * (len(values) - 1)
    lower = int(idx)
    upper = min(lower + 1, len(values) - 1)
    weight = idx - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def _prepare_option_feeds(
    *,
    cerebro: bt.Cerebro,
    dao: BacktestDAO,
    symbol: str,
    start: datetime,
    end: datetime,
    provided_contracts: Mapping[str, ContractSelection] | None,
    signals: Sequence[object] | None = None,
) -> Tuple[Dict[str, ContractSelection], Dict[tuple[object, ...], bt.feeds.PandasData]]:
    if start.tzinfo is None:
        start_utc = start.replace(tzinfo=timezone.utc)
    else:
        start_utc = start.astimezone(timezone.utc)
    trade_date = start_utc.date()
    contracts: Dict[str, ContractSelection] = {}
    if provided_contracts:
        for right, contract in provided_contracts.items():
            contracts[right.upper()] = contract
    else:
        contracts = _auto_select_contracts(
            dao=dao,
            symbol=symbol,
            trade_date=trade_date,
            signals=signals or (),
            start=start,
            end=end,
        )

    feeds: Dict[tuple[object, ...], bt.feeds.PandasData] = {}
    for contract in contracts.values():
        try:
            feed = TimescaleOptionData.from_timescale(
                dao=dao,
                contract={
                    "conid": contract.conid,
                    "symbol": contract.symbol,
                    "expiry": contract.expiry,
                    "strike": contract.strike,
                    "right": contract.option_right,
                },
                start=start,
                end=end,
            )
        except RuntimeError as exc:
            if provided_contracts:
                raise RuntimeError(
                    f"bars1m_option data missing for {contract.symbol} {contract.option_right} conid={contract.conid}"
                ) from exc
            LOGGER.warning(
                "backtest.option_feed_skipped",
                symbol=contract.symbol,
                right=contract.option_right,
                conid=contract.conid,
                reason=str(exc),
            )
            continue
        cerebro.adddata(feed)
        key = _feed_key_for_contract(contract)
        feeds[key] = feed

    if not feeds:
        raise RuntimeError(f"No option L1 data for {symbol} signal contracts")
    return contracts, feeds


def _auto_select_contracts(
    *,
    dao: BacktestDAO,
    symbol: str,
    trade_date,
    signals: Sequence[object] = (),
    start: datetime | None = None,
    end: datetime | None = None,
) -> Dict[str, ContractSelection]:
    if not signals:
        contracts: Dict[str, ContractSelection] = {}
        for option_right in ("CALL", "PUT"):
            candidates = dao.fetch_option_candidates(
                trade_date=trade_date,
                underlying_symbol=symbol,
                option_right=option_right,
                dte_min=0,
                dte_max=7,
            )
            selection = _select_best_contract(candidates, option_right, symbol)
            if selection:
                contracts[option_right] = selection
        if not contracts:
            raise RuntimeError(f"No option contracts available for {symbol} on {trade_date}")
        return contracts

    contracts_by_key: Dict[tuple[object, ...], ContractSelection] = {}
    for signal in signals:
        side = getattr(signal, "side", None)
        if getattr(side, "value", side) != "BUY":
            continue
        signal_ts = _ensure_utc(getattr(signal, "ts_end"))
        signal_date = signal_ts.astimezone(EASTERN).date()
        option_hint = getattr(signal, "option_hint", {}) or {}
        dte_min, dte_max = _hint_int_range(option_hint.get("dte"), default=(0, 7))
        delta_min, delta_max = _hint_decimal_range(
            option_hint.get("delta"), default=(Decimal("0.35"), Decimal("0.55"))
        )
        candidates = dao.fetch_option_candidates(
            trade_date=signal_date,
            underlying_symbol=symbol,
            option_right="CALL",
            dte_min=dte_min,
            dte_max=dte_max,
        )
        for candidate in candidates:
            contract = _candidate_to_contract(candidate, "CALL", symbol)
            if contract is None:
                continue
            if contract.delta is not None and not (
                delta_min <= abs(contract.delta) <= delta_max
            ):
                continue
            if start is not None and end is not None and contract.conid is not None:
                minutes = dao.count_option_minutes(conid=int(contract.conid), start_ts=start, end_ts=end)
                if minutes <= 0:
                    continue
            contracts_by_key[_feed_key_for_contract(contract)] = contract
    contracts = {f"CALL:{idx}": contract for idx, contract in enumerate(contracts_by_key.values())}
    if not contracts:
        raise RuntimeError(f"No option contracts available for {symbol} on {trade_date}")
    return contracts


def _select_best_contract(candidates, option_right: str, symbol: str) -> ContractSelection | None:
    settings = get_settings()
    liquidity_required = bool(settings.option_liquidity_required)
    filtered = []
    for candidate in candidates:
        selection = _candidate_to_contract(candidate, option_right, symbol)
        if selection is None:
            continue
        bid_d = selection.bid
        ask_d = selection.ask
        mid_d = selection.mid
        spread_d = ask_d - bid_d
        if ask_d <= bid_d or spread_d <= 0:
            continue
        threshold = max(Decimal("0.10"), mid_d * Decimal("0.05"))
        if liquidity_required and spread_d > threshold:
            continue
        filtered.append(selection)
    if not filtered:
        return None
    filtered.sort(key=lambda c: (c.ask - c.bid, c.mid))
    return filtered[0]


def _candidate_to_contract(candidate, option_right: str, symbol: str) -> ContractSelection | None:
    bid = candidate.bid
    ask = candidate.ask
    mid = candidate.mid
    min_tick = candidate.min_tick
    strike_value = candidate.strike
    if None in (bid, ask, mid, min_tick, strike_value):
        return None
    bid_d = Decimal(str(bid))
    ask_d = Decimal(str(ask))
    mid_d = Decimal(str(mid))
    if ask_d <= bid_d or ask_d <= 0 or bid_d <= 0 or mid_d <= 0:
        return None
    expiry = candidate.expiry
    if expiry is None:
        return None
    if not isinstance(expiry, datetime):
        expiry = datetime.combine(expiry, datetime.min.time(), tzinfo=timezone.utc)
    try:
        conid_int = int(candidate.conid) if candidate.conid is not None else 0
        open_interest_int = int(candidate.open_interest or 0)
        volume_int = int(candidate.volume or 0)
        dte_int = int(candidate.dte or 0)
    except (TypeError, ValueError):
        return None
    delta_value = Decimal(str(candidate.delta)) if candidate.delta is not None else None
    underlying = (
        Decimal(str(candidate.underlying_price))
        if candidate.underlying_price is not None
        else None
    )
    return ContractSelection(
        conid=conid_int or None,
        symbol=candidate.underlying_symbol or symbol,
        expiry=expiry,
        strike=Decimal(str(strike_value)),
        right=option_right,
        bid=bid_d,
        ask=ask_d,
        mid=mid_d,
        min_tick=Decimal(str(min_tick)),
        option_right=option_right,
        open_interest=open_interest_int,
        volume=volume_int,
        dte=dte_int,
        delta=delta_value,
        underlying_price=underlying,
    )


def _feed_key_for_contract(contract: ContractSelection) -> tuple[object, ...]:
    expiry = contract.expiry.date().isoformat() if isinstance(contract.expiry, datetime) else str(contract.expiry)
    return (
        contract.symbol.upper(),
        contract.option_right.upper(),
        int(contract.conid) if contract.conid is not None else None,
        expiry,
        str(contract.strike),
    )


def _hint_int_range(value, *, default: tuple[int, int]) -> tuple[int, int]:
    source = value if isinstance(value, (list, tuple, set)) else default
    parsed: list[int] = []
    for item in source:
        try:
            parsed.append(int(item))
        except (TypeError, ValueError):
            continue
    if len(parsed) >= 2:
        return min(parsed), max(parsed)
    if len(parsed) == 1:
        return parsed[0], parsed[0]
    return default


def _hint_decimal_range(value, *, default: tuple[Decimal, Decimal]) -> tuple[Decimal, Decimal]:
    source = value if isinstance(value, (list, tuple, set)) else default
    parsed: list[Decimal] = []
    for item in source:
        try:
            parsed.append(Decimal(str(item)))
        except Exception:
            continue
    if len(parsed) >= 2:
        return min(parsed), max(parsed)
    if len(parsed) == 1:
        return parsed[0], parsed[0]
    return default


def _option_selector_from_contracts(symbol: str, contracts: Mapping[str, ContractSelection]) -> OptionSelector:
    from libs.schemas.signals import SignalSide

    lookup: Dict[tuple[str, str], ContractSelection] = {}
    for right, contract in contracts.items():
        lookup[(symbol.upper(), right.upper())] = contract

    def selector(signal, ts):
        if signal.side == SignalSide.SELL:
            key = (signal.symbol.upper(), "PUT")
        else:
            key = (signal.symbol.upper(), "CALL")
        return lookup.get(key)

    return selector
