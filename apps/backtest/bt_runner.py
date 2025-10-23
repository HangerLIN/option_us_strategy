from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import json
from typing import Dict, Mapping, Sequence, Tuple

import backtrader as bt
import pandas as pd
import structlog
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

LOGGER = structlog.get_logger(__name__)

from apps.backtest.broker.commission_ib import IBOptionCommissionInfo
from apps.backtest.dao import BacktestDAO
from apps.backtest.datafeed.timescale_equity import TimescaleEquityData
from apps.backtest.strategy.option_signal_strategy import (
    ContractSelection,
    OptionSignalStrategy,
    OptionSelector,
)
from apps.backtest.analyzers.metrics_writer import MetricsWriter
from apps.backtest.signal_source import load_signals
from apps.backtest.risk_adapter import RiskCtx, build_risk_precheck
from libs.core import EASTERN, get_settings
from apps.backtest.datafeed.timescale_option import TimescaleOptionData
from apps.signal_svc.top5_source import Top5Source
from libs.schemas.signals import SignalSide


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
        parameters=run_params,
        notes=None,
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
            )
            dao.update_run_status(
                run_id=run_id, status="COMPLETED", completed_at=datetime.utcnow(), notes=None
            )
        except Exception as exc:
            dao.update_run_status(
                run_id=run_id,
                status="FAILED",
                completed_at=datetime.utcnow(),
                notes=str(exc),
            )
            raise
        finally:
            session.commit()
            session.close()
        return run_id

    cerebro = bt.Cerebro(stdstats=False)
    equity_data = TimescaleEquityData.from_timescale(
        session=session, dao=dao, symbol=symbol, start=start, end=end
    )
    cerebro.adddata(equity_data)
    contracts_map, option_feeds = _prepare_option_feeds(
        cerebro=cerebro,
        dao=dao,
        symbol=symbol,
        start=start,
        end=end,
        provided_contracts=option_contracts,
    )
    if option_selector is None:
        option_selector = _option_selector_from_contracts(symbol, contracts_map)
    cerebro.broker.addcommissioninfo(IBOptionCommissionInfo())

    signals = load_signals(
        mode=signal_mode,
        session_factory=SessionLocal,
        dao=dao,
        symbols=[symbol],
        start=start,
        end=end,
        top5_source=top5_source,
    )

    risk_ctx = RiskCtx(
        mode=risk_mode, session_factory=SessionLocal, base_url=settings.risk_service_url
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
    )
    cerebro.addanalyzer(MetricsWriter, _name="metrics", dao=dao, run_id=run_id)

    try:
        cerebro.run()
        dao.update_run_status(
            run_id=run_id, status="COMPLETED", completed_at=datetime.utcnow(), notes=None
        )
    except Exception as exc:
        dao.update_run_status(
            run_id=run_id, status="FAILED", completed_at=datetime.utcnow(), notes=str(exc)
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
) -> None:
    settings = get_settings()

    signals = load_signals(
        mode=signal_mode,
        session_factory=session_factory,
        dao=dao,
        symbols=[symbol],
        start=start,
        end=end,
        top5_source=top5_source,
    )

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
    frame = frame.set_index("ts_end").sort_index()

    accumulator = _EquityMetricsAccumulator()
    vix_mode = settings.vix_gate_mode.lower()
    vix_threshold = settings.vix_gate

    open_entries: List[Dict[str, Decimal]] = []

    for event in signals:
        ts_utc = _ensure_utc(event.ts_end)

        if event.side == SignalSide.SELL:
            reason_payload = {
                "track": "equity",
                "status": "PASS",
                "side": getattr(event.side, "value", str(event.side)),
            }
            dao.record_signal(
                {
                    "run_id": run_id,
                    "ts_end": ts_utc,
                    "symbol": symbol,
                    "signal_code": event.signal_code,
                    "accepted": True,
                    "reason": json.dumps(reason_payload, default=str),
                }
            )
            exit_bar = _lookup_bar(frame, ts_utc)
            if exit_bar is not None and not pd.isna(exit_bar.get("close")):
                exit_price = Decimal(str(exit_bar["close"]))
                if exit_price > 0:
                    for entry in open_entries:
                        tfe_ret = (exit_price - entry["entry_price"]) / entry["entry_price"]
                        accumulator.add_tfe(entry["signal_code"], tfe_ret)
                    open_entries.clear()
            continue

        if event.side != SignalSide.BUY:
            reason_payload = {
                "track": "equity",
                "status": "PASS",
                "side": getattr(event.side, "value", str(event.side)),
            }
            dao.record_signal(
                {
                    "run_id": run_id,
                    "ts_end": ts_utc,
                    "symbol": symbol,
                    "signal_code": event.signal_code,
                    "accepted": True,
                    "reason": json.dumps(reason_payload, default=str),
                }
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
            "track": "equity",
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
                "signal_code": event.signal_code,
                "accepted": accepted,
                "reason": json.dumps(reason_payload, default=str),
            }
        )

        if accepted:
            accumulator.add_execution(signal_code=event.signal_code)
            open_entries.append(
                {"signal_code": event.signal_code, "entry_price": entry_price}
            )
        else:
            if block_reason:
                accumulator.add_block_reason(block_reason)
            if opportunity_reason:
                accumulator.add_opportunity(event.signal_code, opportunity_reason)

    if open_entries:
        tail_close = frame["close"].dropna()
        if not tail_close.empty:
            exit_price = Decimal(str(tail_close.iloc[-1]))
            if exit_price > 0:
                for entry in open_entries:
                    tfe_ret = (exit_price - entry["entry_price"]) / entry["entry_price"]
                    accumulator.add_tfe(entry["signal_code"], tfe_ret)
        open_entries.clear()

    accumulator.flush(dao, run_id)


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
            values_sorted = sorted(values)
            mean_val = sum(values_sorted) / len(values_sorted)
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"RET_SIG_{signal_code}_TFE_MEAN",
                    "metric_value": mean_val,
                }
            )
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"RET_SIG_{signal_code}_TFE_P50",
                    "metric_value": _percentile(values_sorted, 0.50),
                }
            )
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"RET_SIG_{signal_code}_TFE_P90",
                    "metric_value": _percentile(values_sorted, 0.90),
                }
            )

        for signal_code, count in self._exec_counts.items():
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"COUNT_EXEC_SIG_{signal_code}",
                    "metric_value": float(count),
                }
            )

        for signal_code, count in self._opp_counts.items():
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_code": f"COUNT_OPP_SIG_{signal_code}",
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
) -> Tuple[Dict[str, ContractSelection], Dict[tuple[str, str], bt.feeds.PandasData]]:
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
        contracts = _auto_select_contracts(dao=dao, symbol=symbol, trade_date=trade_date)

    feeds: Dict[tuple[str, str], bt.feeds.PandasData] = {}
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
            raise RuntimeError(
                f"bars1m_option data missing for {contract.symbol} {contract.option_right} conid={contract.conid}"
            ) from exc
        cerebro.adddata(feed)
        key = (contract.symbol.upper(), contract.option_right.upper())
        feeds[key] = feed

    return contracts, feeds


def _auto_select_contracts(
    *,
    dao: BacktestDAO,
    symbol: str,
    trade_date,
) -> Dict[str, ContractSelection]:
    contracts: Dict[str, ContractSelection] = {}
    for option_right in ("CALL", "PUT"):
        candidates = dao.fetch_option_candidates(
            trade_date=trade_date,
            underlying_symbol=symbol,
            option_right=option_right,
            dte_min=2,
            dte_max=7,
        )
        selection = _select_best_contract(candidates, option_right, symbol)
        if selection:
            contracts[option_right] = selection
    if not contracts:
        raise RuntimeError(f"No option contracts available for {symbol} on {trade_date}")
    return contracts


def _select_best_contract(candidates, option_right: str, symbol: str) -> ContractSelection | None:
    filtered = []
    for candidate in candidates:
        bid = candidate.bid
        ask = candidate.ask
        mid = candidate.mid
        min_tick = candidate.min_tick
        strike_value = candidate.strike
        if None in (bid, ask, mid, min_tick, strike_value):
            continue
        bid_d = Decimal(str(bid))
        ask_d = Decimal(str(ask))
        mid_d = Decimal(str(mid))
        spread_d = ask_d - bid_d
        if ask_d <= bid_d or spread_d <= 0:
            continue
        if Decimal(str(candidate.open_interest or 0)) < Decimal("500"):
            continue
        if Decimal(str(candidate.volume or 0)) < Decimal("100"):
            continue
        threshold = max(Decimal("0.10"), mid_d * Decimal("0.05"))
        if spread_d > threshold:
            continue
        expiry = candidate.expiry
        if expiry is None:
            continue
        if not isinstance(expiry, datetime):
            expiry = datetime.combine(expiry, datetime.min.time(), tzinfo=timezone.utc)
        try:
            conid_int = int(candidate.conid) if candidate.conid is not None else 0
            open_interest_int = int(candidate.open_interest or 0)
            volume_int = int(candidate.volume or 0)
            dte_int = int(candidate.dte or 0)
        except (TypeError, ValueError):
            continue
        filtered.append(
            ContractSelection(
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
            )
        )
    if not filtered:
        return None
    filtered.sort(key=lambda c: (c.ask - c.bid, c.mid))
    return filtered[0]


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
