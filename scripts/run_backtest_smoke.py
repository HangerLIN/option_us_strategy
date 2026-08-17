# path: scripts/run_backtest_smoke.py
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, TYPE_CHECKING

import structlog
import subprocess
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from dotenv import load_dotenv
from libs.core import configure_logging, get_settings, trading_session_window
from libs.infra.db import get_session_factory

if TYPE_CHECKING:  # pragma: no cover
    from apps.backtest.dao import BacktestDAO
    from apps.backtest.strategy.option_signal_strategy import ContractSelection, OptionSelector

LOGGER = structlog.get_logger(__name__)


class SmokeRetry(RuntimeError):
    """Raised when the current trade date should be skipped and a new date attempted."""


@dataclass(frozen=True)
class ContractDescriptor:
    symbol: str
    option_right: str
    expiry: str
    strike: float
    conid: int
    min_tick: float
    open_interest: int | None = None
    volume: int | None = None


@dataclass
class SymbolSelection:
    symbol: str
    contracts: Dict[str, "ContractSelection"]
    run_id: int | None = None


@dataclass
class SymbolRunSummary:
    symbol: str
    run_id: int
    contracts: int
    signals_total: int
    signals_accepted: int
    signals_blocked: int
    trades: int
    fees_total: float
    fee_ratio: float
    slip_p50: float
    slip_p90: float
    metrics: Dict[str, float] = field(default_factory=dict)


@dataclass
class SmokeRunSummary:
    trade_date: date
    start_utc: datetime
    end_utc: datetime
    contract_file: Path
    symbols: List[SymbolRunSummary]
    run_ids: Dict[str, int]


@dataclass(frozen=True)
class TradeDatePlan:
    trade_date: date
    symbols: List[str]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run automated backtest smoke with optional ingestion")
    parser.add_argument("--auto-ingest", action="store_true", help="Auto run equity/option ingest scripts if data missing")
    parser.add_argument("--lookback-days", type=int, default=10, help="Number of recent trade dates to try")
    parser.add_argument("--max-retries", type=int, default=2, help="Maximum retries per trade date before moving on")
    parser.add_argument("--symbols-limit", type=int, default=2, help="Maximum number of symbols to backtest")
    parser.add_argument("--contracts-cache-dir", default=".cache", help="Directory storing pre-selected contracts")
    parser.add_argument("--print-ib-config", action="store_true", help="Print IB connection parameters and exit")
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file with connection credentials (default .env)",
    )
    parser.add_argument(
        "--min-equity-coverage",
        type=_coverage_ratio,
        default=0.85,
        help="Required fraction (0-1] of RTH minutes for equity data (default 0.85)",
    )
    parser.add_argument(
        "--min-option-coverage",
        type=_coverage_ratio,
        default=0.77,
        help="Required fraction (0-1] of RTH minutes for option L1 data (default 0.77 ≈ 300/390)",
    )
    parser.add_argument(
        "--option-market-data-type",
        type=int,
        choices=[1, 2, 3, 4],
        default=4,
        help="IBKR market data type for option ingestion (1=live,2=frozen,3=delayed,4=delayed frozen)",
    )
    return parser.parse_args()


def _coverage_ratio(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Coverage ratio must be a float") from exc
    if not 0 < value <= 1:
        raise argparse.ArgumentTypeError("Coverage ratio must be between 0 (exclusive) and 1 (inclusive)")
    return value


def _load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
        LOGGER.debug("env.file_missing", path=str(env_path))
        return
    load_dotenv(env_path, override=False)
    LOGGER.info("env.file_loaded", path=str(env_path))


def _dedupe_symbols(symbols: Sequence[str]) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized = str(symbol).strip().upper()
        if not normalized or normalized in seen:
            continue
        ordered.append(normalized)
        seen.add(normalized)
    return ordered


def _python_command(*args: str) -> List[str]:
    env_prefix = ["env", "PYTHONPATH=."]
    return env_prefix + [sys.executable, *args]


def _run_subprocess(label: str, cmd: Sequence[str]) -> None:
    LOGGER.info("subprocess.spawn", label=label, command=" ".join(cmd))
    completed = subprocess.run(cmd, capture_output=True, text=True)
    if completed.stdout:
        LOGGER.debug("subprocess.stdout", label=label, stdout=completed.stdout.strip())
    if completed.stderr:
        LOGGER.debug("subprocess.stderr", label=label, stderr=completed.stderr.strip())
    if completed.returncode != 0:
        raise SmokeRetry(f"{label} failed with exit {completed.returncode}")


def _print_ib_config(settings) -> None:
    print(
        json.dumps(
            {
                "IB_HOST": settings.ib_host,
                "IB_PORT": settings.ib_port,
                "IB_CLIENT_ID": settings.ib_client_id,
                "IB_ACCOUNT": settings.ib_account,
            },
            indent=2,
        )
    )


def _validate_environment() -> None:
    required_env = ("DATABASE_URL", "REDIS_URL", "IB_ACCOUNT", "IB_CLIENT_ID", "IB_HOST", "IB_PORT")
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(sorted(missing))}")


def _candidate_trade_dates(session_factory: sessionmaker[Session], lookback: int) -> List[date]:
    from apps.backtest.dao import BacktestDAO

    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=get_settings().option_bar_table,
            option_chain_table=get_settings().option_chain_table,
        )
        dates = dao.fetch_recent_trade_dates(limit=lookback, as_of=datetime.now(timezone.utc).date())
    if not dates:
        raise RuntimeError("No recent trade dates available in dim_trading_calendar")
    return dates


def _ensure_equity_data(
    settings,
    session_factory: sessionmaker[Session],
    trade_date: date,
    symbols: Sequence[str],
    *,
    min_coverage: float,
    auto_ingest: bool,
) -> None:
    from apps.backtest.dao import BacktestDAO

    expected = _expected_minutes(trade_date)
    required = max(1, math.ceil(expected * min_coverage))
    start_et, end_et = trading_session_window(trade_date)
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        counts = {
            symbol: dao.count_equity_minutes(symbol=symbol, start_ts=start_utc, end_ts=end_utc) for symbol in symbols
        }
        missing = [symbol for symbol, count in counts.items() if count < required]
    if not missing:
        return
    if not auto_ingest:
        detail = ", ".join(f"{symbol} {counts[symbol]}/{required}" for symbol in missing)
        raise SmokeRetry(f"Equity data missing for {trade_date}: {detail}")
    cmd = _python_command(
        "-m",
        "apps.backtest.data_prep.ingest_equity_1m_ibkr",
        "--date",
        trade_date.isoformat(),
        "--symbols",
        *missing,
    )
    _run_subprocess("ingest_equity_1m", cmd)
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        refreshed = {
            symbol: dao.count_equity_minutes(symbol=symbol, start_ts=start_utc, end_ts=end_utc) for symbol in missing
        }
        remaining = {symbol: count for symbol, count in refreshed.items() if count < required}
    if remaining:
        detail = ", ".join(f"{symbol} {count}/{required}" for symbol, count in remaining.items())
        raise SmokeRetry(f"Equity ingestion incomplete for symbols: {detail}")


def _ensure_option_chain(
    settings,
    trade_date: date,
    symbols: Sequence[str],
    *,
    market_data_type: int,
    auto_ingest: bool,
) -> None:
    if not auto_ingest:
        return
    cmd = _python_command(
        "scripts/ingest_option_chain_meta_ibkr.py",
        "--date",
        trade_date.isoformat(),
        "--symbols",
        *symbols,
        "--dte-min",
        "2",
        "--dte-max",
        "7",
        "--otm-min",
        "0",
        "--otm-max",
        "5",
        "--max-per-side",
        "2",
        "--market-data-type",
        str(market_data_type),
    )
    _run_subprocess("ingest_option_chain_meta", cmd)


def _ensure_option_l1(
    settings,
    session_factory: sessionmaker[Session],
    trade_date: date,
    symbols: Sequence[str],
    contracts_dir: Path,
    *,
    min_minutes: int,
    market_data_type: int,
    auto_ingest: bool,
) -> Path:
    from apps.backtest.dao import BacktestDAO

    contracts_dir.mkdir(parents=True, exist_ok=True)
    target_path = _contracts_cache_path(contracts_dir, trade_date, symbols)
    if auto_ingest:
        cmd = _python_command(
            "scripts/ingest_option_l1_ibkr.py",
            "--date",
            trade_date.isoformat(),
            "--underlyings",
            *symbols,
            "--output-contracts",
            str(target_path),
            "--dte-min",
            "2",
            "--dte-max",
            "7",
            "--otm-min",
            "0",
            "--otm-max",
            "5",
            "--min-minutes",
            str(min_minutes),
            "--market-data-type",
            str(market_data_type),
        )
        _run_subprocess("ingest_option_l1", cmd)
    source_path, descriptors = _resolve_cached_contracts(contracts_dir, trade_date, symbols)
    if source_path is None:
        raise SmokeRetry(f"Contracts file not found at {target_path}")
    if not descriptors:
        raise SmokeRetry("Contracts file empty; no viable options selected")
    if source_path != target_path:
        _write_contract_descriptors(target_path, descriptors)
    start_utc, end_utc = _session_bounds(trade_date)
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        deficits: list[str] = []
        for desc in descriptors:
            count = dao.count_option_minutes(conid=desc.conid, start_ts=start_utc, end_ts=end_utc)
            if count < min_minutes:
                deficits.append(f"{desc.conid} {count}/{min_minutes}")
        if deficits:
            detail = ", ".join(deficits)
            raise SmokeRetry(f"Option L1 minutes insufficient: {detail}")
    return target_path


def _load_contracts_from_file(path: Path) -> List[ContractDescriptor]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    descriptors: List[ContractDescriptor] = []
    for entry in raw:
        descriptors.append(
            ContractDescriptor(
                symbol=str(entry["symbol"]).upper(),
                option_right=str(entry["option_right"]).upper(),
                expiry=str(entry["expiry"]),
                strike=float(entry["strike"]),
                conid=int(entry["conid"]),
                min_tick=float(entry["min_tick"]),
                open_interest=int(entry.get("open_interest") or 0),
                volume=int(entry.get("volume") or 0),
            )
        )
    return descriptors


def _session_bounds(trade_date: date) -> tuple[datetime, datetime]:
    start_et, end_et = trading_session_window(trade_date)
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
    return start_utc, end_utc


def _candidate_symbols_for_trade_date(
    dao: BacktestDAO,
    trade_date: date,
    *,
    limit: int,
    start_utc: datetime,
    end_utc: datetime,
) -> List[str]:
    search_limit = max(limit * 5, 10)
    candidates: List[str] = []
    candidates.extend(str(sym).upper() for sym in dao.fetch_premarket_top(trade_date=trade_date))
    volume_rank = dao.fetch_equity_volume_top(
        start_ts=start_utc,
        end_ts=end_utc,
        limit=search_limit,
    )
    candidates.extend(
        row[0].upper() if isinstance(row, tuple) else str(row).upper() for row in volume_rank
    )
    option_rank = dao.fetch_option_underlying_top(trade_date=trade_date, limit=search_limit)
    candidates.extend(str(sym).upper() for sym in option_rank)
    fallback = [
        token.strip().upper()
        for token in get_settings().top5_fixed_pool.split(",")
        if token.strip()
    ][:search_limit]
    if fallback:
        LOGGER.warning(
            "smoke.symbols_fallback_fixed_pool",
            trade_date=str(trade_date),
            symbols=fallback,
        )
        candidates.extend(fallback)
    return _dedupe_symbols(candidates)


def _choose_symbols(
    dao: BacktestDAO,
    trade_date: date,
    *,
    limit: int,
    start_utc: datetime,
    end_utc: datetime,
) -> List[str]:
    candidates = _candidate_symbols_for_trade_date(
        dao,
        trade_date,
        limit=limit,
        start_utc=start_utc,
        end_utc=end_utc,
    )
    if not candidates:
        raise SmokeRetry(f"No candidate symbols found for {trade_date}")
    return candidates[:limit]


def _contracts_cache_path(contracts_dir: Path, trade_date: date, symbols: Sequence[str]) -> Path:
    key = "_".join(sorted(_dedupe_symbols(symbols)))
    return contracts_dir / f"contracts_{trade_date.isoformat()}_{key}.json"


def _filter_contract_descriptors(
    descriptors: Sequence[ContractDescriptor],
    symbols: Sequence[str],
) -> List[ContractDescriptor]:
    target_symbols = set(_dedupe_symbols(symbols))
    return [desc for desc in descriptors if desc.symbol.upper() in target_symbols]


def _resolve_cached_contracts(
    contracts_dir: Path,
    trade_date: date,
    symbols: Sequence[str],
) -> tuple[Path | None, List[ContractDescriptor]]:
    target_path = _contracts_cache_path(contracts_dir, trade_date, symbols)
    if target_path.exists():
        return target_path, _filter_contract_descriptors(_load_contracts_from_file(target_path), symbols)

    pattern = f"contracts_{trade_date.isoformat()}_*.json"
    expected_symbols = set(_dedupe_symbols(symbols))
    for path in sorted(contracts_dir.glob(pattern)):
        descriptors = _filter_contract_descriptors(_load_contracts_from_file(path), symbols)
        if expected_symbols.issubset({desc.symbol.upper() for desc in descriptors}):
            return path, descriptors
    return None, []


def _write_contract_descriptors(path: Path, descriptors: Sequence[ContractDescriptor]) -> None:
    payload = [
        {
            "symbol": desc.symbol,
            "option_right": desc.option_right,
            "expiry": desc.expiry,
            "strike": desc.strike,
            "conid": desc.conid,
            "min_tick": desc.min_tick,
            "open_interest": desc.open_interest,
            "volume": desc.volume,
        }
        for desc in descriptors
    ]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _symbol_has_complete_local_data(
    settings,
    session_factory: sessionmaker[Session],
    trade_date: date,
    symbol: str,
    *,
    contracts_dir: Path,
    min_equity_coverage: float,
    min_option_coverage: float,
) -> tuple[bool, str]:
    from apps.backtest.dao import BacktestDAO

    start_utc, end_utc = _session_bounds(trade_date)
    expected_equity = _expected_minutes(trade_date)
    required_equity = max(1, math.ceil(expected_equity * min_equity_coverage))
    required_option = max(1, math.ceil(expected_equity * min_option_coverage))

    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        equity_count = dao.count_equity_minutes(
            symbol=symbol,
            start_ts=start_utc,
            end_ts=end_utc,
        )
        if equity_count < required_equity:
            return False, f"{symbol}: equity {equity_count}/{required_equity}"

        contracts_path, descriptors = _resolve_cached_contracts(
            contracts_dir,
            trade_date,
            [symbol],
        )
        if contracts_path is None:
            return False, f"{symbol}: missing cached contracts"
        if not descriptors:
            return False, f"{symbol}: empty cached contracts"

        deficits: List[str] = []
        for desc in descriptors:
            candidate = dao.fetch_option_candidate_by_conid(desc.conid)
            if candidate is None or None in (
                candidate.bid,
                candidate.ask,
                candidate.mid,
                candidate.min_tick,
            ):
                deficits.append(f"{desc.conid}: missing option chain quote")
                continue
            option_count = dao.count_option_minutes(
                conid=desc.conid,
                start_ts=start_utc,
                end_ts=end_utc,
            )
            if option_count < required_option:
                deficits.append(f"{desc.conid}: option {option_count}/{required_option}")
        if deficits:
            return False, f"{symbol}: " + ", ".join(deficits)

    return True, symbol


def _select_complete_symbols_for_trade_date(
    args: argparse.Namespace,
    settings,
    session_factory: sessionmaker[Session],
    trade_date: date,
) -> tuple[List[str], List[str]]:
    from apps.backtest.dao import BacktestDAO

    start_utc, end_utc = _session_bounds(trade_date)
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        candidates = _candidate_symbols_for_trade_date(
            dao,
            trade_date,
            limit=args.symbols_limit,
            start_utc=start_utc,
            end_utc=end_utc,
        )
    selected: List[str] = []
    reasons: List[str] = []
    contracts_dir = Path(args.contracts_cache_dir)
    for symbol in candidates:
        complete, detail = _symbol_has_complete_local_data(
            settings,
            session_factory,
            trade_date,
            symbol,
            contracts_dir=contracts_dir,
            min_equity_coverage=args.min_equity_coverage,
            min_option_coverage=args.min_option_coverage,
        )
        if complete:
            selected.append(symbol)
            if len(selected) >= args.symbols_limit:
                break
        else:
            reasons.append(detail)
    return selected, reasons


def _find_latest_complete_trade_date(
    args: argparse.Namespace,
    settings,
    session_factory: sessionmaker[Session],
    trade_dates: Sequence[date],
) -> TradeDatePlan | None:
    for trade_date in trade_dates:
        symbols, reasons = _select_complete_symbols_for_trade_date(
            args,
            settings,
            session_factory,
            trade_date,
        )
        if symbols:
            return TradeDatePlan(trade_date=trade_date, symbols=symbols)
        LOGGER.info(
            "smoke.trade_date_incomplete",
            trade_date=str(trade_date),
            reasons=reasons[:10],
        )
    return None


def _build_contract_selection(
    dao: BacktestDAO,
    descriptor: ContractDescriptor,
    trade_date: date,
) -> ContractSelection:
    from apps.backtest.strategy.option_signal_strategy import ContractSelection
    from apps.backtest.datafeed.timescale_option import TimescaleOptionData

    candidate = dao.fetch_option_candidate_by_conid(descriptor.conid)
    if candidate is None:
        raise SmokeRetry(f"Option chain metadata missing for conid={descriptor.conid}")
    expiry = candidate.expiry
    if not isinstance(expiry, datetime):
        expiry = datetime.combine(expiry, datetime.min.time(), tzinfo=timezone.utc)
    bid = candidate.bid
    ask = candidate.ask
    mid = candidate.mid
    min_tick = candidate.min_tick
    if None in (bid, ask, mid, min_tick):
        raise SmokeRetry(f"Option candidate missing bid/ask for conid={descriptor.conid}")
    session_start, session_end = _session_bounds(trade_date)
    TimescaleOptionData.from_timescale(
        dao=dao,
        contract={
            "conid": descriptor.conid,
            "symbol": descriptor.symbol,
            "expiry": expiry,
            "strike": Decimal(str(descriptor.strike)),
            "right": (candidate.option_right or descriptor.option_right).upper(),
        },
        start=session_start,
        end=session_end,
    )
    return ContractSelection(
        conid=int(descriptor.conid),
        symbol=descriptor.symbol,
        expiry=expiry,
        strike=Decimal(str(descriptor.strike)),
        right=candidate.option_right or descriptor.option_right,
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        mid=Decimal(str(mid)),
        min_tick=Decimal(str(min_tick)),
        option_right=(candidate.option_right or descriptor.option_right).upper(),
        open_interest=int(candidate.open_interest or 0),
        volume=int(candidate.volume or 0),
        dte=int(candidate.dte or 0),
    )


def _prepare_symbol_selections(
    settings,
    session_factory: sessionmaker[Session],
    trade_date: date,
    descriptors: Sequence[ContractDescriptor],
) -> List[SymbolSelection]:
    from apps.backtest.dao import BacktestDAO

    selections: Dict[str, Dict[str, ContractSelection]] = {}
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        for desc in descriptors:
            selection = _build_contract_selection(dao, desc, trade_date)
            selections.setdefault(desc.symbol.upper(), {})[selection.option_right] = selection
    return [SymbolSelection(symbol=symbol, contracts=mapping) for symbol, mapping in selections.items()]


def _option_selector_factory(mapping: Mapping[tuple[str, str], ContractSelection]) -> OptionSelector:
    from libs.schemas.signals import SignalSide

    def selector(signal, ts):
        if signal.side == SignalSide.SELL:
            key = (signal.symbol.upper(), "PUT")
        else:
            key = (signal.symbol.upper(), "CALL")
        return mapping.get(key)

    return selector


def _build_smoke_top5_source(
    settings,
    session_factory: sessionmaker[Session],
    trade_date: date,
    symbols: Sequence[str],
):
    from apps.backtest.pipeline.premarket import PremarketTop5Builder
    from apps.signal_svc.top5_source import BacktestTop5Source

    batch_id = f"smoke-{trade_date.isoformat()}-{'-'.join(_dedupe_symbols(symbols))}"
    with session_factory() as session:
        builder = PremarketTop5Builder(
            session,
            preearn_days_min=settings.preearn_days_min,
            preearn_days_max=settings.preearn_days_max,
            preearn_atr_pct_max=settings.preearn_atr_pct_max,
        )
        builder.build_for_date(
            batch_id=batch_id,
            trade_date=trade_date,
            symbols=_dedupe_symbols(symbols),
            universe_code="smoke",
        )
        session.commit()
    return BacktestTop5Source(batch_id=batch_id)


def _execute_backtests(
    session_factory: sessionmaker[Session],
    settings,
    selections: List[SymbolSelection],
    trade_date: date,
) -> Dict[str, int]:
    from apps.backtest.bt_runner import run_backtest

    start_utc, end_utc = _session_bounds(trade_date)
    top5_source = _build_smoke_top5_source(
        settings,
        session_factory,
        trade_date,
        [selection.symbol for selection in selections],
    )
    contract_map: Dict[tuple[str, str], ContractSelection] = {}
    for selection in selections:
        for right, contract in selection.contracts.items():
            contract_map[(selection.symbol.upper(), right.upper())] = contract
    option_selector = _option_selector_factory(contract_map)
    run_ids: Dict[str, int] = {}
    for selection in selections:
        run_id = run_backtest(
            symbol=selection.symbol,
            start=start_utc,
            end=end_utc,
            signal_mode="recompute",
            risk_mode="inproc",
            option_selector=option_selector,
            option_contracts=selection.contracts,
            top5_source=top5_source,
        )
        run_ids[selection.symbol.upper()] = run_id
        LOGGER.info("backtest.completed", symbol=selection.symbol, run_id=run_id)
    return run_ids


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
    gross = Decimal(row.gross_notional or 0)
    fees = Decimal(row.fees_total or 0)
    fee_ratio = float((fees / gross) if gross > 0 else Decimal("0"))
    return {
        "trades": float(row.trades or 0),
        "fees_total": float(fees),
        "fee_ratio": fee_ratio,
        "slip_p50": float(row.slip_p50 or 0),
        "slip_p90": float(row.slip_p90 or 0),
    }


def _collect_metrics(session: Session, run_id: int) -> Dict[str, float]:
    sql = text(
        """
        SELECT metric_code, metric_value
        FROM bt_metrics_total
        WHERE run_id = :run_id
        """
    )
    rows = session.execute(sql, {"run_id": run_id}).all()
    return {row.metric_code: float(row.metric_value or 0) for row in rows}


def _build_symbol_summary(
    session_factory: sessionmaker[Session],
    symbol: str,
    run_id: int,
    contracts: Mapping[str, ContractSelection],
) -> SymbolRunSummary:
    with session_factory() as session:
        signal_stats = _collect_signal_stats(session, run_id)
        trade_stats = _collect_trade_stats(session, run_id)
        metrics = _collect_metrics(session, run_id)
    return SymbolRunSummary(
        symbol=symbol,
        run_id=run_id,
        contracts=len(contracts),
        signals_total=signal_stats["total"],
        signals_accepted=signal_stats["accepted"],
        signals_blocked=signal_stats["blocked"],
        trades=int(trade_stats["trades"]),
        fees_total=trade_stats["fees_total"],
        fee_ratio=trade_stats["fee_ratio"],
        slip_p50=trade_stats["slip_p50"],
        slip_p90=trade_stats["slip_p90"],
        metrics=metrics,
    )


def _print_summary(summary: SmokeRunSummary) -> None:
    header = (
        f"回测窗口 {summary.trade_date.isoformat()} "
        f"[{summary.start_utc.astimezone(timezone.utc).isoformat()} → "
        f"{summary.end_utc.astimezone(timezone.utc).isoformat()}]"
    )
    print(header)
    print(f"合约缓存: {summary.contract_file}")
    for line in summary.symbols:
        metrics = ", ".join(
            f"{key}={value:.4f}" for key, value in sorted(line.metrics.items()) if key in {"sharpe", "max_drawdown"}
        )
        print(
            f"{line.symbol}: run_id={line.run_id} 合约={line.contracts} "
            f"信号={line.signals_total} (通过={line.signals_accepted}, 拒绝={line.signals_blocked}) "
            f"成交={line.trades} 费用={line.fees_total:.2f} "
            f"费率={line.fee_ratio:.4%} 滑点P50={line.slip_p50:.4f} P90={line.slip_p90:.4f} "
            f"{metrics}"
        )


def _expected_minutes(trade_date: date) -> int:
    start_et, end_et = trading_session_window(trade_date)
    return int((end_et - start_et).total_seconds() / 60)


def _process_trade_date(
    args: argparse.Namespace,
    settings,
    session_factory: sessionmaker[Session],
    trade_date: date,
    *,
    symbols_override: Sequence[str] | None = None,
) -> SmokeRunSummary:
    from apps.backtest.dao import BacktestDAO

    LOGGER.info("smoke.trade_date", trade_date=str(trade_date))
    start_utc, end_utc = _session_bounds(trade_date)
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        if symbols_override is not None:
            symbols = _dedupe_symbols(symbols_override)
        else:
            symbols = _choose_symbols(
                dao,
                trade_date,
                limit=args.symbols_limit,
                start_utc=start_utc,
                end_utc=end_utc,
            )
    LOGGER.info("smoke.symbols", trade_date=str(trade_date), symbols=symbols)

    _ensure_equity_data(
        settings,
        session_factory,
        trade_date,
        symbols,
        min_coverage=args.min_equity_coverage,
        auto_ingest=args.auto_ingest,
    )
    _ensure_option_chain(
        settings,
        trade_date,
        symbols,
        market_data_type=args.option_market_data_type,
        auto_ingest=args.auto_ingest,
    )
    option_required_minutes = max(1, math.ceil(_expected_minutes(trade_date) * args.min_option_coverage))
    contracts_path = _ensure_option_l1(
        settings,
        session_factory,
        trade_date,
        symbols,
        Path(args.contracts_cache_dir),
        min_minutes=option_required_minutes,
        market_data_type=args.option_market_data_type,
        auto_ingest=args.auto_ingest,
    )
    descriptors = _load_contracts_from_file(contracts_path)
    selections = _prepare_symbol_selections(settings, session_factory, trade_date, descriptors)
    if not selections:
        raise SmokeRetry("No contract selections available after ingestion")
    run_ids = _execute_backtests(session_factory, settings, selections, trade_date)
    summaries = [
        _build_symbol_summary(session_factory, selection.symbol, run_ids[selection.symbol.upper()], selection.contracts)
        for selection in selections
    ]
    return SmokeRunSummary(
        trade_date=trade_date,
        start_utc=start_utc,
        end_utc=end_utc,
        contract_file=contracts_path,
        symbols=summaries,
        run_ids=run_ids,
    )


def main() -> int:
    args = _parse_args()
    _load_env_file(args.env_file)
    settings = get_settings()
    configure_logging(settings)
    if args.print_ib_config:
        _print_ib_config(settings)
        return 0
    _validate_environment()
    session_factory = get_session_factory(settings)

    trade_dates = _candidate_trade_dates(session_factory, args.lookback_days)
    attempt_plans: List[TradeDatePlan] = []
    preferred = _find_latest_complete_trade_date(args, settings, session_factory, trade_dates)
    if preferred is not None:
        if preferred.trade_date != trade_dates[0]:
            LOGGER.info(
                "smoke.trade_date_fallback",
                requested_trade_date=str(trade_dates[0]),
                selected_trade_date=str(preferred.trade_date),
                symbols=preferred.symbols,
            )
        attempt_plans.append(preferred)
    for trade_date in trade_dates:
        if preferred is not None and trade_date == preferred.trade_date:
            continue
        attempt_plans.append(TradeDatePlan(trade_date=trade_date, symbols=[]))

    for plan in attempt_plans:
        for attempt in range(max(1, args.max_retries)):
            try:
                summary = _process_trade_date(
                    args,
                    settings,
                    session_factory,
                    plan.trade_date,
                    symbols_override=plan.symbols or None,
                )
                _print_summary(summary)
                _exercise_api(summary)
                return 0
            except SmokeRetry as exc:
                LOGGER.warning(
                    "smoke.retry",
                    trade_date=str(plan.trade_date),
                    attempt=attempt + 1,
                    detail=str(exc),
                )
    LOGGER.error("smoke.failed", reason="all candidate trade dates exhausted")
    return 1


def _exercise_api(summary: SmokeRunSummary) -> None:
    from apps.backtest.api import app as backtest_api_app

    client = TestClient(backtest_api_app)
    expected_run_ids = set(summary.run_ids.values())
    runs_resp = client.get("/runs", params={"limit": 100})
    if runs_resp.status_code != 200:
        raise RuntimeError(f"/runs probe failed with HTTP {runs_resp.status_code}")
    runs_payload = runs_resp.json()
    runs_data = runs_payload.get("data", {})
    returned_run_ids = {int(row["run_id"]) for row in runs_data.get("runs", []) if "run_id" in row}
    missing_run_ids = sorted(expected_run_ids - returned_run_ids)
    if missing_run_ids:
        raise RuntimeError(f"/runs probe did not return completed run ids: {missing_run_ids}")
    LOGGER.info("api.runs_probe", run_ids=sorted(expected_run_ids), count=runs_data.get("count"))
    for run_id in summary.run_ids.values():
        metrics_resp = client.get(f"/backtest-metrics/{run_id}")
        if metrics_resp.status_code != 200:
            raise RuntimeError(
                f"/backtest-metrics/{run_id} probe failed with HTTP {metrics_resp.status_code}"
            )
        metrics_payload = metrics_resp.json()
        total_metrics = (metrics_payload.get("data") or {}).get("total") or {}
        if not total_metrics:
            raise RuntimeError(f"/backtest-metrics/{run_id} returned no total metrics")
        LOGGER.info(
            "api.metrics_probe",
            run_id=run_id,
            status=metrics_resp.status_code,
            metric_count=len(total_metrics),
        )


if __name__ == "__main__":
    raise SystemExit(main())
