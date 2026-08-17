from __future__ import annotations

import argparse
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from apps.backtest.dao import BacktestDAO
from apps.backtest.universe import UniverseResolver
from libs.core import EASTERN, configure_logging, get_settings, trading_session_window
from libs.infra import build_ibkr_client, get_session_factory
from libs.infra.ibkr_client import IBClient


LOGGER = structlog.get_logger(__name__)


@dataclass
class OptionMetaRecord:
    trade_date: date
    underlying_symbol: str
    conid: int
    expiry: date
    right: str
    strike: Decimal
    trading_class: str | None
    multiplier: str | None
    exchange: str | None
    dte: int
    delta: Decimal | None
    gamma: Decimal | None
    theta: Decimal | None
    vega: Decimal | None
    implied_vol: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    mid: Decimal | None
    open_interest: int | None
    volume: int | None
    min_tick: Decimal | None
    underlying_price: Decimal | None


@dataclass
class OptionBarRecord:
    conid: int
    ts_end: datetime
    underlying_symbol: str
    expiry: date
    right: str
    strike: Decimal
    trading_class: str | None
    multiplier: str | None
    exchange: str | None
    bid: Decimal
    ask: Decimal
    mid: Decimal
    volume: int | None
    open_interest: int | None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="IBKR option chain ingestion")
    parser.add_argument("--date-from", required=True, help="Start trade date (YYYY-MM-DD)")
    parser.add_argument("--date-to", required=True, help="End trade date (YYYY-MM-DD)")
    parser.add_argument(
        "--symbols-from",
        default="file",
        choices=["premarket_top5", "ref_universe", "list", "file"],
        help="Source of underlying symbols (default: file)",
    )
    parser.add_argument(
        "--symbol-list",
        help="Comma separated list of symbols (used when --symbols-from=list)",
    )
    parser.add_argument(
        "--universe-file",
        default="config/stock_universe.txt",
        help="Path to stock universe file (used when --symbols-from=file)",
    )
    parser.add_argument(
        "--what",
        default="midpoint",
        choices=["midpoint"],
        help="Historical data type (currently only midpoint supported)",
    )
    parser.add_argument(
        "--useRTH",
        type=int,
        default=1,
        choices=[0, 1],
        help="Limit historical bars to regular trading hours (1=yes)",
    )
    parser.add_argument(
        "--max-contracts-per-underlying",
        type=int,
        default=80,
        help="Maximum number of option contracts to persist per underlying",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=7,
        help="Maximum days to expiry to consider (2-7 days for short-term options)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum worker threads for historical fetch",
    )
    return parser.parse_args()


def _parse_trade_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _ib_epoch_to_ts_end(epoch: int) -> datetime:
    dt_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
    dt_et = dt_utc.astimezone(EASTERN).replace(second=0, microsecond=0)
    minute_end = dt_et + timedelta(minutes=1)
    return minute_end.astimezone(timezone.utc)


def _dec(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _int(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def _abs_delta(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return abs(value)


def _rth_filter(ts: datetime, trade_date: date) -> bool:
    start_et, end_et = trading_session_window(trade_date, tz=EASTERN)
    ts_et = ts.astimezone(EASTERN)
    return start_et <= ts_et < (end_et + timedelta(minutes=1))


def _resolve_trade_dates(dao: BacktestDAO, start_date: date, end_date: date) -> list[date]:
    dates = dao.fetch_trade_dates_between(start_date=start_date, end_date=end_date)
    return sorted({d for d in dates if start_date <= d <= end_date})


def _resolve_underlyings(
    session: Session,
    dao: BacktestDAO,
    trade_date: date,
    mode: str,
    *,
    symbol_list: Sequence[str],
    universe_file: str | None = None,
) -> list[str]:
    if mode == "list":
        return [symbol.upper() for symbol in symbol_list]

    if mode == "file":
        if not universe_file:
            raise ValueError("universe_file is required when mode=file")
        file_path = Path(universe_file)
        if not file_path.exists():
            raise FileNotFoundError(f"Universe file not found: {file_path}")
        symbols = []
        for line in file_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            symbols.append(line.upper())
        if not symbols:
            raise ValueError(f"Universe file produced no valid symbols: {file_path}")
        LOGGER.info("symbols.from_file", file=str(file_path), count=len(symbols))
        return symbols

    if mode == "premarket_top5":
        top_rows = dao.fetch_premarket_top(trade_date=trade_date)
        symbols = [row.upper() for row in top_rows]
        if symbols:
            return symbols
        LOGGER.warning("symbols.premarket_empty", trade_date=trade_date.isoformat())

    resolver = UniverseResolver(session)
    universe = resolver.resolve(None)
    return universe.symbols


def _resolve_underlying_price(
    session_factory: sessionmaker[Session],
    client: IBClient,
    symbol: str,
    trade_date: date,
) -> Decimal | None:
    """
    获取标的价格，优先级：
    1. 盘前09:25的第一根K线收盘价
    2. 前一交易日RTH收盘价（v_daily_ohlcv_enriched.prev_close_rth）
    3. 实时快照
    4. 返回None（调用方将跳过strike过滤）
    """
    with session_factory() as session:
        # 方法1：盘前09:25的第一根K线
        start_dt = datetime.combine(trade_date, time(9, 25), EASTERN)
        end_dt = datetime.combine(trade_date, time(9, 30), EASTERN)
        start_utc = start_dt.astimezone(timezone.utc)
        end_utc = end_dt.astimezone(timezone.utc)
        row = session.execute(
            text(
                """
                SELECT close
                FROM bars1m_equity
                WHERE symbol = :symbol
                  AND ts_end >= :start_ts
                  AND ts_end < :end_ts
                ORDER BY ts_end
                LIMIT 1
                """
            ),
            {"symbol": symbol, "start_ts": start_utc, "end_ts": end_utc},
        ).scalar()
        if row is not None:
            px = Decimal(str(row))
            LOGGER.info("underlying_price.pre_first_close", symbol=symbol, price=str(px))
            return px
        
        # 方法2：前一交易日RTH收盘价
        row2 = session.execute(
            text(
                """
                SELECT prev_close_rth
                FROM v_daily_ohlcv_enriched
                WHERE symbol = :symbol AND trade_date_et = :trade_date
                """
            ),
            {"symbol": symbol, "trade_date": trade_date},
        ).scalar()
        if row2 is not None:
            px = Decimal(str(row2))
            LOGGER.info("underlying_price.prev_close_rth", symbol=symbol, price=str(px))
            return px

    # 方法3：实时快照
    try:
        snapshot = client.market_data_snapshot(
            client.stock_contract(symbol),
            generic_ticks="",
            timeout=5.0,
        )
        bid = _dec(snapshot.get("bid"))
        ask = _dec(snapshot.get("ask"))
        if bid and ask and bid > 0 and ask > 0:
            px = (bid + ask) / Decimal("2")
            LOGGER.info("underlying_price.snapshot_mid", symbol=symbol, price=str(px))
            return px
        last = _dec(snapshot.get("last"))
        if last and last > 0:
            LOGGER.info("underlying_price.snapshot_last", symbol=symbol, price=str(last))
            return last
    except Exception as exc:  # pragma: no cover - external dependency
        LOGGER.warning("underlying.snapshot_failed", symbol=symbol, error=str(exc))

    # 方法4：无法获取，返回None
    LOGGER.warning("underlying_price.unavailable", symbol=symbol, trade_date=trade_date.isoformat())
    return None


def _select_option_entry(params: Sequence[Mapping], symbol: str) -> Mapping:
    symbol_up = symbol.upper()

    def score(entry: Mapping) -> tuple[int, int, int, int]:
        trading_class = str(entry.get("trading_class") or entry.get("tradingClass") or "").upper()
        exchange = str(entry.get("exchange") or "").upper()
        expirations = entry.get("expirations") or []
        strikes = entry.get("strikes") or []
        return (
            1 if trading_class == symbol_up else 0,
            1 if exchange == "SMART" else 0,
            len(expirations),
            len(strikes),
        )

    return max(params, key=score)


def _filter_otm_levels(
    candidates: list[tuple[int, Decimal, str, date]],
    spot_price: Decimal,
    min_otm: int = 2,
    max_otm: int = 7,
) -> list[tuple[int, Decimal, str, date]]:
    """
    筛选OTM（虚值）期权的指定档位
    
    OTM定义：
    - CALL: strike > spot_price（看涨期权的执行价高于现价）
    - PUT: strike < spot_price（看跌期权的执行价低于现价）
    
    档位计算：
    - 按照距离ATM的strike数量排序
    - 第1档OTM = 最接近ATM的虚值期权
    - 第2档OTM = 第2接近ATM的虚值期权
    - ...
    
    Args:
        candidates: (dte, strike, right, expiry_date) 元组列表
        spot_price: 标的现价
        min_otm: 最小OTM档位（包含），默认2
        max_otm: 最大OTM档位（包含），默认7
    
    Returns:
        筛选后的候选列表
    """
    # 按DTE分组
    by_dte: dict[int, list[tuple[int, Decimal, str, date]]] = {}
    for item in candidates:
        dte, strike, right, expiry = item
        if dte not in by_dte:
            by_dte[dte] = []
        by_dte[dte].append(item)
    
    result: list[tuple[int, Decimal, str, date]] = []
    
    for dte, items in by_dte.items():
        # 分离CALL和PUT
        calls = [(dte, strike, right, expiry) for dte, strike, right, expiry in items if right == "CALL"]
        puts = [(dte, strike, right, expiry) for dte, strike, right, expiry in items if right == "PUT"]
        
        # CALL：OTM = strike > spot，按strike升序排列，取第min_otm到max_otm档
        otm_calls = [(d, s, r, e) for d, s, r, e in calls if s > spot_price]
        otm_calls.sort(key=lambda x: x[1])  # 按strike升序
        selected_calls = otm_calls[min_otm-1:max_otm] if len(otm_calls) >= min_otm else []
        
        # PUT：OTM = strike < spot，按strike降序排列，取第min_otm到max_otm档
        otm_puts = [(d, s, r, e) for d, s, r, e in puts if s < spot_price]
        otm_puts.sort(key=lambda x: x[1], reverse=True)  # 按strike降序
        selected_puts = otm_puts[min_otm-1:max_otm] if len(otm_puts) >= min_otm else []
        
        result.extend(selected_calls)
        result.extend(selected_puts)
    
    return result


def _collect_option_metadata(
    client: IBClient,
    *,
    symbol: str,
    trade_date: date,
    underlying_price: Decimal | None,
    max_dte: int,
    strike_min: Decimal | None,
    strike_max: Decimal | None,
    max_contracts: int,
) -> list[OptionMetaRecord]:
    underlying_contract = client.stock_contract(symbol)
    try:
        underlying_detail = client.req_contract_details(underlying_contract)
    except Exception as exc:  # pragma: no cover - external dependency
        LOGGER.error("ingest.secdef_underlying_failed", symbol=symbol, error=str(exc))
        raise
    underlying_resolved = underlying_detail.contract
    if underlying_contract.conId and not underlying_resolved.conId:
        underlying_resolved.conId = underlying_contract.conId
    underlying_conid = int(underlying_resolved.conId or 0)
    if not underlying_conid:
        raise RuntimeError(f"Underlying contract for {symbol} missing conId")

    params = client.get_secdef_opt_params(
        symbol,
        exchange="",  # 使用空字符串，这是关键！
        underlying_conid=underlying_conid,
    )
    if not params:
        LOGGER.warning("secdef.empty", symbol=symbol)
        return []
    
    smart_entry = _select_option_entry(params, symbol)
    returned_exchange = smart_entry.get("exchange", "UNKNOWN")
    LOGGER.debug("secdef.returned_exchange", symbol=symbol, exchange=returned_exchange)
    expirations = sorted(set(smart_entry.get("expirations") or []))
    strike_values = sorted(set(Decimal(str(val)) for val in smart_entry.get("strikes") or []))
    multiplier = str(smart_entry.get("multiplier") or "100")
    trading_class = smart_entry.get("trading_class") or smart_entry.get("tradingClass")
    
    LOGGER.info(
        "secdef_count",
        symbol=symbol,
        expirations=len(expirations),
        strikes=len(strike_values),
        trading_class=trading_class,
    )

    # 调试：打印前5个到期日及其DTE
    debug_exp_dte = []
    for i, expiry_token in enumerate(expirations[:5]):
        try:
            expiry_date = datetime.strptime(expiry_token, "%Y%m%d").date()
            dte = (expiry_date - trade_date).days
            debug_exp_dte.append(f"{expiry_token}(DTE={dte})")
        except ValueError:
            pass
    if debug_exp_dte:
        LOGGER.info(
            "debug_expiries", 
            symbol=symbol, 
            trade_date=trade_date.isoformat(),
            sample_exp=", ".join(debug_exp_dte),
            max_dte=max_dte,
        )

    # 第一步：按DTE过滤（2-7天，短期期权策略）
    candidates_after_dte: list[tuple[int, Decimal, str, date]] = []
    for expiry_token in expirations:
        try:
            expiry_date = datetime.strptime(expiry_token, "%Y%m%d").date()
        except ValueError:
            continue
        dte = (expiry_date - trade_date).days
        # DTE范围：2-7天（短期期权策略的典型到期日范围）
        if dte < 2 or dte > max_dte:
            continue
        for strike in strike_values:
            candidates_after_dte.append((dte, strike, "CALL", expiry_date))
            candidates_after_dte.append((dte, strike, "PUT", expiry_date))
    
    LOGGER.info("after_dte", symbol=symbol, count=len(candidates_after_dte))
    
    if not candidates_after_dte:
        LOGGER.warning("no_contracts_after_dte", symbol=symbol, trade_date=trade_date.isoformat())
        return []
    
    # 第二步：按strike窗口过滤（如果有underlying_price的话）
    candidates: list[tuple[int, Decimal, str, date]] = []
    if strike_min is not None and strike_max is not None:
        for dte, strike, right, expiry_date in candidates_after_dte:
            if strike < strike_min or strike > strike_max:
                continue
            candidates.append((dte, strike, right, expiry_date))
        
        LOGGER.info("after_strike", symbol=symbol, count=len(candidates))
        if not candidates:
            LOGGER.warning(
                "no_contracts_after_strike",
                symbol=symbol,
                strike_min=str(strike_min),
                strike_max=str(strike_max),
                underlying_price=str(underlying_price) if underlying_price else "N/A",
            )
            return []
    else:
        # 没有underlying_price，跳过strike过滤
        candidates = candidates_after_dte
        LOGGER.warning(
            "skip_strike_filter",
            symbol=symbol,
            reason="underlying_price_unavailable",
            candidate_count=len(candidates),
        )

    # 第三步：OTM档位筛选（只选择2-7档虚值期权）
    if underlying_price:
        otm_candidates = _filter_otm_levels(candidates, underlying_price, min_otm=2, max_otm=7)
        LOGGER.info(
            "after_otm_filter",
            symbol=symbol,
            before=len(candidates),
            after=len(otm_candidates),
            spot=str(underlying_price),
        )
        candidates = otm_candidates
    
    # 排序：按DTE，然后按与标的价格的距离
    if underlying_price:
        candidates.sort(key=lambda item: (item[0], abs(item[1] - underlying_price)))
    else:
        candidates.sort(key=lambda item: (item[0], item[1]))  # 按DTE和strike排序

    records: list[OptionMetaRecord] = []
    seen: set[int] = set()

    for dte, strike, right, expiry_date in candidates:
        if len(records) >= max_contracts:
            break
        contract = client.option_contract(
            symbol,
            expiry=expiry_date.strftime("%Y%m%d"),
            strike=float(strike),
            right=right,
            exchange="SMART",
            multiplier=multiplier,
            include_expired=True,  # 关键：历史数据需要包含已过期合约
        )
        try:
            detail = client.req_contract_details(contract)
        except Exception as exc:  # pragma: no cover - external dependency
            LOGGER.warning(
                "option.contract_detail_failed",
                symbol=symbol,
                expiry=expiry_date.isoformat(),
                strike=str(strike),
                right=right,
                error=str(exc),
            )
            continue

        resolved = detail.contract
        conid = int(resolved.conId or contract.conId or 0)
        if not conid or conid in seen:
            continue
        seen.add(conid)

        resolved.exchange = resolved.exchange or detail.validExchanges.split(",")[0]
        resolved.includeExpired = True

        # 元数据阶段：尝试获取快照，但不强制要求bid/ask
        # 流动性过滤将在后续bar拉取阶段进行
        try:
            snapshot = client.market_data_snapshot(
                resolved,
                generic_ticks="100,101,104,106,588,611",
                timeout=5.0,
            )
        except Exception as exc:  # pragma: no cover - external dependency
            LOGGER.warning(
                "option.snapshot_failed",
                symbol=symbol,
                conid=conid,
                error=str(exc),
            )
            snapshot = {}

        bid = _dec(snapshot.get("bid"))
        ask = _dec(snapshot.get("ask"))
        mid = None
        if bid and ask and bid > 0 and ask > 0:
            mid = (bid + ask) / Decimal("2")
        
        min_tick = _dec(detail.minTick)

        records.append(
            OptionMetaRecord(
                trade_date=trade_date,
                underlying_symbol=symbol,
                conid=conid,
                expiry=expiry_date,
                right=right,
                strike=strike,
                trading_class=resolved.tradingClass or trading_class,
                multiplier=resolved.multiplier or multiplier,
                exchange=resolved.exchange or "SMART",
                dte=dte,
                delta=_dec(snapshot.get("delta")),
                gamma=_dec(snapshot.get("gamma")),
                theta=_dec(snapshot.get("theta")),
                vega=_dec(snapshot.get("vega")),
                implied_vol=_dec(snapshot.get("implied_vol") or snapshot.get("iv")),
                bid=bid,
                ask=ask,
                mid=mid,
                open_interest=_int(snapshot.get("open_interest")),
                volume=_int(snapshot.get("option_volume") or snapshot.get("volume")),
                min_tick=min_tick,
                underlying_price=underlying_price,
            )
        )
    
    LOGGER.info("after_contract_details", symbol=symbol, count=len(records))
    return records


def _select_bar_candidates(records: Sequence[OptionMetaRecord]) -> list[OptionMetaRecord]:
    filtered: list[OptionMetaRecord] = []
    for record in records:
        if record.dte < 2 or record.dte > 7:
            continue
        if record.bid is None or record.ask is None or record.mid is None:
            continue
        spread = record.ask - record.bid
        if spread <= 0:
            continue
        threshold = max(Decimal("0.10"), record.mid * Decimal("0.05"))
        if spread > threshold:
            continue
        oi = record.open_interest or 0
        vol = record.volume or 0
        if oi < 100 or vol < 20:
            continue
        abs_delta = _abs_delta(record.delta)
        if abs_delta is None or abs_delta < Decimal("0.35") or abs_delta > Decimal("0.45"):
            continue
        filtered.append(record)

    filtered.sort(
        key=lambda rec: (
            abs((_abs_delta(rec.delta) or Decimal("0")) - Decimal("0.40")),
            abs(rec.strike - rec.underlying_price),
        )
    )
    return filtered


def _fetch_option_bars(
    client: IBClient,
    record: OptionMetaRecord,
    *,
    trade_date: date,
    use_rth: bool,
) -> list[OptionBarRecord] | None:
    contract = client.option_contract(
        record.underlying_symbol,
        expiry=record.expiry.strftime("%Y%m%d"),
        strike=float(record.strike),
        right=record.right,
        exchange=record.exchange or "SMART",
        multiplier=record.multiplier or "100",
        conid=record.conid,
    )

    try:
        mid_bars = client.get_hist_1m_option(
            contract,
            trade_date=trade_date,
            what_to_show="MIDPOINT",
            use_rth=use_rth,
            include_expired=True,
        )
        bid_bars = client.get_hist_1m_option(
            contract,
            trade_date=trade_date,
            what_to_show="BID",
            use_rth=use_rth,
            include_expired=True,
        )
        ask_bars = client.get_hist_1m_option(
            contract,
            trade_date=trade_date,
            what_to_show="ASK",
            use_rth=use_rth,
            include_expired=True,
        )
    except Exception as exc:  # pragma: no cover - external dependency
        LOGGER.warning(
            "option.historical_failed",
            conid=record.conid,
            symbol=record.underlying_symbol,
            error=str(exc),
        )
        return None

    if not mid_bars or not bid_bars or not ask_bars:
        return None

    def build_series(items: Iterable[Mapping[str, object]]) -> dict[datetime, Decimal]:
        series: dict[datetime, Decimal] = {}
        for bar in items:
            try:
                epoch = int(bar.get("time"))
            except (TypeError, ValueError):
                continue
            ts_end = _ib_epoch_to_ts_end(epoch)
            price = _dec(bar.get("close") or bar.get("price"))
            if price is None:
                continue
            if not _rth_filter(ts_end, trade_date):
                continue
            series[ts_end] = price
        return series

    mid_series = build_series(mid_bars)
    bid_series = build_series(bid_bars)
    ask_series = build_series(ask_bars)

    common_ts = sorted(set(mid_series) & set(bid_series) & set(ask_series))
    if len(common_ts) < 300:
        return None

    records: list[OptionBarRecord] = []
    for ts in common_ts:
        bid = bid_series[ts]
        ask = ask_series[ts]
        if bid <= 0 or ask <= 0 or bid >= ask:
            continue
        mid = (bid + ask) / Decimal("2")
        volume = None
        records.append(
            OptionBarRecord(
                conid=record.conid,
                ts_end=ts,
                underlying_symbol=record.underlying_symbol,
                expiry=record.expiry,
                right=record.right,
                strike=record.strike,
                trading_class=record.trading_class,
                multiplier=record.multiplier,
                exchange=record.exchange,
                bid=bid,
                ask=ask,
                mid=mid,
                volume=volume,
                open_interest=record.open_interest,
            )
        )

    return records if records else None


def _persist_option_chain_meta(session: Session, records: Sequence[OptionMetaRecord]) -> None:
    if not records:
        return
    # 注意：只插入数据库表中实际存在的列
    # 表结构：conid, trade_date, underlying_symbol, expiry, strike, right, dte, delta, bid, ask, mid, open_interest, volume, min_tick
    insert_sql = text(
        """
        INSERT INTO option_chain_meta (
            trade_date,
            underlying_symbol,
            conid,
            expiry,
            strike,
            "right",
            dte,
            delta,
            bid,
            ask,
            mid,
            open_interest,
            volume,
            min_tick
        ) VALUES (
            :trade_date,
            :underlying_symbol,
            :conid,
            :expiry,
            :strike,
            :right,
            :dte,
            :delta,
            COALESCE(:bid, 0),
            COALESCE(:ask, 0),
            COALESCE(:mid, 0),
            COALESCE(:open_interest, 0),
            COALESCE(:volume, 0),
            COALESCE(:min_tick, 0.01)
        )
        ON CONFLICT (trade_date, conid) DO UPDATE SET
            underlying_symbol = EXCLUDED.underlying_symbol,
            expiry = EXCLUDED.expiry,
            strike = EXCLUDED.strike,
            "right" = EXCLUDED."right",
            dte = EXCLUDED.dte,
            delta = EXCLUDED.delta,
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            mid = EXCLUDED.mid,
            open_interest = EXCLUDED.open_interest,
            volume = EXCLUDED.volume,
            min_tick = EXCLUDED.min_tick
        """
    )

    payloads = [record.__dict__ for record in records]
    session.execute(insert_sql, payloads)


def _persist_option_bars(session: Session, records: Sequence[OptionBarRecord]) -> None:
    if not records:
        return
    insert_sql = text(
        """
        INSERT INTO bars1m_option (
            conid,
            ts_end,
            underlying_symbol,
            expiry,
            right,
            strike,
            trading_class,
            multiplier,
            exchange,
            bid,
            ask,
            mid,
            volume,
            open_interest
        ) VALUES (
            :conid,
            :ts_end,
            :underlying_symbol,
            :expiry,
            :right,
            :strike,
            :trading_class,
            :multiplier,
            :exchange,
            :bid,
            :ask,
            :mid,
            :volume,
            :open_interest
        )
        ON CONFLICT (conid, ts_end) DO UPDATE SET
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            mid = EXCLUDED.mid,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            updated_at = now()
        """
    )
    payloads = [record.__dict__ for record in records]
    session.execute(insert_sql, payloads)


def _record_risk_event(
    session: Session,
    *,
    trade_date: date,
    symbol: str | None,
    code: str,
    message: str,
    payload: Mapping[str, object],
) -> None:
    payload_json = json.dumps(dict(payload, trade_date=trade_date.isoformat(), message=message))
    session.execute(
        text(
            """
            INSERT INTO risk_events (event_ts, symbol, event_code, severity, payload)
            VALUES (:event_ts, :symbol, :event_code, :severity, CAST(:payload AS jsonb))
            """
        ),
        {
            "event_ts": datetime.now(timezone.utc),
            "symbol": symbol,
            "event_code": code,
            "severity": "WARN",
            "payload": payload_json,
        },
    )


def main() -> int:
    args = _parse_args()
    settings = get_settings()
    configure_logging(settings)

    date_from = _parse_trade_date(args.date_from)
    date_to = _parse_trade_date(args.date_to)
    if date_to < date_from:
        raise SystemExit("--date-to must not be earlier than --date-from")

    symbol_list = []
    if args.symbols_from == "list":
        if not args.symbol_list:
            raise SystemExit("--symbol-list is required when --symbols-from=list")
        symbol_list = [token.strip().upper() for token in args.symbol_list.split(",") if token.strip()]
        if not symbol_list:
            raise SystemExit("--symbol-list produced no valid symbols")

    session_factory = get_session_factory(settings)
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        trade_dates = _resolve_trade_dates(dao, date_from, date_to)
        if not trade_dates:
            raise SystemExit("No trading dates available in requested window")

    client = build_ibkr_client(settings)
    try:
        try:
            client.reqMarketDataType(1)
        except Exception as exc:  # pragma: no cover - external dependency
            LOGGER.error("marketdatatype.live_failed", error=str(exc))
            raise RuntimeError("缺少期权实时行情订阅") from exc

        strike_lower_pct = Decimal(str(settings.option_strike_lower_pct))
        strike_upper_pct = Decimal(str(settings.option_strike_upper_pct))

        failures: dict[date, list[str]] = defaultdict(list)
        hist_workers = max(1, int(args.max_workers))

        for trade_date in trade_dates:
            LOGGER.info("ingest.day_start", trade_date=trade_date.isoformat())
            with session_factory() as session:
                dao = BacktestDAO(
                    session,
                    option_bar_table=settings.option_bar_table,
                    option_chain_table=settings.option_chain_table,
                )
                symbols = _resolve_underlyings(
                    session,
                    dao,
                    trade_date,
                    args.symbols_from,
                    symbol_list=symbol_list,
                    universe_file=args.universe_file,
                )
                if not symbols:
                    LOGGER.warning("ingest.symbols_empty", trade_date=trade_date.isoformat())
                    failures[trade_date].append("NO_SYMBOLS")
                    _record_risk_event(
                        session,
                        trade_date=trade_date,
                        symbol=None,
                        code="OPTION_CHAIN_EMPTY",
                        message="No symbols available for ingestion",
                        payload={},
                    )
                    session.commit()
                    continue

                day_meta: list[OptionMetaRecord] = []
                day_bars: list[OptionBarRecord] = []

                for symbol in symbols:
                    LOGGER.info("ingest.symbol_start", trade_date=trade_date.isoformat(), symbol=symbol)
                    spot = _resolve_underlying_price(session_factory, client, symbol, trade_date)
                    
                    # 如果获取到了标的价格，则计算strike窗口；否则为None（将跳过strike过滤）
                    if spot is not None:
                        strike_min = (spot * strike_lower_pct).quantize(Decimal("0.01"))
                        strike_max = (spot * strike_upper_pct).quantize(Decimal("0.01"))
                    else:
                        strike_min = None
                        strike_max = None

                    meta_records = _collect_option_metadata(
                        client,
                        symbol=symbol,
                        trade_date=trade_date,
                        underlying_price=spot,
                        max_dte=args.max_dte,
                        strike_min=strike_min,
                        strike_max=strike_max,
                        max_contracts=args.max_contracts_per_underlying,
                    )

                    if not meta_records:
                        LOGGER.warning("ingest.meta_empty", symbol=symbol, trade_date=trade_date.isoformat())
                        failures[trade_date].append(symbol)
                        _record_risk_event(
                            session,
                            trade_date=trade_date,
                            symbol=symbol,
                            code="OPTION_CHAIN_EMPTY",
                            message="No option contracts after filtering",
                            payload={},
                        )
                        session.commit()
                        continue

                    day_meta.extend(meta_records)
                    bar_candidates = _select_bar_candidates(meta_records)

                    if not bar_candidates:
                        LOGGER.warning("ingest.bars_candidates_empty", symbol=symbol, trade_date=trade_date.isoformat())
                        failures[trade_date].append(symbol)
                        _record_risk_event(
                            session,
                            trade_date=trade_date,
                            symbol=symbol,
                            code="OPTION_BAR_MISSING",
                            message="No eligible contracts for minute bars",
                            payload={},
                        )
                        session.commit()
                        continue

                    selected = bar_candidates[: args.max_contracts_per_underlying]
                    symbol_bars: list[OptionBarRecord] = []

                    def fetch_bar(record: OptionMetaRecord) -> tuple[int, list[OptionBarRecord] | None]:
                        bars = _fetch_option_bars(
                            client,
                            record,
                            trade_date=trade_date,
                            use_rth=bool(args.useRTH),
                        )
                        return record.conid, bars

                    with ThreadPoolExecutor(max_workers=hist_workers) as executor:
                        futures = {executor.submit(fetch_bar, record): record for record in selected}
                        for future in futures:
                            record = futures[future]
                            try:
                                _, bars = future.result()
                            except Exception as exc:  # pragma: no cover - concurrency guard
                                LOGGER.error(
                                    "ingest.bars_exception",
                                    conid=record.conid,
                                    symbol=symbol,
                                    error=str(exc),
                                )
                                bars = None
                            if bars is None:
                                failures[trade_date].append(symbol)
                                LOGGER.warning(
                                    "ingest.bars_missing",
                                    conid=record.conid,
                                    symbol=symbol,
                                    trade_date=trade_date.isoformat(),
                                )
                                continue
                            symbol_bars.extend(bars)

                    if not symbol_bars:
                        _record_risk_event(
                            session,
                            trade_date=trade_date,
                            symbol=symbol,
                            code="OPTION_BAR_MISSING",
                            message="Historical data returned no valid bars",
                            payload={},
                        )
                        session.commit()
                        continue

                    day_bars.extend(symbol_bars)
                    LOGGER.info(
                        "ingest.symbol_complete",
                        symbol=symbol,
                        trade_date=trade_date.isoformat(),
                        contracts=len(meta_records),
                        bars=len(symbol_bars),
                    )

                if not day_meta:
                    _record_risk_event(
                        session,
                        trade_date=trade_date,
                        symbol=None,
                        code="OPTION_CHAIN_EMPTY",
                        message="No option metadata persisted for day",
                        payload={},
                    )
                    session.commit()
                    continue

                _persist_option_chain_meta(session, day_meta)
                _persist_option_bars(session, day_bars)
                session.commit()
                LOGGER.info(
                    "ingest.day_complete",
                    trade_date=trade_date.isoformat(),
                    contracts=len(day_meta),
                    bar_rows=len(day_bars),
                )

        if failures:
            failure_days = ", ".join(f"{d}:{sorted(set(v))}" for d, v in failures.items())
            LOGGER.error("ingest.failures_detected", details=failure_days)
            return 1
        return 0
    finally:
        client.disconnect_and_stop()


if __name__ == "__main__":
    raise SystemExit(main())
