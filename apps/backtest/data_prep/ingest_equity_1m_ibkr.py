# path: scripts/ingest_equity_1m_ibkr.py
"""
从IBKR获取股票1分钟K线数据并写入TimescaleDB。

数据获取策略：
  - 外层循环：遍历交易日（按日期顺序）
  - 内层循环：遍历股票列表
  - 每次仅请求单个股票在单个交易日的数据
  - 每次请求后延时2秒，遵守IBKR API调频限制
  
这种策略的优点：
  1. 避免长时间阻塞：每次请求的数据量小，响应快
  2. 防止API限流：通过延时避免触发Pacing Violation
  3. 清晰的进度反馈：每次请求都有日志输出
  4. 容错性好：单个请求失败不影响后续处理
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Mapping, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from apps.rt_engine.indicators import compute_indicator_row
from apps.backtest.dao import BacktestDAO
from apps.backtest.universe import UniverseResolver
from apps.backtest.data_prep.metrics import DataIngestionMonitor, start_metrics_server
from libs.core import EASTERN, configure_logging, get_settings
from libs.infra import build_ibkr_client, get_session_factory
import os

LOGGER = logging.getLogger("ingest_equity_1m")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest 1m equity bars from IBKR into TimescaleDB")
    parser.add_argument("--date", help="Single trade date in YYYY-MM-DD (Eastern)")
    parser.add_argument("--start", help="Start trade date (inclusive, YYYY-MM-DD)")
    parser.add_argument("--end", help="End trade date (inclusive, YYYY-MM-DD)")
    parser.add_argument("--symbols", nargs="+", help="Explicit equity symbols to ingest")
    parser.add_argument(
        "--universe",
        default=None,
        help="Universe specification (default ref_market_cap:GLOBAL when --symbols omitted)",
    )
    parser.add_argument(
        "--symbols-offset",
        type=int,
        default=0,
        help="Skip the first N symbols after universe resolution (default 0)",
    )
    parser.add_argument(
        "--symbols-limit",
        type=int,
        help="Limit number of symbols processed after applying offset",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Process symbols in batches of N; defaults to all symbols in one batch",
    )
    parser.add_argument(
        "--rth-only",
        type=int,
        default=0,
        choices=[0, 1],
        help="When set to 1 only request Regular Trading Hours (09:30-16:00), when 0 (default) includes pre-market (09:00-16:00)",
    )
    args = parser.parse_args()
    if not args.date:
        if not (args.start and args.end):
            parser.error("either --date or both --start/--end must be provided")
    if args.date and (args.start or args.end):
        parser.error("--date cannot be combined with --start/--end")
    return args


def _to_trade_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid --date value {value!r}: must be YYYY-MM-DD") from exc


def _ib_epoch_to_ts_end(epoch: int) -> datetime:
    dt_utc = datetime.fromtimestamp(epoch, tz=timezone.utc)
    dt_et = dt_utc.astimezone(EASTERN).replace(second=0, microsecond=0)
    minute_end = dt_et + timedelta(minutes=1)
    # 去掉时区信息，让PostgreSQL按东部时间字面值存储
    return minute_end.replace(tzinfo=None)


def _bars_to_records(bars: Sequence[Mapping[str, object]]) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for bar in bars:
        raw_time = bar.get("time")
        if raw_time is None:
            continue
        epoch = int(raw_time)
        ts_end = _ib_epoch_to_ts_end(epoch)
        record = {
            "ts_end": ts_end,
            "open": float(bar.get("open") or 0.0),
            "high": float(bar.get("high") or 0.0),
            "low": float(bar.get("low") or 0.0),
            "close": float(bar.get("close") or 0.0),
            "volume": int(bar.get("volume") or 0),
        }
        records.append(record)
    records.sort(key=lambda row: row["ts_end"])
    return records


def _expected_rth_minutes(trade_date: date) -> int:
    """
    Calculate expected RTH minutes from 08:00 to 16:00 (inclusive).
    This includes the minute at 16:00, so total is 481 minutes.
    Note: Data starts from 08:00 to ensure sufficient history for indicators at 09:34.
    """
    start = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
        hour=8, minute=0
    )
    end = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
        hour=16, minute=1  # 16:01 to include the 16:00 minute
    )
    return int((end - start).total_seconds() / 60)


def _load_baseline_map(session: Session, symbol: str) -> Dict[int, Decimal]:
    """
    加载RVOL基线数据（已废弃）
    
    由于RVOL6现在基于K线数据直接计算，不再需要baseline数据。
    返回空字典，让指标计算使用滚动窗口方法。
    """
    return {}


def _upsert_indicator_row(
    session: Session,
    symbol: str,
    ts_end: datetime,
    df: pd.DataFrame,
    baseline_map: Mapping[int, Decimal],
) -> None:
    metrics = compute_indicator_row(df, baseline_map)
    if not metrics:
        return
    payload = {
        "symbol": symbol,
        "ts_end": ts_end,
        **{key: Decimal(str(value)) if value is not None else None for key, value in metrics.items()},
    }
    session.execute(
        text(
            """
            INSERT INTO indicators_eq_1m (
                symbol, ts_end,
                rsi6, rsi12, rsi24,
                boll_mid, boll_up, boll_dn,
                atr14, ao,
                stoch_k, stoch_d, stoch_rsi_k, stoch_rsi_d,
                cci14, cci6,
                sma5, lr_m5_slope, lr_boll_dn_slope, lr_obv_slope,
                obv, obv_ma6, obv_ema20,
                mfi14, rvol6
            ) VALUES (
                :symbol, :ts_end,
                :rsi6, :rsi12, :rsi24,
                :boll_mid, :boll_up, :boll_dn,
                :atr14, :ao,
                :stoch_k, :stoch_d, :stoch_rsi_k, :stoch_rsi_d,
                :cci14, :cci6,
                :sma5, :lr_m5_slope, :lr_boll_dn_slope, :lr_obv_slope,
                :obv, :obv_ma6, :obv_ema20,
                :mfi14, :rvol6
            )
            ON CONFLICT (symbol, ts_end) DO UPDATE SET
                rsi6 = EXCLUDED.rsi6,
                rsi12 = EXCLUDED.rsi12,
                rsi24 = EXCLUDED.rsi24,
                boll_mid = EXCLUDED.boll_mid,
                boll_up = EXCLUDED.boll_up,
                boll_dn = EXCLUDED.boll_dn,
                atr14 = EXCLUDED.atr14,
                ao = EXCLUDED.ao,
                stoch_k = EXCLUDED.stoch_k,
                stoch_d = EXCLUDED.stoch_d,
                stoch_rsi_k = EXCLUDED.stoch_rsi_k,
                stoch_rsi_d = EXCLUDED.stoch_rsi_d,
                cci14 = EXCLUDED.cci14,
                cci6 = EXCLUDED.cci6,
                sma5 = EXCLUDED.sma5,
                lr_m5_slope = EXCLUDED.lr_m5_slope,
                lr_boll_dn_slope = EXCLUDED.lr_boll_dn_slope,
                lr_obv_slope = EXCLUDED.lr_obv_slope,
                obv = EXCLUDED.obv,
                obv_ma6 = EXCLUDED.obv_ma6,
                obv_ema20 = EXCLUDED.obv_ema20,
                mfi14 = EXCLUDED.mfi14,
                rvol6 = EXCLUDED.rvol6
            """
        ),
        payload,
    )


def _store_equity_rows(
    session_factory: sessionmaker[Session],
    symbol: str,
    records: Sequence[Mapping[str, object]],
) -> None:
    if not records:
        raise RuntimeError(f"No bars to persist for {symbol}")
    insert_sql = text(
        """
        INSERT INTO bars1m_equity (ts_end, symbol, open, high, low, close, volume)
        VALUES (:ts_end, :symbol, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, ts_end) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
    )
    with session_factory() as session:
        for row in records:
            session.execute(
                insert_sql,
                {
                    "ts_end": row["ts_end"],
                    "symbol": symbol,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                },
            )
        session.commit()


def _store_vix_to_risk_state(
    session_factory: sessionmaker[Session],
    records: Sequence[Mapping[str, object]],
) -> None:
    """
    将VIX数据写入risk_state表
    
    每条记录包含：
    - ts: 时间戳（K线结束时间）
    - close: VIX收盘价（作为metric_value）
    """
    if not records:
        return
    
    insert_sql = text(
        """
        INSERT INTO risk_state (ts, symbol, metric_code, metric_value)
        VALUES (:ts, 'GLOBAL', 'VIX', :metric_value)
        ON CONFLICT (ts, symbol, metric_code) DO UPDATE SET
            metric_value = EXCLUDED.metric_value
        """
    )
    
    with session_factory() as session:
        for row in records:
            session.execute(
                insert_sql,
                {
                    "ts": row["ts_end"],
                    "metric_value": row["close"],
                },
            )
        session.commit()
        LOGGER.info("Stored %d VIX records to risk_state", len(records))


def _recompute_indicators(
    session_factory: sessionmaker[Session],
    symbol: str,
    trade_date: date,
) -> None:
    start_window = datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc) - timedelta(
        days=7
    )
    with session_factory() as session:
        rows = session.execute(
            text(
                """
                SELECT ts_end, open, high, low, close, volume
                FROM bars1m_equity
                WHERE symbol = :symbol
                  AND ts_end >= :start_window
                ORDER BY ts_end ASC
                """
            ),
            {
                "symbol": symbol,
                "start_window": start_window,
            },
        ).all()
        if not rows:
            return
        df = pd.DataFrame(rows, columns=["ts_end", "open", "high", "low", "close", "volume"])
        df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)
        df.set_index("ts_end", inplace=True)
        baseline_map = _load_baseline_map(session, symbol)
        
        # 批量计算指标（仅针对今天的数据）
        trade_date_start = datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc)
        trade_date_end = trade_date_start + timedelta(days=1)
        
        # 一次性计算所有指标
        from apps.rt_engine.indicators import compute_indicators
        indicators_df = compute_indicators(df, baseline_map=baseline_map)
        
        # 只更新今天的数据
        today_mask = (df.index >= trade_date_start) & (df.index < trade_date_end)
        today_indices = df.index[today_mask]
        
        # 批量upsert
        for ts_end in today_indices:
            if ts_end not in indicators_df.index:
                continue
            indicator_row = indicators_df.loc[ts_end]
            payload = {
                "symbol": symbol,
                "ts_end": ts_end,
            }
            for col in indicator_row.index:
                val = indicator_row[col]
                if pd.isna(val):
                    payload[col] = None
                else:
                    payload[col] = Decimal(str(float(val)))
            
            session.execute(
                text(
                    """
                    INSERT INTO indicators_eq_1m (
                        symbol, ts_end,
                        rsi6, rsi12, rsi24,
                        boll_mid, boll_up, boll_dn,
                        atr14, ao,
                        stoch_k, stoch_d, stoch_rsi_k, stoch_rsi_d,
                        cci14, cci6,
                        sma5, lr_m5_slope, lr_boll_dn_slope, lr_obv_slope,
                        obv, obv_ma6, obv_ema20,
                        mfi14, rvol6
                    ) VALUES (
                        :symbol, :ts_end,
                        :rsi6, :rsi12, :rsi24,
                        :boll_mid, :boll_up, :boll_dn,
                        :atr14, :ao,
                        :stoch_k, :stoch_d, :stoch_rsi_k, :stoch_rsi_d,
                        :cci14, :cci6,
                        :sma5, :lr_m5_slope, :lr_boll_dn_slope, :lr_obv_slope,
                        :obv, :obv_ma6, :obv_ema20,
                        :mfi14, :rvol6
                    )
                    ON CONFLICT (symbol, ts_end) DO UPDATE SET
                        rsi6 = EXCLUDED.rsi6,
                        rsi12 = EXCLUDED.rsi12,
                        rsi24 = EXCLUDED.rsi24,
                        boll_mid = EXCLUDED.boll_mid,
                        boll_up = EXCLUDED.boll_up,
                        boll_dn = EXCLUDED.boll_dn,
                        atr14 = EXCLUDED.atr14,
                        ao = EXCLUDED.ao,
                        stoch_k = EXCLUDED.stoch_k,
                        stoch_d = EXCLUDED.stoch_d,
                        stoch_rsi_k = EXCLUDED.stoch_rsi_k,
                        stoch_rsi_d = EXCLUDED.stoch_rsi_d,
                        cci14 = EXCLUDED.cci14,
                        cci6 = EXCLUDED.cci6,
                        sma5 = EXCLUDED.sma5,
                        lr_m5_slope = EXCLUDED.lr_m5_slope,
                        lr_boll_dn_slope = EXCLUDED.lr_boll_dn_slope,
                        lr_obv_slope = EXCLUDED.lr_obv_slope,
                        obv = EXCLUDED.obv,
                        obv_ma6 = EXCLUDED.obv_ma6,
                        obv_ema20 = EXCLUDED.obv_ema20,
                        mfi14 = EXCLUDED.mfi14,
                        rvol6 = EXCLUDED.rvol6
                    """
                ),
                payload,
            )
        session.commit()


def _refresh_materialized_views(session_factory: sessionmaker[Session]) -> None:
    matviews = ("mv_daily_ohlcv_bounds", "mv_daily_ind_last")
    with session_factory() as session:
        for mv_name in matviews:
            exists = session.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_matviews
                        WHERE schemaname = 'public' AND matviewname = :name
                    )
                    """
                ),
                {"name": mv_name},
            ).scalar()
            if not exists:
                continue
            LOGGER.info("Refreshing materialized view %s", mv_name)
            session.execute(text(f"REFRESH MATERIALIZED VIEW {mv_name}"))
        session.commit()


def _validate_rth_count(
    session_factory: sessionmaker[Session],
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    expected: int,
) -> None:
    with session_factory() as session:
        count = session.execute(
            text(
                """
                SELECT COUNT(*) FROM bars1m_equity
                WHERE symbol = :symbol
                  AND ts_end >= :start_ts
                  AND ts_end <= :end_ts
                """
            ),
            {
                "symbol": symbol,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        ).scalar()
    if count is None or count < expected:
        raise RuntimeError(
            f"{symbol} RTH minute count insufficient ({count or 0}/{expected}) "
            f"for window {start_ts.isoformat()} – {end_ts.isoformat()}"
        )


def _request_equity_bars_with_retry(
    client,
    *,
    symbol: str,
    trade_date: date,
    rth_only: bool,
    monitor: Optional[DataIngestionMonitor] = None,
    max_attempts: int = 4,
    backoff_seconds: float = 5.0,
):
    # 记录请求
    if monitor:
        monitor.record_bars_request(symbol, trade_date)
    
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return client.req_historical_1m_equity(symbol, trade_date, rth_only=rth_only)
        except Exception as exc:  # pragma: no cover - network/ib failure
            last_exc = exc
            
            # 记录重试
            if monitor:
                monitor.record_ibkr_retry(symbol, trade_date, attempt)
                
                # 检测 pacing violation
                error_msg = str(exc).lower()
                if "pacing" in error_msg or "rate" in error_msg:
                    monitor.record_ibkr_pacing_violation()
                
                # 记录错误
                error_code = type(exc).__name__
                monitor.record_ibkr_error(error_code, symbol, trade_date)
            
            wait_time = backoff_seconds * attempt
            LOGGER.warning(
                "ingest_equity.retry symbol=%s trade_date=%s attempt=%d/%d wait=%ds error=%s",
                symbol,
                trade_date.isoformat(),
                attempt,
                max_attempts,
                wait_time,
                str(exc),
            )
            if attempt == max_attempts:
                break
            time.sleep(wait_time)
    raise RuntimeError(f"Failed to fetch historical data for {symbol}: {last_exc}")


def ingest_vix(
    client,
    session_factory: sessionmaker[Session],
    trade_date: date,
) -> None:
    """
    从IBKR拉取VIX指数的日级别数据并写入risk_state表
    
    由于IBKR不支持VIX的分钟级历史数据，我们拉取日级别的VIX收盘价，
    然后将其应用到当天交易时间的所有分钟（08:00-16:00）。
    
    Args:
        client: IBKR客户端
        session_factory: SQLAlchemy session factory
        trade_date: 交易日期
    """
    LOGGER.info("Fetching VIX daily data for trade_date=%s", trade_date.isoformat())
    
    try:
        # 使用IBKR客户端获取VIX日级别数据
        vix_close = client.req_historical_daily_vix(trade_date)
        
        if vix_close is None:
            LOGGER.warning("No VIX data returned for trade_date=%s", trade_date.isoformat())
            return
        
        # 为当天所有交易分钟创建VIX记录（08:00-16:00）
        records = []
        start_et = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
            hour=8, minute=0
        )
        end_et = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
            hour=16, minute=0
        )
        
        current = start_et + timedelta(minutes=1)  # 第一分钟结束时间是08:01
        while current <= end_et + timedelta(minutes=1):
            records.append({
                "ts_end": current.replace(tzinfo=None),  # 存储为naive datetime (ET字面值)
                "close": vix_close,
            })
            current += timedelta(minutes=1)
        
        # 存储到risk_state表
        _store_vix_to_risk_state(session_factory, records)
        
        LOGGER.info(
            "VIX data ingestion complete: trade_date=%s vix_close=%s records=%d",
            trade_date.isoformat(),
            vix_close,
            len(records),
        )
        
    except Exception as exc:
        LOGGER.error(
            "Failed to fetch VIX data for trade_date=%s: %s",
            trade_date.isoformat(),
            str(exc),
        )
        # VIX拉取失败不应该中断整个流程（已记录错误日志）


def ingest_equity(
    client,
    session_factory: sessionmaker[Session],
    trade_date: date,
    symbol: str,
    *,
    rth_only: bool,
    monitor: Optional[DataIngestionMonitor] = None,
) -> None:
    LOGGER.info("Fetching 1m bars symbol=%s trade_date=%s", symbol, trade_date.isoformat())
    bars = _request_equity_bars_with_retry(
        client,
        symbol=symbol,
        trade_date=trade_date,
        rth_only=rth_only,
        monitor=monitor,
    )
    records = _bars_to_records(bars)
    if not records:
        raise RuntimeError(f"IBKR returned no bars for {symbol} on {trade_date}")
    
    # 记录接收到的 bars
    if monitor:
        monitor.record_bars_received(symbol, trade_date, len(records))
    
    _store_equity_rows(session_factory, symbol, records)
    
    # 记录存储成功
    if monitor:
        monitor.record_bars_stored(symbol, trade_date, len(records))
    
    # 计算指标，使用监控上下文
    if monitor:
        with monitor.track_indicator_computation(symbol, trade_date):
            _recompute_indicators(session_factory, symbol, trade_date)
    else:
        _recompute_indicators(session_factory, symbol, trade_date)
    
    # 记录指标计算完成
    if monitor:
        monitor.record_indicators_computed(symbol, trade_date, len(records))

    start_et = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
        hour=8, minute=0
    )
    end_et = datetime.combine(trade_date, datetime.min.time(), tzinfo=EASTERN).replace(
        hour=16, minute=0
    )
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
    expected = max(480, _expected_rth_minutes(trade_date))
    _validate_rth_count(session_factory, symbol, start_utc, end_utc, expected)
    
    # 记录完成
    if monitor:
        monitor.record_symbol_date_completed(symbol, trade_date)
    
    LOGGER.info("Ingest completed symbol=%s records=%s", symbol, len(records))


def main() -> int:
    args = _parse_args()

    settings = get_settings()
    configure_logging(settings)
    
    # 启动 Prometheus metrics server
    metrics_port = int(os.getenv("DATA_INGEST_METRICS_PORT", "9091"))
    metrics_enabled = os.getenv("DATA_INGEST_METRICS_ENABLED", "true").lower() == "true"
    
    if metrics_enabled:
        start_metrics_server(metrics_port)
        LOGGER.info("Prometheus metrics server started on port %d", metrics_port)
    
    session_factory = get_session_factory(settings)
    with session_factory() as session:
        dao = BacktestDAO(
            session,
            option_bar_table=settings.option_bar_table,
            option_chain_table=settings.option_chain_table,
        )
        if args.date:
            trade_dates = [_to_trade_date(args.date)]
        else:
            start_date = _to_trade_date(args.start)
            end_date = _to_trade_date(args.end)
            if end_date < start_date:
                raise SystemExit("--end must not be earlier than --start")
            trade_dates = dao.fetch_trade_dates_between(start_date=start_date, end_date=end_date)
        if not trade_dates:
            raise SystemExit("No trading dates available in the requested window")

        if args.symbols:
            symbols = [symbol.upper() for symbol in args.symbols]
        else:
            resolver = UniverseResolver(session)
            universe = resolver.resolve(args.universe)
            symbols = universe.symbols
        if not symbols:
            raise SystemExit("Symbol universe resolved to zero entries")
        offset = max(0, int(args.symbols_offset))
        if offset:
            symbols = symbols[offset:]
        if args.symbols_limit is not None:
            limit = max(0, int(args.symbols_limit))
            symbols = symbols[:limit]
        if not symbols:
            raise SystemExit("No symbols remain after applying offset/limit")

    LOGGER.info(
        "ingest_equity.schedule trade_dates=%s symbols=%d offset=%d limit=%s batch_size=%s",
        [d.isoformat() for d in trade_dates],
        len(symbols),
        args.symbols_offset,
        args.symbols_limit,
        args.batch_size,
    )
    
    # 创建监控器
    monitor: Optional[DataIngestionMonitor] = None
    if metrics_enabled:
        monitor = DataIngestionMonitor(
            symbols=symbols,
            trade_dates=trade_dates,
            batch_size=int(args.batch_size) if args.batch_size else len(symbols),
            start_date_str=trade_dates[0].isoformat() if trade_dates else "",
            end_date_str=trade_dates[-1].isoformat() if trade_dates else "",
            universe=args.universe or "explicit_symbols",
        )
    
    client = build_ibkr_client(settings)
    try:
        # 记录连接成功
        if monitor:
            monitor.record_ibkr_connected()
        
        batches: list[list[str]]
        if args.batch_size and int(args.batch_size) > 0:
            size = max(1, int(args.batch_size))
            batches = [
                symbols[index : index + size] for index in range(0, len(symbols), size)
            ]
        else:
            batches = [symbols]

        for batch_index, batch_symbols in enumerate(batches, start=1):
            # 记录批次开始
            if monitor:
                monitor.record_batch_start(batch_index)
                monitor.record_symbol_in_progress(len(batch_symbols))
            LOGGER.info(
                "ingest_equity.batch_start index=%d/%d size=%d symbols=%s",
                batch_index,
                len(batches),
                len(batch_symbols),
                batch_symbols,
            )
            for trade_date in trade_dates:
                # 首先拉取VIX数据（每个交易日一次）
                try:
                    ingest_vix(
                        client=client,
                        session_factory=session_factory,
                        trade_date=trade_date,
                    )
                except Exception as exc:
                    LOGGER.error(
                        "ingest_vix.failed trade_date=%s error=%s",
                        trade_date.isoformat(),
                        str(exc),
                    )
                    # VIX拉取失败不影响equity数据拉取，继续执行
                
                # 拉取股票数据
                for symbol in batch_symbols:
                    try:
                        ingest_equity(
                            client=client,
                            session_factory=session_factory,
                            trade_date=trade_date,
                            symbol=symbol,
                            rth_only=bool(args.rth_only),
                            monitor=monitor,
                        )
                    except Exception as exc:  # pragma: no cover - network failure path
                        # 记录失败
                        if monitor:
                            monitor.record_symbol_failed(symbol, type(exc).__name__)
                        
                        LOGGER.error(
                            "ingest_equity.symbol_failed symbol=%s trade_date=%s error=%s",
                            symbol,
                            trade_date.isoformat(),
                            str(exc),
                        )
                        continue
            
            # 批次完成后输出统计
            LOGGER.info(
                "ingest_equity.batch_complete index=%d/%d processed=%d",
                batch_index,
                len(batches),
                len(batch_symbols),
            )
            
            if monitor:
                stats = monitor.get_stats()
                LOGGER.info(
                    "📊 Progress: %d/%d symbols (%.1f%%), ETA: %.0fs (%.1fmin), Bars: %d, Rate: %.1f bars/s",
                    stats["symbols_completed"],
                    stats["symbols_total"],
                    stats["completion_pct"],
                    stats["eta_seconds"],
                    stats["eta_minutes"],
                    stats["total_bars"],
                    stats["rate_bars_per_sec"],
                )
        
        _refresh_materialized_views(session_factory)
        
        # 标记完成
        if monitor:
            monitor.mark_completed()
            final_stats = monitor.get_stats()
            LOGGER.info(
                "✅ All ingestion completed! Total bars: %d, Elapsed: %.1fs, Avg rate: %.1f bars/s",
                final_stats["total_bars"],
                final_stats["elapsed_seconds"],
                final_stats["rate_bars_per_sec"],
            )
        
    except Exception as exc:
        # 标记失败
        if monitor:
            monitor.mark_failed()
            monitor.record_ibkr_disconnected()
        raise
    finally:
        client.disconnect_and_stop()
    
    LOGGER.info("All symbols ingested successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
