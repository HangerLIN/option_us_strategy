from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Tuple

import pytz
import structlog
from ibapi.scanner import ScannerSubscription
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from libs.core import EASTERN, current_trace_context, utc_now
from libs.core.config import Settings
from libs.db import (
    PremarketTop5DAO,
    RiskEventDAO,
)
from libs.infra import IBClient
from apps.rt_engine.pg_locks import top5_day_lock

LOGGER = structlog.get_logger(__name__)


@dataclass
class Top5Entry:
    symbol: str
    ret_0925_0930: Decimal


class Top5Service:
    SCANNER_MAX = 50
    # 移除市值相关常量，改用M60均线过滤（如果可用）

    def __init__(self, ib_client: IBClient, session_factory: sessionmaker, settings: Settings) -> None:
        self._ib_client = ib_client
        self._session_factory = session_factory
        self._settings = settings
        self._logger = structlog.get_logger(__name__)

    def run_for_today(self, et_date: date | None = None) -> None:
        et_date = et_date or datetime.now(EASTERN).date()
        session: Session = self._session_factory()
        try:
            success = self._run(session, et_date)
            session.commit()
            if success:
                self._logger.info("top5.completed", trade_date=str(et_date))
        except Exception:
            session.rollback()
            self._logger.exception("top5.failed", trade_date=str(et_date))
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    def _run(self, session: Session, et_date: date) -> bool:
        top5_dao = PremarketTop5DAO(session)
        risk_dao = RiskEventDAO(session)
        
        # 同日互斥锁：防止并发/重试导致同一交易日多个任务运行
        try:
            with top5_day_lock(session, et_date.isoformat()):
                self._logger.info("top5.job_lock_acquired", trade_date=str(et_date))
                return self._run_with_lock(session, top5_dao, risk_dao, et_date)
        except RuntimeError as exc:
            if "job_locked" in str(exc):
                self._logger.warning("top5.job_locked", trade_date=str(et_date), reason=str(exc))
                return False
            raise
    
    def _run_with_lock(self, session: Session, top5_dao: PremarketTop5DAO, 
                       risk_dao: RiskEventDAO, et_date: date) -> bool:
        # 一次性检查v_daily_ma60视图是否存在
        m60_available = self._check_m60_view_exists(session)
        self._logger.info("top5.m60_check", available=m60_available, trade_date=str(et_date))
        
        # 根据配置选择股票池来源
        top5_mode = self._settings.top5_mode
        self._logger.info("top5.mode", mode=top5_mode, trade_date=str(et_date))
        
        if top5_mode == "scanner":
            # 使用Scanner扫描
            try:
                subscription = self._build_subscription()
                scanner_results = self._ib_client.scanner_premarket(subscription)
            except Exception as exc:
                self._handle_failure(
                    session,
                    top5_dao,
                    risk_dao,
                    et_date,
                    reason="scanner_failed",
                    detail=str(exc),
                )
                return False

            candidate_symbols = self._extract_symbols(scanner_results)
            if not candidate_symbols:
                self._handle_failure(
                    session,
                    top5_dao,
                    risk_dao,
                    et_date,
                    reason="no_scanner_candidates",
                    detail="scanner returned no eligible symbols",
                )
                return False
        else:
            # 使用固定股票池
            candidate_symbols = self._get_fixed_pool_symbols()
            self._logger.info(
                "top5.fixed_pool", 
                count=len(candidate_symbols),
                symbols=",".join(candidate_symbols[:10]) + "..." if len(candidate_symbols) > 10 else ",".join(candidate_symbols)
            )
            if not candidate_symbols:
                self._handle_failure(
                    session,
                    top5_dao,
                    risk_dao,
                    et_date,
                    reason="fixed_pool_empty",
                    detail="fixed pool has no symbols",
                )
                return False

        returns: Dict[str, Decimal] = {}
        for symbol in candidate_symbols:
            try:
                ret = self._compute_return(symbol, et_date, session)
            except Exception as exc:
                self._logger.warning(
                    "top5.symbol_historical_fail",
                    symbol=symbol,
                    trade_date=str(et_date),
                    error=str(exc),
                )
                continue
            if ret is not None:
                returns[symbol] = ret

        if not returns:
            self._handle_failure(
                session,
                top5_dao,
                risk_dao,
                et_date,
                reason="no_returns",
                detail="historical data empty",
            )
            return False

        # 移除市值过滤，只使用M60均线过滤（如果视图存在）
        filtered = []
        for symbol, ret in returns.items():
            # 如果M60视图可用，则进行过滤
            if m60_available:
                if not self._passes_m60(session, symbol, et_date):
                    self._logger.info("top5.m60_filtered", symbol=symbol)
                    continue
            
            filtered.append(Top5Entry(symbol=symbol, ret_0925_0930=ret))

        if not filtered:
            self._handle_failure(
                session,
                top5_dao,
                risk_dao,
                et_date,
                reason="filters_removed_all",
                detail="no symbol passed market cap & M60 filters",
            )
            return False

        filtered.sort(key=lambda entry: entry.ret_0925_0930, reverse=True)
        top5 = filtered[:5]

        rows = [
            {
                "rank": idx,
                "symbol": entry.symbol,
                "ret_0925_0930": entry.ret_0925_0930,
            }
            for idx, entry in enumerate(top5, start=1)
        ]
        top5_dao.replace(et_date, rows)
        self._logger.info(
            "top5.persisted",
            trade_date=str(et_date),
            entries=[
                {
                    "rank": row["rank"],
                    "symbol": row["symbol"],
                    "ret": str(row["ret_0925_0930"]),
                }
                for row in rows
            ],
        )
        return True

    # ------------------------------------------------------------------
    def _get_fixed_pool_symbols(self) -> List[str]:
        """从配置获取固定股票池"""
        pool_str = self._settings.top5_fixed_pool
        symbols = [s.strip().upper() for s in pool_str.split(',') if s.strip()]
        return symbols
    
    def _build_subscription(self) -> ScannerSubscription:
        subscription = ScannerSubscription()
        subscription.instrument = "STK"
        subscription.scanCode = "TOP_PERC_GAIN"
        subscription.locationCode = "STK.US.MAJOR"
        subscription.abovePrice = 5.0
        subscription.aboveVolume = 100000
        subscription.stockTypeFilter = "STOCK"
        subscription.numberOfRows = self.SCANNER_MAX
        return subscription

    def _extract_symbols(self, scanner_results: List[Mapping[str, Any]]) -> List[str]:
        symbols: List[str] = []
        seen = set()
        for entry in scanner_results:
            if not self._is_equity_candidate(entry):
                continue
            symbol = str(entry.get("symbol", "")).upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            last_price = entry.get("lastPrice") or entry.get("price")
            if last_price is not None:
                try:
                    if float(last_price) < 5:
                        continue
                except (TypeError, ValueError):
                    pass
            symbols.append(symbol)
            if len(symbols) >= self.SCANNER_MAX:
                break
        return symbols

    def _is_equity_candidate(self, entry: Mapping[str, Any]) -> bool:
        stock_type = str(entry.get("stockType") or entry.get("secType") or "").upper()
        if stock_type in {"ETF", "ETN"}:
            return False
        instrument = str(entry.get("instrument") or entry.get("assetType") or "").upper()
        if "ETF" in instrument or "ETN" in instrument:
            return False
        description = str(entry.get("description") or "").upper()
        if "ETF" in description or "ETN" in description:
            return False
        return True

    def _compute_return(self, symbol: str, et_date: date, session: Session) -> Decimal | None:
        """计算盘前涨幅：09:25的价格相比昨日收盘价的涨幅
        
        公式：(price_0925 - prev_close) / prev_close
        """
        # 1. 获取昨日收盘价（从v_daily_ohlcv）
        prev_close = self._get_prev_close(symbol, et_date, session)
        if prev_close is None or prev_close <= 0:
            LOGGER.warning(
                "top5.prev_close_unavailable",
                symbol=symbol,
                trade_date=str(et_date),
            )
            return None
        
        # 2. 获取09:25的价格
        request_start = datetime.combine(et_date, time(9, 10), EASTERN)
        request_end = datetime.combine(et_date, time(9, 35), EASTERN)
        start_et = datetime.combine(et_date, time(9, 25), EASTERN)
        end_et = datetime.combine(et_date, time(9, 30), EASTERN)
        
        # 三层兜底策略：TRADES → MIDPOINT → BID_ASK
        for what_to_show in ["TRADES", "MIDPOINT", "BID_ASK"]:
            bars = self._request_with_retry(
                symbol, request_start, request_end, what_to_show=what_to_show
            )
            window = self._filter_bars(bars, start_et, end_et)
            
            # 寻找09:25的bar
            bar_0925 = next((b for b in window if b["time"].hour == 9 and b["time"].minute == 25), None)
            
            if bar_0925:
                price_0925 = bar_0925["close"]  # 09:25的收盘价
                if price_0925 > 0:
                    premarket_return = (price_0925 - prev_close) / prev_close
                    LOGGER.info(
                        "top5.data_source",
                        symbol=symbol,
                        what_to_show=what_to_show,
                        bars_count=len(window),
                        prev_close=float(prev_close),
                        price_0925=float(price_0925),
                        return_pct=float(premarket_return * 100),
                    )
                    return premarket_return
                elif bar_0925["open"] > 0:
                    # 如果close为0，fallback到open
                    price_0925_fallback = bar_0925["open"]
                    premarket_return = (price_0925_fallback - prev_close) / prev_close
                    LOGGER.warning(
                        "top5.close_fallback_to_open",
                        symbol=symbol,
                        what_to_show=what_to_show,
                        prev_close=float(prev_close),
                        price_0925_open=float(price_0925_fallback),
                        return_pct=float(premarket_return * 100),
                    )
                    return premarket_return
            else:
                # 记录09:25 bar缺失
                LOGGER.warning(
                    "top5.bar_0925_missing",
                    symbol=symbol,
                    what_to_show=what_to_show,
                    bars_count=len(window),
                )
        
        return None
    
    def _request_with_retry(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        what_to_show: str = "TRADES",
        max_retries: int = 3,
    ) -> list:
        """历史数据请求，带指数退避重试（应对HMDS维护窗口）"""
        import time
        
        for attempt in range(max_retries):
            try:
                # 构造带primaryExchange的合约以提高精度（特别是ETH）
                contract = self._ib_client.stock_contract(symbol, "SMART", "USD")
                # 通过contract details获取primaryExchange（60秒超时以应对农场慢响应）
                try:
                    details = self._ib_client.req_contract_details(contract, timeout=60.0)
                    if details and details.contract.primaryExchange:
                        contract.primaryExchange = details.contract.primaryExchange
                except Exception as e:
                    LOGGER.debug("top5.contract_details_failed", symbol=symbol, error=str(e))
                    pass  # 如果获取失败，继续使用SMART
                
                if what_to_show == "TRADES":
                    return self._ib_client.req_historical_1m_contract(
                        contract, start, end, use_rth=False, what_to_show="TRADES"
                    )
                else:
                    return self._ib_client.req_historical_1m_contract(
                        contract,
                        start,
                        end,
                        use_rth=False,
                        what_to_show=what_to_show,
                    )
            except (TimeoutError, RuntimeError) as exc:
                error_str = str(exc)
                # 检测HMDS维护窗口或超时
                is_maintenance = any(
                    code in error_str for code in ["2105", "2106", "2107", "162"]
                )
                is_timeout = "Timed out" in error_str or isinstance(exc, TimeoutError)
                
                if (is_maintenance or is_timeout) and attempt < max_retries - 1:
                    delay = 2 ** attempt  # 指数退避：1s, 2s, 4s
                    LOGGER.warning(
                        "top5.retry_historical",
                        symbol=symbol,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        delay=delay,
                        reason="maintenance" if is_maintenance else "timeout",
                    )
                    time.sleep(delay)
                    continue
                else:
                    # 最后一次重试失败或其他错误，直接抛出
                    raise
        
        return []

    def _filter_bars(
        self,
        bars: List[Mapping[str, Any]],
        start_et: datetime,
        end_et: datetime,
    ) -> List[dict]:
        """过滤出指定时间窗口内的K线，并规范化到分钟"""
        def _normalize_minute(dt):
            """按分钟对齐，避免秒级偏移"""
            return dt.replace(second=0, microsecond=0)
        
        bucket: List[dict] = []
        for bar in bars:
            bar_time = bar.get("time")
            if not bar_time:
                continue
            try:
                # IBKR formatDate=2 返回Unix时间戳（UTC），需要转换为美东时间
                timestamp = int(bar_time)
                dt_utc = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
                dt_et = dt_utc.astimezone(EASTERN)
                dt_normalized = _normalize_minute(dt_et)
            except (ValueError, TypeError):
                continue
            if not (start_et <= dt_normalized <= end_et):
                continue
            try:
                bucket.append({
                    "time": dt_normalized,
                    "open": Decimal(str(bar.get("open", 0))),
                    "high": Decimal(str(bar.get("high", 0))),
                    "low": Decimal(str(bar.get("low", 0))),
                    "close": Decimal(str(bar.get("close", 0))),
                    "volume": int(bar.get("volume", 0)),
                })
            except (ValueError, TypeError):
                continue
        
        bucket.sort(key=lambda item: item["time"])
        
        # 打印ET窗口用于调试
        if bucket:
            min_time = min(b["time"] for b in bucket)
            max_time = max(b["time"] for b in bucket)
            LOGGER.info(
                "ET_window",
                min_time=min_time.strftime("%Y-%m-%d %H:%M"),
                max_time=max_time.strftime("%Y-%m-%d %H:%M"),
                count=len(bucket),
            )
        
        return bucket

    def _get_prev_close(self, symbol: str, et_date: date, session: Session) -> Decimal | None:
        """获取昨日收盘价（从IBKR API）
        
        使用reqHistoricalData获取昨日的1 day bar
        """
        try:
            # 请求昨日的日线数据
            # endDateTime设为当日凌晨，这样能获取到昨日的完整日线
            end_dt = datetime.combine(et_date, time(0, 0), EASTERN)
            
            # 请求2天的日线数据，确保能拿到昨日
            # 使用现有的req_historical_1m方法的模式，但改为日线
            from ibapi.contract import Contract
            
            contract = Contract()
            contract.symbol = symbol
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
            
            # 获取contract details
            detail = self._ib_client.req_contract_details(contract)
            resolved = detail.contract
            
            # 构建请求
            end_str = end_dt.strftime("%Y%m%d %H:%M:%S US/Eastern")
            req_id = next(self._ib_client._req_id_counter)
            
            from libs.infra.ibkr_client import HistoricalRequest
            future = HistoricalRequest()
            self._ib_client._historical_requests[req_id] = future
            
            LOGGER.debug(
                "top5.requesting_prev_close",
                symbol=symbol,
                trade_date=str(et_date),
                req_id=req_id,
            )
            
            # 调用reqHistoricalData获取日线
            self._ib_client.reqHistoricalData(
                req_id,
                resolved,
                end_str,
                "2 D",  # 2天的数据
                "1 day",  # 日线
                "TRADES",
                1,  # useRTH=True，只要RTH时段
                2,  # formatDate=2
                False,
                [],
            )
            
            # 等待结果
            if not future.done.wait(timeout=30.0):
                self._ib_client._historical_requests.pop(req_id, None)
                LOGGER.warning(
                    "top5.prev_close_timeout",
                    symbol=symbol,
                    trade_date=str(et_date),
                )
                return None
            
            if future.error:
                LOGGER.warning(
                    "top5.prev_close_error",
                    symbol=symbol,
                    trade_date=str(et_date),
                    error=str(future.error),
                )
                return None
            
            # 取最后一根bar的收盘价（即昨日收盘）
            if future.bars and len(future.bars) > 0:
                last_bar = future.bars[-1]
                prev_close = last_bar.get("close")
                if prev_close and prev_close > 0:
                    LOGGER.debug(
                        "top5.prev_close_success",
                        symbol=symbol,
                        prev_close=float(prev_close),
                    )
                    return Decimal(str(prev_close))
            
            LOGGER.warning(
                "top5.prev_close_no_data",
                symbol=symbol,
                trade_date=str(et_date),
                bars_count=len(future.bars) if future.bars else 0,
            )
            return None
            
        except Exception as exc:
            LOGGER.warning(
                "top5.prev_close_failed",
                symbol=symbol,
                trade_date=str(et_date),
                error=str(exc),
            )
            return None
    
    def _check_m60_view_exists(self, session: Session) -> bool:
        """检查v_daily_ma60视图是否存在"""
        try:
            stmt = text(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.views 
                    WHERE table_schema = 'public' 
                    AND table_name = 'v_daily_ma60'
                )
                """
            )
            result = session.execute(stmt).scalar()
            return bool(result)
        except Exception as exc:
            self._logger.warning("top5.m60_view_check_failed", error=str(exc))
            return False
    
    def _passes_m60(self, session: Session, symbol: str, et_date: date) -> bool:
        stmt = text(
            """
            SELECT close_rth, sma60, sma60_slope
            FROM v_daily_ma60
            WHERE symbol = :symbol AND trade_date_et::date = :trade_date
            """
        )
        row = session.execute(stmt, {"symbol": symbol, "trade_date": et_date}).fetchone()
        if not row:
            return False
        close_rth, sma60, sma60_slope = row
        if close_rth is None or sma60 is None or sma60_slope is None:
            return False
        return close_rth > sma60 and sma60_slope > 0

    def _handle_failure(
        self,
        session: Session,
        top5_dao: PremarketTop5DAO,
        risk_dao: RiskEventDAO,
        et_date: date,
        *,
        reason: str,
        detail: str,
    ) -> None:
        self._logger.warning("top5.aborted", trade_date=str(et_date), reason=reason, detail=detail)
        top5_dao.clear(et_date)
        event_payload = {"detail": detail, "trade_date": str(et_date)}
        trace_ctx = current_trace_context()
        trace_id = trace_ctx.get("trace_id")
        if trace_id:
            event_payload["trace_id"] = trace_id
        risk_dao.create_event(
            event_ts=utc_now(),
            event_code="TOP5_EMPTY",
            severity="WARN",
            message=f"Premarket Top5 aborted: {reason}",
            payload=event_payload,
        )
