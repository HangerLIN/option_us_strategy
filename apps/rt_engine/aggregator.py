from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from queue import Empty, Queue
from typing import Any, Awaitable, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Literal, cast
from uuid import uuid4
import json

import structlog
import pandas as pd
import redis
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from libs.core import EASTERN, attach_trace_metadata, configure_logging, get_settings, utc_now
from libs.core.config import Settings
from libs.db import Base, PremarketTop5, StrategyPosition
from libs.db.dao import RiskStateDAO
from libs.infra import IBClient, RedisBus, build_ibkr_client
from libs.schemas.events import BarsClosed
from .top5_service import Top5Service
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from .indicators import compute_indicator_row


@dataclass
class AggregatedBar:
    """
    聚合后的1分钟K线数据结构
    
    Attributes:
        symbol: 股票代码（大写）
        open: 开盘价
        high: 最高价
        low: 最低价
        close: 收盘价
        volume: 成交量
        ts_end: K线结束时间（UTC，精确到分钟）
    """
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    ts_end: datetime


@dataclass
class SymbolMeta:
    """
    订阅标的的元数据
    
    用于管理股票和期权的订阅信息，每个订阅的symbol都有对应的Meta
    
    Attributes:
        alias: 内部别名（股票=symbol，期权=OPT:AAPL:CALL:20251025:175.0）
        kind: 类型（EQUITY=股票，OPTION=期权）
        underlying: 标的股票代码
        conid: IBKR合约ID（期权专用）
        expiry: 到期日（期权专用）
        right: CALL/PUT（期权专用）
        strike: 行权价（期权专用）
        min_tick: 最小价格变动单位（期权专用）
        contract: IBKR合约对象（期权专用，用于订阅）
    """
    alias: str
    kind: Literal["EQUITY", "OPTION"]
    underlying: str
    conid: int | None = None
    expiry: datetime | None = None
    right: str | None = None
    strike: Decimal | None = None
    min_tick: Decimal | None = None
    contract: Contract | None = None


class MinuteAggregator:
    """
    1分钟K线聚合器 - 实盘交易系统的核心数据引擎
    
    核心职责：
    1. 订阅IBKR实时tick数据（股票+期权+VIX）
    2. 将tick流聚合成1分钟OHLCV K线
    3. 计算13个技术指标（RSI/BOLL/ATR/AO/CCI/OBV/MFI/RVOL等）
    4. 检测并补充短期数据缺口（≤15分钟）
    5. 发布bars_closed事件到Redis（触发信号生成）
    6. 每日刷新RVOL基线（用于成交量异常检测）
    7. 动态管理watchlist（盘前Top5 + 持仓股票）
    
    设计特点：
    - NBBO回退：无tick时使用bid/ask中间价
    - 短期补数：自动从IBKR补充≤15分钟的缺口
    - 多线程消费：每个symbol独立线程处理tick队列
    - 异步发布：Redis发布失败不阻塞主循环
    - 缓存优化：RVOL基线、VIX快照缓存
    
    数据流：
    IBKR tick → Queue → _consume_ticks() → _buffers[symbol] →
    每分钟触发 → _close_minute() → 聚合OHLCV → 写入DB →
    计算指标 → 发布bars_closed事件 → signal_svc消费
    """

    def __init__(
        self,
        ib_client: IBClient,
        session_factory: sessionmaker,
        settings: Settings,
        *,
        watchlist_refresh_seconds: int = 30,
        static_symbols: Iterable[str] | None = None,
        vix_required: bool = True,
    ) -> None:
        """
        初始化MinuteAggregator
        
        Args:
            ib_client: IBKR客户端实例（用于订阅数据、查询历史）
            session_factory: SQLAlchemy session工厂（用于数据库操作）
            settings: 全局配置对象
            watchlist_refresh_seconds: watchlist刷新间隔（秒，最小5秒）
            static_symbols: 固定订阅的股票列表（不受watchlist影响）
            vix_required: VIX订阅是否必需（True=启动失败抛异常）
        """
        # ========== 核心依赖 ==========
        self._ib_client = ib_client
        self._session_factory = session_factory
        self._settings = settings
        
        # ========== Tick数据缓冲区 ==========
        # _buffers: {symbol: [(tick_time, {"price": 100.5, "size": 100}), ...]}
        # 存储未聚合的tick，每分钟提取并清空
        self._buffers: Dict[str, List[Tuple[datetime, Mapping[str, Decimal]]]] = defaultdict(list)
        
        # _quotes: {symbol: {"bid": 100.0, "ask": 100.2}}
        # 最新的NBBO报价，用于无tick时的NBBO回退
        self._quotes: Dict[str, Dict[str, Optional[Decimal]]] = defaultdict(
            lambda: {"bid": None, "ask": None}
        )
        
        # _last_trade: {symbol: 100.1}
        # 最新成交价，NBBO回退的第二优先级
        self._last_trade: Dict[str, Optional[Decimal]] = {}
        
        # ========== 持久化追踪 ==========
        # _last_persisted: {symbol: datetime}
        # 记录每个symbol最后写入数据库的K线时间，用于缺口检测
        self._last_persisted: Dict[str, Optional[datetime]] = {}
        
        # ========== RVOL基线缓存 ==========
        # _baseline_cache: {symbol: {minute_index: mean_vol}}
        # 缓存rvol_baseline_eq表数据，避免重复查询
        # 示例：{"AAPL": {570: 1000000, 571: 950000, ...}}  # 570=09:30
        self._baseline_cache: Dict[str, Dict[int, Decimal]] = {}
        self._last_rvol_refresh: date | None = None  # 最后刷新RVOL基线的日期
        
        # ========== 订阅管理 ==========
        # _symbol_tasks: {alias: asyncio.Future}
        # 每个symbol的消费线程任务
        self._symbol_tasks: Dict[str, asyncio.Future] = {}
        
        # _symbol_queues: {alias: Queue}
        # 每个symbol的tick数据队列（IBKR写入，_consume_ticks读取）
        self._symbol_queues: Dict[str, Queue] = {}
        
        # _active_symbols: {"AAPL", "OPT:AAPL:CALL:20251025:175.0", ...}
        # 当前活跃订阅的所有alias集合
        self._active_symbols: Set[str] = set()
        
        # _static_symbols: {"SPY", "QQQ"}
        # 固定订阅的股票（命令行指定），不受watchlist动态变化影响
        self._static_symbols: Set[str] = {symbol.upper() for symbol in (static_symbols or [])}
        
        # ========== VIX订阅 ==========
        self._vix_symbol = "VIX"
        self._vix_queue: Optional[Queue] = None
        self._vix_task: Optional[asyncio.Future] = None
        self._vix_last_price: Optional[Decimal] = None  # 最新VIX值
        self._vix_required = vix_required  # VIX是否必需（风控依赖）
        
        # ========== Watchlist刷新 ==========
        refresh = max(watchlist_refresh_seconds, 5)  # 最小5秒
        self._watchlist_refresh = timedelta(seconds=refresh)
        self._next_watchlist_refresh: Optional[datetime] = None
        
        # ========== 运行状态 ==========
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = True
        self._logger = structlog.get_logger(__name__)
        
        # ========== 订阅元数据 ==========
        # _symbol_meta: {alias: SymbolMeta}
        # 所有订阅symbol的元数据（股票+期权）
        self._symbol_meta: Dict[str, SymbolMeta] = {}
        
        # _underlying_options: {underlying: [option_alias, ...]}
        # 记录每个股票对应的期权alias列表
        # 示例：{"AAPL": ["OPT:AAPL:CALL:...", "OPT:AAPL:PUT:..."]}
        self._underlying_options: Dict[str, List[str]] = defaultdict(list)
        
        # ========== 期权相关缓存 ==========
        # _last_equity_close: {symbol: close_price}
        # 最新股票收盘价，用于期权Greeks的underlying_price回填
        self._last_equity_close: Dict[str, Decimal] = {}
        
        # _option_metrics: {option_alias: {"implied_vol": 0.25, "delta": 0.45, ...}}
        # 期权Greeks和其他指标
        self._option_metrics: Dict[str, Dict[str, Decimal]] = {}
        
        # _volume_snapshots: {option_alias: cumulative_volume}
        # 期权累计成交量快照，用于计算增量
        self._volume_snapshots: Dict[str, Decimal] = {}
        
        # 期权订阅的genericTicks参数
        # 100=Option Volume, 101=Option Open Interest, 104=Historical Volatility
        # 106=Option Implied Volatility, 233=RT Volume
        self._option_generic_ticks = "100,101,104,106,233"
        
        # ========== Redis连接 ==========
        try:
            # 普通Redis连接（用于VIX快照缓存）
            self._redis: redis.Redis | None = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        except Exception:  # pragma: no cover - external dependency
            self._redis = None
            self._logger.warning("aggregator.redis_init_failed")
        
        try:
            # Redis Stream连接（用于发布bars_closed事件）
            self._redis_bus: RedisBus | None = RedisBus(settings.redis_url)
        except Exception:  # pragma: no cover - external dependency
            self._redis_bus = None
            self._logger.warning("aggregator.redis_bus_init_failed")

        # ========== 初始化固定订阅股票的元数据 ==========
        for symbol in self._static_symbols:
            self._ensure_equity_meta(symbol)

    async def run(self) -> None:
        """
        主运行循环 - 分钟级精准时钟驱动
        
        执行流程：
        1. 启动VIX订阅（风控必需）
        2. 进入无限循环：
           a. 定期刷新watchlist（默认30秒）
           b. 精准等待到下一分钟整点（如10:31:00）
           c. 关闭上一分钟，聚合K线，计算指标，发布事件
           d. 检查是否需要刷新RVOL基线（16:10后）
        3. 退出时清理资源
        
        时间精度：
        - 使用UTC时间，避免夏令时问题
        - 每分钟触发误差<1秒
        - 通过动态sleep计算保证精准触发
        """
        self._loop = asyncio.get_running_loop()
        vix_started = False
        
        # ========== 1. 启动VIX订阅 ==========
        try:
            self._start_vix_subscription()
            vix_started = True
        except Exception as exc:  # pragma: no cover - external dependency
            if self._vix_required:
                # VIX是必需的（风控VIX Gate依赖），启动失败抛异常
                self._logger.exception("aggregator.vix_subscription_failed", error=str(exc))
                raise
            # VIX是可选的，记录警告继续运行
            self._logger.warning("aggregator.vix_subscription_optional_failed", error=str(exc))
        
        # ========== 2. 主循环 ==========
        try:
            while True:
                now = utc_now()
                
                # 2.1 检查是否需要刷新watchlist
                if self._next_watchlist_refresh is None or now >= self._next_watchlist_refresh:
                    self._refresh_watchlist()  # 从premarket_top5 + positions读取，订阅股票+期权
                    self._next_watchlist_refresh = now + self._watchlist_refresh
                
                # 2.2 计算下一分钟整点时间
                # 示例：now=10:30:45.123 → next_close=10:31:00.000
                next_close = (now.replace(second=0, microsecond=0) + timedelta(minutes=1)).replace(
                    tzinfo=timezone.utc
                )
                
                # 2.3 精准等待到下一分钟整点
                # 示例：10:30:45.123 → 等待14.877秒 → 10:31:00触发
                await asyncio.sleep(max(0.0, (next_close - now).total_seconds()))
                
                # 2.4 关闭上一分钟（聚合[10:30:00, 10:31:00]的tick）
                self._close_minute(next_close)
                
                # 2.5 检查是否需要刷新RVOL基线（每日16:10后执行一次）
                self._maybe_refresh_rvol_baseline(next_close)
        
        # ========== 3. 清理资源 ==========
        finally:
            self._running = False
            
            # 停止VIX订阅
            if vix_started:
                self._stop_vix_subscription()
            
            # 停止所有symbol订阅
            self._teardown_subscriptions()
            
            # 关闭Redis连接
            if self._redis_bus is not None:
                try:
                    await self._redis_bus.close()
                except Exception:  # pragma: no cover - best-effort close
                    self._logger.warning("aggregator.redis_bus_close_failed")

    # ------------------------------------------------------------------
    # Tick数据消费与处理
    # ------------------------------------------------------------------
    
    def _consume_ticks(self, symbol: str, queue: Queue) -> None:
        """
        Tick数据消费线程 - 每个symbol一个独立线程
        
        运行在executor线程池中（非asyncio），阻塞等待queue数据
        
        处理的tick类型：
        1. price: bid/ask/last价格tick（field=1/2/4）
        2. rt_volume: RTVolume tick（包含price+size）
        3. greeks: 期权Greeks数据（IV/Delta/Gamma/Theta/Vega）
        4. size: 成交量/持仓量tick
        5. generic: 通用tick（如OI）
        
        数据流：
        IBKR回调 → queue.put() → 本方法queue.get() → 
        解析tick → 更新quotes/last_trade/option_metrics →
        存入_buffers待聚合
        
        Args:
            symbol: 订阅的symbol alias（如"AAPL"或"OPT:AAPL:CALL:..."）
            queue: IBKR订阅的tick数据队列
        """
        while True:
            # 检查运行状态（优雅退出）
            if not self._running:
                break
            
            try:
                # 阻塞等待tick数据，超时1秒检查运行状态
                payload = queue.get(timeout=1)
            except Empty:
                continue
            
            # None是停止信号（_deactivate_symbol时发送）
            if payload is None:
                break
            
            tick_time = utc_now()
            record: Dict[str, Decimal] = {}
            event_type = payload.get("type")
            
            # ========== 处理price tick ==========
            if event_type == "price":
                # field=1: bid, 2: ask, 4: last
                # 来自IBKR tickPrice回调
                price = Decimal(str(payload.get("price")))
                field = payload.get("field")
                
                if field == 1:
                    # Bid价格更新
                    self._quotes[symbol]["bid"] = price
                elif field == 2:
                    # Ask价格更新
                    self._quotes[symbol]["ask"] = price
                elif field == 4:
                    # Last成交价更新
                    self._last_trade[symbol] = price
                
                # 记录price tick待聚合
                record["price"] = price
            
            # ========== 处理RTVolume tick ==========
            elif event_type == "rt_volume":
                # RTVolume包含：price, size, time, total_volume, vwap, single_market_maker
                # 这是实时成交数据，包含price和size
                price = Decimal(str(payload.get("price", 0)))
                size = Decimal(str(payload.get("single_trade_volume", 0)))
                
                # 更新最新成交价
                self._last_trade[symbol] = price
                
                # 记录price+size待聚合
                record["price"] = price
                record["size"] = size
            
            # ========== 处理期权Greeks tick ==========
            elif event_type == "greeks":
                # 期权Greeks数据：IV, Delta, Gamma, Theta, Vega, Underlying Price
                # 仅期权symbol会收到此类tick
                meta = self._symbol_meta.get(symbol)
                if meta and meta.kind == "OPTION":
                    metrics = self._option_metrics.setdefault(symbol, {})
                    
                    # Implied Volatility（隐含波动率）
                    iv_val = payload.get("implied_vol")
                    if iv_val is not None:
                        metrics["implied_vol"] = Decimal(str(iv_val))
                    
                    # Delta（价格敏感度，0-1之间）
                    delta_val = payload.get("delta")
                    if delta_val is not None:
                        metrics["delta"] = Decimal(str(delta_val))
                    
                    # Gamma（Delta变化率）
                    gamma_val = payload.get("gamma")
                    if gamma_val is not None:
                        metrics["gamma"] = Decimal(str(gamma_val))
                    
                    # Vega（波动率敏感度）
                    vega_val = payload.get("vega")
                    if vega_val is not None:
                        metrics["vega"] = Decimal(str(vega_val))
                    
                    # Theta（时间衰减，每日损失）
                    theta_val = payload.get("theta")
                    if theta_val is not None:
                        metrics["theta"] = Decimal(str(theta_val))
                    
                    # Underlying Price（标的股票价格）
                    # 用于计算期权内在价值，同时更新股票收盘价缓存
                    und_val = payload.get("underlying_price")
                    if und_val is not None:
                        und_price = Decimal(str(und_val))
                        metrics["underlying_price"] = und_price
                        # 更新股票收盘价缓存（用于无stock tick时的回填）
                        self._last_equity_close[meta.underlying] = und_price
            elif event_type == "size":
                meta = self._symbol_meta.get(symbol)
                if meta and meta.kind == "OPTION":
                    field = payload.get("field")
                    size_value = Decimal(str(payload.get("size", "0")))
                    if field == TickTypeEnum.LAST_SIZE:
                        record["size"] = size_value
                    elif field in {
                        TickTypeEnum.VOLUME,
                        TickTypeEnum.OPTION_CALL_VOLUME,
                        TickTypeEnum.OPTION_PUT_VOLUME,
                    }:
                        previous = self._volume_snapshots.get(symbol)
                        self._volume_snapshots[symbol] = size_value
                        if previous is not None:
                            delta = size_value - previous
                            if delta > 0:
                                record["size"] = delta
                    elif field in {
                        TickTypeEnum.OPTION_CALL_OPEN_INTEREST,
                        TickTypeEnum.OPTION_PUT_OPEN_INTEREST,
                    }:
                        metrics = self._option_metrics.setdefault(symbol, {})
                        metrics["open_interest"] = size_value
            elif event_type == "generic":
                meta = self._symbol_meta.get(symbol)
                if meta and meta.kind == "OPTION":
                    field = payload.get("field")
                    value = payload.get("value")
                    if field == TickTypeEnum.OPEN_INTEREST and value is not None:
                        metrics = self._option_metrics.setdefault(symbol, {})
                        metrics["open_interest"] = Decimal(str(value))

            if record:
                self._buffers[symbol].append((tick_time, record))

    def _ensure_equity_meta(self, symbol: str) -> SymbolMeta:
        """
        确保股票元数据存在（如不存在则创建）
        
        Args:
            symbol: 股票代码
        
        Returns:
            股票元数据
        """
        alias = symbol.upper()
        meta = self._symbol_meta.get(alias)
        if meta is None:
            meta = SymbolMeta(alias=alias, kind="EQUITY", underlying=alias)
            self._symbol_meta[alias] = meta
        return meta

    def _option_alias(self, underlying: str, candidate: Mapping[str, Any]) -> str:
        """
        生成期权的内部alias
        
        格式：
        - 优先使用conid：OPT:AAPL:12345678
        - 否则使用详细信息：OPT:AAPL:CALL:20251025:175.0
        
        Args:
            underlying: 标的股票代码
            candidate: 期权候选合约信息
        
        Returns:
            期权alias
        """
        conid = candidate.get("conid")
        if conid:
            return f"OPT:{underlying}:{int(conid)}"
        
        expiry = candidate["expiry"].strftime("%Y%m%d") if candidate.get("expiry") else ""
        right = str(candidate.get("option_right") or "").upper()
        strike = Decimal(str(candidate.get("strike"))).normalize()
        return f"OPT:{underlying}:{right}:{expiry}:{strike}"

    def _select_option_candidate(
        self, symbol: str, ts_end: datetime, option_right: str
    ) -> Optional[dict[str, Any]]:
        """
        为股票选择最佳期权合约
        
        选择标准（与实盘exec_svc一致）：
        - DTE: 2-7天
        - OTM: +2~+5档（远离当前价格，降低Gamma风险）
        - OI: ≥500（流动性）
        - Volume: ≥100（流动性）
        - 价差: ≤max($0.10, 5%*mid)（流动性）
        
        排序优先级：
        1. 价差最小
        2. OI最大
        3. 成交量最大
        
        Args:
            symbol: 股票代码
            ts_end: 参考时间
            option_right: CALL或PUT
        
        Returns:
            期权候选合约信息，如无符合条件的合约则返回None
        """
        try:
            quote = self._ib_client.best_option_contract(
                symbol,
                right=option_right,
                reference=ts_end,
                dte_range=(2, 7),
                otm_steps=(2, 5),
                min_open_interest=500,
                min_volume=100,
                max_spread_pct=Decimal("0.05"),
                max_abs_spread=Decimal("0.10"),
            )
        except Exception as exc:
            self._logger.warning(
                "aggregator.option_candidate_ibkr_failed",
                symbol=symbol,
                option_right=option_right,
                error=str(exc),
            )
            return None

        if quote is None:
            return None

        return {
            "conid": quote.conid,
            "underlying_symbol": symbol,
            "expiry": quote.expiry,
            "strike": quote.strike,
            "option_right": quote.right,
            "bid": quote.bid,
            "ask": quote.ask,
            "mid": quote.mid,
            "open_interest": quote.open_interest,
            "volume": quote.volume,
            "min_tick": quote.min_tick,
            "dte": quote.dte,
            "contract": quote.contract,
        }

    def _ensure_option_aliases(self, session: Session, symbol: str) -> List[str]:
        """
        为股票查询并缓存最佳期权合约（CALL + PUT）
        
        流程：
        1. 查询最佳CALL合约
        2. 查询最佳PUT合约
        3. 生成alias并创建/更新元数据
        4. 初始化option_metrics
        5. 清理旧的期权alias（过期或不再最佳）
        
        每次watchlist刷新都会重新查询，确保：
        - 期权合约临近到期时自动切换到新合约
        - 价格变化导致OTM档位变化时自动切换
        - 流动性变化时切换到更优合约
        
        Args:
            session: DB session
            symbol: 股票代码
        
        Returns:
            期权alias列表（如["OPT:AAPL:CALL:...", "OPT:AAPL:PUT:..."]）
        """
        aliases: List[str] = []
        previous_aliases = list(self._underlying_options.get(symbol, []))
        
        # ========== 1. 查询CALL和PUT合约 ==========
        for option_right in ("CALL", "PUT"):
            try:
                candidate = self._select_option_candidate(symbol, utc_now(), option_right)
            except Exception as exc:
                self._logger.exception(
                    "aggregator.option_candidate_failed",
                    symbol=symbol,
                    option_right=option_right,
                    error=str(exc),
                )
                continue
            
            if not candidate:
                continue
            
            # ========== 2. 生成alias ==========
            alias = self._option_alias(symbol, candidate)
            
            # ========== 3. 创建/更新元数据 ==========
            meta = self._symbol_meta.get(alias)
            if meta is None:
                # 新建元数据
                meta = SymbolMeta(
                    alias=alias,
                    kind="OPTION",
                    underlying=symbol,
                    conid=candidate.get("conid"),
                    expiry=candidate.get("expiry"),
                    right=option_right,
                    strike=candidate.get("strike"),
                    min_tick=candidate.get("min_tick"),
                    contract=candidate.get("contract"),
                )
                self._symbol_meta[alias] = meta
            else:
                # 更新已存在的元数据（可能合约参数变化）
                meta.conid = candidate.get("conid")
                meta.expiry = candidate.get("expiry")
                meta.right = option_right
                meta.strike = candidate.get("strike")
                meta.min_tick = candidate.get("min_tick")
                meta.contract = candidate.get("contract")
            
            # ========== 4. 初始化metrics ==========
            self._option_metrics.setdefault(alias, {})
            aliases.append(alias)
        
        # ========== 5. 更新underlying_options映射 ==========
        self._underlying_options[symbol] = aliases
        
        # ========== 6. 清理旧的期权alias ==========
        # 删除不再使用的期权元数据（过期或不再最佳）
        for old_alias in previous_aliases:
            if old_alias not in aliases:
                self._symbol_meta.pop(old_alias, None)
        
        return aliases

    def _close_minute(self, ts_end: datetime) -> None:
        """
        关闭分钟K线 - 核心聚合逻辑
        
        每分钟整点触发（如10:31:00），聚合[10:30:00, 10:31:00]的tick
        
        执行流程：
        1. 从_buffers提取本分钟的tick
        2. 补充短期缺口（≤15分钟）
        3. 聚合OHLCV
        4. 写入bars1m_equity/bars1m_option
        5. 计算技术指标 → indicators_eq_1m
        6. 发布bars_closed事件到Redis
        7. 写入VIX快照到risk_state
        
        Args:
            ts_end: K线结束时间（UTC，如2025-10-23 14:31:00+00:00）
        """
        session: Session = self._session_factory()
        try:
            risk_states: List[dict[str, Any]] = []
            equity_payloads: List[Tuple[str, SymbolMeta, List[Mapping[str, Decimal]]]] = []
            option_payloads: List[Tuple[str, SymbolMeta, List[Mapping[str, Decimal]]]] = []

            # ========== 1. 遍历所有活跃symbol，提取本分钟的tick ==========
            for alias, ticks in list(self._buffers.items()):
                if not ticks:
                    continue
                
                meta = self._symbol_meta.get(alias)
                if meta is None:
                    continue
                
                # ========== 2. 股票先补短期缺口 ==========
                # 检测上一根K线到target之间是否有缺口（≤15分钟）
                # 如有缺口，调用IBKR历史数据API补充
                if meta.kind == "EQUITY":
                    self._backfill_missing(session, alias, ts_end - timedelta(minutes=1))
                
                # ========== 3. 提取本分钟的tick ==========
                # 从ticks中筛选 (ts_end-1min, ts_end] 的tick
                # 示例：ts_end=10:31:00 → 提取 (10:30:00, 10:31:00] 的tick
                bucket = self._extract_bucket(ticks, ts_end)
                if not bucket:
                    continue
                
                # ========== 4. 按类型分类（股票/期权） ==========
                if meta.kind == "OPTION":
                    option_payloads.append((alias, meta, bucket))
                else:
                    equity_payloads.append((alias, meta, bucket))

            # ========== 5. 处理股票K线 ==========
            for alias, meta, bucket in equity_payloads:
                # 5.1 聚合OHLCV
                bar = self._aggregate(meta, bucket, ts_end)
                
                # 5.2 写入bars1m_equity表
                self._persist_equity_bar(session, bar)
                
                # 5.3 缓存最新收盘价（用于期权Greeks的underlying_price回填）
                self._last_equity_close[meta.underlying] = bar.close
                
                # 5.4 计算13个技术指标并写入indicators_eq_1m
                # 指标：RSI, BOLL, ATR, AO, Stoch, CCI, OBV, MFI, RVOL, Slopes
                self._update_indicators(session, meta.underlying, bar.ts_end)
                
                # 5.5 发布bars_closed事件到Redis Stream
                # 触发signal_svc消费并评估信号
                self._publish_bar(meta, bar)
                
                # 5.6 更新持久化时间（用于缺口检测）
                self._last_persisted[alias] = bar.ts_end

            # ========== 6. 处理期权K线 ==========
            for alias, meta, bucket in option_payloads:
                # 6.1 聚合OHLCV
                bar = self._aggregate(meta, bucket, ts_end)
                
                # 6.2 写入bars1m_option表（包含bid/ask/Greeks/OI等）
                self._persist_option_bar(session, meta, bar)
                
                # 6.3 更新持久化时间
                self._last_persisted[alias] = bar.ts_end
            
            # ========== 7. 写入VIX快照到risk_state ==========
            if self._vix_last_price is not None:
                risk_states.append(
                    {
                        "ts": ts_end,
                        "symbol": self._vix_symbol,
                        "metric_code": "VIX",
                        "metric_value": self._vix_last_price,
                        "detail": {"source": "ibkr"},
                    }
                )
                # 同时缓存到Redis（TTL=180秒，供risk_svc快速查询）
                self._cache_vix_snapshot(self._vix_last_price)
            
            if risk_states:
                RiskStateDAO(session).insert_states(risk_states)
            
            session.commit()
        
        except Exception:  # pragma: no cover - defensive
            session.rollback()
            self._logger.exception("aggregator.minute_close_failed", ts_end=str(ts_end))
        
        finally:
            session.close()

    def _maybe_refresh_rvol_baseline(self, ts_end: datetime) -> None:
        et = ts_end.astimezone(EASTERN)
        if et.hour < 16 or (et.hour == 16 and et.minute < 10):
            return
        trade_date = et.date()
        if self._last_rvol_refresh == trade_date:
            return
        try:
            self._refresh_rvol_baseline(trade_date)
        except Exception:  # pragma: no cover - defensive
            self._logger.exception("aggregator.rvol_refresh_failed", trade_date=str(trade_date))
        else:
            self._last_rvol_refresh = trade_date

    def _refresh_rvol_baseline(self, trade_date: date) -> None:
        symbols = sorted(
            {
                meta.underlying
                for meta in self._symbol_meta.values()
                if meta.kind == "EQUITY"
            }
        )
        if not symbols:
            self._logger.info(
                "aggregator.rvol_refresh_skipped",
                trade_date=str(trade_date),
                reason="no_equity_symbols",
            )
            return

        end_et = datetime.combine(trade_date, time(16, 0), EASTERN)
        lookback_days = max(self._settings.rvol_baseline_days * 2, 30)
        start_et = end_et - timedelta(days=lookback_days)

        session: Session = self._session_factory()
        try:
            for symbol in symbols:
                try:
                    bars = self._ib_client.req_historical_1m(
                        symbol, start_et, end_et, use_rth=True
                    )
                except Exception as exc:  # pragma: no cover - external call
                    self._logger.warning(
                        "aggregator.rvol_refresh_ib_error",
                        symbol=symbol,
                        trade_date=str(trade_date),
                        error=str(exc),
                    )
                    continue

                per_day: Dict[date, Dict[int, Decimal]] = {}
                for raw in bars:
                    dt_et = self._ib_bar_to_eastern(raw.get("time"))
                    if dt_et is None:
                        continue
                    if dt_et.date() > trade_date:
                        continue
                    if (
                        dt_et.time() < time(9, 30)
                        or dt_et.time() >= time(16, 0)
                    ):
                        continue
                    minute_index = dt_et.hour * 60 + dt_et.minute
                    try:
                        volume_val = Decimal(str(raw.get("volume", "0") or "0"))
                    except Exception:
                        volume_val = Decimal("0")
                    day_bucket = per_day.setdefault(dt_et.date(), {})
                    day_bucket[minute_index] = volume_val

                if not per_day:
                    self._logger.info(
                        "aggregator.rvol_refresh_no_data",
                        symbol=symbol,
                        trade_date=str(trade_date),
                    )
                    continue

                selected_days = sorted(per_day.keys())[-self._settings.rvol_baseline_days :]
                minute_samples: Dict[int, List[Decimal]] = defaultdict(list)
                for day in selected_days:
                    for minute_idx, volume_val in per_day[day].items():
                        minute_samples[minute_idx].append(volume_val)

                if not minute_samples:
                    self._logger.info(
                        "aggregator.rvol_refresh_no_samples",
                        symbol=symbol,
                        trade_date=str(trade_date),
                    )
                    continue

                entries: List[Dict[str, Any]] = []
                baseline_map: Dict[int, Decimal] = {}
                for minute_idx, samples in minute_samples.items():
                    if not samples:
                        continue
                    total = sum(samples, Decimal("0"))
                    mean_volume = total / Decimal(len(samples))
                    baseline_map[minute_idx] = mean_volume
                    entries.append(
                        {
                            "symbol": symbol,
                            "minute_index": minute_idx,
                            "mean_vol_20d": float(mean_volume),
                        }
                    )

                try:
                    session.execute(
                        text("DELETE FROM rvol_baseline_eq WHERE symbol = :symbol"),
                        {"symbol": symbol},
                    )
                    if entries:
                        session.execute(
                            text(
                                """
                                INSERT INTO rvol_baseline_eq (symbol, minute_index, mean_vol_20d)
                                VALUES (:symbol, :minute_index, :mean_vol_20d)
                                ON CONFLICT (symbol, minute_index) DO UPDATE
                                SET mean_vol_20d = EXCLUDED.mean_vol_20d,
                                    updated_at = now()
                                """
                            ),
                            entries,
                        )
                    session.commit()
                    self._baseline_cache[symbol] = baseline_map
                    self._logger.info(
                        "aggregator.rvol_refresh_symbol",
                        symbol=symbol,
                        minutes=len(entries),
                        trade_date=str(trade_date),
                        days=len(selected_days),
                    )
                except Exception:
                    session.rollback()
                    self._logger.exception(
                        "aggregator.rvol_refresh_persist_failed", symbol=symbol
                    )
        finally:
            session.close()

    def _extract_bucket(
        self,
        ticks: List[Tuple[datetime, Mapping[str, Decimal]]],
        ts_end: datetime,
    ) -> List[Mapping[str, Decimal]]:
        """
        从tick buffer提取本分钟的tick
        
        提取规则：
        - 提取 (ts_end-1min, ts_end] 的tick
        - 示例：ts_end=10:31:00 → 提取 (10:30:00, 10:31:00] 的tick
        - 剩余tick（ts_end之后）保留在buffer，等待下一分钟
        
        Args:
            ticks: tick buffer（原地修改）
            ts_end: K线结束时间
        
        Returns:
            本分钟的tick列表
        """
        cutoff = ts_end - timedelta(minutes=1)
        
        # 提取 cutoff < tick_ts <= ts_end 的tick
        bucket = [payload for tick_ts, payload in ticks if cutoff < tick_ts <= ts_end]
        
        # 保留 tick_ts > ts_end 的tick（原地修改ticks）
        ticks[:] = [(tick_ts, payload) for tick_ts, payload in ticks if tick_ts > ts_end]
        
        return bucket

    def _aggregate(
        self,
        meta: SymbolMeta,
        bucket: List[Mapping[str, Decimal]],
        ts_end: datetime,
    ) -> AggregatedBar:
        """
        聚合tick为OHLCV K线
        
        聚合策略：
        1. 优先级1：使用tick数据聚合
           - open = prices[0]（第一笔成交价）
           - high = max(prices)（最高成交价）
           - low = min(prices)（最低成交价）
           - close = prices[-1]（最后一笔成交价）
           - volume = sum(sizes)（成交量总和）
        
        2. 优先级2：NBBO回退（无tick时）
           - mid = (bid + ask) / 2
           - open = high = low = close = mid
           - volume = 0
        
        3. 优先级3：last_trade回退（无NBBO时）
           - open = high = low = close = last_trade
           - volume = 0
        
        Args:
            meta: symbol元数据
            bucket: 本分钟的tick列表
            ts_end: K线结束时间
        
        Returns:
            聚合后的K线
        """
        # ========== 策略1：使用tick数据聚合 ==========
        prices = [record["price"] for record in bucket if "price" in record]
        if prices:
            open_price = prices[0]   # 第一笔成交价
            high_price = max(prices)  # 最高成交价
            low_price = min(prices)   # 最低成交价
            close_price = prices[-1]  # 最后一笔成交价
            volume = sum((record.get("size", Decimal("0")) for record in bucket), Decimal("0"))
            return AggregatedBar(
                meta.alias, open_price, high_price, low_price, close_price, volume, ts_end
            )

        # ========== 策略2：NBBO回退（无tick时） ==========
        quote = self._quotes[meta.alias]
        bid = quote.get("bid")
        ask = quote.get("ask")
        if bid is not None and ask is not None:
            mid = (bid + ask) / Decimal(2)
        else:
            # ========== 策略3：last_trade回退 ==========
            mid = self._last_trade.get(meta.alias) or Decimal("0")
        
        # NBBO回退K线：open=high=low=close=mid, volume=0
        return AggregatedBar(meta.alias, mid, mid, mid, mid, Decimal("0"), ts_end)

    def _persist_equity_bar(self, session: Session, bar: AggregatedBar) -> None:
        stmt = text(
            """
            INSERT INTO bars1m_equity (ts_end, symbol, open, high, low, close, volume)
            VALUES (:ts_end, :symbol, :open, :high, :low, :close, :volume)
            ON CONFLICT (symbol, ts_end) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """
        )
        session.execute(
            stmt,
            {
                "ts_end": bar.ts_end,
                "symbol": bar.symbol,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            },
        )

    def _persist_option_bar(self, session: Session, meta: SymbolMeta, bar: AggregatedBar) -> None:
        if meta.conid is None:
            return
        quote = self._quotes.get(meta.alias, {})
        bid = quote.get("bid")
        ask = quote.get("ask")
        metrics = self._option_metrics.get(meta.alias, {})
        iv = metrics.get("implied_vol")
        delta = metrics.get("delta")
        gamma = metrics.get("gamma")
        theta = metrics.get("theta")
        vega = metrics.get("vega")
        oi = metrics.get("open_interest")
        und_price = metrics.get("underlying_price") or self._last_equity_close.get(meta.underlying)
        stmt = text(
            """
            INSERT INTO bars1m_option (
                conid,
                underlying_symbol,
                expiry,
                right,
                strike,
                ts_end,
                bid,
                ask,
                mid,
                volume,
                open_interest,
                implied_vol,
                delta,
                gamma,
                theta,
                vega,
                underlying_price
            ) VALUES (
                :conid,
                :underlying_symbol,
                :expiry,
                :right,
                :strike,
                :ts_end,
                :bid,
                :ask,
                :mid,
                :volume,
                :open_interest,
                :implied_vol,
                :delta,
                :gamma,
                :theta,
                :vega,
                :underlying_price
            )
            ON CONFLICT (conid, ts_end) DO UPDATE SET
                bid = EXCLUDED.bid,
                ask = EXCLUDED.ask,
                mid = EXCLUDED.mid,
                volume = EXCLUDED.volume,
                open_interest = EXCLUDED.open_interest,
                implied_vol = EXCLUDED.implied_vol,
                delta = EXCLUDED.delta,
                gamma = EXCLUDED.gamma,
                theta = EXCLUDED.theta,
                vega = EXCLUDED.vega,
                underlying_price = EXCLUDED.underlying_price,
                updated_at = now()
            """
        )
        session.execute(
            stmt,
            {
                "conid": meta.conid,
                "underlying_symbol": meta.underlying,
                "expiry": meta.expiry.date() if meta.expiry else None,
                "right": meta.right,
                "strike": float(meta.strike) if meta.strike is not None else None,
                "ts_end": bar.ts_end,
                "bid": float(bid) if bid is not None else None,
                "ask": float(ask) if ask is not None else None,
                "mid": float(((bid + ask) / 2)) if bid is not None and ask is not None else None,
                "volume": int(bar.volume),
                "open_interest": int(oi) if oi is not None else None,
                "implied_vol": float(iv) if iv is not None else None,
                "delta": float(delta) if delta is not None else None,
                "gamma": float(gamma) if gamma is not None else None,
                "theta": float(theta) if theta is not None else None,
                "vega": float(vega) if vega is not None else None,
                "underlying_price": float(und_price) if und_price is not None else None,
            },
        )

    def _publish_bar(self, meta: SymbolMeta, bar: AggregatedBar) -> None:
        if meta.kind != "EQUITY":
            return
        event = BarsClosed(
            trace_id=str(uuid4()),
            symbol=meta.underlying,
            bar_start=bar.ts_end - timedelta(minutes=1),
            bar_end=bar.ts_end,
            timeframe="1m",
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=int(bar.volume),
            vwap=None,
            source="aggregator",
            received_at=utc_now(),
        )
        payload = attach_trace_metadata(event.model_dump())
        self._logger.info("aggregator.bar_closed", payload=payload)
        if self._redis_bus is not None:
            trace_id = str(payload.get("trace_id") or "")
            if trace_id:
                publish_coro = self._redis_bus.publish("bars_closed", payload, trace_id=trace_id)
                if self._loop and self._loop.is_running():
                    self._loop.create_task(self._safe_publish(publish_coro, meta.underlying))
                else:
                    asyncio.create_task(self._safe_publish(publish_coro, meta.underlying))

    async def _safe_publish(self, awaitable: Awaitable[Any], symbol: str) -> None:
        try:
            await awaitable
        except Exception:  # pragma: no cover - external dependency
            self._logger.exception("aggregator.bar_publish_failed", symbol=symbol)

    # ------------------------------------------------------------------
    def _backfill_missing(self, session: Session, symbol: str, target_ts_end: datetime) -> None:
        """
        补充短期数据缺口（≤15分钟）
        
        场景：
        - IBKR连接短暂断开（网络抖动）
        - 某些分钟没有tick（低流动性股票）
        - 订阅延迟导致数据丢失
        
        策略：
        - 检测last_persisted到target之间的缺口（分钟数）
        - 如果缺口≤15分钟，调用IBKR历史数据API补充
        - 如果缺口>15分钟，记录警告，不补（避免阻塞主循环）
        
        仅处理股票，期权不补（期权历史数据API限制较多）
        
        Args:
            session: DB session
            symbol: 股票代码
            target_ts_end: 目标时间（要补到的时间）
        """
        # 仅处理股票
        meta = self._symbol_meta.get(symbol)
        if meta is None or meta.kind == "OPTION":
            return
        
        # ========== 1. 获取上一根K线时间 ==========
        last_ts = self._last_persisted.get(symbol)
        if last_ts is None:
            # 首次，从数据库查询最后一根K线时间
            result = session.execute(
                text("SELECT max(ts_end) FROM bars1m_equity WHERE symbol=:symbol"),
                {"symbol": symbol},
            ).scalar_one_or_none()
            if isinstance(result, datetime):
                last_ts = result.astimezone(timezone.utc)
            self._last_persisted[symbol] = last_ts

        # 如果没有历史数据，跳过（新股票）
        if last_ts is None:
            return

        # ========== 2. 计算缺口大小 ==========
        missing = int((target_ts_end - last_ts).total_seconds() // 60)
        
        # 无缺口或已补全
        if missing <= 0:
            return
        
        # 缺口太大（>15分钟），记录警告，不补
        # 避免阻塞主循环，影响实时数据处理
        if missing > 15:
            self._logger.warning(
                "aggregator.gap_exceeds_window",
                symbol=symbol,
                last_seen=str(last_ts),
                target=str(target_ts_end),
                missing_minutes=missing,
            )
            return

        start = last_ts + timedelta(minutes=1)
        end = target_ts_end
        try:
            bars = self._ib_client.req_historical_1m(
                symbol,
                start.astimezone(EASTERN),
                end.astimezone(EASTERN),
                use_rth=True,
            )
        except Exception as exc:  # pragma: no cover - external call
            self._logger.warning("aggregator.backfill_failed", symbol=symbol, error=str(exc))
            return

        for raw in bars:
            aggregated = self._convert_historical_bar(symbol, raw)
            if aggregated.ts_end <= last_ts or aggregated.ts_end > target_ts_end:
                continue
            session.execute(
                text(
                    """
                    INSERT INTO bars1m_equity (ts_end, symbol, open, high, low, close, volume)
                    VALUES (:ts_end, :symbol, :open, :high, :low, :close, :volume)
                    ON CONFLICT (symbol, ts_end) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                    """
                ),
                {
                    "ts_end": aggregated.ts_end,
                    "symbol": aggregated.symbol,
                    "open": aggregated.open,
                    "high": aggregated.high,
                    "low": aggregated.low,
                    "close": aggregated.close,
                    "volume": aggregated.volume,
                },
            )
            last_ts = aggregated.ts_end
        self._last_persisted[symbol] = last_ts

    def _convert_historical_bar(self, symbol: str, raw: Mapping[str, object]) -> AggregatedBar:
        time_str = str(raw.get("time"))
        dt_et = datetime.strptime(time_str, "%Y%m%d %H:%M:%S").replace(tzinfo=EASTERN)
        ts_end_utc = (dt_et + timedelta(minutes=1)).astimezone(timezone.utc)
        return AggregatedBar(
            symbol=symbol,
            open=Decimal(str(raw.get("open", "0"))),
            high=Decimal(str(raw.get("high", "0"))),
            low=Decimal(str(raw.get("low", "0"))),
            close=Decimal(str(raw.get("close", "0"))),
            volume=Decimal(str(raw.get("volume", "0"))),
            ts_end=ts_end_utc,
        )

    def _update_indicators(self, session: Session, symbol: str, ts_end: datetime) -> None:
        """
        计算技术指标并写入indicators_eq_1m表
        
        指标列表（13个）：
        - RSI: rsi6, rsi12, rsi24
        - Bollinger: boll_mid, boll_up, boll_dn
        - ATR: atr14
        - AO: Awesome Oscillator
        - Stochastic: stoch_k, stoch_d, stoch_rsi_k, stoch_rsi_d
        - CCI: cci14, cci6
        - SMA: sma5
        - OBV: obv, obv_ma6, obv_ema20
        - MFI: mfi14
        - RVOL: rvol6（相对成交量，依赖baseline）
        - Slopes: lr_m5_slope, lr_boll_dn_slope, lr_obv_slope
        
        数据需求：
        - 读取最近120根K线（足够计算所有指标）
        - 从rvol_baseline_eq获取RVOL基线
        
        计算引擎：
        - apps/rt_engine/indicators.py::compute_indicator_row()
        - 使用pandas-ta和自定义计算
        
        Args:
            session: DB session
            symbol: 股票代码
            ts_end: K线结束时间
        """
        # ========== 1. 读取最近120根K线 ==========
        rows = session.execute(
            text(
                """
                SELECT ts_end, open, high, low, close, volume
                FROM bars1m_equity
                WHERE symbol = :symbol AND ts_end <= :ts_end
                ORDER BY ts_end DESC
                LIMIT 120
                """
            ),
            {"symbol": symbol, "ts_end": ts_end},
        ).all()
        if not rows:
            return

        # ========== 2. 转换为DataFrame ==========
        df = pd.DataFrame(rows, columns=["ts_end", "open", "high", "low", "close", "volume"])
        df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)
        df.set_index("ts_end", inplace=True)
        df = df.sort_index()  # 从旧到新排序

        # ========== 3. 获取RVOL基线 ==========
        # 从rvol_baseline_eq表读取或使用缓存
        # baseline_map: {minute_index: mean_vol}
        # 示例：{570: 1000000, 571: 950000, ...}  # 570=09:30
        baseline_map = self._get_baseline_map(session, symbol)
        
        # ========== 4. 调用指标计算引擎 ==========
        # compute_indicator_row来自apps/rt_engine/indicators.py
        # 返回: {"rsi6": 45.3, "atr14": 2.15, "rvol6": 1.8, ...}
        indicators = compute_indicator_row(df, baseline_map)
        if not indicators:
            return

        # ========== 5. 准备写入payload ==========
        payload = {
            "ts_end": ts_end,
            "symbol": symbol,
            **{key: _to_decimal(value) for key, value in indicators.items()},
        }

        session.execute(
            text(
                """
                INSERT INTO indicators_eq_1m (
                    ts_end, symbol,
                    rsi6, rsi12, rsi24,
                    boll_mid, boll_up, boll_dn,
                    atr14, ao,
                    stoch_k, stoch_d, stoch_rsi_k, stoch_rsi_d,
                    cci14, cci6,
                    sma5, lr_m5_slope, lr_boll_dn_slope, lr_obv_slope,
                    obv, obv_ma6, obv_ema20,
                    mfi14, rvol6
                ) VALUES (
                    :ts_end, :symbol,
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

    def _get_baseline_map(self, session: Session, symbol: str) -> Dict[int, Decimal]:
        cached = self._baseline_cache.get(symbol)
        if cached is not None:
            return cached
        rows = session.execute(
            text("SELECT minute_index, mean_vol_20d FROM rvol_baseline_eq WHERE symbol = :symbol"),
            {"symbol": symbol},
        ).all()
        baseline = {row[0]: Decimal(row[1]) for row in rows}
        self._baseline_cache[symbol] = baseline
        return baseline

    def _ib_bar_to_eastern(self, raw_time: Any) -> datetime | None:
        if raw_time is None:
            return None
        try:
            timestamp = int(raw_time)
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(EASTERN)
        except (TypeError, ValueError):
            try:
                return datetime.strptime(str(raw_time), "%Y%m%d %H:%M:%S").replace(tzinfo=EASTERN)
            except (TypeError, ValueError):
                return None

    # ------------------------------------------------------------------
    # Watchlist动态管理
    # ------------------------------------------------------------------
    
    def _refresh_watchlist(self) -> None:
        """
        刷新订阅列表（定期触发，默认30秒）
        
        订阅来源：
        1. premarket_top5表（当日盘前Top5，动态）
        2. strategy_positions表（有持仓的股票，动态）
        3. static_symbols（命令行指定的固定股票，静态）
        
        对每个股票：
        1. 订阅股票L1行情（bid/ask/last）
        2. 查询最佳CALL合约 → 订阅期权L1+Greeks
        3. 查询最佳PUT合约 → 订阅期权L1+Greeks
        
        期权选择标准：
        - DTE: 2-7天
        - OTM: +2~+5档
        - OI: ≥500
        - Volume: ≥100
        - 价差: ≤max($0.10, 5%*mid)
        
        增量更新：
        - 仅订阅新增的symbol
        - 仅取消订阅不再需要的symbol
        - 已存在的订阅保持不变（避免重复订阅）
        """
        # ========== 1. 加载活跃股票列表 ==========
        try:
            desired_equities = self._load_watchlist()
            # 从premarket_top5（today）+ strategy_positions（qty!=0）读取
        except Exception:  # pragma: no cover - defensive
            self._logger.exception("aggregator.watchlist_refresh_failed")
            desired_equities = set()
        
        # 合并固定订阅股票（命令行指定）
        desired_equities |= self._static_symbols

        # ========== 2. 为每个股票生成订阅列表（股票+期权） ==========
        desired_aliases: Set[str] = set()
        session: Session = self._session_factory()
        try:
            for symbol in desired_equities:
                # 2.1 确保股票元数据存在
                meta = self._ensure_equity_meta(symbol)
                desired_aliases.add(meta.alias)  # 添加股票alias
                
                # 2.2 查询最佳期权合约（CALL + PUT）
                # 返回: ["OPT:AAPL:CALL:20251025:175.0", "OPT:AAPL:PUT:20251025:170.0"]
                option_aliases = self._ensure_option_aliases(session, symbol)
                desired_aliases.update(option_aliases)  # 添加期权alias
        finally:
            session.close()

        # ========== 3. 计算增量（新增/移除） ==========
        current = set(self._active_symbols)
        to_add = desired_aliases - current      # 需要新增订阅
        to_remove = current - desired_aliases   # 需要取消订阅

        if to_add or to_remove:
            self._logger.info(
                "aggregator.watchlist_updated",
                added=sorted(to_add),
                removed=sorted(to_remove),
            )

        client = self._ib_client
        for alias in to_add:
            if self._loop is None:
                raise RuntimeError("Event loop not initialised before watchlist refresh")
            alias_meta = self._symbol_meta.get(alias)
            if alias_meta is None:
                continue
            try:
                if alias_meta.kind == "EQUITY":
                    req_id, queue = client.subscribe_l1(
                        alias_meta.underlying,
                        alias=alias,
                        sec_type="STK",
                    )
                else:
                    if not (
                        alias_meta.expiry and alias_meta.right and alias_meta.strike is not None
                    ):
                        self._logger.warning("aggregator.option_meta_incomplete", alias=alias)
                        continue
                    contract = alias_meta.contract
                    if contract is None:
                        contract = client.option_contract(
                            symbol=alias_meta.underlying,
                            expiry=alias_meta.expiry.strftime("%Y%m%d"),
                            strike=float(alias_meta.strike),
                            right=alias_meta.right,
                            conid=alias_meta.conid,
                        )
                    req_id, queue = client.subscribe_l1(
                        alias_meta.underlying,
                        sec_type="OPT",
                        exchange="SMART",
                        currency="USD",
                        generic_ticks=self._option_generic_ticks,
                        alias=alias,
                        contract=contract,
                    )
                    if contract.conId:
                        alias_meta.conid = contract.conId
                        alias_meta.contract = contract
            except Exception as exc:  # pragma: no cover - external
                self._logger.exception("aggregator.subscribe_failed", symbol=alias, error=str(exc))
                continue
            self._symbol_queues[alias] = queue
            task = self._loop.run_in_executor(None, self._consume_ticks, alias, queue)
            self._symbol_tasks[alias] = task
            self._active_symbols.add(alias)
            self._logger.info("aggregator.subscribed", symbol=alias, req_id=req_id)

        for alias in to_remove:
            self._deactivate_symbol(alias)

    def _load_watchlist(self) -> Set[str]:
        session: Session = self._session_factory()
        try:
            et_today = datetime.now(EASTERN).date()
            top5_symbols = session.execute(
                select(PremarketTop5.symbol).where(PremarketTop5.trade_date == et_today)
            ).scalars()
            open_positions = session.execute(
                select(StrategyPosition.symbol).where(StrategyPosition.open_quantity != 0)
            ).scalars()
            symbols = {symbol.upper() for symbol in top5_symbols}
            symbols.update(symbol.upper() for symbol in open_positions)
            return symbols
        finally:
            session.close()

    def _deactivate_symbol(self, alias: str) -> None:
        self._active_symbols.discard(alias)
        queue = self._symbol_queues.pop(alias, None)
        if queue is not None:
            queue.put(None)
        task = self._symbol_tasks.pop(alias, None)
        if task is not None:
            task.cancel()
        try:
            self._ib_client.unsubscribe_l1(alias)
        except Exception:  # pragma: no cover - external
            self._logger.exception("aggregator.unsubscribe_failed", symbol=alias)
        self._buffers.pop(alias, None)
        self._quotes.pop(alias, None)
        self._last_trade.pop(alias, None)
        self._last_persisted.pop(alias, None)
        self._baseline_cache.pop(alias, None)
        self._option_metrics.pop(alias, None)
        self._volume_snapshots.pop(alias, None)
        meta = self._symbol_meta.pop(alias, None)
        if meta and meta.kind == "OPTION":
            option_list = self._underlying_options.get(meta.underlying, [])
            if alias in option_list:
                option_list.remove(alias)
        elif meta and meta.kind == "EQUITY":
            self._last_equity_close.pop(meta.underlying, None)
            option_list = list(self._underlying_options.pop(meta.underlying, []))
            for opt_alias in option_list:
                if opt_alias in self._active_symbols:
                    self._deactivate_symbol(opt_alias)

    def _teardown_subscriptions(self) -> None:
        for symbol in list(self._active_symbols):
            self._deactivate_symbol(symbol)

    # ------------------------------------------------------------------
    def _cache_vix_snapshot(self, price: Decimal) -> None:
        if self._redis is None:
            return
        payload = {"value": str(price), "ts": datetime.now(timezone.utc).isoformat()}
        try:
            self._redis.setex("risk:vix:last", 180, json.dumps(payload))
        except Exception:  # pragma: no cover - external dependency
            self._logger.warning("aggregator.vix_snapshot_store_failed")

    def _start_vix_subscription(self) -> None:
        if self._loop is None:
            raise RuntimeError("Event loop not initialised for VIX subscription")
        req_id, queue = self._ib_client.subscribe_l1(
            self._vix_symbol,
            sec_type="IND",
            exchange="CBOE",
            currency="USD",
            generic_ticks="",
        )
        self._logger.info("aggregator.vix_subscribed", req_id=req_id)
        self._vix_queue = queue
        self._vix_task = self._loop.run_in_executor(None, self._consume_vix_ticks, queue)

    def _consume_vix_ticks(self, queue: Queue) -> None:
        last_trade: Optional[Decimal] = None
        bid: Optional[Decimal] = None
        ask: Optional[Decimal] = None
        while True:
            if not self._running:
                break
            try:
                payload = queue.get(timeout=1)
            except Empty:
                continue
            if payload is None:
                break
            event_type = payload.get("type")
            if event_type == "price":
                price = Decimal(str(payload.get("price")))
                field = payload.get("field")
                if field == 4:
                    last_trade = price
                elif field == 1:
                    bid = price
                elif field == 2:
                    ask = price
            elif event_type == "rt_volume":
                price = Decimal(str(payload.get("price", "0")))
                last_trade = price

            if last_trade is not None:
                self._vix_last_price = last_trade
            elif bid is not None and ask is not None:
                self._vix_last_price = (bid + ask) / Decimal(2)
            if self._vix_last_price is not None:
                self._cache_vix_snapshot(self._vix_last_price)

    def _stop_vix_subscription(self) -> None:
        queue = self._vix_queue
        if queue is not None:
            queue.put(None)
        task = self._vix_task
        if task is not None:
            task.cancel()
        try:
            self._ib_client.unsubscribe_l1(self._vix_symbol)
        except Exception:  # pragma: no cover - external dependency
            self._logger.exception("aggregator.vix_unsubscribe_failed")
        self._vix_queue = None
        self._vix_task = None


async def _top5_schedule_loop(top5_service: Top5Service) -> None:
    """
    Top5选股定时任务 - 每个交易日09:30:01触发
    
    功能：
    - 基于盘前5分钟（09:25-09:30）涨幅排序
    - 应用Universe过滤（市值/流动性）
    - 应用Pre-Earnings过滤（财报前3-7天）
    - 写入premarket_top5表
    
    触发时机：
    - 每个交易日（周一~周五）
    - 东部时间09:30:01（盘前窗口关闭后立即计算）
    - 每天只运行一次（last_run标记）
    
    失败策略：
    - 捕获异常，记录日志
    - 不中断aggregator主循环
    - 下一个交易日重试
    
    Args:
        top5_service: Top5服务实例
    """
    logger = structlog.get_logger(__name__).bind(component="top5_scheduler")
    last_run: date | None = None
    
    try:
        while True:
            now_et = utc_now().astimezone(EASTERN)
            
            # 周末跳过（周六日不计算）
            if now_et.weekday() >= 5:
                await asyncio.sleep(1800)  # 休眠30分钟
                continue
            
            # 目标触发时间：09:30:01（盘前窗口关闭后立即计算）
            target = datetime.combine(now_et.date(), time(9, 30, 1), tzinfo=EASTERN)
            
            if now_et >= target:
                # 已过触发时间，检查今天是否已运行
                if last_run != now_et.date():
                    logger.info("top5_scheduler.run_start", trade_date=str(now_et.date()))
                    try:
                        # 在线程池中运行（避免阻塞asyncio主循环）
                        await asyncio.to_thread(top5_service.run_for_today, now_et.date())
                        last_run = now_et.date()
                        logger.info("top5_scheduler.run_success", trade_date=str(now_et.date()))
                    except Exception:
                        logger.exception("top5_scheduler.run_failed", trade_date=str(now_et.date()))
                
                # 已运行，休眠5分钟等待下一次检查
                await asyncio.sleep(300)
            else:
                # 未到触发时间，计算等待秒数
                wait_seconds = max((target - now_et).total_seconds(), 5.0)
                # 最多休眠60秒，避免错过触发时间
                await asyncio.sleep(min(wait_seconds, 60.0))
    
    except asyncio.CancelledError:  # pragma: no cover - cooperative shutdown
        logger.info("top5_scheduler.stopped")
        raise


async def _run_services(aggregator: MinuteAggregator, top5_service: Top5Service) -> None:
    scheduler_task = asyncio.create_task(_top5_schedule_loop(top5_service))
    try:
        await aggregator.run()
    finally:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass


def main(argv: Optional[List[str]] = None) -> int:
    """
    MinuteAggregator主程序入口
    
    功能：
    1. 初始化IBKR客户端（连接TWS/Gateway）
    2. 启动MinuteAggregator（K线聚合+指标计算）
    3. 启动Top5定时任务（09:30:01选股）
    4. 优雅退出（Ctrl+C断开IBKR连接）
    
    命令行参数：
    - --extra-symbols: 固定订阅的股票列表（不受watchlist影响）
      示例：--extra-symbols SPY QQQ AAPL
    - --watchlist-refresh: watchlist刷新间隔（秒，默认30）
    - --database-url: 覆盖配置文件的数据库URL
    
    运行示例：
    ```bash
    # 默认配置运行
    python -m apps.rt_engine.aggregator
    
    # 固定订阅SPY和QQQ，60秒刷新watchlist
    python -m apps.rt_engine.aggregator --extra-symbols SPY QQQ --watchlist-refresh 60
    ```
    
    启动检查：
    1. IBKR连接：确保TWS/Gateway已启动并启用API连接
    2. 数据库连接：确保TimescaleDB可访问
    3. Redis连接：确保Redis可访问（用于bars_closed事件）
    
    Args:
        argv: 命令行参数列表（测试用，默认None使用sys.argv）
    
    Returns:
        0: 正常退出
    """
    # ========== 1. 解析命令行参数 ==========
    parser = argparse.ArgumentParser(description="Real-time minute aggregator")
    parser.add_argument(
        "--extra-symbols",
        nargs="+",
        default=[],
        help="Additional symbols to always subscribe regardless of watchlist",
    )
    parser.add_argument(
        "--watchlist-refresh",
        type=int,
        default=30,
        help="Watchlist refresh cadence in seconds (default: 30)",
    )
    parser.add_argument("--database-url", default=None, help="Override database URL")
    args = parser.parse_args(argv)

    # ========== 2. 初始化配置和日志 ==========
    settings = get_settings()
    configure_logging(settings)

    # ========== 3. 初始化数据库连接 ==========
    database_url = args.database_url or settings.database_url
    engine = create_engine(database_url, future=True)
    Base.metadata.create_all(engine)  # 确保表结构存在
    session_factory = sessionmaker(bind=engine, future=True)

    # ========== 4. 初始化IBKR客户端 ==========
    # 连接到TWS/Gateway（配置在settings中）
    ib_client = build_ibkr_client(settings)
    
    # ========== 5. 初始化MinuteAggregator ==========
    aggregator = MinuteAggregator(
        ib_client,
        session_factory,
        settings,
        watchlist_refresh_seconds=args.watchlist_refresh,
        static_symbols=args.extra_symbols,
        vix_required=settings.vix_required,
    )
    
    # ========== 6. 初始化Top5服务 ==========
    top5_service = Top5Service(ib_client, session_factory, settings)

    # ========== 7. 运行服务（阻塞） ==========
    try:
        # 同时运行aggregator和top5_scheduler
        # aggregator: 主循环（每分钟触发）
        # top5_scheduler: 定时任务（每天09:30:01触发）
        asyncio.run(_run_services(aggregator, top5_service))
    except KeyboardInterrupt:  # pragma: no cover
        # Ctrl+C优雅退出
        pass
    finally:
        # ========== 8. 清理资源 ==========
        # 断开IBKR连接，停止后台线程
        ib_client.disconnect_and_stop()

    return 0


if __name__ == "__main__":
    main()


def _to_decimal(value: Optional[Decimal]) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
