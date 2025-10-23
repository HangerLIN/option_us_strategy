"""
数据拉取进度监控指标

与 ingest_equity_1m_ibkr.py 深度集成的 Prometheus 指标定义
"""
from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import date
from typing import Iterator, List, Optional

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

__all__ = [
    "DataIngestionMonitor",
    "start_metrics_server",
]


# ============================================================
# Prometheus 指标定义
# ============================================================

# 任务信息
TASK_INFO = Info(
    "data_ingest_task",
    "Current data ingestion task metadata",
)

TASK_STATUS = Gauge(
    "data_ingest_task_status",
    "Task status: 0=idle, 1=running, 2=completed, 3=failed",
)

TASK_START_TIME = Gauge(
    "data_ingest_task_start_timestamp",
    "Task start timestamp (Unix seconds)",
)

# K线数据进度
BARS_REQUESTED = Counter(
    "data_ingest_bars_requested_total",
    "Total number of historical bar requests sent to IBKR",
    labelnames=["symbol", "trade_date"],
)

BARS_RECEIVED = Counter(
    "data_ingest_bars_received_total",
    "Number of bars received from IBKR",
    labelnames=["symbol", "trade_date"],
)

BARS_STORED = Counter(
    "data_ingest_bars_stored_total",
    "Number of bars stored to database",
    labelnames=["symbol", "trade_date"],
)

BARS_PER_SYMBOL_DATE = Gauge(
    "data_ingest_bars_count",
    "Number of bars for specific symbol and date",
    labelnames=["symbol", "trade_date"],
)

# 指标计算进度
INDICATORS_COMPUTED = Counter(
    "data_ingest_indicators_computed_total",
    "Number of indicator rows computed",
    labelnames=["symbol", "trade_date"],
)

INDICATORS_FAILED = Counter(
    "data_ingest_indicators_failed_total",
    "Number of indicator computation failures",
    labelnames=["symbol", "trade_date", "reason"],
)

INDICATOR_COMPUTATION_TIME = Histogram(
    "data_ingest_indicator_computation_seconds",
    "Time spent computing indicators per symbol/date",
    labelnames=["symbol", "trade_date"],
    buckets=(0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0),
)

# 股票处理进度
SYMBOLS_TOTAL = Gauge(
    "data_ingest_symbols_total",
    "Total number of symbols to process",
)

SYMBOLS_COMPLETED = Gauge(
    "data_ingest_symbols_completed",
    "Number of symbols fully completed (all dates)",
)

SYMBOLS_IN_PROGRESS = Gauge(
    "data_ingest_symbols_in_progress",
    "Number of symbols currently being processed",
)

SYMBOLS_FAILED = Counter(
    "data_ingest_symbols_failed_total",
    "Number of symbol processing failures",
    labelnames=["symbol", "reason"],
)

# 日期处理进度
DATES_TOTAL = Gauge(
    "data_ingest_dates_total",
    "Total number of trading dates to process",
)

DATES_COMPLETED = Gauge(
    "data_ingest_dates_completed",
    "Number of trading dates completed",
)

SYMBOL_DATE_PAIRS_TOTAL = Gauge(
    "data_ingest_symbol_date_pairs_total",
    "Total number of (symbol, date) pairs to process",
)

SYMBOL_DATE_PAIRS_COMPLETED = Counter(
    "data_ingest_symbol_date_pairs_completed_total",
    "Number of (symbol, date) pairs completed",
)

# 批次进度
BATCH_INDEX = Gauge(
    "data_ingest_batch_index",
    "Current batch being processed (1-indexed)",
)

BATCH_TOTAL = Gauge(
    "data_ingest_batch_total",
    "Total number of batches",
)

# IBKR 连接状态
IBKR_CONNECTION = Gauge(
    "data_ingest_ibkr_connected",
    "IBKR connection status: 1=connected, 0=disconnected",
)

IBKR_ERRORS = Counter(
    "data_ingest_ibkr_errors_total",
    "IBKR API errors encountered",
    labelnames=["error_code", "symbol", "date"],
)

IBKR_PACING_VIOLATIONS = Counter(
    "data_ingest_ibkr_pacing_violations_total",
    "IBKR pacing violations encountered",
)

IBKR_RETRIES = Counter(
    "data_ingest_ibkr_retries_total",
    "Number of IBKR request retries",
    labelnames=["symbol", "trade_date", "attempt"],
)

# 性能指标
INGESTION_RATE = Gauge(
    "data_ingest_rate_bars_per_second",
    "Current ingestion rate (bars/second)",
)

ETA_SECONDS = Gauge(
    "data_ingest_eta_seconds",
    "Estimated time to completion (seconds)",
)

ELAPSED_SECONDS = Gauge(
    "data_ingest_elapsed_seconds",
    "Elapsed time since task start (seconds)",
)

COMPLETION_PCT = Gauge(
    "data_ingest_completion_percentage",
    "Overall completion percentage (0-100)",
)

# 数据库状态（可选，定期查询）
DB_BARS_TOTAL = Gauge(
    "data_ingest_db_bars_total",
    "Total bars in database (from periodic query)",
)

DB_INDICATORS_COMPLETE_PCT = Gauge(
    "data_ingest_db_indicators_complete_pct",
    "Percentage of bars with complete indicators (0-100)",
)


# ============================================================
# 监控器类
# ============================================================

class DataIngestionMonitor:
    """
    数据拉取监控器
    
    与 ingest_equity_1m_ibkr.py 深度集成，追踪进度并暴露 Prometheus 指标
    """
    
    def __init__(
        self,
        *,
        symbols: List[str],
        trade_dates: List[date],
        batch_size: Optional[int] = None,
        start_date_str: str,
        end_date_str: str,
        universe: str,
    ):
        self.symbols = symbols
        self.trade_dates = trade_dates
        self.batch_size = batch_size or len(symbols)
        self.start_time = time.time()
        
        self._lock = threading.Lock()
        self._completed_pairs: set[tuple[str, date]] = set()
        self._completed_symbols: set[str] = set()
        self._completed_dates: set[date] = set()
        self._total_bars_ingested = 0
        
        # 初始化任务信息
        TASK_INFO.info({
            "start_date": start_date_str,
            "end_date": end_date_str,
            "universe": universe,
            "symbols_count": str(len(symbols)),
            "dates_count": str(len(trade_dates)),
            "batch_size": str(self.batch_size),
        })
        
        TASK_STATUS.set(1)  # running
        TASK_START_TIME.set(self.start_time)
        
        SYMBOLS_TOTAL.set(len(symbols))
        DATES_TOTAL.set(len(trade_dates))
        
        total_pairs = len(symbols) * len(trade_dates)
        SYMBOL_DATE_PAIRS_TOTAL.set(total_pairs)
        
        SYMBOLS_COMPLETED.set(0)
        DATES_COMPLETED.set(0)
        ELAPSED_SECONDS.set(0)
        COMPLETION_PCT.set(0)
        
        IBKR_CONNECTION.set(1)  # 假设初始已连接
        
        # 批次信息
        num_batches = (len(symbols) + self.batch_size - 1) // self.batch_size
        BATCH_TOTAL.set(num_batches)
        BATCH_INDEX.set(0)
    
    def record_ibkr_connected(self):
        """记录 IBKR 连接成功"""
        IBKR_CONNECTION.set(1)
    
    def record_ibkr_disconnected(self):
        """记录 IBKR 连接断开"""
        IBKR_CONNECTION.set(0)
    
    def record_ibkr_error(self, error_code: str, symbol: str, trade_date: date):
        """记录 IBKR API 错误"""
        IBKR_ERRORS.labels(
            error_code=error_code,
            symbol=symbol,
            date=trade_date.isoformat(),
        ).inc()
    
    def record_ibkr_pacing_violation(self):
        """记录 IBKR pacing violation"""
        IBKR_PACING_VIOLATIONS.inc()
    
    def record_ibkr_retry(self, symbol: str, trade_date: date, attempt: int):
        """记录 IBKR 请求重试"""
        IBKR_RETRIES.labels(
            symbol=symbol,
            trade_date=trade_date.isoformat(),
            attempt=str(attempt),
        ).inc()
    
    def record_bars_request(self, symbol: str, trade_date: date):
        """记录发送 K线数据请求"""
        BARS_REQUESTED.labels(
            symbol=symbol,
            trade_date=trade_date.isoformat(),
        ).inc()
    
    def record_bars_received(self, symbol: str, trade_date: date, count: int):
        """记录接收到的 K线数据"""
        date_str = trade_date.isoformat()
        BARS_RECEIVED.labels(symbol=symbol, trade_date=date_str).inc(count)
        BARS_PER_SYMBOL_DATE.labels(symbol=symbol, trade_date=date_str).set(count)
        
        with self._lock:
            self._total_bars_ingested += count
            self._update_rate_and_eta()
    
    def record_bars_stored(self, symbol: str, trade_date: date, count: int):
        """记录 K线存储到数据库"""
        BARS_STORED.labels(
            symbol=symbol,
            trade_date=trade_date.isoformat(),
        ).inc(count)
    
    @contextmanager
    def track_indicator_computation(
        self, symbol: str, trade_date: date
    ) -> Iterator[None]:
        """上下文管理器：追踪指标计算时间"""
        date_str = trade_date.isoformat()
        start = time.time()
        try:
            yield
            # 成功
            duration = time.time() - start
            INDICATOR_COMPUTATION_TIME.labels(
                symbol=symbol,
                trade_date=date_str,
            ).observe(duration)
        except Exception as exc:
            # 失败
            INDICATORS_FAILED.labels(
                symbol=symbol,
                trade_date=date_str,
                reason=type(exc).__name__,
            ).inc()
            raise
    
    def record_indicators_computed(self, symbol: str, trade_date: date, count: int):
        """记录指标计算完成"""
        INDICATORS_COMPUTED.labels(
            symbol=symbol,
            trade_date=trade_date.isoformat(),
        ).inc(count)
    
    def record_symbol_date_completed(self, symbol: str, trade_date: date):
        """记录 (symbol, date) 对完成"""
        with self._lock:
            pair = (symbol, trade_date)
            if pair in self._completed_pairs:
                return
            
            self._completed_pairs.add(pair)
            SYMBOL_DATE_PAIRS_COMPLETED.inc()
            
            # 检查该 symbol 是否所有日期都完成
            if all((symbol, d) in self._completed_pairs for d in self.trade_dates):
                if symbol not in self._completed_symbols:
                    self._completed_symbols.add(symbol)
                    SYMBOLS_COMPLETED.set(len(self._completed_symbols))
            
            # 检查该 date 是否所有 symbol 都完成
            if all((s, trade_date) in self._completed_pairs for s in self.symbols):
                if trade_date not in self._completed_dates:
                    self._completed_dates.add(trade_date)
                    DATES_COMPLETED.set(len(self._completed_dates))
            
            # 更新完成百分比
            total_pairs = len(self.symbols) * len(self.trade_dates)
            completed_pairs = len(self._completed_pairs)
            pct = (completed_pairs / total_pairs) * 100 if total_pairs > 0 else 0
            COMPLETION_PCT.set(pct)
            
            self._update_rate_and_eta()
    
    def record_symbol_failed(self, symbol: str, reason: str):
        """记录 symbol 处理失败"""
        SYMBOLS_FAILED.labels(symbol=symbol, reason=reason).inc()
    
    def record_batch_start(self, batch_index: int):
        """记录批次开始"""
        BATCH_INDEX.set(batch_index)
    
    def record_symbol_in_progress(self, count: int):
        """记录当前正在处理的 symbol 数量"""
        SYMBOLS_IN_PROGRESS.set(count)
    
    def _update_rate_and_eta(self):
        """更新速率和 ETA（需要持锁调用）"""
        elapsed = time.time() - self.start_time
        ELAPSED_SECONDS.set(elapsed)
        
        # 计算速率（bars/秒）
        if elapsed > 0:
            rate = self._total_bars_ingested / elapsed
            INGESTION_RATE.set(rate)
        else:
            INGESTION_RATE.set(0)
        
        # 计算 ETA
        total_pairs = len(self.symbols) * len(self.trade_dates)
        completed_pairs = len(self._completed_pairs)
        
        if completed_pairs > 0 and elapsed > 0:
            avg_time_per_pair = elapsed / completed_pairs
            remaining_pairs = total_pairs - completed_pairs
            eta = avg_time_per_pair * remaining_pairs
            ETA_SECONDS.set(max(0, eta))
        else:
            ETA_SECONDS.set(0)
    
    def mark_completed(self):
        """标记任务完成"""
        TASK_STATUS.set(2)  # completed
        COMPLETION_PCT.set(100)
        ETA_SECONDS.set(0)
    
    def mark_failed(self):
        """标记任务失败"""
        TASK_STATUS.set(3)  # failed
        
    def get_stats(self) -> dict:
        """获取当前统计信息（用于日志）"""
        with self._lock:
            total_pairs = len(self.symbols) * len(self.trade_dates)
            completed_pairs = len(self._completed_pairs)
            pct = (completed_pairs / total_pairs) * 100 if total_pairs > 0 else 0
            elapsed = time.time() - self.start_time
            
            eta = 0
            rate = 0
            if completed_pairs > 0 and elapsed > 0:
                avg_time = elapsed / completed_pairs
                remaining = total_pairs - completed_pairs
                eta = avg_time * remaining
                rate = self._total_bars_ingested / elapsed
            
            return {
                "symbols_completed": len(self._completed_symbols),
                "symbols_total": len(self.symbols),
                "dates_completed": len(self._completed_dates),
                "dates_total": len(self.trade_dates),
                "pairs_completed": completed_pairs,
                "pairs_total": total_pairs,
                "completion_pct": round(pct, 2),
                "elapsed_seconds": round(elapsed, 1),
                "eta_seconds": round(eta, 1),
                "eta_minutes": round(eta / 60, 1),
                "total_bars": self._total_bars_ingested,
                "rate_bars_per_sec": round(rate, 2),
            }


def start_metrics_server(port: int = 9091) -> None:
    """
    启动 Prometheus metrics HTTP server
    
    Args:
        port: HTTP server 端口，默认 9091
    """
    try:
        start_http_server(port)
    except OSError as exc:
        # 端口已被占用，可能是之前的进程
        if "Address already in use" in str(exc):
            import logging
            logging.warning(f"Metrics server port {port} already in use, skipping start")
        else:
            raise

