from __future__ import annotations

import asyncio
import copy
import statistics
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple
from uuid import uuid4

import pandas as pd
import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from libs.core import EASTERN, get_settings
from libs.db import RiskStateDAO, SignalLogDAO
from libs.db.preearn import evaluate_preearn_guard
from libs.infra.metrics import (
    observe_bar_latency,
    record_signal_emitted,
    observe_signal_generation,
    record_signal_filtered,
)
from libs.infra.redis_bus import RedisBus
from libs.schemas.events import BarsClosed
from libs.schemas.signals import SignalEnvelope, SignalPushItem, SignalSide

from .top5_source import PremarketTop5Source, Top5Source
from apps.risk_svc.limits import LimitsCache

LOGGER = structlog.get_logger(__name__)

OPTION_HINT = {
    "dte": [2, 7],
    "otm_steps": [2, 5],
    "delta": [0.35, 0.45],
}

RISK_HINT = {
    "size": "1R",
    "max_total": "2R",
    "stop_pct": "-0.08",
}

BUY_SIGNALS = {
    "SIG_OPEN_CHASE_BUY",
    "SIG_REBOUND_BUY",
    "SIG_PM_BOTTOM_A2",
    "SIG_PM_BOTTOM_A3",
    "SIG_PM_BOTTOM_A4",
    "SIG_AM_BOTTOM_A1",
    "SIG_AM_CONFLUENCE_BUY_A2",
}

SELL_SIGNALS = {
    "SIG_EXIT_UPPER_TAP_X2",
    "SIG_EXIT_BOX2MID",
    "SIG_TIME_CLEAR_12_14",
    "SIG_OVERNIGHT_GAP_EXIT",
    "SIG_AM_SELL_C1",
    "SIG_AM_CONFLUENCE_SELL_S2",
}


@dataclass
class PositionState:
    signal_code: str
    opened_at: datetime
    additions: int = 0


class DetectedSignal(NamedTuple):
    signal: SignalEnvelope
    trace_id: str
    ts_end: datetime


class SignalEngine:
    """Rule-based signal generator aligned with trading playbook."""

    def __init__(
        self,
        session_factory: sessionmaker,
        redis_bus: Optional[RedisBus] = None,
        *,
        strategy_code: str = "core-vol",
        history_window: int = 180,
        top5_source: Top5Source | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._redis_bus = redis_bus
        self._strategy_code = strategy_code
        self._history_window = max(60, history_window)
        self._settings = get_settings()
        self._ttl = self._settings.ttl_buy_seconds
        self._cooldown = self._settings.cooldown_buy_seconds
        self._mfi_stoch_filter = self._settings.feature_mfi_stoch_filter
        self._preearn_days_min = int(self._settings.preearn_days_min)
        self._preearn_days_max = int(self._settings.preearn_days_max)
        self._preearn_atr_pct_max = Decimal(str(self._settings.preearn_atr_pct_max))
        self._positions: Dict[str, PositionState] = {}
        self._upper_break: Dict[str, datetime] = {}
        self._last_signal_ts: Dict[Tuple[str, str], datetime] = {}
        self._top5_recent_cache: Dict[date, set[str]] = {}
        self._top5_today_cache: Dict[date, set[str]] = {}
        self._top5_source: Top5Source = top5_source or PremarketTop5Source()
        self._vix_cache: Tuple[Optional[Decimal], Optional[datetime]] = (None, None)
        self._metrics_enabled: bool = bool(self._settings.monitoring_enabled)
        limits_cache = LimitsCache(session_factory)
        risk_limits = limits_cache.snapshot
        self._am_bottom_limits = risk_limits.am_bottom
        self._am_sell1_limits = risk_limits.am_sell1
        self._am_conf_limits = risk_limits.am_conf
        self._vix_gate_mode = risk_limits.gate.vix.mode
        self._vix_gate = Decimal(str(risk_limits.gate.vix.thresh))
        self._ma60_cache: Dict[Tuple[str, date], Optional[Decimal]] = {}

        # Resolve bt_run for logging
        session: Session = self._session_factory()
        try:
            log_dao = SignalLogDAO(session)
            run = log_dao.ensure_run(self._strategy_code)
            self._run_id = run.run_id
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def process_bar(self, event: BarsClosed) -> List[SignalEnvelope]:
        """Evaluate strategies for a given 1m bar and emit qualified signals."""
        session: Session = self._session_factory()
        try:
            if self._metrics_enabled and event.received_at is not None:
                try:
                    latency = (event.received_at - event.bar_end).total_seconds()
                    observe_bar_latency(latency if latency >= 0 else 0.0)
                except Exception:
                    pass
            t0 = time_module.perf_counter()
            detected = self._evaluate_event(
                session,
                event,
                persist=True,
                publish=True,
                mutate_state=True,
                record_metric=True,
            )
            session.commit()
            try:
                if self._metrics_enabled:
                    dt = max(0.0, time_module.perf_counter() - t0)
                    observe_signal_generation(dt)
            except Exception:
                pass
            return [entry.signal for entry in detected]
        except Exception:
            session.rollback()
            LOGGER.exception(
                "signal_engine.failed", symbol=event.symbol, bar_end=str(event.bar_end)
            )
            raise
        finally:
            session.close()
        return []

    def preview_signals(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> List[DetectedSignal]:
        """Replay historical bars for preview without side effects."""
        session: Session = self._session_factory()
        snapshot = self._snapshot_state()
        detected: List[DetectedSignal] = []
        try:
            rows = session.execute(
                text(
                    """
                    SELECT ts_end, open, high, low, close, volume
                    FROM bars1m_equity
                    WHERE symbol = :symbol
                      AND ts_end BETWEEN :start AND :end
                    ORDER BY ts_end ASC
                    """
                ),
                {"symbol": symbol, "start": start, "end": end},
            ).all()
            if not rows:
                session.rollback()
                return []
            for ts_end, open_, high_, low_, close_, volume in rows:
                trace_id = f"preview-{symbol.upper()}-{int(ts_end.timestamp())}"
                event = BarsClosed(
                    trace_id=trace_id,
                    symbol=symbol,
                    bar_start=ts_end - timedelta(minutes=1),
                    bar_end=ts_end,
                    timeframe="1m",
                    open=_to_decimal(open_),
                    high=_to_decimal(high_),
                    low=_to_decimal(low_),
                    close=_to_decimal(close_),
                    volume=int(volume or 0),
                    vwap=None,
                    source="preview",
                    received_at=ts_end,
                )
                batch = self._evaluate_event(
                    session,
                    event,
                    persist=False,
                    publish=False,
                    mutate_state=True,
                    record_metric=False,
                )
                detected.extend(batch)
            session.rollback()
            return detected
        except Exception:
            session.rollback()
            LOGGER.exception("signal_engine.preview_failed", symbol=symbol)
            raise
        finally:
            session.close()
            self._restore_state(snapshot)

    def push_signals(self, items: Sequence[SignalPushItem]) -> List[Dict[str, Any]]:
        """Ingest externally supplied signals and forward to downstream channels."""
        if not items:
            return []
        session: Session = self._session_factory()
        results: List[Dict[str, Any]] = []
        try:
            for item in items:
                signal = item.signal
                ts_end = signal.generated_at
                trace_id = item.trace_id or str(uuid4())
                if (
                    signal.signal_code in BUY_SIGNALS
                    and not item.force
                    and not self._can_open_position(signal.symbol)[0]
                ):
                    results.append(
                        {
                            "accepted": False,
                            "trace_id": trace_id,
                            "reason": "portfolio_limit",
                            "signal": signal,
                        }
                    )
                    continue
                if not item.force and not self._should_emit(signal, ts_end):
                    results.append(
                        {
                            "accepted": False,
                            "trace_id": trace_id,
                            "reason": "cooldown",
                            "signal": signal,
                        }
                    )
                    continue
                detected = DetectedSignal(signal=signal, trace_id=trace_id, ts_end=ts_end)
                self._finalise_signal(
                    session,
                    detected,
                    persist=True,
                    publish=True,
                    mutate_state=True,
                    record_metric=True,
                )
                results.append(
                    {
                        "accepted": True,
                        "trace_id": trace_id,
                        "reason": None,
                        "signal": signal,
                    }
                )
            session.commit()
            return results
        except Exception:
            session.rollback()
            LOGGER.exception("signal_engine.push_failed")
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Detectors
    # ------------------------------------------------------------------
    def _detect_am_bottom_a1(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        if len(df) < max(20, self._am_bottom_limits.neg_seq_min + 6):
            return None
        et_time = _to_eastern(event.bar_end)
        if et_time.time() < time(9, 30) or et_time.time() >= time(12, 0):
            return None
        if not self._top5_today(trade_date, session, event.symbol):
            return None

        current_idx = len(df) - 1
        l2_idx = current_idx - 1
        pivots = self._find_pivot_lows(df, self._am_bottom_limits.pivot_w)
        if l2_idx not in pivots:
            return None
        pos = pivots.index(l2_idx)
        if pos == 0:
            return None
        l1_idx = pivots[pos - 1]
        low_l1 = df.iloc[l1_idx]["low"]
        low_l2 = df.iloc[l2_idx]["low"]
        if None in (low_l1, low_l2) or low_l2 >= low_l1:
            return None
        if not self._check_slope_sequences(
            df,
            l2_idx,
            self._am_bottom_limits.neg_seq_min,
            self._am_bottom_limits.pos_seq_min,
        ):
            return None
        current = df.iloc[current_idx]
        if (
            self._am_bottom_limits.allow_mid_filter
            and (
                current["close"] is None
                or current["boll_mid"] is None
                or current["close"] < current["boll_mid"]
            )
        ):
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None
        reason = {
            "pivot_l1": df.iloc[l1_idx]["ts_end"].isoformat(),
            "pivot_l2": df.iloc[l2_idx]["ts_end"].isoformat(),
            "low_l1": str(low_l1),
            "low_l2": str(low_l2),
            "m5_slope": str(df.iloc[l2_idx]["lr_m5_slope"]),
            "dn_slope": str(df.iloc[l2_idx]["lr_boll_dn_slope"]),
            "close_vs_mid": (
                None
                if current["close"] is None or current["boll_mid"] is None
                else float(current["close"] - current["boll_mid"])
            ),
            "add_position": addition,
            "option_liquidity": True,
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_AM_BOTTOM_A1",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
            cooldown_override=self._am_bottom_limits.cooldown_s,
        )

    def _detect_am_confluence_buy_a2(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        lookback = max(3, self._am_conf_limits.buy.lookback_n)
        if len(df) < max(lookback + 5, 30):
            return None
        et_time = _to_eastern(event.bar_end)
        if et_time.time() < time(9, 30) or et_time.time() >= time(12, 0):
            return None
        if not self._top5_today(trade_date, session, event.symbol):
            return None
        window = df.iloc[-lookback:]
        current = window.iloc[-1]
        if current["close"] is None or current["boll_mid"] is None or current["close"] < current["boll_mid"]:
            return None
        support_pass = False
        for _, row in window.iterrows():
            if None in (row["close"], row["low"], row["boll_mid"]):
                continue
            mid_touch = row["low"] <= row["boll_mid"] and row["close"] >= row["boll_mid"]
            up_band = row.get("boll_up")
            up_touch = (
                up_band is not None and row["low"] <= up_band and row["close"] >= up_band
            )
            if mid_touch or up_touch:
                support_pass = True
                break
        if not support_pass:
            return None
        macd_line, macd_signal, macd_hist = self._macd_components(df["close"])
        if (
            macd_line is None
            or macd_signal is None
            or macd_hist is None
            or len(macd_line) < 2
        ):
            return None
        if any(
            pd.isna(val)
            for val in (
                macd_line.iloc[-2],
                macd_signal.iloc[-2],
                macd_hist.iloc[-2],
                macd_line.iloc[-1],
                macd_signal.iloc[-1],
                macd_hist.iloc[-1],
            )
        ):
            return None
        if not (
            macd_line.iloc[-2] <= macd_signal.iloc[-2]
            and macd_line.iloc[-1] > macd_signal.iloc[-1]
            and macd_hist.iloc[-2] <= 0
            and macd_hist.iloc[-1] > 0
        ):
            return None
        if not self._series_cross_up(window["rsi6"], Decimal("30")):
            return None
        if not (
            self._series_cross_up(window["stoch_rsi_k"], Decimal("20"))
            or self._series_cross_up(window["stoch_rsi_d"], Decimal("20"))
        ):
            return None
        if (
            current["lr_obv_slope"] is None
            or current["lr_obv_slope"] <= 0
            or current["obv"] is None
            or current["obv_ma6"] is None
            or current["obv"] < current["obv_ma6"]
        ):
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None
        reason = {
            "support_touch": support_pass,
            "macd_cross": True,
            "rsi_cross_30": True,
            "stoch_rsi_cross_20": True,
            "obv_slope": str(current["lr_obv_slope"]),
            "add_position": addition,
            "option_liquidity": True,
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_AM_CONFLUENCE_BUY_A2",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
            cooldown_override=self._am_conf_limits.cooldown_s,
        )

    def _detect_overnight_gap_exit(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Optional[Session],
    ) -> Optional[SignalEnvelope]:
        state = self._positions.get(event.symbol)
        if state is None or state.signal_code not in BUY_SIGNALS:
            return None
        et_time = _to_eastern(event.bar_end)
        if et_time.time() not in (time(9, 30), time(9, 31)):
            return None
        opened_et = _to_eastern(state.opened_at)
        if opened_et.date() >= trade_date:
            return None
        current = df.iloc[-1]
        today_open = None
        today_source = "bar"
        if session is not None:
            today_open = self._daily_open_price(session, event.symbol, trade_date)
            if today_open is not None:
                today_source = "daily"
        if today_open is None:
            open_value = current.get("open")
            if open_value is None:
                return None
            try:
                today_open = Decimal(str(open_value))
            except (InvalidOperation, TypeError):
                return None
        prev_trade_date = opened_et.date()
        prev_close = self._daily_close_price(session, event.symbol, prev_trade_date)
        if prev_close is None:
            # fallback: 从df获取前日16:00的close
            prev_close = self._day_close_price(df, prev_trade_date)
        if prev_close is None or prev_close <= 0:
            return None
        
        # 计算缺口：(今日开盘 - 昨日收盘) / 昨日收盘
        gap = (today_open - prev_close) / prev_close
        
        # 如果高开超过0.5%，不退出（让利润奔跑）
        if gap > Decimal("0.005"):
            return None
        reason = {
            "prev_close": str(prev_close),
            "prev_trade_date": str(prev_trade_date),
            "today_open": str(today_open),
            "today_source": today_source,
            "gap_pct": str(gap),
            "threshold": "0.005",
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_OVERNIGHT_GAP_EXIT",
            side=SignalSide.SELL,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_am_sell_c1(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        state = self._positions.get(event.symbol)
        if state is None:
            return None
        # AM 卖出仅在上午时段有效
        et_time = _to_eastern(event.bar_end)
        if et_time.time() < time(9, 30) or et_time.time() >= time(12, 0):
            return None
        consecutive = max(2, self._am_sell1_limits.consecutive)
        if len(df) < consecutive + 5:
            return None
        tail = df.iloc[-consecutive:]
        median_window = df.iloc[-min(len(df), 20):]
        range_values: List[Decimal] = []
        for _, row in median_window.iterrows():
            if None in (row["high"], row["low"]):
                continue
            try:
                range_values.append(Decimal(str(row["high"] - row["low"])))
            except (InvalidOperation, TypeError):
                continue
        if not range_values:
            return None
        median_range = statistics.median(range_values)
        if median_range <= 0:
            return None

        def _is_big_red(row: pd.Series) -> bool:
            if None in (row["open"], row["close"], row["high"], row["low"]):
                return False
            open_p = Decimal(str(row["open"]))
            close_p = Decimal(str(row["close"]))
            if close_p >= open_p:
                return False
            high_p = Decimal(str(row["high"]))
            low_p = Decimal(str(row["low"]))
            total_range = high_p - low_p
            if total_range <= 0:
                return False
            body_ratio = abs(close_p - open_p) / total_range
            if body_ratio < Decimal(str(self._am_sell1_limits.body_ratio_min)):
                return False
            if total_range < median_range * Decimal(str(self._am_sell1_limits.range_mult_min)):
                return False
            return True

        if not all(_is_big_red(row) for _, row in tail.iterrows()):
            return None
        current = df.iloc[-1]
        if current["boll_mid"] is None:
            return None
        highest = None
        for _, row in tail.iterrows():
            if row["high"] is None:
                return None
            high_val = Decimal(str(row["high"]))
            highest = high_val if highest is None else max(highest, high_val)
        if highest is None or highest >= Decimal(str(current["boll_mid"])):
            return None
        reason = {
            "consecutive": consecutive,
            "median_range": str(median_range),
            "boll_mid": str(current["boll_mid"]),
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_AM_SELL_C1",
            side=SignalSide.SELL,
            reason=reason,
            generated_at=event.bar_end,
            cooldown_override=self._am_conf_limits.cooldown_s,
        )

    def _detect_am_confluence_sell_s2(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        state = self._positions.get(event.symbol)
        if state is None:
            return None
        # AM 卖出仅在上午时段有效
        et_time = _to_eastern(event.bar_end)
        if et_time.time() < time(9, 30) or et_time.time() >= time(12, 0):
            return None
        lookback = max(3, self._am_conf_limits.sell.lookback_n)
        if len(df) < lookback + 1:
            return None
        window = df.iloc[-lookback:]
        current = window.iloc[-1]
        if current["close"] is None or current["boll_mid"] is None or current["close"] >= current["boll_mid"]:
            return None
        pressure_pass = False
        for _, row in window.iterrows():
            if None in (row["close"], row["high"], row["boll_mid"]):
                continue
            failed_mid = row["close"] < row["boll_mid"] and row["high"] >= row["boll_mid"]
            boll_dn = row.get("boll_dn")
            broke_dn = boll_dn is not None and row["close"] < boll_dn
            if failed_mid or broke_dn:
                pressure_pass = True
                break
        if not pressure_pass:
            return None
        if not self._series_cross_down(window["rsi6"], Decimal("70")):
            return None
        if not (
            self._series_cross_down(window["stoch_rsi_k"], Decimal("70"))
            or self._series_cross_down(window["stoch_rsi_d"], Decimal("70"))
        ):
            return None
        if (
            current["obv"] is None
            or current["obv_ma6"] is None
            or current["obv"] >= current["obv_ma6"]
            or current["lr_obv_slope"] is None
            or current["lr_obv_slope"] >= 0
        ):
            return None
        reason = {
            "pressure": pressure_pass,
            "rsi_cross_70": True,
            "stoch_rsi_cross_70": True,
            "obv_below_ma": True,
            "obv_slope": str(current["lr_obv_slope"]),
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_AM_CONFLUENCE_SELL_S2",
            side=SignalSide.SELL,
            reason=reason,
            generated_at=event.bar_end,
            cooldown_override=self._am_conf_limits.cooldown_s,
        )

    def _detect_e1(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        et_time = _to_eastern(event.bar_end)
        if (et_time.hour, et_time.minute) != (9, 31):
            return None
        if (
            vix_value is not None
            and self._vix_gate_mode == "enforce"
            and vix_value >= self._vix_gate
        ):
            try:
                if self._metrics_enabled:
                    record_signal_filtered("VIX_GATE")
            except Exception:
                pass
            return None
        if not self._top5_today(trade_date, session, event.symbol):
            try:
                if self._metrics_enabled:
                    record_signal_filtered("TOP5_FILTER")
            except Exception:
                pass
            return None
        if len(df) < 6:
            return None

        # 盘前涨幅过滤（权威来源）：09:28 价相对昨收 > 0.5%
        # 使用数据库查询 prev_close_rth 与 09:28 的 bars1m_equity
        try:
            # 昨日收盘价（RTH 收盘）
            row_prev = session.execute(
                text(
                    """
                    SELECT prev_close_rth
                    FROM v_daily_ohlcv_enriched
                    WHERE symbol = :symbol AND trade_date_et::date = :trade_date
                    """
                ),
                {"symbol": event.symbol, "trade_date": trade_date},
            ).fetchone()
            # 09:28 分钟的 ts_end（美东时区）
            ts_0928_et = datetime.combine(trade_date, time(9, 28), EASTERN)
            ts_0928_utc = ts_0928_et.astimezone(timezone.utc)
            row_0928 = session.execute(
                text(
                    """
                    SELECT close
                    FROM bars1m_equity
                    WHERE symbol = :symbol AND ts_end = :ts_end
                    """
                ),
                {"symbol": event.symbol, "ts_end": ts_0928_utc},
            ).fetchone()

            if row_prev is None or row_prev[0] is None or row_0928 is None or row_0928[0] is None:
                # 缺少关键数据则不通过过滤
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("PREMARKET_GAP_MISSING")
                except Exception:
                    pass
                return None

            try:
                prev_close = Decimal(str(row_prev[0]))
                price_0928 = Decimal(str(row_0928[0]))
            except Exception:
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("PREMARKET_GAP_PARSE_FAIL")
                except Exception:
                    pass
                return None

            if prev_close <= 0 or price_0928 <= 0:
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("PREMARKET_GAP_INVALID")
                except Exception:
                    pass
                return None

            pre_gap = (price_0928 - prev_close) / prev_close
            if pre_gap < Decimal("0.005"):
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("PREMARKET_GAP_FILTER")
                except Exception:
                    pass
                return None
        except Exception:
            # 防御性：任何异常都视为不通过，避免放行
            try:
                if self._metrics_enabled:
                    record_signal_filtered("PREMARKET_GAP_EXCEPTION")
            except Exception:
                pass
            return None
        
        prev = df.iloc[-2]
        prev2 = df.iloc[-3]
        two_green = bool(
            prev["close"] > prev["open"]
            and prev2["close"] > prev2["open"]
            and prev["close"] > prev2["close"]
        )
        opens_above_mid = bool(
            prev["open"] >= prev["boll_mid"] and prev2["open"] >= prev2["boll_mid"]
        )
        if not (two_green or opens_above_mid):
            return None

        ao_up = bool(prev["ao"] > 0 and prev["ao"] >= prev2["ao"])
        cci_positive = bool(prev["cci14"] > 0 and prev["cci6"] > 0)
        obv_rising = bool(prev["obv"] >= prev["obv_ema20"])
        if sum([ao_up, cci_positive, obv_rising]) < 2:
            return None

        mfi_pass = False
        stoch_pass = False
        if self._mfi_stoch_filter:
            mfi_slope = prev["mfi14"] - df.iloc[-5]["mfi14"]
            mfi_pass = bool(prev["mfi14"] > 50 and mfi_slope > 0)
            stoch_pass = bool(
                prev2["stoch_k"] <= prev2["stoch_d"]
                and prev["stoch_k"] > prev["stoch_d"]
                and prev["stoch_k"] < 80
            )
            if not (mfi_pass or stoch_pass):
                return None

        if 12 <= et_time.hour < 14:
            try:
                if self._metrics_enabled:
                    record_signal_filtered("TIME_WINDOW")
            except Exception:
                pass
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None

        reason = {
            "two_green": two_green,
            "opens_above_mid": opens_above_mid,
            "ao_up": ao_up,
            "cci_positive": cci_positive,
            "obv_rising": obv_rising,
            "mfi_stoch_filter": self._mfi_stoch_filter,
            "mfi_pass": mfi_pass,
            "stoch_pass": stoch_pass,
            "add_position": addition,
            "option_liquidity": True,
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_OPEN_CHASE_BUY",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_e2(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        et_time = _to_eastern(event.bar_end)
        if et_time < et_time.replace(hour=9, minute=32) or et_time > et_time.replace(
            hour=10, minute=30
        ):
            return None
        if not self._top5_today(trade_date, session, event.symbol):
            return None
        if len(df) < 4:
            return None
        current = df.iloc[-1]
        prev = df.iloc[-2]
        prev_close = prev["close"]
        if prev_close <= 0:
            return None
        drop_pct = (prev["open"] - prev["close"]) / prev["open"] if prev["open"] else Decimal("0")
        atr = abs(prev["atr14"])
        price_threshold = Decimal("0.01") if prev_close < 100 else Decimal("0.005")
        big_red = bool(drop_pct >= price_threshold and abs(prev["close"] - prev["open"]) >= atr)
        if not big_red:
            return None

        rvol_pass = bool(current["rvol6"] >= 2)
        obv_cross = bool(prev["obv"] <= prev["obv_ema20"] and current["obv"] > current["obv_ema20"])
        ao_positive = bool(prev["ao"] <= 0 and current["ao"] > 0)
        if sum([rvol_pass, obv_cross, ao_positive]) < 2:
            return None

        scenario_a = False
        scenario_b = False
        bounce_amount = Decimal("0")

        morning_cutoff = et_time.replace(hour=9, minute=33)
        late_start = et_time.replace(hour=10, minute=0)
        late_end = et_time.replace(hour=10, minute=30)

        if et_time <= morning_cutoff:
            rsi_cross = bool(prev["rsi6"] < 30 and current["rsi6"] >= 30)
            scenario_a = rsi_cross
        elif late_start <= et_time <= late_end:
            scenario_b, bounce_amount = self._e2_second_low_rebound(df, trade_date)
        else:
            return None

        if not (scenario_a or scenario_b):
            return None

        if 12 <= et_time.hour < 14:
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None

        mfi_pass = False
        stoch_pass = False
        if self._mfi_stoch_filter:
            mfi_slope = current["mfi14"] - df.iloc[-4]["mfi14"]
            mfi_pass = bool(current["mfi14"] > 50 and mfi_slope > 0)
            stoch_pass = bool(
                prev["stoch_k"] <= prev["stoch_d"]
                and current["stoch_k"] > current["stoch_d"]
                and current["stoch_k"] < 80
            )
            if not (mfi_pass or stoch_pass):
                return None

        reason = {
            "big_red": big_red,
            "rsi_cross_30": scenario_a,
            "second_lower_low": scenario_b,
            "bounce": str(bounce_amount) if scenario_b else None,
            "rvol_pass": rvol_pass,
            "obv_cross": obv_cross,
            "ao_positive": ao_positive,
            "mfi_stoch_filter": self._mfi_stoch_filter,
            "mfi_pass": mfi_pass,
            "stoch_pass": stoch_pass,
            "add_position": addition,
            "option_liquidity": True,
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_REBOUND_BUY",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_pm_a2(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        et_time = _to_eastern(event.bar_end)
        if et_time < et_time.replace(hour=14, minute=0):
            return None
        if not self._a1_gate(df, trade_date, session, event.symbol):
            return None

        window = df.iloc[-12:]
        local_highs = [
            idx
            for idx in range(1, len(window) - 1)
            if window.iloc[idx]["close"] > window.iloc[idx - 1]["close"]
            and window.iloc[idx]["close"] > window.iloc[idx + 1]["close"]
        ]
        if len(local_highs) < 2:
            return None

        atr = abs(df.iloc[-1]["atr14"])
        recent_low = window["low"].min()
        bounce = df.iloc[-1]["close"] - recent_low
        if bounce > atr * Decimal("0.5"):
            return None

        rsi_cross = bool(df.iloc[-2]["rsi6"] < 40 and df.iloc[-1]["rsi6"] >= 40)
        ao_cross = bool(df.iloc[-2]["ao"] <= 0 and df.iloc[-1]["ao"] > 0)
        cci_positive = bool(df.iloc[-1]["cci14"] > 0)
        if sum([rsi_cross, ao_cross, cci_positive]) < 2:
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None

        reason = {
            "gate_pass": True,
            "local_highs": len(local_highs),
            "bounce": str(bounce),
            "atr": str(atr),
            "rsi_cross_40": rsi_cross,
            "ao_positive": ao_cross,
            "cci_positive": cci_positive,
            "add_position": addition,
            "option_liquidity": True,
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_PM_BOTTOM_A2",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_pm_a3(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        et_time = _to_eastern(event.bar_end)
        if et_time < et_time.replace(hour=14, minute=0):
            return None
        if len(df) < 10:
            return None
        if not self._a1_gate(df, trade_date, session, event.symbol):
            return None

        prev = df.iloc[-2]
        current = df.iloc[-1]
        boll_slope = current["boll_dn"] - df.iloc[-6]["boll_dn"]
        if boll_slope < 0:
            return None
        dipped = bool(prev["close"] < prev["boll_dn"])
        strong_reversal = bool(
            (current["close"] - current["open"]) >= abs(current["atr14"]) * Decimal("0.5")
            and current["close"] > current["boll_dn"]
        )
        if not (dipped and strong_reversal):
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None

        reason = {
            "gate_pass": True,
            "boll_dn_slope": str(boll_slope),
            "dipped": dipped,
            "strong_reversal": strong_reversal,
            "add_position": addition,
            "option_liquidity": True,
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_PM_BOTTOM_A3",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_pm_a4(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        et_time = _to_eastern(event.bar_end)
        if et_time < et_time.replace(hour=14, minute=0):
            return None
        if not self._a1_gate(df, trade_date, session, event.symbol):
            return None

        recent = df[df["ts_end"] >= df.iloc[-1]["ts_end"] - pd.Timedelta(minutes=90)]
        peaks: List[int] = []
        rvol_series = recent["rvol6"].tolist()
        for idx in range(1, len(rvol_series) - 1):
            if (
                rvol_series[idx] >= 3
                and rvol_series[idx] >= rvol_series[idx - 1]
                and rvol_series[idx] >= rvol_series[idx + 1]
            ):
                peaks.append(idx)
        if len(peaks) < 2:
            return None
        if (peaks[-1] - peaks[-2]) < 5:
            return None

        between = recent.iloc[peaks[-2] : peaks[-1] + 1]
        valley_low = between["low"].min()
        rebound = df.iloc[-1]["close"] - valley_low
        if rebound > abs(df.iloc[-1]["atr14"]) * Decimal("0.5"):
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None

        reason = {
            "gate_pass": True,
            "peaks": len(peaks),
            "rebound": str(rebound),
            "add_position": addition,
            "option_liquidity": True,
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_PM_BOTTOM_A4",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_s2(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        """
        S2: 两根回踩上轨 - 严格三根形态判断
        
        核心逻辑：先在上轨之上，然后连续两根回到上轨内
        - t-2: close > boll_up  (先在上轨之上)
        - t-1: close < boll_up  (第一根回到上轨内)
        - t:   close < boll_up  (第二根仍在上轨内)
        
        修复说明：
        之前只检查"连续两根 < 上轨"，导致价格一直在上轨下方时也会触发。
        现在要求"从上轨上方回落"，才算真正的"回踩"。
        """
        state = self._positions.get(event.symbol)
        if state is None or state.signal_code != "SIG_OPEN_CHASE_BUY":
            return None
        if len(df) < 3:
            return None
        
        # 获取最近三根K线
        prev2 = df.iloc[-3]  # t-2: 应该在上轨之上
        prev1 = df.iloc[-2]  # t-1: 第一根回踩
        curr = df.iloc[-1]   # t:   第二根回踩
        
        # 容差：避免浮点误差
        from decimal import Decimal
        eps = Decimal("1e-6")
        
        # 严格三根形态：上 → 下 → 下
        was_above = bool(prev2["close"] > prev2["boll_up"] + eps)  # 先在上轨之上
        tap_1 = bool(prev1["close"] < prev1["boll_up"] - eps)      # 第一根回到上轨内
        tap_2 = bool(curr["close"] < curr["boll_up"] - eps)        # 第二根仍在上轨内
        
        two_taps_after_above = was_above and tap_1 and tap_2
        
        if not two_taps_after_above:
            return None
        
        reason = {
            "two_upper_tap": True,
            "was_above_upper": was_above,
            "prev2_close": float(prev2["close"]),
            "prev2_upper": float(prev2["boll_up"]),
            "prev1_close": float(prev1["close"]),
            "prev1_upper": float(prev1["boll_up"]),
            "curr_close": float(curr["close"]),
            "curr_upper": float(curr["boll_up"]),
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_EXIT_UPPER_TAP_X2",
            side=SignalSide.SELL,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_s1(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        if len(df) < 3:
            return None
        prev = df.iloc[-2]
        current = df.iloc[-1]
        # Check for valid decimal values before comparison
        if (
            pd.notna(prev["low"])
            and pd.notna(prev["boll_up"])
            and prev["low"] > prev["boll_up"]
        ):
            self._upper_break[event.symbol] = prev["ts_end"]
        if event.symbol not in self._upper_break:
            return None
        # Check for valid decimal values before comparison
        if (
            pd.isna(current["low"])
            or pd.isna(current["boll_mid"])
            or pd.isna(current["high"])
            or pd.isna(current["close"])
        ):
            return None
        touches_mid = bool(
            current["low"] <= current["boll_mid"] <= current["high"]
            and current["close"] >= current["boll_mid"]
        )
        if not touches_mid:
            return None
        reason = {
            "upper_break_ts": str(self._upper_break[event.symbol]),
            "touch_mid": True,
        }
        self._upper_break.pop(event.symbol, None)
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_EXIT_BOX2MID",
            side=SignalSide.SELL,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_time_clear(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        state = self._positions.get(event.symbol)
        if state is None:
            return None
        et_time = _to_eastern(event.bar_end)
        if (
            state.signal_code == "SIG_OPEN_CHASE_BUY"
            and et_time.time() >= time(14, 0)
        ):
            return self._build_signal(
                symbol=event.symbol,
                signal_code="SIG_TIME_CLEAR_12_14",
                side=SignalSide.SELL,
                reason={"clear_reason": "post_14_00_cleanup"},
                generated_at=event.bar_end,
            )
        if (
            state.opened_at.date() < et_time.date()
            and et_time.time() >= time(12, 0)
        ):
            return self._build_signal(
                symbol=event.symbol,
                signal_code="SIG_TIME_CLEAR_12_14",
                side=SignalSide.SELL,
                reason={"clear_reason": "overnight_exit"},
                generated_at=event.bar_end,
            )
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _evaluate_event(
        self,
        session: Session,
        event: BarsClosed,
        *,
        persist: bool,
        publish: bool,
        mutate_state: bool,
        record_metric: bool,
    ) -> List[DetectedSignal]:
        df = self._load_history(session, event.symbol, event.bar_end, limit=self._history_window)
        if df is None or len(df) < 5:
            return []

        current_et = _to_eastern(event.bar_end)
        trade_date = current_et.date()
        vix_value = self._current_vix(session, event.bar_end)

        detectors = [
            self._detect_overnight_gap_exit,
            self._detect_am_sell_c1,
            self._detect_am_confluence_sell_s2,
            self._detect_s2,
            self._detect_s1,
            self._detect_time_clear,
            self._detect_e1,
            self._detect_e2,
            self._detect_pm_a2,
            self._detect_pm_a3,
            self._detect_pm_a4,
            self._detect_am_bottom_a1,
            self._detect_am_confluence_buy_a2,
        ]

        detected: List[DetectedSignal] = []
        for detector in detectors:
            signal = detector(event, df, trade_date, vix_value, session)
            if signal is None:
                continue
            if not self._should_emit(signal, event.bar_end):
                continue
            trace_id = event.trace_id or str(uuid4())
            entry = DetectedSignal(signal=signal, trace_id=trace_id, ts_end=event.bar_end)
            self._finalise_signal(
                session,
                entry,
                persist=persist,
                publish=publish,
                mutate_state=mutate_state,
                record_metric=record_metric,
            )
            detected.append(entry)
        return detected

    def _finalise_signal(
        self,
        session: Session,
        detected: DetectedSignal,
        *,
        persist: bool,
        publish: bool,
        mutate_state: bool,
        record_metric: bool,
    ) -> None:
        signal = detected.signal
        ts_end = detected.ts_end
        if persist:
            self._persist_signal(session, signal, ts_end)
        if mutate_state:
            self._register_state(signal, ts_end)
        if publish:
            self._publish_signal(signal, detected.trace_id, ts_end)
        if record_metric:
            if self._metrics_enabled:
                try:
                    record_signal_emitted(signal.signal_code)
                except Exception:
                    pass

    def _publish_signal(self, signal: SignalEnvelope, trace_id: str, ts_end: datetime) -> None:
        if self._redis_bus is None:
            return
        payload = signal.model_dump()
        payload["trace_id"] = trace_id
        payload["bar_end"] = ts_end.isoformat()
        payload["strategy_code"] = self._strategy_code
        try:
            coro = self._redis_bus.publish("signals", payload, trace_id=trace_id)
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(coro)
            except RuntimeError:
                asyncio.run(coro)
        except Exception:  # pragma: no cover - external dependency
            LOGGER.exception(
                "signal_engine.publish_failed", symbol=signal.symbol, signal_code=signal.signal_code
            )

    def _snapshot_state(self) -> Dict[str, Any]:
        return {
            "positions": copy.deepcopy(self._positions),
            "upper_break": copy.deepcopy(self._upper_break),
            "last_signal_ts": copy.deepcopy(self._last_signal_ts),
        }

    def _restore_state(self, snapshot: Dict[str, Any]) -> None:
        self._positions = snapshot["positions"]
        self._upper_break = snapshot["upper_break"]
        self._last_signal_ts = snapshot["last_signal_ts"]

    def _daily_open_price(
        self,
        session: Optional[Session],
        symbol: str,
        trade_date: date,
    ) -> Optional[Decimal]:
        if session is None:
            return None
        row = session.execute(
            text(
                """
                SELECT open_rth
                FROM v_daily_ohlcv_enriched
                WHERE symbol = :symbol AND trade_date_et::date = :trade_date
                """
            ),
            {"symbol": symbol, "trade_date": trade_date},
        ).fetchone()
        if not row:
            return None
        open_rth = row[0]
        if open_rth is None:
            return None
        try:
            return Decimal(str(open_rth))
        except (InvalidOperation, TypeError):
            return None

    def _day_open_price(self, df: pd.DataFrame, trade_date: date) -> Optional[Decimal]:
        if df is None or df.empty:
            return None
        if "et" in df.columns:
            et_series = df["et"]
        else:
            et_series = df["ts_end"].dt.tz_convert("US/Eastern")
        mask = et_series.dt.date == trade_date
        if not mask.any():
            return None
        todays = df.loc[mask]
        if todays.empty:
            return None
        if "et" in todays.columns:
            time_series = todays["et"].dt.time
        else:
            time_series = todays["ts_end"].dt.tz_convert("US/Eastern").dt.time
        open_rows = todays.loc[time_series == time(9, 31)]
        if not open_rows.empty:
            open_value = open_rows.iloc[0].get("open")
        else:
            open_value = todays.iloc[0].get("open")
        if open_value is None:
            return None
        try:
            return Decimal(str(open_value))
        except (InvalidOperation, TypeError):
            return None

    def _should_emit(self, signal: SignalEnvelope, ts_end: datetime) -> bool:
        key = (signal.symbol, signal.signal_code)
        last_ts = self._last_signal_ts.get(key)
        if last_ts is None:
            return True
        return (ts_end - last_ts).total_seconds() >= signal.cooldown_seconds

    def _daily_ma60_value(
        self, session: Session, symbol: str, ts_end: datetime
    ) -> Optional[Decimal]:
        trade_date = ts_end.astimezone(EASTERN).date()
        key = (symbol.upper(), trade_date)
        if key in self._ma60_cache:
            return self._ma60_cache[key]
        try:
            row = session.execute(
                text(
                    """
                    SELECT sma60
                    FROM v_daily_ma60
                    WHERE symbol = :symbol AND trade_date_et::date = :trade_date
                    """
                ),
                {"symbol": symbol, "trade_date": trade_date},
            ).fetchone()
        except Exception:
            LOGGER.exception(
                "signal_engine.ma60_query_failed", symbol=symbol, trade_date=str(trade_date)
            )
            self._ma60_cache[key] = None
            return None
        if row is None:
            LOGGER.warning(
                "signal_engine.ma60_missing",
                symbol=symbol,
                trade_date=str(trade_date),
            )
            self._ma60_cache[key] = None
            return None
        ma60_raw = row[0]
        if ma60_raw is None:
            LOGGER.warning(
                "signal_engine.ma60_null",
                symbol=symbol,
                trade_date=str(trade_date),
            )
            self._ma60_cache[key] = None
            return None
        try:
            ma60_value = Decimal(str(ma60_raw))
        except (InvalidOperation, TypeError, ValueError):
            LOGGER.warning(
                "signal_engine.ma60_parse_failed",
                symbol=symbol,
                trade_date=str(trade_date),
                raw=str(ma60_raw),
            )
            self._ma60_cache[key] = None
            return None
        self._ma60_cache[key] = ma60_value
        return ma60_value

    def _can_open_position(
        self,
        symbol: str,
        df: Optional[pd.DataFrame] = None,
        session: Optional[Session] = None,
        ts_end: Optional[datetime] = None,
    ) -> Tuple[bool, bool]:
        if session is not None and ts_end is not None:
            trade_date = ts_end.astimezone(EASTERN).date()
            try:
                preearn_allowed, _ = evaluate_preearn_guard(
                    session,
                    symbol=symbol,
                    trade_date=trade_date,
                    days_min=self._preearn_days_min,
                    days_max=self._preearn_days_max,
                    atr_pct_max=self._preearn_atr_pct_max,
                )
            except Exception:  # pragma: no cover - defensive
                LOGGER.exception(
                    "signal_engine.preearn_guard_failed",
                    symbol=symbol,
                    trade_date=str(trade_date),
                )
                preearn_allowed = True
            if not preearn_allowed:
                return False, False

        if session is not None and ts_end is not None and df is not None and not df.empty:
            current_close_raw = df.iloc[-1].get("close")
            current_close: Optional[Decimal]
            try:
                if current_close_raw is None:
                    current_close = None
                elif isinstance(current_close_raw, Decimal):
                    current_close = current_close_raw
                else:
                    current_close = Decimal(str(current_close_raw))
            except (InvalidOperation, TypeError, ValueError):
                current_close = None
            if current_close is None:
                LOGGER.debug(
                    "signal_engine.ma60_no_close",
                    symbol=symbol,
                    ts_end=str(ts_end),
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("MA60_FILTER")
                except Exception:
                    pass
                return False, False
            ma60_value = self._daily_ma60_value(session, symbol, ts_end)
            if ma60_value is None or current_close <= ma60_value:
                LOGGER.debug(
                    "signal_engine.ma60_blocked",
                    symbol=symbol,
                    ts_end=str(ts_end),
                    close=str(current_close),
                    sma60=str(ma60_value) if ma60_value is not None else None,
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("MA60_FILTER")
                except Exception:
                    pass
                return False, False

        state = self._positions.get(symbol)
        if state is None:
            if len(self._positions) >= int(self._settings.max_concurrent_top):
                return False, False
            if session is not None and ts_end is not None:
                if not self._option_liquidity_pass(session, symbol, ts_end):
                    return False, False
            return True, False

        if state.additions >= 1:
            return False, False
        if df is None or session is None or ts_end is None or len(df) < 2:
            return False, False

        current = df.iloc[-1]
        prev = df.iloc[-2]

        ao_curr = _to_decimal(current["ao"])
        ao_prev = _to_decimal(prev["ao"])
        obv_curr = _to_decimal(current["obv"])
        obv_prev = _to_decimal(prev["obv"])
        rvol_curr = _to_decimal(current["rvol6"])

        ao_up = ao_curr > ao_prev and ao_curr > Decimal("0")
        obv_rising = obv_curr > obv_prev
        rvol_high = rvol_curr >= Decimal("2")

        if not (ao_up and obv_rising and rvol_high):
            return False, False
        if not self._option_liquidity_pass(session, symbol, ts_end):
            return False, False
        return True, True

    def _option_liquidity_pass(self, session: Session, symbol: str, ts_end: datetime) -> bool:
        # Temporarily disabled due to missing underlying_price field in bars1m_option
        return True
        window_start = ts_end - timedelta(minutes=15)
        rows = session.execute(
            text(
                """
                SELECT
                    bid,
                    ask,
                    volume,
                    open_interest,
                    expiry,
                    strike,
                    ts_end
                FROM bars1m_option
                WHERE symbol = :symbol
                  AND "right" = 'CALL'
                  AND ts_end BETWEEN :start_ts AND :end_ts
                ORDER BY ts_end DESC, volume DESC NULLS LAST
                LIMIT 100
                """
            ),
            {"symbol": symbol, "start_ts": window_start, "end_ts": ts_end},
        ).all()
        if not rows:
            return False

        trade_date = ts_end.astimezone(EASTERN).date()
        for row in rows:
            bid = row.bid
            ask = row.ask
            volume = row.volume
            oi = row.open_interest
            expiry = row.expiry
            und_price = row.underlying_price
            if (
                bid is None
                or ask is None
                or volume is None
                or oi is None
                or expiry is None
                or und_price is None
            ):
                continue
            bid_dec = Decimal(str(bid))
            ask_dec = Decimal(str(ask))
            if bid_dec <= 0 or ask_dec <= 0 or ask_dec <= bid_dec:
                continue
            if int(volume) < 100 or int(oi) < 500:
                continue
            und_dec = Decimal(str(und_price))
            if und_dec <= 0:
                continue
            if isinstance(expiry, datetime):
                expiry_date = expiry.date()
            elif isinstance(expiry, str):
                try:
                    expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
                except ValueError:
                    continue
            else:
                expiry_date = expiry
            if not isinstance(expiry_date, date):
                continue
            dte = (expiry_date - trade_date).days
            if dte < 2 or dte > 7:
                continue
            mid = (bid_dec + ask_dec) / Decimal(2)
            spread = ask_dec - bid_dec
            threshold = max(Decimal("0.10"), mid * Decimal("0.05"))
            if spread > threshold:
                continue
            return True
        return False

    def _register_state(self, signal: SignalEnvelope, ts_end: datetime) -> None:
        key = (signal.symbol, signal.signal_code)
        self._last_signal_ts[key] = ts_end
        if signal.signal_code in BUY_SIGNALS:
            state = self._positions.get(signal.symbol)
            if state is None:
                self._positions[signal.symbol] = PositionState(
                    signal_code=signal.signal_code,
                    opened_at=ts_end,
                    additions=0,
                )
            else:
                state.signal_code = signal.signal_code
                state.opened_at = ts_end
                state.additions = min(state.additions + 1, 1)
        elif signal.signal_code in SELL_SIGNALS:
            self._positions.pop(signal.symbol, None)
            self._upper_break.pop(signal.symbol, None)

    def _persist_signal(self, session: Session, signal: SignalEnvelope, ts_end: datetime) -> None:
        log_dao = SignalLogDAO(session)
        log_dao.record_signal(
            run_id=self._run_id,
            symbol=signal.symbol,
            signal_code=signal.signal_code,
            ts_end=ts_end,
            accepted=True,
            reason=None,
        )

    def _build_signal(
        self,
        *,
        symbol: str,
        signal_code: str,
        side: SignalSide,
        reason: Dict[str, Any],
        generated_at: datetime,
        ttl_override: Optional[int] = None,
        cooldown_override: Optional[int] = None,
    ) -> SignalEnvelope:
        ttl_value = int(ttl_override) if ttl_override is not None else self._ttl
        cooldown_value = int(cooldown_override) if cooldown_override is not None else self._cooldown
        return SignalEnvelope(
            strategy_code=self._strategy_code,
            symbol=symbol,
            signal_code=signal_code,
            side=side,
            confidence=1.0,
            reason=reason,
            risk_hint=RISK_HINT,
            option_hint=OPTION_HINT,
            ttl_seconds=ttl_value,
            cooldown_seconds=cooldown_value,
            generated_at=generated_at,
        )

    def _find_pivot_lows(self, df: pd.DataFrame, window: int) -> List[int]:
        if window <= 0 or len(df) < window * 2 + 1:
            return []
        pivots: List[int] = []
        for idx in range(window, len(df) - window):
            low_value = df.iloc[idx]["low"]
            if low_value is None:
                continue
            neighbourhood = [
                row_low
                for row_low in df["low"].iloc[idx - window : idx + window + 1]
                if row_low is not None
            ]
            if not neighbourhood:
                continue
            if low_value > min(neighbourhood):
                continue
            prev_idx = max(idx - 1, 0)
            m5_prev = df.iloc[prev_idx]["lr_m5_slope"]
            dn_prev = df.iloc[prev_idx]["lr_boll_dn_slope"]
            m5_curr = df.iloc[idx]["lr_m5_slope"]
            dn_curr = df.iloc[idx]["lr_boll_dn_slope"]
            if None in (m5_prev, dn_prev, m5_curr, dn_curr):
                continue
            # 检查是否为有效数字（排除NaN、Infinity等）
            try:
                if isinstance(m5_curr, Decimal) and not m5_curr.is_finite():
                    continue
                if isinstance(dn_curr, Decimal) and not dn_curr.is_finite():
                    continue
                if isinstance(m5_prev, Decimal) and not m5_prev.is_finite():
                    continue
                if isinstance(dn_prev, Decimal) and not dn_prev.is_finite():
                    continue
                if m5_curr > 0 and dn_curr > 0 and (m5_prev < 0 or dn_prev < 0):
                    pivots.append(idx)
            except (InvalidOperation, ValueError):
                continue
        return pivots

    def _check_slope_sequences(
        self,
        df: pd.DataFrame,
        idx: int,
        neg_len: int,
        pos_len: int,
    ) -> bool:
        if neg_len <= 0 or idx < neg_len or idx >= len(df):
            return False
        neg_slice = df.iloc[idx - neg_len : idx]
        if len(neg_slice) < neg_len:
            return False
        for _, row in neg_slice.iterrows():
            if row["lr_m5_slope"] is None or row["lr_boll_dn_slope"] is None:
                return False
            if row["lr_m5_slope"] > 0 or row["lr_boll_dn_slope"] > 0:
                return False
        pos_required = max(pos_len, 2)
        pos_slice = df.iloc[idx : min(len(df), idx + pos_required)]
        if len(pos_slice) < 2:
            return False
        for _, row in pos_slice.iterrows():
            if row["lr_m5_slope"] is None or row["lr_boll_dn_slope"] is None:
                return False
            if row["lr_m5_slope"] < 0 or row["lr_boll_dn_slope"] < 0:
                return False
        return True

    def _macd_components(
        self, close_series: pd.Series
    ) -> Tuple[Optional[pd.Series], Optional[pd.Series], Optional[pd.Series]]:
        if close_series is None or len(close_series) < 26:
            return None, None, None
        values = []
        for val in close_series:
            if val is None:
                values.append(float("nan"))
            else:
                try:
                    values.append(float(val))
                except (TypeError, ValueError):
                    values.append(float("nan"))
        close_float = pd.Series(values, index=close_series.index, dtype="float64")
        if close_float.isna().all():
            return None, None, None
        ema12 = close_float.ewm(span=12, adjust=False).mean()
        ema26 = close_float.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - macd_signal
        return macd_line, macd_signal, macd_hist

    def _series_cross_up(self, series: pd.Series, threshold: Decimal) -> bool:
        if series is None or len(series) < 2:
            return False
        prev_value: Optional[Decimal] = None
        for val in series:
            if val is None:
                prev_value = None
                continue
            try:
                current = Decimal(str(val))
            except (InvalidOperation, TypeError):
                prev_value = None
                continue
            if prev_value is not None and prev_value < threshold and current >= threshold:
                return True
            prev_value = current
        return False

    def _series_cross_down(self, series: pd.Series, threshold: Decimal) -> bool:
        if series is None or len(series) < 2:
            return False
        prev_value: Optional[Decimal] = None
        for val in series:
            if val is None:
                prev_value = None
                continue
            try:
                current = Decimal(str(val))
            except (InvalidOperation, TypeError):
                prev_value = None
                continue
            if prev_value is not None and prev_value > threshold and current <= threshold:
                return True
            prev_value = current
        return False

    def _e2_second_low_rebound(self, df: pd.DataFrame, trade_date: date) -> Tuple[bool, Decimal]:
        todays = df[df["et"].dt.date == trade_date]
        if len(todays) < 4:
            return False, Decimal("0")
        current = todays.iloc[-1]
        prev = todays.iloc[-2]
        earlier = todays.iloc[:-2]
        if earlier.empty:
            return False, Decimal("0")
        prev_low = prev["low"]
        earlier_low = earlier["low"].min()
        if prev_low >= earlier_low:
            return False, Decimal("0")
        bounce = current["close"] - prev_low
        atr = abs(current["atr14"])
        if bounce <= 0 or atr <= 0:
            return False, Decimal("0")
        if bounce > atr * Decimal("0.5"):
            return False, Decimal("0")
        return True, bounce

    def _load_history(
        self, session: Session, symbol: str, ts_end: datetime, *, limit: int
    ) -> Optional[pd.DataFrame]:
        query = text(
            """
            SELECT
                b.ts_end,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                i.boll_mid,
                i.boll_up,
                i.boll_dn,
                i.rsi6,
                i.rsi12,
                i.rsi24,
                i.atr14,
                i.ao,
                i.stoch_k,
                i.stoch_d,
                i.stoch_rsi_k,
                i.stoch_rsi_d,
                i.cci14,
                i.cci6,
                i.sma5,
                i.lr_m5_slope,
                i.lr_boll_dn_slope,
                i.lr_obv_slope,
                i.obv,
                i.obv_ma6,
                i.obv_ema20,
                i.mfi14,
                i.rvol6
            FROM bars1m_equity b
            LEFT JOIN indicators_eq_1m i
                ON b.symbol = i.symbol AND b.ts_end = i.ts_end
            WHERE b.symbol = :symbol AND b.ts_end <= :ts_end
            ORDER BY b.ts_end DESC
            LIMIT :limit
            """
        )
        rows = session.execute(
            query,
            {"symbol": symbol, "ts_end": ts_end, "limit": limit},
        ).all()
        if not rows:
            return None
        columns = [
            "ts_end",
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
            "sma5",
            "lr_m5_slope",
            "lr_boll_dn_slope",
            "lr_obv_slope",
            "obv",
            "obv_ma6",
            "obv_ema20",
            "mfi14",
            "rvol6",
        ]
        df = pd.DataFrame(rows, columns=columns)
        df = df.sort_values("ts_end")
        for col in columns[1:]:
            df[col] = df[col].apply(_to_decimal)
        df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)
        df["et"] = df["ts_end"].dt.tz_convert("US/Eastern")
        return df

    def _a1_gate(self, df: pd.DataFrame, trade_date: date, session: Session, symbol: str) -> bool:
        current = df.iloc[-1]
        et = current["et"]
        if et.hour < 14:
            return False
        todays = df[df["et"].dt.date == trade_date]
        if todays.empty:
            return False
        open_today = todays.iloc[0]["open"]
        if current["close"] >= open_today:
            return False
        if not self._top5_recent(trade_date, session, symbol):
            return False
        if self._is_high_open(session, symbol, trade_date):
            ts_end = (
                current["ts_end"].to_pydatetime()
                if hasattr(current["ts_end"], "to_pydatetime")
                else current["ts_end"]
            )
            if self._option_price_exceeds_open(session, symbol, trade_date, ts_end):
                return False
        return True

    def _top5_recent(
        self,
        trade_date: date,
        session: Session,
        symbol: str,
    ) -> bool:
        cache = self._top5_recent_cache.get(trade_date)
        if cache is None:
            start = trade_date - timedelta(days=5)
            cache = self._top5_source.symbols_between(session, start, trade_date)
            self._top5_recent_cache[trade_date] = cache
        return symbol.upper() in cache

    def _top5_today(
        self,
        trade_date: date,
        session: Session,
        symbol: str,
    ) -> bool:
        cache = self._top5_today_cache.get(trade_date)
        if cache is None:
            cache = self._top5_source.symbols_for_date(session, trade_date)
            self._top5_today_cache[trade_date] = cache
        return symbol.upper() in cache

    def _is_high_open(self, session: Session, symbol: str, trade_date: date) -> bool:
        row = session.execute(
            text(
                """
                SELECT open_rth, prev_close_rth
                FROM v_daily_ohlcv_enriched
                WHERE symbol = :symbol AND trade_date_et::date = :trade_date
                """
            ),
            {"symbol": symbol, "trade_date": trade_date},
        ).fetchone()
        if not row:
            return False
        open_rth, prev_close_rth = row
        if open_rth is None or prev_close_rth is None:
            return False
        try:
            open_rth_dec = Decimal(str(open_rth))
            prev_close_dec = Decimal(str(prev_close_rth))
        except Exception:
            return False
        return open_rth_dec > prev_close_dec

    def _option_price_exceeds_open(
        self,
        session: Session,
        symbol: str,
        trade_date: date,
        ts_end: datetime,
    ) -> bool:
        # Temporarily disabled due to missing option data fields
        return False
        start_et = datetime.combine(trade_date, time(9, 30), EASTERN)
        start_utc = start_et.astimezone(timezone.utc)
        window_end = start_utc + timedelta(minutes=5)
        row = session.execute(
            text(
                """
                SELECT conid, mid
                FROM bars1m_option
                WHERE symbol = :symbol
                  AND "right" = 'CALL'
                  AND ts_end >= :start_ts
                  AND ts_end < :end_ts
                ORDER BY ts_end ASC, volume DESC NULLS LAST
                LIMIT 1
                """
            ),
            {"symbol": symbol, "start_ts": start_utc, "end_ts": window_end},
        ).fetchone()
        if not row:
            return False
        conid, open_last = row
        if conid is None or open_last is None:
            return False
        try:
            open_price = Decimal(str(open_last))
        except Exception:
            return False

        ts_query = ts_end
        if ts_query.tzinfo is None:
            ts_query = ts_query.replace(tzinfo=timezone.utc)
        row_latest = session.execute(
            text(
                """
                SELECT mid
                FROM bars1m_option
                WHERE conid = :conid
                  AND ts_end <= :ts_end
                ORDER BY ts_end DESC
                LIMIT 1
                """
            ),
            {"conid": conid, "ts_end": ts_query},
        ).fetchone()
        if not row_latest:
            return False
        latest_last = row_latest[0]
        if latest_last is None:
            return False
        try:
            latest_price = Decimal(str(latest_last))
        except Exception:
            return False
        return latest_price > open_price

    def _current_vix(self, session: Session, ts_end: datetime) -> Optional[Decimal]:
        cached_value, cached_ts = self._vix_cache
        if (
            cached_value is not None
            and cached_ts is not None
            and (ts_end - cached_ts).total_seconds() < 60
        ):
            return cached_value
        dao = RiskStateDAO(session)
        record = dao.latest_for_symbol("VIX", "VIX")
        if record is None:
            return None
        self._vix_cache = (record.metric_value, record.ts)
        return record.metric_value


def _to_eastern(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=EASTERN)
    return ts.astimezone(EASTERN)


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")
