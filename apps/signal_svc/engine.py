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
from sqlalchemy import inspect as sa_inspect, text
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
from libs.schemas.signals import (
    BUY_SIGNAL_CODES,
    SELL_SIGNAL_CODES,
    SignalEnvelope,
    SignalPushItem,
    SignalSide,
)

from .top5_source import PremarketTop5Source, Top5Source
from apps.risk_svc.limits import LimitsCache

LOGGER = structlog.get_logger(__name__)

OPTION_QUOTE_MAX_AGE_SECONDS = 60

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

BUY_SIGNALS = BUY_SIGNAL_CODES
SELL_SIGNALS = SELL_SIGNAL_CODES


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
        is_backtest: bool = False,
        allow_missing_option_liquidity_metrics: bool = False,
        strategy_variant: str | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._redis_bus = redis_bus
        self._strategy_code = strategy_code
        self._history_window = max(60, history_window)
        self._settings = get_settings()
        self._strategy_variant = str(
            strategy_variant or self._settings.backtest_strategy_variant or "v1"
        ).lower()
        self._orb_entry_enabled = self._strategy_variant in {
            "v3",
            "v6",
            "v7",
            "orb-entry",
            "combined-v2",
            "conservative-execution",
        }
        self._delayed_entry_enabled = self._strategy_variant in {
            "v2",
            "v3",
            "v6",
            "v7",
            "delayed-entry",
            "orb-entry",
            "combined-v2",
            "conservative-execution",
        }
        self._separated_exit_enabled = self._strategy_variant in {
            "v5",
            "v6",
            "v7",
            "separated-exit-policy",
            "combined-v2",
            "conservative-execution",
        }
        self._reversal_1030_enabled = self._strategy_variant in {
            "v8",
            "v8_1030_reversal",
            "1030-reversal",
            "reversal-1030",
            "v8_guard_filter",
            "v8-guard-filter",
            "v8_guard_filter_followthrough",
            "v8-guard-filter-followthrough",
            "v8_guard_rsi_floor",
            "v8-guard-rsi-floor",
            "v8_guard_rsi_floor_core_room_025",
            "v8-guard-rsi-floor-core-room-025",
            "v8_guard_rsi_floor_core_room_035",
            "v8-guard-rsi-floor-core-room-035",
            "v8_guard_continuation",
            "v8-guard-continuation",
            "v8_guard_continuation_v2",
            "v8-guard-continuation-v2",
            "v8_guard_scalp_observe",
            "v8-guard-scalp-observe",
            "v8_guard_stop",
            "v8-guard-stop",
            "v8a",
            "v8a_low_reversal",
            "v8a-low-reversal",
            "v8b",
            "v8b_reclaim",
            "v8b-reclaim",
            "v8b2",
            "v8b2_mid_reclaim_quality",
            "v8b2-mid-reclaim-quality",
            "v8b2_loose",
            "v8b2-mid-reclaim-quality-loose",
            "v8b2_mid_reclaim_quality_loose",
            "v8b2_room040",
            "v8b2-mid-reclaim-quality-room040",
            "v8b2_mid_reclaim_quality_room040",
            "v8b2_strict",
            "v8b2-mid-reclaim-quality-strict",
            "v8b2_mid_reclaim_quality_strict",
            "v8b3",
            "v8b3_mid_reclaim_followthrough",
            "v8b3-mid-reclaim-followthrough",
            "v8_ab",
            "v8ab",
            "v8-a-b",
        }
        self._reversal_1030_legacy_enabled = self._strategy_variant in {
            "v8",
            "v8_1030_reversal",
            "1030-reversal",
            "reversal-1030",
            "v8_guard_filter",
            "v8-guard-filter",
            "v8_guard_filter_followthrough",
            "v8-guard-filter-followthrough",
            "v8_guard_rsi_floor",
            "v8-guard-rsi-floor",
            "v8_guard_rsi_floor_core_room_025",
            "v8-guard-rsi-floor-core-room-025",
            "v8_guard_rsi_floor_core_room_035",
            "v8-guard-rsi-floor-core-room-035",
            "v8_guard_continuation",
            "v8-guard-continuation",
            "v8_guard_continuation_v2",
            "v8-guard-continuation-v2",
            "v8_guard_scalp_observe",
            "v8-guard-scalp-observe",
            "v8_guard_stop",
            "v8-guard-stop",
        }
        self._reversal_1030_v8_guard_filter_enabled = self._strategy_variant in {
            "v8_guard_filter",
            "v8-guard-filter",
            "v8_guard_filter_followthrough",
            "v8-guard-filter-followthrough",
            "v8_guard_rsi_floor",
            "v8-guard-rsi-floor",
            "v8_guard_rsi_floor_core_room_025",
            "v8-guard-rsi-floor-core-room-025",
            "v8_guard_rsi_floor_core_room_035",
            "v8-guard-rsi-floor-core-room-035",
            "v8_guard_continuation",
            "v8-guard-continuation",
            "v8_guard_continuation_v2",
            "v8-guard-continuation-v2",
            "v8_guard_scalp_observe",
            "v8-guard-scalp-observe",
        }
        self._reversal_1030_v8_guard_followthrough_enabled = self._strategy_variant in {
            "v8_guard_filter_followthrough",
            "v8-guard-filter-followthrough",
        }
        self._reversal_1030_v8_guard_rsi_floor_enabled = self._strategy_variant in {
            "v8_guard_rsi_floor",
            "v8-guard-rsi-floor",
            "v8_guard_rsi_floor_core_room_025",
            "v8-guard-rsi-floor-core-room-025",
            "v8_guard_rsi_floor_core_room_035",
            "v8-guard-rsi-floor-core-room-035",
            "v8_guard_continuation",
            "v8-guard-continuation",
            "v8_guard_continuation_v2",
            "v8-guard-continuation-v2",
            "v8_guard_scalp_observe",
            "v8-guard-scalp-observe",
        }
        self._reversal_1030_v8_guard_continuation_enabled = self._strategy_variant in {
            "v8_guard_continuation",
            "v8-guard-continuation",
        }
        self._reversal_1030_v8_guard_core_room_025_enabled = self._strategy_variant in {
            "v8_guard_rsi_floor_core_room_025",
            "v8-guard-rsi-floor-core-room-025",
        }
        self._reversal_1030_v8_guard_core_room_035_enabled = self._strategy_variant in {
            "v8_guard_rsi_floor_core_room_035",
            "v8-guard-rsi-floor-core-room-035",
        }
        self._reversal_1030_v8_guard_continuation_v2_enabled = self._strategy_variant in {
            "v8_guard_continuation_v2",
            "v8-guard-continuation-v2",
        }
        self._reversal_1030_v8_guard_scalp_observe_enabled = self._strategy_variant in {
            "v8_guard_scalp_observe",
            "v8-guard-scalp-observe",
        }
        self._reversal_1030_v8_guard_stop_enabled = self._strategy_variant in {
            "v8_guard_stop",
            "v8-guard-stop",
        }
        self._reversal_1030_v8a_enabled = self._strategy_variant in {
            "v8a",
            "v8a_low_reversal",
            "v8a-low-reversal",
            "v8_ab",
            "v8ab",
            "v8-a-b",
        }
        self._reversal_1030_v8b_enabled = self._strategy_variant in {
            "v8b",
            "v8b_reclaim",
            "v8b-reclaim",
            "v8_ab",
            "v8ab",
            "v8-a-b",
        }
        self._reversal_1030_v8b2_enabled = self._strategy_variant in {
            "v8b2",
            "v8b2_mid_reclaim_quality",
            "v8b2-mid-reclaim-quality",
            "v8b2_loose",
            "v8b2-mid-reclaim-quality-loose",
            "v8b2_mid_reclaim_quality_loose",
            "v8b2_room040",
            "v8b2-mid-reclaim-quality-room040",
            "v8b2_mid_reclaim_quality_room040",
            "v8b2_strict",
            "v8b2-mid-reclaim-quality-strict",
            "v8b2_mid_reclaim_quality_strict",
            "v8b3",
            "v8b3_mid_reclaim_followthrough",
            "v8b3-mid-reclaim-followthrough",
        }
        self._reversal_1030_v8b3_enabled = self._strategy_variant in {
            "v8b3",
            "v8b3_mid_reclaim_followthrough",
            "v8b3-mid-reclaim-followthrough",
        }
        self._reversal_1030_v8b2_loose_enabled = self._strategy_variant in {
            "v8b2_loose",
            "v8b2-mid-reclaim-quality-loose",
            "v8b2_mid_reclaim_quality_loose",
        }
        self._reversal_1030_v8b2_room040_enabled = self._strategy_variant in {
            "v8b2_room040",
            "v8b2-mid-reclaim-quality-room040",
            "v8b2_mid_reclaim_quality_room040",
        }
        self._reversal_1030_v8b2_strict_enabled = self._strategy_variant in {
            "v8b2_strict",
            "v8b2-mid-reclaim-quality-strict",
            "v8b2_mid_reclaim_quality_strict",
        }
        self._ttl = self._settings.ttl_buy_seconds
        self._cooldown = self._settings.cooldown_buy_seconds
        self._mfi_stoch_filter = self._settings.feature_mfi_stoch_filter
        self._preearn_days_min = int(self._settings.preearn_days_min)
        self._is_backtest = is_backtest
        self._allow_missing_option_liquidity_metrics = allow_missing_option_liquidity_metrics
        self._option_liquidity_required = bool(self._settings.option_liquidity_required)
        self._preearn_days_max = int(self._settings.preearn_days_max)
        self._preearn_atr_pct_max = Decimal(str(self._settings.preearn_atr_pct_max))
        self._positions: Dict[str, PositionState] = {}
        self._reversal_1030_entries: Dict[str, Dict[str, Any]] = {}
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
        self._buy_priority_slots: int = max(1, int(self._am_bottom_limits.top_n))
        self._vix_gate_mode = risk_limits.gate.vix.mode
        self._vix_gate = Decimal(str(risk_limits.gate.vix.thresh))
        self._ma60_cache: Dict[Tuple[str, date], Optional[Decimal]] = {}
        self._option_symbol_columns: Dict[str, str] = {}
        # 记录最近一次期权流动性筛选结果，供 reason / option_hint 使用
        self._liquidity_snapshot: Dict[str, Dict[str, Any]] = {}
        # 当日买入信号优先级缓存：按布林下轨斜率排名选前N只
        self._buy_priority: Dict[date, Dict[str, Decimal]] = {}

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
            cumulative_notional = Decimal("0")
            cumulative_volume = Decimal("0")
            vwap_session_date: date | None = None
            for ts_end, open_, high_, low_, close_, volume in rows:
                trace_id = f"preview-{symbol.upper()}-{int(ts_end.timestamp())}"
                et = ts_end.astimezone(EASTERN)
                session_date = et.date()
                if vwap_session_date != session_date:
                    cumulative_notional = Decimal("0")
                    cumulative_volume = Decimal("0")
                    vwap_session_date = session_date

                close_dec = _to_decimal(close_)
                volume_dec = Decimal(volume or 0)
                if volume_dec > 0:
                    cumulative_notional += close_dec * volume_dec
                    cumulative_volume += volume_dec
                vwap_value = close_dec if cumulative_volume <= 0 else cumulative_notional / cumulative_volume

                event = BarsClosed(
                    trace_id=trace_id,
                    symbol=symbol,
                    bar_start=ts_end - timedelta(minutes=1),
                    bar_end=ts_end,
                    timeframe="1m",
                    open=_to_decimal(open_),
                    high=_to_decimal(high_),
                    low=_to_decimal(low_),
                    close=close_dec,
                    volume=int(volume or 0),
                    vwap=vwap_value,
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
                ):
                    df = self._load_history(
                        session,
                        signal.symbol,
                        ts_end,
                        limit=self._history_window,
                    )
                    allowed, _ = self._can_open_position(signal.symbol, df, session, ts_end)
                else:
                    allowed = True
                if signal.signal_code in BUY_SIGNALS and not item.force and not allowed:
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
            "rank_score": float(_to_decimal(df.iloc[l2_idx]["lr_boll_dn_slope"] or Decimal("0"))),
            "rank_metric": "boll_dn_slope",
            "add_position": addition,
            "option_liquidity": self._liquidity_snapshot.get(event.symbol, {"pass": True}),
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
            "option_liquidity": self._liquidity_snapshot.get(event.symbol, {"pass": True}),
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
        if getattr(self, "_orb_entry_enabled", False):
            return None
        if getattr(self, "_delayed_entry_enabled", False):
            earliest = self._parse_hhmm(
                str(getattr(self._settings, "open_chase_earliest_entry_time", "09:36"))
            )
            if et_time.time() < earliest or et_time.time() >= time(10, 30):
                LOGGER.info(
                    "signal.e1.delayed_entry_filtered",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    earliest_entry_time=earliest.isoformat(timespec="minutes"),
                    reason="open chase delayed-entry variant blocks 09:30-09:34 baseline entry",
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("OPEN_CHASE_DELAYED_ENTRY")
                except Exception:
                    pass
                return None
        elif (et_time.hour, et_time.minute) != (9, 31):
            return None
        
        # 入口日志：记录进入E1检测
        LOGGER.info(
            "signal.e1.enter",
            symbol=event.symbol,
            timestamp=et_time.isoformat(),
            df_length=len(df),
            vix=str(vix_value) if vix_value else "None",
            vix_gate_mode=self._vix_gate_mode,
            vix_gate_threshold=str(self._vix_gate) if self._vix_gate else "None"
        )
        if (
            vix_value is not None
            and self._vix_gate_mode == "enforce"
            and vix_value >= self._vix_gate
        ):
            LOGGER.info(
                "signal.e1.vix_filtered",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                vix_value=str(vix_value),
                vix_threshold=str(self._vix_gate),
                reason="VIX过高，超过阈值"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("VIX_GATE")
            except Exception:
                pass
            return None
        if not self._top5_today(trade_date, session, event.symbol):
            LOGGER.info(
                "signal.e1.top5_filtered",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                trade_date=trade_date.isoformat(),
                reason="不在当天Top5列表中"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("TOP5_FILTER")
            except Exception:
                pass
            return None
        if len(df) < 6:
            LOGGER.info(
                "signal.e1.history_insufficient",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                df_length=len(df),
                required=6,
                reason="历史K线数据不足（需要至少6根）"
            )
            return None

        # 盘前涨幅过滤（权威来源）：09:28 价相对昨收 > 0.5%
        # 使用数据库查询 prev_close_rth 与 09:28 的 bars1m_equity
        LOGGER.info(
            "signal.e1.gap_check_start",
            symbol=event.symbol,
            timestamp=et_time.isoformat(),
            trade_date=trade_date.isoformat(),
            reason="开始盘前涨幅检查"
        )
        try:
            # 昨日收盘价（RTH 收盘）
            prev_close_value = self._fallback_daily_field_value(
                session,
                event.symbol,
                trade_date,
                field_name="prev_close_rth",
            )
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

            LOGGER.info(
                "signal.e1.gap_data_fetched",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                has_prev_close=prev_close_value is not None,
                has_0928_price=bool(row_0928 and row_0928[0]),
                prev_close_value=str(prev_close_value) if prev_close_value is not None else "None",
                price_0928_value=str(row_0928[0]) if row_0928 and row_0928[0] else "None"
            )
            
            if prev_close_value is None or row_0928 is None or row_0928[0] is None:
                # 缺少关键数据则不通过过滤
                LOGGER.info(
                    "signal.e1.gap_data_missing",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    prev_close_available=prev_close_value is not None,
                    price_0928_available=bool(row_0928 and row_0928[0]),
                    reason="盘前涨幅过滤：缺少昨收或09:28价格数据"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("PREMARKET_GAP_MISSING")
                except Exception:
                    pass
                return None

            try:
                prev_close = Decimal(str(prev_close_value))
                price_0928 = Decimal(str(row_0928[0]))
                LOGGER.info(
                    "signal.e1.gap_data_parsed",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    prev_close=str(prev_close),
                    price_0928=str(price_0928)
                )
            except Exception as e:
                LOGGER.info(
                    "signal.e1.gap_data_parse_fail",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    error=str(e),
                    reason="盘前涨幅过滤：价格数据解析失败"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("PREMARKET_GAP_PARSE_FAIL")
                except Exception:
                    pass
                return None

            if prev_close <= 0 or price_0928 <= 0:
                LOGGER.info(
                    "signal.e1.gap_data_invalid",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    prev_close=str(prev_close),
                    price_0928=str(price_0928),
                    reason="盘前涨幅过滤：价格数据无效（<=0）"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("PREMARKET_GAP_INVALID")
                except Exception:
                    pass
                return None

            pre_gap = (price_0928 - prev_close) / prev_close
            LOGGER.info(
                "signal.e1.gap_calculated",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                prev_close=str(prev_close),
                price_0928=str(price_0928),
                pre_gap_pct=f"{pre_gap * 100:.2f}%",
                required_pct="0.50%",
                passed=bool(pre_gap >= Decimal("0.005"))
            )
            if pre_gap < Decimal("0.005"):
                LOGGER.info(
                    "signal.e1.gap_filtered",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    prev_close=str(prev_close),
                    price_0928=str(price_0928),
                    pre_gap_pct=f"{pre_gap * 100:.2f}%",
                    required_pct="0.50%",
                    reason="盘前涨幅过滤：09:28涨幅不足0.5%"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("PREMARKET_GAP_FILTER")
                except Exception:
                    pass
                return None
            LOGGER.info(
                "signal.e1.gap_passed",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                reason="通过盘前涨幅检查，进入K线形态判断"
            )
        except Exception as e:
            # 防御性：任何异常都视为不通过，避免放行
            LOGGER.warning(
                "signal.e1.gap_exception",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                error=str(e),
                reason="盘前涨幅过滤：发生异常"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("PREMARKET_GAP_EXCEPTION")
            except Exception:
                pass
                return None
        
        LOGGER.info(
            "signal.e1.candle_check_start",
            symbol=event.symbol,
            timestamp=et_time.isoformat(),
            reason="开始K线形态检查"
        )
        try:
            prev = df.iloc[-2]
            prev2 = df.iloc[-3]
            LOGGER.info(
                "signal.e1.candle_data_loaded",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                prev_close=str(prev["close"]),
                prev2_close=str(prev2["close"]),
                prev_boll_mid=str(prev["boll_mid"])
            )
            two_green = bool(
                prev["close"] > prev["open"]
                and prev2["close"] > prev2["open"]
                and prev["close"] > prev2["close"]
            )
            opens_above_mid = bool(
                prev["open"] >= prev["boll_mid"] and prev2["open"] >= prev2["boll_mid"]
            )
        except Exception as e:
            LOGGER.error(
                "signal.e1.candle_data_error",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                error=str(e),
                df_length=len(df),
                reason="K线形态数据加载失败"
            )
            return None
        if not (two_green or opens_above_mid):
            LOGGER.info(
                "signal.e1.candle_pattern_filtered",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                two_green=two_green,
                opens_above_mid=opens_above_mid,
                prev_candle=f"O:{prev['open']:.2f} C:{prev['close']:.2f}",
                prev2_candle=f"O:{prev2['open']:.2f} C:{prev2['close']:.2f}",
                boll_mid_prev=f"{prev['boll_mid']:.2f}",
                reason="K线形态不符：既不是连续两根阳线，也不是开盘在布林中轨上方"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("CANDLE_PATTERN")
            except Exception:
                pass
            return None

        LOGGER.info(
            "signal.e1.candle_pattern_passed",
            symbol=event.symbol,
            timestamp=et_time.isoformat(),
            two_green=two_green,
            opens_above_mid=opens_above_mid,
            reason="K线形态检查通过，进入技术指标检查"
        )
        try:
            ao_up = bool(prev["ao"] > 0 and prev["ao"] >= prev2["ao"])
            cci_positive = bool(prev["cci14"] > 0 and prev["cci6"] > 0)
            obv_rising = bool(prev["obv"] >= prev["obv_ema20"])
            tech_score = sum([ao_up, cci_positive, obv_rising])
        except Exception as e:
            LOGGER.error(
                "signal.e1.tech_indicator_error",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                error=str(e),
                reason="技术指标计算失败"
            )
            return None
        if tech_score < 2:
            LOGGER.info(
                "signal.e1.technical_filtered",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                ao_up=ao_up,
                ao_value=f"{prev['ao']:.4f}",
                ao_prev2=f"{prev2['ao']:.4f}",
                cci_positive=cci_positive,
                cci14=f"{prev['cci14']:.2f}",
                cci6=f"{prev['cci6']:.2f}",
                obv_rising=obv_rising,
                obv=f"{prev['obv']:.0f}",
                obv_ema20=f"{prev['obv_ema20']:.0f}",
                tech_score=f"{tech_score}/3",
                reason=f"技术指标不足：需满足2/3条件，实际{tech_score}/3"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("TECHNICAL_INDICATORS")
            except Exception:
                pass
            return None

        mfi_pass = False
        stoch_pass = False
        if self._mfi_stoch_filter:
            # 检查MFI数据有效性
            current_mfi = prev["mfi14"]
            lag5_mfi = df.iloc[-5]["mfi14"]
            
            if pd.isna(current_mfi) or pd.isna(lag5_mfi):
                LOGGER.warning(
                    "signal.e1.mfi_data_missing",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    current_mfi=str(current_mfi),
                    lag5_mfi=str(lag5_mfi),
                    reason="MFI数据缺失，拒绝信号"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("MFI_DATA_MISSING")
                except Exception:
                    pass
                return None
            
            mfi_slope = current_mfi - lag5_mfi
            mfi_pass = bool(current_mfi > 50 and mfi_slope > 0)
            
            stoch_pass = bool(
                prev2["stoch_k"] <= prev2["stoch_d"]
                and prev["stoch_k"] > prev["stoch_d"]
                and prev["stoch_k"] < 80
            )
            
            if not (mfi_pass or stoch_pass):
                LOGGER.info(
                    "signal.e1.mfi_stoch_filtered",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    mfi_pass=mfi_pass,
                    mfi_current=f"{current_mfi:.2f}",
                    mfi_lag5=f"{lag5_mfi:.2f}",
                    mfi_slope=f"{mfi_slope:.2f}",
                    mfi_check=f"MFI>{50}: {current_mfi > 50}, 斜率>0: {mfi_slope > 0}",
                    stoch_pass=stoch_pass,
                    stoch_k_prev2=f"{prev2['stoch_k']:.2f}",
                    stoch_d_prev2=f"{prev2['stoch_d']:.2f}",
                    stoch_k_prev=f"{prev['stoch_k']:.2f}",
                    stoch_d_prev=f"{prev['stoch_d']:.2f}",
                    stoch_check=f"金叉: {prev2['stoch_k'] <= prev2['stoch_d'] and prev['stoch_k'] > prev['stoch_d']}, K<80: {prev['stoch_k'] < 80}",
                    reason="MFI/Stoch过滤：MFI和Stoch条件均不满足"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("MFI_STOCH_FILTER")
                except Exception:
                    pass
                return None

        if 12 <= et_time.hour < 14:
            LOGGER.info(
                "signal.e1.time_window_filtered",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                hour=et_time.hour,
                reason="时间窗口过滤：12:00-14:00禁止开仓"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("TIME_WINDOW")
            except Exception:
                pass
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            LOGGER.info(
                "signal.e1.position_limit_filtered",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                reason="位置管理过滤：不允许开新仓位"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("POSITION_LIMIT")
            except Exception:
                pass
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
            "option_liquidity": self._liquidity_snapshot.get(event.symbol, {"pass": True}),
        }
        
        # 成功生成买入信号的日志
        LOGGER.info(
            "signal.e1.buy_signal_generated",
            symbol=event.symbol,
            timestamp=et_time.isoformat(),
            signal_code="SIG_OPEN_CHASE_BUY",
            candle_pattern="two_green" if two_green else "opens_above_mid",
            tech_indicators=f"AO:{ao_up}, CCI:{cci_positive}, OBV:{obv_rising}",
            mfi_stoch=f"MFI:{mfi_pass}, Stoch:{stoch_pass}" if self._mfi_stoch_filter else "disabled",
            reason="所有过滤条件已通过，生成开盘追涨买入信号"
        )
        
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
        e2_tech_score = sum([rvol_pass, obv_cross, ao_positive])
        if e2_tech_score < 2:
            LOGGER.info(
                "signal.e2.technical_filtered",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                rvol_pass=rvol_pass,
                rvol6=f"{current['rvol6']:.2f}",
                obv_cross=obv_cross,
                obv_prev=f"{prev['obv']:.0f}",
                obv_current=f"{current['obv']:.0f}",
                obv_ema20=f"{current['obv_ema20']:.0f}",
                ao_positive=ao_positive,
                ao_prev=f"{prev['ao']:.4f}",
                ao_current=f"{current['ao']:.4f}",
                tech_score=f"{e2_tech_score}/3",
                reason=f"E2技术指标不足：需满足2/3条件，实际{e2_tech_score}/3"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("E2_TECHNICAL_INDICATORS")
            except Exception:
                pass
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
            LOGGER.info(
                "signal.e2.scenario_filtered",
                symbol=event.symbol,
                timestamp=et_time.isoformat(),
                scenario_a=scenario_a,
                scenario_b=scenario_b,
                time_window=f"{et_time.hour}:{et_time.minute:02d}",
                rsi6_prev=f"{prev['rsi6']:.2f}" if et_time <= morning_cutoff else "N/A",
                rsi6_current=f"{current['rsi6']:.2f}" if et_time <= morning_cutoff else "N/A",
                reason="E2情景不符：既不是RSI30金叉（9:33前），也不是二次探底反弹（10:00-10:30）"
            )
            try:
                if self._metrics_enabled:
                    record_signal_filtered("E2_SCENARIO")
            except Exception:
                pass
            return None

        if 12 <= et_time.hour < 14:
            return None
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None

        mfi_pass = False
        stoch_pass = False
        if self._mfi_stoch_filter:
            # 检查MFI数据有效性
            current_mfi = current["mfi14"]
            lag4_mfi = df.iloc[-4]["mfi14"]
            
            if pd.isna(current_mfi) or pd.isna(lag4_mfi):
                LOGGER.warning(
                    "signal.e2.mfi_data_missing",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    current_mfi=str(current_mfi),
                    lag4_mfi=str(lag4_mfi),
                    reason="E2 MFI数据缺失，拒绝信号"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("E2_MFI_DATA_MISSING")
                except Exception:
                    pass
                return None
            
            mfi_slope = current_mfi - lag4_mfi
            mfi_pass = bool(current_mfi > 50 and mfi_slope > 0)
            stoch_pass = bool(
                prev["stoch_k"] <= prev["stoch_d"]
                and current["stoch_k"] > current["stoch_d"]
                and current["stoch_k"] < 80
            )
            
            if not (mfi_pass or stoch_pass):
                LOGGER.info(
                    "signal.e2.mfi_stoch_filtered",
                    symbol=event.symbol,
                    timestamp=et_time.isoformat(),
                    mfi_pass=mfi_pass,
                    mfi_current=f"{current_mfi:.2f}",
                    mfi_lag4=f"{lag4_mfi:.2f}",
                    mfi_slope=f"{mfi_slope:.2f}",
                    stoch_pass=stoch_pass,
                    stoch_k_prev=f"{prev['stoch_k']:.2f}",
                    stoch_d_prev=f"{prev['stoch_d']:.2f}",
                    stoch_k_current=f"{current['stoch_k']:.2f}",
                    stoch_d_current=f"{current['stoch_d']:.2f}",
                    reason="E2 MFI/Stoch过滤：MFI和Stoch条件均不满足"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("E2_MFI_STOCH_FILTER")
                except Exception:
                    pass
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
            "option_liquidity": self._liquidity_snapshot.get(event.symbol, {"pass": True}),
        }
        
        # 成功生成E2买入信号的日志
        LOGGER.info(
            "signal.e2.buy_signal_generated",
            symbol=event.symbol,
            timestamp=et_time.isoformat(),
            signal_code="SIG_REBOUND_BUY",
            scenario="RSI30金叉" if scenario_a else f"二次探底反弹(+{bounce_amount:.2%})",
            tech_indicators=f"RVOL:{rvol_pass}, OBV_Cross:{obv_cross}, AO+:{ao_positive}",
            mfi_stoch=f"MFI:{mfi_pass}, Stoch:{stoch_pass}" if self._mfi_stoch_filter else "disabled",
            reason="所有E2过滤条件已通过，生成反弹买入信号"
        )
        
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_REBOUND_BUY",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_open_chase_orb(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        if not getattr(self, "_orb_entry_enabled", False):
            return None
        et_time = _to_eastern(event.bar_end)
        earliest = self._parse_hhmm(
            str(getattr(self._settings, "open_chase_earliest_entry_time", "09:36"))
        )
        if et_time.time() < earliest or et_time.time() >= time(10, 30):
            return None
        if not self._top5_today(trade_date, session, event.symbol):
            return None
        todays = df[df["et"].dt.date == trade_date]
        if len(todays) < max(6, int(getattr(self._settings, "open_chase_opening_range_minutes", 5))):
            return None
        current = todays.iloc[-1]
        close = current["close"]
        high = current["high"]
        low = current["low"]
        open_px = current["open"]
        if None in (close, high, low, open_px):
            return None

        or_high, or_low = self._opening_range_from_df(todays)
        vwap = self._vwap_from_df(todays)
        if or_high is None or or_low is None or vwap is None:
            return None
        if close <= vwap:
            return self._log_orb_reject(event, "reject_below_vwap")
        if close <= or_high:
            return self._log_orb_reject(event, "reject_not_breaking_orh")

        total_range = high - low
        if total_range <= 0:
            return self._log_orb_reject(event, "reject_invalid_range")
        close_position = (close - low) / total_range
        if close_position < Decimal("0.50"):
            return self._log_orb_reject(event, "reject_reclaim_failed")
        upper_shadow = high - max(open_px, close)
        upper_shadow_pct = upper_shadow / total_range if total_range > 0 else Decimal("1")
        max_shadow = Decimal(
            str(getattr(self._settings, "open_chase_max_upper_shadow_pct", Decimal("0.40")))
        )
        if upper_shadow_pct > max_shadow:
            return self._log_orb_reject(event, "reject_large_upper_shadow")
        if self._first_bar_blowoff(todays):
            return self._log_orb_reject(event, "reject_first_bar_blowoff")
        rvol = current.get("rvol6")
        min_rvol = Decimal(str(getattr(self._settings, "open_chase_min_rvol", Decimal("1.0"))))
        if rvol is not None and rvol < min_rvol:
            return self._log_orb_reject(event, "reject_low_relative_volume")
        if bool(getattr(self._settings, "open_chase_require_market_confirmed", False)):
            return self._log_orb_reject(event, "reject_market_not_confirmed")

        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None
        reason = {
            "entry_mode": "breakout",
            "opening_range_high": str(or_high),
            "opening_range_low": str(or_low),
            "vwap": str(vwap),
            "close": str(close),
            "close_position": str(close_position),
            "upper_shadow_pct": str(upper_shadow_pct),
            "rvol6": str(rvol) if rvol is not None else None,
            "add_position": addition,
            "filters": {
                "reject_below_vwap": False,
                "reject_not_breaking_orh": False,
                "reject_reclaim_failed": False,
                "reject_large_upper_shadow": False,
                "reject_first_bar_blowoff": False,
                "reject_low_relative_volume": False,
                "reject_market_not_confirmed": False,
            },
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_OPEN_CHASE_ORB_BUY",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_1030_reversal_call(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        if not getattr(
            self,
            "_reversal_1030_legacy_enabled",
            getattr(self, "_reversal_1030_enabled", False),
        ):
            return None
        symbol = event.symbol.upper()
        symbols = {
            item.strip().upper()
            for item in str(getattr(self._settings, "reversal_1030_symbols", "")).split(",")
            if item.strip()
        }
        if symbols and symbol not in symbols:
            return None

        et_time = _to_eastern(event.bar_end)
        start_time = self._parse_hhmm(
            str(getattr(self._settings, "reversal_1030_start_time", "10:25"))
        )
        end_time = self._parse_hhmm(
            str(getattr(self._settings, "reversal_1030_end_time", "10:40"))
        )
        if et_time.time() < start_time or et_time.time() > end_time:
            return None
        max_vix = Decimal(str(getattr(self._settings, "reversal_1030_max_vix", Decimal("20"))))
        if vix_value is not None and vix_value > max_vix:
            return self._log_1030_reject(event, "reject_vix_high", vix=str(vix_value))

        todays = df[df["et"].dt.date == trade_date]
        morning = todays[
            (todays["et"].dt.time >= time(9, 30))
            & (todays["et"].dt.time <= et_time.time())
        ]
        if len(morning) < 20:
            return None
        current = morning.iloc[-1]
        prev = morning.iloc[-2]
        session_open = _to_decimal(morning.iloc[0]["open"])
        close = _to_decimal(current["close"])
        open_px = _to_decimal(current["open"])
        high = _to_decimal(current["high"])
        low = _to_decimal(current["low"])
        boll_dn = _to_decimal(current.get("boll_dn"))
        boll_mid = _to_decimal(current.get("boll_mid"))
        boll_up = _to_decimal(current.get("boll_up"))
        rsi = _to_decimal(current.get("rsi6"))
        prev_rsi = _to_decimal(prev.get("rsi6"))
        if session_open <= 0 or close <= 0 or high <= 0 or low <= 0:
            return None
        if boll_dn <= 0 or boll_mid <= 0 or boll_up <= 0:
            return None

        abs_gap_limit = Decimal(
            str(getattr(self._settings, "reversal_1030_max_abs_gap_pct", Decimal("0.06")))
        )
        intraday_move = (close - session_open) / session_open
        if abs(intraday_move) > abs_gap_limit:
            return self._log_1030_reject(
                event,
                "reject_abs_move_gt_limit",
                intraday_move=str(intraday_move),
                limit=str(abs_gap_limit),
            )

        min_drop = Decimal(
            str(getattr(self._settings, "reversal_1030_min_morning_drop_pct", Decimal("0.003")))
        )
        morning_low = _to_decimal(morning["low"].min())
        morning_drop = (session_open - morning_low) / session_open
        if morning_drop < min_drop:
            return self._log_1030_reject(
                event,
                "reject_no_morning_selloff",
                morning_drop=str(morning_drop),
                threshold=str(min_drop),
            )

        lower_buffer = Decimal(
            str(getattr(self._settings, "reversal_1030_lower_band_buffer_pct", Decimal("0.0015")))
        )
        lower_touch_window = morning.tail(8)
        lower_touch = False
        doji_stability_count = 0
        for _, row in lower_touch_window.iterrows():
            row_low = _to_decimal(row.get("low"))
            row_close = _to_decimal(row.get("close"))
            row_open = _to_decimal(row.get("open"))
            row_high = _to_decimal(row.get("high"))
            row_dn = _to_decimal(row.get("boll_dn"))
            if row_dn <= 0:
                continue
            if row_low <= row_dn * (Decimal("1") + lower_buffer):
                lower_touch = True
            bar_range = row_high - row_low
            body = abs(row_close - row_open)
            if (
                row_low <= row_dn * (Decimal("1") + lower_buffer * Decimal("2"))
                and bar_range > 0
                and body / bar_range <= Decimal("0.35")
            ):
                doji_stability_count += 1
        if not lower_touch and doji_stability_count < 3:
            return self._log_1030_reject(event, "reject_no_lower_band_reversal")

        green_bar = close > open_px
        reclaim_lower = close > boll_dn and prev.get("close") is not None and _to_decimal(prev["close"]) <= boll_dn
        recent_high_break = close > _to_decimal(morning.tail(4).iloc[:-1]["high"].max())
        rsi_reclaim_level = Decimal(
            str(getattr(self._settings, "reversal_1030_rsi_reclaim", Decimal("30")))
        )
        rsi_reclaim = rsi >= rsi_reclaim_level and (
            prev_rsi < rsi_reclaim_level or rsi > prev_rsi
        )
        if not (green_bar and rsi_reclaim and (reclaim_lower or recent_high_break)):
            return self._log_1030_reject(
                event,
                "reject_no_reversal_confirmation",
                green_bar=str(green_bar),
                rsi=str(rsi),
                prev_rsi=str(prev_rsi),
                reclaim_lower=str(reclaim_lower),
                recent_high_break=str(recent_high_break),
            )

        vwap = self._vwap_from_df(morning)
        boll_position = self._bollinger_position(close, boll_dn, boll_up)
        vwap_distance = None
        if vwap is not None and vwap > 0:
            vwap_distance = (close - vwap) / vwap
        guard_boll_pos_threshold = Decimal(
            str(
                getattr(
                    self._settings,
                    "reversal_1030_v8_guard_boll_pos_threshold",
                    Decimal("0.30"),
                )
            )
        )
        v8_guard_triggered = bool(
            vwap is not None
            and close > vwap
            and boll_position < guard_boll_pos_threshold
        )
        if (
            getattr(self, "_reversal_1030_v8_guard_filter_enabled", False)
            and v8_guard_triggered
        ):
            return self._log_1030_reject(
                event,
                "reject_v8_guard_above_vwap_low_boll_position",
                close=str(close),
                vwap=str(vwap),
                boll_position=str(boll_position),
                limit=str(guard_boll_pos_threshold),
            )
        if getattr(self, "_reversal_1030_v8_guard_rsi_floor_enabled", False):
            rsi_floor_boll_pos_threshold = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_v8_guard_rsi_floor_boll_pos_threshold",
                        Decimal("0.20"),
                    )
                )
            )
            rsi_floor_min_rsi6 = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_v8_guard_rsi_floor_min_rsi6",
                        Decimal("35"),
                    )
                )
            )
            if (
                vwap is not None
                and close <= vwap
                and boll_position < rsi_floor_boll_pos_threshold
                and rsi < rsi_floor_min_rsi6
            ):
                return self._log_1030_reject(
                    event,
                    "reject_v8_guard_rsi_floor_below_vwap_deep_low",
                    close=str(close),
                    vwap=str(vwap),
                    boll_position=str(boll_position),
                    boll_pos_limit=str(rsi_floor_boll_pos_threshold),
                    rsi=str(rsi),
                    rsi_limit=str(rsi_floor_min_rsi6),
                )
        upper_room = (boll_up - close) / close
        continuation_enabled = bool(
            getattr(self, "_reversal_1030_v8_guard_continuation_enabled", False)
        )
        thin_room_filter = Decimal(
            str(
                getattr(
                    self._settings,
                    "reversal_1030_thin_room_filter_pct",
                    Decimal("0.0010"),
                )
            )
        )
        thin_room_min_boll_pos = Decimal(
            str(
                getattr(
                    self._settings,
                    "reversal_1030_thin_room_filter_min_boll_pos",
                    Decimal("0.80"),
                )
            )
        )
        if (
            continuation_enabled
            and upper_room < thin_room_filter
            and boll_position > thin_room_min_boll_pos
            and not (vwap is not None and close > vwap)
        ):
            return self._log_1030_reject(
                event,
                "reject_v8_guard_continuation_thin_room_near_upper",
                close=str(close),
                vwap=str(vwap) if vwap is not None else None,
                boll_position=str(boll_position),
                boll_pos_limit=str(thin_room_min_boll_pos),
                upper_room_pct=str(upper_room),
                upper_room_limit=str(thin_room_filter),
            )
        core_room_min_upper_room = None
        if getattr(self, "_reversal_1030_v8_guard_core_room_025_enabled", False):
            core_room_min_upper_room = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_v8_guard_core_room_025_min_upper_room_pct",
                        Decimal("0.0025"),
                    )
                )
            )
        elif getattr(self, "_reversal_1030_v8_guard_core_room_035_enabled", False):
            core_room_min_upper_room = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_v8_guard_core_room_035_min_upper_room_pct",
                        Decimal("0.0035"),
                    )
                )
            )
        core_max_boll_pos = Decimal(
            str(
                getattr(
                    self._settings,
                    "reversal_1030_v8_guard_core_max_boll_pos",
                    Decimal("0.80"),
                )
            )
        )
        core_room_enabled = core_room_min_upper_room is not None
        if core_room_enabled:
            if upper_room < core_room_min_upper_room:
                return self._log_1030_reject(
                    event,
                    "reject_v8_guard_core_upper_room_small",
                    upper_room_pct=str(upper_room),
                    limit=str(core_room_min_upper_room),
                )
            if boll_position > core_max_boll_pos:
                return self._log_1030_reject(
                    event,
                    "reject_v8_guard_core_boll_position_high",
                    boll_position=str(boll_position),
                    limit=str(core_max_boll_pos),
                )

        continuation_v2_enabled = bool(
            getattr(self, "_reversal_1030_v8_guard_continuation_v2_enabled", False)
        )
        continuation_min_boll_pos = Decimal(
            str(
                getattr(
                    self._settings,
                    "reversal_1030_v8_guard_continuation_min_boll_pos",
                    Decimal("0.60"),
                )
            )
        )
        is_continuation_candidate = bool(
            vwap is not None
            and close >= vwap
            and close >= boll_mid
            and boll_position >= continuation_min_boll_pos
        )
        if continuation_v2_enabled and not is_continuation_candidate:
            return self._log_1030_reject(
                event,
                "reject_v8_guard_continuation_v2_not_quality_continuation",
                close=str(close),
                vwap=str(vwap) if vwap is not None else None,
                boll_mid=str(boll_mid),
                boll_position=str(boll_position),
                boll_pos_min=str(continuation_min_boll_pos),
            )

        scalp_observe_enabled = bool(
            getattr(self, "_reversal_1030_v8_guard_scalp_observe_enabled", False)
        )
        is_scalp_thin_room = bool(
            vwap is not None
            and close >= vwap
            and boll_position > thin_room_min_boll_pos
            and upper_room < thin_room_filter
        )
        if scalp_observe_enabled and not is_scalp_thin_room:
            return self._log_1030_reject(
                event,
                "reject_v8_guard_scalp_observe_not_thin_room_scalp",
                close=str(close),
                vwap=str(vwap) if vwap is not None else None,
                boll_position=str(boll_position),
                boll_pos_min=str(thin_room_min_boll_pos),
                upper_room_pct=str(upper_room),
                upper_room_limit=str(thin_room_filter),
            )
        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None
        entry_mode = (
            "v8_guard_stop"
            if getattr(self, "_reversal_1030_v8_guard_stop_enabled", False)
            else "v8_guard_filter_followthrough"
            if getattr(self, "_reversal_1030_v8_guard_followthrough_enabled", False)
            else "v8_guard_continuation_v2"
            if continuation_v2_enabled
            else "v8_guard_scalp_observe"
            if scalp_observe_enabled
            else "v8_guard_rsi_floor_core_room_025"
            if getattr(self, "_reversal_1030_v8_guard_core_room_025_enabled", False)
            else "v8_guard_rsi_floor_core_room_035"
            if getattr(self, "_reversal_1030_v8_guard_core_room_035_enabled", False)
            else "v8_guard_continuation"
            if continuation_enabled
            else "v8_guard_rsi_floor"
            if getattr(self, "_reversal_1030_v8_guard_rsi_floor_enabled", False)
            else "1030_reversal_call"
        )
        if core_room_enabled:
            trade_type = "option_core_reversal"
            quality_profile = "core_room_025" if entry_mode.endswith("_025") else "core_room_035"
        elif continuation_v2_enabled:
            trade_type = "continuation_candidate"
            quality_profile = "continuation_v2"
        elif scalp_observe_enabled:
            trade_type = "scalp_thin_room"
            quality_profile = "scalp_observe"
        else:
            trade_type = None
            quality_profile = None
        reason = {
            "entry_mode": entry_mode,
            "watchlist": sorted(symbols),
            "session_open": str(session_open),
            "morning_low": str(morning_low),
            "morning_drop_pct": str(morning_drop),
            "intraday_move_pct": str(intraday_move),
            "boll_dn": str(boll_dn),
            "boll_mid": str(boll_mid),
            "boll_up": str(boll_up),
            "boll_position": str(boll_position),
            "upper_room_pct": str(upper_room),
            "vwap": str(vwap) if vwap is not None else None,
            "vwap_distance_pct": str(vwap_distance) if vwap_distance is not None else None,
            "rsi6": str(rsi),
            "prev_rsi6": str(prev_rsi),
            "lower_touch": lower_touch,
            "doji_stability_count": doji_stability_count,
            "green_bar": green_bar,
            "reclaim_lower": reclaim_lower,
            "recent_high_break": recent_high_break,
            "entry_bar_low": str(low),
            "entry_close": str(close),
            "entry_vwap": str(vwap) if vwap is not None else None,
            "entry_boll_mid": str(boll_mid),
            "v8_guard_triggered": v8_guard_triggered,
            "v8_guard_boll_pos_threshold": str(guard_boll_pos_threshold),
            "is_option_core": bool(core_room_enabled),
            "is_continuation_candidate": bool(is_continuation_candidate),
            "is_scalp_thin_room": bool(is_scalp_thin_room),
            "trade_type": trade_type,
            "quality_profile": quality_profile,
            "min_upper_room_pct": (
                str(core_room_min_upper_room) if core_room_min_upper_room is not None else None
            ),
            "max_core_boll_pos": str(core_max_boll_pos),
            "v8_guard_entry_low_stop_enabled": bool(
                getattr(self, "_reversal_1030_v8_guard_stop_enabled", False)
                and v8_guard_triggered
            ),
            "exit_plan": {
                "primary": "sell_call_when_underlying_or_option_taps_1m_boll_upper",
                "deadline": str(getattr(self._settings, "reversal_1030_exit_deadline", "12:00")),
            },
            "add_position": addition,
            "option_liquidity": self._liquidity_snapshot.get(event.symbol, {"pass": True}),
        }
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_1030_REVERSAL_CALL_BUY",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
            ttl_override=min(int(self._ttl), 90),
        )

    def _detect_1030_v8a_low_reversal_call(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        if not getattr(self, "_reversal_1030_v8a_enabled", False):
            return None
        ctx = self._reversal_1030_context(event, df, trade_date, vix_value)
        if ctx is None:
            return None

        vwap = ctx["vwap"]
        close = ctx["close"]
        if vwap is None or close >= vwap:
            return self._log_1030_reject(
                event,
                "reject_v8a_not_below_vwap",
                close=str(close),
                vwap=str(vwap) if vwap is not None else None,
            )

        max_boll_pos = Decimal(
            str(getattr(self._settings, "reversal_1030_max_boll_pos_low", Decimal("0.55")))
        )
        if ctx["boll_position"] > max_boll_pos:
            return self._log_1030_reject(
                event,
                "reject_v8a_boll_position_high",
                boll_position=str(ctx["boll_position"]),
                limit=str(max_boll_pos),
            )

        max_rsi = Decimal(str(getattr(self._settings, "reversal_1030_max_rsi6", Decimal("65"))))
        if ctx["rsi"] > max_rsi:
            return self._log_1030_reject(
                event,
                "reject_v8a_rsi_hot",
                rsi=str(ctx["rsi"]),
                limit=str(max_rsi),
            )

        if not (
            ctx["green_bar"]
            and ctx["rsi_reclaim"]
            and (ctx["reclaim_lower"] or ctx["recent_high_break"])
            and (ctx["lower_touch"] or ctx["doji_stability_count"] >= 3)
        ):
            return self._log_1030_reject(
                event,
                "reject_v8a_no_low_reversal_confirmation",
                green_bar=str(ctx["green_bar"]),
                rsi=str(ctx["rsi"]),
                prev_rsi=str(ctx["prev_rsi"]),
                reclaim_lower=str(ctx["reclaim_lower"]),
                recent_high_break=str(ctx["recent_high_break"]),
                lower_touch=str(ctx["lower_touch"]),
            )

        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None
        reason = self._reversal_1030_reason(ctx, entry_mode="v8a_low_reversal", addition=addition)
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_1030_V8A_LOW_REVERSAL_CALL_BUY",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
            ttl_override=min(int(self._ttl), 90),
        )

    def _detect_1030_v8b_reclaim_call(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        if not getattr(self, "_reversal_1030_v8b_enabled", False):
            return None
        if self._positions.get(event.symbol) is not None:
            return None
        ctx = self._reversal_1030_context(event, df, trade_date, vix_value)
        if ctx is None:
            return None

        max_boll_pos = Decimal(
            str(getattr(self._settings, "reversal_1030_max_boll_pos_reclaim", Decimal("0.60")))
        )
        if ctx["boll_position"] > max_boll_pos:
            return self._log_1030_reject(
                event,
                "reject_v8b_boll_position_high",
                boll_position=str(ctx["boll_position"]),
                limit=str(max_boll_pos),
            )

        max_rsi = Decimal(str(getattr(self._settings, "reversal_1030_max_rsi6", Decimal("65"))))
        if ctx["rsi"] > max_rsi:
            return self._log_1030_reject(
                event,
                "reject_v8b_rsi_hot",
                rsi=str(ctx["rsi"]),
                limit=str(max_rsi),
            )

        max_upper_shadow = Decimal(
            str(getattr(self._settings, "reversal_1030_max_upper_shadow_pct", Decimal("0.40")))
        )
        if ctx["upper_shadow_pct"] > max_upper_shadow:
            return self._log_1030_reject(
                event,
                "reject_v8b_large_upper_shadow",
                upper_shadow_pct=str(ctx["upper_shadow_pct"]),
                limit=str(max_upper_shadow),
            )

        reclaim_level = None
        reclaim_type = None
        if ctx["reclaim_vwap"]:
            reclaim_level = ctx["vwap"]
            reclaim_type = "vwap"
        elif ctx["reclaim_mid"]:
            reclaim_level = ctx["boll_mid"]
            reclaim_type = "boll_mid"
        if reclaim_level is None or reclaim_level <= 0:
            return self._log_1030_reject(
                event,
                "reject_v8b_no_reclaim",
                reclaim_vwap=str(ctx["reclaim_vwap"]),
                reclaim_mid=str(ctx["reclaim_mid"]),
            )

        max_distance = Decimal(
            str(
                getattr(
                    self._settings,
                    "reversal_1030_reclaim_max_distance_vwap_pct",
                    Decimal("0.0025"),
                )
            )
        )
        reclaim_distance = (ctx["close"] - reclaim_level) / reclaim_level
        if reclaim_distance < 0 or reclaim_distance > max_distance:
            return self._log_1030_reject(
                event,
                "reject_v8b_reclaim_extended",
                reclaim_type=reclaim_type,
                reclaim_distance=str(reclaim_distance),
                limit=str(max_distance),
            )

        if not ctx["green_bar"]:
            return self._log_1030_reject(event, "reject_v8b_not_green")

        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None
        reason = self._reversal_1030_reason(
            ctx,
            entry_mode="v8b_reclaim",
            addition=addition,
            extra={
                "reclaim_type": reclaim_type,
                "reclaim_level": str(reclaim_level),
                "reclaim_distance_pct": str(reclaim_distance),
            },
        )
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_1030_V8B_RECLAIM_CALL_BUY",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
            ttl_override=min(int(self._ttl), 90),
        )

    def _detect_1030_v8b2_mid_reclaim_quality_call(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        if not getattr(self, "_reversal_1030_v8b2_enabled", False):
            return None
        if self._positions.get(event.symbol) is not None:
            return None
        ctx = self._reversal_1030_context(event, df, trade_date, vix_value)
        if ctx is None:
            return None

        strict = bool(getattr(self, "_reversal_1030_v8b2_strict_enabled", False))
        loose = bool(getattr(self, "_reversal_1030_v8b2_loose_enabled", False))
        room040 = bool(getattr(self, "_reversal_1030_v8b2_room040_enabled", False))
        followthrough = bool(getattr(self, "_reversal_1030_v8b3_enabled", False))
        if followthrough:
            max_boll_pos = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_max_boll_pos_reclaim",
                        Decimal("0.60"),
                    )
                )
            )
            min_upper_room = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_mid_reclaim_min_upper_room_pct",
                        Decimal("0.0035"),
                    )
                )
            )
            quality_profile = "followthrough"
            entry_mode = "v8b3_mid_reclaim_followthrough"
        elif strict:
            max_boll_pos = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_mid_reclaim_strict_max_boll_pos",
                        Decimal("0.55"),
                    )
                )
            )
            min_upper_room = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_mid_reclaim_strict_min_upper_room_pct",
                        Decimal("0.0035"),
                    )
                )
            )
            quality_profile = "strict"
            entry_mode = "v8b2_mid_reclaim_quality"
        elif room040:
            max_boll_pos = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_max_boll_pos_reclaim",
                        Decimal("0.60"),
                    )
                )
            )
            min_upper_room = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_mid_reclaim_room040_min_upper_room_pct",
                        Decimal("0.0040"),
                    )
                )
            )
            quality_profile = "room040"
            entry_mode = "v8b2_mid_reclaim_quality"
        elif loose:
            max_boll_pos = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_max_boll_pos_reclaim",
                        Decimal("0.60"),
                    )
                )
            )
            min_upper_room = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_mid_reclaim_loose_min_upper_room_pct",
                        Decimal("0.0030"),
                    )
                )
            )
            quality_profile = "loose"
            entry_mode = "v8b2_mid_reclaim_quality"
        else:
            max_boll_pos = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_max_boll_pos_reclaim",
                        Decimal("0.60"),
                    )
                )
            )
            min_upper_room = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_mid_reclaim_min_upper_room_pct",
                        Decimal("0.0030"),
                    )
                )
            )
            quality_profile = "base"
            entry_mode = "v8b2_mid_reclaim_quality"

        if ctx["boll_position"] > max_boll_pos:
            return self._log_1030_reject(
                event,
                "reject_v8b2_boll_position_high",
                boll_position=str(ctx["boll_position"]),
                limit=str(max_boll_pos),
            )

        max_rsi = Decimal(str(getattr(self._settings, "reversal_1030_max_rsi6", Decimal("65"))))
        if ctx["rsi"] > max_rsi:
            return self._log_1030_reject(
                event,
                "reject_v8b2_rsi_hot",
                rsi=str(ctx["rsi"]),
                limit=str(max_rsi),
            )

        max_upper_shadow = Decimal(
            str(getattr(self._settings, "reversal_1030_max_upper_shadow_pct", Decimal("0.40")))
        )
        if ctx["upper_shadow_pct"] > max_upper_shadow:
            return self._log_1030_reject(
                event,
                "reject_v8b2_large_upper_shadow",
                upper_shadow_pct=str(ctx["upper_shadow_pct"]),
                limit=str(max_upper_shadow),
            )

        if not ctx["reclaim_mid"]:
            return self._log_1030_reject(
                event,
                "reject_v8b2_no_mid_reclaim",
                reclaim_vwap=str(ctx["reclaim_vwap"]),
                reclaim_mid=str(ctx["reclaim_mid"]),
            )

        reclaim_level = ctx["boll_mid"]
        max_distance = Decimal(
            str(
                getattr(
                    self._settings,
                    "reversal_1030_mid_reclaim_max_distance_pct",
                    Decimal("0.0010"),
                )
            )
        )
        reclaim_distance = (ctx["close"] - reclaim_level) / reclaim_level
        if reclaim_distance < 0 or reclaim_distance > max_distance:
            return self._log_1030_reject(
                event,
                "reject_v8b2_mid_reclaim_extended",
                reclaim_distance=str(reclaim_distance),
                limit=str(max_distance),
            )

        upper_room = (ctx["boll_up"] - ctx["close"]) / ctx["close"]
        if upper_room < min_upper_room:
            return self._log_1030_reject(
                event,
                "reject_v8b2_upper_room_small",
                upper_room_pct=str(upper_room),
                limit=str(min_upper_room),
            )

        if not ctx["green_bar"]:
            return self._log_1030_reject(event, "reject_v8b2_not_green")

        allowed, addition = self._can_open_position(event.symbol, df, session, event.bar_end)
        if not allowed:
            return None
        reason = self._reversal_1030_reason(
            ctx,
            entry_mode=entry_mode,
            addition=addition,
            extra={
                "quality_profile": quality_profile,
                "signal_type": "boll_mid_reclaim_only",
                "reclaim_type": "boll_mid",
                "reclaim_level": str(reclaim_level),
                "reclaim_distance_pct": str(reclaim_distance),
                "upper_room_pct": str(upper_room),
                "min_upper_room_pct": str(min_upper_room),
            },
        )
        return self._build_signal(
            symbol=event.symbol,
            signal_code="SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
            side=SignalSide.BUY,
            reason=reason,
            generated_at=event.bar_end,
            ttl_override=min(int(self._ttl), 90),
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
            "option_liquidity": self._liquidity_snapshot.get(event.symbol, {"pass": True}),
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
            "option_liquidity": self._liquidity_snapshot.get(event.symbol, {"pass": True}),
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
            "option_liquidity": self._liquidity_snapshot.get(event.symbol, {"pass": True}),
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
        if state is None or state.signal_code not in {"SIG_OPEN_CHASE_BUY", "SIG_OPEN_CHASE_ORB_BUY"}:
            return None
        if getattr(self, "_separated_exit_enabled", False):
            opened_at = _to_eastern(state.opened_at)
            held_seconds = (_to_eastern(event.bar_end) - opened_at).total_seconds()
            min_hold = int(
                getattr(self._settings, "open_chase_min_hold_seconds_before_upper_tap_exit", 300)
            )
            if held_seconds < min_hold:
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
        if getattr(self, "_separated_exit_enabled", False):
            current = df.iloc[-1]
            opened_at = _to_eastern(state.opened_at)
            entry_rows = df[df["et"] <= opened_at]
            if entry_rows.empty:
                return None
            entry_close = _to_decimal(entry_rows.iloc[-1]["close"])
            current_close = _to_decimal(current["close"])
            if (
                bool(getattr(self._settings, "open_chase_require_profit_for_upper_tap_exit", True))
                and current_close <= entry_close
            ):
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
            signal_code=(
                "OPEN_CHASE_UPPER_TAP_PROFIT_TAKE"
                if getattr(self, "_separated_exit_enabled", False)
                else "SIG_EXIT_UPPER_TAP_X2"
            ),
            side=SignalSide.SELL,
            reason=reason,
            generated_at=event.bar_end,
        )

    def _detect_1030_reversal_exit(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
        session: Session,
    ) -> Optional[SignalEnvelope]:
        state = self._positions.get(event.symbol)
        reversal_buy_codes = {
            "SIG_1030_REVERSAL_CALL_BUY",
            "SIG_1030_V8A_LOW_REVERSAL_CALL_BUY",
            "SIG_1030_V8B_RECLAIM_CALL_BUY",
            "SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
        }
        if state is None or state.signal_code not in reversal_buy_codes:
            return None
        if len(df) < 2:
            return None
        current = df.iloc[-1]
        close = _to_decimal(current.get("close"))
        low = _to_decimal(current.get("low"))
        high = _to_decimal(current.get("high"))
        boll_up = _to_decimal(current.get("boll_up"))
        boll_mid = _to_decimal(current.get("boll_mid"))
        if close <= 0 or high <= 0:
            return None
        et_time = _to_eastern(event.bar_end)
        entry = self._reversal_1030_entries.get(event.symbol)
        entry_mode_for_exit = str(entry.get("entry_mode") or "") if entry else ""
        v8_guard_entry_low_stop_enabled = bool(
            entry
            and state.signal_code == "SIG_1030_REVERSAL_CALL_BUY"
            and entry.get("v8_guard_entry_low_stop_enabled")
        )
        v8_guard_followthrough_enabled = bool(
            entry
            and state.signal_code == "SIG_1030_REVERSAL_CALL_BUY"
            and entry_mode_for_exit == "v8_guard_filter_followthrough"
        )
        v8_guard_continuation_enabled = bool(
            entry
            and state.signal_code == "SIG_1030_REVERSAL_CALL_BUY"
            and entry_mode_for_exit in {"v8_guard_continuation", "v8_guard_continuation_v2"}
        )
        if entry and (
            state.signal_code != "SIG_1030_REVERSAL_CALL_BUY"
            or v8_guard_entry_low_stop_enabled
            or v8_guard_followthrough_enabled
            or v8_guard_continuation_enabled
        ):
            entry_close = _to_decimal(entry.get("entry_close"))
            entry_mode = str(entry.get("entry_mode") or "")
            if entry_close > 0 and close > entry_close:
                entry["had_positive_unrealized"] = True
            if entry_close > 0:
                current_mfe = _to_decimal(entry.get("max_unrealized_pct"))
                if current_mfe < 0:
                    current_mfe = Decimal("0")
                if high > 0:
                    high_unrealized = (high - entry_close) / entry_close
                    if high_unrealized > current_mfe:
                        entry["max_unrealized_pct"] = str(high_unrealized)
                        current_mfe = high_unrealized
                if entry_mode in {"v8_guard_continuation", "v8_guard_continuation_v2"}:
                    target_profit = Decimal(
                        str(
                            getattr(
                                self._settings,
                                "reversal_1030_continuation_min_exit_profit_pct",
                                Decimal("0.0030"),
                            )
                        )
                    )
                    if high >= entry_close * (Decimal("1") + target_profit):
                        return self._build_signal(
                            symbol=event.symbol,
                            signal_code="SIG_1030_REVERSAL_CONTINUATION_TARGET_EXIT",
                            side=SignalSide.SELL,
                            reason={
                                "exit_reason": "reversal_1030_continuation_target",
                                "entry_mode": entry_mode,
                                "target_profit_pct": str(target_profit),
                                "entry_close": str(entry_close),
                                "close": str(close),
                                "high": str(high),
                            },
                            generated_at=event.bar_end,
                            ttl_override=min(int(self._ttl), 90),
                        )

            entry_low = _to_decimal(entry.get("entry_bar_low"))
            stop_buffer = Decimal(
                str(
                    getattr(
                        self._settings,
                        "reversal_1030_failure_stop_entry_low_buffer_pct",
                        Decimal("0.0005"),
                    )
                )
            )
            if (
                (
                    state.signal_code != "SIG_1030_REVERSAL_CALL_BUY"
                    or v8_guard_entry_low_stop_enabled
                )
                and entry_low > 0
                and low <= entry_low * (Decimal("1") - stop_buffer)
            ):
                return self._build_signal(
                    symbol=event.symbol,
                    signal_code="SIG_1030_REVERSAL_FAILURE_ENTRY_LOW_EXIT",
                    side=SignalSide.SELL,
                    reason={
                        "exit_reason": "reversal_1030_entry_low_failure",
                        "entry_mode": entry.get("entry_mode"),
                            "entry_bar_low": str(entry_low),
                            "stop_buffer_pct": str(stop_buffer),
                            "close": str(close),
                            "low": str(low),
                            "v8_guard_entry_low_stop_enabled": v8_guard_entry_low_stop_enabled,
                        },
                    generated_at=event.bar_end,
                    ttl_override=min(int(self._ttl), 90),
                )

            if entry_mode in {
                "v8b3_mid_reclaim_followthrough",
                "v8_guard_filter_followthrough",
            } and entry_close > 0:
                opened_at_raw = entry.get("opened_at")
                opened_at = opened_at_raw if isinstance(opened_at_raw, datetime) else state.opened_at
                held_seconds = max(0, int((event.bar_end - opened_at).total_seconds()))
                check_minutes = int(
                    getattr(
                        self._settings,
                        "reversal_1030_early_followthrough_check_minutes",
                        3,
                    )
                )
                min_mfe = Decimal(
                    str(
                        getattr(
                            self._settings,
                            "reversal_1030_early_followthrough_min_mfe_pct",
                            Decimal("0.0010"),
                        )
                    )
                )
                if (
                    held_seconds >= check_minutes * 60
                    and current_mfe < min_mfe
                    and close <= entry_close
                    and boll_mid > 0
                    and close < boll_mid
                ):
                    return self._build_signal(
                        symbol=event.symbol,
                        signal_code="SIG_1030_REVERSAL_EARLY_NO_FOLLOW_THROUGH_EXIT",
                        side=SignalSide.SELL,
                        reason={
                            "exit_reason": "reversal_1030_early_no_follow_through",
                            "entry_mode": entry_mode,
                            "check_minutes": check_minutes,
                            "held_seconds": held_seconds,
                            "max_unrealized_pct": str(current_mfe),
                            "min_mfe_pct": str(min_mfe),
                            "entry_close": str(entry_close),
                            "close": str(close),
                            "boll_mid": str(boll_mid),
                        },
                        generated_at=event.bar_end,
                        ttl_override=min(int(self._ttl), 90),
                    )

            reclaim_lost_modes = {
                "v8b_reclaim",
                "v8b2_mid_reclaim_quality",
                "v8b3_mid_reclaim_followthrough",
            }
            if entry.get("had_positive_unrealized") and entry_mode in reclaim_lost_modes:
                entry_vwap = _to_decimal(entry.get("entry_vwap"))
                entry_boll_mid = _to_decimal(entry.get("entry_boll_mid"))
                reclaim_level = None
                reclaim_name = None
                if entry_mode in reclaim_lost_modes and str(entry.get("reclaim_type")) == "boll_mid":
                    reclaim_level = boll_mid if boll_mid > 0 else entry_boll_mid
                    reclaim_name = "boll_mid"
                elif entry_vwap > 0:
                    reclaim_level = entry_vwap
                    reclaim_name = "vwap"
                elif entry_boll_mid > 0:
                    reclaim_level = boll_mid if boll_mid > 0 else entry_boll_mid
                    reclaim_name = "boll_mid"
                lost_buffer = Decimal("0")
                if entry_mode in {"v8b2_mid_reclaim_quality", "v8b3_mid_reclaim_followthrough"}:
                    lost_buffer = Decimal(
                        str(
                            getattr(
                                self._settings,
                                "reversal_1030_reclaim_lost_buffer_pct",
                                Decimal("0.0005"),
                            )
                        )
                    )
                if (
                    reclaim_level is not None
                    and reclaim_level > 0
                    and close < reclaim_level * (Decimal("1") - lost_buffer)
                ):
                    return self._build_signal(
                        symbol=event.symbol,
                        signal_code="SIG_1030_REVERSAL_FAILURE_RECLAIM_LOST_EXIT",
                        side=SignalSide.SELL,
                        reason={
                            "exit_reason": "reversal_1030_reclaim_lost_after_profit",
                            "entry_mode": entry_mode,
                            "reclaim_type": reclaim_name,
                            "reclaim_level": str(reclaim_level),
                            "lost_buffer_pct": str(lost_buffer),
                            "close": str(close),
                            "had_positive_unrealized": True,
                        },
                        generated_at=event.bar_end,
                        ttl_override=min(int(self._ttl), 90),
                    )
        if boll_up > 0 and high >= boll_up:
            if entry and entry_mode_for_exit in {"v8_guard_continuation", "v8_guard_continuation_v2"}:
                entry_close = _to_decimal(entry.get("entry_close"))
                entry_vwap = _to_decimal(entry.get("entry_vwap"))
                fail_buffer = Decimal(
                    str(
                        getattr(
                            self._settings,
                            "reversal_1030_continuation_fail_buffer_pct",
                            Decimal("0.0003"),
                        )
                    )
                )
                max_vwap_distance = Decimal(
                    str(
                        getattr(
                            self._settings,
                            "reversal_1030_continuation_max_vwap_distance_pct",
                            Decimal("0.0075"),
                        )
                    )
                )
                vwap_distance = (
                    (entry_close - entry_vwap) / entry_vwap
                    if entry_vwap > 0 and entry_close > 0
                    else None
                )
                continuation_active = bool(entry.get("continuation_active"))
                qualifies = bool(
                    entry_close > 0
                    and close > entry_close
                    and entry_vwap > 0
                    and entry_close > entry_vwap
                    and vwap_distance is not None
                    and vwap_distance <= max_vwap_distance
                    and (boll_mid <= 0 or close >= boll_mid * (Decimal("1") - fail_buffer))
                    and close >= entry_vwap * (Decimal("1") - fail_buffer)
                )
                if qualifies and not continuation_active:
                    entry["continuation_active"] = True
                    entry["first_boll_up_tap_ts"] = event.bar_end.isoformat()
                    entry["first_boll_up_tap_close"] = str(close)
                    return None
                if continuation_active:
                    if (
                        boll_mid > 0
                        and close < boll_mid * (Decimal("1") - fail_buffer)
                    ) or close < entry_vwap * (Decimal("1") - fail_buffer):
                        return self._build_signal(
                            symbol=event.symbol,
                            signal_code="SIG_1030_REVERSAL_CONTINUATION_STRUCTURE_LOST_EXIT",
                            side=SignalSide.SELL,
                            reason={
                                "exit_reason": "reversal_1030_continuation_structure_lost",
                                "entry_mode": entry_mode_for_exit,
                                "entry_close": str(entry_close),
                                "entry_vwap": str(entry_vwap),
                                "fail_buffer_pct": str(fail_buffer),
                                "close": str(close),
                                "boll_mid": str(boll_mid) if boll_mid > 0 else None,
                            },
                            generated_at=event.bar_end,
                            ttl_override=min(int(self._ttl), 90),
                        )
                    return None
            signal_code = "SIG_1030_REVERSAL_BOLL_UP_EXIT"
            exit_reason = "underlying_tapped_1m_boll_upper"
            if entry and str(entry.get("entry_mode") or "") == "v8b3_mid_reclaim_followthrough":
                entry_close = _to_decimal(entry.get("entry_close"))
                true_profit_min = Decimal(
                    str(
                        getattr(
                            self._settings,
                            "reversal_1030_boll_up_true_profit_min_pct",
                            Decimal("0"),
                        )
                    )
                )
                true_exit_level = entry_close * (Decimal("1") + true_profit_min)
                if entry_close > 0 and close > true_exit_level:
                    signal_code = "SIG_1030_REVERSAL_BOLL_UP_TRUE_EXIT"
                    exit_reason = "underlying_tapped_1m_boll_upper_true_profit"
                elif entry_close > 0 and close <= entry_close:
                    signal_code = "SIG_1030_REVERSAL_BOLL_UP_REBOUND_FAILURE_EXIT"
                    exit_reason = "underlying_tapped_lowered_boll_upper_after_failure"
                else:
                    signal_code = "SIG_1030_REVERSAL_BOLL_UP_WEAK_EXIT"
                    exit_reason = "underlying_tapped_1m_boll_upper_weak_profit"
            return self._build_signal(
                symbol=event.symbol,
                signal_code=signal_code,
                side=SignalSide.SELL,
                reason={
                    "exit_reason": exit_reason,
                    "entry_mode": entry.get("entry_mode") if entry else None,
                    "entry_close": str(_to_decimal(entry.get("entry_close"))) if entry else None,
                    "close": str(close),
                    "high": str(high),
                    "boll_up": str(boll_up),
                    "boll_mid": str(boll_mid) if boll_mid > 0 else None,
                },
                generated_at=event.bar_end,
                ttl_override=min(int(self._ttl), 90),
            )
        deadline = self._parse_hhmm(
            str(getattr(self._settings, "reversal_1030_exit_deadline", "12:00"))
        )
        if et_time.time() >= deadline:
            return self._build_signal(
                symbol=event.symbol,
                signal_code="SIG_1030_REVERSAL_TIME_EXIT",
                side=SignalSide.SELL,
                reason={
                    "exit_reason": "reversal_1030_deadline",
                    "deadline": deadline.isoformat(timespec="minutes"),
                    "close": str(close),
                    "boll_mid": str(boll_mid) if boll_mid > 0 else None,
                    "boll_up": str(boll_up) if boll_up > 0 else None,
                },
                generated_at=event.bar_end,
                ttl_override=min(int(self._ttl), 90),
            )
        return None

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
            state.signal_code in {"SIG_OPEN_CHASE_BUY", "SIG_OPEN_CHASE_ORB_BUY"}
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
            self._detect_1030_reversal_exit,
            self._detect_s2,
            self._detect_s1,
            self._detect_time_clear,
            self._detect_open_chase_orb,
            self._detect_1030_reversal_call,
            self._detect_1030_v8a_low_reversal_call,
            self._detect_1030_v8b_reclaim_call,
            self._detect_1030_v8b2_mid_reclaim_quality_call,
            self._detect_e1,
            self._detect_e2,
            self._detect_pm_a2,
            self._detect_pm_a3,
            self._detect_pm_a4,
            self._detect_am_bottom_a1,
            self._detect_am_confluence_buy_a2,
        ]
        if getattr(self, "_reversal_1030_v8a_enabled", False) or getattr(
            self, "_reversal_1030_v8b_enabled", False
        ) or getattr(
            self, "_reversal_1030_v8b2_enabled", False
        ):
            detectors = [
                self._detect_1030_reversal_exit,
                self._detect_time_clear,
            ]
            if getattr(self, "_reversal_1030_v8a_enabled", False):
                detectors.append(self._detect_1030_v8a_low_reversal_call)
            if getattr(self, "_reversal_1030_v8b_enabled", False):
                detectors.append(self._detect_1030_v8b_reclaim_call)
            if getattr(self, "_reversal_1030_v8b2_enabled", False):
                detectors.append(self._detect_1030_v8b2_mid_reclaim_quality_call)

        detected: List[DetectedSignal] = []
        for detector in detectors:
            signal = detector(event, df, trade_date, vix_value, session)
            if signal is None:
                continue
            if signal.side == SignalSide.BUY:
                if not self._is_1030_reversal_buy(signal.signal_code) and not self._passes_buy_priority(trade_date, signal):
                    if self._metrics_enabled:
                        try:
                            record_signal_filtered("PRIORITY_FILTER")
                        except Exception:
                            pass
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
        payload["status"] = "EMITTED"
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
            "reversal_1030_entries": copy.deepcopy(
                getattr(self, "_reversal_1030_entries", {})
            ),
        }

    def _restore_state(self, snapshot: Dict[str, Any]) -> None:
        self._positions = snapshot["positions"]
        self._upper_break = snapshot["upper_break"]
        self._last_signal_ts = snapshot["last_signal_ts"]
        self._reversal_1030_entries = snapshot.get("reversal_1030_entries", {})

    def _daily_open_price(
        self,
        session: Optional[Session],
        symbol: str,
        trade_date: date,
    ) -> Optional[Decimal]:
        return self._fallback_daily_field_value(
            session,
            symbol,
            trade_date,
            field_name="open_rth",
        )

    def _fallback_daily_field_value(
        self,
        session: Optional[Session],
        symbol: str,
        trade_date: date,
        *,
        field_name: str,
    ) -> Optional[Decimal]:
        if session is None:
            return None
        try:
            row = session.execute(
                text(
                    f"""
                    SELECT {field_name}
                    FROM v_daily_ohlcv_enriched
                    WHERE symbol = :symbol AND trade_date_et::date = :trade_date
                    """
                ),
                {"symbol": symbol, "trade_date": trade_date},
            ).fetchone()
        except Exception:
            LOGGER.exception(
                "signal_engine.daily_field_query_failed",
                symbol=symbol,
                trade_date=str(trade_date),
                field_name=field_name,
            )
            row = None
        if row and row[0] is not None:
            try:
                return Decimal(str(row[0]))
            except (InvalidOperation, TypeError):
                return None
        if field_name == "prev_close_rth":
            return self._previous_rth_close_from_bars(session, symbol, trade_date)
        if field_name == "close_rth":
            return self._rth_close_from_bars(session, symbol, trade_date)
        if field_name == "open_rth":
            return self._rth_open_from_bars(session, symbol, trade_date)
        return None

    def _previous_rth_close_from_bars(
        self,
        session: Optional[Session],
        symbol: str,
        trade_date: date,
    ) -> Optional[Decimal]:
        if session is None:
            return None
        try:
            row = session.execute(
                text(
                    """
                    SELECT close
                    FROM bars1m_equity
                    WHERE symbol = :symbol
                      AND (ts_end AT TIME ZONE 'America/New_York')::date < :trade_date
                      AND (ts_end AT TIME ZONE 'America/New_York')::time <= TIME '16:00'
                    ORDER BY ts_end DESC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol, "trade_date": trade_date},
            ).fetchone()
        except Exception:
            LOGGER.exception(
                "signal_engine.prev_close_bars_query_failed",
                symbol=symbol,
                trade_date=str(trade_date),
            )
            return None
        if not row or row[0] is None:
            return None
        try:
            return Decimal(str(row[0]))
        except (InvalidOperation, TypeError):
            return None

    def _rth_close_from_bars(
        self,
        session: Optional[Session],
        symbol: str,
        trade_date: date,
    ) -> Optional[Decimal]:
        if session is None:
            return None
        try:
            row = session.execute(
                text(
                    """
                    SELECT close
                    FROM bars1m_equity
                    WHERE symbol = :symbol
                      AND (ts_end AT TIME ZONE 'America/New_York')::date = :trade_date
                      AND (ts_end AT TIME ZONE 'America/New_York')::time <= TIME '16:00'
                    ORDER BY ts_end DESC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol, "trade_date": trade_date},
            ).fetchone()
        except Exception:
            LOGGER.exception(
                "signal_engine.close_bars_query_failed",
                symbol=symbol,
                trade_date=str(trade_date),
            )
            return None
        if not row or row[0] is None:
            return None
        try:
            return Decimal(str(row[0]))
        except (InvalidOperation, TypeError):
            return None

    def _rth_open_from_bars(
        self,
        session: Optional[Session],
        symbol: str,
        trade_date: date,
    ) -> Optional[Decimal]:
        if session is None:
            return None
        try:
            row = session.execute(
                text(
                    """
                    SELECT open
                    FROM bars1m_equity
                    WHERE symbol = :symbol
                      AND (ts_end AT TIME ZONE 'America/New_York')::date = :trade_date
                      AND (ts_end AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
                    ORDER BY ts_end ASC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol, "trade_date": trade_date},
            ).fetchone()
        except Exception:
            LOGGER.exception(
                "signal_engine.open_bars_query_failed",
                symbol=symbol,
                trade_date=str(trade_date),
            )
            return None
        if not row or row[0] is None:
            return None
        try:
            return Decimal(str(row[0]))
        except (InvalidOperation, TypeError):
            return None

    def _daily_close_price(
        self,
        session: Optional[Session],
        symbol: str,
        trade_date: date,
    ) -> Optional[Decimal]:
        return self._fallback_daily_field_value(
            session,
            symbol,
            trade_date,
            field_name="close_rth",
        )

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

    def _day_close_price(self, df: pd.DataFrame, trade_date: date) -> Optional[Decimal]:
        if df is None or df.empty:
            return None
        if "et" in df.columns:
            et_series = df["et"]
        else:
            et_series = df["ts_end"].dt.tz_convert("US/Eastern")
        mask = et_series.dt.date == trade_date
        if not mask.any():
            return None
        rows = df.loc[mask]
        if rows.empty:
            return None
        close_value = rows.iloc[-1].get("close")
        if close_value is None:
            return None
        try:
            return Decimal(str(close_value))
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
                    WHERE symbol = :symbol
                      AND trade_date_et::date <= :trade_date
                      AND sma60 IS NOT NULL
                    ORDER BY trade_date_et DESC
                    LIMIT 1
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

    def _option_symbol_column(self, session: Session, table_name: str = "bars1m_option") -> str:
        """Return the underlying symbol column used by the configured option bar table."""
        cache = getattr(self, "_option_symbol_columns", None)
        if cache is None:
            cache = {}
            self._option_symbol_columns = cache
        if table_name in cache:
            return cache[table_name]
        column_name = "symbol"
        try:
            bind = session.get_bind()
            columns = {col["name"] for col in sa_inspect(bind).get_columns(table_name)}
            if "underlying_symbol" in columns:
                column_name = "underlying_symbol"
            elif "symbol" in columns:
                column_name = "symbol"
        except Exception:
            column_name = "symbol"
        cache[table_name] = column_name
        return column_name

    def _can_open_position(
        self,
        symbol: str,
        df: Optional[pd.DataFrame] = None,
        session: Optional[Session] = None,
        ts_end: Optional[datetime] = None,
    ) -> Tuple[bool, bool]:
        LOGGER.info(
            "position_check.start",
            symbol=symbol,
            has_df=df is not None,
            has_session=session is not None,
            has_ts_end=ts_end is not None,
            is_backtest=self._is_backtest,
        )
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
            LOGGER.info(
                "position_check.preearn",
                symbol=symbol,
                trade_date=str(trade_date),
                preearn_allowed=preearn_allowed
            )
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
                LOGGER.info(
                    "position_check.ma60_no_close",
                    symbol=symbol,
                    ts_end=str(ts_end),
                    reason="无法获取当前收盘价"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("MA60_FILTER")
                except Exception:
                    pass
                return False, False
            ma60_value = self._daily_ma60_value(session, symbol, ts_end)
            LOGGER.info(
                "position_check.ma60",
                symbol=symbol,
                current_close=str(current_close),
                ma60_value=str(ma60_value) if ma60_value is not None else "None",
                passed=bool(ma60_value is not None and current_close > ma60_value)
            )
            if ma60_value is None or current_close <= ma60_value:
                LOGGER.info(
                    "position_check.ma60_blocked",
                    symbol=symbol,
                    ts_end=str(ts_end),
                    close=str(current_close),
                    sma60=str(ma60_value) if ma60_value is not None else "None",
                    reason="MA60过滤：当前价格未突破MA60" if ma60_value else "MA60数据缺失"
                )
                try:
                    if self._metrics_enabled:
                        record_signal_filtered("MA60_FILTER")
                except Exception:
                    pass
                return False, False

        state = self._positions.get(symbol)
        if state is None:
            LOGGER.info(
                "position_check.new_position",
                symbol=symbol,
                current_positions=len(self._positions),
                max_positions=int(self._settings.max_concurrent_top)
            )
            if len(self._positions) >= int(self._settings.max_concurrent_top):
                LOGGER.info(
                    "position_check.max_positions",
                    symbol=symbol,
                    current=len(self._positions),
                    max=int(self._settings.max_concurrent_top),
                    reason="已达到最大持仓数量限制"
                )
                return False, False
            LOGGER.info(
                "position_check.allowed",
                symbol=symbol,
                reason="允许开仓"
            )
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
        return True, True

    def _option_liquidity_pass(
        self, session: Session, symbol: str, ts_end: datetime
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        读取截至 signal 时刻的最新 option quote 判定期权流动性，返回 (是否通过, 详情字典)。
        """
        if not getattr(self, "_option_liquidity_required", True):
            return True, {"pass": True, "reason": "disabled_by_config"}
        window_start = ts_end - timedelta(minutes=15)
        try:
            rows = self._option_quote_candidates(session, symbol, ts_end, window_start=window_start)
        except Exception:
            LOGGER.exception("signal_engine.option_liquidity_query_failed", symbol=symbol)
            return False, {
                "pass": False,
                "reason": "query_failed",
                "window_start": window_start.isoformat(),
                "window_end": ts_end.isoformat(),
            }
        if not rows:
            LOGGER.info(
                "signal_engine.option_liquidity_empty",
                symbol=symbol,
                window_start=window_start.isoformat(),
                window_end=ts_end.isoformat(),
            )
            return False, {
                "pass": False,
                "reason": "no_option_ticks",
                "window_start": window_start.isoformat(),
                "window_end": ts_end.isoformat(),
            }

        trade_date = ts_end.astimezone(EASTERN).date()
        selected: Optional[Dict[str, Any]] = None
        fallback_selected: Optional[Dict[str, Any]] = None
        for row in rows:
            bid = row.get("bid")
            ask = row.get("ask")
            volume = row.get("volume")
            oi = row.get("open_interest")
            expiry = row.get("expiry")
            strike = row.get("strike")
            ts_sample = row.get("ts_end")
            if None in (bid, ask, volume, oi, expiry, strike):
                continue
            try:
                bid_dec = Decimal(str(bid))
                ask_dec = Decimal(str(ask))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if bid_dec <= 0 or ask_dec <= 0 or ask_dec <= bid_dec:
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
            mid = (bid_dec + ask_dec) / Decimal("2")
            if mid <= 0:
                continue
            spread = ask_dec - bid_dec
            threshold = max(Decimal("0.10"), mid * Decimal("0.05"))
            if spread > threshold:
                continue
            candidate = {
                "pass": True,
                "bid": float(bid_dec),
                "ask": float(ask_dec),
                "spread": float(spread),
                "threshold": float(threshold),
                "volume": int(volume),
                "open_interest": int(oi),
                "strike": float(Decimal(str(strike))),
                "expiry": expiry_date.isoformat(),
                "dte": int(dte),
                "ts_sample": ts_sample.isoformat() if isinstance(ts_sample, datetime) else str(ts_sample),
            }
            quote_age_seconds = None
            if isinstance(ts_sample, datetime):
                quote_age_seconds = max(0.0, (ts_end - ts_sample).total_seconds())
                candidate["quote_age_seconds"] = quote_age_seconds
            if quote_age_seconds is not None and quote_age_seconds > OPTION_QUOTE_MAX_AGE_SECONDS:
                continue
            allow_missing_metrics = getattr(
                self, "_allow_missing_option_liquidity_metrics", False
            )
            if allow_missing_metrics and int(volume) == 0 and int(oi) == 0:
                fallback_candidate = dict(candidate)
                fallback_candidate["reason"] = "quote_only_missing_oi_volume"
                fallback_candidate["missing_metrics_fallback"] = True
                if (
                    fallback_selected is None
                    or fallback_candidate["spread"] < fallback_selected.get("spread", float("inf"))
                ):
                    fallback_selected = fallback_candidate
            if int(volume) < 100 or int(oi) < 500:
                continue
            if selected is None or candidate["volume"] > selected.get("volume", 0):
                selected = candidate

        if selected is None and fallback_selected is not None:
            LOGGER.info(
                "signal_engine.option_liquidity_fallback",
                symbol=symbol,
                window_start=window_start.isoformat(),
                window_end=ts_end.isoformat(),
                snapshot=fallback_selected,
            )
            return True, fallback_selected

        if selected is None:
            LOGGER.info(
                "signal_engine.option_liquidity_blocked",
                symbol=symbol,
                window_start=window_start.isoformat(),
                window_end=ts_end.isoformat(),
            )
            return False, {
                "pass": False,
                "reason": "threshold_not_met",
                "window_start": window_start.isoformat(),
                "window_end": ts_end.isoformat(),
            }

        LOGGER.debug(
            "signal_engine.option_liquidity_pass",
            symbol=symbol,
            snapshot=selected,
        )
        return True, selected

    def _option_quote_candidates(
        self,
        session: Session,
        symbol: str,
        ts_end: datetime,
        *,
        window_start: datetime | None = None,
    ) -> List[Dict[str, Any]]:
        symbol_column = self._option_symbol_column(session)
        lower_bound = window_start or (ts_end - timedelta(minutes=15))
        stmt = text(
            f"""
            WITH ranked AS (
                SELECT
                    conid,
                    bid,
                    ask,
                    volume,
                    open_interest,
                    expiry,
                    strike,
                    ts_end,
                    underlying_price,
                    delta,
                    COALESCE(mid, (bid + ask) / 2, last) AS current_price,
                    ROW_NUMBER() OVER (PARTITION BY conid ORDER BY ts_end DESC) AS rn
                FROM bars1m_option
                WHERE {symbol_column} = :symbol
                  AND "right" = 'CALL'
                  AND ts_end BETWEEN :start_ts AND :end_ts
                  AND COALESCE(mid, (bid + ask) / 2, last) IS NOT NULL
            )
            SELECT
                conid,
                bid,
                ask,
                volume,
                open_interest,
                expiry,
                strike,
                ts_end,
                underlying_price,
                delta,
                current_price
            FROM ranked
            WHERE rn = 1
            ORDER BY ts_end DESC, (volume IS NULL) ASC, volume DESC, conid ASC
            LIMIT 200
            """
        )
        return session.execute(
            stmt,
            {"symbol": symbol, "start_ts": lower_bound, "end_ts": ts_end},
        ).mappings().all()

    def _risk_hint_for_signal(
        self,
        symbol: str,
        signal_code: str,
        side: SignalSide,
        reason: Dict[str, Any],
    ) -> Dict[str, Any]:
        """依据信号类型设定风险暴露，默认以 R 倍数描述仓位大小。"""
        hint = dict(RISK_HINT)
        if side == SignalSide.BUY:
            size_map = {
                "SIG_OPEN_CHASE_BUY": "2R",
                "SIG_OPEN_CHASE_ORB_BUY": "2R",
                "SIG_1030_REVERSAL_CALL_BUY": "1R",
                "SIG_1030_V8A_LOW_REVERSAL_CALL_BUY": "1R",
                "SIG_1030_V8B_RECLAIM_CALL_BUY": "1R",
                "SIG_1030_BOLL_MID_RECLAIM_CALL_BUY": "1R",
                "SIG_PM_BOTTOM_A4": "2R",
            }
            hint["size"] = size_map.get(signal_code, "1R")
        else:
            hint["size"] = "exit"
        priority_source = reason.get("rank_score") or reason.get("rebound") or reason.get("bounce")
        if priority_source is not None:
            hint["priority"] = priority_source
        return hint

    def _option_hint_for_signal(
        self,
        symbol: str,
        signal_code: str,
        side: SignalSide,
        reason: Dict[str, Any],
    ) -> Dict[str, Any]:
        """组合期权筛选提示，例如优先级指标与最近流动性快照。"""
        hint: Dict[str, Any] = dict(OPTION_HINT)
        if side == SignalSide.SELL:
            hint["direction"] = "reduce"
            return hint
        priority_metric = None
        priority_score = None
        for key in ("rank_score", "rank_metric", "boll_dn_slope", "rebound", "bounce"):
            if key in reason:
                priority_metric = key
                priority_score = reason.get(key)
                break
        if priority_metric is not None:
            hint["priority_metric"] = priority_metric
            hint["priority_score"] = priority_score
        liquidity = self._liquidity_snapshot.get(symbol)
        if liquidity:
            hint["liquidity"] = liquidity
        hint["signal_code"] = signal_code
        if self._is_1030_reversal_buy(signal_code):
            hint["strategy_family"] = "1030_reversal"
            hint["dte"] = [0, 7]
            hint["delta"] = [0.35, 0.55]
            hint["exit_plan"] = reason.get("exit_plan")
        return hint

    @staticmethod
    def _is_1030_reversal_buy(signal_code: str | None) -> bool:
        return signal_code in {
            "SIG_1030_REVERSAL_CALL_BUY",
            "SIG_1030_V8A_LOW_REVERSAL_CALL_BUY",
            "SIG_1030_V8B_RECLAIM_CALL_BUY",
            "SIG_1030_BOLL_MID_RECLAIM_CALL_BUY",
        }

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
            if self._is_1030_reversal_buy(signal.signal_code):
                self._reversal_1030_entries[signal.symbol] = {
                    "entry_mode": signal.reason.get("entry_mode"),
                    "entry_bar_low": signal.reason.get("entry_bar_low"),
                    "entry_close": signal.reason.get("entry_close"),
                    "entry_vwap": signal.reason.get("entry_vwap"),
                    "entry_boll_mid": signal.reason.get("entry_boll_mid"),
                    "reclaim_type": signal.reason.get("reclaim_type"),
                    "quality_profile": signal.reason.get("quality_profile"),
                    "trade_type": signal.reason.get("trade_type"),
                    "v8_guard_triggered": signal.reason.get("v8_guard_triggered"),
                    "v8_guard_entry_low_stop_enabled": signal.reason.get(
                        "v8_guard_entry_low_stop_enabled"
                    ),
                    "opened_at": ts_end,
                    "had_positive_unrealized": False,
                    "max_unrealized_pct": "0",
                }
        elif signal.signal_code in SELL_SIGNALS:
            self._positions.pop(signal.symbol, None)
            self._upper_break.pop(signal.symbol, None)
            self._reversal_1030_entries.pop(signal.symbol, None)

    def _persist_signal(self, session: Session, signal: SignalEnvelope, ts_end: datetime) -> None:
        log_dao = SignalLogDAO(session)
        log_dao.record_signal(
            run_id=self._run_id,
            symbol=signal.symbol,
            asset_type=signal.asset_type.value,
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
        # 基于信号类型构建更细化的风险提示与期权筛选提示
        risk_hint = self._risk_hint_for_signal(symbol, signal_code, side, reason)
        option_hint = self._option_hint_for_signal(symbol, signal_code, side, reason)
        LOGGER.debug(
            "signal_engine.hints_composed",
            symbol=symbol,
            signal_code=signal_code,
            risk_hint=risk_hint,
            option_hint=option_hint,
        )
        return SignalEnvelope(
            strategy_code=self._strategy_code,
            symbol=symbol,
            signal_code=signal_code,
            side=side,
            confidence=1.0,
            reason=reason,
            risk_hint=risk_hint,
            option_hint=option_hint,
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

    def _passes_buy_priority(self, trade_date: date, signal: SignalEnvelope) -> bool:
        if self._buy_priority_slots <= 0:
            return True
        # 清理过期缓存（仅保留最近3个交易日）
        if len(self._buy_priority) > 4:
            for cached_date in list(self._buy_priority.keys()):
                if (trade_date - cached_date).days > 3:
                    self._buy_priority.pop(cached_date, None)

        ranking = self._buy_priority.setdefault(trade_date, {})
        symbol = signal.symbol.upper()
        score = self._extract_rank_score(signal)
        if score is None:
            score = Decimal("-Infinity")

        ranking[symbol] = score
        sorted_items = sorted(ranking.items(), key=lambda item: item[1], reverse=True)
        top_symbols = {sym for sym, _ in sorted_items[: self._buy_priority_slots]}
        # 仅保留排名前N的缓存，防止无限增长
        self._buy_priority[trade_date] = {
            sym: val for sym, val in sorted_items[: self._buy_priority_slots]
        }
        if symbol in top_symbols:
            return True

        LOGGER.info(
            "signal_engine.buy_priority_filtered",
            symbol=symbol,
            trade_date=str(trade_date),
            score=str(score),
            slots=self._buy_priority_slots,
        )
        return False

    @staticmethod
    def _extract_rank_score(signal: SignalEnvelope) -> Optional[Decimal]:
        def _to_decimal(value: object) -> Optional[Decimal]:
            if value is None:
                return None
            if isinstance(value, Decimal):
                return value
            if isinstance(value, (int, float)):
                return Decimal(str(value))
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    return None
                try:
                    return Decimal(stripped)
                except (InvalidOperation, ValueError):
                    return None
            try:
                return Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                return None

        reason = signal.reason or {}
        for key in ("rank_score", "boll_dn_slope", "dn_slope"):
            score = _to_decimal(reason.get(key))
            if score is not None:
                return score

        option_hint = signal.option_hint or {}
        return _to_decimal(option_hint.get("priority_score"))

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

    def _opening_range_from_df(self, todays: pd.DataFrame) -> tuple[Decimal | None, Decimal | None]:
        minutes = int(getattr(self._settings, "open_chase_opening_range_minutes", 5))
        if todays.empty:
            return None, None
        start_time = time(9, 30)
        end_dt = datetime.combine(date.today(), start_time) + timedelta(minutes=max(1, minutes) - 1)
        end_time = end_dt.time()
        window = todays[(todays["et"].dt.time >= start_time) & (todays["et"].dt.time <= end_time)]
        if window.empty:
            return None, None
        return _to_decimal(window["high"].max()), _to_decimal(window["low"].min())

    @staticmethod
    def _vwap_from_df(todays: pd.DataFrame) -> Decimal | None:
        if todays.empty:
            return None
        frame = todays.dropna(subset=["close", "volume"])
        if frame.empty:
            return None
        total_volume = sum(_to_decimal(value) for value in frame["volume"])
        if total_volume <= 0:
            return None
        total = sum(
            _to_decimal(row["close"]) * _to_decimal(row["volume"])
            for _, row in frame.iterrows()
        )
        return total / total_volume

    def _reversal_1030_context(
        self,
        event: BarsClosed,
        df: pd.DataFrame,
        trade_date: date,
        vix_value: Optional[Decimal],
    ) -> Dict[str, Any] | None:
        symbol = event.symbol.upper()
        symbols = {
            item.strip().upper()
            for item in str(getattr(self._settings, "reversal_1030_symbols", "")).split(",")
            if item.strip()
        }
        if symbols and symbol not in symbols:
            return None

        et_time = _to_eastern(event.bar_end)
        start_time = self._parse_hhmm(
            str(getattr(self._settings, "reversal_1030_start_time", "10:25"))
        )
        end_time = self._parse_hhmm(
            str(getattr(self._settings, "reversal_1030_end_time", "10:40"))
        )
        if et_time.time() < start_time or et_time.time() > end_time:
            return None

        max_vix = Decimal(str(getattr(self._settings, "reversal_1030_max_vix", Decimal("20"))))
        if vix_value is not None and vix_value > max_vix:
            self._log_1030_reject(event, "reject_vix_high", vix=str(vix_value))
            return None

        todays = df[df["et"].dt.date == trade_date]
        morning = todays[
            (todays["et"].dt.time >= time(9, 30))
            & (todays["et"].dt.time <= et_time.time())
        ]
        if len(morning) < 20:
            return None

        current = morning.iloc[-1]
        prev = morning.iloc[-2]
        session_open = _to_decimal(morning.iloc[0]["open"])
        close = _to_decimal(current.get("close"))
        open_px = _to_decimal(current.get("open"))
        high = _to_decimal(current.get("high"))
        low = _to_decimal(current.get("low"))
        prev_close = _to_decimal(prev.get("close"))
        boll_dn = _to_decimal(current.get("boll_dn"))
        boll_mid = _to_decimal(current.get("boll_mid"))
        boll_up = _to_decimal(current.get("boll_up"))
        rsi = _to_decimal(current.get("rsi6"))
        prev_rsi = _to_decimal(prev.get("rsi6"))
        if session_open <= 0 or close <= 0 or high <= 0 or low <= 0:
            return None
        if boll_dn <= 0 or boll_mid <= 0 or boll_up <= 0 or boll_up <= boll_dn:
            self._log_1030_reject(event, "reject_invalid_bollinger_band")
            return None

        abs_gap_limit = Decimal(
            str(getattr(self._settings, "reversal_1030_max_abs_gap_pct", Decimal("0.06")))
        )
        intraday_move = (close - session_open) / session_open
        if abs(intraday_move) > abs_gap_limit:
            self._log_1030_reject(
                event,
                "reject_abs_move_gt_limit",
                intraday_move=str(intraday_move),
                limit=str(abs_gap_limit),
            )
            return None

        min_drop = Decimal(
            str(getattr(self._settings, "reversal_1030_min_morning_drop_pct", Decimal("0.003")))
        )
        morning_low = _to_decimal(morning["low"].min())
        morning_drop = (session_open - morning_low) / session_open
        if morning_drop < min_drop:
            self._log_1030_reject(
                event,
                "reject_no_morning_selloff",
                morning_drop=str(morning_drop),
                threshold=str(min_drop),
            )
            return None

        lower_buffer = Decimal(
            str(getattr(self._settings, "reversal_1030_lower_band_buffer_pct", Decimal("0.0015")))
        )
        lower_touch = False
        doji_stability_count = 0
        for _, row in morning.tail(8).iterrows():
            row_low = _to_decimal(row.get("low"))
            row_close = _to_decimal(row.get("close"))
            row_open = _to_decimal(row.get("open"))
            row_high = _to_decimal(row.get("high"))
            row_dn = _to_decimal(row.get("boll_dn"))
            if row_dn <= 0:
                continue
            if row_low <= row_dn * (Decimal("1") + lower_buffer):
                lower_touch = True
            bar_range = row_high - row_low
            body = abs(row_close - row_open)
            if (
                row_low <= row_dn * (Decimal("1") + lower_buffer * Decimal("2"))
                and bar_range > 0
                and body / bar_range <= Decimal("0.35")
            ):
                doji_stability_count += 1

        vwap = self._vwap_from_df(morning)
        rsi_reclaim_level = Decimal(
            str(getattr(self._settings, "reversal_1030_rsi_reclaim", Decimal("30")))
        )
        rsi_reclaim = rsi >= rsi_reclaim_level and (
            prev_rsi < rsi_reclaim_level or rsi > prev_rsi
        )
        reclaim_lower = close > boll_dn and prev_close <= boll_dn
        recent_high_break = close > _to_decimal(morning.tail(4).iloc[:-1]["high"].max())
        reclaim_vwap = bool(vwap is not None and close >= vwap and prev_close < vwap)
        reclaim_mid = close >= boll_mid and prev_close < boll_mid

        return {
            "symbol": event.symbol,
            "symbols": symbols,
            "session_open": session_open,
            "morning_low": morning_low,
            "morning_drop": morning_drop,
            "intraday_move": intraday_move,
            "current": current,
            "prev": prev,
            "close": close,
            "open": open_px,
            "high": high,
            "low": low,
            "prev_close": prev_close,
            "boll_dn": boll_dn,
            "boll_mid": boll_mid,
            "boll_up": boll_up,
            "boll_position": self._bollinger_position(close, boll_dn, boll_up),
            "vwap": vwap,
            "rsi": rsi,
            "prev_rsi": prev_rsi,
            "rsi_reclaim": rsi_reclaim,
            "lower_touch": lower_touch,
            "doji_stability_count": doji_stability_count,
            "green_bar": close > open_px,
            "reclaim_lower": reclaim_lower,
            "recent_high_break": recent_high_break,
            "reclaim_vwap": reclaim_vwap,
            "reclaim_mid": reclaim_mid,
            "upper_shadow_pct": self._upper_shadow_pct(open_px, high, low, close),
        }

    def _reversal_1030_reason(
        self,
        ctx: Dict[str, Any],
        *,
        entry_mode: str,
        addition: bool,
        extra: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        reason = {
            "entry_mode": entry_mode,
            "watchlist": sorted(ctx["symbols"]),
            "session_open": str(ctx["session_open"]),
            "morning_low": str(ctx["morning_low"]),
            "morning_drop_pct": str(ctx["morning_drop"]),
            "intraday_move_pct": str(ctx["intraday_move"]),
            "boll_dn": str(ctx["boll_dn"]),
            "boll_mid": str(ctx["boll_mid"]),
            "boll_up": str(ctx["boll_up"]),
            "boll_position": str(ctx["boll_position"]),
            "vwap": str(ctx["vwap"]) if ctx["vwap"] is not None else None,
            "rsi6": str(ctx["rsi"]),
            "prev_rsi6": str(ctx["prev_rsi"]),
            "lower_touch": ctx["lower_touch"],
            "doji_stability_count": ctx["doji_stability_count"],
            "green_bar": ctx["green_bar"],
            "reclaim_lower": ctx["reclaim_lower"],
            "recent_high_break": ctx["recent_high_break"],
            "reclaim_vwap": ctx["reclaim_vwap"],
            "reclaim_mid": ctx["reclaim_mid"],
            "upper_shadow_pct": str(ctx["upper_shadow_pct"]),
            "entry_bar_low": str(ctx["low"]),
            "entry_close": str(ctx["close"]),
            "entry_vwap": str(ctx["vwap"]) if ctx["vwap"] is not None else None,
            "entry_boll_mid": str(ctx["boll_mid"]),
            "exit_plan": {
                "primary": "sell_call_when_underlying_taps_1m_boll_upper",
                "failure_entry_low": "sell_call_when_underlying_breaks_entry_bar_low",
                "failure_reclaim_lost": "sell_call_when_underlying_loses_vwap_or_mid_after_profit",
                "deadline": str(getattr(self._settings, "reversal_1030_exit_deadline", "12:00")),
            },
            "add_position": addition,
            "option_liquidity": self._liquidity_snapshot.get(ctx["symbol"], {"pass": True}),
        }
        if extra:
            reason.update(extra)
        return reason

    @staticmethod
    def _bollinger_position(close: Decimal, boll_dn: Decimal, boll_up: Decimal) -> Decimal:
        width = boll_up - boll_dn
        if width <= 0:
            return Decimal("999")
        return (close - boll_dn) / width

    @staticmethod
    def _upper_shadow_pct(open_px: Decimal, high: Decimal, low: Decimal, close: Decimal) -> Decimal:
        total_range = high - low
        if total_range <= 0:
            return Decimal("0")
        return (high - max(open_px, close)) / total_range

    def _first_bar_blowoff(self, todays: pd.DataFrame) -> bool:
        rows = todays[todays["et"].dt.time == time(9, 31)]
        if rows.empty:
            rows = todays.head(1)
        if rows.empty:
            return False
        first = rows.iloc[0]
        high = _to_decimal(first["high"])
        low = _to_decimal(first["low"])
        close = _to_decimal(first["close"])
        open_px = _to_decimal(first["open"])
        total_range = high - low
        if total_range <= 0:
            return False
        range_pct = total_range / open_px if open_px > 0 else Decimal("0")
        threshold = Decimal(
            str(
                getattr(
                    self._settings,
                    "open_chase_first_bar_blowoff_range_pct",
                    Decimal("0.012"),
                )
            )
        )
        close_position = (close - low) / total_range
        return range_pct > threshold and close_position < Decimal("0.50")

    def _log_orb_reject(self, event: BarsClosed, reason: str) -> None:
        LOGGER.info(
            "signal.open_chase_orb.filtered",
            symbol=event.symbol,
            timestamp=_to_eastern(event.bar_end).isoformat(),
            reason=reason,
        )
        try:
            if self._metrics_enabled:
                record_signal_filtered(reason.upper())
        except Exception:
            pass
        return None

    def _log_1030_reject(self, event: BarsClosed, reason: str, **fields: Any) -> None:
        LOGGER.info(
            "signal.reversal_1030.filtered",
            symbol=event.symbol,
            timestamp=_to_eastern(event.bar_end).isoformat(),
            reason=reason,
            **fields,
        )
        try:
            if self._metrics_enabled:
                record_signal_filtered(reason.upper())
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_hhmm(value: str) -> time:
        hour, minute = value.split(":", 1)
        return time(hour=int(hour), minute=int(minute))

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
            LOGGER.info(
                "top5_cache.loaded",
                trade_date=trade_date.isoformat(),
                symbols=sorted(list(cache)),
                count=len(cache)
            )
        result = symbol.upper() in cache
        return result

    def _is_high_open(self, session: Session, symbol: str, trade_date: date) -> bool:
        open_rth_dec = self._fallback_daily_field_value(
            session, symbol, trade_date, field_name="open_rth"
        )
        prev_close_dec = self._fallback_daily_field_value(
            session, symbol, trade_date, field_name="prev_close_rth"
        )
        if open_rth_dec is None or prev_close_dec is None:
            return False
        return open_rth_dec > prev_close_dec

    def _option_price_exceeds_open(
        self,
        session: Session,
        symbol: str,
        trade_date: date,
        ts_end: datetime,
    ) -> bool:
        ts_query = ts_end
        if ts_query.tzinfo is None:
            ts_query = ts_query.replace(tzinfo=timezone.utc)
        window_start = ts_query - timedelta(minutes=15)
        trade_date_et = ts_query.astimezone(EASTERN).date()
        try:
            current_row = self._option_quote_candidates(
                session,
                symbol,
                ts_query,
                window_start=window_start,
            )
        except Exception:
            LOGGER.exception(
                "signal_engine.option_price_open_current_query_failed",
                symbol=symbol,
                trade_date=str(trade_date),
            )
            return False
        candidates: List[Dict[str, Any]] = []
        for row in current_row:
            conid = row.get("conid")
            expiry = row.get("expiry")
            strike = row.get("strike")
            bid = row.get("bid")
            ask = row.get("ask")
            volume = row.get("volume")
            oi = row.get("open_interest")
            delta = row.get("delta")
            underlying_price = row.get("underlying_price")
            current_price = row.get("current_price")
            if None in (conid, expiry, strike, current_price):
                continue
            try:
                current_price_dec = Decimal(str(current_price))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if current_price_dec <= 0:
                continue
            try:
                bid_dec = Decimal(str(bid)) if bid is not None else None
                ask_dec = Decimal(str(ask)) if ask is not None else None
            except (InvalidOperation, TypeError, ValueError):
                continue
            ts_sample = _coerce_utc_datetime(row.get("ts_end"))
            spread = Decimal("Infinity")
            if bid_dec is not None and ask_dec is not None:
                if bid_dec <= 0 or ask_dec <= 0 or ask_dec <= bid_dec:
                    continue
                spread = ask_dec - bid_dec
                threshold = max(Decimal("0.10"), current_price_dec * Decimal("0.05"))
                if getattr(self, "_option_liquidity_required", True) and spread > threshold:
                    continue
            if ts_sample is not None:
                if max(0.0, (ts_query - ts_sample).total_seconds()) > OPTION_QUOTE_MAX_AGE_SECONDS:
                    continue
            try:
                volume_int = int(volume or 0)
                oi_int = int(oi or 0)
                allow_missing_metrics = getattr(
                    self, "_allow_missing_option_liquidity_metrics", False
                )
                if getattr(self, "_option_liquidity_required", True) and not (
                    allow_missing_metrics and volume_int == 0 and oi_int == 0
                ):
                    if volume_int < 100:
                        continue
                    if oi_int < 500:
                        continue
            except (TypeError, ValueError):
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
            dte = (expiry_date - trade_date_et).days
            if dte < 2 or dte > 7:
                continue
            try:
                strike_dec = Decimal(str(strike))
            except (InvalidOperation, TypeError, ValueError):
                continue
            try:
                delta_dec = Decimal(str(delta)) if delta is not None else None
            except (InvalidOperation, TypeError, ValueError):
                delta_dec = None
            try:
                underlying_dec = (
                    Decimal(str(underlying_price)) if underlying_price is not None else None
                )
            except (InvalidOperation, TypeError, ValueError):
                underlying_dec = None
            strike_distance = (
                abs(strike_dec - underlying_dec)
                if underlying_dec is not None
                else Decimal("Infinity")
            )
            quote_age = max(0.0, (ts_query - ts_sample).total_seconds()) if ts_sample else 0.0
            candidates.append(
                {
                    "conid": conid,
                    "current_price": current_price_dec,
                    "expiry": expiry_date,
                    "strike": strike,
                    "quote_ts": ts_sample,
                    "selection_key": (
                        quote_age,
                        -volume_int,
                        self._option_delta_penalty(delta_dec),
                        spread,
                        -oi_int,
                        strike_distance,
                    ),
                }
            )
        selected = min(candidates, key=lambda item: item["selection_key"]) if candidates else None
        if selected is None:
            LOGGER.info(
                "signal_engine.option_price_open_missing_current",
                symbol=symbol,
                trade_date=str(trade_date),
                ts_end=ts_query.isoformat(),
            )
            return False

        start_et = datetime.combine(trade_date, time(9, 30), EASTERN)
        start_utc = start_et.astimezone(timezone.utc)
        window_end = start_utc + timedelta(minutes=5)
        try:
            row = session.execute(
                text(
                    """
                    SELECT COALESCE(mid, (bid + ask) / 2, last) AS open_price
                    FROM bars1m_option
                    WHERE conid = :conid
                      AND ts_end >= :start_ts
                      AND ts_end < :end_ts
                      AND COALESCE(mid, (bid + ask) / 2, last) IS NOT NULL
                    ORDER BY ts_end ASC
                    LIMIT 1
                    """
                ),
                {"conid": selected["conid"], "start_ts": start_utc, "end_ts": window_end},
            ).fetchone()
        except Exception:
            LOGGER.exception(
                "signal_engine.option_price_open_open_query_failed",
                symbol=symbol,
                conid=selected["conid"],
                trade_date=str(trade_date),
            )
            return False
        if not row:
            LOGGER.info(
                "signal_engine.option_price_open_missing_open",
                symbol=symbol,
                conid=selected["conid"],
                trade_date=str(trade_date),
            )
            return False
        open_last = row[0]
        if open_last is None:
            return False
        try:
            open_price = Decimal(str(open_last))
        except (InvalidOperation, TypeError, ValueError):
            return False
        if open_price <= 0:
            return False
        return selected["current_price"] > open_price

    @staticmethod
    def _option_delta_penalty(delta: Optional[Decimal]) -> int:
        if delta is None:
            return 1
        abs_delta = abs(delta)
        return 0 if Decimal("0.35") <= abs_delta <= Decimal("0.45") else 1

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


def _coerce_utc_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        try:
            ts = pd.Timestamp(value).to_pydatetime()
        except (TypeError, ValueError):
            return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")
