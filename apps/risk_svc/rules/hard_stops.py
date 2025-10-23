from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from uuid import uuid4

import pandas as pd
import statistics
from sqlalchemy import text
from sqlalchemy.orm import Session

from libs.core import EASTERN
from libs.db.dao import RiskEventDAO, RiskStateDAO
from libs.db.models import StrategyPosition
from libs.infra.metrics import record_force_close

from libs.infra.redis_bus import RedisBus

from ..publisher import publish_force_close, publish_risk_alert
from ..service import RiskService

_OPTION_MULTIPLIER = Decimal("100")
_ATR_TIGHTENER_MULTIPLIER = Decimal("0.5")


class OvernightDecision(str, Enum):
    APPROVE_PATH_A = "OVERNIGHT_APPROVE_PATH_A"
    APPROVE_PATH_B = "OVERNIGHT_APPROVE_PATH_B"
    RECORD_VIX = "OVERNIGHT_RECORD_VIX"
    REJECT_VIX = "OVERNIGHT_REJECT_VIX"
    REJECT_TREND = "OVERNIGHT_REJECT_TREND"
    REJECT_IV = "OVERNIGHT_REJECT_IV"
    REJECT_BWIDTH = "OVERNIGHT_REJECT_BWIDTH"
    REJECT_OPTION_DATA = "OVERNIGHT_REJECT_OPTION_DATA"
    REJECT_PATH = "OVERNIGHT_REJECT_PATH"

    @property
    def is_rejection(self) -> bool:
        return self.name.startswith("REJECT")


class HardStopRules:
    """Evaluate hard-stop style safeguards on each minute bar."""

    def __init__(self, risk_service: RiskService, redis_bus: RedisBus) -> None:
        self._risk_service = risk_service
        self._redis_bus = redis_bus
        self._overnight_status: Dict[str, OvernightDecision] = {}

    async def evaluate(
        self,
        session: Session,
        ts_end: datetime,
        positions: Iterable[StrategyPosition],
        *,
        trace_id: Optional[str] = None,
    ) -> None:
        trace = trace_id or str(uuid4())
        positions_list: List[StrategyPosition] = list(positions)
        active_positions: List[StrategyPosition] = [
            position
            for position in positions_list
            if int(getattr(position, "open_quantity", 0)) != 0
        ]
        if not active_positions:
            return
        await self._check_stop_loss(session, ts_end, active_positions, trace)
        await self._atr_delta_tighter(session, ts_end, active_positions, trace)
        await self._check_time_cutoffs(session, ts_end, active_positions, trace)
        await self._evaluate_overnight(session, ts_end, active_positions, trace)
        await self._check_iv_cap(session, ts_end, active_positions, trace)

    async def _check_stop_loss(
        self,
        session: Session,
        ts_end: datetime,
        positions: Sequence[StrategyPosition],
        trace_id: str,
    ) -> None:
        threshold = self._risk_service.stop_loss_pct
        for position in positions:
            qty = Decimal(str(position.open_quantity))
            if qty == 0:
                continue
            mark = Decimal(str(position.mark_price))
            avg = Decimal(str(position.avg_open_price))
            if avg <= 0:
                continue
            loss_pct = (mark - avg) / avg
            if loss_pct <= -threshold:
                await self._force_close(
                    session,
                    position,
                    "HARD_STOP",
                    {
                        "loss_pct": str(loss_pct),
                        "threshold": str(threshold),
                        "option_right": position.option_right,
                        "strategy_code": position.strategy_code,
                        "avg_price": str(avg),
                        "mark_price": str(mark),
                    },
                    ts_end,
                    trace_id,
                )

    async def _atr_delta_tighter(
        self,
        session: Session,
        ts_end: datetime,
        positions: Sequence[StrategyPosition],
        trace_id: str,
    ) -> None:
        candidates = [
            position
            for position in positions
            if position.delta is not None and Decimal(str(position.delta or "0")) != 0
        ]
        if not candidates:
            return
        symbols = {position.symbol for position in candidates}
        atr_map = self._load_latest_atr(session, ts_end, list(symbols))
        if not atr_map:
            return
        threshold = self._risk_service.stop_loss_pct
        for position in candidates:
            atr_value = atr_map.get(position.symbol)
            if atr_value is None or atr_value <= 0:
                continue
            avg_price = Decimal(str(position.avg_open_price))
            if avg_price <= 0:
                continue
            delta_value = Decimal(str(position.delta))
            estimated_loss_pct = (
                abs(delta_value) * atr_value * _ATR_TIGHTENER_MULTIPLIER
            ) / avg_price
            if estimated_loss_pct >= threshold:
                await self._force_close(
                    session,
                    position,
                    "ATR_DELTA",
                    {
                        "loss_est_pct": str(estimated_loss_pct),
                        "threshold": str(threshold),
                        "atr14": str(atr_value),
                        "delta": str(delta_value),
                        "option_right": position.option_right,
                        "strategy_code": position.strategy_code,
                    },
                    ts_end,
                    trace_id,
                )

    async def _check_time_cutoffs(
        self,
        session: Session,
        ts_end: datetime,
        positions: Sequence[StrategyPosition],
        trace_id: str,
    ) -> None:
        et = ts_end.astimezone(EASTERN)
        time_of_day = et.time()

        cutoff_open_chase = self._risk_service.cutoff_open_chase
        if time_of_day >= cutoff_open_chase:
            for position in positions:
                if (
                    position.open_quantity
                    and (position.source_signal_code or "").upper() == "SIG_OPEN_CHASE_BUY"
                ):
                    await self._force_close(
                        session,
                        position,
                        "DAILY_CUTOFF",
                        {
                            "cutoff": "open_chase",
                            "cutoff_time": cutoff_open_chase.isoformat(),
                            "option_right": position.option_right,
                            "strategy_code": position.strategy_code,
                            "source_signal_code": position.source_signal_code or "",
                        },
                        ts_end,
                        trace_id,
                    )

        cutoff_overnight = self._risk_service.cutoff_overnight
        if time_of_day >= cutoff_overnight:
            trade_date = et.date()
            for position in positions:
                if not position.open_quantity:
                    continue
                opened_at = position.opened_at
                if opened_at is None:
                    continue
                opened_date = opened_at.astimezone(EASTERN).date()
                if opened_date < trade_date:
                    await self._force_close(
                        session,
                        position,
                        "OVERNIGHT_CUTOFF",
                        {
                            "cutoff": "overnight",
                            "cutoff_time": cutoff_overnight.isoformat(),
                            "option_right": position.option_right,
                            "strategy_code": position.strategy_code,
                            "opened_at": opened_at.isoformat(),
                        },
                        ts_end,
                        trace_id,
                    )

    async def _check_iv_cap(
        self,
        session: Session,
        ts_end: datetime,
        positions: Sequence[StrategyPosition],
        trace_id: str,
    ) -> None:
        et_time = ts_end.astimezone(EASTERN).time()
        if not (time(15, 50) <= et_time <= time(16, 0)):
            return
        call_positions: Dict[str, List[StrategyPosition]] = defaultdict(list)
        for position in positions:
            if position.open_quantity and position.option_right == "CALL":
                call_positions[position.symbol].append(position)
        if not call_positions:
            return
        cap = self._risk_service.iv_overnight_cap

        for symbol, symbol_positions in call_positions.items():
            row = session.execute(
                text(
                    """
                    SELECT implied_vol
                    FROM bars1m_option
                    WHERE underlying_symbol = :symbol
                      AND right = 'CALL'
                      AND ts_end <= :ts_end
                    ORDER BY ts_end DESC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol, "ts_end": ts_end},
            ).first()
            if not row:
                continue
            iv_value_raw = row[0]
            if iv_value_raw is None:
                continue
            iv_value = Decimal(str(iv_value_raw))
            if iv_value <= cap:
                continue
            for position in symbol_positions:
                await self._force_close(
                    session,
                    position,
                    "IV_OVERNIGHT_CAP",
                    {
                        "iv": str(iv_value),
                        "cap": str(cap),
                        "option_right": position.option_right,
                        "strategy_code": position.strategy_code,
                    },
                    ts_end,
                    trace_id,
                )

    async def _evaluate_overnight(
        self,
        session: Session,
        ts_end: datetime,
        positions: Sequence[StrategyPosition],
        trace_id: str,
    ) -> None:
        et = ts_end.astimezone(EASTERN)
        if et.time() < time(14, 0):
            return
        call_positions: Dict[str, List[StrategyPosition]] = defaultdict(list)
        for position in positions:
            if position.open_quantity and position.option_right == "CALL":
                call_positions[position.symbol.upper()].append(position)
        if not call_positions:
            return

        trade_date = et.date()
        limits = self._risk_service.current_limits()
        vix_value = self._current_vix_value(session)

        for symbol_upper, symbol_positions in call_positions.items():
            outcome, detail = self._assess_overnight_symbol(
                session=session,
                ts_end=ts_end,
                trade_date=trade_date,
                symbol=symbol_upper,
                positions=symbol_positions,
                vix_value=vix_value,
                limits=limits,
            )
            if outcome is None:
                continue
            previous = self._overnight_status.get(symbol_upper)
            if previous == outcome:
                continue
            self._overnight_status[symbol_upper] = outcome
            if outcome.is_rejection:
                for position in symbol_positions:
                    await self._force_close(
                        session,
                        position,
                        outcome.value,
                        {**detail, "symbol": symbol_upper},
                        ts_end,
                        trace_id,
                    )
            else:
                await self._log_overnight_event(session, symbol_upper, outcome, detail, ts_end)

    async def _log_overnight_event(
        self,
        session: Session,
        symbol: str,
        outcome: OvernightDecision,
        detail: Mapping[str, object],
        ts_end: datetime,
    ) -> None:
        payload = {key: str(value) for key, value in detail.items()}
        dao = RiskEventDAO(session)
        dao.create_event(
            event_ts=ts_end,
            event_code=outcome.value,
            severity="INFO" if not outcome.is_rejection else "WARN",
            message=outcome.value.lower(),
            symbol=symbol,
            payload=payload,
        )
        alert_payload = {
            "symbol": symbol,
            "event_code": outcome.value,
            "detail": payload,
        }
        await publish_risk_alert(
            self._redis_bus,
            alert_payload,
            trace_id=f"overnight-{symbol}-{outcome.value.lower()}",
        )

    def _current_vix_value(self, session: Session) -> Optional[Decimal]:
        record = RiskStateDAO(session).latest_for_symbol("VIX", "VIX")
        if record is None or record.metric_value is None:
            return None
        try:
            return Decimal(str(record.metric_value))
        except (InvalidOperation, TypeError):
            return None

    def _assess_overnight_symbol(
        self,
        session: Session,
        ts_end: datetime,
        trade_date: date,
        symbol: str,
        positions: Sequence[StrategyPosition],
        vix_value: Optional[Decimal],
        limits,
    ) -> Tuple[Optional[OvernightDecision], Mapping[str, object]]:
        vix_gate_mode = getattr(self._risk_service, "vix_gate_mode", "enforce")
        vix_gate_threshold = self._risk_service.vix_gate
        if (
            vix_gate_mode == "enforce"
            and vix_value is not None
            and vix_value >= vix_gate_threshold
        ):
            return OvernightDecision.REJECT_VIX, {
                "vix": str(vix_value),
                "threshold": str(vix_gate_threshold),
                "mode": vix_gate_mode,
            }

        option_metrics = self._load_option_metrics(session, positions, ts_end, trade_date)
        if option_metrics is None:
            return OvernightDecision.REJECT_OPTION_DATA, {"reason": "missing_option_bars"}

        iv_cap = Decimal(str(limits.overnight.iv_max))
        option_iv = option_metrics.get("iv")
        if option_iv is None or option_iv > iv_cap:
            return OvernightDecision.REJECT_IV, {
                "iv": str(option_iv) if option_iv is not None else "null",
                "cap": str(iv_cap),
            }

        trend_ok, trend_detail = self._daily_trend_pass(session, symbol, trade_date)
        if not trend_ok:
            return OvernightDecision.REJECT_TREND, trend_detail

        equity_df = self._load_intraday_equity(
            session, symbol, ts_end, limits.overnight.bw.lookback
        )
        if equity_df is None or equity_df.empty:
            return OvernightDecision.REJECT_BWIDTH, {"reason": "missing_intraday"}

        bw_ok, bw_detail = self._bandwidth_state(equity_df, limits)
        if not bw_ok:
            return OvernightDecision.REJECT_BWIDTH, bw_detail

        if (
            vix_gate_mode == "record"
            and vix_value is not None
            and vix_value >= vix_gate_threshold
        ):
            self._log_overnight_event(
                session,
                symbol,
                OvernightDecision.RECORD_VIX,
                {
                    "vix": str(vix_value),
                    "threshold": str(vix_gate_threshold),
                    "mode": vix_gate_mode,
                },
                ts_end,
            )

        path_a_pass, path_a_detail = self._check_path_a(
            equity_df,
            option_metrics,
            ts_end,
            limits,
        )
        if path_a_pass:
            return OvernightDecision.APPROVE_PATH_A, path_a_detail

        path_b_pass, path_b_detail = self._check_path_b(
            equity_df,
            option_metrics,
            ts_end,
            limits,
        )
        if path_b_pass:
            return OvernightDecision.APPROVE_PATH_B, path_b_detail

        detail = {"path_a": path_a_detail, "path_b": path_b_detail}
        return OvernightDecision.REJECT_PATH, detail

    def _load_latest_atr(
        self,
        session: Session,
        ts_end: datetime,
        symbols: Sequence[str],
    ) -> Dict[str, Decimal]:
        atr_values: Dict[str, Decimal] = {}
        for symbol in symbols:
            row = session.execute(
                text(
                    """
                    SELECT atr14
                    FROM indicators_eq_1m
                    WHERE symbol = :symbol
                      AND ts_end <= :ts_end
                    ORDER BY ts_end DESC
                    LIMIT 1
                    """
                ),
                {"symbol": symbol, "ts_end": ts_end},
            ).first()
            if not row:
                continue
            atr_raw = row[0]
            if atr_raw is None:
                continue
            atr_values[symbol] = Decimal(str(atr_raw))
        return atr_values

    async def _force_close(
        self,
        session: Session,
        position: Optional[StrategyPosition],
        event_code: str,
        detail: Mapping[str, str],
        ts_end: datetime,
        trace_id: str,
    ) -> None:
        symbol = position.symbol if position else detail.get("symbol", "")
        dao = RiskEventDAO(session)
        detail_payload = {key: str(value) for key, value in detail.items()}
        dao.create_event(
            event_ts=ts_end,
            event_code=event_code,
            severity="WARN",
            message=event_code.lower(),
            symbol=symbol,
            payload=detail_payload,
        )
        self._overnight_status.pop(symbol.upper(), None)
        await publish_force_close(
            self._redis_bus,
            {
                "symbol": symbol,
                "strategy_code": position.strategy_code
                if position
                else detail_payload.get("strategy_code", "core-vol"),
                "option_right": position.option_right if position else detail_payload.get("option_right"),
                "reason": event_code,
                "detail": detail_payload,
                "triggered_at": ts_end,
            },
            trace_id=trace_id,
        )
        record_force_close(event_code)

    def _load_option_metrics(
        self,
        session: Session,
        positions: Sequence[StrategyPosition],
        ts_end: datetime,
        trade_date: date,
    ) -> Optional[Dict[str, object]]:
        for position in positions:
            conid = getattr(position, "conid", None)
            if not conid:
                continue
            start_et = datetime.combine(trade_date, time(9, 30), EASTERN)
            start_utc = start_et.astimezone(timezone.utc)
            rows = session.execute(
                text(
                    """
                    SELECT ts_end, mid, last, implied_vol, volume
                    FROM bars1m_option
                    WHERE conid = :conid
                      AND ts_end BETWEEN :start_ts AND :end_ts
                    ORDER BY ts_end ASC
                    """
                ),
                {"conid": conid, "start_ts": start_utc, "end_ts": ts_end},
            ).all()
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=["ts_end", "mid", "last", "iv", "volume"])
            df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)
            df["price"] = df["mid"].where(pd.notna(df["mid"]), df["last"])
            df["price"] = pd.to_numeric(df["price"], errors="coerce")
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
            df = df.dropna(subset=["price"])
            if df.empty:
                continue
            option_open = Decimal(str(df.iloc[0]["price"]))
            option_close = Decimal(str(df.iloc[-1]["price"]))
            iv_raw = df["iv"].dropna().iloc[-1] if df["iv"].notna().any() else None
            option_iv = Decimal(str(iv_raw)) if iv_raw is not None else None
            return {
                "conid": conid,
                "option_open": option_open,
                "option_close": option_close,
                "iv": option_iv,
                "series": df,
            }
        return None

    def _load_intraday_equity(
        self,
        session: Session,
        symbol: str,
        ts_end: datetime,
        bw_lookback: int,
    ) -> Optional[pd.DataFrame]:
        window_minutes = max(240, bw_lookback + 30)
        start_ts = ts_end - timedelta(minutes=window_minutes)
        rows = session.execute(
            text(
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
                    i.stoch_rsi_k,
                    i.stoch_rsi_d,
                    i.lr_m5_slope,
                    i.lr_boll_dn_slope,
                    i.lr_obv_slope,
                    i.rvol6,
                    i.sma5
                FROM bars1m_equity b
                LEFT JOIN indicators_eq_1m i
                  ON b.symbol = i.symbol AND b.ts_end = i.ts_end
                WHERE b.symbol = :symbol
                  AND b.ts_end BETWEEN :start_ts AND :end_ts
                ORDER BY b.ts_end ASC
                """
            ),
            {"symbol": symbol, "start_ts": start_ts, "end_ts": ts_end},
        ).all()
        if not rows:
            return None
        cols = [
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
            "stoch_rsi_k",
            "stoch_rsi_d",
            "lr_m5_slope",
            "lr_boll_dn_slope",
            "lr_obv_slope",
            "rvol6",
            "sma5",
        ]
        df = pd.DataFrame(rows, columns=cols)
        df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)
        df = df.sort_values("ts_end")
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
            "stoch_rsi_k",
            "stoch_rsi_d",
            "lr_m5_slope",
            "lr_boll_dn_slope",
            "lr_obv_slope",
            "rvol6",
            "sma5",
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["et"] = df["ts_end"].dt.tz_convert("US/Eastern")
        return df

    def _bandwidth_state(
        self,
        df: pd.DataFrame,
        limits,
    ) -> Tuple[bool, Dict[str, object]]:
        bw = []
        for _, row in df.iterrows():
            mid = row["boll_mid"]
            up = row["boll_up"]
            dn = row["boll_dn"]
            if None in (mid, up, dn) or mid == 0:
                bw.append(float("nan"))
            else:
                bw.append((up - dn) / abs(mid))
        bw_series = pd.Series(bw, index=df["ts_end"], dtype="float64")
        lookback = int(max(limits.overnight.bw.lookback, 30))
        if bw_series.dropna().empty or len(bw_series.dropna()) < lookback:
            return (
                False,
                {"reason": "insufficient_bw_history", "available": len(bw_series.dropna())},
            )
        baseline = bw_series.dropna().iloc[-lookback:].mean()
        if baseline is None or baseline <= 0:
            return False, {"reason": "invalid_baseline", "baseline": str(baseline)}
        ma_window = max(1, limits.overnight.bw.ma)
        short_ma = bw_series.rolling(window=ma_window, min_periods=ma_window).mean()
        hold = max(1, limits.overnight.bw.hold_min)
        recent = short_ma.dropna().iloc[-hold:]
        if len(recent) < hold:
            return False, {"reason": "insufficient_hold", "hold": len(recent)}
        threshold = baseline * float(limits.overnight.bw.ratio)
        below_threshold = (recent <= threshold).all()
        detail = {
            "baseline": f"{baseline:.6f}",
            "threshold": f"{threshold:.6f}",
            "recent": f"{recent.iloc[-1]:.6f}",
            "hold": hold,
        }
        return below_threshold, detail

    def _daily_trend_pass(
        self,
        session: Session,
        symbol: str,
        trade_date: date,
    ) -> Tuple[bool, Dict[str, object]]:
        rows = session.execute(
            text(
                """
                SELECT trade_date, close_rth
                FROM v_daily_ohlcv
                WHERE symbol = :symbol
                  AND trade_date <= :trade_date
                ORDER BY trade_date DESC
                LIMIT 65
                """
            ),
            {"symbol": symbol, "trade_date": trade_date},
        ).all()
        if not rows or len(rows) < 22:
            return False, {"reason": "insufficient_daily_history"}
        df = pd.DataFrame(rows, columns=["trade_date", "close"])
        df = df.dropna(subset=["close"]).sort_values("trade_date")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["sma20"] = df["close"].rolling(window=20, min_periods=20).mean()
        df["sma60"] = df["close"].rolling(window=60, min_periods=60).mean()
        if df["sma20"].isna().iloc[-3:].any() or df["sma60"].isna().iloc[-1]:
            return False, {"reason": "insufficient_sma"}
        recent = df.iloc[-3:]
        close_above_sma = bool((recent["close"] > recent["sma20"]).all())
        if not close_above_sma:
            return False, {"reason": "close_below_sma20"}
        sma60_curr = df["sma60"].iloc[-1]
        sma60_prev = df["sma60"].iloc[-2] if len(df) >= 61 else None
        if sma60_prev is None or sma60_curr is None or sma60_curr <= sma60_prev:
            return False, {
                "reason": "sma60_non_positive_slope",
                "sma60_curr": str(sma60_curr),
                "sma60_prev": str(sma60_prev),
            }
        return True, {
            "sma60_curr": str(sma60_curr),
            "sma60_prev": str(sma60_prev),
            "close_gt_sma20_days": 3,
        }

    def _compute_vwap(self, df: pd.DataFrame, start_et: datetime, end_et: datetime) -> Optional[Decimal]:
        window = df[(df["et"] >= start_et) & (df["et"] <= end_et)]
        if window.empty:
            return None
        window = window.dropna(subset=["close", "volume"])
        if window.empty or window["volume"].sum() <= 0:
            return None
        vwap = (window["close"] * window["volume"]).sum() / window["volume"].sum()
        return Decimal(str(vwap))

    def _check_path_a(
        self,
        equity_df: pd.DataFrame,
        option_metrics: Mapping[str, object],
        ts_end: datetime,
        limits,
    ) -> Tuple[bool, Dict[str, object]]:
        et = ts_end.astimezone(EASTERN)
        if et.time() < time(15, 40):
            return False, {"reason": "time_before_1540"}
        option_open = option_metrics.get("option_open")
        option_close = option_metrics.get("option_close")
        if None in (option_open, option_close):
            return False, {"reason": "missing_option_prices"}
        if option_close <= option_open:
            return False, {"reason": "option_not_strong"}
        vwap_start = datetime.combine(et.date(), time(10, 0), EASTERN)
        vwap_end = datetime.combine(et.date(), time(15, 0), EASTERN)
        vwap = self._compute_vwap(equity_df, vwap_start, vwap_end)
        if vwap is None:
            return False, {"reason": "vwap_unavailable"}
        window = equity_df[(equity_df["et"] >= vwap_start) & (equity_df["et"] <= vwap_end)]
        if window.empty:
            return False, {"reason": "insufficient_vwap_window"}
        if not (window["close"] >= float(vwap)).all():
            return False, {"reason": "price_not_above_vwap"}
        current = equity_df.iloc[-1]
        prev = equity_df.iloc[-2] if len(equity_df) >= 2 else None
        if current["boll_mid"] is None or current["close"] is None or current["close"] > current["boll_mid"]:
            return False, {"reason": "close_not_below_mid"}
        if prev is None or prev["rsi6"] is None or current["rsi6"] is None:
            return False, {"reason": "missing_rsi"}
        rsi_series = equity_df["rsi6"].dropna()
        rsi_window = rsi_series.iloc[-5:] if len(rsi_series) > 5 else rsi_series
        if not self._series_cross_up(rsi_window, Decimal("30")):
            return False, {"reason": "rsi_not_cross_30"}
        detail = {
            "option_open": str(option_open),
            "option_close": str(option_close),
            "vwap": str(vwap),
            "close": str(current["close"]),
            "mid": str(current["boll_mid"]),
            "rsi_prev": str(prev["rsi6"]),
            "rsi_curr": str(current["rsi6"]),
        }
        return True, detail

    def _check_path_b(
        self,
        equity_df: pd.DataFrame,
        option_metrics: Mapping[str, object],
        ts_end: datetime,
        limits,
    ) -> Tuple[bool, Dict[str, object]]:
        option_open = option_metrics.get("option_open")
        option_close = option_metrics.get("option_close")
        if None in (option_open, option_close) or option_close >= option_open:
            return False, {"reason": "option_not_weak"}

        current = equity_df.iloc[-1]
        prev = equity_df.iloc[-2] if len(equity_df) >= 2 else None
        if prev is None or None in (current["close"], prev["close"]):
            return False, {"reason": "missing_prices"}
        rebound_pct = (current["close"] - prev["close"]) / prev["close"]
        max_rebound = float(limits.overnight.rebound.max_pct)
        if rebound_pct <= 0 or rebound_pct > max_rebound:
            return False, {
                "reason": "rebound_threshold",
                "rebound_pct": f"{rebound_pct:.6f}",
                "max_rebound": str(max_rebound),
            }

        slope = equity_df["lr_m5_slope"]
        peaks: List[int] = []
        for idx in range(6, len(slope) - 6):
            before = slope.iloc[idx - 6 : idx].dropna()
            after = slope.iloc[idx + 1 : idx + 7].dropna()
            if before.empty or after.empty:
                continue
            if (before > 0).all() and (after < 0).all():
                peaks.append(idx)
        volume_detail = {}
        volume_ok = False
        if len(peaks) >= 2:
            base_volume = equity_df["volume"].iloc[max(peaks[0] - 6, 0) : peaks[0] + 7].sum()
            compare_idx = peaks[1] if len(peaks) == 2 else peaks[2]
            compare_volume = equity_df["volume"].iloc[
                max(compare_idx - 6, 0) : compare_idx + 7
            ].sum()
            volume_ok = compare_volume <= base_volume
            volume_detail = {
                "base_volume": str(base_volume),
                "compare_volume": str(compare_volume),
                "peaks": len(peaks),
            }

        rvol_series = equity_df["rvol6"].dropna()
        rvol_peaks = rvol_series[rvol_series >= 3]
        rvol_condition = len(rvol_peaks) >= 2

        if not (volume_ok or rvol_condition):
            return False, {
                "reason": "pattern_not_satisfied",
                "volume_check": volume_detail,
                "rvol_count": len(rvol_peaks),
            }

        detail = {
            "option_open": str(option_open),
            "option_close": str(option_close),
            "rebound_pct": f"{rebound_pct:.6f}",
            "volume_check": volume_detail,
            "rvol_count": len(rvol_peaks),
        }
        return True, detail

    def _series_cross_up(self, series: pd.Series, threshold: Decimal) -> bool:
        if series is None or len(series) < 2:
            return False
        prev_value: Optional[Decimal] = None
        for val in series:
            if pd.isna(val):
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
