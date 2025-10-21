from __future__ import annotations

from collections import defaultdict
from datetime import datetime, time
from decimal import Decimal
from typing import Dict, Iterable, List, Mapping, Optional, Sequence
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from libs.core import EASTERN
from libs.db.dao import RiskEventDAO
from libs.db.models import StrategyPosition
from libs.infra.metrics import record_force_close

from libs.infra.redis_bus import RedisBus

from ..publisher import publish_force_close
from ..service import RiskService

_OPTION_MULTIPLIER = Decimal("100")
_ATR_TIGHTENER_MULTIPLIER = Decimal("0.5")


class HardStopRules:
    """Evaluate hard-stop style safeguards on each minute bar."""

    def __init__(self, risk_service: RiskService, redis_bus: RedisBus) -> None:
        self._risk_service = risk_service
        self._redis_bus = redis_bus

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
