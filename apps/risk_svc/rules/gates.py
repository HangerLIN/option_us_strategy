from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

import structlog

from libs.core import EASTERN
from libs.db.dao import RiskEventDAO
from libs.infra.redis_bus import RedisBus

from ..publisher import publish_risk_alert

LOGGER = structlog.get_logger(__name__)


@dataclass
class VixGateState:
    open: bool
    last_value: Optional[Decimal]


class VixGateMonitor:
    """Track VIX threshold crossings and broadcast gate state changes."""

    def __init__(self, threshold: Decimal) -> None:
        self._threshold = threshold
        self._state = VixGateState(open=True, last_value=None)

    async def update(
        self,
        vix_value: Optional[Decimal],
        ts: datetime,
        session: Session,
        *,
        trace_id: Optional[str] = None,
    ) -> None:
        if vix_value is None:
            return
        gate_open = vix_value < self._threshold
        if gate_open == self._state.open:
            self._state.last_value = vix_value
            return

        event_code = "VIX_GATE_OFF" if gate_open else "VIX_GATE_ON"
        dao = RiskEventDAO(session)
        dao.create_event(
            event_ts=ts,
            event_code=event_code,
            severity="WARN" if gate_open else "ERROR",
            message=event_code.lower(),
            symbol="GLOBAL",
            payload={"vix": str(vix_value), "threshold": str(self._threshold)},
        )
        redis_bus = getattr(session.bind, "redis_bus", None)
        if isinstance(redis_bus, RedisBus):
            await publish_risk_alert(
                redis_bus,
                {
                    "symbol": "GLOBAL",
                    "event_code": event_code,
                    "vix": str(vix_value),
                },
                trace_id=trace_id or event_code,
            )
        else:
            LOGGER.warning("vix_gate.redis_unavailable", event_code=event_code)
        self._state.open = gate_open
        self._state.last_value = vix_value

    @property
    def is_open(self) -> bool:
        return self._state.open


class ConcurrencyTracker:
    """Track symbols attempted during the opening window."""

    def __init__(self, window_minutes: int, open_time: time) -> None:
        self._window = timedelta(minutes=window_minutes)
        self._open_time = open_time
        self._records: Dict[date, Dict[str, datetime]] = {}

    def register(self, trade_date: date, symbol: str, ts: datetime) -> None:
        day_records = self._records.setdefault(trade_date, {})
        day_records[symbol.upper()] = ts

    def within_window(self, ts: datetime) -> bool:
        start = datetime.combine(ts.date(), self._open_time, tzinfo=EASTERN)
        return start <= ts < start + self._window

    def prune(self, trade_date: date, cutoff: datetime) -> None:
        records = self._records.setdefault(trade_date, {})
        expired = [symbol for symbol, ts in records.items() if ts < cutoff]
        for symbol in expired:
            records.pop(symbol, None)

    def count(self, trade_date: date) -> int:
        return len(self._records.get(trade_date, {}))


class MidfailDetector:
    """Detect opening-midfail condition (touch mid band but no recovery)."""

    def __init__(self) -> None:
        self._tracking: Dict[str, datetime] = {}

    def evaluate(
        self,
        session: Session,
        event_ts: datetime,
        symbol: str,
        *,
        trace_id: Optional[str] = None,
    ) -> Optional[datetime]:
        et = event_ts.astimezone(EASTERN)
        if et.time() > time(9, 45):
            # clear any tracking once window ends
            self._tracking.pop(symbol.upper(), None)
            return None

        indicator = session.execute(
            text(
                """
                SELECT i.boll_mid
                FROM indicators_eq_1m i
                WHERE i.symbol = :symbol AND i.ts_end = :ts_end
                """
            ),
            {"symbol": symbol, "ts_end": event_ts},
        ).scalar_one_or_none()
        if indicator is None:
            return None

        bar = session.execute(
            text(
                """
                SELECT close, low
                FROM bars1m_equity
                WHERE symbol = :symbol AND ts_end = :ts_end
                """
            ),
            {"symbol": symbol, "ts_end": event_ts},
        ).fetchone()
        if not bar:
            return None
        close_px, low_px = map(Decimal, map(str, bar))
        mid = Decimal(str(indicator))
        symbol_key = symbol.upper()
        if symbol_key not in self._tracking:
            if low_px <= mid and close_px < mid:
                self._tracking[symbol_key] = event_ts
        else:
            touch_time = self._tracking[symbol_key]
            if close_px >= mid:
                # recovered, remove tracking
                self._tracking.pop(symbol_key, None)
            elif event_ts >= touch_time + timedelta(minutes=10):
                self._tracking.pop(symbol_key, None)
                return event_ts
        return None
