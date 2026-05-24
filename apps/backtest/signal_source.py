from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, Iterator, List
from uuid import uuid4

from sqlalchemy.orm import sessionmaker

from apps.backtest.dao import BacktestDAO
from apps.signal_svc.engine import SignalEngine
from apps.signal_svc.top5_source import Top5Source
from libs.schemas.events import BarsClosed
from libs.schemas.signals import (
    ENTRY_SIGNAL_CODES,
    EXIT_SIGNAL_CODES,
    SignalEnvelope,
    SignalSide,
    signal_side_for_code,
)


@dataclass
class SignalEvent:
    symbol: str
    ts_end: datetime
    signal_code: str
    trace_id: str
    side: SignalSide
    risk_hint: Dict[str, Any]
    option_hint: Dict[str, Any]
    ttl_seconds: int
    cooldown_seconds: int
    reason: Dict[str, Any]


ENTRY_CODES = ENTRY_SIGNAL_CODES
EXIT_CODES = EXIT_SIGNAL_CODES


def load_signals(
    *,
    mode: str,
    session_factory: sessionmaker,
    dao: BacktestDAO,
    symbols: Iterable[str],
    start: datetime,
    end: datetime,
    top5_source: Top5Source | None = None,
) -> List[SignalEvent]:
    mode = mode.lower()
    if mode not in {"recompute", "db"}:
        raise ValueError("signal mode must be 'recompute' or 'db'")

    if mode == "recompute":
        events = list(
            _generate_signals_recompute(
                session_factory, dao, symbols, start, end, top5_source=top5_source
            )
        )
    else:
        events = list(_generate_signals_db(dao, symbols, start, end))

    events.sort(key=lambda evt: (evt.ts_end, evt.symbol, evt.signal_code))
    return events


def _generate_signals_recompute(
    session_factory: sessionmaker,
    dao: BacktestDAO,
    symbols: Iterable[str],
    start: datetime,
    end: datetime,
    *,
    top5_source: Top5Source | None,
) -> Iterator[SignalEvent]:
    engine = SignalEngine(
        session_factory=session_factory,
        redis_bus=None,
        top5_source=top5_source,
        is_backtest=True,
    )
    for symbol in symbols:
        rows = dao.fetch_equity_bars(symbol=symbol, start_ts=start, end_ts=end)
        cumulative_notional = Decimal("0")
        cumulative_volume = Decimal("0")
        vwap_session_date: date | None = None
        for row in rows:
            ts_end = row.ts_end
            if isinstance(ts_end, datetime) and ts_end.tzinfo is None:
                ts_end = ts_end.replace(tzinfo=start.tzinfo)
            bar_end = ts_end
            bar_start = bar_end - timedelta(minutes=1)
            
            # 过滤盘前时段（RTH = Regular Trading Hours: 9:30-16:00）
            from libs.core import EASTERN
            bar_et = bar_end.astimezone(EASTERN) if bar_end.tzinfo else bar_end.replace(tzinfo=EASTERN)
            hour = bar_et.hour
            minute = bar_et.minute
            session_date = bar_et.date()

            if vwap_session_date != session_date:
                cumulative_notional = Decimal("0")
                cumulative_volume = Decimal("0")
                vwap_session_date = session_date
            
            # 只处理9:30-16:00的K线，排除盘前时段（8:00-9:30）
            if hour < 9 or (hour == 9 and minute < 30) or hour >= 16:
                continue

            volume_dec = Decimal(row.volume or 0)
            if volume_dec > 0:
                cumulative_notional += row.close * volume_dec
                cumulative_volume += volume_dec
            vwap_value = row.close if cumulative_volume <= 0 else cumulative_notional / cumulative_volume
            
            event = BarsClosed(
                trace_id=str(uuid4()),
                symbol=symbol,
                bar_start=bar_start,
                bar_end=bar_end,
                timeframe="1m",
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=int(row.volume or 0),
                vwap=vwap_value,
                source="backtest",
                received_at=bar_end,
            )
            envelopes = engine.process_bar(event)
            for signal in envelopes:
                if signal.generated_at < start or signal.generated_at >= end:
                    continue
                yield _envelope_to_event(signal, trace_id=str(uuid4()))


def _generate_signals_db(
    dao: BacktestDAO,
    symbols: Iterable[str],
    start: datetime,
    end: datetime,
) -> Iterator[SignalEvent]:
    for symbol in symbols:
        rows = dao.fetch_signals_window(symbol=symbol, start_ts=start, end_ts=end)
        for row in rows:
            if not row.accepted:
                continue
            ts_end = row.ts_end
            if isinstance(ts_end, datetime):
                if ts_end.tzinfo is None:
                    ts_end = ts_end.replace(tzinfo=timezone.utc)
                ts_end = ts_end.astimezone(timezone.utc).replace(tzinfo=None)
            signal_code = row.signal_code
            side = _determine_side(signal_code)
            trace_id = f"{symbol}-{ts_end.isoformat()}-{signal_code}"
            event = SignalEvent(
                symbol=symbol,
                ts_end=ts_end,
                signal_code=signal_code,
                trace_id=trace_id,
                side=side,
                risk_hint={"size": 1, "stop_pct": -0.08},
                option_hint={"dte": [2, 7], "otm_steps": [2, 5]},
                ttl_seconds=180,
                cooldown_seconds=600,
                reason={},
            )
            yield event


def _envelope_to_event(signal: SignalEnvelope, trace_id: str) -> SignalEvent:
    return SignalEvent(
        symbol=signal.symbol,
        ts_end=signal.generated_at,
        signal_code=signal.signal_code,
        trace_id=trace_id,
        side=signal.side,
        risk_hint=dict(signal.risk_hint),
        option_hint=dict(signal.option_hint),
        ttl_seconds=signal.ttl_seconds,
        cooldown_seconds=signal.cooldown_seconds,
        reason=dict(signal.reason),
    )


def _determine_side(signal_code: str) -> SignalSide:
    return signal_side_for_code(signal_code)
