from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List
from uuid import uuid4

from sqlalchemy.orm import sessionmaker

from apps.backtest.dao import BacktestDAO
from apps.signal_svc.engine import SignalEngine
from apps.signal_svc.top5_source import Top5Source
from libs.schemas.events import BarsClosed
from libs.schemas.signals import SignalEnvelope, SignalSide


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


ENTRY_CODES = {
    "SIG_OPEN_CHASE_BUY",
    "SIG_REBOUND_BUY",
    "SIG_PM_BOTTOM_A2",
    "SIG_PM_BOTTOM_A3",
    "SIG_PM_BOTTOM_A4",
}

EXIT_CODES = {
    "SIG_EXIT_UPPER_TAP_X2",
    "SIG_EXIT_BOX2MID",
    "SIG_TIME_CLEAR_12_14",
}


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
    )
    for symbol in symbols:
        rows = dao.fetch_equity_bars(symbol=symbol, start_ts=start, end_ts=end)
        for row in rows:
            ts_end = row.ts_end
            if isinstance(ts_end, datetime) and ts_end.tzinfo is None:
                ts_end = ts_end.replace(tzinfo=start.tzinfo)
            bar_end = ts_end
            bar_start = bar_end - timedelta(minutes=1)
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
                vwap=None,
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
    if signal_code in EXIT_CODES:
        return SignalSide.SELL
    return SignalSide.BUY
