from __future__ import annotations

from datetime import datetime, timedelta, timezone, time as time_cls
from decimal import Decimal
from typing import Iterable, Tuple

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from apps.risk_svc.rules.hard_stops import HardStopRules
from libs.db import Base
from libs.db.models import RiskEvent, StrategyPosition
from libs.schemas.risk import (
    RiskLimits,
    AmBottomLimits,
    AmSell1Limits,
    AmConfluenceLimits,
    AmConfluenceBuyLimits,
    AmConfluenceSellLimits,
    OvernightLimits,
    OvernightBandwidthLimits,
    OvernightReboundLimits,
    GateLimits,
    GateVixLimits,
)


class StubRiskService:
    stop_loss_pct = Decimal("0.08")
    cutoff_open_chase = time_cls(14, 0)
    cutoff_overnight = time_cls(12, 0)
    iv_overnight_cap = Decimal("1.0")
    vix_gate = Decimal("20")
    vix_gate_mode = "enforce"

    def current_limits(self) -> RiskLimits:
        updated = datetime.now(timezone.utc)
        return RiskLimits(
            notional_cap=Decimal("5000000"),
            updated_at=updated,
            am_bottom=AmBottomLimits(),
            am_sell1=AmSell1Limits(),
            am_conf=AmConfluenceLimits(
                buy=AmConfluenceBuyLimits(),
                sell=AmConfluenceSellLimits(),
            ),
            overnight=OvernightLimits(
                iv_max=float(self.iv_overnight_cap),
                bw=OvernightBandwidthLimits(),
                rebound=OvernightReboundLimits(),
            ),
            gate=GateLimits(vix=GateVixLimits(mode=self.vix_gate_mode, thresh=self.vix_gate)),
        )


class StubRedisBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict, str]] = []

    async def publish(
        self, event: str, payload: dict, *, trace_id: str, maxlen: int | None = None
    ) -> str:
        self.published.append((event, payload, trace_id))
        return "1-0"


@pytest.fixture()
def hard_stop_env() -> Iterable[Tuple[Session, HardStopRules, StubRedisBus]]:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE indicators_eq_1m (
                symbol TEXT NOT NULL,
                ts_end TIMESTAMP NOT NULL,
                atr14 NUMERIC,
                boll_mid NUMERIC,
                boll_up NUMERIC,
                boll_dn NUMERIC,
                ao NUMERIC,
                stoch_k NUMERIC,
                stoch_d NUMERIC,
                stoch_rsi_k NUMERIC,
                stoch_rsi_d NUMERIC,
                cci14 NUMERIC,
                cci6 NUMERIC,
                sma5 NUMERIC,
                lr_m5_slope NUMERIC,
                lr_boll_dn_slope NUMERIC,
                lr_obv_slope NUMERIC,
                obv NUMERIC,
                obv_ma6 NUMERIC,
                obv_ema20 NUMERIC,
                mfi14 NUMERIC,
                rvol6 NUMERIC
            )
            """
        )
        conn.exec_driver_sql("DROP TABLE IF EXISTS bars1m_option")
        conn.exec_driver_sql(
            """
            CREATE TABLE bars1m_option (
                underlying_symbol TEXT NOT NULL,
                ts_end TIMESTAMP NOT NULL,
                right TEXT NOT NULL,
                implied_vol NUMERIC
            )
            """
        )
    SessionLocal = sessionmaker(bind=engine, future=True)
    bus = StubRedisBus()
    rules = HardStopRules(StubRiskService(), bus)
    with SessionLocal() as session:
        yield session, rules, bus
        session.rollback()


def _add_position(
    session: Session,
    *,
    strategy_code: str = "core-vol",
    symbol: str = "AAPL",
    option_right: str = "CALL",
    open_quantity: int = 1,
    avg_price: Decimal = Decimal("10"),
    mark_price: Decimal = Decimal("10"),
    delta: Decimal | None = None,
    source_signal_code: str | None = None,
    opened_at: datetime | None = None,
) -> StrategyPosition:
    position = StrategyPosition(
        strategy_code=strategy_code,
        symbol=symbol,
        option_right=option_right,
        open_quantity=open_quantity,
        avg_open_price=avg_price,
        mark_price=mark_price,
        delta=delta,
        source_signal_code=source_signal_code,
        opened_at=opened_at,
    )
    session.add(position)
    session.flush()
    return position


@pytest.mark.asyncio
async def test_hard_stop_triggers_force_close(
    hard_stop_env: Tuple[Session, HardStopRules, StubRedisBus],
) -> None:
    session, rules, bus = hard_stop_env
    opened_at = datetime(2024, 7, 1, 14, 30, tzinfo=timezone.utc)
    position = _add_position(
        session,
        avg_price=Decimal("10"),
        mark_price=Decimal("9.1"),
        opened_at=opened_at,
    )
    ts_end = datetime(2024, 7, 1, 17, 30, tzinfo=timezone.utc)  # 13:30 ET

    await rules.evaluate(session, ts_end, [position], trace_id="test-hard-stop")

    events = session.execute(select(RiskEvent)).scalars().all()
    assert any(event.event_code == "HARD_STOP" for event in events)
    assert any(payload["reason"] == "HARD_STOP" for _, payload, _ in bus.published)


@pytest.mark.asyncio
async def test_atr_delta_tighter_triggers_force_close(
    hard_stop_env: Tuple[Session, HardStopRules, StubRedisBus],
) -> None:
    session, rules, bus = hard_stop_env
    ts_end = datetime(2024, 7, 1, 16, 0, tzinfo=timezone.utc)
    position = _add_position(
        session,
        avg_price=Decimal("2"),
        mark_price=Decimal("2"),
        delta=Decimal("0.6"),
        opened_at=ts_end - timedelta(hours=1),
    )
    session.execute(
        text(
            """
            INSERT INTO indicators_eq_1m (symbol, ts_end, atr14)
            VALUES (:symbol, :ts_end, :atr)
            """
        ),
        {"symbol": position.symbol, "ts_end": ts_end, "atr": float(1.0)},
    )

    await rules.evaluate(session, ts_end, [position], trace_id="test-atr-delta")

    events = session.execute(select(RiskEvent)).scalars().all()
    assert any(event.event_code == "ATR_DELTA" for event in events)
    assert any(payload["reason"] == "ATR_DELTA" for _, payload, _ in bus.published)


@pytest.mark.asyncio
async def test_overnight_rejects_vix_gate(
    hard_stop_env: Tuple[Session, HardStopRules, StubRedisBus], monkeypatch
) -> None:
    session, rules, bus = hard_stop_env
    position = _add_position(
        session,
        symbol="AAPL",
        option_right="CALL",
        avg_price=Decimal("2"),
        mark_price=Decimal("2.1"),
        opened_at=datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
    )
    ts_end = datetime(2025, 1, 2, 19, 55, tzinfo=timezone.utc)  # 14:55 ET

    monkeypatch.setattr(rules, "_current_vix_value", lambda session: Decimal("35"))

    await rules._evaluate_overnight(session, ts_end, [position], trace_id="test-overnight")

    events = session.execute(select(RiskEvent)).scalars().all()
    assert any(event.event_code == "OVERNIGHT_REJECT_VIX" for event in events)
    assert any(evt[0] == "force_close" for evt in bus.published)


@pytest.mark.asyncio
async def test_open_chase_cutoff_triggers_force_close(
    hard_stop_env: Tuple[Session, HardStopRules, StubRedisBus],
) -> None:
    session, rules, bus = hard_stop_env
    ts_end = datetime(2024, 7, 1, 18, 0, tzinfo=timezone.utc)  # 14:00 ET
    position = _add_position(
        session,
        source_signal_code="SIG_OPEN_CHASE_BUY",
        opened_at=ts_end - timedelta(hours=4),
    )

    await rules.evaluate(session, ts_end, [position], trace_id="test-open-chase")

    events = session.execute(select(RiskEvent)).scalars().all()
    assert any(event.event_code == "DAILY_CUTOFF" for event in events)
    assert any(payload["reason"] == "DAILY_CUTOFF" for _, payload, _ in bus.published)


@pytest.mark.asyncio
async def test_overnight_cutoff_triggers_force_close(
    hard_stop_env: Tuple[Session, HardStopRules, StubRedisBus],
) -> None:
    session, rules, bus = hard_stop_env
    ts_end = datetime(2024, 7, 2, 16, 5, tzinfo=timezone.utc)  # 12:05 ET next day
    opened_at = datetime(2024, 7, 1, 19, 0, tzinfo=timezone.utc)
    position = _add_position(
        session,
        opened_at=opened_at,
    )

    await rules.evaluate(session, ts_end, [position], trace_id="test-overnight")

    events = session.execute(select(RiskEvent)).scalars().all()
    assert any(event.event_code == "OVERNIGHT_CUTOFF" for event in events)
    assert any(payload["reason"] == "OVERNIGHT_CUTOFF" for _, payload, _ in bus.published)


@pytest.mark.asyncio
async def test_iv_cap_triggers_force_close(
    hard_stop_env: Tuple[Session, HardStopRules, StubRedisBus],
) -> None:
    session, rules, bus = hard_stop_env
    ts_end = datetime(2024, 7, 1, 19, 55, tzinfo=timezone.utc)  # 15:55 ET
    position = _add_position(
        session,
        mark_price=Decimal("2"),
        avg_price=Decimal("2"),
        delta=Decimal("0.5"),
        opened_at=ts_end - timedelta(hours=6),
    )
    session.execute(
        text(
            """
            INSERT INTO bars1m_option (underlying_symbol, ts_end, right, implied_vol)
            VALUES (:symbol, :ts_end, 'CALL', :iv)
            """
        ),
        {"symbol": position.symbol, "ts_end": ts_end, "iv": float(1.20)},
    )

    await rules.evaluate(session, ts_end, [position], trace_id="test-iv-cap")

    events = session.execute(select(RiskEvent)).scalars().all()
    assert any(event.event_code == "IV_OVERNIGHT_CAP" for event in events)
    assert any(payload["reason"] == "IV_OVERNIGHT_CAP" for _, payload, _ in bus.published)
