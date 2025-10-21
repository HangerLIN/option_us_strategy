from __future__ import annotations

from datetime import datetime, timezone, date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session, sessionmaker

from libs.db import (
    BacktestMetricDaily,
    BacktestMetricTotal,
    BacktestResultDAO,
    Base,
    OrdersDAO,
    RiskEventDAO,
    RiskStateDAO,
)


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        yield session
        session.rollback()


def test_orders_dao_upsert_and_paginate(session: Session) -> None:
    dao = OrdersDAO(session)
    created1 = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    created2 = datetime(2024, 7, 1, 11, 0, tzinfo=timezone.utc)

    dao.upsert_orders(
        [
            {
                "client_order_id": "coid-1",
                "strategy_code": "core-vol",
                "symbol": "AAPL",
                "side": "BUY",
                "order_type": "LMT",
                "quantity": Decimal("10"),
                "limit_price": Decimal("150"),
                "status": "NEW",
                "created_at": created1,
            },
            {
                "client_order_id": "coid-2",
                "strategy_code": "core-vol",
                "symbol": "AAPL",
                "side": "SELL",
                "order_type": "MKT",
                "quantity": Decimal("5"),
                "status": "NEW",
                "created_at": created2,
            },
        ]
    )

    dao.upsert_orders(
        [
            {
                "client_order_id": "coid-1",
                "status": "FILLED",
                "quantity": Decimal("10"),
            }
        ]
    )
    session.commit()

    first_page = dao.list_orders_paginated(symbol="AAPL", page=0, page_size=1)
    assert len(first_page) == 1
    assert first_page[0].client_order_id == "coid-2"

    second_page = dao.list_orders_paginated(symbol="AAPL", page=1, page_size=1)
    assert len(second_page) == 1
    assert second_page[0].client_order_id == "coid-1"
    assert second_page[0].status == "FILLED"


def test_risk_state_dao_inserts(session: Session) -> None:
    dao = RiskStateDAO(session)
    ts1 = datetime(2024, 7, 1, 9, 30, tzinfo=timezone.utc)
    ts2 = datetime(2024, 7, 1, 9, 31, tzinfo=timezone.utc)

    dao.insert_states(
        [
            {"ts": ts1, "symbol": "AAPL", "metric_code": "NET", "metric_value": Decimal("1000")},
            {"ts": ts2, "symbol": "AAPL", "metric_code": "NET", "metric_value": Decimal("1100")},
        ]
    )
    session.commit()

    latest = dao.latest_for_symbol("AAPL", "NET")
    assert latest is not None
    assert latest.metric_value == Decimal("1100")


def test_backtest_result_dao_metrics(session: Session) -> None:
    dao = BacktestResultDAO(session)
    run = dao.create_run(
        strategy_code="core-vol", started_at=datetime.now(timezone.utc), status="RUNNING"
    )

    dao.add_daily_metrics(
        [
            {
                "run_id": run.run_id,
                "trade_date": date(2024, 7, 1),
                "metric_code": "return",
                "metric_value": Decimal("0.01"),
            }
        ]
    )
    dao.add_total_metrics(
        [
            {
                "run_id": run.run_id,
                "metric_code": "sharpe",
                "metric_value": Decimal("1.5"),
            }
        ]
    )
    session.commit()

    daily_metrics = (
        session.execute(select(BacktestMetricDaily).where(BacktestMetricDaily.run_id == run.run_id))
        .scalars()
        .all()
    )
    assert len(daily_metrics) == 1
    assert daily_metrics[0].metric_code == "return"

    total_metrics = (
        session.execute(select(BacktestMetricTotal).where(BacktestMetricTotal.run_id == run.run_id))
        .scalars()
        .all()
    )
    assert len(total_metrics) == 1
    assert total_metrics[0].metric_value == Decimal("1.5")


def test_risk_event_dao_pagination(session: Session) -> None:
    dao = RiskEventDAO(session)
    base_ts = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    for idx in range(3):
        dao.create_event(
            event_ts=base_ts + timedelta(minutes=idx),
            event_code="TEST",
            severity="INFO",
            message="test",
            symbol="AAPL",
            payload={"reason": f"reason-{idx}"},
        )
    session.commit()

    first_page = dao.list_events(start_ts=base_ts - timedelta(minutes=1), limit=2, offset=0)
    second_page = dao.list_events(start_ts=base_ts - timedelta(minutes=1), limit=2, offset=2)

    assert len(first_page) == 2
    assert len(second_page) == 1
