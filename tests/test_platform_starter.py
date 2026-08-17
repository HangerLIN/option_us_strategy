from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from apps.exec_svc.main import _build_contract, _local_precheck, _risk_notional_price
from libs.calibration import (
    GridSearchCalibrationJob,
    WalkForwardSplit,
    persist_calibration_result,
)
from libs.db import Base, CalibrationMetric, CalibrationParam, CalibrationRun, StrategyPositionDAO
from libs.portfolio import AllocationBudget, Candidate, EqualWeightPortfolioConstructor
from libs.schemas.assets import AssetType, InstrumentRef
from libs.schemas.exec import ExecutionMode, ExecutionRequest, OrderSide
from libs.schemas.signals import SignalEnvelope, SignalSide


@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as db:
        yield db
        db.rollback()


def _signal(symbol: str, asset_type: AssetType, score: str) -> SignalEnvelope:
    return SignalEnvelope(
        strategy_code="starter",
        symbol=symbol,
        asset_type=asset_type,
        signal_code="SIG_STARTER_BUY",
        side=SignalSide.BUY,
        confidence=0.8,
        reason={"score": score},
        generated_at=datetime(2026, 5, 22, 13, 40, tzinfo=timezone.utc),
    )


def test_asset_models_validate_option_requirements() -> None:
    equity = InstrumentRef(asset_type=AssetType.EQUITY, symbol="AAPL")
    assert equity.option_right is None

    with pytest.raises(ValueError, match="OPTION instrument missing fields"):
        InstrumentRef(asset_type=AssetType.OPTION, symbol="AAPL")

    option = InstrumentRef(
        asset_type=AssetType.OPTION,
        symbol="AAPL",
        option_right="CALL",
        strike=Decimal("200"),
        expiry=date(2026, 6, 19),
    )
    assert option.asset_type == AssetType.OPTION


def test_portfolio_constructor_allocates_equity_and_option_candidates() -> None:
    equity = InstrumentRef(asset_type=AssetType.ETF, symbol="SPY")
    option = InstrumentRef(
        asset_type=AssetType.OPTION,
        symbol="AAPL",
        option_right="CALL",
        strike=Decimal("200"),
        expiry=date(2026, 6, 19),
    )
    candidates = [
        Candidate(equity, _signal("SPY", AssetType.ETF, "0.9"), Decimal("0.9")),
        Candidate(option, _signal("AAPL", AssetType.OPTION, "0.8"), Decimal("0.8")),
    ]

    decisions = EqualWeightPortfolioConstructor(max_candidates=2).construct(
        candidates,
        budget=AllocationBudget(strategy_code="starter", total_notional=Decimal("10000")),
        prices={
            "SPY": Decimal("500"),
            "OPTION:AAPL:20260619:CALL:200": Decimal("5"),
        },
    )

    assert [decision.instrument.asset_type for decision in decisions] == [
        AssetType.ETF,
        AssetType.OPTION,
    ]
    assert [decision.quantity for decision in decisions] == [Decimal("10"), Decimal("1000")]


def test_calibration_grid_search_and_persistence(session) -> None:
    split = WalkForwardSplit(
        train_start=date(2026, 1, 1),
        train_end=date(2026, 3, 31),
        validation_start=date(2026, 4, 1),
        validation_end=date(2026, 4, 30),
    )
    job = GridSearchCalibrationJob(
        strategy_code="starter",
        calibration_version="cal-v1",
        param_grid={"ttl": [60, 90], "spread_limit": [Decimal("0.1"), Decimal("0.2")]},
        objective=lambda params: {
            "profit_factor": Decimal(str(params["ttl"])) / Decimal("100")
            - Decimal(str(params["spread_limit"]))
        },
        metric_name="profit_factor",
        split=split,
    )

    result = job.run()
    run = persist_calibration_result(session, result)
    session.commit()

    stored_run = session.get(CalibrationRun, run.calibration_id)
    assert stored_run is not None
    assert stored_run.calibration_version == "cal-v1"
    params = session.execute(select(CalibrationParam)).scalars().all()
    metrics = session.execute(select(CalibrationMetric)).scalars().all()
    assert {param.param_name for param in params} == {"ttl", "spread_limit"}
    assert metrics[0].metric_name == "profit_factor"


def test_strategy_position_dao_supports_equity_positions(session) -> None:
    dao = StrategyPositionDAO(session)
    realized, position = dao.apply_fill(
        strategy_code="starter-equity",
        symbol="SPY",
        asset_type=AssetType.ETF.value,
        option_right=None,
        side="BUY",
        quantity=Decimal("10"),
        price=Decimal("500"),
        fees=Decimal("1"),
        filled_at=datetime(2026, 5, 22, 13, 40, tzinfo=timezone.utc),
    )
    session.flush()

    assert realized == Decimal("0")
    assert position.asset_type == "ETF"
    assert position.option_right is None
    assert position.open_quantity == 10
    assert dao.get_position("starter-equity", "SPY", None, asset_type="ETF") is not None


def test_exec_local_precheck_skips_option_rules_for_equity() -> None:
    request = ExecutionRequest(
        strategy_code="starter-equity",
        symbol="SPY",
        asset_type=AssetType.ETF,
        side=OrderSide.BUY,
        quantity=10,
        limit_price=Decimal("500"),
        tif="DAY",
        execution_mode=ExecutionMode.ADAPTIVE,
        trace_id="eq-1",
    )

    ok, code, message = _local_precheck(request)
    assert (ok, code, message) == (True, "OK", "local-pass")
    assert _risk_notional_price(request) == Decimal("500")
    assert _build_contract("SPY", request).secType == "STK"
