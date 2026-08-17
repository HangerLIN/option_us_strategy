from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    JSON,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Numeric,
    String,
    Text,
    func,
    BigInteger,
    CheckConstraint,
    Boolean,
    Index,
    SmallInteger,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class StrategyPosition(Base):
    """Tracks live exposures per strategy, symbol, and asset bucket."""

    __tablename__ = "strategy_positions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    strategy_code: Mapped[str] = mapped_column(String(32), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    asset_type: Mapped[str] = mapped_column(
        String(16), nullable=False, default="OPTION", server_default="OPTION"
    )
    option_right: Mapped[str | None] = mapped_column(
        Enum("CALL", "PUT", name="option_right"), nullable=True
    )
    open_quantity: Mapped[int] = mapped_column(default=0, nullable=False)
    avg_open_price: Mapped[Decimal] = mapped_column(Numeric(14, 6), nullable=False)
    mark_price: Mapped[Decimal] = mapped_column(Numeric(14, 6), nullable=False)
    source_signal_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    conid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    expiry: Mapped[date | None] = mapped_column(Date, nullable=True)
    delta: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class RiskLimit(Base):
    __tablename__ = "risk_limits"

    limit_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(64), nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    scope: Mapped[str] = mapped_column(String(32), nullable=False, default="global")
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    bucket: Mapped[str | None] = mapped_column(String(64), nullable=True)
    effective_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    updated_by: Mapped[str | None] = mapped_column(String(64), nullable=True, default="system")

    __table_args__ = (
        CheckConstraint(
            "scope <> 'symbol' OR (symbol IS NOT NULL AND trim(symbol) <> '')",
            name="ck_risk_limits_symbol_scope",
        ),
        CheckConstraint(
            "scope <> 'symbol_bucket' OR (bucket IS NOT NULL AND trim(bucket) <> '')",
            name="ck_risk_limits_bucket_scope",
        ),
    )


class RiskState(Base):
    __tablename__ = "risk_state"

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    metric_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    metric_value: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    detail: Mapped[dict | None] = mapped_column(JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RiskEvent(Base):
    __tablename__ = "risk_events"

    event_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    event_code: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="INFO")
    payload: Mapped[dict | None] = mapped_column(JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OptionChainMeta(Base):
    __tablename__ = "option_chain_meta"

    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    conid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    underlying_symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    expiry: Mapped[date] = mapped_column(Date, nullable=False)
    right: Mapped[str] = mapped_column(Enum("CALL", "PUT", name="option_right"), nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    trading_class: Mapped[str | None] = mapped_column(String(32), nullable=True)
    multiplier: Mapped[str | None] = mapped_column(String(16), nullable=True)
    exchange: Mapped[str | None] = mapped_column(String(32), nullable=True)
    dte: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    delta: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    gamma: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    theta: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    vega: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    implied_vol: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    open_interest: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    underlying_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    min_tick: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("ix_option_chain_meta_symbol_date", "underlying_symbol", "trade_date"),
        Index("ix_option_chain_meta_trade_date_right", "trade_date", "right"),
    )


class OptionMinuteBar(Base):
    __tablename__ = "bars1m_option"

    conid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ts_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    underlying_symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    expiry: Mapped[date] = mapped_column(Date, nullable=False)
    right: Mapped[str] = mapped_column(Enum("CALL", "PUT", name="option_right"), nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    trading_class: Mapped[str | None] = mapped_column(String(32), nullable=True)
    multiplier: Mapped[str | None] = mapped_column(String(16), nullable=True)
    exchange: Mapped[str | None] = mapped_column(String(32), nullable=True)
    bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    last: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    open_interest: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    implied_vol: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    delta: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    gamma: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    theta: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    vega: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    underlying_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        CheckConstraint("date_trunc('minute', ts_end) = ts_end", name="ck_bars1m_option_minute"),
        Index("ix_bars1m_option_symbol_ts", "underlying_symbol", "ts_end"),
    )


class Order(Base):
    __tablename__ = "orders"

    order_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_order_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    strategy_code: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    asset_type: Mapped[str] = mapped_column(
        String(16), nullable=False, default="OPTION", server_default="OPTION"
    )
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    order_type: Mapped[str] = mapped_column(String(16), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    rejection_code: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Fill(Base):
    __tablename__ = "fills"

    fill_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        ForeignKey("orders.order_id", ondelete="CASCADE"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    asset_type: Mapped[str] = mapped_column(
        String(16), nullable=False, default="OPTION", server_default="OPTION"
    )
    fill_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fill_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    fill_quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    execution_venue: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Position(Base):
    __tablename__ = "positions"

    strategy_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    asset_type: Mapped[str] = mapped_column(
        String(16), primary_key=True, default="OPTION", server_default="OPTION"
    )
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    avg_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(
        Numeric(18, 6), nullable=False, default=Decimal("0")
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PnLIntraday(Base):
    __tablename__ = "pnl_intraday"

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    strategy_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    asset_type: Mapped[str] = mapped_column(
        String(16), primary_key=True, default="OPTION", server_default="OPTION"
    )
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    realized: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=Decimal("0"))
    unrealized: Mapped[Decimal] = mapped_column(
        Numeric(18, 6), nullable=False, default=Decimal("0")
    )
    fees: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=Decimal("0"))


class PnLDaily(Base):
    __tablename__ = "pnl_daily"

    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    strategy_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    asset_type: Mapped[str] = mapped_column(
        String(16), primary_key=True, default="OPTION", server_default="OPTION"
    )
    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    realized: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=Decimal("0"))
    unrealized: Mapped[Decimal] = mapped_column(
        Numeric(18, 6), nullable=False, default=Decimal("0")
    )
    fees: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=Decimal("0"))


class BacktestRun(Base):
    __tablename__ = "bt_runs"

    run_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    strategy_code: Mapped[str] = mapped_column(String(64), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    strategy_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    calibration_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    data_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    data_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    parameters: Mapped[dict | None] = mapped_column(JSON, default=None)
    notes: Mapped[str | None] = mapped_column(Text())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BacktestTrade(Base):
    __tablename__ = "bt_trades"

    trade_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("bt_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    asset_type: Mapped[str] = mapped_column(
        String(16), nullable=False, default="OPTION", server_default="OPTION"
    )
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    trade_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    trace_id: Mapped[str | None] = mapped_column(Text(), nullable=True)
    option_right: Mapped[str | None] = mapped_column(String(8), nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    expiry: Mapped[date | None] = mapped_column(Date, nullable=True)
    fees: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False, default=Decimal("0"))
    slippage: Mapped[Decimal] = mapped_column(
        Numeric(18, 6), nullable=False, default=Decimal("0")
    )
    reason_code: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BacktestSignal(Base):
    __tablename__ = "bt_signals"

    signal_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("bt_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    asset_type: Mapped[str] = mapped_column(
        String(16), nullable=False, default="OPTION", server_default="OPTION"
    )
    signal_code: Mapped[str] = mapped_column(String(64), nullable=False)
    ts_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    accepted: Mapped[bool | None] = mapped_column(default=None)
    reason: Mapped[str | None] = mapped_column(Text(), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BacktestOrderEvent(Base):
    __tablename__ = "bt_order_events"

    event_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("bt_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    trace_id: Mapped[str | None] = mapped_column(Text(), nullable=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    asset_type: Mapped[str] = mapped_column(
        String(16), nullable=False, default="OPTION", server_default="OPTION"
    )
    signal_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    side: Mapped[str | None] = mapped_column(String(8), nullable=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    signal_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    order_submit_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    fill_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    signal_to_fill_seconds: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6), nullable=True
    )
    order_ttl_seconds: Mapped[int | None] = mapped_column(nullable=True)
    is_stale_fill: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    order_attempts: Mapped[int | None] = mapped_column(nullable=True)
    execution_mode: Mapped[str | None] = mapped_column(String(32), nullable=True)
    original_limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    final_limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    fill_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    option_bid_at_signal: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    option_ask_at_signal: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    option_mid_at_signal: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    option_bid_at_fill: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    option_ask_at_fill: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    option_mid_at_fill: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    option_spread_abs_at_fill: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6), nullable=True
    )
    option_spread_pct_at_fill: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6), nullable=True
    )
    underlying_price_at_signal: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6), nullable=True
    )
    underlying_price_at_fill: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 6), nullable=True
    )
    vwap_at_signal: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    vwap_at_fill: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    opening_range_high: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    opening_range_low: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    above_vwap_at_signal: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    above_vwap_at_fill: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    above_orh_at_signal: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    above_orh_at_fill: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    stale_reject_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)
    option_right: Mapped[str | None] = mapped_column(String(8), nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    expiry: Mapped[date | None] = mapped_column(Date, nullable=True)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    payload: Mapped[dict | None] = mapped_column(JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class BacktestMetricDaily(Base):
    __tablename__ = "bt_metrics_daily"

    run_id: Mapped[int] = mapped_column(
        ForeignKey("bt_runs.run_id", ondelete="CASCADE"), primary_key=True
    )
    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    metric_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    metric_value: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)


class BacktestMetricTotal(Base):
    __tablename__ = "bt_metrics_total"

    run_id: Mapped[int] = mapped_column(
        ForeignKey("bt_runs.run_id", ondelete="CASCADE"), primary_key=True
    )
    metric_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    metric_value: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)


class CalibrationRun(Base):
    __tablename__ = "calibration_runs"

    calibration_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    strategy_code: Mapped[str] = mapped_column(String(64), nullable=False)
    calibration_version: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="COMPLETED")
    train_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    train_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    validation_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    validation_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    test_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    test_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    objective_metric: Mapped[str | None] = mapped_column(String(64), nullable=True)
    maximize: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    metadata_json: Mapped[dict | None] = mapped_column("metadata", JSON, default=None)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ux_calibration_runs_strategy_version", "strategy_code", "calibration_version", unique=True),
    )


class CalibrationParam(Base):
    __tablename__ = "calibration_params"

    param_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    calibration_id: Mapped[int] = mapped_column(
        ForeignKey("calibration_runs.calibration_id", ondelete="CASCADE"), nullable=False
    )
    param_name: Mapped[str] = mapped_column(String(128), nullable=False)
    param_value: Mapped[dict] = mapped_column(JSON, nullable=False)
    param_group: Mapped[str] = mapped_column(String(64), nullable=False, default="strategy")

    __table_args__ = (
        Index("ix_calibration_params_run_name", "calibration_id", "param_name"),
    )


class CalibrationMetric(Base):
    __tablename__ = "calibration_metrics"

    metric_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    calibration_id: Mapped[int] = mapped_column(
        ForeignKey("calibration_runs.calibration_id", ondelete="CASCADE"), nullable=False
    )
    metric_name: Mapped[str] = mapped_column(String(128), nullable=False)
    metric_value: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    metric_scope: Mapped[str] = mapped_column(String(64), nullable=False, default="validation")

    __table_args__ = (
        Index("ix_calibration_metrics_run_name", "calibration_id", "metric_name"),
    )


class CalibrationArtifact(Base):
    __tablename__ = "calibration_artifacts"

    artifact_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    calibration_id: Mapped[int] = mapped_column(
        ForeignKey("calibration_runs.calibration_id", ondelete="CASCADE"), nullable=False
    )
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    uri: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict | None] = mapped_column("metadata", JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RefMarketCap(Base):
    __tablename__ = "ref_market_cap"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    market_cap_usd: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ComplianceWhitelistLargecap(Base):
    __tablename__ = "compliance_whitelist_largecap"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    min_market_cap_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 2), default=Decimal("100000000000")
    )


class EarningsCalendar(Base):
    __tablename__ = "earnings_calendar"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    earnings_date: Mapped[date] = mapped_column(Date, primary_key=True)
    source: Mapped[str | None] = mapped_column(String(32), nullable=True)
    confidence: Mapped[str | None] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class PremarketTop5(Base):
    __tablename__ = "premarket_top5"

    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    rank: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    ret_0925_0930: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)  # 盘前涨幅：(price_0925 - prev_close) / prev_close


class BacktestTop5(Base):
    __tablename__ = "bt_top5"

    batch_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)
    rank: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    ret_0925_0930: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    price_0925: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    prev_close: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    market_cap_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 2), nullable=True)
    m60_up: Mapped[bool | None] = mapped_column(Boolean, default=None)
    preearn_allowed: Mapped[bool | None] = mapped_column(Boolean, default=None)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    context: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
