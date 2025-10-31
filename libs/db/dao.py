from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .models import (
    BacktestMetricDaily,
    BacktestMetricTotal,
    BacktestRun,
    BacktestTop5,
    ComplianceWhitelistLargecap,
    Fill,
    Order,
    Position,
    PremarketTop5,
    PnLDaily,
    PnLIntraday,
    RefMarketCap,
    RiskEvent,
    RiskState,
    StrategyPosition,
    BacktestSignal,
)
from libs.core.constants import EASTERN
from libs.db.dim_trading_calendar import get_trading_session


class StrategyPositionDAO:
    """Data access helpers for strategy_positions."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def list_positions(
        self,
        strategy_code: str | None = None,
        symbol: str | None = None,
        option_right: str | None = None,
    ) -> Sequence[StrategyPosition]:
        query = select(StrategyPosition)
        if strategy_code:
            query = query.where(StrategyPosition.strategy_code == strategy_code)
        if symbol:
            query = query.where(StrategyPosition.symbol == symbol)
        if option_right:
            query = query.where(StrategyPosition.option_right == option_right)
        return self._session.execute(query).scalars().all()

    def get_position(
        self, strategy_code: str, symbol: str, option_right: str
    ) -> StrategyPosition | None:
        stmt = select(StrategyPosition).where(
            StrategyPosition.strategy_code == strategy_code,
            StrategyPosition.symbol == symbol,
            StrategyPosition.option_right == option_right,
        )
        return self._session.execute(stmt).scalar_one_or_none()

    def apply_option_fill(
        self,
        *,
        strategy_code: str,
        symbol: str,
        option_right: str,
        side: str,
        quantity: Decimal,
        price: Decimal,
        fees: Decimal,
        delta: float | None,
        conid: int | None,
        strike: Decimal | None,
        expiry: date | None,
        signal_code: str | None,
        filled_at: datetime,
    ) -> tuple[Decimal, StrategyPosition]:
        quantity = Decimal(str(quantity))
        price = Decimal(str(price))
        signed_qty = quantity if side.upper() == "BUY" else -quantity
        stmt = select(StrategyPosition).where(
            StrategyPosition.strategy_code == strategy_code,
            StrategyPosition.symbol == symbol,
            StrategyPosition.option_right == option_right,
        )
        position = self._session.execute(stmt).scalar_one_or_none()
        if position is None:
            position = StrategyPosition(
                strategy_code=strategy_code,
                symbol=symbol,
                option_right=option_right,
                open_quantity=0,
                avg_open_price=Decimal("0"),
                mark_price=Decimal("0"),
            )
            self._session.add(position)

        qty_old = Decimal(str(position.open_quantity))
        avg_old = Decimal(str(position.avg_open_price))
        signed_qty_dec = Decimal(signed_qty)
        realized = Decimal("0")

        if qty_old == 0 or qty_old * signed_qty_dec > 0:
            qty_new = qty_old + signed_qty_dec
            if qty_new != 0:
                avg_new = ((qty_old * avg_old) + (signed_qty_dec * price)) / qty_new
            else:
                avg_new = Decimal("0")
        else:
            closing_qty = min(abs(qty_old), abs(signed_qty_dec))
            direction = Decimal("1") if qty_old > 0 else Decimal("-1")
            realized = closing_qty * (price - avg_old) * direction
            qty_new = qty_old + signed_qty_dec
            if qty_new == 0:
                avg_new = Decimal("0")
            else:
                avg_new = price

        position.open_quantity = int(qty_new)
        position.avg_open_price = avg_new
        position.mark_price = price
        if delta is not None:
            position.delta = Decimal(str(delta))
        if conid is not None:
            position.conid = conid
        if strike is not None:
            position.strike = Decimal(str(strike))
        if expiry is not None:
            position.expiry = expiry
        if qty_old == 0 and qty_new != 0:
            position.opened_at = filled_at
            position.source_signal_code = signal_code
        elif qty_new == 0:
            position.opened_at = None
            position.source_signal_code = None
            position.conid = None
            position.strike = None
            position.expiry = None
            position.delta = None
        elif qty_old == 0 or qty_old * qty_new < 0:
            position.opened_at = filled_at
            position.source_signal_code = signal_code
        position.updated_at = filled_at
        return realized, position

    def close_position(
        self, strategy_code: str, symbol: str, option_right: str
    ) -> StrategyPosition | None:
        position = self.get_position(strategy_code, symbol, option_right)
        if position is None:
            return None
        position.open_quantity = 0
        self._session.add(position)
        return position


class OrdersDAO:
    """Order lifecycle helpers supporting upsert and pagination."""

    _COLUMNS = {c.name for c in Order.__table__.columns}

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_orders(self, orders: Sequence[Mapping[str, Any]]) -> Sequence[Order]:
        stored: list[Order] = []
        for payload in orders:
            data = {k: v for k, v in dict(payload).items() if k in self._COLUMNS}
            client_order_id = data["client_order_id"]
            existing = self._session.execute(
                select(Order).where(Order.client_order_id == client_order_id)
            ).scalar_one_or_none()
            if existing:
                for key, value in data.items():
                    setattr(existing, key, value)
                stored.append(existing)
            else:
                order = Order(**data)
                self._session.add(order)
                stored.append(order)
        self._session.flush()
        return stored

    def list_orders_paginated(self, *, symbol: str, page: int, page_size: int) -> Sequence[Order]:
        if page < 0 or page_size <= 0:
            raise ValueError("page must be >= 0 and page_size > 0")
        query = (
            select(Order)
            .where(Order.symbol == symbol)
            .order_by(Order.created_at.desc())
            .offset(page * page_size)
            .limit(page_size)
        )
        return self._session.execute(query).scalars().all()

    def insert_fills(self, fills: Sequence[Mapping[str, Any]]) -> None:
        columns = {c.name for c in Fill.__table__.columns}
        objects = [Fill(**{k: v for k, v in dict(fill).items() if k in columns}) for fill in fills]
        if objects:
            self._session.bulk_save_objects(objects)
            self._session.flush()


class RiskStateDAO:
    """Hypertable-style inserts for intraday risk metrics."""

    _COLUMNS = {c.name for c in RiskState.__table__.columns}

    def __init__(self, session: Session) -> None:
        self._session = session

    def insert_states(self, states: Sequence[Mapping[str, Any]]) -> None:
        """Insert or update risk states (upsert on conflict)."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        
        if not states:
            return
        
        for state in states:
            filtered = {k: v for k, v in dict(state).items() if k in self._COLUMNS}
            stmt = pg_insert(RiskState).values(**filtered)
            # On conflict, update metric_value and detail
            stmt = stmt.on_conflict_do_update(
                index_elements=["ts", "symbol", "metric_code"],
                set_={"metric_value": stmt.excluded.metric_value, "detail": stmt.excluded.detail}
            )
            self._session.execute(stmt)
        self._session.flush()

    def latest_for_symbol(self, symbol: str, metric_code: str) -> RiskState | None:
        stmt = (
            select(RiskState)
            .where(RiskState.symbol == symbol, RiskState.metric_code == metric_code)
            .order_by(RiskState.ts.desc())
            .limit(1)
        )
        return self._session.execute(stmt).scalar_one_or_none()


class PnLIntradayDAO:
    """Convenience methods for recording PnL intraday points."""

    _COLUMNS = {c.name for c in PnLIntraday.__table__.columns}

    def __init__(self, session: Session) -> None:
        self._session = session

    def insert_points(self, points: Sequence[Mapping[str, Any]]) -> None:
        objects = [
            PnLIntraday(**{k: v for k, v in dict(point).items() if k in self._COLUMNS})
            for point in points
        ]
        if objects:
            self._session.bulk_save_objects(objects)
            self._session.flush()


class BacktestResultDAO:
    """Persistence helpers for backtest runs and metrics."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def create_run(
        self,
        *,
        strategy_code: str,
        started_at: datetime,
        status: str,
        parameters: Mapping[str, Any] | None = None,
        notes: str | None = None,
    ) -> BacktestRun:
        run = BacktestRun(
            strategy_code=strategy_code,
            started_at=started_at,
            status=status,
            parameters=dict(parameters) if parameters else None,
            notes=notes,
        )
        self._session.add(run)
        self._session.flush()
        return run

    def add_daily_metrics(self, metrics: Iterable[Mapping[str, Any]]) -> None:
        columns = {c.name for c in BacktestMetricDaily.__table__.columns}
        objects = [
            BacktestMetricDaily(**{k: v for k, v in dict(metric).items() if k in columns})
            for metric in metrics
        ]
        if objects:
            self._session.bulk_save_objects(objects)
            self._session.flush()

    def add_total_metrics(self, metrics: Iterable[Mapping[str, Any]]) -> None:
        columns = {c.name for c in BacktestMetricTotal.__table__.columns}
        objects = [
            BacktestMetricTotal(**{k: v for k, v in dict(metric).items() if k in columns})
            for metric in metrics
        ]
        if objects:
            self._session.bulk_save_objects(objects)
            self._session.flush()


class ReferenceDataDAO:
    """Access reference datasets such as market cap and whitelist."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_market_cap(self, symbol: str) -> RefMarketCap | None:
        return self._session.get(RefMarketCap, symbol)

    def get_whitelist_entry(self, symbol: str) -> ComplianceWhitelistLargecap | None:
        return self._session.get(ComplianceWhitelistLargecap, symbol)


class PremarketTop5DAO:
    """Helpers for managing premarket top5 table."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def replace(self, trade_date: date, rows: Sequence[Mapping[str, Any]]) -> None:
        self._session.query(PremarketTop5).filter(PremarketTop5.trade_date == trade_date).delete()
        objects = [
            PremarketTop5(
                trade_date=trade_date,
                rank=row["rank"],
                symbol=row["symbol"],
                ret_0925_0930=row["ret_0925_0930"],
            )
            for row in rows
        ]
        if objects:
            self._session.bulk_save_objects(objects)

    def fetch(self, trade_date: date) -> list[PremarketTop5]:
        """Fetch top5 records for a given date."""
        return (
            self._session.query(PremarketTop5)
            .filter(PremarketTop5.trade_date == trade_date)
            .order_by(PremarketTop5.rank)
            .all()
        )
    
    def clear(self, trade_date: date) -> None:
        self._session.query(PremarketTop5).filter(PremarketTop5.trade_date == trade_date).delete()


class BacktestTop5DAO:
    """Manage backtest-specific Top5 snapshots keyed by batch."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def replace(self, batch_id: str, trade_date: date, rows: Sequence[Mapping[str, Any]]) -> None:
        self._session.query(BacktestTop5).filter(
            BacktestTop5.batch_id == batch_id,
            BacktestTop5.trade_date == trade_date,
        ).delete()
        objects = [
            BacktestTop5(
                batch_id=batch_id,
                trade_date=trade_date,
                rank=int(row["rank"]),
                symbol=str(row["symbol"]).upper(),
                ret_0925_0930=row["ret_0925_0930"],
                price_0925=row.get("price_0925"),
                prev_close=row.get("prev_close"),
                market_cap_usd=row.get("market_cap_usd"),
                m60_up=row.get("m60_up"),
                preearn_allowed=row.get("preearn_allowed"),
                source=row.get("source"),
                context=row.get("context"),
            )
            for row in rows
        ]
        if objects:
            self._session.bulk_save_objects(objects)
            self._session.flush()

    def delete_batch(self, batch_id: str) -> None:
        self._session.query(BacktestTop5).filter(BacktestTop5.batch_id == batch_id).delete()

    def symbols_for_date(self, batch_id: str, trade_date: date) -> list[str]:
        stmt = (
            select(BacktestTop5.symbol)
            .where(
                BacktestTop5.batch_id == batch_id,
                BacktestTop5.trade_date == trade_date,
            )
            .order_by(BacktestTop5.rank.asc())
        )
        rows = self._session.execute(stmt).scalars().all()
        return [symbol.upper() for symbol in rows]

    def symbols_between(self, batch_id: str, start_date: date, end_date: date) -> list[str]:
        stmt = (
            select(BacktestTop5.symbol)
            .where(
                BacktestTop5.batch_id == batch_id,
                BacktestTop5.trade_date >= start_date,
                BacktestTop5.trade_date <= end_date,
            )
            .distinct()
        )
        rows = self._session.execute(stmt).scalars().all()
        return [symbol.upper() for symbol in rows]

    def list_trade_dates(self, batch_id: str) -> list[date]:
        stmt = (
            select(BacktestTop5.trade_date)
            .where(BacktestTop5.batch_id == batch_id)
            .distinct()
            .order_by(BacktestTop5.trade_date.asc())
        )
        return self._session.execute(stmt).scalars().all()


class RiskEventDAO:
    """Persist risk events emitted by services."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def create_event(
        self,
        *,
        event_ts: datetime,
        event_code: str,
        severity: str,
        message: str,
        symbol: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> RiskEvent:
        payload_dict = dict(payload or {})
        if message and "message" not in payload_dict:
            payload_dict["message"] = message
        event = RiskEvent(
            event_ts=event_ts,
            event_code=event_code,
            severity=severity,
            symbol=symbol,
            payload=payload_dict,
        )
        self._session.add(event)
        return event

    def list_events(
        self,
        *,
        start_ts: datetime,
        end_ts: datetime | None = None,
        symbol: str | None = None,
        event_code: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> Sequence[RiskEvent]:
        query = select(RiskEvent).where(RiskEvent.event_ts >= start_ts)
        if end_ts:
            query = query.where(RiskEvent.event_ts <= end_ts)
        if symbol:
            query = query.where(RiskEvent.symbol == symbol.upper())
        if event_code:
            query = query.where(RiskEvent.event_code == event_code.upper())
        query = (
            query.order_by(RiskEvent.event_ts.desc()).offset(max(0, offset)).limit(max(1, limit))
        )
        return self._session.execute(query).scalars().all()


class PnLDAO:
    """Maintain live positions and realised PnL aggregates."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def apply_fill(
        self,
        *,
        strategy_code: str,
        symbol: str,
        side: str,
        quantity: Decimal,
        price: Decimal,
        fees: Decimal,
        filled_at: datetime,
    ) -> tuple[Decimal, Position]:
        quantity = Decimal(quantity)
        price = Decimal(price)
        fees = Decimal(fees)
        signed_qty = quantity if side.upper() == "BUY" else -quantity

        position = self._session.get(Position, {"strategy_code": strategy_code, "symbol": symbol})
        if position is None:
            position = Position(
                strategy_code=strategy_code,
                symbol=symbol,
                quantity=Decimal("0"),
                avg_price=Decimal("0"),
                unrealized_pnl=Decimal("0"),
            )
            self._session.add(position)
            existing_qty = Decimal("0")
            avg_price = Decimal("0")
        else:
            existing_qty = Decimal(position.quantity)
            avg_price = Decimal(position.avg_price)

        realized = Decimal("0")
        new_qty = existing_qty + signed_qty

        if existing_qty == 0 or existing_qty * signed_qty > 0:
            if new_qty != 0:
                position.avg_price = ((existing_qty * avg_price) + (signed_qty * price)) / new_qty
            else:
                position.avg_price = Decimal("0")
        else:
            closing_qty = min(abs(existing_qty), abs(signed_qty))
            direction = Decimal("1") if existing_qty > 0 else Decimal("-1")
            realized = closing_qty * (price - avg_price) * direction
            if new_qty == 0:
                position.avg_price = Decimal("0")
            elif existing_qty > 0 and new_qty < 0:
                position.avg_price = price
            elif existing_qty < 0 and new_qty > 0:
                position.avg_price = price
            else:
                position.avg_price = avg_price

        position.quantity = new_qty
        position.unrealized_pnl = Decimal("0")

        net_realized = realized - fees
        self._upsert_intraday(
            ts=filled_at,
            strategy_code=strategy_code,
            symbol=symbol,
            realized=net_realized,
            unrealized=Decimal("0"),
            fees=fees,
        )
        self._upsert_daily(
            ts=filled_at,
            strategy_code=strategy_code,
            symbol=symbol,
            realized=net_realized,
            unrealized=Decimal("0"),
            fees=fees,
        )

        return net_realized, position

    def _upsert_intraday(
        self,
        *,
        ts: datetime,
        strategy_code: str,
        symbol: str,
        realized: Decimal,
        unrealized: Decimal,
        fees: Decimal,
    ) -> None:
        record = self._session.get(
            PnLIntraday, {"ts": ts, "strategy_code": strategy_code, "symbol": symbol}
        )
        if record is None:
            record = PnLIntraday(
                ts=ts,
                strategy_code=strategy_code,
                symbol=symbol,
                realized=realized,
                unrealized=unrealized,
                fees=fees,
            )
            self._session.add(record)
        else:
            record.realized += realized
            record.unrealized += unrealized
            record.fees += fees

    def _upsert_daily(
        self,
        *,
        ts: datetime,
        strategy_code: str,
        symbol: str,
        realized: Decimal,
        unrealized: Decimal,
        fees: Decimal,
    ) -> None:
        trade_date = ts.astimezone(timezone.utc).date()
        record = self._session.get(
            PnLDaily,
            {"trade_date": trade_date, "strategy_code": strategy_code, "symbol": symbol},
        )
        if record is None:
            record = PnLDaily(
                trade_date=trade_date,
                strategy_code=strategy_code,
                symbol=symbol,
                realized=realized,
                unrealized=unrealized,
                fees=fees,
            )
            self._session.add(record)
        else:
            record.realized += realized
            record.unrealized += unrealized
            record.fees += fees

    def list_positions(self, *, strategy_code: str | None = None) -> Sequence[Position]:
        stmt = select(Position)
        if strategy_code:
            stmt = stmt.where(Position.strategy_code == strategy_code)
        return self._session.execute(stmt).scalars().all()

    def list_daily_pnl(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        strategy_code: str | None = None,
    ) -> Sequence[PnLDaily]:
        stmt = select(PnLDaily)
        if strategy_code:
            stmt = stmt.where(PnLDaily.strategy_code == strategy_code)
        if start_date:
            stmt = stmt.where(PnLDaily.trade_date >= start_date)
        if end_date:
            stmt = stmt.where(PnLDaily.trade_date <= end_date)
        stmt = stmt.order_by(PnLDaily.trade_date.asc())
        return self._session.execute(stmt).scalars().all()


class SignalLogDAO:
    """Helpers for recording generated signals for backtesting and audit."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def ensure_run(self, strategy_code: str) -> BacktestRun:
        stmt = (
            select(BacktestRun)
            .where(BacktestRun.strategy_code == strategy_code)
            .order_by(BacktestRun.started_at.desc())
            .limit(1)
        )
        run = self._session.execute(stmt).scalar_one_or_none()
        if run is not None:
            return run
        run = BacktestRun(
            strategy_code=strategy_code,
            started_at=datetime.now(timezone.utc),
            status="LIVE",
        )
        self._session.add(run)
        self._session.flush()
        return run

    def record_signal(
        self,
        *,
        run_id: int,
        symbol: str,
        signal_code: str,
        ts_end: datetime,
        accepted: bool | None = None,
        reason: str | None = None,
    ) -> BacktestSignal:
        entry = BacktestSignal(
            run_id=run_id,
            symbol=symbol,
            signal_code=signal_code,
            ts_end=ts_end,
            accepted=accepted,
            reason=reason,
        )
        self._session.add(entry)
        return entry
