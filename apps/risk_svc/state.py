from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from libs.core import EASTERN, utc_now
from libs.db.dao import RiskStateDAO
from libs.db.models import StrategyPosition
from libs.infra.metrics import set_risk_drawdown, set_risk_used_r
from libs.infra.redis_bus import RedisBus
from libs.schemas.assets import AssetType
from libs.schemas.events import BarsClosed

from .publisher import publish_risk_alert, publish_risk_block, publish_risk_unblock
from .service import RiskService
from .rules.hard_stops import HardStopRules
from .rules.gates import MidfailDetector

_OPTION_MULTIPLIER = Decimal("100")


class RiskStateAggregator:
    """Aggregate and persist per-minute risk metrics from market events."""

    def __init__(
        self,
        session_factory: sessionmaker,
        risk_service: RiskService,
        redis_bus: RedisBus,
    ) -> None:
        self._session_factory = session_factory
        self._risk_service = risk_service
        self._redis_bus = redis_bus
        self._peak_equity: Dict[date, Decimal] = {}
        self._hard_stop_rules = HardStopRules(risk_service, redis_bus)
        self._midfail_detector = MidfailDetector()

    async def handle_bars_closed(self, payload: Dict[str, object], trace_id: Optional[str]) -> None:
        try:
            event = BarsClosed.model_validate(payload)
        except Exception:  # pragma: no cover - defensive
            return

        ts_end = event.bar_end
        trade_date = ts_end.astimezone(EASTERN).date()

        equity = Decimal("0")
        realized = Decimal("0")
        unrealized = Decimal("0")
        used_r = Decimal("0")
        drawdown_r = Decimal("0")
        num_positions = 0
        vix_value: Optional[Decimal] = None
        gate_open = True
        kill_switch = self._risk_service.kill_switch_active

        with self._session_factory() as session:
            positions = self._fetch_positions(session)
            num_positions, unrealized, total_notional = self._compute_position_metrics(positions)
            realized = self._fetch_realized_pnl(session, ts_end)
            equity = realized + unrealized
            used_r = self._compute_used_r(total_notional)
            drawdown_r = self._compute_drawdown(trade_date, equity)

            set_risk_used_r(used_r)
            set_risk_drawdown(drawdown_r)

            vix_value = self._risk_service.get_vix_value(session, ts_end)
            gate_open = self._is_gate_open(vix_value)

            kill_switch = await self._evaluate_kill_switch(realized, trace_id)

            states = self._build_state_rows(
                ts_end,
                equity,
                realized,
                unrealized,
                used_r,
                drawdown_r,
                num_positions,
                vix_value,
                gate_open,
                kill_switch,
            )

            dao = RiskStateDAO(session)
            dao.insert_states(states)
            await self._hard_stop_rules.evaluate(session, ts_end, positions, trace_id=trace_id)
            midfail = self._midfail_detector.evaluate(session, ts_end, event.symbol)
            if midfail:
                await self._risk_service.record_midfail(
                    event.symbol,
                    ts_end,
                    redis_bus=self._redis_bus,
                    trace_id=trace_id,
                )
            # 在09:31-09:35之间检查一次VIX并锁定当日决策
            await self._risk_service.check_vix_gate_at_open(
                session,
                ts_end,
                vix_value,
                redis_bus=self._redis_bus,
                trace_id=trace_id,
            )
            # 兜底：盘中VIX更新（如果未锁定）
            await self._risk_service.update_vix_gate(
                session,
                ts_end,
                vix_value,
                redis_bus=self._redis_bus,
                trace_id=trace_id,
            )
            session.commit()

    def _fetch_positions(self, session: Session) -> list[StrategyPosition]:
        rows = session.execute(select(StrategyPosition)).scalars()
        return list(rows)

    def _compute_position_metrics(
        self, positions: list[StrategyPosition]
    ) -> tuple[int, Decimal, Decimal]:
        total_notional = Decimal("0")
        unrealized = Decimal("0")
        num_positions = 0
        for pos in positions:
            qty = Decimal(str(pos.open_quantity))
            if qty == 0:
                continue
            mark = Decimal(str(pos.mark_price))
            avg = Decimal(str(pos.avg_open_price))
            multiplier = self._position_multiplier(pos)
            total_notional += abs(qty) * mark * multiplier
            unrealized += (mark - avg) * qty * multiplier
            num_positions += 1
        return num_positions, unrealized, total_notional

    @staticmethod
    def _position_multiplier(position: StrategyPosition) -> Decimal:
        raw = str(getattr(position, "asset_type", "OPTION") or "OPTION").upper()
        return _OPTION_MULTIPLIER if raw == AssetType.OPTION.value else Decimal("1")

    def _fetch_realized_pnl(self, session: Session, ts_end: datetime) -> Decimal:
        et_date = ts_end.astimezone(EASTERN).date()
        start_et = datetime.combine(et_date, time(0, 0), tzinfo=EASTERN)
        start_utc = start_et.astimezone(timezone.utc)
        end_utc = ts_end.astimezone(timezone.utc)
        rows = session.execute(
            text(
                """
                SELECT realized, fees
                FROM pnl_intraday
                WHERE ts >= :start_ts AND ts <= :end_ts
                """
            ),
            {"start_ts": start_utc, "end_ts": end_utc},
        ).all()
        realized = Decimal("0")
        for realized_val, fees in rows:
            realized += Decimal(str(realized_val or 0)) - Decimal(str(fees or 0))
        return realized

    def _compute_used_r(self, total_notional: Decimal) -> Decimal:
        risk_unit = self._risk_service.risk_unit_value
        if risk_unit <= 0:
            return Decimal("0")
        value = total_notional / risk_unit
        try:
            return value.quantize(Decimal("0.0001"))
        except InvalidOperation:
            return value

    def _compute_drawdown(self, trade_date: date, equity: Decimal) -> Decimal:
        peak = self._peak_equity.get(trade_date)
        if peak is None or equity > peak:
            self._peak_equity[trade_date] = equity
            return Decimal("0")
        risk_unit = self._risk_service.risk_unit_value
        if risk_unit <= 0:
            return Decimal("0")
        drawdown = (peak - equity) / risk_unit
        try:
            drawdown = drawdown.quantize(Decimal("0.0001"))
        except InvalidOperation:
            pass
        return max(Decimal("0"), drawdown)

    def _is_gate_open(self, vix_value: Optional[Decimal]) -> bool:
        if vix_value is None:
            return True
        return vix_value < self._risk_service.vix_gate

    async def _evaluate_kill_switch(self, realized: Decimal, trace_id: Optional[str]) -> bool:
        threshold = -self._risk_service.daily_loss_r * self._risk_service.risk_unit_value
        current = self._risk_service.kill_switch_active
        if realized <= threshold and not current:
            self._risk_service.set_kill_switch(True)
            await self._emit_kill_switch_alert(True, realized, threshold, trace_id)
            return True
        if realized > threshold and current:
            self._risk_service.set_kill_switch(False)
            await self._emit_kill_switch_alert(False, realized, threshold, trace_id)
            return False
        return current

    async def _emit_kill_switch_alert(
        self,
        active: bool,
        realized: Decimal,
        threshold: Decimal,
        trace_id: Optional[str],
    ) -> None:
        payload = {
            "symbol": "GLOBAL",
            "event_code": "KILL_SWITCH_ON" if active else "KILL_SWITCH_OFF",
            "realized_pnl": str(realized),
            "threshold": str(threshold),
            "kill_switch": active,
        }
        await publish_risk_alert(
            self._redis_bus,
            payload,
            trace_id=trace_id or str(uuid4()),
        )
        if active:
            triggered_at = utc_now()
            await publish_risk_block(
                self._redis_bus,
                {
                    "strategy_code": "core-vol",
                    "symbol": "GLOBAL",
                    "metric_code": "KILL_SWITCH",
                    "metric_value": Decimal("1"),
                    "limit_code": "KILL_SWITCH",
                    "reason": "KILL_SWITCH_ON",
                    "triggered_at": triggered_at,
                },
                trace_id=trace_id or f"kill-switch-block-{triggered_at.isoformat()}",
            )
        else:
            triggered_at = utc_now()
            await publish_risk_unblock(
                self._redis_bus,
                {
                    "strategy_code": "core-vol",
                    "symbol": "GLOBAL",
                    "reason": "KILL_SWITCH_OFF",
                },
                trace_id=trace_id or f"kill-switch-unblock-{triggered_at.isoformat()}",
            )

    def _build_state_rows(
        self,
        ts_end: datetime,
        equity: Decimal,
        realized: Decimal,
        unrealized: Decimal,
        used_r: Decimal,
        drawdown_r: Decimal,
        num_positions: int,
        vix_value: Optional[Decimal],
        gate_open: bool,
        kill_switch: bool,
    ) -> list[Dict[str, object]]:
        regime = self._derive_regime(vix_value)
        states: list[Dict[str, object]] = [
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "EQUITY",
                "metric_value": equity,
                "detail": None,
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "REALIZED_PNL_DAY",
                "metric_value": realized,
                "detail": None,
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "UNREALIZED_PNL",
                "metric_value": unrealized,
                "detail": None,
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "USED_R",
                "metric_value": used_r,
                "detail": None,
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "DRAWDOWN_R",
                "metric_value": drawdown_r,
                "detail": None,
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "NUM_POSITIONS",
                "metric_value": Decimal(num_positions),
                "detail": None,
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "VIX_LAST",
                "metric_value": vix_value if vix_value is not None else Decimal("0"),
                "detail": None,
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "REGIME",
                "metric_value": Decimal("1"),
                "detail": self._safe_detail({"regime": regime}),
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "GATE_OPEN_CHASE",
                "metric_value": Decimal("1") if gate_open else Decimal("0"),
                "detail": None,
            },
            {
                "ts": ts_end,
                "symbol": "GLOBAL",
                "metric_code": "KILL_SWITCH",
                "metric_value": Decimal("1") if kill_switch else Decimal("0"),
                "detail": None,
            },
        ]
        return states

    @staticmethod
    def _derive_regime(vix_value: Optional[Decimal]) -> str:
        if vix_value is None:
            return "UNKNOWN"
        if vix_value < Decimal("15"):
            return "LOW"
        if vix_value < Decimal("25"):
            return "MID"
        return "HIGH"

    def _safe_detail(self, detail: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
        if detail is None:
            return None
        normalized: Dict[str, Any] = {}
        for key, value in dict(detail).items():
            if isinstance(value, Decimal):
                normalized[key] = str(value)
            elif isinstance(value, dict):
                normalized[key] = self._safe_detail(value)
            else:
                normalized[key] = value
        return normalized
