from __future__ import annotations

import asyncio
import json
import os
import threading
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Protocol, cast

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

import structlog

from libs.core import EASTERN, current_trace_context, get_settings, utc_now
from libs.db import RiskEventDAO, RiskStateDAO, StrategyPositionDAO
from libs.db.preearn import evaluate_preearn_guard
from libs.infra.metrics import (
    record_limit_approaching,
    record_risk_block,
    set_capital_utilization,
    set_kill_switch_state,
    set_limit_usage,
    set_position_count,
    set_risk_drawdown,
    set_risk_used_r,
    set_risk_vix,
    set_total_exposure,
)
from libs.infra import build_ibkr_client, IBClient
from libs.infra.redis_bus import RedisBus
from libs.schemas.risk import RiskCheckRequest, RiskLimits
from libs.schemas.signals import BUY_SIGNAL_CODES
from .block_store import RedisBlockStore
from .limits import LimitsCache
from .publisher import publish_risk_alert, publish_risk_block, publish_risk_unblock
import redis

LOGGER = structlog.get_logger(__name__)


@dataclass
class RiskDecision:
    approved: bool
    code: str
    detail: str
    utilisation: float
    reasons: Dict[str, object]


class ExposureView(Protocol):
    symbol: str
    open_quantity: Any


class RiskService:
    """Centralised risk rule evaluation and state updater."""

    _BUY_SIGNALS = BUY_SIGNAL_CODES

    _VIX_SENSITIVE = {"SIG_OPEN_CHASE_BUY"}

    def __init__(
        self,
        session_factory: sessionmaker,
        *,
        redis_url: str | None = None,
        redis_client: Optional[redis.Redis] = None,
    ) -> None:
        self._session_factory = session_factory
        settings = get_settings()
        self._settings = settings
        self._redis_url = redis_url or settings.redis_url
        try:
            self._redis: Optional[redis.Redis] = redis_client or redis.Redis.from_url(
                self._redis_url, decode_responses=True
            )
        except Exception:  # pragma: no cover - fallback when redis unavailable
            LOGGER.exception("risk_service.redis_init_failed")
            self._redis = None
        self._limits_cache = LimitsCache(session_factory)
        self._blocks = RedisBlockStore(self._redis_url, client=self._redis)
        self._stop_loss_pct = Decimal("0.08")
        self._iv_overnight_call_cap = Decimal("1.0")
        self._preearn_days_min = int(settings.preearn_days_min)
        self._preearn_days_max = int(settings.preearn_days_max)
        self._preearn_atr_pct_max = Decimal(str(settings.preearn_atr_pct_max))
        self._sync_limits()
        self._concurrency_window = timedelta(minutes=10)
        self._opening_time = time(9, 30)
        self._concurrent_tracker: Dict[date, List[Tuple[datetime, str]]] = {}
        self._current_trade_date: Optional[date] = None
        self._kill_switch_active: bool = False
        self._vix_gate_open: bool = True
        self._vix_cache: Tuple[Optional[Decimal], Optional[datetime]] = (None, None)
        self._ibkr_vix_enabled = self._determine_ibkr_vix_flag(settings)
        self._ibkr_vix_lock = threading.Lock()
        self._last_ibkr_vix_fetch: Optional[datetime] = None
        self._limits_updated_at: datetime = utc_now()
        self._vix_gate_mode: str = settings.vix_gate_mode
        # 当日VIX门控检查标志（09:31检查后固定决策）
        self._vix_checked_today: Optional[date] = None
        self._vix_gate_locked_today: bool = False
        self._restore_gate_state()
        self.reload_limits()
        self._metrics_enabled = bool(settings.monitoring_enabled)
        self._emit_metric(set_kill_switch_state, self._kill_switch_active)

    def current_limits(self) -> RiskLimits:
        return self._limits_cache.snapshot

    def reload_limits(self, *, refresh_settings: bool = False) -> RiskLimits:
        snapshot = self._limits_cache.reload(refresh_settings=refresh_settings)
        self._limits_updated_at = snapshot.updated_at
        self._sync_limits()
        return self.current_limits()

    @staticmethod
    def _determine_ibkr_vix_flag(settings) -> bool:
        env_flag = os.getenv("ENABLE_IBKR_VIX_SNAPSHOT")
        if env_flag is not None:
            return env_flag.strip().lower() not in {"0", "false", "no"}
        return settings.app_env in {"staging", "production"}

    @property
    def risk_unit_value(self) -> Decimal:
        return self._risk_unit_value

    @property
    def daily_loss_r(self) -> Decimal:
        return self._daily_loss_r

    @property
    def vix_gate(self) -> Decimal:
        return self._vix_gate

    @property
    def vix_gate_mode(self) -> str:
        return self._vix_gate_mode

    @property
    def kill_switch_active(self) -> bool:
        return self._kill_switch_active

    @property
    def vix_gate_open(self) -> bool:
        return self._vix_gate_open

    def set_kill_switch(self, active: bool) -> None:
        self._kill_switch_active = active
        self._emit_metric(set_kill_switch_state, active)

    @property
    def iv_overnight_cap(self) -> Decimal:
        return self._iv_overnight_call_cap

    def _emit_metric(
        self,
        func: Callable[..., None],
        *args: object,
        **kwargs: object,
    ) -> None:
        if not self._metrics_enabled:
            return
        try:
            func(*args, **kwargs)
        except Exception:  # pragma: no cover - metrics best-effort
            return

    def signal_emitted(
        self,
        strategy_code: str,
        symbol: Optional[str],
        signal_code: Optional[str],
        ts_utc: datetime,
    ) -> None:
        if not symbol or not self.is_buy_signal(signal_code):
            return
        now_et = self._to_eastern(ts_utc)
        self._append_concurrency_entry(symbol.upper(), now_et)

    def signal_filled(
        self,
        strategy_code: str,
        symbol: Optional[str],
        ts_utc: datetime,
    ) -> None:
        if not symbol:
            return
        now_et = self._to_eastern(ts_utc)
        self._remove_concurrency_symbol(symbol.upper(), now_et)

    def signal_expired(
        self,
        strategy_code: str,
        symbol: Optional[str],
        ts_utc: datetime,
    ) -> None:
        if not symbol:
            return
        now_et = self._to_eastern(ts_utc)
        self._remove_concurrency_symbol(symbol.upper(), now_et)

    def signal_sell(
        self,
        strategy_code: str,
        symbol: Optional[str],
        signal_code: Optional[str],
        ts_utc: datetime,
    ) -> None:
        if not symbol or not signal_code:
            return
        with self._session_factory() as session:
            dao = StrategyPositionDAO(session)
            positions = dao.list_positions(strategy_code=strategy_code, symbol=symbol.upper())
            updated = False
            for position in positions:
                if position.source_signal_code != signal_code:
                    position.source_signal_code = signal_code
                    position.updated_at = ts_utc
                    updated = True
            if updated:
                session.commit()

    def _restore_gate_state(self) -> None:
        if self._redis is None:
            return
        try:
            raw = self._redis.get("risk:gate:vix")
        except Exception:
            LOGGER.exception("risk_service.gate_state_restore_failed")
            return
        if not raw:
            return
        raw_str = cast(str, raw)
        try:
            data = json.loads(raw_str)
            self._vix_gate_open = bool(data.get("open", True))
        except (TypeError, ValueError, json.JSONDecodeError):
            LOGGER.warning("risk_service.gate_state_invalid", payload=raw)

    def _store_gate_state(self, gate_open: bool, updated_at: datetime, vix_value: Decimal) -> None:
        if self._redis is None:
            return
        payload = {
            "open": gate_open,
            "updated_at": updated_at.isoformat(),
            "vix": str(vix_value),
        }
        try:
            self._redis.set("risk:gate:vix", json.dumps(payload))
        except Exception:
            LOGGER.exception("risk_service.gate_state_persist_failed")

    def _read_vix_snapshot(self) -> Optional[Tuple[Decimal, datetime]]:
        if self._redis is None:
            return None
        try:
            raw = self._redis.get("risk:vix:last")
        except Exception:
            LOGGER.exception("risk_service.vix_snapshot_read_failed")
            return None
        if not raw:
            return None
        raw_str = cast(str, raw)
        try:
            data = json.loads(raw_str)
            value = Decimal(str(data["value"]))
            ts_raw = data.get("ts")
            if ts_raw is None:
                return None
            ts = datetime.fromisoformat(ts_raw)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return value, ts
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            LOGGER.warning("risk_service.vix_snapshot_invalid", payload=raw)
            return None

    def _concurrency_key(self, trade_date: date) -> str:
        return f"risk:concurrency:{trade_date.isoformat()}"

    def _persist_concurrency_symbol(self, now_et: datetime, symbol: str) -> None:
        if self._redis is None:
            return
        try:
            key = self._concurrency_key(now_et.date())
            window_start = datetime.combine(
                now_et.date(), self._opening_time, tzinfo=EASTERN
            ).timestamp()
            self._redis.zremrangebyscore(key, 0, window_start)
            symbol_key = symbol.upper()
            self._redis.zadd(key, {symbol_key: float(now_et.timestamp())})
            self._redis.expire(key, 60 * 60 * 24)
        except Exception:
            LOGGER.exception("risk_service.concurrency_store_failed", symbol=symbol)

    def _fetch_concurrency_symbols(self, now_et: datetime) -> Optional[set[str]]:
        if self._redis is None:
            return None
        try:
            key = self._concurrency_key(now_et.date())
            window_start = datetime.combine(
                now_et.date(), self._opening_time, tzinfo=EASTERN
            ).timestamp()
            self._redis.zremrangebyscore(key, 0, window_start)
            members_raw = self._redis.zrange(key, 0, -1)
            members_seq = cast(Sequence[Any], members_raw)
            return {str(member).upper() for member in members_seq}
        except Exception:
            LOGGER.exception("risk_service.concurrency_fetch_failed")
            return None

    def symbol_block_reason(self, symbol: str) -> Optional[str]:
        """Return the active block reason for the provided symbol, if any."""
        return self._blocks.reason(symbol)

    async def release_symbol_block(
        self,
        session: Session,
        *,
        symbol: str,
        strategy_code: str,
        trace_id: Optional[str],
        redis_bus: RedisBus,
        reason: str = "FILLED",
    ) -> bool:
        """Clear symbol level blocks when downstream executions succeed."""
        block_reason = self._blocks.reason(symbol)
        if block_reason not in {"MISSED_ENTRY", "MIDFAIL_UNTIL_1400"}:
            return False

        dao = RiskEventDAO(session)
        event_payload = {
            "strategy_code": strategy_code,
            "reason": reason,
            "blocked_reason": block_reason,
        }
        dao.create_event(
            event_ts=utc_now(),
            event_code="UNBLOCK",
            severity="INFO",
            message="symbol_unblocked",
            symbol=symbol,
            payload=event_payload,
        )
        self._blocks.unblock(symbol)
        await publish_risk_unblock(
            redis_bus,
            {
                "strategy_code": strategy_code,
                "symbol": symbol,
                "reason": reason,
            },
            trace_id=trace_id or f"unblock-{symbol}",
        )
        return True

    def get_vix_value(self, session: Session, ts: datetime) -> Optional[Decimal]:
        snapshot = self._read_vix_snapshot()
        if snapshot is not None:
            value, snap_ts = snapshot
            if (ts - snap_ts).total_seconds() <= 300:
                self._vix_cache = (value, snap_ts)
                set_risk_vix(value)
                return value

        if self._should_fetch_vix_preopen(ts):
            fetched = self._fetch_vix_from_ibkr()
            if fetched is not None:
                snap_ts = utc_now()
                self._write_vix_snapshot(fetched, snap_ts)
                self._vix_cache = (fetched, snap_ts)
                set_risk_vix(fetched)
                return fetched

        value = self._current_vix(session, ts)
        if value is not None:
            set_risk_vix(value)
        return value

    def _should_fetch_vix_preopen(self, ts: datetime) -> bool:
        if not self._ibkr_vix_enabled:
            return False
        local_ts = self._to_eastern(ts)
        if local_ts.time() >= self._opening_time:
            return False
        if self._last_ibkr_vix_fetch is None:
            return True
        last_local = self._to_eastern(self._last_ibkr_vix_fetch)
        if local_ts.date() != last_local.date():
            return True
        return (local_ts - last_local).total_seconds() >= 180

    def _fetch_vix_from_ibkr(self) -> Optional[Decimal]:
        if not self._ibkr_vix_enabled:
            return None
        with self._ibkr_vix_lock:
            now = utc_now()
            client: Optional[IBClient] = None
            try:
                client = build_ibkr_client(self._settings)
                contract = IBClient.index_contract("VIX", "CBOE", "USD")
                snapshot = client.market_data_snapshot(contract, generic_ticks="", timeout=7.0)
                price = self._extract_vix_price(snapshot)
                if price is not None:
                    self._last_ibkr_vix_fetch = now
                    LOGGER.info(
                        "risk_service.vix_snapshot_refreshed",
                        price=str(price),
                        source="ibkr",
                    )
                    return price
                LOGGER.warning("risk_service.vix_snapshot_empty", snapshot=snapshot)
            except Exception:
                LOGGER.exception("risk_service.vix_snapshot_ibkr_failed")
            finally:
                if client is not None:
                    try:
                        client.disconnect_and_stop()
                    except Exception:
                        LOGGER.exception("risk_service.vix_snapshot_disconnect_failed")
            return None

    def _write_vix_snapshot(self, value: Decimal, ts: datetime) -> None:
        if self._redis is None:
            return
        payload = {"value": str(value), "ts": ts.isoformat()}
        try:
            self._redis.setex("risk:vix:last", 180, json.dumps(payload))
        except Exception:
            LOGGER.exception("risk_service.vix_snapshot_store_failed")

    @staticmethod
    def _extract_vix_price(snapshot: Mapping[str, Any]) -> Optional[Decimal]:
        for field in ("last", "mark", "close", "bid", "ask"):
            raw = snapshot.get(field)
            if raw is None:
                continue
            try:
                return Decimal(str(raw))
            except (InvalidOperation, ValueError, TypeError):
                continue
        return None

    @property
    def stop_loss_pct(self) -> Decimal:
        return self._stop_loss_pct

    @property
    def cutoff_open_chase(self) -> time:
        return self._cutoff_open_chase

    @property
    def cutoff_overnight(self) -> time:
        return self._cutoff_overnight

    @property
    def use_atr_delta(self) -> bool:
        return False

    def is_buy_signal(self, signal_code: Optional[str]) -> bool:
        return bool(signal_code and signal_code in self._BUY_SIGNALS)

    def _sync_limits(self) -> None:
        self._risk_notional_cap = self._limits_cache.get_decimal(
            symbol=None, key="RISK_NOTIONAL_CAP", default=Decimal("5000000")
        )
        self._daily_loss_r = self._limits_cache.get_decimal(
            symbol=None, key="DAILY_LOSS_R", default=Decimal("3")
        )
        self._max_concurrent_top = int(
            self._limits_cache.get_decimal(
                symbol=None, key="MAX_CONCURRENT_TOP", default=Decimal("3")
            )
        )
        self._risk_unit_value = self._limits_cache.get_decimal(
            symbol=None, key="RISK_UNIT_VALUE", default=Decimal("1000")
        )
        self._stop_loss_pct = self._limits_cache.get_decimal(
            symbol=None, key="STOP_LOSS_PCT", default=Decimal("0.08")
        )
        self._symbol_block_minutes = int(
            self._limits_cache.get_decimal(
                symbol=None, key="SYMBOL_BLOCK_MINUTES", default=Decimal("10")
            )
        )
        self._no_new_from, self._no_new_to = self._limits_cache.no_new_window()
        self._cutoff_open_chase = self._limits_cache.get_time("CUTOFF_OPEN_CHASE", time(14, 0))
        self._cutoff_overnight = self._limits_cache.get_time("CUTOFF_OVERNIGHT", time(12, 0))
        gate_vix_cfg = self._limits_cache.gate_vix_limits()
        self._vix_gate_mode = gate_vix_cfg.mode
        self._vix_gate = self._limits_cache.get_decimal(
            symbol=None, key="VIX_GATE", default=gate_vix_cfg.thresh
        )
        self._iv_overnight_call_cap = self._limits_cache.get_decimal(
            symbol=None, key="IV_OVERNIGHT_CALL_CAP", default=Decimal("1.0")
        )
        self._limits_updated_at = self._limits_cache.updated_at

    # ------------------------------------------------------------------
    def evaluate_order(
        self,
        payload: RiskCheckRequest,
        *,
        exposures: Sequence[ExposureView],
        current_notional: Decimal,
        pending_notional: Decimal,
        utilisation: float,
        record_state: bool = False,
        record_events: bool = True,
        redis_bus: Optional[RedisBus] = None,
    ) -> RiskDecision:
        now_utc = payload.timestamp
        now_et = self._to_eastern(now_utc)
        trade_date = now_et.date()
        self._reset_if_new_day(trade_date)

        open_positions = sum(1 for exposure in exposures if getattr(exposure, "open_quantity", 0))
        self._emit_metric(set_position_count, open_positions)
        try:
            total_exposure_value = float(current_notional)
        except (TypeError, ValueError, InvalidOperation):
            total_exposure_value = None
        if total_exposure_value is not None:
            self._emit_metric(set_total_exposure, total_exposure_value)
        self._emit_metric(set_capital_utilization, utilisation)
        self._emit_metric(set_limit_usage, "notional", utilisation)
        if utilisation >= 0.8:
            threshold = "0.80" if utilisation < 0.95 else "0.95"
            self._emit_metric(record_limit_approaching, "notional", threshold)

        approved = True
        code = "OK"
        detail = "limit-ok"
        reasons: Dict[str, object] = {}
        symbol_block_until: Optional[datetime] = None
        option_snapshot: Dict[str, Any] | None = None
        if payload.signal_code in self._BUY_SIGNALS:
            passed, snapshot = self._check_option_liquidity(payload)
            option_snapshot = snapshot
            if not passed:
                approved = False
                code = "BLOCK:OPTION_LIQUIDITY"
                detail = "option-liquidity-insufficient"
                reasons["option_liquidity"] = snapshot

        with self._session_factory() as session:
            try:
                kill_active, kill_changed, daily_pnl = self._ensure_kill_switch(session, trade_date)
                vix_value = self.get_vix_value(session, now_utc)
                gate_open = self._vix_gate_open
                self._blocks.prune(now_utc)

                if kill_active:
                    approved = False
                    code = "BLOCK:KILL_SWITCH"
                    detail = "daily-loss-threshold"
                    reasons["kill_switch"] = True

                if approved and utilisation >= 1.0:
                    approved = False
                    code = "BLOCK:NOTIONAL_LIMIT"
                    detail = "requested notional breaches configured cap"
                    reasons["utilisation"] = utilisation

                if approved and self._in_block_window(now_et.time()):
                    approved = False
                    code = "BLOCK:TIME_WINDOW"
                    detail = "no-new-orders-window"
                    reasons["time_window"] = True

                if approved and payload.signal_code and payload.signal_code in self._VIX_SENSITIVE:
                    if vix_value is not None and vix_value >= self._vix_gate:
                        approved = False
                        code = "BLOCK:VIX_GATE"
                        detail = f"vix={vix_value}"
                        reasons["vix"] = str(vix_value)

                if approved and self._blocks.is_blocked(payload.symbol, now_utc):
                    block_reason = self._blocks.reason(payload.symbol)
                    approved = False
                    code = f"BLOCK:{block_reason or 'SYMBOL'}"
                    detail = "symbol-blocked"
                    reasons["symbol_blocked"] = block_reason or True

                if approved and payload.signal_code in self._BUY_SIGNALS:
                    try:
                        preearn_allowed, preearn_context = evaluate_preearn_guard(
                            session,
                            symbol=payload.symbol,
                            trade_date=trade_date,
                            days_min=self._preearn_days_min,
                            days_max=self._preearn_days_max,
                            atr_pct_max=self._preearn_atr_pct_max,
                        )
                    except Exception:  # pragma: no cover - defensive
                        LOGGER.exception(
                            "risk_service.preearn_guard_failed",
                            symbol=payload.symbol,
                            trade_date=str(trade_date),
                        )
                        preearn_allowed = True
                        preearn_context = {"pre_earn": False}
                    if not preearn_allowed:
                        approved = False
                        code = "BLOCK:PRE_EARN"
                        detail = "pre-earn-guard"
                        reasons["pre_earn"] = preearn_context

                if approved and payload.signal_code in self._BUY_SIGNALS:
                    if self._concurrency_limited(payload.symbol, exposures, now_et):
                        approved = False
                        code = "BLOCK:CONCURRENCY"
                        detail = "max-concurrent-top-reached"
                        reasons["max_concurrent"] = self._max_concurrent_top
                        reasons["active_symbols"] = sorted({exp.symbol for exp in exposures if getattr(exp, "open_quantity", 0)})
                        if self._symbol_block_minutes > 0:
                            symbol_block_until = now_utc + timedelta(
                                minutes=self._symbol_block_minutes
                            )
                            reasons["block_until"] = symbol_block_until.isoformat()
                        LOGGER.info(
                            "risk_service.concurrency_block",
                            symbol=payload.symbol,
                            max_concurrent=self._max_concurrent_top,
                            active=reasons["active_symbols"],
                        )

                decision = RiskDecision(
                    approved=approved,
                    code=code,
                    detail=detail,
                    utilisation=utilisation,
                    reasons=reasons,
                )

                if symbol_block_until is not None:
                    self._blocks.block(payload.symbol, symbol_block_until, code)

                self._decorate_reasons(
                    decision,
                    remaining_capacity=self._risk_notional_cap
                    - (current_notional + pending_notional),
                    session=session,
                )
                if option_snapshot and "option_liquidity" not in decision.reasons:
                    decision.reasons["option_liquidity"] = option_snapshot

                if record_events and not decision.approved:
                    self._log_block_event(session, now_utc, payload, decision)

                if record_state:
                    self._record_state(session, now_utc, kill_active, gate_open, vix_value)
                if decision.approved:
                    self._register_concurrency(payload, now_et, exposures)
                else:
                    self._emit_block_signal(decision, payload, now_utc, redis_bus)

                if record_events and kill_changed:
                    self._log_kill_switch_event(session, now_utc, kill_active, daily_pnl)

                session.commit()
                return decision
            except Exception:
                session.rollback()
                raise

    # ------------------------------------------------------------------
    def _check_option_liquidity(self, payload: RiskCheckRequest) -> Tuple[bool, Dict[str, Any]]:
        """基于风控请求中附带的合约细节判断期权流动性是否达标。"""
        snapshot: Dict[str, Any] = {"pass": True, "reason": "insufficient_data"}
        if payload.signal_code not in self._BUY_SIGNALS:
            return True, snapshot
        bid = payload.option_bid
        ask = payload.option_ask
        oi = payload.option_open_interest
        volume = payload.option_volume
        strike = payload.option_strike
        expiry = payload.option_expiry
        mid = payload.option_mid
        spread = payload.option_spread
        dte = payload.option_dte
        snapshot.update(
            {
                "bid": float(bid) if bid is not None else None,
                "ask": float(ask) if ask is not None else None,
                "open_interest": oi,
                "volume": volume,
                "strike": float(strike) if strike is not None else None,
                "expiry": expiry,
                "mid": float(mid) if mid is not None else None,
                "spread": float(spread) if spread is not None else None,
                "dte": dte,
            }
        )
        if None in (bid, ask, oi, volume):
            snapshot["reason"] = "missing_fields"
            return True, snapshot
        if mid is None:
            mid = (bid + ask) / Decimal("2")
            snapshot["mid"] = float(mid)
        if spread is None:
            spread = ask - bid
            snapshot["spread"] = float(spread)
        threshold = max(Decimal("0.10"), mid * Decimal("0.05"))
        snapshot["threshold"] = float(threshold)
        if oi < 500 or volume < 100 or spread > threshold:
            snapshot["pass"] = False
            snapshot["reason"] = "volume_or_spread"
            LOGGER.info(
                "risk_service.option_liquidity_block",
                symbol=payload.symbol,
                snapshot=snapshot,
            )
            return False, snapshot
        if dte is not None and not (2 <= dte <= 7):
            snapshot["pass"] = False
            snapshot["reason"] = "dte_out_of_range"
            LOGGER.info(
                "risk_service.option_liquidity_dte_block",
                symbol=payload.symbol,
                snapshot=snapshot,
            )
            return False, snapshot
        snapshot["reason"] = "ok"
        return True, snapshot

    # ------------------------------------------------------------------
    def _reset_if_new_day(self, trade_date: date) -> None:
        if self._current_trade_date == trade_date:
            return
        self._current_trade_date = trade_date
        self._concurrent_tracker = {trade_date: []}
        self._kill_switch_active = False
        if self._redis is not None:
            try:
                previous_date = trade_date - timedelta(days=1)
                self._redis.expire(self._concurrency_key(previous_date), 60 * 60)
            except Exception:
                LOGGER.exception("risk_service.concurrency_expire_failed")

    def _register_concurrency(
        self,
        payload: RiskCheckRequest,
        now_et: datetime,
        exposures: Sequence[ExposureView],
    ) -> None:
        if payload.signal_code not in self._BUY_SIGNALS:
            return
        if not self._within_open_window(now_et):
            return
        existing_symbols = {pos.symbol.upper() for pos in exposures if pos.open_quantity}
        if payload.symbol.upper() in existing_symbols:
            return
        self._append_concurrency_entry(payload.symbol.upper(), now_et)

    def _within_open_window(self, now_et: datetime) -> bool:
        window_start = datetime.combine(now_et.date(), self._opening_time, tzinfo=EASTERN)
        window_end = window_start + self._concurrency_window
        return window_start <= now_et < window_end

    def _concurrency_limited(
        self,
        symbol: str,
        exposures: Sequence[ExposureView],
        now_et: datetime,
    ) -> bool:
        if not self._within_open_window(now_et):
            return False
        existing_symbols = {pos.symbol.upper() for pos in exposures if pos.open_quantity}
        if symbol.upper() in existing_symbols:
            return False
        entries = self._concurrent_tracker.setdefault(now_et.date(), [])
        window_start = datetime.combine(now_et.date(), self._opening_time, tzinfo=EASTERN)
        entries[:] = [(ts, sym) for ts, sym in entries if ts >= window_start]
        unique_symbols = {sym for _, sym in entries}
        redis_symbols = self._fetch_concurrency_symbols(now_et)
        if redis_symbols is not None:
            unique_symbols = redis_symbols
        return len(unique_symbols) >= self._max_concurrent_top

    def _append_concurrency_entry(self, symbol: str, now_et: datetime) -> None:
        symbol_key = symbol.upper()
        entries = self._concurrent_tracker.setdefault(now_et.date(), [])
        for _, sym in entries:
            if sym == symbol_key:
                break
        else:
            entries.append((now_et, symbol_key))
        self._persist_concurrency_symbol(now_et, symbol_key)

    def _remove_concurrency_symbol(self, symbol: str, now_et: Optional[datetime] = None) -> None:
        symbol_key = symbol.upper()
        target_dates: List[date]
        if now_et is not None:
            target_dates = [now_et.date()]
        else:
            target_dates = list(self._concurrent_tracker.keys())

        for trade_date in target_dates:
            entries = self._concurrent_tracker.get(trade_date)
            if not entries:
                continue
            entries[:] = [(ts, sym) for ts, sym in entries if sym != symbol_key]
            if not entries:
                self._concurrent_tracker.pop(trade_date, None)

        if self._redis is not None:
            for trade_date in target_dates:
                key = self._concurrency_key(trade_date)
                try:
                    self._redis.zrem(key, symbol_key)
                except Exception:
                    LOGGER.exception("risk_service.concurrency_remove_failed", symbol=symbol_key)

    def _decorate_reasons(
        self,
        decision: RiskDecision,
        *,
        remaining_capacity: Decimal,
        session: Session,
    ) -> None:
        remaining = remaining_capacity if remaining_capacity > 0 else Decimal("0")
        decision.reasons["remaining_capacity"] = str(remaining)
        used_r = self._latest_global_metric(session, "USED_R")
        drawdown_r = self._latest_global_metric(session, "DRAWDOWN_R")
        if used_r is not None:
            decision.reasons["used_r"] = str(used_r)
            self._emit_metric(set_risk_used_r, used_r)
        if drawdown_r is not None:
            decision.reasons["drawdown_r"] = str(drawdown_r)
            self._emit_metric(set_risk_drawdown, drawdown_r)

    def _emit_block_signal(
        self,
        decision: RiskDecision,
        payload: RiskCheckRequest,
        now_utc: datetime,
        redis_bus: Optional[RedisBus],
    ) -> None:
        if decision.approved or redis_bus is None:
            return
        code = decision.code
        symbol = payload.symbol
        base_payload = {
            "strategy_code": payload.strategy_code,
            "symbol": symbol,
            "metric_value": Decimal("1"),
            "triggered_at": now_utc,
        }
        if code == "BLOCK:TIME_WINDOW":
            event_payload = dict(
                base_payload,
                metric_code="TIME_WINDOW",
                limit_code="NO_NEW_WINDOW",
                reason="TIME_WINDOW",
            )
        elif code == "BLOCK:VIX_GATE":
            event_payload = dict(
                base_payload, metric_code="VIX_GATE", limit_code="VIX_GATE_ON", reason="VIX_GATE_ON"
            )
        elif code == "BLOCK:KILL_SWITCH":
            event_payload = dict(
                base_payload,
                metric_code="KILL_SWITCH",
                limit_code="KILL_SWITCH",
                reason="KILL_SWITCH",
            )
        elif code == "BLOCK:PRE_EARN":
            event_payload = dict(
                base_payload,
                metric_code="PRE_EARN",
                limit_code="PRE_EARN",
                reason="PRE_EARN",
            )
            context = decision.reasons.get("pre_earn")
            if context is not None:
                event_payload["detail"] = context
        else:
            return

        trace_id = (
            current_trace_context().get("trace_id") or f"{code}-{symbol}-{int(now_utc.timestamp())}"
        )
        coroutine = publish_risk_block(redis_bus, event_payload, trace_id=trace_id)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coroutine)
        except RuntimeError:
            asyncio.run(coroutine)

    def _latest_global_metric(self, session: Session, metric_code: str) -> Optional[Decimal]:
        record = RiskStateDAO(session).latest_for_symbol("GLOBAL", metric_code)
        if record is None or record.metric_value is None:
            return None
        return Decimal(str(record.metric_value))

    def list_blocks(self) -> List[Dict[str, object]]:
        return self._blocks.snapshot()

    def concurrency_snapshot(self, trade_date: Optional[date] = None) -> Dict[str, object]:
        if trade_date is None:
            if self._concurrent_tracker:
                trade_date = max(self._concurrent_tracker.keys())
            else:
                trade_date = self._current_trade_date or self._to_eastern(utc_now()).date()
        entries = self._concurrent_tracker.get(trade_date, [])
        combined: Dict[str, datetime] = {}
        for ts, sym in entries:
            combined[sym] = ts.astimezone(timezone.utc)

        if self._redis is not None:
            key = self._concurrency_key(trade_date)
            try:
                redis_entries_raw = self._redis.zrange(key, 0, -1, withscores=True)  # type: ignore[arg-type]
            except Exception:
                LOGGER.exception("risk_service.concurrency_snapshot_failed", key=key)
                redis_entries_raw = []
            redis_entries = cast(Sequence[tuple[Any, float]], redis_entries_raw)
            for member, score in redis_entries:
                try:
                    ts = datetime.fromtimestamp(float(score), tz=timezone.utc)
                except Exception:
                    continue
                sym = str(member).upper()
                existing = combined.get(sym)
                if existing is None or ts > existing:
                    combined[sym] = ts

        snapshot_entries = [
            {"symbol": sym, "last_seen": combined[sym].isoformat()} for sym in sorted(combined)
        ]
        return {
            "trade_date": trade_date.isoformat(),
            "count": len(snapshot_entries),
            "entries": snapshot_entries,
        }

    def _in_block_window(self, now_time: time) -> bool:
        return self._limits_cache.is_no_new_window(now_time)

    def _ensure_kill_switch(
        self,
        session: Session,
        trade_date: date,
    ) -> Tuple[bool, bool, Decimal]:
        total_pnl = self._compute_daily_realized(session, trade_date)
        threshold = -(self._risk_unit_value * self._daily_loss_r)
        active = total_pnl <= threshold
        changed = active != self._kill_switch_active
        self._kill_switch_active = active
        return active, changed, total_pnl

    def _compute_daily_realized(self, session: Session, trade_date: date) -> Decimal:
        start_et = datetime.combine(trade_date, time(0, 0), tzinfo=EASTERN)
        end_et = start_et + timedelta(days=1)
        start_utc = start_et.astimezone(timezone.utc)
        end_utc = end_et.astimezone(timezone.utc)
        rows = session.execute(
            text(
                """
                SELECT realized, fees
                FROM pnl_intraday
                WHERE ts >= :start_ts AND ts < :end_ts
                """
            ),
            {"start_ts": start_utc, "end_ts": end_utc},
        ).all()
        total = Decimal("0")
        for realized, fees in rows:
            realised_val = Decimal(str(realized)) if realized is not None else Decimal("0")
            fee_val = Decimal(str(fees)) if fees is not None else Decimal("0")
            total += realised_val - fee_val
        return total

    def _record_state(
        self,
        session: Session,
        now_utc: datetime,
        kill_switch_active: bool,
        gate_open: bool,
        vix_value: Optional[Decimal],
    ) -> None:
        self._emit_metric(set_kill_switch_state, kill_switch_active)
        self._emit_metric(set_risk_vix, vix_value)
        states: List[Dict[str, object]] = [
            {
                "ts": now_utc,
                "symbol": "GLOBAL",
                "metric_code": "KILL_SWITCH",
                "metric_value": Decimal("1") if kill_switch_active else Decimal("0"),
                "detail": None,
            },
            {
                "ts": now_utc,
                "symbol": "GLOBAL",
                "metric_code": "GATE_OPEN_CHASE",
                "metric_value": Decimal("1") if gate_open else Decimal("0"),
                "detail": None,
            },
        ]
        if vix_value is not None:
            states.append(
                {
                    "ts": now_utc,
                    "symbol": "GLOBAL",
                    "metric_code": "VIX_LAST",
                    "metric_value": vix_value,
                    "detail": None,
                }
            )
        RiskStateDAO(session).insert_states(states)

    def _log_block_event(
        self,
        session: Session,
        event_ts: datetime,
        payload: RiskCheckRequest,
        decision: RiskDecision,
    ) -> None:
        trace_id = current_trace_context().get("trace_id")
        self._emit_metric(record_risk_block, decision.code)
        event_payload = {
            "strategy_code": payload.strategy_code,
            "reasons": decision.reasons,
            "utilisation": decision.utilisation,
        }
        if trace_id:
            event_payload["trace_id"] = trace_id
        RiskEventDAO(session).create_event(
            event_ts=event_ts,
            event_code=decision.code,
            severity="WARN",
            message=decision.detail,
            symbol=payload.symbol,
            payload=event_payload,
        )

    def _log_kill_switch_event(
        self,
        session: Session,
        event_ts: datetime,
        active: bool,
        daily_pnl: Decimal,
    ) -> None:
        event_code = "KILL_SWITCH_ON" if active else "KILL_SWITCH_OFF"
        severity = "ERROR" if active else "INFO"
        payload_data = {
            "active": active,
            "daily_pnl": str(daily_pnl),
            "threshold_r": str(self._daily_loss_r),
        }
        trace_id = current_trace_context().get("trace_id")
        if trace_id:
            payload_data["trace_id"] = trace_id
        RiskEventDAO(session).create_event(
            event_ts=event_ts,
            event_code=event_code,
            severity=severity,
            message="kill-switch-state-changed",
            payload=payload_data,
        )

    def _current_vix(self, session: Session, ts: datetime) -> Optional[Decimal]:
        cached_value, cached_ts = self._vix_cache
        if cached_value is not None and cached_ts is not None:
            if (ts - cached_ts).total_seconds() <= 60:
                return cached_value
        record = RiskStateDAO(session).latest_for_symbol("VIX", "VIX")
        if record is None:
            return None
        self._vix_cache = (record.metric_value, record.ts)
        return record.metric_value

    async def check_vix_gate_at_open(
        self,
        session: Session,
        now_utc: datetime,
        vix_value: Optional[Decimal],
        *,
        redis_bus,
        trace_id: Optional[str] = None,
    ) -> None:
        """
        在09:31-09:35之间检查一次VIX并锁定当日决策
        
        如果VIX>=20，则当天剩余时间都禁止开盘追高信号。
        这样避免盘中VIX波动导致策略反复调整。
        """
        if vix_value is None:
            return
        
        now_et = now_utc.astimezone(EASTERN)
        today = now_et.date()
        
        # 检查是否已经在今天检查过VIX
        if self._vix_checked_today == today:
            return
        
        # 只在09:31-09:35之间检查
        opening_window_start = time(9, 31)
        opening_window_end = time(9, 35)
        if not (opening_window_start <= now_et.time() <= opening_window_end):
            return
        
        # 标记今天已检查
        self._vix_checked_today = today
        self._vix_gate_locked_today = vix_value >= self._vix_gate
        
        # 更新门控状态
        gate_open = vix_value < self._vix_gate
        if gate_open != self._vix_gate_open:
            self._vix_gate_open = gate_open
            self._store_gate_state(gate_open, now_utc, vix_value)
            
            event_code = "VIX_GATE_OFF" if gate_open else "VIX_GATE_ON"
            dao = RiskEventDAO(session)
            dao.create_event(
                event_ts=now_utc,
                event_code=event_code,
                severity="INFO" if gate_open else "WARN",
                message=f"{event_code.lower()}_at_open (VIX={vix_value}, threshold={self._vix_gate})",
                symbol="GLOBAL",
                payload={
                    "vix": str(vix_value),
                    "threshold": str(self._vix_gate),
                    "locked_for_day": True,
                },
            )
            await publish_risk_alert(
                redis_bus,
                {
                    "symbol": "GLOBAL",
                    "event_code": event_code,
                    "vix": str(vix_value),
                    "locked_for_day": True,
                },
                trace_id=trace_id or f"{event_code}_OPEN",
            )
            
            if gate_open:
                await publish_risk_unblock(
                    redis_bus,
                    {
                        "strategy_code": "core-vol",
                        "symbol": "GLOBAL",
                        "reason": "VIX_GATE_OFF",
                    },
                    trace_id=trace_id or f"vix-gate-unblock-open-{now_utc.isoformat()}",
                )
            else:
                self._emit_metric(record_risk_block, "VIX_GATE_ON")
        
        LOGGER.info(
            "vix_gate.checked_at_open",
            vix=str(vix_value),
            gate_open=gate_open,
            locked_for_day=True,
            trade_date=today.isoformat(),
        )

    async def update_vix_gate(
        self,
        session: Session,
        now_utc: datetime,
        vix_value: Optional[Decimal],
        *,
        redis_bus,
        trace_id: Optional[str] = None,
    ) -> None:
        if vix_value is None:
            return
        
        # 如果今天已经锁定决策（通过check_vix_gate_at_open），则不再更新
        now_et = now_utc.astimezone(EASTERN)
        if self._vix_checked_today == now_et.date() and self._vix_gate_locked_today:
            return
        
        gate_open = vix_value < self._vix_gate
        if gate_open == self._vix_gate_open:
            return
        self._vix_gate_open = gate_open
        self._store_gate_state(gate_open, now_utc, vix_value)
        event_code = "VIX_GATE_OFF" if gate_open else "VIX_GATE_ON"
        dao = RiskEventDAO(session)
        dao.create_event(
            event_ts=now_utc,
            event_code=event_code,
            severity="INFO" if gate_open else "WARN",
            message=event_code.lower(),
            symbol="GLOBAL",
            payload={"vix": str(vix_value), "threshold": str(self._vix_gate)},
        )
        await publish_risk_alert(
            redis_bus,
            {
                "symbol": "GLOBAL",
                "event_code": event_code,
                "vix": str(vix_value),
            },
            trace_id=trace_id or event_code,
        )
        if gate_open:
            await publish_risk_unblock(
                redis_bus,
                {
                    "strategy_code": "core-vol",
                    "symbol": "GLOBAL",
                    "reason": "VIX_GATE_OFF",
                },
                trace_id=trace_id or f"vix-gate-unblock-{now_utc.isoformat()}",
            )
        else:
            self._emit_metric(record_risk_block, "VIX_GATE_ON")
            await publish_risk_block(
                redis_bus,
                {
                    "strategy_code": "core-vol",
                    "symbol": "GLOBAL",
                    "metric_code": "VIX_GATE",
                    "metric_value": Decimal("1"),
                    "limit_code": "VIX_GATE_ON",
                    "reason": "VIX_GATE_ON",
                    "triggered_at": now_utc,
                },
                trace_id=trace_id or f"vix-gate-block-{now_utc.isoformat()}",
            )
        set_risk_vix(vix_value)

    async def record_missed_entry(
        self,
        symbol: str,
        strategy_code: str,
        signal_code: Optional[str],
        ts_utc: datetime,
        *,
        redis_bus,
        trace_id: Optional[str] = None,
    ) -> None:
        until = datetime.combine(
            ts_utc.astimezone(EASTERN).date(), time(14, 0), tzinfo=EASTERN
        ).astimezone(timezone.utc)
        with self._session_factory() as session:
            dao = RiskEventDAO(session)
            dao.create_event(
                event_ts=ts_utc,
                event_code="MISSED_ENTRY",
                severity="WARN",
                message="missed_entry",
                symbol=symbol,
                payload={
                    "block_until": until.isoformat(),
                    "strategy_code": strategy_code,
                    "signal_code": signal_code,
                },
            )
            session.commit()
        self._blocks.block(symbol, until, "MISSED_ENTRY")
        self._emit_metric(record_risk_block, "MISSED_ENTRY")
        await publish_risk_block(
            redis_bus,
            {
                "strategy_code": strategy_code,
                "symbol": symbol,
                "metric_code": "MISSED_ENTRY",
                "metric_value": Decimal("1"),
                "limit_code": "MISSED_ENTRY",
                "reason": f"MISSED_ENTRY:{signal_code or ''}",
                "block_until": until.isoformat(),
                "triggered_at": ts_utc,
            },
            trace_id=trace_id or f"missed-entry-{symbol}",
        )
        self._remove_concurrency_symbol(symbol, ts_utc.astimezone(EASTERN))

    async def record_midfail(
        self,
        symbol: str,
        ts_utc: datetime,
        *,
        redis_bus,
        trace_id: Optional[str] = None,
    ) -> None:
        block_until_et = datetime.combine(
            ts_utc.astimezone(EASTERN).date(), time(14, 0), tzinfo=EASTERN
        )
        block_until = block_until_et.astimezone(timezone.utc)
        with self._session_factory() as session:
            dao = RiskEventDAO(session)
            dao.create_event(
                event_ts=ts_utc,
                event_code="MIDFAIL",
                severity="WARN",
                message="midfail_block",
                symbol=symbol,
                payload={"block_until": block_until.isoformat()},
            )
            session.commit()
        self._blocks.block(symbol, block_until, "MIDFAIL_UNTIL_1400")
        self._emit_metric(record_risk_block, "MIDFAIL_UNTIL_1400")
        await publish_risk_block(
            redis_bus,
            {
                "strategy_code": "core-vol",
                "symbol": symbol,
                "metric_code": "MIDFAIL",
                "metric_value": Decimal("1"),
                "limit_code": "MIDFAIL_UNTIL_1400",
                "reason": "MIDFAIL_UNTIL_1400",
                "block_until": block_until.isoformat(),
                "triggered_at": ts_utc,
            },
            trace_id=trace_id or f"midfail-{symbol}",
        )

    @staticmethod
    def _to_eastern(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc).astimezone(EASTERN)
        return ts.astimezone(EASTERN)
