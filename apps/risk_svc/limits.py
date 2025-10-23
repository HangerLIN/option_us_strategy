from __future__ import annotations

import copy
import json
import logging
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from pydantic import BaseModel, ValidationError

from libs.core import get_settings, utc_now
from libs.db.models import RiskLimit
from libs.schemas.risk import (
    AmBottomLimits,
    AmConfluenceLimits,
    AmSell1Limits,
    GateLimits,
    GateVixLimits,
    OvernightLimits,
    RiskLimits,
)

LOGGER = logging.getLogger(__name__)

_STRUCTURED_KEYS = {
    "AM_BOTTOM",
    "AM_SELL1",
    "AM_CONF",
    "OVERNIGHT",
    "GATE.VIX",
}


class LimitsCache:
    """Cache risk_limits with scope-aware overrides and helper accessors."""

    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory
        self._settings = get_settings()
        self._global: Dict[str, Decimal] = {}
        self._bucket_limits: Dict[str, Dict[str, Decimal]] = {}
        self._symbol_limits: Dict[str, Dict[str, Decimal]] = {}
        self._structured_global: Dict[str, Any] = {}
        self._structured_bucket: Dict[str, Dict[str, Any]] = {}
        self._structured_symbol: Dict[str, Dict[str, Any]] = {}
        self._structured_defaults: Dict[str, Any] = {}
        self._updated_at: datetime = utc_now()
        self._snapshot: RiskLimits | None = None
        self.reload()

    # ------------------------------------------------------------------
    @property
    def updated_at(self) -> datetime:
        return self._updated_at

    def reload(self, refresh_settings: bool = False) -> RiskLimits:
        if refresh_settings:
            get_settings.cache_clear()  # type: ignore[attr-defined]
            self._settings = get_settings()

        self._global = self._default_global_limits()
        self._structured_defaults = self._default_structured_limits()
        self._structured_global = copy.deepcopy(self._structured_defaults)
        self._bucket_limits.clear()
        self._symbol_limits.clear()
        self._structured_bucket.clear()
        self._structured_symbol.clear()

        session: Session = self._session_factory()
        now_utc = utc_now()
        try:
            rows = session.execute(
                select(
                    RiskLimit.key,
                    RiskLimit.value,
                    RiskLimit.scope,
                    RiskLimit.symbol,
                    RiskLimit.bucket,
                    RiskLimit.effective_from,
                    RiskLimit.limit_id,
                )
                .where(RiskLimit.effective_from <= now_utc)
                .order_by(
                    RiskLimit.key,
                    RiskLimit.scope,
                    RiskLimit.symbol,
                    RiskLimit.bucket,
                    RiskLimit.effective_from.desc(),
                    RiskLimit.limit_id.desc(),
                )
            ).all()
        finally:
            session.close()

        seen: set[tuple[str, str, str, str]] = set()
        for key, value, scope, symbol, bucket, _effective_from, _limit_id in rows:
            if not key or not scope or value is None:
                continue
            scope_key = str(scope).lower()
            symbol_key = (symbol or "").upper()
            bucket_key = (bucket or "").upper()
            key_upper = str(key).upper()
            identity = (scope_key, symbol_key, bucket_key, key_upper)
            if identity in seen:
                continue
            seen.add(identity)

            if self._is_structured_key(key_upper):
                structured_value = self._parse_structured_value(value, key_upper)
                if structured_value is None:
                    LOGGER.warning(
                        "limits_cache.invalid_structured_value",
                        key=key_upper,
                        scope=scope_key,
                        value=value,
                    )
                    continue
                target_struct: Dict[str, Any]
                if scope_key == "global":
                    target_struct = self._structured_global
                elif scope_key == "symbol":
                    if not symbol_key:
                        continue
                    target_struct = self._structured_symbol.setdefault(symbol_key, {})
                elif scope_key == "symbol_bucket":
                    if not bucket_key:
                        continue
                    target_struct = self._structured_bucket.setdefault(bucket_key, {})
                else:
                    continue
                target_struct[key_upper] = structured_value
                continue

            decimal_value = self._parse_decimal(value)
            if decimal_value is None:
                LOGGER.warning(
                    "limits_cache.invalid_decimal_value",
                    key=key_upper,
                    scope=scope_key,
                    value=value,
                )
                continue

            if scope_key == "global":
                self._global[key_upper] = decimal_value
            elif scope_key == "symbol":
                if symbol_key:
                    self._symbol_limits.setdefault(symbol_key, {})[key_upper] = decimal_value
            elif scope_key == "symbol_bucket":
                if bucket_key:
                    self._bucket_limits.setdefault(bucket_key, {})[key_upper] = decimal_value

        self._updated_at = now_utc

        snapshot = self._build_snapshot()
        self._snapshot = snapshot
        return snapshot.model_copy(deep=True)

    @property
    def snapshot(self) -> RiskLimits:
        if self._snapshot is None:
            self._snapshot = self._build_snapshot()
        return self._snapshot.model_copy(deep=True)

    def am_bottom_limits(
        self, *, symbol: Optional[str] = None, bucket: Optional[str] = None
    ) -> AmBottomLimits:
        return self._coerce_structured(
            "AM_BOTTOM", AmBottomLimits, symbol=symbol, bucket=bucket
        )

    def am_sell1_limits(
        self, *, symbol: Optional[str] = None, bucket: Optional[str] = None
    ) -> AmSell1Limits:
        return self._coerce_structured(
            "AM_SELL1", AmSell1Limits, symbol=symbol, bucket=bucket
        )

    def am_confluence_limits(
        self, *, symbol: Optional[str] = None, bucket: Optional[str] = None
    ) -> AmConfluenceLimits:
        return self._coerce_structured(
            "AM_CONF", AmConfluenceLimits, symbol=symbol, bucket=bucket
        )

    def overnight_limits(
        self, *, symbol: Optional[str] = None, bucket: Optional[str] = None
    ) -> OvernightLimits:
        return self._coerce_structured(
            "OVERNIGHT", OvernightLimits, symbol=symbol, bucket=bucket
        )

    def gate_vix_limits(
        self, *, symbol: Optional[str] = None, bucket: Optional[str] = None
    ) -> GateVixLimits:
        return self._coerce_structured(
            "GATE.VIX", GateVixLimits, symbol=symbol, bucket=bucket
        )

    # ------------------------------------------------------------------
    def get_decimal(
        self,
        symbol: Optional[str],
        key: str,
        default: Decimal | float | int | None = None,
        *,
        bucket: Optional[str] = None,
    ) -> Decimal:
        key_upper = key.upper()
        if symbol:
            value = self._symbol_limits.get(symbol.upper(), {}).get(key_upper)
            if value is not None:
                return value
        if bucket:
            value = self._bucket_limits.get(bucket.upper(), {}).get(key_upper)
            if value is not None:
                return value
        value = self._global.get(key_upper)
        if value is not None:
            return value
        if default is None:
            return Decimal("0")
        return Decimal(str(default))

    def get_int(
        self,
        symbol: Optional[str],
        key: str,
        default: int,
        *,
        bucket: Optional[str] = None,
    ) -> int:
        value = self.get_decimal(symbol, key, Decimal(default), bucket=bucket)
        return int(value)

    def get_time(
        self,
        key: str,
        default: time,
        *,
        symbol: Optional[str] = None,
        bucket: Optional[str] = None,
    ) -> time:
        default_decimal = Decimal(default.hour) + (Decimal(default.minute) / Decimal(60))
        value = self.get_decimal(symbol, key, default_decimal, bucket=bucket)
        return self._decimal_to_time(value)

    def no_new_window(self) -> Tuple[time, time]:
        start = self.get_time("NO_NEW_FROM", time(12, 0))
        end = self.get_time("NO_NEW_TO", time(14, 0))
        return start, end

    def is_no_new_window(self, now_time: time) -> bool:
        start, end = self.no_new_window()
        if start <= end:
            return start <= now_time < end
        # Window passes midnight
        return now_time >= start or now_time < end

    # ------------------------------------------------------------------
    def _build_snapshot(self) -> RiskLimits:
        notional_cap = self.get_decimal(
            None, "RISK_NOTIONAL_CAP", self._settings.risk_notional_cap
        )
        am_bottom = self._coerce_structured("AM_BOTTOM", AmBottomLimits)
        am_sell1 = self._coerce_structured("AM_SELL1", AmSell1Limits)
        am_conf = self._coerce_structured("AM_CONF", AmConfluenceLimits)
        overnight = self._coerce_structured("OVERNIGHT", OvernightLimits)
        gate_vix = self._coerce_structured("GATE.VIX", GateVixLimits)
        return RiskLimits(
            notional_cap=notional_cap,
            updated_at=self._updated_at,
            am_bottom=am_bottom,
            am_sell1=am_sell1,
            am_conf=am_conf,
            overnight=overnight,
            gate=GateLimits(vix=gate_vix),
        )

    def _coerce_structured(
        self,
        key: str,
        model: type[BaseModel],
        *,
        symbol: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        raw = self._structured_lookup(key, symbol=symbol, bucket=bucket)
        if raw is None:
            raw = self._structured_defaults.get(key.upper())
        if raw is None:
            raw = {}
        try:
            return model.model_validate(raw)
        except ValidationError:
            LOGGER.warning("limits_cache.validation_failed", key=key.upper(), payload=raw)
            fallback = self._structured_defaults.get(key.upper(), {})
            return model.model_validate(fallback)

    def _structured_lookup(
        self,
        key: str,
        *,
        symbol: Optional[str] = None,
        bucket: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        key_upper = key.upper()
        if symbol:
            symbol_data = self._structured_symbol.get(symbol.upper(), {}).get(key_upper)
            if symbol_data is not None:
                return copy.deepcopy(symbol_data)
        if bucket:
            bucket_data = self._structured_bucket.get(bucket.upper(), {}).get(key_upper)
            if bucket_data is not None:
                return copy.deepcopy(bucket_data)
        base = self._structured_global.get(key_upper)
        if base is None:
            return None
        return copy.deepcopy(base)

    def _default_structured_limits(self) -> Dict[str, Any]:
        return {
            "AM_BOTTOM": {
                "pivot_w": 3,
                "neg_seq_min": 6,
                "pos_seq_min": 3,
                "allow_mid_filter": True,
                "top_n": 3,
                "cooldown_s": 600,
            },
            "AM_SELL1": {
                "body_ratio_min": 0.6,
                "range_mult_min": 1.5,
                "consecutive": 2,
            },
            "AM_CONF": {
                "buy": {"lookback_n": 5, "obv_slope_w": 5},
                "sell": {"lookback_n": 5},
                "cooldown_s": 600,
            },
            "OVERNIGHT": {
                "iv_max": float(self._settings.iv_overnight_call_cap),
                "bw": {"lookback": 120, "ma": 5, "ratio": 0.5, "hold_min": 10},
                "rebound": {"max_pct": 0.005},
            },
            "GATE.VIX": {
                "mode": self._settings.vix_gate_mode,
                "thresh": str(self._settings.vix_gate),
            },
        }

    @staticmethod
    def _is_structured_key(key: str) -> bool:
        return key in _STRUCTURED_KEYS

    @staticmethod
    def _parse_structured_value(value: object, key: str) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        if isinstance(value, Mapping):
            return copy.deepcopy(dict(value))
        raw_str = str(value).strip()
        if not raw_str:
            return None
        try:
            parsed = json.loads(raw_str)
        except (TypeError, ValueError, json.JSONDecodeError):
            LOGGER.warning("limits_cache.json_parse_failed", key=key, raw=value)
            return None
        if not isinstance(parsed, Mapping):
            LOGGER.warning("limits_cache.json_not_mapping", key=key, raw=parsed)
            return None
        return dict(parsed)

    # ------------------------------------------------------------------
    def _default_global_limits(self) -> Dict[str, Decimal]:
        return {
            "RISK_NOTIONAL_CAP": Decimal(self._settings.risk_notional_cap),
            "DAILY_LOSS_R": Decimal(self._settings.daily_loss_r),
            "MAX_CONCURRENT_TOP": Decimal(self._settings.max_concurrent_top),
            "RISK_UNIT_VALUE": Decimal(self._settings.risk_unit_value),
            "SYMBOL_BLOCK_MINUTES": Decimal(self._settings.symbol_block_minutes),
            "NO_NEW_FROM": Decimal("12.0"),
            "NO_NEW_TO": Decimal("14.0"),
            "CUTOFF_OPEN_CHASE": Decimal("14.0"),
            "CUTOFF_OVERNIGHT": Decimal("12.0"),
            "VIX_GATE": Decimal(self._settings.vix_gate),
            "IV_OVERNIGHT_CALL_CAP": Decimal(self._settings.iv_overnight_call_cap),
        }

    @staticmethod
    def _decimal_to_time(value: Decimal) -> time:
        minutes_total = int((value * Decimal(60)).to_integral_value(rounding="ROUND_FLOOR"))
        hours, minutes = divmod(minutes_total, 60)
        hours %= 24
        return time(hour=hours, minute=minutes)

    @staticmethod
    def _parse_decimal(raw: object) -> Optional[Decimal]:
        if raw is None:
            return None
        if isinstance(raw, Decimal):
            return raw
        raw_str = str(raw).strip()
        if not raw_str:
            return None
        try:
            return Decimal(raw_str)
        except InvalidOperation:
            if ":" in raw_str:
                parts = raw_str.split(":")
                if len(parts) == 2:
                    try:
                        hours = Decimal(parts[0])
                        minutes = Decimal(parts[1])
                        return hours + (minutes / Decimal(60))
                    except InvalidOperation:
                        return None
            return None
