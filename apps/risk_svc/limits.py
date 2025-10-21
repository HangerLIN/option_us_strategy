from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from libs.core import get_settings, utc_now
from libs.db.models import RiskLimit
from libs.schemas.risk import RiskLimits


class LimitsCache:
    """Cache risk_limits with scope-aware overrides and helper accessors."""

    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory
        self._settings = get_settings()
        self._global: Dict[str, Decimal] = {}
        self._bucket_limits: Dict[str, Dict[str, Decimal]] = {}
        self._symbol_limits: Dict[str, Dict[str, Decimal]] = {}
        self._updated_at: datetime = utc_now()
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
        self._bucket_limits.clear()
        self._symbol_limits.clear()

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

            decimal_value = self._parse_decimal(value)
            if decimal_value is None:
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

        return RiskLimits(
            notional_cap=self.get_decimal(
                None, "RISK_NOTIONAL_CAP", self._settings.risk_notional_cap
            ),
            updated_at=self._updated_at,
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
