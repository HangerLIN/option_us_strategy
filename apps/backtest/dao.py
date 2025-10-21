from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy.engine import RowMapping

from sqlalchemy import text
from sqlalchemy.orm import Session

from libs.db.dao import RiskEventDAO


_EQ_INDICATOR_COLUMNS = [
    "rsi6",
    "rsi12",
    "rsi24",
    "atr14",
    "ao",
    "stoch_k",
    "stoch_d",
    "cci14",
    "cci6",
    "obv",
    "obv_ema20",
    "mfi14",
    "rvol6",
]


@dataclass(frozen=True, slots=True)
class EquityBarRow:
    ts_end: datetime
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    rsi6: Decimal | None = None
    rsi12: Decimal | None = None
    rsi24: Decimal | None = None
    atr14: Decimal | None = None
    ao: Decimal | None = None
    stoch_k: Decimal | None = None
    stoch_d: Decimal | None = None
    cci14: Decimal | None = None
    cci6: Decimal | None = None
    obv: Decimal | None = None
    obv_ema20: Decimal | None = None
    mfi14: Decimal | None = None
    rvol6: Decimal | None = None


@dataclass(frozen=True, slots=True)
class OptionBarRow:
    ts_end: datetime
    conid: int | None
    symbol: str
    expiry: date | datetime | None
    strike: Decimal | None
    right: str | None
    trading_class: str | None
    multiplier: str | None
    exchange: str | None
    bid: Decimal | None
    ask: Decimal | None
    mid: Decimal | None
    last: Decimal | None
    volume: int | None
    open_interest: int | None
    implied_vol: Decimal | None
    delta: Decimal | None
    gamma: Decimal | None
    theta: Decimal | None
    vega: Decimal | None
    underlying_price: Decimal | None


@dataclass(frozen=True, slots=True)
class OptionCandidateRow:
    conid: int | None
    underlying_symbol: str
    expiry: date | datetime | None
    strike: Decimal | None
    option_right: str | None
    dte: int | None
    delta: Decimal | None
    gamma: Decimal | None
    theta: Decimal | None
    vega: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    mid: Decimal | None
    open_interest: int | None
    volume: int | None
    min_tick: Decimal | None
    trading_class: str | None
    multiplier: str | None
    exchange: str | None
    implied_vol: Decimal | None
    underlying_price: Decimal | None


@dataclass(frozen=True, slots=True)
class BacktestRunRow:
    run_id: int
    strategy_code: str
    started_at: datetime
    completed_at: datetime | None
    status: str
    parameters: Mapping[str, Any] | None
    notes: str | None


@dataclass(frozen=True, slots=True)
class BacktestMetricDailyRow:
    trade_date: date
    metric_code: str
    metric_value: Decimal


@dataclass(frozen=True, slots=True)
class BacktestMetricTotalRow:
    metric_code: str
    metric_value: Decimal


@dataclass(frozen=True, slots=True)
class BacktestSignalRow:
    ts_end: datetime
    symbol: str
    signal_code: str
    accepted: bool | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class RiskEventRow:
    event_ts: datetime
    symbol: str | None
    event_code: str
    severity: str
    payload: Mapping[str, Any] | None


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        raise ValueError("Expected decimal value, got None")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _to_decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    return int(value)


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return _to_int(value)


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _to_bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, Decimal)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "t", "1"}:
            return True
        if lowered in {"false", "f", "0"}:
            return False
    raise ValueError(f"Cannot convert value to bool: {value!r}")


def _ensure_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    raise TypeError(f"Expected datetime-compatible value, got {type(value)!r}")


class _QueryStore:
    _cache: dict[str, str] | None = None

    @classmethod
    def load(cls) -> dict[str, str]:
        if cls._cache is not None:
            return cls._cache

        query_path = Path(__file__).resolve().parents[2] / "libs" / "db" / "queries_backtest.sql"
        if not query_path.exists():
            raise RuntimeError(f"Backtest query file not found: {query_path}")

        queries: dict[str, str] = {}
        current_name: str | None = None
        buffer: list[str] = []

        with query_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if line.strip().startswith("-- name:"):
                    if current_name and buffer:
                        queries[current_name] = "\n".join(buffer).strip()
                    current_name = line.split(":", 1)[1].strip()
                    buffer = []
                else:
                    buffer.append(line)

        if current_name and buffer:
            queries[current_name] = "\n".join(buffer).strip()

        cls._cache = queries
        return queries


class BacktestDAO:
    def __init__(self, session: Session, *, option_bar_table: str, option_chain_table: str) -> None:
        self._session = session
        self._option_bar_table = option_bar_table
        self._option_chain_table = option_chain_table
        self._queries = _QueryStore.load()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _sql(self, name: str) -> str:
        try:
            template = self._queries[name]
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise RuntimeError(f"Missing query template: {name}") from exc
        return template.format(
            option_bar_table=self._option_bar_table,
            option_chain_table=self._option_chain_table,
        )

    def ensure_tables(self, tables: Sequence[str]) -> None:
        sql = text(self._sql("check_table_exists"))
        for table in tables:
            regclass = table if "." in table else f"public.{table}"
            exists = self._session.execute(sql, {"table_name": regclass}).scalar()
            if not exists:
                raise RuntimeError(f"Required table missing: {table}")

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def fetch_equity_bars(self, *, symbol: str, start_ts, end_ts) -> list[EquityBarRow]:
        sql = text(self._sql("select_equity_bars"))
        result = self._session.execute(
            sql,
            {"symbol": symbol, "start_ts": start_ts, "end_ts": end_ts},
        )
        rows = result.mappings().all()
        return [self._map_equity_bar(row) for row in rows]

    def fetch_option_bars_by_conid(
        self, *, conid: int, start_ts, end_ts
    ) -> list[OptionBarRow]:
        sql = text(self._sql("select_option_bars"))
        result = self._session.execute(
            sql,
            {"conid": conid, "start_ts": start_ts, "end_ts": end_ts},
        )
        rows = result.mappings().all()
        return [self._map_option_bar(row) for row in rows]

    def fetch_option_bars_by_attributes(
        self,
        *,
        underlying_symbol: str,
        expiry,
        strike,
        right: str,
        start_ts,
        end_ts,
    ) -> list[OptionBarRow]:
        sql = text(self._sql("select_option_bars_by_attributes"))
        result = self._session.execute(
            sql,
            {
                "underlying_symbol": underlying_symbol,
                "expiry": expiry,
                "strike": strike,
                "right": right,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        )
        rows = result.mappings().all()
        return [self._map_option_bar(row) for row in rows]

    def fetch_option_candidates(
        self,
        *,
        trade_date,
        underlying_symbol: str,
        option_right: str,
        dte_min: int,
        dte_max: int,
    ) -> list[OptionCandidateRow]:
        sql = text(self._sql("select_option_candidates"))
        result = self._session.execute(
            sql,
            {
                "trade_date": trade_date,
                "underlying_symbol": underlying_symbol,
                "option_right": option_right,
                "dte_min": dte_min,
                "dte_max": dte_max,
            },
        )
        rows = result.mappings().all()
        return [self._map_option_candidate(row) for row in rows]

    def fetch_runs(
        self,
        *,
        start_from,
        start_to,
        limit: int,
        offset: int = 0,
    ) -> list[BacktestRunRow]:
        sql = text(self._sql("select_runs"))
        rows = self._session.execute(
            sql,
            {
                "start_from": start_from,
                "start_to": start_to,
                "limit": limit,
                "offset": offset,
            },
        )
        result_rows = rows.mappings().all()
        return [self._map_run(row) for row in result_rows]

    def fetch_metrics_daily(self, run_id: int) -> list[BacktestMetricDailyRow]:
        sql = text(self._sql("select_metrics_daily"))
        rows = self._session.execute(sql, {"run_id": run_id})
        result_rows = rows.mappings().all()
        return [self._map_metric_daily(row) for row in result_rows]

    def fetch_metrics_total(self, run_id: int) -> list[BacktestMetricTotalRow]:
        sql = text(self._sql("select_metrics_total"))
        rows = self._session.execute(sql, {"run_id": run_id})
        result_rows = rows.mappings().all()
        return [self._map_metric_total(row) for row in result_rows]

    def fetch_signals_window(
        self,
        *,
        symbol: str,
        start_ts,
        end_ts,
    ) -> list[BacktestSignalRow]:
        sql = text(self._sql("select_bt_signals_window"))
        rows = self._session.execute(
            sql,
            {
                "symbol": symbol,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        )
        result_rows = rows.mappings().all()
        return [self._map_signal(row) for row in result_rows]

    def fetch_risk_events(
        self,
        *,
        symbol: str | None,
        start_ts,
        end_ts,
    ) -> list[RiskEventRow]:
        sql = text(self._sql("select_risk_events_window"))
        rows = self._session.execute(
            sql,
            {
                "symbol": symbol.upper() if symbol else None,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        )
        result_rows = rows.mappings().all()
        return [self._map_risk_event(row) for row in result_rows]

    def fetch_recent_trade_dates(self, *, limit: int, as_of) -> list[date]:
        sql = text(self._sql("select_recent_trade_dates"))
        rows = self._session.execute(
            sql,
            {
                "limit": limit,
                "as_of": as_of,
            },
        ).scalars()
        return list(rows)

    def fetch_trade_dates_between(self, *, start_date: date, end_date: date) -> list[date]:
        sql = text(self._sql("select_trade_dates_between"))
        rows = self._session.execute(
            sql,
            {
                "start_date": start_date,
                "end_date": end_date,
            },
        ).scalars()
        return list(rows)

    def count_equity_minutes(self, *, symbol: str, start_ts, end_ts) -> int:
        sql = text(self._sql("select_equity_rth_minutes"))
        value = self._session.execute(
            sql,
            {
                "symbol": symbol,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        ).scalar()
        return int(value or 0)

    def fetch_equity_volume_top(self, *, start_ts, end_ts, limit: int) -> list[tuple[str, int]]:
        sql = text(self._sql("select_equity_volume_top"))
        rows = self._session.execute(
            sql,
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "limit": limit,
            },
        ).all()
        return [(row[0], int(row[1] or 0)) for row in rows]

    def count_option_minutes(self, *, conid: int, start_ts, end_ts) -> int:
        sql = text(self._sql("count_option_l1_minutes"))
        value = self._session.execute(
            sql,
            {
                "conid": conid,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        ).scalar()
        return int(value or 0)

    def fetch_option_candidates_filtered(
        self,
        *,
        trade_date,
        underlying_symbol: str,
        option_right: str,
        dte_min: int,
        dte_max: int,
        strike_min,
        strike_max,
    ) -> list[OptionCandidateRow]:
        sql = text(self._sql("select_option_candidates_filtered"))
        rows = self._session.execute(
            sql,
            {
                "trade_date": trade_date,
                "underlying_symbol": underlying_symbol,
                "option_right": option_right,
                "dte_min": dte_min,
                "dte_max": dte_max,
                "strike_min": strike_min,
                "strike_max": strike_max,
            },
        )
        return [self._map_option_candidate(row) for row in rows.mappings().all()]

    def fetch_premarket_top(self, *, trade_date: date) -> list[str]:
        sql = text(self._sql("select_premarket_top_symbols"))
        rows = self._session.execute(sql, {"trade_date": trade_date}).all()
        return [row[0] for row in rows]

    def fetch_option_underlying_top(self, *, trade_date: date, limit: int) -> list[str]:
        sql = text(self._sql("select_option_underlying_top"))
        rows = self._session.execute(
            sql,
            {"trade_date": trade_date, "limit": limit},
        ).all()
        return [row[0] for row in rows]

    def fetch_option_candidate_by_conid(self, conid: int) -> OptionCandidateRow | None:
        sql = text(self._sql("select_option_candidate_by_conid"))
        row = self._session.execute(sql, {"conid": conid}).mappings().first()
        if row is None:
            return None
        return self._map_option_candidate(row)

    def fetch_vix_value(self, *, ts) -> Decimal | None:
        sql = text(self._sql("select_vix_value"))
        value = self._session.execute(sql, {"ts": ts}).scalar()
        if value is None:
            return None
        return _to_decimal(value)

    def relation_exists(self, *, relation_name: str, schema_name: str | None = "public") -> bool:
        sql = text(self._sql("check_relation_exists"))
        value = self._session.execute(
            sql,
            {"relation_name": relation_name, "schema_name": schema_name},
        ).scalar()
        return bool(value)

    # ------------------------------------------------------------------
    # Writes / maintenance helpers
    # ------------------------------------------------------------------
    def upsert_equity_bars_from_df(self, *, symbol: str, df: pd.DataFrame) -> None:
        if df.empty:
            return

        sql = text(self._sql("upsert_equity_bars"))
        payloads: list[dict[str, Any]] = []
        upper_symbol = symbol.upper()

        def _decimal_or_none(value: Any) -> Decimal | None:
            if value is None:
                return None
            if isinstance(value, float) and math.isnan(value):
                return None
            if pd.isna(value):
                return None
            return _to_decimal_or_none(value)

        for ts, row in df.iterrows():
            ts_end = pd.Timestamp(ts)
            if ts_end.tzinfo is None:
                raise ValueError("upsert_equity_bars_from_df requires timezone-aware index")
            ts_utc = ts_end.tz_convert(timezone.utc).to_pydatetime()

            volume_value = row.get("volume")
            if pd.isna(volume_value):
                volume = 0
            else:
                volume = _to_int(volume_value)

            payloads.append(
                {
                    "symbol": upper_symbol,
                    "ts_end": ts_utc,
                    "open": _decimal_or_none(row.get("open")),
                    "high": _decimal_or_none(row.get("high")),
                    "low": _decimal_or_none(row.get("low")),
                    "close": _decimal_or_none(row.get("close")),
                    "volume": volume,
                }
            )

        if payloads:
            self._session.execute(sql, payloads)

    def upsert_equity_indicators_from_df(self, *, symbol: str, df: pd.DataFrame) -> None:
        if df.empty:
            return

        sql = text(self._sql("upsert_equity_indicators"))
        payloads: list[dict[str, Any]] = []
        upper_symbol = symbol.upper()
        indicators = df.reindex(columns=_EQ_INDICATOR_COLUMNS)

        def _decimal_or_none(value: Any) -> Decimal | None:
            if value is None:
                return None
            if isinstance(value, float) and math.isnan(value):
                return None
            if pd.isna(value):
                return None
            return _to_decimal_or_none(value)

        for ts, row in indicators.iterrows():
            ts_end = pd.Timestamp(ts)
            if ts_end.tzinfo is None:
                raise ValueError("upsert_equity_indicators_from_df requires timezone-aware index")
            ts_utc = ts_end.tz_convert(timezone.utc).to_pydatetime()

            payload = {"symbol": upper_symbol, "ts_end": ts_utc}
            for column in _EQ_INDICATOR_COLUMNS:
                payload[column] = _decimal_or_none(row.get(column))
            payloads.append(payload)

        if payloads:
            self._session.execute(sql, payloads)

    def insert_risk_event(
        self,
        *,
        symbol: str,
        event_code: str,
        severity: str = "INFO",
        payload_json: str | None = None,
    ) -> None:
        sql = text(self._sql("insert_risk_event"))
        self._session.execute(
            sql,
            {
                "event_ts": datetime.now(timezone.utc),
                "symbol": symbol.upper(),
                "event_code": event_code,
                "severity": severity,
                "payload": payload_json or "{}",
            },
        )

    def fetch_rvol_baseline(self, *, symbol: str) -> dict[int, Decimal]:
        sql = text(self._sql("select_rvol_baseline"))
        rows = self._session.execute(sql, {"symbol": symbol}).all()
        baseline: dict[int, Decimal] = {}
        for minute_index, mean_vol in rows:
            baseline[int(minute_index)] = _to_decimal(mean_vol)
        return baseline

    # ------------------------------------------------------------------
    # Row adapters
    # ------------------------------------------------------------------
    @staticmethod
    def _map_equity_bar(row: RowMapping) -> EquityBarRow:
        return EquityBarRow(
            ts_end=_ensure_datetime(row["ts_end"]),
            symbol=str(row["symbol"]),
            open=_to_decimal(row["open"]),
            high=_to_decimal(row["high"]),
            low=_to_decimal(row["low"]),
            close=_to_decimal(row["close"]),
            volume=_to_int(row.get("volume")),
            rsi6=_to_decimal_or_none(row.get("rsi6")),
            rsi12=_to_decimal_or_none(row.get("rsi12")),
            rsi24=_to_decimal_or_none(row.get("rsi24")),
            atr14=_to_decimal_or_none(row.get("atr14")),
            ao=_to_decimal_or_none(row.get("ao")),
            stoch_k=_to_decimal_or_none(row.get("stoch_k")),
            stoch_d=_to_decimal_or_none(row.get("stoch_d")),
            cci14=_to_decimal_or_none(row.get("cci14")),
            cci6=_to_decimal_or_none(row.get("cci6")),
            obv=_to_decimal_or_none(row.get("obv")),
            obv_ema20=_to_decimal_or_none(row.get("obv_ema20")),
            mfi14=_to_decimal_or_none(row.get("mfi14")),
            rvol6=_to_decimal_or_none(row.get("rvol6")),
        )

    @staticmethod
    def _map_option_bar(row: RowMapping) -> OptionBarRow:
        return OptionBarRow(
            ts_end=_ensure_datetime(row["ts_end"]),
            conid=_to_int_or_none(row.get("conid")),
            symbol=str(row.get("symbol") or ""),
            expiry=row.get("expiry"),
            strike=_to_decimal_or_none(row.get("strike")),
            right=row.get("right"),
            trading_class=row.get("trading_class"),
            multiplier=row.get("multiplier"),
            exchange=row.get("exchange"),
            bid=_to_decimal_or_none(row.get("bid")),
            ask=_to_decimal_or_none(row.get("ask")),
            mid=_to_decimal_or_none(row.get("mid")),
            last=_to_decimal_or_none(row.get("last")),
            volume=_to_int_or_none(row.get("volume")),
            open_interest=_to_int_or_none(row.get("open_interest")),
            implied_vol=_to_decimal_or_none(row.get("implied_vol")),
            delta=_to_decimal_or_none(row.get("delta")),
            gamma=_to_decimal_or_none(row.get("gamma")),
            theta=_to_decimal_or_none(row.get("theta")),
            vega=_to_decimal_or_none(row.get("vega")),
            underlying_price=_to_decimal_or_none(row.get("underlying_price")),
        )

    @staticmethod
    def _map_option_candidate(row: RowMapping) -> OptionCandidateRow:
        return OptionCandidateRow(
            conid=_to_int_or_none(row.get("conid")),
            underlying_symbol=str(row.get("underlying_symbol") or ""),
            expiry=row.get("expiry"),
            strike=_to_decimal_or_none(row.get("strike")),
            option_right=row.get("option_right"),
            dte=_to_int_or_none(row.get("dte")),
            delta=_to_decimal_or_none(row.get("delta")),
            gamma=_to_decimal_or_none(row.get("gamma")),
            theta=_to_decimal_or_none(row.get("theta")),
            vega=_to_decimal_or_none(row.get("vega")),
            bid=_to_decimal_or_none(row.get("bid")),
            ask=_to_decimal_or_none(row.get("ask")),
            mid=_to_decimal_or_none(row.get("mid")),
            open_interest=_to_int_or_none(row.get("open_interest")),
            volume=_to_int_or_none(row.get("volume")),
            min_tick=_to_decimal_or_none(row.get("min_tick")),
            trading_class=row.get("trading_class"),
            multiplier=row.get("multiplier"),
            exchange=row.get("exchange"),
            implied_vol=_to_decimal_or_none(row.get("implied_vol")),
            underlying_price=_to_decimal_or_none(row.get("underlying_price")),
        )

    @staticmethod
    def _map_run(row: RowMapping) -> BacktestRunRow:
        return BacktestRunRow(
            run_id=_to_int(row["run_id"]),
            strategy_code=str(row["strategy_code"]),
            started_at=_ensure_datetime(row["started_at"]),
            completed_at=(
                _ensure_datetime(row["completed_at"]) if row.get("completed_at") is not None else None
            ),
            status=str(row["status"]),
            parameters=row.get("parameters"),
            notes=row.get("notes"),
        )

    @staticmethod
    def _map_metric_daily(row: RowMapping) -> BacktestMetricDailyRow:
        return BacktestMetricDailyRow(
            trade_date=row["trade_date"],
            metric_code=str(row["metric_code"]),
            metric_value=_to_decimal(row["metric_value"]),
        )

    @staticmethod
    def _map_metric_total(row: RowMapping) -> BacktestMetricTotalRow:
        return BacktestMetricTotalRow(
            metric_code=str(row["metric_code"]),
            metric_value=_to_decimal(row["metric_value"]),
        )

    @staticmethod
    def _map_signal(row: RowMapping) -> BacktestSignalRow:
        return BacktestSignalRow(
            ts_end=_ensure_datetime(row["ts_end"]),
            symbol=str(row["symbol"]),
            signal_code=str(row["signal_code"]),
            accepted=_to_bool_or_none(row.get("accepted")),
            reason=row.get("reason"),
        )

    @staticmethod
    def _map_risk_event(row: RowMapping) -> RiskEventRow:
        payload = row.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}
        return RiskEventRow(
            event_ts=_ensure_datetime(row["event_ts"]),
            symbol=str(row["symbol"]) if row.get("symbol") else None,
            event_code=str(row["event_code"]),
            severity=str(row.get("severity") or "INFO"),
            payload=payload if isinstance(payload, Mapping) else None,
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------
    def create_run(
        self,
        *,
        strategy_code: str,
        started_at,
        status: str,
        parameters: Mapping[str, Any] | None = None,
        notes: str | None = None,
    ) -> int:
        sql = text(self._sql("insert_bt_run"))
        payload = {
            "strategy_code": strategy_code,
            "started_at": started_at,
            "status": status,
            "parameters": json.dumps(parameters) if isinstance(parameters, Mapping) else parameters,
            "notes": notes,
        }
        row = self._session.execute(sql, payload).first()
        if row is None:
            raise RuntimeError("Failed to insert bt_run")
        return int(row[0])

    def update_run_status(
        self,
        *,
        run_id: int,
        status: str,
        completed_at,
        notes: str | None = None,
    ) -> None:
        sql = text(self._sql("update_bt_run_status"))
        self._session.execute(
            sql,
            {
                "run_id": run_id,
                "status": status,
                "completed_at": completed_at,
                "notes": notes,
            },
        )

    def record_trade(self, payload: Mapping[str, Any]) -> None:
        sql = text(self._sql("insert_bt_trade"))
        self._session.execute(sql, payload)

    def record_block(self, payload: Mapping[str, Any]) -> None:
        reason_payload = {
            "code": payload.get("reason_code"),
            "side": payload.get("side"),
            "trace_id": payload.get("trace_id"),
            "option_right": payload.get("option_right"),
            "strike": payload.get("strike"),
            "expiry": payload.get("expiry"),
            "price": payload.get("price"),
            "quantity": payload.get("quantity"),
            "slippage": payload.get("slippage"),
        }
        sql = text(self._sql("update_bt_signal_reason"))
        self._session.execute(
            sql,
            {
                "run_id": payload["run_id"],
                "ts_end": payload["trade_ts"],
                "symbol": payload["symbol"],
                "signal_code": payload["signal_code"],
                "reason": json.dumps(reason_payload, default=_json_default),
            },
        )

    def record_signal(self, payload: Mapping[str, Any]) -> None:
        sql = text(self._sql("insert_bt_signal"))
        self._session.execute(sql, payload)

    def record_metrics_daily(self, rows: Iterable[Mapping[str, Any]]) -> None:
        sql = text(self._sql("insert_bt_metric_daily"))
        for row in rows:
            self._session.execute(sql, row)

    def record_metrics_total(self, rows: Iterable[Mapping[str, Any]]) -> None:
        sql = text(self._sql("insert_bt_metric_total"))
        for row in rows:
            self._session.execute(sql, row)

    def record_risk_event(
        self,
        *,
        event_ts: datetime,
        symbol: str | None,
        event_code: str,
        severity: str,
        payload: Mapping[str, Any] | None = None,
        message: str | None = None,
    ) -> None:
        dao = RiskEventDAO(self._session)
        dao.create_event(
            event_ts=event_ts,
            event_code=event_code,
            severity=severity,
            symbol=symbol.upper() if symbol else None,
            message=message or event_code.lower(),
            payload=payload or {},
        )
