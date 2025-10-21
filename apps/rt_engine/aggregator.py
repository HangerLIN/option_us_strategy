from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from queue import Empty, Queue
from typing import Any, Awaitable, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Literal, cast
from uuid import uuid4
import json

import structlog
import pandas as pd
import redis
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from libs.core import EASTERN, attach_trace_metadata, configure_logging, get_settings, utc_now
from libs.core.config import Settings
from libs.db import Base, PremarketTop5, StrategyPosition
from libs.db.dao import RiskStateDAO
from libs.infra import IBClient, RedisBus, build_ibkr_client
from libs.schemas.events import BarsClosed
from .top5_service import Top5Service
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from .indicators import compute_indicator_row


@dataclass
class AggregatedBar:
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    ts_end: datetime


@dataclass
class SymbolMeta:
    alias: str
    kind: Literal["EQUITY", "OPTION"]
    underlying: str
    conid: int | None = None
    expiry: datetime | None = None
    right: str | None = None
    strike: Decimal | None = None
    min_tick: Decimal | None = None
    contract: Contract | None = None


class MinuteAggregator:
    """1m bar aggregator with NBBO fallback and short-gap backfill."""

    def __init__(
        self,
        ib_client: IBClient,
        session_factory: sessionmaker,
        settings: Settings,
        *,
        watchlist_refresh_seconds: int = 30,
        static_symbols: Iterable[str] | None = None,
        vix_required: bool = True,
    ) -> None:
        self._ib_client = ib_client
        self._session_factory = session_factory
        self._settings = settings
        self._buffers: Dict[str, List[Tuple[datetime, Mapping[str, Decimal]]]] = defaultdict(list)
        self._quotes: Dict[str, Dict[str, Optional[Decimal]]] = defaultdict(
            lambda: {"bid": None, "ask": None}
        )
        self._last_trade: Dict[str, Optional[Decimal]] = {}
        self._last_persisted: Dict[str, Optional[datetime]] = {}
        self._baseline_cache: Dict[str, Dict[int, Decimal]] = {}
        self._last_rvol_refresh: date | None = None
        self._symbol_tasks: Dict[str, asyncio.Future] = {}
        self._symbol_queues: Dict[str, Queue] = {}
        self._active_symbols: Set[str] = set()
        self._static_symbols: Set[str] = {symbol.upper() for symbol in (static_symbols or [])}
        self._vix_symbol = "VIX"
        self._vix_queue: Optional[Queue] = None
        self._vix_task: Optional[asyncio.Future] = None
        self._vix_last_price: Optional[Decimal] = None
        self._vix_required = vix_required
        refresh = max(watchlist_refresh_seconds, 5)
        self._watchlist_refresh = timedelta(seconds=refresh)
        self._next_watchlist_refresh: Optional[datetime] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = True
        self._logger = structlog.get_logger(__name__)
        self._symbol_meta: Dict[str, SymbolMeta] = {}
        self._underlying_options: Dict[str, List[str]] = defaultdict(list)
        self._last_equity_close: Dict[str, Decimal] = {}
        self._option_metrics: Dict[str, Dict[str, Decimal]] = {}
        self._volume_snapshots: Dict[str, Decimal] = {}
        self._option_generic_ticks = "100,101,104,106,233"
        try:
            self._redis: redis.Redis | None = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        except Exception:  # pragma: no cover - external dependency
            self._redis = None
            self._logger.warning("aggregator.redis_init_failed")
        try:
            self._redis_bus: RedisBus | None = RedisBus(settings.redis_url)
        except Exception:  # pragma: no cover - external dependency
            self._redis_bus = None
            self._logger.warning("aggregator.redis_bus_init_failed")

        for symbol in self._static_symbols:
            self._ensure_equity_meta(symbol)

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        vix_started = False
        try:
            self._start_vix_subscription()
            vix_started = True
        except Exception as exc:  # pragma: no cover - external dependency
            if self._vix_required:
                self._logger.exception("aggregator.vix_subscription_failed", error=str(exc))
                raise
            self._logger.warning("aggregator.vix_subscription_optional_failed", error=str(exc))
        try:
            while True:
                now = utc_now()
                if self._next_watchlist_refresh is None or now >= self._next_watchlist_refresh:
                    self._refresh_watchlist()
                    self._next_watchlist_refresh = now + self._watchlist_refresh
                next_close = (now.replace(second=0, microsecond=0) + timedelta(minutes=1)).replace(
                    tzinfo=timezone.utc
                )
                await asyncio.sleep(max(0.0, (next_close - now).total_seconds()))
                self._close_minute(next_close)
                self._maybe_refresh_rvol_baseline(next_close)
        finally:
            self._running = False
            if vix_started:
                self._stop_vix_subscription()
            self._teardown_subscriptions()
            if self._redis_bus is not None:
                try:
                    await self._redis_bus.close()
                except Exception:  # pragma: no cover - best-effort close
                    self._logger.warning("aggregator.redis_bus_close_failed")

    # ------------------------------------------------------------------
    def _consume_ticks(self, symbol: str, queue: Queue) -> None:
        while True:
            if not self._running:
                break
            try:
                payload = queue.get(timeout=1)
            except Empty:
                continue
            if payload is None:
                break
            tick_time = utc_now()
            record: Dict[str, Decimal] = {}
            event_type = payload.get("type")
            if event_type == "price":
                price = Decimal(str(payload.get("price")))
                field = payload.get("field")
                if field == 1:
                    self._quotes[symbol]["bid"] = price
                elif field == 2:
                    self._quotes[symbol]["ask"] = price
                elif field == 4:
                    self._last_trade[symbol] = price
                record["price"] = price
            elif event_type == "rt_volume":
                price = Decimal(str(payload.get("price", 0)))
                size = Decimal(str(payload.get("single_trade_volume", 0)))
                self._last_trade[symbol] = price
                record["price"] = price
                record["size"] = size
            elif event_type == "greeks":
                meta = self._symbol_meta.get(symbol)
                if meta and meta.kind == "OPTION":
                    metrics = self._option_metrics.setdefault(symbol, {})
                    iv_val = payload.get("implied_vol")
                    if iv_val is not None:
                        metrics["implied_vol"] = Decimal(str(iv_val))
                    delta_val = payload.get("delta")
                    if delta_val is not None:
                        metrics["delta"] = Decimal(str(delta_val))
                    gamma_val = payload.get("gamma")
                    if gamma_val is not None:
                        metrics["gamma"] = Decimal(str(gamma_val))
                    vega_val = payload.get("vega")
                    if vega_val is not None:
                        metrics["vega"] = Decimal(str(vega_val))
                    theta_val = payload.get("theta")
                    if theta_val is not None:
                        metrics["theta"] = Decimal(str(theta_val))
                    und_val = payload.get("underlying_price")
                    if und_val is not None:
                        und_price = Decimal(str(und_val))
                        metrics["underlying_price"] = und_price
                        self._last_equity_close[meta.underlying] = und_price
            elif event_type == "size":
                meta = self._symbol_meta.get(symbol)
                if meta and meta.kind == "OPTION":
                    field = payload.get("field")
                    size_value = Decimal(str(payload.get("size", "0")))
                    if field == TickTypeEnum.LAST_SIZE:
                        record["size"] = size_value
                    elif field in {
                        TickTypeEnum.VOLUME,
                        TickTypeEnum.OPTION_CALL_VOLUME,
                        TickTypeEnum.OPTION_PUT_VOLUME,
                    }:
                        previous = self._volume_snapshots.get(symbol)
                        self._volume_snapshots[symbol] = size_value
                        if previous is not None:
                            delta = size_value - previous
                            if delta > 0:
                                record["size"] = delta
                    elif field in {
                        TickTypeEnum.OPTION_CALL_OPEN_INTEREST,
                        TickTypeEnum.OPTION_PUT_OPEN_INTEREST,
                    }:
                        metrics = self._option_metrics.setdefault(symbol, {})
                        metrics["open_interest"] = size_value
            elif event_type == "generic":
                meta = self._symbol_meta.get(symbol)
                if meta and meta.kind == "OPTION":
                    field = payload.get("field")
                    value = payload.get("value")
                    if field == TickTypeEnum.OPEN_INTEREST and value is not None:
                        metrics = self._option_metrics.setdefault(symbol, {})
                        metrics["open_interest"] = Decimal(str(value))

            if record:
                self._buffers[symbol].append((tick_time, record))

    def _ensure_equity_meta(self, symbol: str) -> SymbolMeta:
        alias = symbol.upper()
        meta = self._symbol_meta.get(alias)
        if meta is None:
            meta = SymbolMeta(alias=alias, kind="EQUITY", underlying=alias)
            self._symbol_meta[alias] = meta
        return meta

    def _option_alias(self, underlying: str, candidate: Mapping[str, Any]) -> str:
        conid = candidate.get("conid")
        if conid:
            return f"OPT:{underlying}:{int(conid)}"
        expiry = candidate["expiry"].strftime("%Y%m%d") if candidate.get("expiry") else ""
        right = str(candidate.get("option_right") or "").upper()
        strike = Decimal(str(candidate.get("strike"))).normalize()
        return f"OPT:{underlying}:{right}:{expiry}:{strike}"

    def _select_option_candidate(
        self, symbol: str, ts_end: datetime, option_right: str
    ) -> Optional[dict[str, Any]]:
        try:
            quote = self._ib_client.best_option_contract(
                symbol,
                right=option_right,
                reference=ts_end,
                dte_range=(2, 7),
                otm_steps=(2, 5),
                min_open_interest=500,
                min_volume=100,
                max_spread_pct=Decimal("0.05"),
                max_abs_spread=Decimal("0.10"),
            )
        except Exception as exc:
            self._logger.warning(
                "aggregator.option_candidate_ibkr_failed",
                symbol=symbol,
                option_right=option_right,
                error=str(exc),
            )
            return None

        if quote is None:
            return None

        return {
            "conid": quote.conid,
            "underlying_symbol": symbol,
            "expiry": quote.expiry,
            "strike": quote.strike,
            "option_right": quote.right,
            "bid": quote.bid,
            "ask": quote.ask,
            "mid": quote.mid,
            "open_interest": quote.open_interest,
            "volume": quote.volume,
            "min_tick": quote.min_tick,
            "dte": quote.dte,
            "contract": quote.contract,
        }

    def _ensure_option_aliases(self, session: Session, symbol: str) -> List[str]:
        aliases: List[str] = []
        previous_aliases = list(self._underlying_options.get(symbol, []))
        for option_right in ("CALL", "PUT"):
            try:
                candidate = self._select_option_candidate(symbol, utc_now(), option_right)
            except Exception as exc:
                self._logger.exception(
                    "aggregator.option_candidate_failed",
                    symbol=symbol,
                    option_right=option_right,
                    error=str(exc),
                )
                continue
            if not candidate:
                continue
            alias = self._option_alias(symbol, candidate)
            meta = self._symbol_meta.get(alias)
            if meta is None:
                meta = SymbolMeta(
                    alias=alias,
                    kind="OPTION",
                    underlying=symbol,
                    conid=candidate.get("conid"),
                    expiry=candidate.get("expiry"),
                    right=option_right,
                    strike=candidate.get("strike"),
                    min_tick=candidate.get("min_tick"),
                    contract=candidate.get("contract"),
                )
                self._symbol_meta[alias] = meta
            else:
                meta.conid = candidate.get("conid")
                meta.expiry = candidate.get("expiry")
                meta.right = option_right
                meta.strike = candidate.get("strike")
                meta.min_tick = candidate.get("min_tick")
                meta.contract = candidate.get("contract")
            self._option_metrics.setdefault(alias, {})
            aliases.append(alias)
        self._underlying_options[symbol] = aliases
        for old_alias in previous_aliases:
            if old_alias not in aliases:
                self._symbol_meta.pop(old_alias, None)
        return aliases

    def _close_minute(self, ts_end: datetime) -> None:
        session: Session = self._session_factory()
        try:
            risk_states: List[dict[str, Any]] = []
            equity_payloads: List[Tuple[str, SymbolMeta, List[Mapping[str, Decimal]]]] = []
            option_payloads: List[Tuple[str, SymbolMeta, List[Mapping[str, Decimal]]]] = []

            for alias, ticks in list(self._buffers.items()):
                if not ticks:
                    continue
                meta = self._symbol_meta.get(alias)
                if meta is None:
                    continue
                if meta.kind == "EQUITY":
                    self._backfill_missing(session, alias, ts_end - timedelta(minutes=1))
                bucket = self._extract_bucket(ticks, ts_end)
                if not bucket:
                    continue
                if meta.kind == "OPTION":
                    option_payloads.append((alias, meta, bucket))
                else:
                    equity_payloads.append((alias, meta, bucket))

            for alias, meta, bucket in equity_payloads:
                bar = self._aggregate(meta, bucket, ts_end)
                self._persist_equity_bar(session, bar)
                self._last_equity_close[meta.underlying] = bar.close
                self._update_indicators(session, meta.underlying, bar.ts_end)
                self._publish_bar(meta, bar)
                self._last_persisted[alias] = bar.ts_end

            for alias, meta, bucket in option_payloads:
                bar = self._aggregate(meta, bucket, ts_end)
                self._persist_option_bar(session, meta, bar)
                self._last_persisted[alias] = bar.ts_end
            if self._vix_last_price is not None:
                risk_states.append(
                    {
                        "ts": ts_end,
                        "symbol": self._vix_symbol,
                        "metric_code": "VIX",
                        "metric_value": self._vix_last_price,
                        "detail": {"source": "ibkr"},
                    }
                )
                self._cache_vix_snapshot(self._vix_last_price)
            if risk_states:
                RiskStateDAO(session).insert_states(risk_states)
            session.commit()
        except Exception:  # pragma: no cover - defensive
            session.rollback()
            self._logger.exception("aggregator.minute_close_failed", ts_end=str(ts_end))
        finally:
            session.close()

    def _maybe_refresh_rvol_baseline(self, ts_end: datetime) -> None:
        et = ts_end.astimezone(EASTERN)
        if et.hour < 16 or (et.hour == 16 and et.minute < 10):
            return
        trade_date = et.date()
        if self._last_rvol_refresh == trade_date:
            return
        try:
            self._refresh_rvol_baseline(trade_date)
        except Exception:  # pragma: no cover - defensive
            self._logger.exception("aggregator.rvol_refresh_failed", trade_date=str(trade_date))
        else:
            self._last_rvol_refresh = trade_date

    def _refresh_rvol_baseline(self, trade_date: date) -> None:
        symbols = sorted(
            {
                meta.underlying
                for meta in self._symbol_meta.values()
                if meta.kind == "EQUITY"
            }
        )
        if not symbols:
            self._logger.info(
                "aggregator.rvol_refresh_skipped",
                trade_date=str(trade_date),
                reason="no_equity_symbols",
            )
            return

        end_et = datetime.combine(trade_date, time(16, 0), EASTERN)
        lookback_days = max(self._settings.rvol_baseline_days * 2, 30)
        start_et = end_et - timedelta(days=lookback_days)

        session: Session = self._session_factory()
        try:
            for symbol in symbols:
                try:
                    bars = self._ib_client.req_historical_1m(
                        symbol, start_et, end_et, use_rth=True
                    )
                except Exception as exc:  # pragma: no cover - external call
                    self._logger.warning(
                        "aggregator.rvol_refresh_ib_error",
                        symbol=symbol,
                        trade_date=str(trade_date),
                        error=str(exc),
                    )
                    continue

                per_day: Dict[date, Dict[int, Decimal]] = {}
                for raw in bars:
                    dt_et = self._ib_bar_to_eastern(raw.get("time"))
                    if dt_et is None:
                        continue
                    if dt_et.date() > trade_date:
                        continue
                    if (
                        dt_et.time() < time(9, 30)
                        or dt_et.time() >= time(16, 0)
                    ):
                        continue
                    minute_index = dt_et.hour * 60 + dt_et.minute
                    try:
                        volume_val = Decimal(str(raw.get("volume", "0") or "0"))
                    except Exception:
                        volume_val = Decimal("0")
                    day_bucket = per_day.setdefault(dt_et.date(), {})
                    day_bucket[minute_index] = volume_val

                if not per_day:
                    self._logger.info(
                        "aggregator.rvol_refresh_no_data",
                        symbol=symbol,
                        trade_date=str(trade_date),
                    )
                    continue

                selected_days = sorted(per_day.keys())[-self._settings.rvol_baseline_days :]
                minute_samples: Dict[int, List[Decimal]] = defaultdict(list)
                for day in selected_days:
                    for minute_idx, volume_val in per_day[day].items():
                        minute_samples[minute_idx].append(volume_val)

                if not minute_samples:
                    self._logger.info(
                        "aggregator.rvol_refresh_no_samples",
                        symbol=symbol,
                        trade_date=str(trade_date),
                    )
                    continue

                entries: List[Dict[str, Any]] = []
                baseline_map: Dict[int, Decimal] = {}
                for minute_idx, samples in minute_samples.items():
                    if not samples:
                        continue
                    total = sum(samples, Decimal("0"))
                    mean_volume = total / Decimal(len(samples))
                    baseline_map[minute_idx] = mean_volume
                    entries.append(
                        {
                            "symbol": symbol,
                            "minute_index": minute_idx,
                            "mean_vol_20d": float(mean_volume),
                        }
                    )

                try:
                    session.execute(
                        text("DELETE FROM rvol_baseline_eq WHERE symbol = :symbol"),
                        {"symbol": symbol},
                    )
                    if entries:
                        session.execute(
                            text(
                                """
                                INSERT INTO rvol_baseline_eq (symbol, minute_index, mean_vol_20d)
                                VALUES (:symbol, :minute_index, :mean_vol_20d)
                                ON CONFLICT (symbol, minute_index) DO UPDATE
                                SET mean_vol_20d = EXCLUDED.mean_vol_20d,
                                    updated_at = now()
                                """
                            ),
                            entries,
                        )
                    session.commit()
                    self._baseline_cache[symbol] = baseline_map
                    self._logger.info(
                        "aggregator.rvol_refresh_symbol",
                        symbol=symbol,
                        minutes=len(entries),
                        trade_date=str(trade_date),
                        days=len(selected_days),
                    )
                except Exception:
                    session.rollback()
                    self._logger.exception(
                        "aggregator.rvol_refresh_persist_failed", symbol=symbol
                    )
        finally:
            session.close()

    def _extract_bucket(
        self,
        ticks: List[Tuple[datetime, Mapping[str, Decimal]]],
        ts_end: datetime,
    ) -> List[Mapping[str, Decimal]]:
        cutoff = ts_end - timedelta(minutes=1)
        bucket = [payload for tick_ts, payload in ticks if cutoff < tick_ts <= ts_end]
        ticks[:] = [(tick_ts, payload) for tick_ts, payload in ticks if tick_ts > ts_end]
        return bucket

    def _aggregate(
        self,
        meta: SymbolMeta,
        bucket: List[Mapping[str, Decimal]],
        ts_end: datetime,
    ) -> AggregatedBar:
        prices = [record["price"] for record in bucket if "price" in record]
        if prices:
            open_price = prices[0]
            high_price = max(prices)
            low_price = min(prices)
            close_price = prices[-1]
            volume = sum((record.get("size", Decimal("0")) for record in bucket), Decimal("0"))
            return AggregatedBar(
                meta.alias, open_price, high_price, low_price, close_price, volume, ts_end
            )

        quote = self._quotes[meta.alias]
        bid = quote.get("bid")
        ask = quote.get("ask")
        if bid is not None and ask is not None:
            mid = (bid + ask) / Decimal(2)
        else:
            mid = self._last_trade.get(meta.alias) or Decimal("0")
        return AggregatedBar(meta.alias, mid, mid, mid, mid, Decimal("0"), ts_end)

    def _persist_equity_bar(self, session: Session, bar: AggregatedBar) -> None:
        stmt = text(
            """
            INSERT INTO bars1m_equity (ts_end, symbol, open, high, low, close, volume)
            VALUES (:ts_end, :symbol, :open, :high, :low, :close, :volume)
            ON CONFLICT (symbol, ts_end) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """
        )
        session.execute(
            stmt,
            {
                "ts_end": bar.ts_end,
                "symbol": bar.symbol,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            },
        )

    def _persist_option_bar(self, session: Session, meta: SymbolMeta, bar: AggregatedBar) -> None:
        if meta.conid is None:
            return
        quote = self._quotes.get(meta.alias, {})
        bid = quote.get("bid")
        ask = quote.get("ask")
        metrics = self._option_metrics.get(meta.alias, {})
        iv = metrics.get("implied_vol")
        delta = metrics.get("delta")
        gamma = metrics.get("gamma")
        theta = metrics.get("theta")
        vega = metrics.get("vega")
        oi = metrics.get("open_interest")
        und_price = metrics.get("underlying_price") or self._last_equity_close.get(meta.underlying)
        stmt = text(
            """
            INSERT INTO bars1m_option (
                conid,
                underlying_symbol,
                expiry,
                right,
                strike,
                ts_end,
                bid,
                ask,
                mid,
                volume,
                open_interest,
                implied_vol,
                delta,
                gamma,
                theta,
                vega,
                underlying_price
            ) VALUES (
                :conid,
                :underlying_symbol,
                :expiry,
                :right,
                :strike,
                :ts_end,
                :bid,
                :ask,
                :mid,
                :volume,
                :open_interest,
                :implied_vol,
                :delta,
                :gamma,
                :theta,
                :vega,
                :underlying_price
            )
            ON CONFLICT (conid, ts_end) DO UPDATE SET
                bid = EXCLUDED.bid,
                ask = EXCLUDED.ask,
                mid = EXCLUDED.mid,
                volume = EXCLUDED.volume,
                open_interest = EXCLUDED.open_interest,
                implied_vol = EXCLUDED.implied_vol,
                delta = EXCLUDED.delta,
                gamma = EXCLUDED.gamma,
                theta = EXCLUDED.theta,
                vega = EXCLUDED.vega,
                underlying_price = EXCLUDED.underlying_price,
                updated_at = now()
            """
        )
        session.execute(
            stmt,
            {
                "conid": meta.conid,
                "underlying_symbol": meta.underlying,
                "expiry": meta.expiry.date() if meta.expiry else None,
                "right": meta.right,
                "strike": float(meta.strike) if meta.strike is not None else None,
                "ts_end": bar.ts_end,
                "bid": float(bid) if bid is not None else None,
                "ask": float(ask) if ask is not None else None,
                "mid": float(((bid + ask) / 2)) if bid is not None and ask is not None else None,
                "volume": int(bar.volume),
                "open_interest": int(oi) if oi is not None else None,
                "implied_vol": float(iv) if iv is not None else None,
                "delta": float(delta) if delta is not None else None,
                "gamma": float(gamma) if gamma is not None else None,
                "theta": float(theta) if theta is not None else None,
                "vega": float(vega) if vega is not None else None,
                "underlying_price": float(und_price) if und_price is not None else None,
            },
        )

    def _publish_bar(self, meta: SymbolMeta, bar: AggregatedBar) -> None:
        if meta.kind != "EQUITY":
            return
        event = BarsClosed(
            trace_id=str(uuid4()),
            symbol=meta.underlying,
            bar_start=bar.ts_end - timedelta(minutes=1),
            bar_end=bar.ts_end,
            timeframe="1m",
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=int(bar.volume),
            vwap=None,
            source="aggregator",
            received_at=utc_now(),
        )
        payload = attach_trace_metadata(event.model_dump())
        self._logger.info("aggregator.bar_closed", payload=payload)
        if self._redis_bus is not None:
            trace_id = str(payload.get("trace_id") or "")
            if trace_id:
                publish_coro = self._redis_bus.publish("bars_closed", payload, trace_id=trace_id)
                if self._loop and self._loop.is_running():
                    self._loop.create_task(self._safe_publish(publish_coro, meta.underlying))
                else:
                    asyncio.create_task(self._safe_publish(publish_coro, meta.underlying))

    async def _safe_publish(self, awaitable: Awaitable[Any], symbol: str) -> None:
        try:
            await awaitable
        except Exception:  # pragma: no cover - external dependency
            self._logger.exception("aggregator.bar_publish_failed", symbol=symbol)

    # ------------------------------------------------------------------
    def _backfill_missing(self, session: Session, symbol: str, target_ts_end: datetime) -> None:
        meta = self._symbol_meta.get(symbol)
        if meta is None or meta.kind == "OPTION":
            return
        last_ts = self._last_persisted.get(symbol)
        if last_ts is None:
            result = session.execute(
                text("SELECT max(ts_end) FROM bars1m_equity WHERE symbol=:symbol"),
                {"symbol": symbol},
            ).scalar_one_or_none()
            if isinstance(result, datetime):
                last_ts = result.astimezone(timezone.utc)
            self._last_persisted[symbol] = last_ts

        if last_ts is None:
            return

        missing = int((target_ts_end - last_ts).total_seconds() // 60)
        if missing <= 0:
            return
        if missing > 15:
            self._logger.warning(
                "aggregator.gap_exceeds_window",
                symbol=symbol,
                last_seen=str(last_ts),
                target=str(target_ts_end),
            )
            return

        start = last_ts + timedelta(minutes=1)
        end = target_ts_end
        try:
            bars = self._ib_client.req_historical_1m(
                symbol,
                start.astimezone(EASTERN),
                end.astimezone(EASTERN),
                use_rth=True,
            )
        except Exception as exc:  # pragma: no cover - external call
            self._logger.warning("aggregator.backfill_failed", symbol=symbol, error=str(exc))
            return

        for raw in bars:
            aggregated = self._convert_historical_bar(symbol, raw)
            if aggregated.ts_end <= last_ts or aggregated.ts_end > target_ts_end:
                continue
            session.execute(
                text(
                    """
                    INSERT INTO bars1m_equity (ts_end, symbol, open, high, low, close, volume)
                    VALUES (:ts_end, :symbol, :open, :high, :low, :close, :volume)
                    ON CONFLICT (symbol, ts_end) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                    """
                ),
                {
                    "ts_end": aggregated.ts_end,
                    "symbol": aggregated.symbol,
                    "open": aggregated.open,
                    "high": aggregated.high,
                    "low": aggregated.low,
                    "close": aggregated.close,
                    "volume": aggregated.volume,
                },
            )
            last_ts = aggregated.ts_end
        self._last_persisted[symbol] = last_ts

    def _convert_historical_bar(self, symbol: str, raw: Mapping[str, object]) -> AggregatedBar:
        time_str = str(raw.get("time"))
        dt_et = datetime.strptime(time_str, "%Y%m%d %H:%M:%S").replace(tzinfo=EASTERN)
        ts_end_utc = (dt_et + timedelta(minutes=1)).astimezone(timezone.utc)
        return AggregatedBar(
            symbol=symbol,
            open=Decimal(str(raw.get("open", "0"))),
            high=Decimal(str(raw.get("high", "0"))),
            low=Decimal(str(raw.get("low", "0"))),
            close=Decimal(str(raw.get("close", "0"))),
            volume=Decimal(str(raw.get("volume", "0"))),
            ts_end=ts_end_utc,
        )

    def _update_indicators(self, session: Session, symbol: str, ts_end: datetime) -> None:
        rows = session.execute(
            text(
                """
                SELECT ts_end, open, high, low, close, volume
                FROM bars1m_equity
                WHERE symbol = :symbol AND ts_end <= :ts_end
                ORDER BY ts_end DESC
                LIMIT 120
                """
            ),
            {"symbol": symbol, "ts_end": ts_end},
        ).all()
        if not rows:
            return

        df = pd.DataFrame(rows, columns=["ts_end", "open", "high", "low", "close", "volume"])
        df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)
        df.set_index("ts_end", inplace=True)
        df = df.sort_index()

        baseline_map = self._get_baseline_map(session, symbol)
        indicators = compute_indicator_row(df, baseline_map)
        if not indicators:
            return

        payload = {
            "ts_end": ts_end,
            "symbol": symbol,
            **{key: _to_decimal(value) for key, value in indicators.items()},
        }

        session.execute(
            text(
                """
                INSERT INTO indicators_eq_1m (
                    ts_end, symbol,
                    rsi6, rsi12, rsi24,
                    boll_mid, boll_up, boll_dn,
                    atr14, ao,
                    stoch_k, stoch_d,
                    cci14, cci6,
                    obv, obv_ema20,
                    mfi14, rvol6
                ) VALUES (
                    :ts_end, :symbol,
                    :rsi6, :rsi12, :rsi24,
                    :boll_mid, :boll_up, :boll_dn,
                    :atr14, :ao,
                    :stoch_k, :stoch_d,
                    :cci14, :cci6,
                    :obv, :obv_ema20,
                    :mfi14, :rvol6
                )
                ON CONFLICT (symbol, ts_end) DO UPDATE SET
                    rsi6 = EXCLUDED.rsi6,
                    rsi12 = EXCLUDED.rsi12,
                    rsi24 = EXCLUDED.rsi24,
                    boll_mid = EXCLUDED.boll_mid,
                    boll_up = EXCLUDED.boll_up,
                    boll_dn = EXCLUDED.boll_dn,
                    atr14 = EXCLUDED.atr14,
                    ao = EXCLUDED.ao,
                    stoch_k = EXCLUDED.stoch_k,
                    stoch_d = EXCLUDED.stoch_d,
                    cci14 = EXCLUDED.cci14,
                    cci6 = EXCLUDED.cci6,
                    obv = EXCLUDED.obv,
                    obv_ema20 = EXCLUDED.obv_ema20,
                    mfi14 = EXCLUDED.mfi14,
                    rvol6 = EXCLUDED.rvol6
                """
            ),
            payload,
        )

    def _get_baseline_map(self, session: Session, symbol: str) -> Dict[int, Decimal]:
        cached = self._baseline_cache.get(symbol)
        if cached is not None:
            return cached
        rows = session.execute(
            text("SELECT minute_index, mean_vol_20d FROM rvol_baseline_eq WHERE symbol = :symbol"),
            {"symbol": symbol},
        ).all()
        baseline = {row[0]: Decimal(row[1]) for row in rows}
        self._baseline_cache[symbol] = baseline
        return baseline

    def _ib_bar_to_eastern(self, raw_time: Any) -> datetime | None:
        if raw_time is None:
            return None
        try:
            timestamp = int(raw_time)
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(EASTERN)
        except (TypeError, ValueError):
            try:
                return datetime.strptime(str(raw_time), "%Y%m%d %H:%M:%S").replace(tzinfo=EASTERN)
            except (TypeError, ValueError):
                return None

    # ------------------------------------------------------------------
    def _refresh_watchlist(self) -> None:
        try:
            desired_equities = self._load_watchlist()
        except Exception:  # pragma: no cover - defensive
            self._logger.exception("aggregator.watchlist_refresh_failed")
            desired_equities = set()
        desired_equities |= self._static_symbols

        desired_aliases: Set[str] = set()
        session: Session = self._session_factory()
        try:
            for symbol in desired_equities:
                meta = self._ensure_equity_meta(symbol)
                desired_aliases.add(meta.alias)
                option_aliases = self._ensure_option_aliases(session, symbol)
                desired_aliases.update(option_aliases)
        finally:
            session.close()

        current = set(self._active_symbols)
        to_add = desired_aliases - current
        to_remove = current - desired_aliases

        if to_add or to_remove:
            self._logger.info(
                "aggregator.watchlist_updated",
                added=sorted(to_add),
                removed=sorted(to_remove),
            )

        client = self._ib_client
        for alias in to_add:
            if self._loop is None:
                raise RuntimeError("Event loop not initialised before watchlist refresh")
            alias_meta = self._symbol_meta.get(alias)
            if alias_meta is None:
                continue
            try:
                if alias_meta.kind == "EQUITY":
                    req_id, queue = client.subscribe_l1(
                        alias_meta.underlying,
                        alias=alias,
                        sec_type="STK",
                    )
                else:
                    if not (
                        alias_meta.expiry and alias_meta.right and alias_meta.strike is not None
                    ):
                        self._logger.warning("aggregator.option_meta_incomplete", alias=alias)
                        continue
                    contract = alias_meta.contract
                    if contract is None:
                        contract = client.option_contract(
                            symbol=alias_meta.underlying,
                            expiry=alias_meta.expiry.strftime("%Y%m%d"),
                            strike=float(alias_meta.strike),
                            right=alias_meta.right,
                            conid=alias_meta.conid,
                        )
                    req_id, queue = client.subscribe_l1(
                        alias_meta.underlying,
                        sec_type="OPT",
                        exchange="SMART",
                        currency="USD",
                        generic_ticks=self._option_generic_ticks,
                        alias=alias,
                        contract=contract,
                    )
                    if contract.conId:
                        alias_meta.conid = contract.conId
                        alias_meta.contract = contract
            except Exception as exc:  # pragma: no cover - external
                self._logger.exception("aggregator.subscribe_failed", symbol=alias, error=str(exc))
                continue
            self._symbol_queues[alias] = queue
            task = self._loop.run_in_executor(None, self._consume_ticks, alias, queue)
            self._symbol_tasks[alias] = task
            self._active_symbols.add(alias)
            self._logger.info("aggregator.subscribed", symbol=alias, req_id=req_id)

        for alias in to_remove:
            self._deactivate_symbol(alias)

    def _load_watchlist(self) -> Set[str]:
        session: Session = self._session_factory()
        try:
            et_today = datetime.now(EASTERN).date()
            top5_symbols = session.execute(
                select(PremarketTop5.symbol).where(PremarketTop5.trade_date == et_today)
            ).scalars()
            open_positions = session.execute(
                select(StrategyPosition.symbol).where(StrategyPosition.open_quantity != 0)
            ).scalars()
            symbols = {symbol.upper() for symbol in top5_symbols}
            symbols.update(symbol.upper() for symbol in open_positions)
            return symbols
        finally:
            session.close()

    def _deactivate_symbol(self, alias: str) -> None:
        self._active_symbols.discard(alias)
        queue = self._symbol_queues.pop(alias, None)
        if queue is not None:
            queue.put(None)
        task = self._symbol_tasks.pop(alias, None)
        if task is not None:
            task.cancel()
        try:
            self._ib_client.unsubscribe_l1(alias)
        except Exception:  # pragma: no cover - external
            self._logger.exception("aggregator.unsubscribe_failed", symbol=alias)
        self._buffers.pop(alias, None)
        self._quotes.pop(alias, None)
        self._last_trade.pop(alias, None)
        self._last_persisted.pop(alias, None)
        self._baseline_cache.pop(alias, None)
        self._option_metrics.pop(alias, None)
        self._volume_snapshots.pop(alias, None)
        meta = self._symbol_meta.pop(alias, None)
        if meta and meta.kind == "OPTION":
            option_list = self._underlying_options.get(meta.underlying, [])
            if alias in option_list:
                option_list.remove(alias)
        elif meta and meta.kind == "EQUITY":
            self._last_equity_close.pop(meta.underlying, None)
            option_list = list(self._underlying_options.pop(meta.underlying, []))
            for opt_alias in option_list:
                if opt_alias in self._active_symbols:
                    self._deactivate_symbol(opt_alias)

    def _teardown_subscriptions(self) -> None:
        for symbol in list(self._active_symbols):
            self._deactivate_symbol(symbol)

    # ------------------------------------------------------------------
    def _cache_vix_snapshot(self, price: Decimal) -> None:
        if self._redis is None:
            return
        payload = {"value": str(price), "ts": datetime.now(timezone.utc).isoformat()}
        try:
            self._redis.setex("risk:vix:last", 180, json.dumps(payload))
        except Exception:  # pragma: no cover - external dependency
            self._logger.warning("aggregator.vix_snapshot_store_failed")

    def _start_vix_subscription(self) -> None:
        if self._loop is None:
            raise RuntimeError("Event loop not initialised for VIX subscription")
        req_id, queue = self._ib_client.subscribe_l1(
            self._vix_symbol,
            sec_type="IND",
            exchange="CBOE",
            currency="USD",
            generic_ticks="",
        )
        self._logger.info("aggregator.vix_subscribed", req_id=req_id)
        self._vix_queue = queue
        self._vix_task = self._loop.run_in_executor(None, self._consume_vix_ticks, queue)

    def _consume_vix_ticks(self, queue: Queue) -> None:
        last_trade: Optional[Decimal] = None
        bid: Optional[Decimal] = None
        ask: Optional[Decimal] = None
        while True:
            if not self._running:
                break
            try:
                payload = queue.get(timeout=1)
            except Empty:
                continue
            if payload is None:
                break
            event_type = payload.get("type")
            if event_type == "price":
                price = Decimal(str(payload.get("price")))
                field = payload.get("field")
                if field == 4:
                    last_trade = price
                elif field == 1:
                    bid = price
                elif field == 2:
                    ask = price
            elif event_type == "rt_volume":
                price = Decimal(str(payload.get("price", "0")))
                last_trade = price

            if last_trade is not None:
                self._vix_last_price = last_trade
            elif bid is not None and ask is not None:
                self._vix_last_price = (bid + ask) / Decimal(2)
            if self._vix_last_price is not None:
                self._cache_vix_snapshot(self._vix_last_price)

    def _stop_vix_subscription(self) -> None:
        queue = self._vix_queue
        if queue is not None:
            queue.put(None)
        task = self._vix_task
        if task is not None:
            task.cancel()
        try:
            self._ib_client.unsubscribe_l1(self._vix_symbol)
        except Exception:  # pragma: no cover - external dependency
            self._logger.exception("aggregator.vix_unsubscribe_failed")
        self._vix_queue = None
        self._vix_task = None


async def _top5_schedule_loop(top5_service: Top5Service) -> None:
    logger = structlog.get_logger(__name__).bind(component="top5_scheduler")
    last_run: date | None = None
    try:
        while True:
            now_et = utc_now().astimezone(EASTERN)
            if now_et.weekday() >= 5:
                await asyncio.sleep(1800)
                continue
            target = datetime.combine(now_et.date(), time(9, 30, 1), tzinfo=EASTERN)
            if now_et >= target:
                if last_run != now_et.date():
                    logger.info("top5_scheduler.run_start", trade_date=str(now_et.date()))
                    try:
                        await asyncio.to_thread(top5_service.run_for_today, now_et.date())
                        last_run = now_et.date()
                        logger.info("top5_scheduler.run_success", trade_date=str(now_et.date()))
                    except Exception:
                        logger.exception("top5_scheduler.run_failed", trade_date=str(now_et.date()))
                await asyncio.sleep(300)
            else:
                wait_seconds = max((target - now_et).total_seconds(), 5.0)
                await asyncio.sleep(min(wait_seconds, 60.0))
    except asyncio.CancelledError:  # pragma: no cover - cooperative shutdown
        logger.info("top5_scheduler.stopped")
        raise


async def _run_services(aggregator: MinuteAggregator, top5_service: Top5Service) -> None:
    scheduler_task = asyncio.create_task(_top5_schedule_loop(top5_service))
    try:
        await aggregator.run()
    finally:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Real-time minute aggregator")
    parser.add_argument(
        "--extra-symbols",
        nargs="+",
        default=[],
        help="Additional symbols to always subscribe regardless of watchlist",
    )
    parser.add_argument(
        "--watchlist-refresh",
        type=int,
        default=30,
        help="Watchlist refresh cadence in seconds (default: 30)",
    )
    parser.add_argument("--database-url", default=None, help="Override database URL")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings)

    database_url = args.database_url or settings.database_url
    engine = create_engine(database_url, future=True)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, future=True)

    ib_client = build_ibkr_client(settings)
    aggregator = MinuteAggregator(
        ib_client,
        session_factory,
        settings,
        watchlist_refresh_seconds=args.watchlist_refresh,
        static_symbols=args.extra_symbols,
        vix_required=settings.vix_required,
    )
    top5_service = Top5Service(ib_client, session_factory, settings)

    try:
        asyncio.run(_run_services(aggregator, top5_service))
    except KeyboardInterrupt:  # pragma: no cover
        pass
    finally:
        ib_client.disconnect_and_stop()

    return 0


if __name__ == "__main__":
    main()


def _to_decimal(value: Optional[Decimal]) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
