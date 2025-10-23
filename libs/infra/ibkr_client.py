from __future__ import annotations

import asyncio
import itertools
import logging
import math
import random
import threading
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from queue import Queue
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, TypeVar

from ibapi.client import EClient
from ibapi.common import BarData, HistoricalTickBidAsk
from ibapi.contract import Contract, ContractDetails
from ibapi.scanner import ScannerSubscription
from ibapi.wrapper import EWrapper
from ibapi.ticktype import TickTypeEnum

from libs.core.config import Settings
from libs.core import EASTERN, trading_session_window, utc_now

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class HistoricalRequest:
    bars: list[Mapping[str, Any]] = field(default_factory=list)
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[str] = None


@dataclass
class ContractRequest:
    details: list[ContractDetails] = field(default_factory=list)
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[str] = None


@dataclass
class GenericRequest:
    payload: list[Any] = field(default_factory=list)
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[str] = None


@dataclass
class HistoricalTickRequest:
    ticks: list[HistoricalTickBidAsk] = field(default_factory=list)
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[str] = None

@dataclass
class MarketDataSnapshot:
    data: dict[str, Any] = field(default_factory=dict)
    done: threading.Event = field(default_factory=threading.Event)
    error: Optional[str] = None


@dataclass(frozen=True)
class OptionContractQuote:
    contract: Contract
    conid: int
    expiry: datetime
    strike: Decimal
    right: str
    bid: Decimal
    ask: Decimal
    mid: Decimal
    open_interest: int
    volume: int
    min_tick: Decimal
    underlying_price: Decimal
    dte: int

    @property
    def spread(self) -> Decimal:
        return self.ask - self.bid


class TokenBucket:
    """Simple token bucket for rate limiting."""

    def __init__(self, capacity: int, refill_seconds: int) -> None:
        self._capacity = max(1, capacity)
        self._tokens = float(self._capacity)
        self._refill_seconds = max(1, refill_seconds)
        self._last_refill = _time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = _time.monotonic()
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return
        refill_ratio = min(1.0, elapsed / self._refill_seconds)
        if refill_ratio > 0:
            self._tokens = min(self._capacity, self._tokens + self._capacity * refill_ratio)
            self._last_refill = now

    def consume(self, amount: int = 1, timeout: float = 10.0) -> None:
        deadline = _time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= amount:
                    self._tokens -= amount
                    return
            if _time.monotonic() >= deadline:
                raise TimeoutError("Rate limit exceeded while waiting for IBKR tokens")
            _time.sleep(0.05)


class IBClient(EWrapper, EClient):
    """Interactive Brokers client with basic throttling, caching, and self-check logic."""

    def __init__(self, settings: Settings) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self._settings = settings

        self._thread: Optional[threading.Thread] = None
        self._loop_event = threading.Event()
        self._connected = threading.Event()
        self._next_valid_id_ready = threading.Event()
        self._current_time_ready = threading.Event()
        self._current_time: Optional[int] = None
        self._server_version: Optional[int] = None
        self._account: Optional[str] = settings.ib_account

        self._order_id_lock = threading.Lock()
        self._next_order_id: Optional[int] = None
        self._req_id_counter = itertools.count(1_000_000)

        self._market_bucket = TokenBucket(
            settings.ib_market_bucket, settings.ib_market_refill_seconds
        )
        self._historical_bucket = TokenBucket(
            settings.ib_historical_bucket, settings.ib_historical_refill_seconds
        )
        self._contract_bucket = TokenBucket(
            settings.ib_contract_bucket, settings.ib_contract_refill_seconds
        )
        self._scanner_bucket = TokenBucket(
            settings.ib_scanner_bucket, settings.ib_scanner_refill_seconds
        )

        self._req_symbol: Dict[int, str] = {}
        self._symbol_req: Dict[str, int] = {}
        self._symbol_queues: Dict[str, Queue] = {}
        self._subscriptions: Dict[str, Dict[str, Any]] = {}
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._event_queue: Optional["asyncio.Queue[Dict[str, Any]]"] = None
        self._order_refs: Dict[int, str] = {}
        self._order_contracts: Dict[int, Contract] = {}

        self._historical_requests: Dict[int, HistoricalRequest] = {}
        self._contract_requests: Dict[int, ContractRequest] = {}
        self._scanner_requests: Dict[int, GenericRequest] = {}
        self._option_requests: Dict[int, GenericRequest] = {}
        self._market_rule_requests: Dict[int, GenericRequest] = {}
        self._snapshot_requests: Dict[int, MarketDataSnapshot] = {}
        self._historical_tick_requests: Dict[int, HistoricalTickRequest] = {}

        self._contract_cache: Dict[tuple[str, str, str, str], ContractDetails] = {}
        self._market_rule_cache: Dict[int, list[Any]] = {}

        self._reconnect_lock = threading.Lock()
        self._should_reconnect = True
        self._shutting_down = False

    # ---------------------------------------------------------------------
    # Internal retry helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def _is_pacing_exception(exc: BaseException) -> bool:
        message = str(exc).lower()
        if "pacing" in message or "max rate of messages" in message:
            return True
        if "too many requests" in message or "max number of market data" in message:
            return True
        for code in ("162", "200", "420", "421", "10147", "10148", "10149", "366"):
            if code in message:
                return True
        return False

    def _with_pacing_retry(
        self,
        label: str,
        func: Callable[[], T],
        *,
        max_attempts: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
    ) -> T:
        delay = base_delay
        attempt = 1
        while True:
            try:
                return func()
            except (RuntimeError, TimeoutError) as exc:
                if not self._is_pacing_exception(exc) or attempt >= max_attempts:
                    raise
                jitter = random.uniform(0.0, delay * 0.3)
                wait_seconds = min(max_delay, delay + jitter)
                LOGGER.warning(
                    "%s pacing backoff attempt=%s wait=%.2fs message=%s",
                    label,
                    attempt,
                    wait_seconds,
                    str(exc),
                )
                _time.sleep(wait_seconds)
                attempt += 1
                delay = min(delay * 2, max_delay)

    @staticmethod
    def _duration_to_seconds(duration: str) -> int:
        token = (duration or "1 D").strip()
        if " " in token:
            amount_str, unit_str = token.split(None, 1)
        else:
            amount_str = "".join(ch for ch in token if ch.isdigit()) or "1"
            unit_str = "".join(ch for ch in token if ch.isalpha()) or "D"
        try:
            amount = int(amount_str)
        except ValueError:
            amount = 1
        unit = unit_str.strip().upper()
        if unit.startswith("S"):
            multiplier = 1
        elif unit.startswith("M"):
            multiplier = 60
        elif unit.startswith("H"):
            multiplier = 3600
        elif unit.startswith("W"):
            multiplier = 604800
        else:
            multiplier = 86400
        return max(1, amount * multiplier)

    # ---------------------------------------------------------------------
    # Connection management
    # ---------------------------------------------------------------------
    def connect_and_wait(self, timeout: float = 30.0) -> None:
        if self.isConnected():
            return

        LOGGER.info(
            "Connecting to IBKR host=%s port=%s clientId=%s",
            self._settings.ib_host,
            self._settings.ib_port,
            self._settings.ib_client_id,
        )
        super().connect(
            self._settings.ib_host, self._settings.ib_port, clientId=self._settings.ib_client_id
        )
        self._start_reader()

        if not self._next_valid_id_ready.wait(timeout):
            raise TimeoutError("Timed out waiting for nextValidId from IBKR")

        self.reqCurrentTime()
        if not self._current_time_ready.wait(timeout):
            raise TimeoutError("Timed out waiting for currentTime from IBKR")
        LOGGER.info(
            "IBKR connection established nextValidId=%s currentTime=%s",
            self._next_order_id,
            self._current_time,
        )
        self._connected.set()

    def _start_reader(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._loop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="ibkr-reader", daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        try:
            self.run()
        except Exception:  # pragma: no cover - defensive
            LOGGER.exception("IBKR reader loop terminated unexpectedly")
        finally:
            self._connected.clear()
            self._loop_event.set()
            if not self._shutting_down and self._should_reconnect:
                LOGGER.warning("IBKR connection dropped; attempting to reconnect")
                self._attempt_reconnect()

    def _attempt_reconnect(self) -> None:
        with self._reconnect_lock:
            if self.isConnected() or self._shutting_down:
                return
            backoff = 2
            while not self._shutting_down:
                try:
                    super().connect(
                        self._settings.ib_host,
                        self._settings.ib_port,
                        clientId=self._settings.ib_client_id,
                    )
                    self._start_reader()
                    if self._next_valid_id_ready.wait(timeout=10):
                        LOGGER.info("IBKR reconnected successfully")
                        self._resubscribe_all()
                        return
                except Exception:  # pragma: no cover - defensive
                    LOGGER.exception("Error during IBKR reconnect attempt")
                LOGGER.warning("IBKR reconnect failed, retrying in %ss", backoff)
                _time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def disconnect_and_stop(self) -> None:
        self._shutting_down = True
        self._should_reconnect = False
        if self.isConnected():
            super().disconnect()
        if self._thread and self._thread.is_alive():
            self._loop_event.wait(timeout=2)

    # ---------------------------------------------------------------------
    # Contract helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def stock_contract(
        symbol: str,
        exchange: str = "SMART",
        currency: str = "USD",
        primary_exchange: str = "",
    ) -> Contract:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = currency
        if primary_exchange:
            contract.primaryExchange = primary_exchange
        return contract

    @staticmethod
    def index_contract(symbol: str, exchange: str = "CBOE", currency: str = "USD") -> Contract:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "IND"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def option_contract(
        symbol: str,
        *,
        expiry: str,
        strike: float,
        right: str,
        exchange: str = "SMART",
        currency: str = "USD",
        multiplier: str = "100",
        conid: int | None = None,
        include_expired: bool = False,
    ) -> Contract:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = float(strike)
        contract.right = right.upper()
        contract.multiplier = multiplier
        contract.includeExpired = include_expired
        if conid is not None:
            contract.conId = int(conid)
        return contract

    def _cache_key(self, contract: Contract) -> tuple[str, str, str, str]:
        return (
            contract.symbol,
            contract.secType,
            contract.exchange or "SMART",
            contract.currency or "USD",
        )

    def _snapshot_mid_price(self, snapshot: Mapping[str, Any]) -> Optional[Decimal]:
        bid = self._to_decimal(snapshot.get("bid"))
        ask = self._to_decimal(snapshot.get("ask"))
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            return (bid + ask) / Decimal("2")
        mark = self._to_decimal(snapshot.get("mark"))
        if mark is not None and mark > 0:
            return mark
        last = self._to_decimal(snapshot.get("last"))
        if last is not None and last > 0:
            return last
        close = self._to_decimal(snapshot.get("close"))
        if close is not None and close > 0:
            return close
        return None

    # ------------------------------------------------------------------
    # Option chain helpers
    # ------------------------------------------------------------------
    def best_option_contract(
        self,
        symbol: str,
        *,
        right: str,
        reference: Optional[datetime] = None,
        dte_range: tuple[int, int] = (2, 7),
        otm_steps: tuple[int, int] = (2, 5),
        min_open_interest: int = 500,
        min_volume: int = 100,
        max_spread_pct: Decimal = Decimal("0.05"),
        max_abs_spread: Decimal = Decimal("0.10"),
        max_candidates: int = 10,
    ) -> Optional[OptionContractQuote]:
        right_upper = right.upper()
        if right_upper not in {"CALL", "PUT"}:
            raise ValueError("right must be 'CALL' or 'PUT'")

        min_dte, max_dte = dte_range
        if min_dte <= 0 or max_dte < min_dte:
            raise ValueError("Invalid dte_range")
        min_step, max_step = otm_steps
        if min_step <= 0 or max_step < min_step:
            raise ValueError("Invalid otm_steps range")

        reference_ts = reference or utc_now()
        trade_date = reference_ts.astimezone(EASTERN).date()

        underlying_contract = self.stock_contract(symbol)
        underlying_details = self.req_contract_details(underlying_contract)
        underlying_resolved = underlying_details.contract
        if not underlying_resolved.conId:
            raise RuntimeError(f"Underlying contract for {symbol} missing conId")

        underlying_snapshot = self.market_data_snapshot(underlying_resolved, generic_ticks="233")
        underlying_mid = self._snapshot_mid_price(underlying_snapshot)
        if underlying_mid is None:
            raise RuntimeError(f"Unable to derive underlying price for {symbol}")

        opt_params = self.req_opt_params(
            symbol,
            underlying_conid=int(underlying_resolved.conId),
            exchange="SMART",
        )
        if not opt_params:
            return None

        params_entry = next((entry for entry in opt_params if entry.get("exchange") == "SMART"), opt_params[0])
        expirations = sorted(params_entry.get("expirations") or [])
        strike_vals = params_entry.get("strikes") or []
        strikes = sorted({Decimal(str(strike)) for strike in strike_vals if strike is not None})
        if not expirations or not strikes:
            return None

        candidates: list[OptionContractQuote] = []
        seen_keys: set[tuple[str, Decimal]] = set()

        for expiry_code in expirations:
            try:
                expiry_date = datetime.strptime(expiry_code, "%Y%m%d").date()
            except ValueError:
                continue
            dte = (expiry_date - trade_date).days
            if dte < min_dte or dte > max_dte:
                continue

            if right_upper == "CALL":
                otm_pool = [strike for strike in strikes if strike >= underlying_mid]
                otm_pool.sort()
            else:
                otm_pool = [strike for strike in strikes if strike <= underlying_mid]
                otm_pool.sort(reverse=True)

            for step in range(min_step, max_step + 1):
                idx = step - 1
                if idx < 0 or idx >= len(otm_pool):
                    continue
                strike_value = otm_pool[idx]
                key = (expiry_code, strike_value)
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                quote = self._build_option_quote(
                    symbol=symbol,
                    right=right_upper,
                    strike=strike_value,
                    expiry_date=expiry_date,
                    underlying_price=underlying_mid,
                    dte=dte,
                    min_open_interest=min_open_interest,
                    min_volume=min_volume,
                    max_spread_pct=max_spread_pct,
                    max_abs_spread=max_abs_spread,
                )
                if quote is not None:
                    candidates.append(quote)
                if len(candidates) >= max_candidates:
                    break
            if len(candidates) >= max_candidates:
                break

        if not candidates:
            return None

        candidates.sort(
            key=lambda q: (
                q.spread,
                -q.open_interest,
                -q.volume,
                q.mid,
            )
        )
        return candidates[0]

    def _build_option_quote(
        self,
        *,
        symbol: str,
        right: str,
        strike: Decimal,
        expiry_date: date,
        underlying_price: Decimal,
        dte: int,
        min_open_interest: int,
        min_volume: int,
        max_spread_pct: Decimal,
        max_abs_spread: Decimal,
    ) -> Optional[OptionContractQuote]:
        expiry_str = expiry_date.strftime("%Y%m%d")
        contract = self.option_contract(
            symbol,
            expiry=expiry_str,
            strike=float(strike),
            right=right,
            exchange="SMART",
        )

        try:
            detail = self.req_contract_details(contract)
        except Exception as exc:
            LOGGER.warning("option_contract_details_failed", symbol=symbol, strike=strike, expiry=expiry_str, error=str(exc))
            return None

        resolved = detail.contract
        conid = resolved.conId or detail.contract.conId
        if not conid:
            return None
        resolved.conId = conid

        min_tick = detail.minTick if detail.minTick else None
        min_tick_dec = self._to_decimal(min_tick) or Decimal("0.01")

        try:
            snapshot = self.market_data_snapshot(
                resolved,
                generic_ticks="100,101,104,106,233",
                timeout=5.0,
            )
        except Exception as exc:
            LOGGER.warning(
                "option_snapshot_failed",
                symbol=symbol,
                strike=strike,
                expiry=expiry_str,
                right=right,
                error=str(exc),
            )
            return None

        bid = self._to_decimal(snapshot.get("bid"))
        ask = self._to_decimal(snapshot.get("ask"))
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask <= bid:
            return None
        mid = (bid + ask) / Decimal("2")
        spread = ask - bid
        threshold = max(max_abs_spread, mid * max_spread_pct)
        if spread > threshold:
            return None

        oi = self._to_int(snapshot.get("open_interest")) or 0
        vol = self._to_int(snapshot.get("option_volume") or snapshot.get("volume")) or 0
        if oi < min_open_interest or vol < min_volume:
            return None

        expiry_dt = datetime.combine(expiry_date, time(0, 0), tzinfo=EASTERN)

        return OptionContractQuote(
            contract=resolved,
            conid=int(conid),
            expiry=expiry_dt,
            strike=strike,
            right=right,
            bid=bid,
            ask=ask,
            mid=mid,
            open_interest=oi,
            volume=vol,
            min_tick=min_tick_dec,
            underlying_price=underlying_price,
            dte=dte,
        )


    def req_contract_details(self, contract: Contract, timeout: float = 30.0) -> ContractDetails:
        key = self._cache_key(contract)
        cached = self._contract_cache.get(key)
        if cached:
            return cached

        req_id = next(self._req_id_counter)
        future = ContractRequest()
        self._contract_requests[req_id] = future
        LOGGER.debug(
            "Requesting contract details reqId=%s contract=%s/%s",
            req_id,
            contract.secType,
            contract.symbol,
        )
        self.reqContractDetails(req_id, contract)
        if not future.done.wait(timeout):
            self._contract_requests.pop(req_id, None)
            raise TimeoutError(f"Timed out waiting for contract details for {contract.symbol}")
        if future.error:
            raise RuntimeError(f"Contract details error: {future.error}")
        if not future.details:
            raise RuntimeError(f"No contract details returned for {contract.symbol}")
        detail = future.details[0]
        self._contract_cache[key] = detail
        return detail

    @staticmethod
    def _to_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(Decimal(str(value)))
        except (InvalidOperation, ValueError, TypeError):
            return None

    # ---------------------------------------------------------------------
    # Market data subscriptions
    # ---------------------------------------------------------------------
    def subscribe_l1(
        self,
        symbol: str,
        *,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        generic_ticks: str = "233",
        alias: str | None = None,
        contract: Contract | None = None,
    ) -> tuple[int, Queue]:
        self._market_bucket.consume()
        if contract is None:
            if sec_type == "STK":
                contract = self.stock_contract(symbol, exchange, currency)
            elif sec_type == "IND":
                contract = self.index_contract(symbol, exchange, currency)
            else:
                raise ValueError("contract must be provided for non-stock subscriptions")
        detail = self.req_contract_details(contract)
        resolved = detail.contract
        if contract.conId and not resolved.conId:
            resolved.conId = contract.conId
        key = alias or self._subscription_key(resolved)

        req_id = next(self._req_id_counter)
        queue = self._symbol_queues.get(key)
        if queue is None:
            queue = Queue()
            self._symbol_queues[key] = queue
        self._req_symbol[req_id] = key
        self._symbol_req[key] = req_id
        self._subscriptions[key] = {
            "symbol": symbol,
            "sec_type": sec_type,
            "exchange": exchange,
            "currency": currency,
            "generic_ticks": generic_ticks,
            "contract": self._snapshot_contract(resolved),
        }
        LOGGER.info(
            "Subscribing L1 reqId=%s alias=%s secType=%s generic=%s",
            req_id,
            key,
            resolved.secType,
            generic_ticks,
        )
        self.reqMktData(req_id, resolved, generic_ticks, False, False, [])
        if detail.marketRuleIds:
            try:
                market_rule_id = int(detail.marketRuleIds.split(",")[0])
                self.req_market_rule(market_rule_id)
            except ValueError:
                pass
        return req_id, queue

    def unsubscribe_l1(self, alias: str) -> None:
        req_id = self._symbol_req.pop(alias, None)
        if req_id is None:
            return
        LOGGER.info("Unsubscribing L1 alias=%s reqId=%s", alias, req_id)
        self.cancelMktData(req_id)
        self._req_symbol.pop(req_id, None)
        self._symbol_queues.pop(alias, None)
        self._subscriptions.pop(alias, None)

    def market_data_snapshot(
        self,
        contract: Contract,
        *,
        generic_ticks: str = "",
        timeout: float = 5.0,
    ) -> dict[str, Any]:
        """Request a blocking market data snapshot for the provided contract."""
        self._market_bucket.consume()
        req_id = next(self._req_id_counter)
        snapshot = MarketDataSnapshot()
        self._snapshot_requests[req_id] = snapshot

        LOGGER.debug(
            "Requesting market data snapshot reqId=%s symbol=%s secType=%s generic=%s",
            req_id,
            contract.symbol,
            contract.secType,
            generic_ticks,
        )
        self.reqMktData(req_id, contract, generic_ticks, True, False, [])

        try:
            if not snapshot.done.wait(timeout):
                raise TimeoutError(
                    f"Timed out waiting for market data snapshot for {contract.symbol}"
                )
            if snapshot.error:
                raise RuntimeError(snapshot.error)
            return snapshot.data
        finally:
            self.cancelMktData(req_id)
            self._snapshot_requests.pop(req_id, None)

    # ---------------------------------------------------------------------
    # Historical data
    # ---------------------------------------------------------------------
    @staticmethod
    def _coerce_historical_timestamp(raw: Any) -> datetime:
        if isinstance(raw, datetime):
            dt = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(int(raw), tz=timezone.utc)
        if raw is None:
            raise ValueError("Missing timestamp in historical bar payload")
        raw_str = str(raw).strip()
        if raw_str.isdigit():
            return datetime.fromtimestamp(int(raw_str), tz=timezone.utc)
        for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d %H:%M:%S %Z"):
            try:
                dt = datetime.strptime(raw_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except ValueError:
                continue
        raise ValueError(f"Unsupported historicalData timestamp: {raw!r}")

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("nan")

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            if value is None:
                return 0
            return int(value)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _normalize_historical_bar(cls, bar: Mapping[str, Any]) -> Dict[str, Any]:
        ts_utc = cls._coerce_historical_timestamp(bar.get("time"))
        normalized: Dict[str, Any] = {
            "ts_end": ts_utc.isoformat(),
            "open": cls._safe_float(bar.get("open")),
            "high": cls._safe_float(bar.get("high")),
            "low": cls._safe_float(bar.get("low")),
            "close": cls._safe_float(bar.get("close")),
            "volume": cls._safe_int(bar.get("volume")),
        }
        if "time" in bar:
            normalized["time"] = bar["time"]
        return normalized

    def req_historical_1m_contract(
        self,
        contract: Contract,
        start: datetime,
        end: datetime,
        use_rth: bool,
        *,
        what_to_show: str = "TRADES",
    ) -> list[Mapping[str, Any]]:
        self._historical_bucket.consume()
        detail = self.req_contract_details(contract)
        resolved = detail.contract
        if contract.conId and not resolved.conId:
            resolved.conId = contract.conId

        duration_minutes = max(1, int((end - start).total_seconds() // 60))
        # IBKR: M=月, S=秒, D=天。对于分钟级数据，用秒避免歧义
        duration_seconds = duration_minutes * 60
        duration_str = f"{duration_seconds} S"
        # IBKR要求时区格式为 xx/xxxx（如US/Eastern），不接受America/New_York
        # 将end时间转换为Eastern时区，然后格式化
        end_et = end.astimezone(EASTERN)
        end_str = end_et.strftime("%Y%m%d %H:%M:%S US/Eastern")

        req_id = next(self._req_id_counter)
        future = HistoricalRequest()
        self._historical_requests[req_id] = future

        LOGGER.info(
            "Requesting historical data reqId=%s symbol=%s secType=%s what=%s duration=%s end=%s useRTH=%s",
            req_id,
            resolved.symbol,
            resolved.secType,
            what_to_show,
            duration_str,
            end_str,
            use_rth,
        )

        self.reqHistoricalData(
            req_id,
            resolved,
            end_str,
            duration_str,
            "1 min",
            what_to_show,
            1 if use_rth else 0,
            2,  # formatDate=2: 返回Unix时间戳，避免时区解析问题
            False,
            [],
        )

        # 超时时间：最少60秒（IB官方建议），对于长duration可适当增加
        timeout_seconds = max(60.0, duration_minutes * 2)
        if not future.done.wait(timeout=timeout_seconds):
            self._historical_requests.pop(req_id, None)
            raise TimeoutError(f"Timed out waiting for historical data for {resolved.symbol} after {timeout_seconds}s")
        if future.error:
            # 检测HMDS维护窗口错误码：2105(断连)/2106(重连)/2107(重置)/162(数据农场连接)
            error_str = str(future.error)
            if any(code in error_str for code in ["2105", "2106", "2107", "162"]):
                LOGGER.warning(
                    "historical.hmds_maintenance",
                    symbol=resolved.symbol,
                    error=error_str,
                    hint="可能处于HMDS维护窗口",
                )
            raise RuntimeError(f"Historical data error: {future.error}")
        normalized: list[Dict[str, Any]] = []
        for bar in future.bars:
            try:
                normalized.append(self._normalize_historical_bar(bar))
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("historical_data.normalize_failed", bar=bar)
        return normalized

    def req_historical_1m(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        use_rth: bool,
        *,
        what_to_show: str = "TRADES",
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> list[Mapping[str, Any]]:
        contract = self.stock_contract(symbol, exchange, currency)
        return self.req_historical_1m_contract(
            contract,
            start,
            end,
            use_rth,
            what_to_show=what_to_show,
        )

    def req_historical_1m_equity(
        self,
        symbol: str,
        session_date: date,
        *,
        rth_only: bool = True,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> list[Mapping[str, Any]]:
        """
        Convenience wrapper that fetches a full US RTH session (09:30-16:00 ET) in 1 minute bars.
        """
        start_et, end_et = trading_session_window(session_date, tz=EASTERN)
        # include the last minute by shifting end forward 1 minute
        start_utc = start_et.astimezone(timezone.utc)
        end_utc = (end_et + timedelta(minutes=1)).astimezone(timezone.utc)
        return self.req_historical_1m(
            symbol=symbol,
            start=start_utc,
            end=end_utc,
            use_rth=rth_only,
            exchange=exchange,
            currency=currency,
        )

    def req_historical_daily_vix(
        self,
        trade_date: date,
    ) -> Optional[Decimal]:
        """
        获取VIX指数在指定交易日的收盘价
        
        由于IBKR不支持直接指定单个交易日，我们请求包含该日期的一段时间（10天），
        然后从返回的数据中找到对应日期的收盘价。
        
        Args:
            trade_date: 交易日期
            
        Returns:
            VIX收盘价，如果获取失败则返回None
        """
        contract = self.index_contract("VIX", "CBOE", "USD")
        
        self._historical_bucket.consume()
        detail = self.req_contract_details(contract)
        resolved = detail.contract
        
        # 计算从trade_date到今天的天数
        today = date.today()
        days_ago = (today - trade_date).days
        
        # 如果trade_date是未来日期，使用空字符串（当前时间）并拉取最近的数据
        if days_ago < 0:
            end_str = ""
            duration_str = "1 M"  # 拉取最近一个月
        else:
            # 使用空字符串表示当前时间，然后通过duration回溯到目标日期
            end_str = ""
            # 增加一些buffer确保包含目标日期
            duration_str = f"{days_ago + 5} D"
        
        req_id = next(self._req_id_counter)
        future = HistoricalRequest()
        self._historical_requests[req_id] = future
        
        LOGGER.info("Requesting VIX daily data reqId=%s target_date=%s duration=%s", 
                   req_id, trade_date.isoformat(), duration_str)
        
        self.reqHistoricalData(
            req_id,
            resolved,
            end_str,  # 空字符串表示当前时间
            duration_str,  # 回溯到目标日期
            "1 day",  # 日级别K线
            "TRADES",
            1,  # use_rth=True
            2,  # formatDate=2: Unix timestamp
            False,
            [],
        )
        
        if not future.done.wait(timeout=30):
            self._historical_requests.pop(req_id, None)
            LOGGER.warning("VIX daily data request timeout for date=%s", trade_date.isoformat())
            return None
        
        if future.error:
            LOGGER.error("VIX daily data error: %s", future.error)
            return None
        
        if not future.bars or len(future.bars) == 0:
            LOGGER.warning("No VIX daily data returned for date=%s", trade_date.isoformat())
            return None
        
        # 从返回的bars中找到对应日期的数据
        # future.bars中的bar是字典，键为"time"
        # 日线数据的"time"是字符串格式（yyyymmdd），而不是Unix timestamp
        for bar in future.bars:
            bar_time = bar.get("time")
            bar_close = bar.get("close")
            
            if bar_time is None or bar_close is None:
                continue
                
            # 日线数据的time是字符串（yyyymmdd），需要解析
            try:
                if isinstance(bar_time, str):
                    # 格式：yyyymmdd
                    bar_date = datetime.strptime(bar_time, "%Y%m%d").date()
                else:
                    # Unix timestamp（分钟数据会是这种格式）
                    bar_date = datetime.fromtimestamp(bar_time, tz=EASTERN).date()
                    
                if bar_date == trade_date:
                    vix_close = Decimal(str(bar_close))
                    LOGGER.info("VIX close price for %s: %s (from %d bars)", 
                               trade_date.isoformat(), vix_close, len(future.bars))
                    return vix_close
            except (ValueError, TypeError, OSError) as e:
                LOGGER.warning("Failed to parse bar time: %s, error: %s", bar_time, e)
                continue
        
        LOGGER.warning("VIX data for date=%s not found in returned %d bars", 
                      trade_date.isoformat(), len(future.bars))
        return None

    def req_option_bid_ask_1m(
        self,
        contract: Contract,
        *,
        start: datetime,
        end: datetime,
        use_rth: bool = True,
    ) -> list[Dict[str, Any]]:
        detail = self.req_contract_details(contract)
        resolved = detail.contract
        if contract.conId and not resolved.conId:
            resolved.conId = contract.conId

        ticks = self._collect_bid_ask_ticks(resolved, start=start, end=end, use_rth=use_rth)
        if not ticks:
            return []
        return self._aggregate_bid_ask_ticks(ticks)

    # ---------------------------------------------------------------------
    # Scanner and other requests
    # ---------------------------------------------------------------------
    def scanner_premarket(
        self, subscription: Optional[ScannerSubscription] = None, timeout: float = 10.0
    ) -> list[Mapping[str, Any]]:
        self._scanner_bucket.consume()
        req_id = next(self._req_id_counter)
        future = GenericRequest()
        self._scanner_requests[req_id] = future
        if subscription is None:
            subscription = ScannerSubscription()
            subscription.instrument = "STK"
            subscription.scanCode = "TOP_PERC_GAIN"
            subscription.abovePrice = 1
            subscription.aboveVolume = 100000
        LOGGER.info(
            "Requesting scanner subscription reqId=%s code=%s", req_id, subscription.scanCode
        )
        self.reqScannerSubscription(req_id, subscription, [], [])
        if not future.done.wait(timeout):
            self.cancelScannerSubscription(req_id)
            self._scanner_requests.pop(req_id, None)
            raise TimeoutError("Timed out waiting for scanner response")
        if future.error:
            raise RuntimeError(f"Scanner error: {future.error}")
        return future.payload  # type: ignore[return-value]

    def req_opt_params(
        self,
        underlying_symbol: str,
        *,
        underlying_conid: int = 0,
        exchange: str = "",
        timeout: float = 10.0,
    ) -> list[Any]:
        """Request option chain parameters for an underlying symbol.
        
        Args:
            underlying_symbol: Symbol of the underlying (e.g. 'SPY')
            underlying_conid: Contract ID of the underlying, 0 to let IBKR find it
            timeout: Request timeout in seconds
        """
        req_id = next(self._req_id_counter)
        future = GenericRequest()
        self._option_requests[req_id] = future
        self.reqSecDefOptParams(req_id, underlying_symbol, exchange, "STK", underlying_conid)
        if not future.done.wait(timeout):
            self._option_requests.pop(req_id, None)
            raise TimeoutError("Timed out waiting for option parameters")
        if future.error:
            raise RuntimeError(f"Option parameter error: {future.error}")
        return future.payload

    def get_secdef_opt_params(
        self,
        underlying_symbol: str,
        *,
        underlying_conid: int = 0,
        exchange: str = "",  # 关键修复：使用空字符串而不是"SMART"
        timeout: float = 10.0,
        max_attempts: int = 5,
    ) -> list[Any]:
        """
        获取期权链参数（到期日、执行价、tradingClass等）
        
        注意：对于股票期权，futFopExchange建议使用空字符串""，而不是"SMART"。
        某些券商路由下，"SMART"会返回空结果。
        """
        symbol = (underlying_symbol or "").upper()

        def _attempt() -> list[Any]:
            self._contract_bucket.consume()
            req_id = next(self._req_id_counter)
            future = GenericRequest()
            self._option_requests[req_id] = future
            LOGGER.debug(
                "Requesting secdef params reqId=%s symbol=%s exchange=%s underlying=%s",
                req_id,
                symbol,
                exchange if exchange else "(empty)",
                underlying_conid,
            )
            self.reqSecDefOptParams(req_id, symbol, exchange, "STK", underlying_conid)
            if not future.done.wait(timeout):
                self._option_requests.pop(req_id, None)
                raise TimeoutError(f"Timed out waiting for option parameters for {symbol}")
            if future.error:
                self._option_requests.pop(req_id, None)
                raise RuntimeError(f"Option parameter error: {future.error}")
            return future.payload

        return self._with_pacing_retry(
            label=f"secdef:{symbol}",
            func=_attempt,
            max_attempts=max_attempts,
        )

    def req_market_rule(self, market_rule_id: int, timeout: float = 10.0) -> list[Any]:
        cached = self._market_rule_cache.get(market_rule_id)
        if cached is not None:
            return cached
        future = GenericRequest()
        self._market_rule_requests[market_rule_id] = future
        LOGGER.debug("Requesting market rule marketRuleId=%s", market_rule_id)
        self.reqMarketRule(market_rule_id)
        if not future.done.wait(timeout):
            self._market_rule_requests.pop(market_rule_id, None)
            raise TimeoutError(f"Timed out waiting for market rule {market_rule_id}")
        if future.error:
            raise RuntimeError(f"Market rule error: {future.error}")
        self._market_rule_cache[market_rule_id] = future.payload
        return future.payload

    # ---------------------------------------------------------------------
    # Self-check helper
    # ---------------------------------------------------------------------
    def run_selfcheck(self) -> None:
        LOGGER.info("Starting IBKR self-check")
        self.connect_and_wait()
        LOGGER.info("Server version: %s", self.serverVersion())
        LOGGER.info("Next order id: %s", self._next_order_id)

        _, vix_queue = self.subscribe_l1("VIX", sec_type="IND", exchange="CBOE", currency="USD")
        try:
            tick = vix_queue.get(timeout=10)
            LOGGER.info("Received VIX tick: %s", tick)
        finally:
            self.unsubscribe_l1("VIX")

        symbol = self._settings.ib_selfcheck_symbol
        _, queue = self.subscribe_l1(symbol, sec_type="STK")
        try:
            tick = queue.get(timeout=10)
            LOGGER.info("Received %s tick: %s", symbol, tick)
        finally:
            self.unsubscribe_l1(symbol)

        end = datetime.utcnow()
        start = end - timedelta(minutes=60)
        bars = self.req_historical_1m(symbol, start, end, use_rth=True)
        if not bars:
            raise RuntimeError("Historical data self-check returned no bars")
        last_bar = bars[-1]
        LOGGER.info(
            "Historical %s last bar end=%s close=%s", symbol, last_bar["time"], last_bar["close"]
        )
        LOGGER.info("IBKR self-check completed successfully")

    # ------------------------------------------------------------------
    # EWrapper overrides
    # ------------------------------------------------------------------
    def nextValidId(self, orderId: int) -> None:  # noqa: N802
        LOGGER.debug("nextValidId orderId=%s", orderId)
        with self._order_id_lock:
            self._next_order_id = orderId
            start = max(orderId + 1, 1_000_000)
            self._req_id_counter = itertools.count(start)
        self._next_valid_id_ready.set()

    def currentTime(self, time_: int) -> None:  # noqa: N802
        LOGGER.debug("currentTime=%s", time_)
        self._current_time = time_
        self._current_time_ready.set()

    def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
        LOGGER.debug("managedAccounts=%s", accountsList)
        if accountsList:
            self._account = accountsList.split(",")[0]

    def tickPrice(self, reqId: int, tickType: int, price: float, attribs) -> None:  # noqa: N802
        payload = {
            "type": "price",
            "field": tickType,
            "price": price,
            "attribs": {
                "can_auto_execute": getattr(attribs, "canAutoExecute", None),
                "past_limit": getattr(attribs, "pastLimit", None),
                "pre_open": getattr(attribs, "preOpen", None),
            },
        }
        if tickType == TickTypeEnum.BID:
            self._record_snapshot_value(reqId, "bid", price)
        elif tickType == TickTypeEnum.ASK:
            self._record_snapshot_value(reqId, "ask", price)
        elif tickType == TickTypeEnum.LAST:
            self._record_snapshot_value(reqId, "last", price)
        elif tickType == TickTypeEnum.MARK_PRICE:
            self._record_snapshot_value(reqId, "mark", price)
        elif tickType == TickTypeEnum.OPEN:
            self._record_snapshot_value(reqId, "open", price)
        elif tickType == TickTypeEnum.HIGH:
            self._record_snapshot_value(reqId, "high", price)
        elif tickType == TickTypeEnum.LOW:
            self._record_snapshot_value(reqId, "low", price)
        elif tickType == TickTypeEnum.CLOSE:
            self._record_snapshot_value(reqId, "close", price)
        self._push_tick(reqId, payload)

    def tickSize(self, reqId: int, tickType: int, size: int) -> None:  # noqa: N802
        if tickType == TickTypeEnum.BID_SIZE:
            self._record_snapshot_value(reqId, "bid_size", size)
        elif tickType == TickTypeEnum.ASK_SIZE:
            self._record_snapshot_value(reqId, "ask_size", size)
        elif tickType == TickTypeEnum.LAST_SIZE:
            self._record_snapshot_value(reqId, "last_size", size)
        elif tickType == TickTypeEnum.VOLUME:
            self._record_snapshot_value(reqId, "volume", size)
        elif tickType in {TickTypeEnum.OPTION_CALL_VOLUME, TickTypeEnum.OPTION_PUT_VOLUME}:
            self._record_snapshot_value(reqId, "option_volume", size)
        elif tickType in {
            TickTypeEnum.OPTION_CALL_OPEN_INTEREST,
            TickTypeEnum.OPTION_PUT_OPEN_INTEREST,
        }:
            self._record_snapshot_value(reqId, "open_interest", size)
        self._push_tick(reqId, {"type": "size", "field": tickType, "size": size})

    def tickGeneric(self, reqId: int, tickType: int, value: float) -> None:  # noqa: N802
        if tickType == TickTypeEnum.OPEN_INTEREST:
            self._record_snapshot_value(reqId, "open_interest", value)
        elif tickType == TickTypeEnum.OPTION_HISTORICAL_VOL:
            self._record_snapshot_value(reqId, "historical_vol", value)
        elif tickType == TickTypeEnum.OPTION_IMPLIED_VOL:
            self._record_snapshot_value(reqId, "implied_vol", value)
        elif tickType == TickTypeEnum.MARK_PRICE:
            self._record_snapshot_value(reqId, "mark", value)
        self._push_tick(reqId, {"type": "generic", "field": tickType, "value": value})

    def tickString(self, reqId: int, tickType: int, value: str) -> None:  # noqa: N802
        payload: Mapping[str, Any] = {"type": "string", "field": tickType, "value": value}
        if tickType == 233:
            parts = value.split(",")
            if len(parts) >= 6:
                try:
                    payload = {
                        "type": "rt_volume",
                        "price": float(parts[0]),
                        "size": float(parts[1]),
                        "last_time": int(parts[2]),
                        "total_volume": float(parts[3]),
                        "vwap": float(parts[4]),
                        "single_trade_volume": float(parts[5]),
                    }
                except ValueError:
                    payload = {"type": "rt_volume_raw", "value": value}
        self._push_tick(reqId, payload)

    def tickOptionComputation(  # noqa: N802
        self,
        reqId: int,
        tickType: int,
        tickAttrib,
        impliedVol: float,
        delta: float,
        optPrice: float,
        pvDividend: float,
        gamma: float,
        vega: float,
        theta: float,
        undPrice: float,
    ) -> None:
        payload: dict[str, Any] = {"type": "greeks"}
        if not math.isnan(impliedVol) and impliedVol > 0:
            self._record_snapshot_value(reqId, "implied_vol", impliedVol)
            payload["implied_vol"] = impliedVol
        if not math.isnan(delta):
            self._record_snapshot_value(reqId, "delta", delta)
            payload["delta"] = delta
        if not math.isnan(gamma):
            self._record_snapshot_value(reqId, "gamma", gamma)
            payload["gamma"] = gamma
        if not math.isnan(vega):
            self._record_snapshot_value(reqId, "vega", vega)
            payload["vega"] = vega
        if not math.isnan(theta):
            self._record_snapshot_value(reqId, "theta", theta)
            payload["theta"] = theta
        if not math.isnan(undPrice) and undPrice > 0:
            self._record_snapshot_value(reqId, "underlying_price", undPrice)
            payload["underlying_price"] = undPrice
        alias = self._req_symbol.get(reqId)
        if alias and len(payload) > 1:
            queue = self._symbol_queues.get(alias)
            if queue is not None:
                queue.put(payload)

    def tickSnapshotEnd(self, reqId: int) -> None:  # noqa: N802
        snapshot = self._snapshot_requests.get(reqId)
        if snapshot:
            snapshot.done.set()

    def historicalData(self, reqId: int, bar: BarData) -> None:  # noqa: N802
        LOGGER.info(f"🔔 historicalData callback reqId={reqId} bar.date={bar.date} close={bar.close}")
        future = self._historical_requests.get(reqId)
        if future is None:
            LOGGER.warning(f"historicalData: reqId={reqId} not found in _historical_requests")
            return
        future.bars.append(
            {
                "time": bar.date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
        )

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:  # noqa: N802
        LOGGER.info(f"🏁 historicalDataEnd callback reqId={reqId} start={start} end={end}")
        future = self._historical_requests.pop(reqId, None)
        if future:
            LOGGER.info(f"historicalDataEnd: setting done for reqId={reqId}, got {len(future.bars)} bars")
            future.done.set()
        else:
            LOGGER.warning(f"historicalDataEnd: reqId={reqId} not found in _historical_requests")

    def historicalTicksBidAsk(  # noqa: N802
        self, reqId: int, ticks: List[HistoricalTickBidAsk], done: bool
    ) -> None:
        future = self._historical_tick_requests.get(reqId)
        if future is None:
            LOGGER.warning("historicalTicksBidAsk: reqId=%s not found", reqId)
            return
        future.ticks.extend(ticks)
        if done:
            future.done.set()

    def historicalTicksBidAskEnd(self, reqId: int, start: str, end: str) -> None:  # noqa: N802
        future = self._historical_tick_requests.pop(reqId, None)
        if future:
            future.done.set()
        else:
            LOGGER.warning("historicalTicksBidAskEnd: reqId=%s not found", reqId)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails) -> None:  # noqa: N802
        LOGGER.debug(f"📋 contractDetails callback reqId={reqId} symbol={contractDetails.contract.symbol}")
        future = self._contract_requests.get(reqId)
        if future is None:
            LOGGER.warning(f"contractDetails: reqId={reqId} not found in _contract_requests")
            return
        future.details.append(contractDetails)

    def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        LOGGER.debug(f"✅ contractDetailsEnd callback reqId={reqId}")
        future = self._contract_requests.pop(reqId, None)
        if future:
            LOGGER.debug(f"contractDetailsEnd: setting done for reqId={reqId}, got {len(future.details)} details")
            future.done.set()
        else:
            LOGGER.warning(f"contractDetailsEnd: reqId={reqId} not found in _contract_requests")

    def scannerData(  # noqa: N802
        self,
        reqId: int,
        rank: int,
        contractDetails: ContractDetails,
        distance: str,
        benchmark: str,
        projection: str,
        legsStr: str,
    ) -> None:
        LOGGER.info(f"📈 scannerData: rank={rank} symbol={contractDetails.contract.symbol} distance={distance}")
        future = self._scanner_requests.get(reqId)
        if future is None:
            LOGGER.warning(f"scannerData: reqId={reqId} not found in _scanner_requests")
            return
        future.payload.append(
            {
                "rank": rank,
                "symbol": contractDetails.contract.symbol,
                "exchange": contractDetails.contract.exchange,
                "distance": distance,
                "benchmark": benchmark,
                "projection": projection,
                "legs": legsStr,
            }
        )

    def scannerDataEnd(self, reqId: int) -> None:  # noqa: N802
        LOGGER.info(f"✅ scannerDataEnd callback reqId={reqId}")
        future = self._scanner_requests.pop(reqId, None)
        if future:
            LOGGER.info(f"scannerDataEnd: got {len(future.payload)} scanner results")
            future.done.set()
        else:
            LOGGER.warning(f"scannerDataEnd: reqId={reqId} not found in _scanner_requests")

    def securityDefinitionOptionParameter(  # noqa: N802
        self,
        reqId: int,
        exchange: str,
        underlyingConId: int,
        tradingClass: str,
        multiplier: str,
        expirations: list[str],
        strikes: list[float],
    ) -> None:
        future = self._option_requests.get(reqId)
        if future is None:
            return
        future.payload.append(
            {
                "exchange": exchange,
                "underlying_con_id": underlyingConId,
                "trading_class": tradingClass,
                "multiplier": multiplier,
                "expirations": expirations,
                "strikes": strikes,
            }
        )

    def securityDefinitionOptionParameterEnd(self, reqId: int) -> None:  # noqa: N802
        future = self._option_requests.pop(reqId, None)
        if future:
            future.done.set()

    def marketRule(self, marketRuleId: int, priceIncrements: list[Any]) -> None:  # noqa: N802
        future = self._market_rule_requests.pop(marketRuleId, None)
        if future:
            future.payload = priceIncrements
            future.done.set()
        else:
            self._market_rule_cache[marketRuleId] = priceIncrements

    def error(self, reqId: int, errorCode: int, errorString: str) -> None:  # noqa: N802
        LOGGER.warning("IBKR error reqId=%s code=%s message=%s", reqId, errorCode, errorString)
        
        # 2176是小数股警告，数据仍会返回，不应终止请求
        # 2100-2199很多是警告，不是致命错误
        is_warning = errorCode in (2176,) or (2100 <= errorCode < 2200 and errorCode in (2103, 2104, 2106, 2107, 2158))
        
        if reqId in self._historical_requests and not is_warning:
            hist_future = self._historical_requests.pop(reqId)
            hist_future.error = errorString
            hist_future.done.set()
        if reqId in self._contract_requests and not is_warning:
            contract_future = self._contract_requests.pop(reqId)
            contract_future.error = errorString
            contract_future.done.set()
        if reqId in self._scanner_requests and not is_warning:
            scanner_future = self._scanner_requests.pop(reqId)
            scanner_future.error = errorString
            scanner_future.done.set()
        if reqId in self._option_requests and not is_warning:
            option_future = self._option_requests.pop(reqId)
            option_future.error = errorString
            option_future.done.set()
        if reqId in self._historical_tick_requests and not is_warning:
            tick_future = self._historical_tick_requests.pop(reqId)
            tick_future.error = errorString
            tick_future.done.set()
        if reqId in self._snapshot_requests:
            snapshot_future = self._snapshot_requests[reqId]
            snapshot_future.error = errorString
            snapshot_future.done.set()

        if errorCode in {1100, 1101, 1102} and not self._shutting_down:
            LOGGER.warning(
                "Detected IBKR connectivity issue (code=%s); attempting reconnect", errorCode
            )
            self._attempt_reconnect()

    def connectionClosed(self) -> None:  # noqa: N802
        LOGGER.warning("IBKR connection closed")
        if not self._shutting_down and self._should_reconnect:
            self._attempt_reconnect()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _push_tick(self, reqId: int, payload: Mapping[str, Any]) -> None:
        symbol = self._req_symbol.get(reqId)
        if not symbol:
            return
        queue = self._symbol_queues.get(symbol)
        if queue is not None:
            queue.put(payload)

    def _record_snapshot_value(self, req_id: int, field: str, value: Any) -> None:
        snapshot = self._snapshot_requests.get(req_id)
        if snapshot is None:
            return
        snapshot.data[field] = value

    def _collect_bid_ask_ticks(
        self,
        contract: Contract,
        *,
        start: datetime,
        end: datetime,
        use_rth: bool,
        batch_size: int = 1000,
    ) -> List[HistoricalTickBidAsk]:
        collected: List[HistoricalTickBidAsk] = []
        current_end = end.astimezone(timezone.utc)
        start_utc = start.astimezone(timezone.utc)
        attempts = 0
        while current_end > start_utc:
            self._historical_bucket.consume()
            req_id = next(self._req_id_counter)
            future = HistoricalTickRequest()
            self._historical_tick_requests[req_id] = future
            end_str = self._format_ib_datetime(current_end)
            LOGGER.info(
                "Requesting bid/ask ticks reqId=%s conid=%s symbol=%s end=%s useRth=%s",
                req_id,
                contract.conId,
                contract.symbol,
                end_str,
                use_rth,
            )
            self.reqHistoricalTicks(
                req_id,
                contract,
                "",
                end_str,
                batch_size,
                "BID_ASK",
                1 if use_rth else 0,
                True,
                [],
            )
            timeout = max(60.0, min(180.0, batch_size * 0.2))
            if not future.done.wait(timeout=timeout):
                self._historical_tick_requests.pop(req_id, None)
                raise TimeoutError(
                    f"Timed out waiting for bid/ask ticks for {contract.symbol} (reqId={req_id})"
                )
            if future.error:
                raise RuntimeError(f"Historical tick error: {future.error}")
            batch = list(future.ticks)
            if not batch:
                LOGGER.info("No more ticks for %s before %s", contract.symbol, end_str)
                break
            batch = [tick for tick in batch if tick is not None]
            batch.sort(key=lambda tick: tick.time)
            for tick in batch:
                tick_dt = datetime.fromtimestamp(tick.time, tz=timezone.utc)
                if tick_dt < start_utc:
                    continue
                if tick_dt > current_end:
                    continue
                collected.append(tick)
            earliest_epoch = batch[0].time
            earliest_dt = datetime.fromtimestamp(earliest_epoch, tz=timezone.utc)
            if earliest_dt <= start_utc:
                break
            current_end = earliest_dt - timedelta(seconds=1)
            attempts += 1
            _time.sleep(min(1.0 * attempts, 5.0))
        collected.sort(key=lambda tick: tick.time)
        return collected

    def _aggregate_bid_ask_ticks(
        self, ticks: Iterable[HistoricalTickBidAsk]
    ) -> list[Dict[str, Any]]:
        buckets: Dict[datetime, Dict[str, Any]] = {}
        for tick in ticks:
            tick_utc = datetime.fromtimestamp(tick.time, tz=timezone.utc)
            tick_et = tick_utc.astimezone(EASTERN)
            minute_start = tick_et.replace(second=0, microsecond=0)
            minute_end = minute_start + timedelta(minutes=1)
            ts_end_utc = minute_end.astimezone(timezone.utc)
            bucket = buckets.setdefault(
                ts_end_utc,
                {
                    "ts_end": ts_end_utc,
                    "bid": None,
                    "ask": None,
                    "bid_size": None,
                    "ask_size": None,
                    "last_tick": tick_utc,
                },
            )
            bucket["bid"] = float(tick.priceBid)
            bucket["ask"] = float(tick.priceAsk)
            try:
                bucket["bid_size"] = float(tick.sizeBid)
            except (TypeError, ValueError):
                bucket["bid_size"] = None
            try:
                bucket["ask_size"] = float(tick.sizeAsk)
            except (TypeError, ValueError):
                bucket["ask_size"] = None
            bucket["last_tick"] = tick_utc

        results: list[Dict[str, Any]] = []
        for ts_end, data in sorted(buckets.items(), key=lambda item: item[0]):
            bid = data["bid"]
            ask = data["ask"]
            if bid is None or ask is None:
                continue
            mid = (bid + ask) / 2
            results.append(
                {
                    "ts_end": ts_end,
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "bid_size": data["bid_size"],
                    "ask_size": data["ask_size"],
                }
            )
        return results

    @staticmethod
    def _format_ib_datetime(dt_value: datetime) -> str:
        return dt_value.astimezone(EASTERN).strftime("%Y%m%d %H:%M:%S US/Eastern")

    @staticmethod
    def _subscription_key(contract: Contract) -> str:
        if contract.secType == "OPT":
            if contract.conId:
                return f"OPT:{contract.conId}"
            expiry = contract.lastTradeDateOrContractMonth or ""
            right = contract.right or ""
            strike = contract.strike or 0
            return f"OPT:{contract.symbol}:{right}:{expiry}:{strike}"
        return contract.symbol

    @staticmethod
    def _snapshot_contract(contract: Contract) -> Dict[str, Any]:
        return {
            "symbol": contract.symbol,
            "secType": contract.secType,
            "exchange": contract.exchange,
            "currency": contract.currency,
            "expiry": contract.lastTradeDateOrContractMonth,
            "strike": contract.strike,
            "right": contract.right,
            "multiplier": contract.multiplier,
            "conId": contract.conId,
        }

    def _contract_from_snapshot(self, snapshot: Mapping[str, Any] | None) -> Contract | None:
        if not snapshot:
            return None
        sec_type = snapshot.get("secType") or "STK"
        symbol = snapshot.get("symbol") or ""
        exchange = snapshot.get("exchange") or "SMART"
        currency = snapshot.get("currency") or "USD"
        if sec_type == "OPT":
            expiry = snapshot.get("expiry") or ""
            strike = float(snapshot.get("strike") or 0)
            right = snapshot.get("right") or "CALL"
            multiplier = snapshot.get("multiplier") or "100"
            conid_value = snapshot.get("conId")
            conid = int(conid_value) if conid_value else None
            return self.option_contract(
                symbol=symbol,
                expiry=expiry,
                strike=strike,
                right=right,
                exchange=exchange,
                currency=currency,
                multiplier=multiplier,
                conid=conid,
            )
        if sec_type == "IND":
            return self.index_contract(symbol, exchange=exchange or "CBOE", currency=currency)
        return self.stock_contract(symbol, exchange=exchange, currency=currency)

    def _resubscribe_all(self) -> None:
        if not self._subscriptions:
            return
        LOGGER.info("Restoring %s subscriptions after reconnect", len(self._subscriptions))
        subscriptions = dict(self._subscriptions)
        for alias, params in subscriptions.items():
            queue = self._symbol_queues.get(alias)
            if queue is None:
                queue = Queue()
                self._symbol_queues[alias] = queue
            contract_snapshot = params.get("contract")
            contract_obj = (
                self._contract_from_snapshot(contract_snapshot) if contract_snapshot else None
            )
            try:
                self.subscribe_l1(
                    params.get("symbol", alias),
                    sec_type=params.get("sec_type", "STK"),
                    exchange=params.get("exchange", "SMART"),
                    currency=params.get("currency", "USD"),
                    generic_ticks=params.get("generic_ticks", "233"),
                    alias=alias,
                    contract=contract_obj,
                )
            except Exception:  # pragma: no cover - best effort during reconnect
                LOGGER.exception("Failed to re-subscribe market data for %s", alias)

    # ---------------------------------------------------------------------
    # Execution reports
    # ---------------------------------------------------------------------
    def req_executions(self, req_id: int | None = None) -> None:
        """Request execution reports for all clients (no clientId filter)."""
        from ibapi.execution import ExecutionFilter
        
        if req_id is None:
            req_id = next(self._req_id_counter)
        
        exec_filter = ExecutionFilter()
        # Don't set clientId to get executions from all clients (including manual TWS orders)
        
        LOGGER.info("Requesting execution reports reqId=%s (all clients)", req_id)
        self.reqExecutions(req_id, exec_filter)

    # ---------------------------------------------------------------------
    # Event bridging
    # ---------------------------------------------------------------------
    def register_event_queue(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: "asyncio.Queue[Dict[str, Any]]",
    ) -> None:
        self._event_loop = loop
        self._event_queue = queue

    def _dispatch_event(self, payload: Dict[str, Any]) -> None:
        if self._event_loop is None or self._event_queue is None:
            return
        try:
            self._event_loop.call_soon_threadsafe(self._event_queue.put_nowait, payload)
        except Exception:  # pragma: no cover - defensive
            LOGGER.exception("Failed to dispatch IB event", payload=payload)

    # ---------------------------------------------------------------------
    # Order / execution callbacks
    # ---------------------------------------------------------------------
    def orderStatus(  # noqa: N802
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        payload = {
            "type": "order_status",
            "order_id": orderId,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avg_fill_price": avgFillPrice,
            "last_fill_price": lastFillPrice,
            "perm_id": permId,
            "parent_id": parentId,
            "client_id": clientId,
            "why_held": whyHeld,
            "mkt_cap_price": mktCapPrice,
            "client_order_id": self._order_refs.get(orderId),
            "timestamp": datetime.utcnow().isoformat(),
        }
        self._dispatch_event(payload)

    def openOrder(  # noqa: N802
        self,
        orderId: int,
        contract: Contract,
        order: "Order",
        orderState,
    ) -> None:
        self._order_refs[orderId] = getattr(order, "orderRef", "") or ""
        self._order_contracts[orderId] = contract
        payload = {
            "type": "open_order",
            "order_id": orderId,
            "client_order_id": self._order_refs[orderId],
            "contract": {
                "symbol": contract.symbol,
                "right": contract.right,
                "strike": contract.strike,
                "expiry": contract.lastTradeDateOrContractMonth,
                "conid": contract.conId,
                "multiplier": contract.multiplier,
            },
        }
        self._dispatch_event(payload)

    def execDetails(self, reqId: int, contract: Contract, execution) -> None:  # noqa: N802
        payload = {
            "type": "execution",
            "order_id": execution.orderId,
            "exec_id": execution.execId,
            "symbol": contract.symbol,
            "side": execution.side,
            "shares": execution.shares,
            "price": execution.price,
            "cum_qty": getattr(execution, "cumQty", 0.0),
            "avg_price": getattr(execution, "avgPrice", execution.price),
            "exchange": execution.exchange,
            "time": execution.time,
            "client_order_id": execution.orderRef or self._order_refs.get(execution.orderId, ""),
            "contract": {
                "symbol": contract.symbol,
                "right": contract.right,
                "strike": contract.strike,
                "expiry": contract.lastTradeDateOrContractMonth,
                "conid": contract.conId,
                "multiplier": contract.multiplier,
            },
        }
        self._dispatch_event(payload)

    def commissionReport(self, commissionReport) -> None:  # noqa: N802
        payload = {
            "type": "commission",
            "exec_id": getattr(commissionReport, "execId", ""),
            "commission": getattr(commissionReport, "commission", 0.0),
            "currency": getattr(commissionReport, "currency", "USD"),
        }
        self._dispatch_event(payload)


def build_ibkr_client(settings: Settings) -> IBClient:
    client = IBClient(settings)
    client.connect_and_wait()
    return client


def get_next_order_id(client: IBClient) -> int:
    if client._next_order_id is None:  # noqa: SLF001
        raise RuntimeError("IBKR client has not received nextValidId yet")
    with client._order_id_lock:  # noqa: SLF001
        order_id = client._next_order_id  # noqa: SLF001
        client._next_order_id += 1  # noqa: SLF001
        return order_id
