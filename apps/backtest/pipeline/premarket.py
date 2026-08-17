from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict, Iterable, List, Mapping, Sequence, Set

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from libs.core import EASTERN
from libs.db.dao import BacktestTop5DAO
from libs.db.preearn import evaluate_preearn_guard

PRICE_QUANT = Decimal("0.000001")
RET_QUANT = Decimal("0.000001")


@dataclass(frozen=True, slots=True)
class DailySnapshot:
    symbol: str
    prev_close: Decimal | None
    close_rth: Decimal | None
    sma60: Decimal | None
    sma60_slope: Decimal | None
    market_cap: Decimal | None


@dataclass(frozen=True, slots=True)
class PriceSample:
    symbol: str
    price_preopen: Decimal
    close: Decimal | None
    open: Decimal | None
    ts_end_et: datetime


@dataclass(frozen=True, slots=True)
class CandidateRow:
    symbol: str
    ret_preopen: Decimal
    prev_close: Decimal
    price_sample: PriceSample
    snapshot: DailySnapshot
    preearn_allowed: bool
    preearn_context: Mapping[str, object]


class PremarketTop5Builder:
    """Compute and persist premarket Top5 data for backtests."""

    MIN_RTH_VOLUME = 800_000

    def __init__(
        self,
        session: Session,
        *,
        preearn_days_min: int,
        preearn_days_max: int,
        preearn_atr_pct_max: Decimal,
    ) -> None:
        self._session = session
        self._top5_dao = BacktestTop5DAO(session)
        self._preearn_days_min = preearn_days_min
        self._preearn_days_max = preearn_days_max
        self._preearn_atr_pct_max = Decimal(str(preearn_atr_pct_max))
        self._lagged_cache: Dict[date, Set[str]] = {}

    # ------------------------------------------------------------------
    def build_for_date(
        self,
        *,
        batch_id: str,
        trade_date: date,
        symbols: Sequence[str],
        universe_code: str,
    ) -> List[Mapping[str, object]]:
        universe_symbols = [s.strip().upper() for s in symbols if s.strip()]
        if not universe_symbols:
            self._top5_dao.replace(batch_id, trade_date, [])
            return []

        active_symbols = universe_symbols

        import structlog
        LOGGER = structlog.get_logger(__name__)
        LOGGER.info(
            "top5.generation.start",
            trade_date=trade_date.isoformat(),
            universe_count=len(universe_symbols),
            active_count=len(active_symbols),
        )
        
        if not active_symbols:
            LOGGER.warning(
                "top5.generation.no_active_symbols",
                trade_date=trade_date.isoformat(),
                reason="无可用股票",
            )
            self._top5_dao.replace(batch_id, trade_date, [])
            return []

        snapshots = self._load_daily_snapshot(trade_date, active_symbols)
        prices = self._load_premarket_prices(trade_date, active_symbols)
        
        LOGGER.info(
            "top5.generation.data_loaded",
            trade_date=trade_date.isoformat(),
            snapshots_count=len(snapshots),
            prices_count=len(prices)
        )
        
        candidates: list[CandidateRow] = []
        filtered_counts = {
            'no_snapshot': 0,
            'no_price': 0,
            'invalid_prev_close': 0,
            'invalid_pm_price': 0,
            'ma60_down': 0,
            'preearn_blocked': 0,
        }

        for symbol in active_symbols:
            key = symbol.upper()
            snapshot = snapshots.get(key)
            price = prices.get(key)
            if snapshot is None or price is None:
                filtered_counts['no_snapshot' if snapshot is None else 'no_price'] += 1
                continue
            prev_close = snapshot.prev_close
            if prev_close is None or prev_close <= Decimal("0"):
                filtered_counts['invalid_prev_close'] += 1
                continue

            price_value = price.price_preopen
            if price_value <= Decimal("0"):
                filtered_counts['invalid_pm_price'] += 1
                continue

            try:
                ret = (price_value - prev_close) / prev_close
                ret = ret.quantize(RET_QUANT, rounding=ROUND_HALF_UP)
            except (InvalidOperation, ZeroDivisionError):
                filtered_counts['invalid_pm_price'] += 1
                continue

            m60_up = _m60_up(snapshot)
            if not m60_up:
                filtered_counts['ma60_down'] += 1
                continue

            allowed, context = evaluate_preearn_guard(
                self._session,
                symbol=key,
                trade_date=trade_date,
                days_min=self._preearn_days_min,
                days_max=self._preearn_days_max,
                atr_pct_max=self._preearn_atr_pct_max,
            )
            if not allowed:
                filtered_counts['preearn_blocked'] += 1
                continue

            candidate = CandidateRow(
                symbol=key,
                ret_preopen=ret,
                prev_close=prev_close,
                price_sample=price,
                snapshot=snapshot,
                preearn_allowed=allowed,
                preearn_context=context,
            )
            candidates.append(candidate)

        LOGGER.info(
            "top5.generation.filtering_summary",
            trade_date=trade_date.isoformat(),
            active_symbols=len(active_symbols),
            candidates_passed=len(candidates),
            filtered_no_snapshot=filtered_counts['no_snapshot'],
            filtered_no_price=filtered_counts['no_price'],
            filtered_invalid_prev_close=filtered_counts['invalid_prev_close'],
            filtered_invalid_pm_price=filtered_counts['invalid_pm_price'],
            filtered_ma60_down=filtered_counts['ma60_down'],
            filtered_preearn_blocked=filtered_counts['preearn_blocked']
        )
        
        if not candidates:
            LOGGER.warning(
                "top5.generation.no_candidates",
                trade_date=trade_date.isoformat(),
                reason="所有股票都被过滤，无候选股票",
                ma60_down_ratio=f"{filtered_counts['ma60_down']}/{len(active_symbols)}"
            )
            self._top5_dao.replace(batch_id, trade_date, [])
            return []

        candidates.sort(key=lambda row: row.ret_preopen, reverse=True)
        top_rows = candidates[:10]
        
        LOGGER.info(
            "top5.generation.completed",
            trade_date=trade_date.isoformat(),
            top10_count=len(top_rows),
            top10_symbols=[c.symbol for c in top_rows],
            gap_range=f"{top_rows[-1].ret_preopen:.2%} ~ {top_rows[0].ret_preopen:.2%}"
        )

        payload: list[dict[str, object]] = []
        for rank, candidate in enumerate(top_rows, start=1):
            payload.append(
                {
                    "rank": rank,
                    "symbol": candidate.symbol,
                    "ret_0925_0930": candidate.ret_preopen,
                    "price_0925": candidate.price_sample.price_preopen,
                    "prev_close": candidate.prev_close,
                    "market_cap_usd": candidate.snapshot.market_cap,
                    "m60_up": True,
                    "preearn_allowed": True,
                    "source": universe_code,
                    "context": _build_context(candidate),
                }
            )

        self._top5_dao.replace(batch_id, trade_date, payload)
        return payload

    # ------------------------------------------------------------------
    def _load_premarket_prices(
        self, trade_date: date, symbols: Sequence[str]
    ) -> Dict[str, PriceSample]:
        start_et = datetime.combine(trade_date, time(9, 27), EASTERN)
        end_et = datetime.combine(trade_date, time(9, 30), EASTERN)
        start_utc = start_et.astimezone(timezone.utc)
        end_utc = end_et.astimezone(timezone.utc)

        stmt = text(
            """
            SELECT symbol, ts_end, open, close
            FROM bars1m_equity
            WHERE ts_end >= :start_ts
              AND ts_end < :end_ts
              AND symbol = ANY(:symbols)
            """
        )
        rows = self._session.execute(
            stmt,
            {
                "start_ts": start_utc,
                "end_ts": end_utc,
                "symbols": list(symbols),
            },
        ).fetchall()

        bucket: Dict[str, list[tuple[datetime, float | None, float | None]]] = {}
        for row in rows:
            symbol = str(row[0]).upper()
            ts_end = row[1]
            if not isinstance(ts_end, datetime):
                continue
            ts_et = ts_end.astimezone(EASTERN)
            bucket.setdefault(symbol, []).append((ts_et, row[2], row[3]))

        prices: Dict[str, PriceSample] = {}
        for symbol, samples in bucket.items():
            target = _select_preopen_bar(samples)
            if target is None:
                continue
            ts_et, open_, close_ = target
            close_dec = _to_decimal(close_, quant=PRICE_QUANT)
            open_dec = _to_decimal(open_, quant=PRICE_QUANT)
            price = close_dec or open_dec
            if price is None or price <= Decimal("0"):
                continue
            prices[symbol] = PriceSample(
                symbol=symbol,
                price_preopen=price,
                close=close_dec,
                open=open_dec,
                ts_end_et=ts_et,
            )
        return prices

    def _load_daily_snapshot(
        self, trade_date: date, symbols: Sequence[str]
    ) -> Dict[str, DailySnapshot]:
        symbols_list = list(symbols)
        metrics = self._load_daily_metrics(trade_date, symbols_list)
        market_caps = self._load_market_caps(symbols_list)

        snapshots: Dict[str, DailySnapshot] = {}
        for symbol in symbols_list:
            upper = symbol.upper()
            metric = metrics.get(upper)
            market_cap = market_caps.get(upper)
            if metric is None:
                continue
            snapshots[upper] = DailySnapshot(
                symbol=upper,
                prev_close=metric.get("prev_close"),
                close_rth=metric.get("close_rth"),
                sma60=metric.get("sma60"),
                sma60_slope=metric.get("sma60_slope"),
                market_cap=market_cap,
            )
        return snapshots

    def _load_daily_metrics(
        self, trade_date: date, symbols: Sequence[str]
    ) -> Dict[str, Dict[str, Decimal | None]]:
        metrics: Dict[str, Dict[str, Decimal | None]] = {}

        ohlcv_stmt = text(
            """
            SELECT DISTINCT ON (symbol) symbol, close_rth
            FROM v_daily_ohlcv_enriched
            WHERE trade_date_et::date < :trade_date
              AND symbol = ANY(:symbols)
            ORDER BY symbol, trade_date_et DESC
            """
        )
        ohlcv_rows = self._session.execute(
            ohlcv_stmt, {"trade_date": trade_date, "symbols": list(symbols)}
        ).fetchall()

        for row in ohlcv_rows:
            symbol = str(row[0]).upper()
            metrics[symbol] = {
                "prev_close": _to_decimal(row[1], quant=PRICE_QUANT),
                "close_rth": _to_decimal(row[1], quant=PRICE_QUANT),
                "sma60": None,
                "sma60_slope": None,
            }

        try:
            ma_stmt = text(
                """
                SELECT DISTINCT ON (symbol) symbol, sma60, sma60_slope
                FROM v_daily_ma60
                WHERE trade_date_et::date < :trade_date
                  AND symbol = ANY(:symbols)
                ORDER BY symbol, trade_date_et DESC
                """
            )
            ma_rows = self._session.execute(
                ma_stmt, {"trade_date": trade_date, "symbols": list(symbols)}
            ).fetchall()
        except SQLAlchemyError:
            self._session.rollback()
            ma_rows = []

        for row in ma_rows:
            symbol = str(row[0]).upper()
            metric = metrics.setdefault(
                symbol,
                {"prev_close": None, "close_rth": None, "sma60": None, "sma60_slope": None},
            )
            metric["sma60"] = _to_decimal(row[1], quant=PRICE_QUANT)
            metric["sma60_slope"] = _to_decimal(row[2], quant=PRICE_QUANT)

        for symbol in symbols:
            metric = metrics.setdefault(
                symbol.upper(),
                {"prev_close": None, "close_rth": None, "sma60": None, "sma60_slope": None},
            )
            if metric["prev_close"] is None:
                metric["prev_close"] = self._fallback_prev_close(symbol, trade_date)
            if metric["close_rth"] is None:
                metric["close_rth"] = metric["prev_close"]

        return metrics

    def _fallback_prev_close(self, symbol: str, trade_date: date) -> Decimal | None:
        stmt = text(
            """
            SELECT close
            FROM bars1m_equity
            WHERE symbol = :symbol
              AND (ts_end AT TIME ZONE 'America/New_York')::date < :trade_date
              AND (ts_end AT TIME ZONE 'America/New_York')::time <= TIME '16:00'
            ORDER BY ts_end DESC
            LIMIT 1
            """
        )
        row = self._session.execute(
            stmt,
            {"symbol": symbol.upper(), "trade_date": trade_date},
        ).fetchone()
        if not row or row[0] is None:
            return None
        return _to_decimal(row[0], quant=PRICE_QUANT)

    def _load_market_caps(self, symbols: Sequence[str]) -> Dict[str, Decimal | None]:
        stmt = text(
            """
            SELECT symbol, market_cap_usd
            FROM ref_market_cap
            WHERE symbol = ANY(:symbols)
            """
        )
        rows = self._session.execute(stmt, {"symbols": list(symbols)}).fetchall()
        caps: Dict[str, Decimal | None] = {}
        for row in rows:
            symbol = str(row[0]).upper()
            caps[symbol] = _to_decimal(row[1], quant=Decimal("0.01"))
        return caps


def _select_preopen_bar(
    items: Iterable[tuple[datetime, float | None, float | None]]
) -> tuple[datetime, float | None, float | None] | None:
    for ts_et, open_, close_ in items:
        if ts_et.hour == 9 and ts_et.minute == 28:
            return ts_et, open_, close_
    return None


def _to_decimal(value: object | None, *, quant: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        result = value
    else:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None
    if quant is not None:
        try:
            return result.quantize(quant, rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return None
    return result


def _m60_up(snapshot: DailySnapshot) -> bool:
    if (
        snapshot.close_rth is None
        or snapshot.sma60 is None
        or snapshot.sma60_slope is None
    ):
        return False
    return snapshot.close_rth > snapshot.sma60 and snapshot.sma60_slope > Decimal("0")


def _build_context(candidate: CandidateRow) -> dict[str, object]:
    price = candidate.price_sample
    snapshot = candidate.snapshot
    context = {
        "price_ts": price.ts_end_et.isoformat(),
        "price_close": _decimal_to_str(price.close),
        "price_open": _decimal_to_str(price.open),
        "prev_close": _decimal_to_str(candidate.prev_close),
        "close_rth": _decimal_to_str(snapshot.close_rth),
        "sma60": _decimal_to_str(snapshot.sma60),
        "sma60_slope": _decimal_to_str(snapshot.sma60_slope),
        "preearn": candidate.preearn_context,
    }
    return context


def _decimal_to_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")
