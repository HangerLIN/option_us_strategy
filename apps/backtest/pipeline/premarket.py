from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict, Iterable, List, Mapping, Sequence

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
    price_0925: Decimal
    close: Decimal | None
    open: Decimal | None
    ts_end_et: datetime


@dataclass(frozen=True, slots=True)
class CandidateRow:
    symbol: str
    ret_0925_0930: Decimal
    prev_close: Decimal
    price_sample: PriceSample
    snapshot: DailySnapshot
    preearn_allowed: bool
    preearn_context: Mapping[str, object]


class PremarketTop5Builder:
    """Compute and persist premarket Top5 data for backtests."""

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

    # ------------------------------------------------------------------
    def build_for_date(
        self,
        *,
        batch_id: str,
        trade_date: date,
        symbols: Sequence[str],
        universe_code: str,
    ) -> List[Mapping[str, object]]:
        if not symbols:
            self._top5_dao.replace(batch_id, trade_date, [])
            return []

        snapshots = self._load_daily_snapshot(trade_date, symbols)
        prices = self._load_premarket_prices(trade_date, symbols)
        candidates: list[CandidateRow] = []

        for symbol in symbols:
            key = symbol.upper()
            snapshot = snapshots.get(key)
            price = prices.get(key)
            if snapshot is None or price is None:
                continue
            prev_close = snapshot.prev_close
            if prev_close is None or prev_close <= Decimal("0"):
                continue

            price_value = price.price_0925
            if price_value <= Decimal("0"):
                continue

            try:
                ret = (price_value - prev_close) / prev_close
                ret = ret.quantize(RET_QUANT, rounding=ROUND_HALF_UP)
            except (InvalidOperation, ZeroDivisionError):
                continue

            m60_up = _m60_up(snapshot)
            if not m60_up:
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
                continue

            candidate = CandidateRow(
                symbol=key,
                ret_0925_0930=ret,
                prev_close=prev_close,
                price_sample=price,
                snapshot=snapshot,
                preearn_allowed=allowed,
                preearn_context=context,
            )
            candidates.append(candidate)

        if not candidates:
            self._top5_dao.replace(batch_id, trade_date, [])
            return []

        candidates.sort(key=lambda row: row.ret_0925_0930, reverse=True)
        top_rows = candidates[:5]

        payload: list[dict[str, object]] = []
        for rank, candidate in enumerate(top_rows, start=1):
            payload.append(
                {
                    "rank": rank,
                    "symbol": candidate.symbol,
                    "ret_0925_0930": candidate.ret_0925_0930,
                    "price_0925": candidate.price_sample.price_0925,
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
        start_et = datetime.combine(trade_date, time(9, 25), EASTERN)
        end_et = datetime.combine(trade_date, time(9, 31), EASTERN)
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
            target = _select_0925_bar(samples)
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
                price_0925=price,
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
            SELECT symbol, prev_close_rth, close_rth
            FROM v_daily_ohlcv_enriched
            WHERE trade_date_et::date = :trade_date
              AND symbol = ANY(:symbols)
            """
        )
        ohlcv_rows = self._session.execute(
            ohlcv_stmt, {"trade_date": trade_date, "symbols": list(symbols)}
        ).fetchall()

        for row in ohlcv_rows:
            symbol = str(row[0]).upper()
            metrics[symbol] = {
                "prev_close": _to_decimal(row[1], quant=PRICE_QUANT),
                "close_rth": _to_decimal(row[2], quant=PRICE_QUANT),
                "sma60": None,
                "sma60_slope": None,
            }

        try:
            ma_stmt = text(
                """
                SELECT symbol, sma60, sma60_slope
                FROM v_daily_ma60
                WHERE trade_date_et::date = :trade_date
                  AND symbol = ANY(:symbols)
                """
            )
            ma_rows = self._session.execute(
                ma_stmt, {"trade_date": trade_date, "symbols": list(symbols)}
            ).fetchall()
        except SQLAlchemyError:
            ma_rows = []

        for row in ma_rows:
            symbol = str(row[0]).upper()
            metric = metrics.setdefault(
                symbol,
                {"prev_close": None, "close_rth": None, "sma60": None, "sma60_slope": None},
            )
            metric["sma60"] = _to_decimal(row[1], quant=PRICE_QUANT)
            metric["sma60_slope"] = _to_decimal(row[2], quant=PRICE_QUANT)

        return metrics

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


def _select_0925_bar(
    items: Iterable[tuple[datetime, float | None, float | None]]
) -> tuple[datetime, float | None, float | None] | None:
    for ts_et, open_, close_ in items:
        if ts_et.hour == 9 and ts_et.minute == 25:
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
