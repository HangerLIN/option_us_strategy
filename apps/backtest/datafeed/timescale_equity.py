from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import pandas as pd
from backtrader.feeds import PandasData
from sqlalchemy.orm import Session

from apps.backtest.dao import BacktestDAO, EquityBarRow
from apps.rt_engine import indicators as indcalc
from libs.core import EASTERN, get_settings
from libs.core.logging import get_logger
from libs.db.dim_trading_calendar import get_trading_session
from libs.infra import build_ibkr_client
from libs.infra.ibkr_client import IBClient

logger = get_logger(__name__)


class TimescaleEquityData(PandasData):
    """Backtrader data feed backed by bars1m_equity + indicators_eq_1m."""

    lines = (
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
    )
    params = tuple(
        (key, -1)
        for key in (
            "datetime",
            "high",
            "low",
            "open",
            "close",
            "volume",
            "openinterest",
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
        )
    )

    @classmethod
    def from_timescale(
        cls,
        *,
        session: Session,
        dao: BacktestDAO,
        symbol: str,
        start: datetime,
        end: datetime,
        auto_fill_missing: bool = True,
        ib: IBClient | None = None,
    ) -> "TimescaleEquityData":
        """
        读取 TimescaleDB，必要时自动补齐缺口（仅限价格 + 指标 1m）。
        - auto_fill_missing=True 时：扫描缺口→调用 IBKR 历史回补→重算指标→入库→二次校验。
        - auto_fill_missing=False 时：仅校验，发现缺口直接报错。
        """
        context_start, context_end = _equity_context_window(start=start, end=end)
        rows = dao.fetch_equity_bars(symbol=symbol, start_ts=context_start, end_ts=context_end)
        if not rows:
            raise RuntimeError(f"No equity data for {symbol} between {start} and {end}")

        df = _rows_to_dataframe(rows)
        if len(df) < 100:
            raise RuntimeError(f"Insufficient equity data for {symbol}: only {len(df)} bars")

        gap_report = _scan_equity_gaps(df=df, symbol=symbol)
        if gap_report.has_any_gap():
            logger.warning("equity_data_gaps_detected", symbol=symbol, summary=gap_report.summary())
            _emit_risk_event(
                dao=dao,
                symbol=symbol,
                code="DATA_GAP_DETECTED",
                severity="WARN",
                payload=gap_report.to_payload(),
            )

            if not auto_fill_missing:
                raise RuntimeError(gap_report.human_readable(symbol))

            client = ib
            owns_client = False
            if client is None:
                settings = get_settings()
                client = build_ibkr_client(settings)
                owns_client = True

            try:
                _fill_equity_gaps_via_ibkr(
                    dao=dao,
                    ib=client,
                    symbol=symbol,
                    gap_report=gap_report,
                )
            finally:
                if owns_client and client is not None:
                    try:
                        client.disconnect_and_stop()
                    except Exception:  # pragma: no cover - defensive
                        logger.exception("ibkr.disconnect_failed")

            rows = dao.fetch_equity_bars(symbol=symbol, start_ts=context_start, end_ts=context_end)
            if not rows:
                raise RuntimeError(f"After fill, still no equity data for {symbol}")

            df = _rows_to_dataframe(rows)
            gap_report_after = _scan_equity_gaps(df=df, symbol=symbol)
            if gap_report_after.has_any_gap():
                _emit_risk_event(
                    dao=dao,
                    symbol=symbol,
                    code="DATA_GAP_PERSIST",
                    severity="ERROR",
                    payload=gap_report_after.to_payload(),
                )
                raise RuntimeError(gap_report_after.human_readable(symbol))

            _emit_risk_event(
                dao=dao,
                symbol=symbol,
                code="DATA_GAP_FILLED",
                severity="INFO",
                payload={
                    "symbol": symbol,
                    "filled_minutes": gap_report.total_missing_minutes(),
                },
            )

        _validate_equity_bar_coverage(df=df, symbol=symbol)

        start_ts = pd.Timestamp(start)
        if start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize("UTC")
        else:
            start_ts = start_ts.tz_convert("UTC")
        end_ts = pd.Timestamp(end)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize("UTC")
        else:
            end_ts = end_ts.tz_convert("UTC")
        df = df[
            (df.index.tz_convert("UTC") >= start_ts)
            & (df.index.tz_convert("UTC") < end_ts)
        ]
        if len(df) < 100:
            raise RuntimeError(f"Insufficient equity data for {symbol}: only {len(df)} bars")

        data = cls(dataname=df, name=symbol)
        return data


@dataclass
class MinuteGap:
    trade_date: date
    missing_minutes: List[pd.Timestamp] = field(default_factory=list)


@dataclass
class IndicatorGap:
    trade_date: date
    rows: List[Tuple[pd.Timestamp, List[str]]] = field(default_factory=list)


@dataclass
class GapReport:
    price_gaps: List[MinuteGap] = field(default_factory=list)
    indicator_gaps: List[IndicatorGap] = field(default_factory=list)
    prev_close_missing: List[date] = field(default_factory=list)

    def has_any_gap(self) -> bool:
        return bool(self.price_gaps or self.indicator_gaps or self.prev_close_missing)

    def total_missing_minutes(self) -> int:
        return sum(len(g.missing_minutes) for g in self.price_gaps)

    def summary(self) -> Dict[str, int]:
        return {
            "price_gap_days": len(self.price_gaps),
            "price_missing_minutes": self.total_missing_minutes(),
            "indicator_gap_days": len(self.indicator_gaps),
            "prev_close_missing_days": len(self.prev_close_missing),
        }

    def to_payload(self) -> Dict[str, Any]:
        return {
            "price_gaps": [
                {
                    "trade_date": g.trade_date.isoformat(),
                    "missing_minutes": [ts.isoformat() for ts in g.missing_minutes],
                }
                for g in self.price_gaps
            ],
            "indicator_gaps": [
                {
                    "trade_date": ig.trade_date.isoformat(),
                    "rows": [
                        {"ts_end": ts.isoformat(), "missing_cols": cols} for ts, cols in ig.rows
                    ],
                }
                for ig in self.indicator_gaps
            ],
            "prev_close_missing": [d.isoformat() for d in self.prev_close_missing],
        }

    def human_readable(self, symbol: str) -> str:
        price_missing = self.total_missing_minutes()
        indicator_rows = sum(len(ig.rows) for ig in self.indicator_gaps)
        prev_close = len(self.prev_close_missing)
        return (
            f"[{symbol}] gaps → price minutes: {price_missing}, "
            f"indicator rows: {indicator_rows}, prev-close-missing days: {prev_close}"
        )


_RTH_BAR_START = time(9, 31)
_NUMERIC_COLS_ALL = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "rsi6",
    "rsi12",
    "rsi24",
    "boll_mid",
    "boll_up",
    "boll_dn",
    "atr14",
    "ao",
    "stoch_k",
    "stoch_d",
    "stoch_rsi_k",
    "stoch_rsi_d",
    "cci14",
    "cci6",
    "sma5",
    "lr_m5_slope",
    "lr_boll_dn_slope",
    "lr_obv_slope",
    "obv",
    "obv_ma6",
    "obv_ema20",
    "mfi14",
    "rvol6",
]
_INDICATOR_COLS = [
    "rsi6",
    "rsi12",
    "rsi24",
    "atr14",
    "ao",
    "stoch_k",
    "stoch_d",
    "stoch_rsi_k",
    "stoch_rsi_d",
    "cci14",
    "cci6",
    "sma5",
    "lr_m5_slope",
    "lr_boll_dn_slope",
    "lr_obv_slope",
    "obv",
    "obv_ma6",
    "obv_ema20",
    "mfi14",
    "rvol6",
]


def _equity_context_window(*, start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start_utc = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end_utc = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
    start_et = start_utc.astimezone(EASTERN)
    try:
        prev_trade_date = _previous_trading_date(start_et.date())
        prev_session = get_trading_session(prev_trade_date)
        prev_close_et = datetime.combine(
            prev_session.session_date,
            prev_session.close_time,
            tzinfo=EASTERN,
        )
        context_start = min(start_et.replace(hour=9, minute=31, second=0, microsecond=0), prev_close_et)
    except KeyError:
        context_start = start_et.replace(hour=9, minute=31, second=0, microsecond=0)
    # DAO queries treat end_ts as exclusive, but RTH gap validation expects the
    # 16:00 ET closing minute to be present in the context dataframe.
    context_end = end_utc.astimezone(timezone.utc) + timedelta(minutes=1)
    return context_start.astimezone(timezone.utc), context_end


def _decimal_to_numeric(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _rows_to_dataframe(rows: Sequence[EquityBarRow]) -> pd.DataFrame:
    df = pd.DataFrame([asdict(row) for row in rows])
    if df.empty:
        return df
    df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True).dt.tz_convert("US/Eastern")
    for col in _NUMERIC_COLS_ALL:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].apply(_decimal_to_numeric), errors="coerce")
    df = df.set_index("ts_end").sort_index()
    return df


def _scan_equity_gaps(*, df: pd.DataFrame, symbol: str) -> GapReport:
    if not isinstance(df.index, pd.DatetimeIndex):
        raise RuntimeError(f"Equity feed index for {symbol} must be datetime-aware.")
    if df.index.tz is None:
        raise RuntimeError(f"Equity feed index for {symbol} must include timezone information.")

    tzinfo = df.index.tz
    trade_dates: List[date] = sorted({ts.date() for ts in df.index})
    report = GapReport()

    indicator_cols = [col for col in _INDICATOR_COLS if col in df.columns]

    for trade_date in trade_dates:
        day_slice = df[df.index.date == trade_date]
        if day_slice.empty:
            continue

        session = get_trading_session(trade_date)
        start_ts = pd.Timestamp(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            _RTH_BAR_START.hour,
            _RTH_BAR_START.minute,
            tz=tzinfo,
        )
        end_ts = pd.Timestamp(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            session.close_time.hour,
            session.close_time.minute,
            tz=tzinfo,
        )
        intraday_slice = day_slice.between_time(
            _RTH_BAR_START.strftime("%H:%M"), session.close_time.strftime("%H:%M")
        )

        if not intraday_slice.empty:
            if len(intraday_slice) < 100:
                continue
            expected = pd.date_range(start_ts, end_ts, freq="1min")
            missing_idx = expected.difference(intraday_slice.index)
            if not missing_idx.empty:
                report.price_gaps.append(
                    MinuteGap(trade_date=trade_date, missing_minutes=list(missing_idx))
                )

            if indicator_cols and len(intraday_slice) >= 100:
                nan_rows: List[Tuple[pd.Timestamp, List[str]]] = []
                for ts, row in intraday_slice.iloc[60:][indicator_cols].iterrows():
                    missing_cols = [col for col in indicator_cols if pd.isna(row.get(col))]
                    if missing_cols:
                        nan_rows.append((ts, missing_cols))
                if nan_rows:
                    report.indicator_gaps.append(
                        IndicatorGap(trade_date=trade_date, rows=nan_rows)
                    )

    return report


def _validate_equity_bar_coverage(*, df: pd.DataFrame, symbol: str) -> None:
    tzinfo = df.index.tz
    trade_dates = sorted({ts.date() for ts in df.index})
    for trade_date in trade_dates:
        day_slice = df[df.index.date == trade_date]
        if day_slice.empty:
            continue

        session = get_trading_session(trade_date)
        expected_start = pd.Timestamp(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            _RTH_BAR_START.hour,
            _RTH_BAR_START.minute,
            tz=tzinfo,
        )
        expected_end = pd.Timestamp(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            session.close_time.hour,
            session.close_time.minute,
            tz=tzinfo,
        )
        intraday_slice = day_slice.between_time(
            _RTH_BAR_START.strftime("%H:%M"), session.close_time.strftime("%H:%M")
        )
        if intraday_slice.empty:
            continue
        if len(intraday_slice) < 100:
            continue

        expected_range = pd.date_range(expected_start, expected_end, freq="1min")
        missing = expected_range.difference(intraday_slice.index)
        if not missing.empty:
            sample = ", ".join(missing.strftime("%Y-%m-%d %H:%M")[:5])
            raise RuntimeError(
                f"Equity data for {symbol} missing {len(missing)} minute bars on "
                f"{trade_date.isoformat()} (sample: {sample})"
            )


def _emit_risk_event(
    *,
    dao: BacktestDAO,
    symbol: str,
    code: str,
    severity: str,
    payload: Dict[str, Any],
) -> None:
    dao.insert_risk_event(
        symbol=symbol,
        event_code=code,
        severity=severity,
        payload_json=json.dumps(payload, ensure_ascii=False),
    )


def _fill_equity_gaps_via_ibkr(
    *,
    dao: BacktestDAO,
    ib: IBClient,
    symbol: str,
    gap_report: GapReport,
) -> None:
    if not gap_report.has_any_gap():
        return

    baseline_map = dao.fetch_rvol_baseline(symbol=symbol)

    for price_gap in gap_report.price_gaps:
        if not price_gap.missing_minutes:
            continue
        missing_minutes = sorted(price_gap.missing_minutes)
        window_start = missing_minutes[0] - pd.Timedelta(minutes=60)
        window_end = missing_minutes[-1] + pd.Timedelta(minutes=1)

        start_utc = window_start.tz_convert("UTC").to_pydatetime()
        end_utc = window_end.tz_convert("UTC").to_pydatetime()

        try:
            bars = ib.req_historical_1m(
                symbol=symbol,
                start=start_utc,
                end=end_utc,
                use_rth=True,
                what_to_show="TRADES",
            )
        except Exception as exc_trades:
            logger.warning(
                "ibkr_trades_failed_fallback_midpoint", symbol=symbol, err=str(exc_trades)
            )
            bars = ib.req_historical_1m(
                symbol=symbol,
                start=start_utc,
                end=end_utc,
                use_rth=True,
                what_to_show="MIDPOINT",
            )

        if not bars:
            _emit_risk_event(
                dao=dao,
                symbol=symbol,
                code="DATA_GAP_FILL_FAILED",
                severity="ERROR",
                payload={
                    "symbol": symbol,
                    "trade_date": price_gap.trade_date.isoformat(),
                    "reason": "ibkr_no_bars",
                    "start": window_start.isoformat(),
                    "end": window_end.isoformat(),
                },
            )
            continue

        df_bars = _ib_bars_to_dataframe(bars)
        df_bars = df_bars[
            (df_bars.index >= window_start) & (df_bars.index <= window_end)
        ]
        if df_bars.empty:
            continue

        dao.upsert_equity_bars_from_df(symbol=symbol, df=df_bars)
        _recompute_equity_indicators_for_window(
            dao=dao,
            symbol=symbol,
            baseline_map=baseline_map,
            window_start=window_start,
            window_end=window_end,
            fallback_df=df_bars,
            failure_payload={
                "symbol": symbol,
                "trade_date": price_gap.trade_date.isoformat(),
                "reason": "indicator_compute_failed_after_price_fill",
                "start": window_start.isoformat(),
                "end": window_end.isoformat(),
            },
        )

    for indicator_gap in gap_report.indicator_gaps:
        missing_ts = [ts for ts, _ in indicator_gap.rows]
        if not missing_ts:
            continue
        window_start = min(missing_ts) - pd.Timedelta(minutes=60)
        window_end = max(missing_ts) + pd.Timedelta(minutes=1)

        start_utc = window_start.tz_convert("UTC").to_pydatetime()
        end_utc = window_end.tz_convert("UTC").to_pydatetime()

        rows = dao.fetch_equity_bars(
            symbol=symbol,
            start_ts=start_utc,
            end_ts=end_utc,
        )
        if not rows:
            _emit_risk_event(
                dao=dao,
                symbol=symbol,
                code="DATA_GAP_FILL_FAILED",
                severity="ERROR",
                payload={
                    "symbol": symbol,
                    "trade_date": indicator_gap.trade_date.isoformat(),
                    "reason": "no_prices_to_compute_indicators",
                    "start": window_start.isoformat(),
                    "end": window_end.isoformat(),
                },
            )
            continue

        df_prices = _rows_to_dataframe(rows)
        df_prices = df_prices[
            (df_prices.index >= window_start) & (df_prices.index <= window_end)
        ]
        if df_prices.empty:
            continue

        _recompute_equity_indicators_for_window(
            dao=dao,
            symbol=symbol,
            baseline_map=baseline_map,
            window_start=window_start,
            window_end=window_end,
            fallback_df=df_prices,
            failure_payload={
                "symbol": symbol,
                "trade_date": indicator_gap.trade_date.isoformat(),
                "reason": "indicator_compute_failed",
                "start": window_start.isoformat(),
                "end": window_end.isoformat(),
            },
        )

    for trade_date in gap_report.prev_close_missing:
        prev_trade_date = _previous_trading_date(trade_date)
        prev_session = get_trading_session(prev_trade_date)
        close_et = datetime.combine(
            prev_session.session_date,
            prev_session.close_time,
            tzinfo=EASTERN,
        )
        window_start = close_et - timedelta(minutes=2)
        window_end = close_et + timedelta(minutes=1)

        start_utc = window_start.astimezone(timezone.utc)
        end_utc = window_end.astimezone(timezone.utc)

        bars = ib.req_historical_1m(
            symbol=symbol,
            start=start_utc,
            end=end_utc,
            use_rth=True,
            what_to_show="TRADES",
        )

        if not bars:
            _emit_risk_event(
                dao=dao,
                symbol=symbol,
                code="DATA_GAP_FILL_FAILED",
                severity="ERROR",
                payload={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "reason": "prev_close_cannot_fill",
                },
            )
            continue

        df_bars = _ib_bars_to_dataframe(bars)
        df_bars = df_bars[
            (df_bars.index >= window_start) & (df_bars.index <= window_end)
        ]
        if df_bars.empty:
            continue

        dao.upsert_equity_bars_from_df(symbol=symbol, df=df_bars)
        _recompute_equity_indicators_for_window(
            dao=dao,
            symbol=symbol,
            baseline_map=baseline_map,
            window_start=window_start,
            window_end=window_end,
            fallback_df=df_bars,
            failure_payload={
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "reason": "indicator_compute_failed_after_prev_close_fill",
                "start": window_start.isoformat(),
                "end": window_end.isoformat(),
            },
        )


def _recompute_equity_indicators_for_window(
    *,
    dao: BacktestDAO,
    symbol: str,
    baseline_map: Mapping[int, Decimal],
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
    fallback_df: pd.DataFrame | None = None,
    failure_payload: Dict[str, Any] | None = None,
) -> None:
    start_utc = window_start.tz_convert("UTC").to_pydatetime()
    end_utc = window_end.tz_convert("UTC").to_pydatetime()
    frames: List[pd.DataFrame] = []
    rows = dao.fetch_equity_bars(symbol=symbol, start_ts=start_utc, end_ts=end_utc)
    if rows:
        frames.append(_rows_to_dataframe(rows))
    if fallback_df is not None and not fallback_df.empty:
        frames.append(fallback_df.copy())
    if not frames:
        return

    df_prices = pd.concat(frames).sort_index()
    df_prices = df_prices[~df_prices.index.duplicated(keep="last")]
    df_prices = df_prices[(df_prices.index >= window_start) & (df_prices.index < window_end)]
    if df_prices.empty:
        return

    try:
        df_inds = indcalc.compute_indicators(df_prices, baseline_map=baseline_map)
    except Exception as exc:  # pragma: no cover - defensive around third-party indicators
        logger.warning(
            "equity_indicator_recompute_failed",
            symbol=symbol,
            rows=len(df_prices),
            err=str(exc),
        )
        if failure_payload is not None:
            payload = dict(failure_payload)
            payload["error"] = str(exc)
            _emit_risk_event(
                dao=dao,
                symbol=symbol,
                code="DATA_GAP_FILL_FAILED",
                severity="ERROR",
                payload=payload,
            )
        return

    if not df_inds.empty:
        dao.upsert_equity_indicators_from_df(symbol=symbol, df=df_inds)


def _ib_bars_to_dataframe(bars: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    for bar in bars:
        raw_ts = bar.get("ts_end") or bar.get("time")
        if raw_ts is None:
            continue
        try:
            ts = pd.Timestamp(raw_ts)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
        except (ValueError, TypeError):
            try:
                ts = pd.Timestamp(int(raw_ts), unit="s", tz="UTC")
            except Exception:
                continue
        ts_et = ts.tz_convert("US/Eastern")
        ts_index = ts_et.replace(second=0, microsecond=0)
        records.append(
            {
                "ts_end": ts_index,
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": bar.get("close"),
                "volume": bar.get("volume"),
            }
        )
    if not records:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["ts_end"]).set_index("ts_end").sort_index()
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    df.index = df.index.tz_convert("US/Eastern")
    return df


def _previous_trading_date(trade_date: date) -> date:
    candidate = trade_date - timedelta(days=1)
    while True:
        try:
            session = get_trading_session(candidate)
            return session.session_date
        except KeyError:
            candidate -= timedelta(days=1)
