from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timedelta, timezone, time as dt_time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping
from uuid import uuid4

import httpx
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from apps.backtest.bt_runner import run_backtest
from apps.backtest.dao import BacktestDAO
from apps.rt_engine.indicators import compute_indicator_row
from apps.rt_engine.top5_service import Top5Service
from libs.core import EASTERN, configure_logging, get_settings, utc_now
from libs.infra import build_ibkr_client


def _print(msg: str) -> None:
    print(f"[demo] {msg}", flush=True)


def _http_get(url: str, *, timeout: float = 10.0) -> Dict[str, Any]:
    with httpx.Client(timeout=timeout) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


def _http_post(url: str, payload: Dict[str, Any], *, timeout: float = 10.0) -> Dict[str, Any]:
    with httpx.Client(timeout=timeout) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        return response.json()


def _ib_time_to_utc(value: str) -> datetime:
    dt_et = datetime.strptime(value, "%Y%m%d %H:%M:%S").replace(tzinfo=EASTERN)
    return (dt_et + timedelta(minutes=1)).astimezone(timezone.utc)


SECONDS_IN_YEAR = 365 * 24 * 60 * 60
RISK_FREE_RATE = float(os.environ.get("OPTION_RISK_FREE_RATE", "0.0"))


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)


def _option_d1_d2(
    underlying: float,
    strike: float,
    ttm: float,
    sigma: float,
    risk_free_rate: float,
) -> tuple[float, float]:
    denom = sigma * math.sqrt(ttm)
    d1 = (math.log(underlying / strike) + (risk_free_rate + 0.5 * sigma * sigma) * ttm) / denom
    d2 = d1 - denom
    return d1, d2


def _black_scholes_price(
    option_right: str,
    underlying: float,
    strike: float,
    ttm: float,
    sigma: float,
    risk_free_rate: float,
) -> float:
    d1, d2 = _option_d1_d2(underlying, strike, ttm, sigma, risk_free_rate)
    discount = math.exp(-risk_free_rate * ttm)
    if option_right == "CALL":
        return underlying * _norm_cdf(d1) - strike * discount * _norm_cdf(d2)
    return strike * discount * _norm_cdf(-d2) - underlying * _norm_cdf(-d1)


def _implied_volatility(
    option_right: str,
    price: float,
    underlying: float,
    strike: float,
    ttm: float,
    risk_free_rate: float,
) -> float | None:
    if price <= 0 or underlying <= 0 or strike <= 0 or ttm <= 0:
        return None

    intrinsic = (
        max(underlying - strike, 0.0) if option_right == "CALL" else max(strike - underlying, 0.0)
    )
    if price <= intrinsic:
        return None

    low = 1e-4
    high = 5.0

    def _price_at(vol: float) -> float:
        return _black_scholes_price(option_right, underlying, strike, ttm, vol, risk_free_rate)

    while _price_at(high) < price and high < 10.0:
        high *= 2

    implied = None
    for _ in range(100):
        mid = 0.5 * (low + high)
        estimate = _price_at(mid)
        if abs(estimate - price) < 1e-4:
            implied = mid
            break
        if estimate > price:
            high = mid
        else:
            low = mid
    if implied is None:
        mid = 0.5 * (low + high)
        estimate = _price_at(mid)
        if abs(estimate - price) < 5e-4:
            implied = mid
    return implied


def _compute_option_metrics(
    option_right: str,
    price: float | None,
    underlying: float | None,
    strike: float,
    expiry: datetime,
    ts_end: datetime,
    risk_free_rate: float,
) -> dict[str, float | None]:
    defaults: dict[str, float | None] = {
        "iv": None,
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
    }
    if price is None or price <= 0:
        return defaults
    if underlying is None or underlying <= 0 or strike <= 0:
        return defaults

    expiry_dt = expiry
    if isinstance(expiry_dt, datetime):
        if expiry_dt.tzinfo is None:
            expiry_dt = expiry_dt.replace(tzinfo=EASTERN)
    else:  # pragma: no cover - defensive
        expiry_dt = datetime.combine(expiry_dt, dt_time(hour=16, minute=0), tzinfo=EASTERN)
    expiry_dt = expiry_dt.replace(hour=16, minute=0, second=0, microsecond=0)
    expiry_utc = expiry_dt.astimezone(timezone.utc)
    ttm_seconds = (expiry_utc - ts_end).total_seconds()
    if ttm_seconds <= 0:
        return defaults
    ttm = ttm_seconds / SECONDS_IN_YEAR

    option_side = option_right.upper()
    implied = _implied_volatility(option_side, price, underlying, strike, ttm, risk_free_rate)
    if implied is None or implied <= 0:
        return defaults

    denom = implied * math.sqrt(ttm)
    if denom <= 0:
        return defaults

    d1, d2 = _option_d1_d2(underlying, strike, ttm, implied, risk_free_rate)
    pdf_d1 = _norm_pdf(d1)
    discount = math.exp(-risk_free_rate * ttm)

    if option_side == "CALL":
        delta = _norm_cdf(d1)
        theta = (-underlying * pdf_d1 * implied) / (
            2 * math.sqrt(ttm)
        ) - risk_free_rate * strike * discount * _norm_cdf(d2)
    else:
        delta = _norm_cdf(d1) - 1
        theta = (-underlying * pdf_d1 * implied) / (
            2 * math.sqrt(ttm)
        ) + risk_free_rate * strike * discount * _norm_cdf(-d2)

    gamma = pdf_d1 / (underlying * denom)
    vega = underlying * pdf_d1 * math.sqrt(ttm) / 100.0
    theta_per_day = theta / 365.0

    return {
        "iv": implied,
        "delta": delta,
        "gamma": gamma,
        "theta": theta_per_day,
        "vega": vega,
    }


def _prepare_equity_records(symbol: str, bars: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for bar in bars:
        ts_end = _ib_time_to_utc(str(bar.get("time")))
        records.append(
            {
                "symbol": symbol,
                "ts_end": ts_end,
                "open": Decimal(str(bar.get("open", 0)) or "0"),
                "high": Decimal(str(bar.get("high", 0)) or "0"),
                "low": Decimal(str(bar.get("low", 0)) or "0"),
                "close": Decimal(str(bar.get("close", 0)) or "0"),
                "volume": int(bar.get("volume", 0) or 0),
            }
        )
    records.sort(key=lambda item: item["ts_end"])
    return records


def _load_baseline_map(session, symbol: str) -> dict[int, Decimal]:
    rows = session.execute(
        text("SELECT minute_index, mean_vol_20d FROM rvol_baseline_eq WHERE symbol = :symbol"),
        {"symbol": symbol},
    ).all()
    return {row[0]: Decimal(str(row[1])) for row in rows}


def _upsert_indicator_row(
    session, symbol: str, ts_end: datetime, baseline_map: Mapping[int, Decimal]
) -> None:
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

    frame = pd.DataFrame(
        [
            {
                "ts_end": row.ts_end,
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "volume": float(row.volume),
            }
            for row in rows
        ]
    )
    frame["ts_end"] = pd.to_datetime(frame["ts_end"], utc=True)
    frame.set_index("ts_end", inplace=True)
    frame.sort_index(inplace=True)

    indicators = compute_indicator_row(frame, baseline_map)
    if not indicators:
        return

    payload = {
        key: (float(value) if value is not None else None) for key, value in indicators.items()
    }
    payload.update({"ts_end": ts_end, "symbol": symbol})

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


def _store_equity_data(
    session_factory, symbol: str, records: list[dict[str, Any]]
) -> dict[datetime, Decimal]:
    if not records:
        raise RuntimeError("No equity bars to persist")

    insert_sql = text(
        """
        INSERT INTO bars1m_equity (ts_end, symbol, open, high, low, close, volume)
        VALUES (:ts_end, :symbol, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, ts_end) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
    )

    with session_factory() as session:
        for record in records:
            session.execute(
                insert_sql,
                {
                    "ts_end": record["ts_end"],
                    "symbol": symbol,
                    "open": record["open"],
                    "high": record["high"],
                    "low": record["low"],
                    "close": record["close"],
                    "volume": record["volume"],
                },
            )
        session.commit()

    with session_factory() as session:
        baseline_map = _load_baseline_map(session, symbol)
        for record in records:
            _upsert_indicator_row(session, symbol, record["ts_end"], baseline_map)
        session.commit()

    return {record["ts_end"]: record["close"] for record in records}


def _select_option_candidate(
    dao: BacktestDAO, symbol: str, ts_end_utc: datetime, option_right: str
) -> dict[str, Any]:
    trade_date = ts_end_utc.astimezone(EASTERN).date()
    rows = dao.fetch_option_candidates(
        trade_date=trade_date,
        underlying_symbol=symbol,
        option_right=option_right,
        dte_min=2,
        dte_max=7,
    )

    filtered: list[dict[str, Any]] = []
    for candidate in rows:
        bid = candidate.bid
        ask = candidate.ask
        mid = candidate.mid
        oi = candidate.open_interest
        vol = candidate.volume
        min_tick = candidate.min_tick
        dte = candidate.dte
        strike_value = candidate.strike
        if None in (bid, ask, mid, oi, vol, min_tick, dte, strike_value):
            continue
        bid_d = Decimal(str(bid))
        ask_d = Decimal(str(ask))
        mid_d = Decimal(str(mid))
        spread = ask_d - bid_d
        if ask_d <= bid_d or spread <= 0:
            continue
        if Decimal(str(oi)) < Decimal("500") or Decimal(str(vol)) < Decimal("100"):
            continue
        threshold = max(Decimal("0.10"), mid_d * Decimal("0.05"))
        if spread > threshold:
            continue
        expiry = candidate.expiry
        if expiry is None:
            continue
        if not isinstance(expiry, datetime):
            expiry = datetime.combine(expiry, datetime.min.time())
        filtered.append(
            {
                "conid": candidate.conid,
                "underlying_symbol": candidate.underlying_symbol or symbol,
                "expiry": expiry,
                "strike": Decimal(str(strike_value)),
                "option_right": option_right,
                "bid": bid_d,
                "ask": ask_d,
                "mid": mid_d,
                "open_interest": int(candidate.open_interest),
                "volume": int(candidate.volume),
                "min_tick": Decimal(str(min_tick)),
                "dte": int(dte),
            }
        )

    if not filtered:
        raise RuntimeError("未找到符合条件的期权合约")

    filtered.sort(key=lambda item: (item["ask"] - item["bid"], item["mid"]))
    return filtered[0]


def _compose_option_records(
    contract,
    candidate: Mapping[str, Any],
    bars_trade: Iterable[Mapping[str, Any]],
    bars_bid: Iterable[Mapping[str, Any]],
    bars_ask: Iterable[Mapping[str, Any]],
    equity_close_map: Mapping[datetime, Decimal],
) -> list[dict[str, Any]]:
    bid_map = {
        _ib_time_to_utc(str(bar.get("time"))): Decimal(str(bar.get("close", 0) or "0"))
        for bar in bars_bid
    }
    ask_map = {
        _ib_time_to_utc(str(bar.get("time"))): Decimal(str(bar.get("close", 0) or "0"))
        for bar in bars_ask
    }
    trade_map = {
        _ib_time_to_utc(str(bar.get("time"))): Decimal(str(bar.get("close", 0) or "0"))
        for bar in bars_trade
    }
    volume_map = {
        _ib_time_to_utc(str(bar.get("time"))): int(bar.get("volume", 0) or 0) for bar in bars_trade
    }

    timestamps = sorted(set(bid_map) & set(ask_map))
    records: list[dict[str, Any]] = []
    conid = contract.conId or candidate.get("conid")
    if conid is None:
        raise RuntimeError("合约缺少 conId")

    for ts in timestamps:
        bid = bid_map[ts]
        ask = ask_map[ts]
        mid = (bid + ask) / Decimal("2")
        last = trade_map.get(ts, mid)
        volume = volume_map.get(ts, 0)
        und_price = equity_close_map.get(ts)
        metrics = _compute_option_metrics(
            candidate["option_right"],
            float(last),
            float(und_price) if und_price is not None else None,
            float(candidate["strike"]),
            candidate["expiry"],
            ts,
            RISK_FREE_RATE,
        )
        records.append(
            {
                "conid": int(conid),
                "underlying_symbol": contract.symbol,
                "expiry": candidate["expiry"].date(),
                "right": candidate["option_right"],
                "strike": float(candidate["strike"]),
                "ts_end": ts,
                "bid": float(bid),
                "ask": float(ask),
                "mid": float(mid),
                "volume": volume,
                "open_interest": candidate["open_interest"],
                "implied_vol": float(metrics["iv"]) if metrics["iv"] is not None else None,
                "delta": float(metrics["delta"]) if metrics["delta"] is not None else None,
                "gamma": float(metrics["gamma"]) if metrics["gamma"] is not None else None,
                "theta": float(metrics["theta"]) if metrics["theta"] is not None else None,
                "vega": float(metrics["vega"]) if metrics["vega"] is not None else None,
                "underlying_price": float(und_price) if und_price is not None else None,
            }
        )
    return records


def _store_option_records(session_factory, records: list[dict[str, Any]]) -> None:
    if not records:
        raise RuntimeError("未获取到期权 K 线数据")

    insert_sql = text(
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

    with session_factory() as session:
        for record in records:
            session.execute(insert_sql, record)
        session.commit()


def main() -> int:
    settings = get_settings()
    configure_logging(settings)

    log_dir = Path(os.environ.get("DEMO_LOG_DIR", "logs"))
    artifacts_dir = Path(os.environ.get("DEMO_ARTIFACT_DIR", "artifacts"))
    log_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    _print("Running IBKR self-check")
    client = build_ibkr_client(settings)
    try:
        client.run_selfcheck()
    finally:
        client.disconnect_and_stop()

    engine = create_engine(settings.database_url, future=True)
    SessionLocal = sessionmaker(bind=engine, future=True)

    _print("Running Premarket Top5 scan")
    client = build_ibkr_client(settings)
    top5_service = Top5Service(client, SessionLocal, settings)
    try:
        top5_service.run_for_today()
    finally:
        client.disconnect_and_stop()

    with SessionLocal() as session:
        row = session.execute(
            text(
                """
                SELECT symbol
                FROM premarket_top5
                ORDER BY trade_date DESC, rank ASC
                LIMIT 1
                """
            )
        ).fetchone()

    if not row or not row[0]:
        raise SystemExit("Top5 未返回可用标的")

    symbol = row[0].upper()
    _print(f"Using symbol: {symbol}")

    client = build_ibkr_client(settings)
    try:
        end_utc = datetime.now(timezone.utc)
        start_utc = end_utc - timedelta(hours=3)
        start_et = start_utc.astimezone(EASTERN)
        end_et = end_utc.astimezone(EASTERN)

        _print("Fetching equity historical bars")
        equity_bars = client.req_historical_1m(symbol, start_et, end_et, use_rth=True)
        equity_records = _prepare_equity_records(symbol, equity_bars)
        if not equity_records:
            raise SystemExit("未获取到股票 K 线")
        equity_close_map = _store_equity_data(SessionLocal, symbol, equity_records)

        last_record = equity_records[-1]
        last_ts_end = last_record["ts_end"]
        bar_start = last_record["ts_end"] - timedelta(minutes=1)
        trace_id = f"demo-{uuid4().hex}"
        _print(f"Trace ID: {trace_id}")

        bars_event = {
            "trace_id": trace_id,
            "symbol": symbol,
            "bar_start": bar_start.isoformat(),
            "bar_end": last_record["ts_end"].isoformat(),
            "timeframe": "1m",
            "open": float(last_record["open"]),
            "high": float(last_record["high"]),
            "low": float(last_record["low"]),
            "close": float(last_record["close"]),
            "volume": last_record["volume"],
            "vwap": None,
            "source": "demo",
            "received_at": utc_now().isoformat(),
        }

        _print("Submitting bar to signal service")
        signal_response = _http_post("http://127.0.0.1:8000/engine/process", bars_event)
        signals = signal_response.get("data") or []
        if not signals:
            raise SystemExit("信号服务未返回任何信号")
        signal_payload = signals[0]
        _print(f"Signal generated: {signal_payload['signal_code']}")

        side = str(signal_payload.get("side") or "BUY").upper()
        option_right = "CALL" if side == "BUY" else "PUT"
        with SessionLocal() as session:
            dao = BacktestDAO(
                session,
                option_bar_table=settings.option_bar_table,
                option_chain_table=settings.option_chain_table,
            )
            candidate = _select_option_candidate(dao, symbol, last_ts_end, option_right)

        _print(
            "Selected option: right=%s strike=%s expiry=%s"
            % (candidate["option_right"], candidate["strike"], candidate["expiry"].date())
        )

        option_contract = client.option_contract(
            symbol=symbol,
            expiry=candidate["expiry"].strftime("%Y%m%d"),
            strike=float(candidate["strike"]),
            right=candidate["option_right"],
            conid=int(candidate["conid"]) if candidate.get("conid") else None,
        )
        detail = client.req_contract_details(option_contract)
        option_contract = detail.contract
        if candidate.get("conid") and not option_contract.conId:
            option_contract.conId = int(candidate["conid"])

        _print("Fetching option historical bars (bid/ask/trades)")
        bars_trade = client.req_historical_1m_contract(
            option_contract, start_et, end_et, use_rth=True, what_to_show="TRADES"
        )
        bars_bid = client.req_historical_1m_contract(
            option_contract, start_et, end_et, use_rth=True, what_to_show="BID"
        )
        bars_ask = client.req_historical_1m_contract(
            option_contract, start_et, end_et, use_rth=True, what_to_show="ASK"
        )
        option_records = _compose_option_records(
            option_contract, candidate, bars_trade, bars_bid, bars_ask, equity_close_map
        )
        _store_option_records(SessionLocal, option_records)

        spread = candidate["ask"] - candidate["bid"]
        order_payload: Dict[str, Any] = {
            "strategy_code": signal_payload.get("strategy_code") or "core-vol",
            "symbol": symbol,
            "side": side,
            "quantity": 1,
            "limit_price": str(last_record["close"]),
            "tif": "DAY",
            "signal_code": signal_payload["signal_code"],
            "execution_mode": "FORCE",
            "trace_id": trace_id,
            "option_right": candidate["option_right"],
            "option_dte": candidate["dte"],
            "option_bid": str(candidate["bid"]),
            "option_ask": str(candidate["ask"]),
            "option_mid": str(candidate["mid"]),
            "option_spread": str(spread),
            "option_open_interest": candidate["open_interest"],
            "option_volume": candidate["volume"],
            "min_tick": str(candidate["min_tick"]),
        }

        _print("Submitting order to execution service")
        order_response = _http_post("http://127.0.0.1:8001/orders/submit", order_payload)
        if not order_response.get("ok"):
            raise SystemExit(f"下单失败: {order_response}")
        order_state = order_response["data"]
        order_id = order_state["order_id"]
        _print(f"Order accepted. order_id={order_id}")

        status = order_state["status"]
        for _ in range(45):
            if status in {"filled", "cancelled", "rejected"}:
                break
            time.sleep(2)
            poll = _http_get(f"http://127.0.0.1:8001/orders/{order_id}")
            status = poll["data"]["status"]
        else:
            _print("Order not completed, cancelling...")
            cancel_resp = _http_post("http://127.0.0.1:8001/orders/cancel", {"order_id": order_id})
            if not cancel_resp.get("ok"):
                raise SystemExit("撤单失败")
            status = cancel_resp["data"]["status"]

        _print(f"Final order status: {status}")

    finally:
        client.disconnect_and_stop()

    with SessionLocal() as session:
        pnl_row = session.execute(
            text(
                """
                SELECT ts, realized, fees
                FROM pnl_intraday
                WHERE symbol = :symbol
                ORDER BY ts DESC
                LIMIT 1
                """
            ),
            {"symbol": symbol},
        ).fetchone()

    if not pnl_row:
        raise SystemExit("PnL 表未产生记录")
    _print(f"PnL snapshot: realized={pnl_row.realized} fees={pnl_row.fees}")

    backtest_start = last_record["ts_end"] - timedelta(minutes=30)
    backtest_end = last_record["ts_end"] + timedelta(minutes=5)
    _print(f"Running backtest from {backtest_start} to {backtest_end}")
    run_id = run_backtest(
        symbol=symbol,
        start=backtest_start,
        end=backtest_end,
        signal_mode="db",
        risk_mode="http",
    )
    _print(f"Backtest run_id={run_id}")

    metrics_response = _http_get(f"http://127.0.0.1:8004/metrics/{run_id}")
    _print(f"Backtest metrics: {json.dumps(metrics_response.get('data', {}), default=str)}")

    with SessionLocal() as session:
        events = session.execute(
            text(
                """
                SELECT event_ts, event_code, action, severity, payload
                FROM risk_events
                WHERE trace_id = :trace_id
                ORDER BY event_ts
                """
            ),
            {"trace_id": trace_id},
        ).all()
        indicators = session.execute(
            text(
                """
                SELECT ts_end, rsi6, boll_mid, ao, stoch_k, stoch_d
                FROM indicators_eq_1m
                WHERE symbol = :symbol
                ORDER BY ts_end DESC
                LIMIT 20
                """
            ),
            {"symbol": symbol},
        ).all()

    events_path = artifacts_dir / f"risk_events_{trace_id}.json"
    events_payload = [
        {
            "event_ts": row.event_ts.isoformat(),
            "event_code": row.event_code,
            "action": row.action,
            "severity": row.severity,
            "payload": row.payload,
        }
        for row in events
    ]
    events_path.write_text(json.dumps(events_payload, indent=2, default=str), encoding="utf-8")

    indicators_path = artifacts_dir / f"indicator_snapshots_{trace_id}.json"
    indicators_payload = [
        {
            "ts_end": row.ts_end.isoformat(),
            "rsi6": float(row.rsi6) if row.rsi6 is not None else None,
            "boll_mid": float(row.boll_mid) if row.boll_mid is not None else None,
            "ao": float(row.ao) if row.ao is not None else None,
            "stoch_k": float(row.stoch_k) if row.stoch_k is not None else None,
            "stoch_d": float(row.stoch_d) if row.stoch_d is not None else None,
        }
        for row in indicators
    ]
    indicators_path.write_text(
        json.dumps(indicators_payload, indent=2, default=str), encoding="utf-8"
    )

    _print(f"Risk events exported to {events_path}")
    _print(f"Indicator snapshots exported to {indicators_path}")
    _print("Demo pipeline completed successfully")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
