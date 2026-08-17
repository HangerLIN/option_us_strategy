from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from apps.backtest.analyzers.metrics_writer import MetricsWriter


def _writer(session: Session) -> MetricsWriter:
    writer = MetricsWriter.__new__(MetricsWriter)
    writer.dao = SimpleNamespace(_session=session, _option_bar_table="bars1m_option")
    writer.run_id = 1
    return writer


def test_signal_metrics_include_option_forward_return_5m() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    with Session(engine, future=True) as session:
        session.execute(
            text(
                """
                CREATE TABLE bt_trades (
                    run_id INTEGER,
                    symbol TEXT,
                    side TEXT,
                    quantity NUMERIC,
                    price NUMERIC,
                    trade_ts TIMESTAMP,
                    option_right TEXT,
                    strike NUMERIC,
                    expiry DATE,
                    fees NUMERIC,
                    slippage NUMERIC,
                    reason_code TEXT
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE bt_signals (
                    run_id INTEGER,
                    signal_code TEXT,
                    accepted BOOLEAN
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE bars1m_option (
                    symbol TEXT,
                    "right" TEXT,
                    strike NUMERIC,
                    expiry DATE,
                    ts_end TIMESTAMP,
                    bid NUMERIC,
                    ask NUMERIC,
                    mid NUMERIC,
                    last NUMERIC
                )
                """
            )
        )

        trade_ts = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)
        expiry = date(2025, 1, 17)
        session.execute(
            text(
                """
                INSERT INTO bt_trades (
                    run_id, symbol, side, quantity, price, trade_ts,
                    option_right, strike, expiry, fees, reason_code
                ) VALUES (
                    1, 'AAPL', 'BUY', 1, 2.00, :trade_ts,
                    'CALL', 100, :expiry, 0, 'SIG_AM_BOTTOM_A1'
                )
                """
            ),
            {"trade_ts": trade_ts, "expiry": expiry},
        )
        session.execute(
            text(
                """
                INSERT INTO bt_signals (run_id, signal_code, accepted)
                VALUES (1, 'SIG_AM_BOTTOM_A1', TRUE)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO bars1m_option (
                    symbol, "right", strike, expiry, ts_end, bid, ask, mid, last
                ) VALUES (
                    'AAPL', 'CALL', 100, :expiry, :ts_end, 2.35, 2.45, 2.40, NULL
                )
                """
            ),
            {"expiry": expiry, "ts_end": trade_ts + timedelta(minutes=5)},
        )
        session.commit()

        metrics = _writer(session)._build_signal_metrics()

    by_code = {row["metric_code"]: row["metric_value"] for row in metrics}
    assert by_code["RET_SIG_AM_BOTTOM_A1_5M"] == pytest.approx(0.2)
    assert by_code["RET_SIG_AM_BOTTOM_A1_5M_P50"] == pytest.approx(0.2)
    assert by_code["RET_SIG_AM_BOTTOM_A1_5M_P90"] == pytest.approx(0.2)
    assert by_code["COUNT_EXEC_SIG_AM_BOTTOM_A1_5M"] == 1


def test_signal_metrics_skip_5m_return_when_option_bar_missing() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    with Session(engine, future=True) as session:
        session.execute(
            text(
                """
                CREATE TABLE bt_trades (
                    run_id INTEGER,
                    symbol TEXT,
                    side TEXT,
                    quantity NUMERIC,
                    price NUMERIC,
                    trade_ts TIMESTAMP,
                    option_right TEXT,
                    strike NUMERIC,
                    expiry DATE,
                    fees NUMERIC,
                    slippage NUMERIC,
                    reason_code TEXT
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE bt_signals (
                    run_id INTEGER,
                    signal_code TEXT,
                    accepted BOOLEAN
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE bars1m_option (
                    symbol TEXT,
                    "right" TEXT,
                    strike NUMERIC,
                    expiry DATE,
                    ts_end TIMESTAMP,
                    bid NUMERIC,
                    ask NUMERIC,
                    mid NUMERIC,
                    last NUMERIC
                )
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO bt_trades (
                    run_id, symbol, side, quantity, price, trade_ts,
                    option_right, strike, expiry, fees, reason_code
                ) VALUES (
                    1, 'AAPL', 'BUY', 1, 2.00, :trade_ts,
                    'CALL', 100, :expiry, 0, 'SIG_AM_BOTTOM_A1'
                )
                """
            ),
            {
                "trade_ts": datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc),
                "expiry": date(2025, 1, 17),
            },
        )
        session.commit()

        metrics = _writer(session)._build_signal_metrics()

    by_code = {row["metric_code"]: row["metric_value"] for row in metrics}
    assert by_code["RET_SIG_AM_BOTTOM_A1_5M"] == 0.0
    assert by_code["COUNT_EXEC_SIG_AM_BOTTOM_A1_5M"] == 0


def test_signal_metrics_5m_return_uses_underlying_symbol_column() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    with Session(engine, future=True) as session:
        session.execute(
            text(
                """
                CREATE TABLE bt_trades (
                    run_id INTEGER,
                    symbol TEXT,
                    side TEXT,
                    quantity NUMERIC,
                    price NUMERIC,
                    trade_ts TIMESTAMP,
                    option_right TEXT,
                    strike NUMERIC,
                    expiry DATE,
                    fees NUMERIC,
                    slippage NUMERIC,
                    reason_code TEXT
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE bt_signals (
                    run_id INTEGER,
                    signal_code TEXT,
                    accepted BOOLEAN
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE bars1m_option (
                    underlying_symbol TEXT,
                    "right" TEXT,
                    strike NUMERIC,
                    expiry DATE,
                    ts_end TIMESTAMP,
                    bid NUMERIC,
                    ask NUMERIC,
                    mid NUMERIC,
                    last NUMERIC
                )
                """
            )
        )

        trade_ts = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)
        expiry = date(2025, 1, 17)
        session.execute(
            text(
                """
                INSERT INTO bt_trades (
                    run_id, symbol, side, quantity, price, trade_ts,
                    option_right, strike, expiry, fees, reason_code
                ) VALUES (
                    1, 'AAPL', 'BUY', 1, 2.00, :trade_ts,
                    'CALL', 100, :expiry, 0, 'SIG_AM_BOTTOM_A1'
                )
                """
            ),
            {"trade_ts": trade_ts, "expiry": expiry},
        )
        session.execute(
            text(
                """
                INSERT INTO bars1m_option (
                    underlying_symbol, "right", strike, expiry, ts_end, bid, ask, mid, last
                ) VALUES (
                    'AAPL', 'CALL', 100, :expiry, :ts_end, 2.55, 2.65, 2.60, NULL
                )
                """
            ),
            {"expiry": expiry, "ts_end": trade_ts + timedelta(minutes=5)},
        )
        session.commit()

        metrics = _writer(session)._build_signal_metrics()

    by_code = {row["metric_code"]: row["metric_value"] for row in metrics}
    assert by_code["RET_SIG_AM_BOTTOM_A1_5M"] == pytest.approx(0.3)
    assert by_code["COUNT_EXEC_SIG_AM_BOTTOM_A1_5M"] == 1


def test_realized_option_metrics_expose_explicit_usd_and_return_units() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    with Session(engine, future=True) as session:
        session.execute(
            text(
                """
                CREATE TABLE bt_trades (
                    run_id INTEGER, symbol TEXT, side TEXT, quantity NUMERIC,
                    price NUMERIC, trade_ts TIMESTAMP, option_right TEXT,
                    strike NUMERIC, expiry DATE, fees NUMERIC,
                    slippage NUMERIC, reason_code TEXT
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE bt_signals (
                    run_id INTEGER, signal_code TEXT, accepted BOOLEAN
                )
                """
            )
        )
        session.execute(
            text(
                """
                CREATE TABLE bars1m_option (
                    symbol TEXT, "right" TEXT, strike NUMERIC, expiry DATE,
                    ts_end TIMESTAMP, bid NUMERIC, ask NUMERIC,
                    mid NUMERIC, last NUMERIC
                )
                """
            )
        )
        trade_ts = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)
        expiry = date(2025, 1, 17)
        session.execute(
            text(
                """
                INSERT INTO bt_trades (
                    run_id, symbol, side, quantity, price, trade_ts,
                    option_right, strike, expiry, fees, slippage, reason_code
                ) VALUES
                    (1, 'AAPL', 'BUY', 1, 2.00, :entry_ts,
                     'CALL', 100, :expiry, 1, 0.05, 'SIG_AM_BOTTOM_A1'),
                    (1, 'AAPL', 'SELL', -1, 2.50, :exit_ts,
                     'CALL', 100, :expiry, 1, 0.05, 'SIG_EXIT_TEST')
                """
            ),
            {
                "entry_ts": trade_ts,
                "exit_ts": trade_ts + timedelta(minutes=3),
                "expiry": expiry,
            },
        )
        session.execute(
            text(
                """
                INSERT INTO bt_signals (run_id, signal_code, accepted)
                VALUES (1, 'SIG_AM_BOTTOM_A1', TRUE)
                """
            )
        )
        session.commit()

        metrics = _writer(session)._build_signal_metrics()

    by_code = {row["metric_code"]: row["metric_value"] for row in metrics}
    assert by_code["PNL_USD_SIG_AM_BOTTOM_A1_MEAN"] == pytest.approx(48.0)
    assert by_code["RETURN_DECIMAL_SIG_AM_BOTTOM_A1_REALIZED_MEAN"] == pytest.approx(48.0 / 201.0)
