from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from apps.backtest.dao import BacktestDAO


def _make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    session = Session(engine, future=True)
    session.execute(
        text(
            """
            CREATE TABLE bt_runs (
                run_id INTEGER PRIMARY KEY,
                strategy_code TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status TEXT,
                notes TEXT
            )
            """
        )
    )
    return session


def test_fail_stale_running_runs_only_marks_old_running_rows() -> None:
    session = _make_session()
    now = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
    cutoff = now - timedelta(hours=24)

    session.execute(
        text(
            """
            INSERT INTO bt_runs (run_id, strategy_code, started_at, status, notes)
            VALUES
              (1, 'core-vol', :stale_ts, 'RUNNING', NULL),
              (2, 'core-vol', :fresh_ts, 'RUNNING', NULL),
              (3, 'core-vol', :stale_ts, 'COMPLETED', NULL)
            """
        ),
        {
            "stale_ts": cutoff - timedelta(hours=1),
            "fresh_ts": cutoff + timedelta(hours=1),
        },
    )
    session.commit()

    dao = BacktestDAO(
        session,
        option_bar_table="bars1m_option",
        option_chain_table="option_chain_meta",
    )
    swept = dao.fail_stale_running_runs(cutoff=cutoff, completed_at=now)
    session.commit()

    assert swept == 1
    rows = {
        run_id: (status, notes)
        for run_id, status, notes in session.execute(
            text("SELECT run_id, status, notes FROM bt_runs ORDER BY run_id")
        ).all()
    }
    assert rows[1][0] == "FAILED"
    assert rows[1][1] == "Marked FAILED by startup orphan sweep"
    assert rows[2][0] == "RUNNING"
    assert rows[3][0] == "COMPLETED"
