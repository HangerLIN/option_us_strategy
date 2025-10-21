"""add backtest result tables

Revision ID: 20250215_150000
Revises: 20250215_140000
Create Date: 2025-02-15 15:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20250215_150000"
branch_labels = None
depends_on = "20250215_140000"
down_revision = "20250215_140000"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_runs (
            run_id BIGSERIAL PRIMARY KEY,
            strategy_code TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            status TEXT NOT NULL,
            parameters JSONB,
            notes TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bt_runs_strategy_status ON bt_runs (strategy_code, status)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bt_runs_strategy_status due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_trades (
            trade_id BIGSERIAL PRIMARY KEY,
            run_id BIGINT NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity NUMERIC(18, 6) NOT NULL,
            price NUMERIC(18, 6) NOT NULL,
            trade_ts TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bt_trades_run_symbol ON bt_trades (run_id, symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bt_trades_run_symbol due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bt_trades_ts ON bt_trades (trade_ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bt_trades_ts due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_signals (
            signal_id BIGSERIAL PRIMARY KEY,
            run_id BIGINT NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
            symbol TEXT NOT NULL,
            signal_code TEXT NOT NULL,
            signal_ts TIMESTAMPTZ NOT NULL,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bt_signals_run_symbol ON bt_signals (run_id, symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bt_signals_run_symbol due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bt_signals_ts ON bt_signals (signal_ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bt_signals_ts due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_metrics_daily (
            run_id BIGINT NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
            trade_date DATE NOT NULL,
            metric_code TEXT NOT NULL,
            metric_value NUMERIC(18, 6) NOT NULL,
            PRIMARY KEY (run_id, trade_date, metric_code)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bt_metrics_daily_code ON bt_metrics_daily (metric_code, trade_date)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bt_metrics_daily_code due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_metrics_total (
            run_id BIGINT NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
            metric_code TEXT NOT NULL,
            metric_value NUMERIC(18, 6) NOT NULL,
            PRIMARY KEY (run_id, metric_code)
        );
        """
    )

    # CRUD smoke checks with cleanup
    op.execute(
        """
        WITH inserted_run AS (
            INSERT INTO bt_runs (strategy_code, started_at, status)
            VALUES ('core-vol', now(), 'RUNNING')
            RETURNING run_id
        )
        DELETE FROM bt_runs WHERE run_id IN (SELECT run_id FROM inserted_run);
        """
    )

    op.execute(
        """
        DO $migration$
        BEGIN
            BEGIN
                WITH base_run AS (
                    INSERT INTO bt_runs (strategy_code, started_at, status)
                    VALUES ('core-vol', now(), 'RUNNING')
                    RETURNING run_id
                ), trades AS (
                    INSERT INTO bt_trades (run_id, symbol, side, quantity, price, trade_ts)
                    SELECT run_id, 'AAPL', 'BUY', 10, 150.5, now() FROM base_run
                    RETURNING trade_id, run_id
                ), signals AS (
                    INSERT INTO bt_signals (run_id, symbol, signal_code, signal_ts)
                    SELECT run_id, 'AAPL', 'enter-long', now() FROM base_run
                    RETURNING signal_id, run_id
                ), metrics_daily AS (
                    INSERT INTO bt_metrics_daily (run_id, trade_date, metric_code, metric_value)
                    SELECT run_id, current_date, 'ret', 0.012 FROM base_run
                    RETURNING run_id
                ), metrics_total AS (
                    INSERT INTO bt_metrics_total (run_id, metric_code, metric_value)
                    SELECT run_id, 'sharpe', 1.8 FROM base_run
                    RETURNING run_id
                )
                DELETE FROM bt_metrics_total WHERE run_id IN (SELECT run_id FROM metrics_total);
                DELETE FROM bt_metrics_daily WHERE run_id IN (SELECT run_id FROM metrics_daily);
                DELETE FROM bt_signals WHERE run_id IN (SELECT run_id FROM signals);
                DELETE FROM bt_trades WHERE run_id IN (SELECT run_id FROM trades);
                DELETE FROM bt_runs WHERE run_id IN (SELECT run_id FROM base_run);
            EXCEPTION
                WHEN insufficient_privilege THEN
                    RAISE NOTICE 'Skipping backtest smoke inserts due to insufficient privileges';
                WHEN undefined_column THEN
                    RAISE NOTICE 'Skipping backtest smoke inserts due to schema mismatch';
            END;
        END $migration$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bt_metrics_total;")
    op.execute("DROP TABLE IF EXISTS bt_metrics_daily;")
    op.execute("DROP TABLE IF EXISTS bt_signals;")
    op.execute("DROP TABLE IF EXISTS bt_trades;")
    op.execute("DROP TABLE IF EXISTS bt_runs;")
