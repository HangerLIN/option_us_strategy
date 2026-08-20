"""add starter asset type and calibration registry tables

Revision ID: 20251127_120000
Revises: 20251126_120000
Create Date: 2025-11-27 12:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20251127_120000"
down_revision = "20251126_120000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table_name in (
        "strategy_positions",
        "orders",
        "fills",
        "positions",
        "pnl_intraday",
        "pnl_daily",
        "bt_trades",
        "bt_signals",
        "bt_order_events",
    ):
        op.execute(
            f"""
            ALTER TABLE IF EXISTS {table_name}
                ADD COLUMN IF NOT EXISTS asset_type TEXT NOT NULL DEFAULT 'OPTION';
            """
        )

    op.execute(
        """
        ALTER TABLE IF EXISTS bt_runs
            ADD COLUMN IF NOT EXISTS strategy_version TEXT,
            ADD COLUMN IF NOT EXISTS calibration_version TEXT,
            ADD COLUMN IF NOT EXISTS data_window_start TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS data_window_end TIMESTAMPTZ;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.strategy_positions') IS NOT NULL THEN
                ALTER TABLE strategy_positions ALTER COLUMN option_right DROP NOT NULL;
            END IF;
        EXCEPTION
            WHEN undefined_column THEN
                RAISE NOTICE 'strategy_positions.option_right missing; skip nullable change';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.positions') IS NOT NULL THEN
                ALTER TABLE positions DROP CONSTRAINT IF EXISTS positions_pkey;
                ALTER TABLE positions
                    ADD CONSTRAINT positions_pkey
                    PRIMARY KEY (strategy_code, asset_type, symbol);
            END IF;

            IF to_regclass('public.pnl_intraday') IS NOT NULL THEN
                ALTER TABLE pnl_intraday DROP CONSTRAINT IF EXISTS pnl_intraday_pkey;
                ALTER TABLE pnl_intraday
                    ADD CONSTRAINT pnl_intraday_pkey
                    PRIMARY KEY (ts, strategy_code, asset_type, symbol);
            END IF;

            IF to_regclass('public.pnl_daily') IS NOT NULL THEN
                ALTER TABLE pnl_daily DROP CONSTRAINT IF EXISTS pnl_daily_pkey;
                ALTER TABLE pnl_daily
                    ADD CONSTRAINT pnl_daily_pkey
                    PRIMARY KEY (trade_date, strategy_code, asset_type, symbol);
            END IF;
        END $$;
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_positions_instrument
            ON strategy_positions (
                strategy_code,
                symbol,
                asset_type,
                COALESCE(option_right::text, '')
            );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS calibration_runs (
            calibration_id BIGSERIAL PRIMARY KEY,
            strategy_code TEXT NOT NULL,
            calibration_version TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'COMPLETED',
            train_start DATE,
            train_end DATE,
            validation_start DATE,
            validation_end DATE,
            test_start DATE,
            test_end DATE,
            objective_metric TEXT,
            maximize BOOLEAN NOT NULL DEFAULT TRUE,
            metadata JSONB,
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT ux_calibration_runs_strategy_version
                UNIQUE (strategy_code, calibration_version)
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS calibration_params (
            param_id BIGSERIAL PRIMARY KEY,
            calibration_id BIGINT NOT NULL
                REFERENCES calibration_runs(calibration_id) ON DELETE CASCADE,
            param_name TEXT NOT NULL,
            param_value JSONB NOT NULL,
            param_group TEXT NOT NULL DEFAULT 'strategy'
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS calibration_metrics (
            metric_id BIGSERIAL PRIMARY KEY,
            calibration_id BIGINT NOT NULL
                REFERENCES calibration_runs(calibration_id) ON DELETE CASCADE,
            metric_name TEXT NOT NULL,
            metric_value NUMERIC(18, 6) NOT NULL,
            metric_scope TEXT NOT NULL DEFAULT 'validation'
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS calibration_artifacts (
            artifact_id BIGSERIAL PRIMARY KEY,
            calibration_id BIGINT NOT NULL
                REFERENCES calibration_runs(calibration_id) ON DELETE CASCADE,
            artifact_type TEXT NOT NULL,
            uri TEXT NOT NULL,
            metadata JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_calibration_params_run_name
            ON calibration_params (calibration_id, param_name);
        CREATE INDEX IF NOT EXISTS ix_calibration_metrics_run_name
            ON calibration_metrics (calibration_id, metric_name);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS calibration_artifacts;")
    op.execute("DROP TABLE IF EXISTS calibration_metrics;")
    op.execute("DROP TABLE IF EXISTS calibration_params;")
    op.execute("DROP TABLE IF EXISTS calibration_runs;")
    op.execute(
        """
        ALTER TABLE IF EXISTS bt_runs
            DROP COLUMN IF EXISTS data_window_end,
            DROP COLUMN IF EXISTS data_window_start,
            DROP COLUMN IF EXISTS calibration_version,
            DROP COLUMN IF EXISTS strategy_version;
        """
    )
    op.execute("DROP INDEX IF EXISTS ux_strategy_positions_instrument;")
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.positions') IS NOT NULL THEN
                ALTER TABLE positions DROP CONSTRAINT IF EXISTS positions_pkey;
                ALTER TABLE positions
                    ADD CONSTRAINT positions_pkey PRIMARY KEY (strategy_code, symbol);
            END IF;

            IF to_regclass('public.pnl_intraday') IS NOT NULL THEN
                ALTER TABLE pnl_intraday DROP CONSTRAINT IF EXISTS pnl_intraday_pkey;
                ALTER TABLE pnl_intraday
                    ADD CONSTRAINT pnl_intraday_pkey PRIMARY KEY (strategy_code, symbol, ts);
            END IF;

            IF to_regclass('public.pnl_daily') IS NOT NULL THEN
                ALTER TABLE pnl_daily DROP CONSTRAINT IF EXISTS pnl_daily_pkey;
                ALTER TABLE pnl_daily
                    ADD CONSTRAINT pnl_daily_pkey PRIMARY KEY (trade_date, strategy_code, symbol);
            END IF;
        END $$;
        """
    )
    for table_name in (
        "bt_order_events",
        "bt_signals",
        "bt_trades",
        "pnl_daily",
        "pnl_intraday",
        "positions",
        "fills",
        "orders",
        "strategy_positions",
    ):
        op.execute(f"ALTER TABLE IF EXISTS {table_name} DROP COLUMN IF EXISTS asset_type;")
