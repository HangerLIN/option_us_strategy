"""align backtest tables with runtime DAO

Revision ID: 20251125_120000
Revises: 20251120_120000
Create Date: 2025-11-25 12:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20251125_120000"
down_revision = "20251120_120000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE IF EXISTS bt_trades
            ADD COLUMN IF NOT EXISTS trace_id TEXT,
            ADD COLUMN IF NOT EXISTS option_right TEXT,
            ADD COLUMN IF NOT EXISTS strike NUMERIC(18, 6),
            ADD COLUMN IF NOT EXISTS expiry DATE,
            ADD COLUMN IF NOT EXISTS fees NUMERIC(18, 6) NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS slippage NUMERIC(18, 6) NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS reason_code TEXT;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.bt_signals') IS NULL THEN
                RETURN;
            END IF;

            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'bt_signals'
                  AND column_name = 'signal_ts'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'bt_signals'
                  AND column_name = 'ts_end'
            ) THEN
                ALTER TABLE bt_signals RENAME COLUMN signal_ts TO ts_end;
            ELSIF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'bt_signals'
                  AND column_name = 'ts_end'
            ) THEN
                ALTER TABLE bt_signals ADD COLUMN ts_end TIMESTAMPTZ;
            END IF;

            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'bt_signals'
                  AND column_name = 'signal_ts'
            ) THEN
                UPDATE bt_signals
                   SET ts_end = COALESCE(ts_end, signal_ts)
                 WHERE ts_end IS NULL;
                ALTER TABLE bt_signals DROP COLUMN signal_ts;
            END IF;

            UPDATE bt_signals SET ts_end = now() WHERE ts_end IS NULL;
            ALTER TABLE bt_signals ALTER COLUMN ts_end SET NOT NULL;
        END $$;
        """
    )
    op.execute("ALTER TABLE IF EXISTS bt_signals ADD COLUMN IF NOT EXISTS accepted BOOLEAN;")
    op.execute("ALTER TABLE IF EXISTS bt_signals ADD COLUMN IF NOT EXISTS reason TEXT;")
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bt_signals_ts ON bt_signals (ts_end DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bt_signals_ts due to insufficient privileges';
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE IF EXISTS bt_trades
            DROP COLUMN IF EXISTS reason_code,
            DROP COLUMN IF EXISTS slippage,
            DROP COLUMN IF EXISTS fees,
            DROP COLUMN IF EXISTS expiry,
            DROP COLUMN IF EXISTS strike,
            DROP COLUMN IF EXISTS option_right,
            DROP COLUMN IF EXISTS trace_id;
        """
    )
    op.execute("ALTER TABLE IF EXISTS bt_signals DROP COLUMN IF EXISTS reason;")
    op.execute("ALTER TABLE IF EXISTS bt_signals DROP COLUMN IF EXISTS accepted;")
