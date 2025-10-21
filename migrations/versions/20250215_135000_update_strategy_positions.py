"""ensure strategy_positions schema matches ORM

Revision ID: 20250215_135000
Revises: 20250215_130000
Create Date: 2025-02-15 13:50:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20250215_135000"
branch_labels = None
depends_on = "20250215_130000"
down_revision = "20250215_130000"


def upgrade() -> None:
    op.execute(
        """
        DO $migration$
        BEGIN
            BEGIN
                EXECUTE $sql$
                CREATE TABLE IF NOT EXISTS strategy_positions (
                    id BIGSERIAL PRIMARY KEY,
                    strategy_code TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    option_right TEXT NOT NULL,
                    open_quantity INTEGER NOT NULL DEFAULT 0,
                    avg_open_price NUMERIC(18, 6) NOT NULL,
                    mark_price NUMERIC(18, 6) NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                $sql$;
            EXCEPTION
                WHEN insufficient_privilege THEN
                    RAISE NOTICE 'Skipping strategy_positions creation due to insufficient privileges';
            END;
        END $migration$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_strategy_positions_symbol ON strategy_positions (symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_strategy_positions_symbol due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        DO $migration$
        BEGIN
            BEGIN
                EXECUTE 'ALTER TABLE strategy_positions
                    ADD COLUMN IF NOT EXISTS source_signal_code TEXT,
                    ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS conid BIGINT,
                    ADD COLUMN IF NOT EXISTS strike NUMERIC(18, 6),
                    ADD COLUMN IF NOT EXISTS expiry DATE,
                    ADD COLUMN IF NOT EXISTS delta NUMERIC(10, 6),
                    ALTER COLUMN updated_at SET DEFAULT now()';
            EXCEPTION
                WHEN insufficient_privilege THEN
                    RAISE NOTICE 'Skipping strategy_positions column alterations due to insufficient privileges';
            END;
        END $migration$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE strategy_positions
            DROP COLUMN IF EXISTS source_signal_code,
            DROP COLUMN IF EXISTS opened_at,
            DROP COLUMN IF EXISTS conid,
            DROP COLUMN IF EXISTS strike,
            DROP COLUMN IF EXISTS expiry,
            DROP COLUMN IF EXISTS delta;
        """
    )
