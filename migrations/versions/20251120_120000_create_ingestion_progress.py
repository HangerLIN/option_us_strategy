"""create ingestion progress table

Revision ID: 20251120_120000
Revises: 20251115_120000
Create Date: 2025-11-20 12:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20251120_120000"
down_revision = "20251115_120000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS data_ingestion_progress (
            ingestion_id TEXT NOT NULL,
            trade_date DATE NOT NULL,
            symbol TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            bars_count INTEGER NOT NULL DEFAULT 0,
            indicators_count INTEGER NOT NULL DEFAULT 0,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (ingestion_id, trade_date, symbol),
            CONSTRAINT ck_data_ingestion_progress_status
                CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED'))
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_data_ingestion_progress_status
            ON data_ingestion_progress (status);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_data_ingestion_progress_symbol_date
            ON data_ingestion_progress (symbol, trade_date DESC);
        """
    )
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_ingestion_daily_completion;")
    op.execute(
        """
        CREATE MATERIALIZED VIEW mv_ingestion_daily_completion AS
        SELECT
            ingestion_id,
            trade_date,
            COUNT(*)::INTEGER AS total_symbols,
            COUNT(*) FILTER (WHERE status = 'COMPLETED')::INTEGER AS completed_symbols,
            COUNT(*) FILTER (WHERE status = 'FAILED')::INTEGER AS failed_symbols,
            BOOL_AND(status = 'COMPLETED') AS is_day_complete,
            MAX(updated_at) AS updated_at
        FROM data_ingestion_progress
        GROUP BY ingestion_id, trade_date
        WITH DATA;
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_ingestion_daily_completion
            ON mv_ingestion_daily_completion (ingestion_id, trade_date);
        """
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_ingestion_daily_completion;")
    op.execute("DROP TABLE IF EXISTS data_ingestion_progress;")
