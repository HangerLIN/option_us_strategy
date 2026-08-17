"""create stock universe table

Revision ID: 20251115_120000
Revises: 20251110_120000
Create Date: 2025-11-15 12:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20251115_120000"
down_revision = "20251110_120000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_universe (
            symbol TEXT PRIMARY KEY,
            category TEXT NOT NULL DEFAULT '',
            market_cap_tier TEXT NOT NULL DEFAULT '',
            sort_order INTEGER NOT NULL DEFAULT 0,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_stock_universe_active_sort
            ON stock_universe (active, sort_order);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_stock_universe_tier
            ON stock_universe (market_cap_tier, active, sort_order);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS stock_universe;")
