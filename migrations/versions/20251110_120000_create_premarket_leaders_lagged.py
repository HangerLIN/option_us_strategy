"""create premarket leaders lagged table

Revision ID: 20251110_120000
Revises: 20251107_110000
Create Date: 2025-11-10 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20251110_120000"
down_revision = "20251107_110000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "premarket_leaders_lagged",
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("source_date", sa.Date(), nullable=False),
        sa.Column("direction", sa.String(length=8), nullable=False),
        sa.Column("rank", sa.SmallInteger(), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("ret_0928", sa.Numeric(18, 6), nullable=False),
        sa.Column("volume_rth", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("trade_date", "source_date", "direction", "rank"),
    )
    op.create_index(
        "ix_premarket_leaders_lagged_trade_date",
        "premarket_leaders_lagged",
        ["trade_date"],
    )
    op.create_index(
        "ix_premarket_leaders_lagged_symbol",
        "premarket_leaders_lagged",
        ["symbol"],
    )


def downgrade() -> None:
    op.drop_index("ix_premarket_leaders_lagged_symbol", table_name="premarket_leaders_lagged")
    op.drop_index("ix_premarket_leaders_lagged_trade_date", table_name="premarket_leaders_lagged")
    op.drop_table("premarket_leaders_lagged")
