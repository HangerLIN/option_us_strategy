"""add bt_top5 table for backtest premarket rankings"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251101090000"
down_revision = "20251016122214"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bt_top5",
        sa.Column("batch_id", sa.String(length=64), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("rank", sa.SmallInteger(), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("ret_0925_0930", sa.Numeric(18, 6), nullable=False),
        sa.Column("price_0925", sa.Numeric(18, 6), nullable=True),
        sa.Column("prev_close", sa.Numeric(18, 6), nullable=True),
        sa.Column("market_cap_usd", sa.Numeric(20, 2), nullable=True),
        sa.Column("m60_up", sa.Boolean(), nullable=True),
        sa.Column("preearn_allowed", sa.Boolean(), nullable=True),
        sa.Column("source", sa.String(length=128), nullable=True),
        sa.Column("context", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("batch_id", "trade_date", "rank"),
        sa.UniqueConstraint("batch_id", "trade_date", "symbol"),
    )
    op.create_index(
        "idx_bt_top5_trade_date_batch",
        "bt_top5",
        ["trade_date", "batch_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_bt_top5_trade_date_batch", table_name="bt_top5")
    op.drop_table("bt_top5")
