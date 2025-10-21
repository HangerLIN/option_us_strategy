"""create earnings calendar table"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251015153738"
down_revision = "20250215_160000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "earnings_calendar",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("earnings_date", sa.Date(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=True),
        sa.Column("confidence", sa.String(length=16), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), server_onupdate=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("symbol", "earnings_date"),
    )


def downgrade() -> None:
    op.drop_table("earnings_calendar")
