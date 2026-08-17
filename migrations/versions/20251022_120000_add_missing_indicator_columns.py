"""add_missing_indicator_columns

Revision ID: 20251022_120000
Revises: 20251101_090000
Create Date: 2025-10-22 12:00:00

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251022_120000'
down_revision = '20251016122214'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """添加 indicators_eq_1m 表中缺失的指标列"""
    op.execute(
        """
        ALTER TABLE indicators_eq_1m
            ADD COLUMN IF NOT EXISTS stoch_rsi_k DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS stoch_rsi_d DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS sma5 DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS lr_m5_slope DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS lr_boll_dn_slope DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS lr_obv_slope DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS obv_ma6 DOUBLE PRECISION;
        """
    )


def downgrade() -> None:
    """移除添加的指标列"""
    op.drop_column('indicators_eq_1m', 'stoch_rsi_k')
    op.drop_column('indicators_eq_1m', 'stoch_rsi_d')
    op.drop_column('indicators_eq_1m', 'sma5')
    op.drop_column('indicators_eq_1m', 'lr_m5_slope')
    op.drop_column('indicators_eq_1m', 'lr_boll_dn_slope')
    op.drop_column('indicators_eq_1m', 'lr_obv_slope')
    op.drop_column('indicators_eq_1m', 'obv_ma6')
