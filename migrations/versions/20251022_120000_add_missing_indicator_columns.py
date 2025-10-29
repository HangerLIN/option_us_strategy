"""add_missing_indicator_columns

Revision ID: 20251022_120000
Revises: 20251101_090000
Create Date: 2025-10-22 12:00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251022_120000'
down_revision = '20251016122214'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """添加 indicators_eq_1m 表中缺失的指标列"""
    
    # 添加 Stochastic RSI 指标
    op.add_column('indicators_eq_1m', 
                  sa.Column('stoch_rsi_k', sa.Double(), nullable=True))
    op.add_column('indicators_eq_1m', 
                  sa.Column('stoch_rsi_d', sa.Double(), nullable=True))
    
    # 添加 SMA5 和斜率指标
    op.add_column('indicators_eq_1m', 
                  sa.Column('sma5', sa.Double(), nullable=True))
    op.add_column('indicators_eq_1m', 
                  sa.Column('lr_m5_slope', sa.Double(), nullable=True))
    op.add_column('indicators_eq_1m', 
                  sa.Column('lr_boll_dn_slope', sa.Double(), nullable=True))
    op.add_column('indicators_eq_1m', 
                  sa.Column('lr_obv_slope', sa.Double(), nullable=True))
    
    # 添加 OBV MA6 指标
    op.add_column('indicators_eq_1m', 
                  sa.Column('obv_ma6', sa.Double(), nullable=True))


def downgrade() -> None:
    """移除添加的指标列"""
    op.drop_column('indicators_eq_1m', 'stoch_rsi_k')
    op.drop_column('indicators_eq_1m', 'stoch_rsi_d')
    op.drop_column('indicators_eq_1m', 'sma5')
    op.drop_column('indicators_eq_1m', 'lr_m5_slope')
    op.drop_column('indicators_eq_1m', 'lr_boll_dn_slope')
    op.drop_column('indicators_eq_1m', 'lr_obv_slope')
    op.drop_column('indicators_eq_1m', 'obv_ma6')

