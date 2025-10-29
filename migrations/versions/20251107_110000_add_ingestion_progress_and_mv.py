"""create ingestion progress helpers and daily aggregates

Revision ID: 20251107_110000
Revises: 20251105_093000
Create Date: 2025-11-07 11:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20251107_110000"
down_revision = "20251105_093000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Daily OHLCV bounds continuous aggregate -----------------------------------
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('mv_daily_ohlcv_bounds') IS NOT NULL THEN
                EXECUTE 'DROP MATERIALIZED VIEW mv_daily_ohlcv_bounds CASCADE';
            END IF;
            BEGIN
                EXECUTE $BODY$
                    CREATE MATERIALIZED VIEW mv_daily_ohlcv_bounds
                    WITH (timescaledb.continuous) AS
                    SELECT
                        symbol,
                        time_bucket('1 day', ts_end, 'US/Eastern') AS bucket_start_et,
                        MIN(CASE WHEN session = 'RTH' THEN ts_end END) AS first_ts_rth,
                        MAX(CASE WHEN session = 'RTH' THEN ts_end END) AS last_ts_rth,
                        MAX(high) FILTER (WHERE session = 'RTH') AS high_rth,
                        MIN(low)  FILTER (WHERE session = 'RTH') AS low_rth
                    FROM bars1m_equity
                    GROUP BY symbol, time_bucket('1 day', ts_end, 'US/Eastern')
                    WITH NO DATA;
                $BODY$;
                EXECUTE 'CREATE INDEX IF NOT EXISTS mv_daily_ohlcv_bounds_idx ON mv_daily_ohlcv_bounds (symbol, bucket_start_et DESC)';
            EXCEPTION
                WHEN undefined_table THEN
                    RAISE NOTICE 'bars1m_equity 不存在，跳过 mv_daily_ohlcv_bounds 创建';
                WHEN undefined_function THEN
                    RAISE NOTICE '缺少 timescaledb/time_bucket 函数，跳过 mv_daily_ohlcv_bounds';
            END;
        END $$;
        """
    )

    # v_daily_ohlcv view --------------------------------------------------------
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('mv_daily_ohlcv_bounds') IS NULL THEN
                RAISE NOTICE 'mv_daily_ohlcv_bounds 不存在，跳过 v_daily_ohlcv 创建';
            ELSE
                EXECUTE $BODY$
                    CREATE OR REPLACE VIEW v_daily_ohlcv AS
                    SELECT
                        m.symbol,
                        (m.bucket_start_et AT TIME ZONE 'US/Eastern')::date AS trade_date,
                        b_open.close  AS open_rth,
                        m.high_rth    AS high_rth,
                        m.low_rth     AS low_rth,
                        b_close.close AS close_rth,
                        LAG(b_close.close) OVER (
                            PARTITION BY m.symbol
                            ORDER BY (m.bucket_start_et AT TIME ZONE 'US/Eastern')::date
                        ) AS prev_close_rth
                    FROM mv_daily_ohlcv_bounds m
                    LEFT JOIN bars1m_equity b_open
                        ON b_open.symbol = m.symbol AND b_open.ts_end = m.first_ts_rth
                    LEFT JOIN bars1m_equity b_close
                        ON b_close.symbol = m.symbol AND b_close.ts_end = m.last_ts_rth;
                $BODY$;
            END IF;
        END $$;
        """
    )

    # Latest indicator timestamp per day ----------------------------------------
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('mv_daily_ind_last') IS NOT NULL THEN
                EXECUTE 'DROP MATERIALIZED VIEW mv_daily_ind_last CASCADE';
            END IF;
            BEGIN
                EXECUTE $BODY$
                    CREATE MATERIALIZED VIEW mv_daily_ind_last
                    WITH (timescaledb.continuous) AS
                    SELECT
                        symbol,
                        time_bucket('1 day', ts_end, 'US/Eastern') AS bucket_start_et,
                        MAX(ts_end) AS last_ts
                    FROM indicators_eq_1m
                    GROUP BY symbol, time_bucket('1 day', ts_end, 'US/Eastern')
                    WITH NO DATA;
                $BODY$;
                EXECUTE 'CREATE INDEX IF NOT EXISTS mv_daily_ind_last_idx ON mv_daily_ind_last (symbol, bucket_start_et DESC)';
            EXCEPTION
                WHEN undefined_table THEN
                    RAISE NOTICE 'indicators_eq_1m 不存在，跳过 mv_daily_ind_last 创建';
                WHEN undefined_function THEN
                    RAISE NOTICE '缺少 timescaledb/time_bucket 函数，跳过 mv_daily_ind_last';
            END;
        END $$;
        """
    )

    # v_daily_atr_pct view ------------------------------------------------------
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('mv_daily_ind_last') IS NULL OR to_regclass('v_daily_ohlcv') IS NULL THEN
                RAISE NOTICE '缺少依赖视图，跳过 v_daily_atr_pct 创建';
            ELSE
                EXECUTE $BODY$
                    CREATE OR REPLACE VIEW v_daily_atr_pct AS
                    SELECT
                        d.symbol,
                        (d.bucket_start_et AT TIME ZONE 'US/Eastern')::date AS trade_date,
                        CASE
                            WHEN o.close_rth > 0 THEN ind.atr14 / o.close_rth
                        END AS atr_pct
                    FROM mv_daily_ind_last d
                    JOIN indicators_eq_1m ind
                        ON ind.symbol = d.symbol AND ind.ts_end = d.last_ts
                    JOIN v_daily_ohlcv o
                        ON o.symbol = d.symbol
                       AND o.trade_date = (d.bucket_start_et AT TIME ZONE 'US/Eastern')::date;
                $BODY$;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS v_daily_atr_pct;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_daily_ind_last CASCADE;")
    op.execute("DROP VIEW IF EXISTS v_daily_ohlcv;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_daily_ohlcv_bounds CASCADE;")
