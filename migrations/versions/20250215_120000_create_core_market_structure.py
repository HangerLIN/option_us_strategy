"""create timeseries core tables

Revision ID: 20250215_120000
Revises:
Create Date: 2025-02-15 12:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20250215_120000"
branch_labels = None
depends_on = None
down_revision = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_trading_calendar (
            trade_date DATE PRIMARY KEY,
            is_half_day BOOLEAN NOT NULL DEFAULT FALSE,
            open_et TIME NOT NULL,
            close_et TIME NOT NULL
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bars1m_equity (
            ts_end TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            open NUMERIC(18, 6) NOT NULL,
            high NUMERIC(18, 6) NOT NULL,
            low NUMERIC(18, 6) NOT NULL,
            close NUMERIC(18, 6) NOT NULL,
            volume BIGINT NOT NULL,
            PRIMARY KEY (symbol, ts_end)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            PERFORM create_hypertable('bars1m_equity', 'ts_end', if_not_exists => TRUE);
        EXCEPTION
            WHEN undefined_function THEN
                RAISE NOTICE 'TimescaleDB not available, skipping hypertable conversion for bars1m_equity';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bars1m_equity_ts_end ON bars1m_equity (ts_end DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bars1m_equity_ts_end due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bars1m_equity_symbol_ts ON bars1m_equity (symbol, ts_end DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_bars1m_equity_symbol_ts due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS indicators_eq_1m (
            ts_end TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            rsi6 NUMERIC(18, 6),
            rsi12 NUMERIC(18, 6),
            rsi24 NUMERIC(18, 6),
            boll_mid NUMERIC(18, 6),
            boll_up NUMERIC(18, 6),
            boll_dn NUMERIC(18, 6),
            atr14 NUMERIC(18, 6),
            ao NUMERIC(18, 6),
            stoch_k NUMERIC(18, 6),
            stoch_d NUMERIC(18, 6),
            stoch_rsi_k NUMERIC(18, 6),
            stoch_rsi_d NUMERIC(18, 6),
            cci14 NUMERIC(18, 6),
            cci6 NUMERIC(18, 6),
            sma5 NUMERIC(18, 6),
            lr_m5_slope NUMERIC(18, 6),
            lr_boll_dn_slope NUMERIC(18, 6),
            lr_obv_slope NUMERIC(18, 6),
            obv NUMERIC(24, 6),
            obv_ma6 NUMERIC(18, 6),
            obv_ema20 NUMERIC(18, 6),
            mfi14 NUMERIC(18, 6),
            rvol6 NUMERIC(18, 6),
            PRIMARY KEY (symbol, ts_end)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            PERFORM create_hypertable('indicators_eq_1m', 'ts_end', if_not_exists => TRUE);
        EXCEPTION
            WHEN undefined_function THEN
                RAISE NOTICE 'TimescaleDB not available, skipping hypertable conversion for indicators_eq_1m';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_indicators_eq_1m_ts ON indicators_eq_1m (ts_end DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_indicators_eq_1m_ts due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_indicators_eq_1m_symbol_ts ON indicators_eq_1m (symbol, ts_end DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_indicators_eq_1m_symbol_ts due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS premarket_top5 (
            trade_date DATE NOT NULL,
            rank INT NOT NULL,
            symbol TEXT NOT NULL,
            ret_0925_0930 NUMERIC(18, 6),
            PRIMARY KEY (trade_date, rank)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_premarket_top5_symbol ON premarket_top5 (symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_premarket_top5_symbol due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS rvol_baseline_eq (
            symbol TEXT NOT NULL,
            minute_index INT NOT NULL,
            mean_vol_20d NUMERIC(18, 6) NOT NULL,
            PRIMARY KEY (symbol, minute_index)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_rvol_baseline_symbol ON rvol_baseline_eq (symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_rvol_baseline_symbol due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        DO $migration$
        BEGIN
            BEGIN
                EXECUTE $sql$
                CREATE MATERIALIZED VIEW IF NOT EXISTS v_daily_ohlcv
                WITH (timescaledb.continuous) AS
                SELECT
                    time_bucket('1 day', ts_end, 'America/New_York') AS bucket,
                    symbol,
                    first(open, ts_end) AS open,
                    max(high) AS high,
                    min(low) AS low,
                    last(close, ts_end) AS close,
                    sum(volume) AS volume
                FROM bars1m_equity
                GROUP BY bucket, symbol
                WITH NO DATA;
                $sql$;
            EXCEPTION
                WHEN undefined_function THEN
                    RAISE NOTICE 'TimescaleDB not available, skipping v_daily_ohlcv materialized view';
            END;
        END $migration$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS idx_v_daily_ohlcv_bucket_symbol ON v_daily_ohlcv (bucket, symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_v_daily_ohlcv_bucket_symbol due to insufficient privileges';
            WHEN undefined_table THEN
                RAISE NOTICE 'Skipping idx_v_daily_ohlcv_bucket_symbol because v_daily_ohlcv is unavailable';
        END $$;
        """
    )

    op.execute(
        """
        DO $migration$
        BEGIN
            BEGIN
                EXECUTE $sql$
                CREATE MATERIALIZED VIEW IF NOT EXISTS v_daily_atr_pct
                WITH (timescaledb.continuous) AS
                SELECT
                    time_bucket('1 day', ind.ts_end, 'America/New_York') AS bucket,
                    ind.symbol,
                    last(ind.atr14, ind.ts_end) AS atr14,
                    last(b.close, b.ts_end) AS close,
                    CASE
                        WHEN last(b.close, b.ts_end) = 0 THEN NULL
                        ELSE last(ind.atr14, ind.ts_end) / last(b.close, b.ts_end)
                    END AS atr_pct
                FROM indicators_eq_1m AS ind
                JOIN bars1m_equity AS b
                  ON b.symbol = ind.symbol
                 AND b.ts_end = ind.ts_end
                GROUP BY bucket, ind.symbol
                WITH NO DATA;
                $sql$;
            EXCEPTION
                WHEN undefined_function THEN
                    RAISE NOTICE 'TimescaleDB not available, skipping v_daily_atr_pct materialized view';
            END;
        END $migration$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS idx_v_daily_atr_bucket_symbol ON v_daily_atr_pct (bucket, symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_v_daily_atr_bucket_symbol due to insufficient privileges';
            WHEN undefined_table THEN
                RAISE NOTICE 'Skipping idx_v_daily_atr_bucket_symbol because v_daily_atr_pct is unavailable';
        END $$;
        """
    )

    op.execute(
        """
        DO $migration$
        BEGIN
            BEGIN
                EXECUTE 'CALL refresh_continuous_aggregate(''v_daily_ohlcv''::regclass, NULL, NULL);';
            EXCEPTION
                WHEN undefined_function THEN
                    RAISE NOTICE 'TimescaleDB not available, skipping v_daily_ohlcv refresh';
                WHEN undefined_table THEN
                    RAISE NOTICE 'Skipping v_daily_ohlcv refresh because materialized view is unavailable';
            END;
            BEGIN
                EXECUTE 'CALL refresh_continuous_aggregate(''v_daily_atr_pct''::regclass, NULL, NULL);';
            EXCEPTION
                WHEN undefined_function THEN
                    RAISE NOTICE 'TimescaleDB not available, skipping v_daily_atr_pct refresh';
                WHEN undefined_table THEN
                    RAISE NOTICE 'Skipping v_daily_atr_pct refresh because materialized view is unavailable';
            END;
        END $migration$;
        """
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS v_daily_atr_pct;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS v_daily_ohlcv;")

    op.execute("DROP TABLE IF EXISTS rvol_baseline_eq;")
    op.execute("DROP TABLE IF EXISTS premarket_top5;")
    op.execute("DROP TABLE IF EXISTS indicators_eq_1m;")
    op.execute("DROP TABLE IF EXISTS bars1m_equity;")
    op.execute("DROP TABLE IF EXISTS dim_trading_calendar;")
