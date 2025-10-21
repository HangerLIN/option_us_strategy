"""add market cap tables and daily views

Revision ID: 20250215_160000
Revises: 20250215_150000
Create Date: 2025-02-15 16:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20250215_160000"
branch_labels = None
depends_on = "20250215_150000"
down_revision = "20250215_150000"

_MARKET_CAP_WHITELIST = [
    "NVDA",
    "MSFT",
    "AAPL",
    "GOOGL",
    "AMZN",
    "META",
    "AVGO",
    "TSM",
    "TSLA",
    "ORCL",
    "NFLX",
    "PLTR",
    "ASML",
    "CSCO",
    "AMD",
    "CRM",
    "DIS",
    "UBER",
    "SHOP",
    "NOW",
    "INTU",
    "ANET",
    "QCOM",
    "TXN",
    "APP",
    "ADBE",
    "ARM",
    "MU",
    "PANW",
    "LRCX",
    "AMAT",
    "ADI",
    "KLAC",
    "INTC",
    "CRWD",
    "CDNS",
    "SNPS",
    "DELL",
    "COIN",
    "EQIX",
    "SNOW",
    "NET",
    "WDAY",
    "FTNT",
    "NXPI",
    "MRVL",
    "DDOG",
    "VEEV",
    "TEAM",
    "ZS",
    "MCHP",
    "HPE",
    "AFRM",
    "MDB",
    "ZM",
    "HUBS",
    "STM",
    "CYBR",
    "ON",
    "GFS",
    "U",
    "RBRK",
    "TWLO",
    "OKTA",
    "DT",
    "GTLB",
    "CFLT",
]


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ref_market_cap (
            symbol TEXT PRIMARY KEY,
            market_cap_usd NUMERIC NOT NULL,
            source TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS compliance_whitelist_largecap (
            symbol TEXT PRIMARY KEY,
            min_market_cap_usd NUMERIC DEFAULT 100000000000
        );
        """
    )

    for symbol in _MARKET_CAP_WHITELIST:
        op.execute(
            f"""
            INSERT INTO compliance_whitelist_largecap(symbol)
            VALUES ('{symbol}')
            ON CONFLICT (symbol) DO NOTHING;
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
                    time_bucket_ng('1 day', ts_end, 'America/New_York') AS trade_date_et,
                    symbol,
                    first(close, ts_end) FILTER (
                        WHERE (ts_end AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
                          AND (ts_end AT TIME ZONE 'America/New_York')::time <  TIME '16:00'
                    ) AS open_rth,
                    max(high) FILTER (
                        WHERE (ts_end AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
                          AND (ts_end AT TIME ZONE 'America/New_York')::time <  TIME '16:00'
                    ) AS high_rth,
                    min(low) FILTER (
                        WHERE (ts_end AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
                          AND (ts_end AT TIME ZONE 'America/New_York')::time <  TIME '16:00'
                    ) AS low_rth,
                    last(close, ts_end) FILTER (
                        WHERE (ts_end AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
                          AND (ts_end AT TIME ZONE 'America/New_York')::time <  TIME '16:00'
                    ) AS close_rth
                FROM bars1m_equity
                GROUP BY 1, symbol
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
            EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS idx_v_daily_ohlcv_symbol_date ON v_daily_ohlcv (trade_date_et, symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_v_daily_ohlcv_symbol_date due to insufficient privileges';
            WHEN undefined_table THEN
                RAISE NOTICE 'Skipping idx_v_daily_ohlcv_symbol_date because v_daily_ohlcv is unavailable';
        END $$;
        """
    )

    op.execute(
        """
        DO $migration$
        BEGIN
            IF to_regclass('v_daily_ohlcv') IS NULL THEN
                RAISE NOTICE 'Skipping v_daily_ohlcv_enriched creation because base view is unavailable';
            ELSE
                EXECUTE $$
                    CREATE OR REPLACE VIEW v_daily_ohlcv_enriched AS
                    SELECT
                        trade_date_et,
                        symbol,
                        open_rth,
                        high_rth,
                        low_rth,
                        close_rth,
                        lag(close_rth) OVER (PARTITION BY symbol ORDER BY trade_date_et) AS prev_close_rth
                    FROM v_daily_ohlcv;
                $$;
            END IF;
        END $migration$;
        """
    )

    op.execute(
        """
        DO $migration$
        BEGIN
            IF to_regclass('v_daily_ohlcv_enriched') IS NULL THEN
                RAISE NOTICE 'Skipping v_daily_ma60 creation because enriched view is unavailable';
            ELSE
                EXECUTE $$
                    CREATE OR REPLACE VIEW v_daily_ma60 AS
                    SELECT
                        d.trade_date_et,
                        d.symbol,
                        d.open_rth,
                        d.high_rth,
                        d.low_rth,
                        d.close_rth,
                        d.prev_close_rth,
                        avg(d.close_rth) OVER (
                            PARTITION BY symbol
                            ORDER BY trade_date_et
                            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                        ) AS sma60,
                        (
                            avg(d.close_rth) OVER (
                                PARTITION BY symbol
                                ORDER BY trade_date_et
                                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                            )
                            -
                            avg(d.close_rth) OVER (
                                PARTITION BY symbol
                                ORDER BY trade_date_et
                                ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
                            )
                        ) AS sma60_slope
                    FROM v_daily_ohlcv_enriched d;
                $$;
            END IF;
        END $migration$;
        """
    )

    op.execute(
        """
        DO $migration$
        BEGIN
            BEGIN
                IF to_regclass('v_daily_ohlcv') IS NOT NULL THEN
                    EXECUTE 'CALL refresh_continuous_aggregate(''v_daily_ohlcv''::regclass, NULL, NULL);';
                ELSE
                    RAISE NOTICE 'Skipping v_daily_ohlcv refresh because view is unavailable';
                END IF;
            EXCEPTION
                WHEN undefined_function THEN
                    RAISE NOTICE 'TimescaleDB not available, skipping v_daily_ohlcv refresh';
            END;
        END $migration$;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS v_daily_ma60;")
    op.execute("DROP VIEW IF EXISTS v_daily_ohlcv_enriched;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS v_daily_ohlcv;")
    op.execute("DROP TABLE IF EXISTS compliance_whitelist_largecap;")
    op.execute("DROP TABLE IF EXISTS ref_market_cap;")
