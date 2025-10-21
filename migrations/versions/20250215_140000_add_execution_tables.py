"""add execution and pnl tables

Revision ID: 20250215_140000
Revises: 20250215_135000
Create Date: 2025-02-15 14:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20250215_140000"
branch_labels = None
depends_on = "20250215_135000"
down_revision = "20250215_135000"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id BIGSERIAL PRIMARY KEY,
            client_order_id TEXT NOT NULL,
            strategy_code TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity NUMERIC(18, 6) NOT NULL,
            limit_price NUMERIC(18, 6),
            status TEXT NOT NULL,
            rejection_code TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_client_order_id ON orders (client_order_id)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping uq_orders_client_order_id due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_orders_symbol_status ON orders (symbol, status, created_at DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_orders_symbol_status due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS fills (
            fill_id BIGSERIAL PRIMARY KEY,
            order_id BIGINT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
            symbol TEXT NOT NULL,
            fill_ts TIMESTAMPTZ NOT NULL,
            fill_price NUMERIC(18, 6) NOT NULL,
            fill_quantity NUMERIC(18, 6) NOT NULL,
            execution_venue TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fills_order_id_ts ON fills (order_id, fill_ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_fills_order_id_ts due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fills_symbol_ts ON fills (symbol, fill_ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_fills_symbol_ts due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            strategy_code TEXT NOT NULL,
            symbol TEXT NOT NULL,
            quantity NUMERIC(18, 6) NOT NULL,
            avg_price NUMERIC(18, 6) NOT NULL,
            unrealized_pnl NUMERIC(18, 6) DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (strategy_code, symbol)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions (symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_positions_symbol due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pnl_intraday (
            ts TIMESTAMPTZ NOT NULL,
            strategy_code TEXT NOT NULL,
            symbol TEXT NOT NULL,
            realized NUMERIC(18, 6) DEFAULT 0,
            unrealized NUMERIC(18, 6) DEFAULT 0,
            fees NUMERIC(18, 6) DEFAULT 0,
            PRIMARY KEY (strategy_code, symbol, ts)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            PERFORM create_hypertable('pnl_intraday', 'ts', if_not_exists => TRUE);
        EXCEPTION
            WHEN undefined_function THEN
                RAISE NOTICE 'TimescaleDB not available, skipping hypertable conversion for pnl_intraday';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_pnl_intraday_symbol_ts ON pnl_intraday (symbol, ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_pnl_intraday_symbol_ts due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_pnl_intraday_strategy_ts ON pnl_intraday (strategy_code, ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_pnl_intraday_strategy_ts due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS pnl_daily (
            trade_date DATE NOT NULL,
            strategy_code TEXT NOT NULL,
            symbol TEXT NOT NULL,
            realized NUMERIC(18, 6) DEFAULT 0,
            unrealized NUMERIC(18, 6) DEFAULT 0,
            fees NUMERIC(18, 6) DEFAULT 0,
            PRIMARY KEY (trade_date, strategy_code, symbol)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_pnl_daily_symbol ON pnl_daily (symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_pnl_daily_symbol due to insufficient privileges';
        END $$;
        """
    )

    # CRUD smoke tests with cleanup
    op.execute(
        """
        WITH inserted AS (
            INSERT INTO orders (client_order_id, strategy_code, symbol, side, order_type, quantity, status)
            VALUES ('TEST-COID', 'core-vol', 'AAPL', 'BUY', 'LMT', 100, 'NEW')
            RETURNING order_id
        )
        DELETE FROM orders WHERE order_id IN (SELECT order_id FROM inserted);
        """
    )

    op.execute(
        """
        WITH created_order AS (
            INSERT INTO orders (client_order_id, strategy_code, symbol, side, order_type, quantity, status)
            VALUES ('TEST-COID-FILL', 'core-vol', 'MSFT', 'BUY', 'MKT', 50, 'PARTIAL')
            RETURNING order_id
        ), inserted_fill AS (
            INSERT INTO fills (order_id, symbol, fill_ts, fill_price, fill_quantity)
            SELECT order_id, 'MSFT', now(), 150.25, 25 FROM created_order
            RETURNING fill_id, order_id
        )
        DELETE FROM fills WHERE fill_id IN (SELECT fill_id FROM inserted_fill);
        """
    )
    op.execute("DELETE FROM orders WHERE client_order_id = 'TEST-COID-FILL';")

    op.execute(
        """
        WITH inserted AS (
            INSERT INTO positions (strategy_code, symbol, quantity, avg_price)
            VALUES ('core-vol', 'TSLA', 10, 210.5)
            RETURNING strategy_code, symbol
        )
        DELETE FROM positions WHERE (strategy_code, symbol) IN (SELECT strategy_code, symbol FROM inserted);
        """
    )

    op.execute(
        """
        WITH inserted AS (
            INSERT INTO pnl_intraday (ts, strategy_code, symbol, realized, unrealized, fees)
            VALUES (now(), 'core-vol', 'TSLA', 100, 50, 0)
            RETURNING ts, strategy_code, symbol
        )
        DELETE FROM pnl_intraday
        WHERE (strategy_code, symbol, ts) IN
            (SELECT strategy_code, symbol, ts FROM inserted);
        """
    )

    op.execute(
        """
        WITH inserted AS (
            INSERT INTO pnl_daily (trade_date, strategy_code, symbol, realized, unrealized, fees)
            VALUES (current_date, 'core-vol', 'TSLA', 200, 80, 0)
            RETURNING trade_date, strategy_code, symbol
        )
        DELETE FROM pnl_daily
        WHERE (trade_date, strategy_code, symbol) IN
            (SELECT trade_date, strategy_code, symbol FROM inserted);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS pnl_daily;")
    op.execute("DROP TABLE IF EXISTS pnl_intraday;")
    op.execute("DROP TABLE IF EXISTS positions;")
    op.execute("DROP TABLE IF EXISTS fills;")
    op.execute("DROP TABLE IF EXISTS orders;")
