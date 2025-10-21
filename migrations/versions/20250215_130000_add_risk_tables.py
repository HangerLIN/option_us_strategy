"""add risk management tables

Revision ID: 20250215_130000
Revises: 20250215_120000
Create Date: 2025-02-15 13:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20250215_130000"
branch_labels = None
depends_on = "20250215_120000"
down_revision = "20250215_120000"


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_limits (
            limit_id BIGSERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            limit_code TEXT NOT NULL,
            limit_value NUMERIC(18, 6) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS uq_risk_limits_symbol_code ON risk_limits (symbol, limit_code)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping uq_risk_limits_symbol_code due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_risk_limits_symbol ON risk_limits (symbol)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_risk_limits_symbol due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_state (
            ts TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            metric_code TEXT NOT NULL,
            metric_value NUMERIC(18, 6) NOT NULL,
            detail JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, metric_code, ts)
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            PERFORM create_hypertable('risk_state', 'ts', if_not_exists => TRUE);
        EXCEPTION
            WHEN undefined_function THEN
                RAISE NOTICE 'TimescaleDB not available, skipping hypertable conversion for risk_state';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_risk_state_symbol_ts ON risk_state (symbol, ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_risk_state_symbol_ts due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_risk_state_metric_ts ON risk_state (metric_code, ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_risk_state_metric_ts due to insufficient privileges';
        END $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_events (
            event_id BIGSERIAL PRIMARY KEY,
            event_ts TIMESTAMPTZ NOT NULL,
            symbol TEXT,
            event_code TEXT NOT NULL,
            severity TEXT NOT NULL DEFAULT 'INFO',
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_risk_events_ts ON risk_events (event_ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_risk_events_ts due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_risk_events_symbol_ts ON risk_events (symbol, event_ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_risk_events_symbol_ts due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_risk_events_code_ts ON risk_events (event_code, event_ts DESC)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_risk_events_code_ts due to insufficient privileges';
        END $$;
        """
    )

    # Smoke-test insert/selection capability and ensure cleanup
    op.execute(
        """
        WITH inserted AS (
            INSERT INTO risk_limits (symbol, limit_code, limit_value)
            VALUES ('TEST', 'NOTIONAL', 1000000)
            RETURNING limit_id
        )
        DELETE FROM risk_limits WHERE limit_id IN (SELECT limit_id FROM inserted);
        """
    )
    op.execute(
        """
        WITH inserted AS (
            INSERT INTO risk_state (ts, symbol, metric_code, metric_value)
            VALUES (now(), 'TEST', 'NET_NOTIONAL', 12345.67)
            RETURNING ts, symbol, metric_code
        )
        DELETE FROM risk_state
        WHERE (symbol, metric_code, ts) IN (SELECT symbol, metric_code, ts FROM inserted);
        """
    )
    op.execute(
        """
        WITH inserted AS (
            INSERT INTO risk_events (event_ts, symbol, event_code, severity, payload)
            VALUES (now(), 'TEST', 'LIMIT_BREACH', 'INFO', '{"detail": "demo"}'::jsonb)
            RETURNING event_id
        )
        DELETE FROM risk_events WHERE event_id IN (SELECT event_id FROM inserted);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS risk_events;")
    op.execute("DROP TABLE IF EXISTS risk_state;")
    op.execute("DROP TABLE IF EXISTS risk_limits;")
