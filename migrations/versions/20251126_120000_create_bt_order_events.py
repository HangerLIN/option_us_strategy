"""create backtest order lifecycle event table"""

from __future__ import annotations

from alembic import op


revision = "20251126_120000"
down_revision = "20251125_120000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_order_events (
            event_id BIGSERIAL PRIMARY KEY,
            run_id BIGINT NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
            trace_id TEXT,
            symbol TEXT NOT NULL,
            signal_code TEXT,
            side TEXT,
            event_type TEXT NOT NULL,
            reason_code TEXT,
            signal_time TIMESTAMPTZ,
            order_submit_time TIMESTAMPTZ,
            fill_time TIMESTAMPTZ,
            signal_to_fill_seconds NUMERIC(18, 6),
            order_ttl_seconds INTEGER,
            is_stale_fill BOOLEAN,
            order_attempts INTEGER,
            execution_mode TEXT,
            original_limit_price NUMERIC(18, 6),
            final_limit_price NUMERIC(18, 6),
            fill_price NUMERIC(18, 6),
            option_bid_at_signal NUMERIC(18, 6),
            option_ask_at_signal NUMERIC(18, 6),
            option_mid_at_signal NUMERIC(18, 6),
            option_bid_at_fill NUMERIC(18, 6),
            option_ask_at_fill NUMERIC(18, 6),
            option_mid_at_fill NUMERIC(18, 6),
            option_spread_abs_at_fill NUMERIC(18, 6),
            option_spread_pct_at_fill NUMERIC(18, 6),
            underlying_price_at_signal NUMERIC(18, 6),
            underlying_price_at_fill NUMERIC(18, 6),
            vwap_at_signal NUMERIC(18, 6),
            vwap_at_fill NUMERIC(18, 6),
            opening_range_high NUMERIC(18, 6),
            opening_range_low NUMERIC(18, 6),
            above_vwap_at_signal BOOLEAN,
            above_vwap_at_fill BOOLEAN,
            above_orh_at_signal BOOLEAN,
            above_orh_at_fill BOOLEAN,
            stale_reject_reason TEXT,
            option_right TEXT,
            strike NUMERIC(18, 6),
            expiry DATE,
            quantity NUMERIC(18, 6),
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bt_order_events_run_symbol
        ON bt_order_events (run_id, symbol, event_type);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bt_order_events_trace
        ON bt_order_events (trace_id);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bt_order_events;")
