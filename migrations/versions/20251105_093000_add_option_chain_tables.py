"""create option chain metadata and option bar tables

Revision ID: 20251105_093000
Revises: 20251101_090000_add_bt_top5_table
Create Date: 2025-11-05 09:30:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20251105_093000"
branch_labels = None
depends_on = "20251101_090000_add_bt_top5_table"
down_revision = "20251101_090000_add_bt_top5_table"


def upgrade() -> None:
    # Option chain metadata table ------------------------------------------------
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS option_chain_meta (
            trade_date DATE NOT NULL,
            underlying_symbol TEXT NOT NULL,
            conid BIGINT NOT NULL,
            expiry DATE NOT NULL,
            right TEXT NOT NULL,
            strike NUMERIC(18, 6) NOT NULL,
            trading_class TEXT,
            multiplier TEXT,
            exchange TEXT,
            dte SMALLINT,
            delta NUMERIC(18, 6),
            gamma NUMERIC(18, 6),
            theta NUMERIC(18, 6),
            vega NUMERIC(18, 6),
            implied_vol NUMERIC(18, 6),
            bid NUMERIC(18, 6),
            ask NUMERIC(18, 6),
            mid NUMERIC(18, 6),
            open_interest BIGINT,
            volume BIGINT,
            underlying_price NUMERIC(18, 6),
            min_tick NUMERIC(10, 6),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        DO $$
        DECLARE
            pk_name text;
        BEGIN
            SELECT tc.constraint_name INTO pk_name
            FROM information_schema.table_constraints tc
            WHERE tc.table_schema = 'public'
              AND tc.table_name = 'option_chain_meta'
              AND tc.constraint_type = 'PRIMARY KEY';
            IF pk_name IS NOT NULL THEN
                EXECUTE format('ALTER TABLE option_chain_meta DROP CONSTRAINT %I', pk_name);
            END IF;
            EXECUTE 'ALTER TABLE option_chain_meta ADD CONSTRAINT option_chain_meta_pkey PRIMARY KEY (trade_date, conid)';
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'trading_class'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN trading_class TEXT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'multiplier'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN multiplier TEXT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'exchange'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN exchange TEXT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'dte'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN dte SMALLINT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'delta'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN delta NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'gamma'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN gamma NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'theta'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN theta NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'vega'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN vega NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'implied_vol'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN implied_vol NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'bid'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN bid NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'ask'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN ask NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'mid'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN mid NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'open_interest'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN open_interest BIGINT';
            END IF;
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'oi'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta RENAME COLUMN oi TO open_interest';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'volume'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN volume BIGINT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'underlying_price'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN underlying_price NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'min_tick'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN min_tick NUMERIC(10, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'created_at'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now()';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'option_chain_meta' AND column_name = 'updated_at'
            ) THEN
                EXECUTE 'ALTER TABLE option_chain_meta ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now()';
            ELSE
                EXECUTE 'ALTER TABLE option_chain_meta ALTER COLUMN updated_at SET DEFAULT now()';
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            BEGIN
                EXECUTE 'ALTER TABLE option_chain_meta ALTER COLUMN bid TYPE NUMERIC(18, 6) USING bid::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE option_chain_meta ALTER COLUMN ask TYPE NUMERIC(18, 6) USING ask::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE option_chain_meta ALTER COLUMN mid TYPE NUMERIC(18, 6) USING mid::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE option_chain_meta ALTER COLUMN delta TYPE NUMERIC(18, 6) USING delta::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE option_chain_meta ALTER COLUMN strike TYPE NUMERIC(18, 6) USING strike::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE option_chain_meta ALTER COLUMN underlying_price TYPE NUMERIC(18, 6) USING underlying_price::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_option_chain_meta_symbol_date ON option_chain_meta (underlying_symbol, trade_date)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_option_chain_meta_symbol_date due to insufficient privileges';
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_option_chain_meta_trade_date_right ON option_chain_meta (trade_date, right)';
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'Skipping idx_option_chain_meta_trade_date_right due to insufficient privileges';
        END $$;
        """
    )

    # Option minute bar table ----------------------------------------------------
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bars1m_option (
            conid BIGINT NOT NULL,
            ts_end TIMESTAMPTZ NOT NULL,
            underlying_symbol TEXT NOT NULL,
            expiry DATE NOT NULL,
            right TEXT NOT NULL,
            strike NUMERIC(18, 6) NOT NULL,
            trading_class TEXT,
            multiplier TEXT,
            exchange TEXT,
            bid NUMERIC(18, 6),
            ask NUMERIC(18, 6),
            mid NUMERIC(18, 6),
            last NUMERIC(18, 6),
            volume BIGINT,
            open_interest BIGINT,
            implied_vol NUMERIC(18, 6),
            delta NUMERIC(18, 6),
            gamma NUMERIC(18, 6),
            theta NUMERIC(18, 6),
            vega NUMERIC(18, 6),
            underlying_price NUMERIC(18, 6),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    op.execute(
        """
        DO $$
        DECLARE
            pk_name text;
        BEGIN
            SELECT tc.constraint_name INTO pk_name
            FROM information_schema.table_constraints tc
            WHERE tc.table_schema = 'public'
              AND tc.table_name = 'bars1m_option'
              AND tc.constraint_type = 'PRIMARY KEY';
            IF pk_name IS NOT NULL THEN
                EXECUTE format('ALTER TABLE bars1m_option DROP CONSTRAINT %I', pk_name);
            END IF;
            EXECUTE 'ALTER TABLE bars1m_option ADD CONSTRAINT bars1m_option_pkey PRIMARY KEY (conid, ts_end)';
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'oi'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'open_interest'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option RENAME COLUMN oi TO open_interest';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'open_interest'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN open_interest BIGINT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'mid'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN mid NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'trading_class'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN trading_class TEXT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'multiplier'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN multiplier TEXT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'exchange'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN exchange TEXT';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'delta'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN delta NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'implied_vol'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN implied_vol NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'last'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN last NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'gamma'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN gamma NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'theta'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN theta NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'vega'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN vega NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'underlying_price'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN underlying_price NUMERIC(18, 6)';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'created_at'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now()';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'updated_at'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now()';
            ELSE
                EXECUTE 'ALTER TABLE bars1m_option ALTER COLUMN updated_at SET DEFAULT now()';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'bars1m_option' AND column_name = 'volume'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD COLUMN volume BIGINT';
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            BEGIN
                EXECUTE 'ALTER TABLE bars1m_option ALTER COLUMN bid TYPE NUMERIC(18, 6) USING bid::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE bars1m_option ALTER COLUMN ask TYPE NUMERIC(18, 6) USING ask::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE bars1m_option ALTER COLUMN mid TYPE NUMERIC(18, 6) USING mid::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE bars1m_option ALTER COLUMN delta TYPE NUMERIC(18, 6) USING delta::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
            BEGIN
                EXECUTE 'ALTER TABLE bars1m_option ALTER COLUMN strike TYPE NUMERIC(18, 6) USING strike::numeric';
            EXCEPTION
                WHEN undefined_column THEN NULL;
            END;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE table_schema = 'public'
                  AND table_name = 'bars1m_option'
                  AND constraint_name = 'bars1m_option_minute_chk'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option DROP CONSTRAINT bars1m_option_minute_chk';
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.table_constraints
                WHERE table_schema = 'public'
                  AND table_name = 'bars1m_option'
                  AND constraint_name = 'ck_bars1m_option_minute'
            ) THEN
                EXECUTE 'ALTER TABLE bars1m_option ADD CONSTRAINT ck_bars1m_option_minute CHECK (date_trunc(''minute'', ts_end) = ts_end)';
            END IF;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            BEGIN
                EXECUTE 'CREATE INDEX IF NOT EXISTS idx_bars1m_option_symbol_ts ON bars1m_option (underlying_symbol, ts_end)';
            EXCEPTION
                WHEN insufficient_privilege THEN
                    RAISE NOTICE 'Skipping idx_bars1m_option_symbol_ts due to insufficient privileges';
            END;
        END $$;
        """
    )

    op.execute(
        """
        DO $$
        BEGIN
            BEGIN
                PERFORM create_hypertable('bars1m_option', 'ts_end', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');
            EXCEPTION
                WHEN undefined_function THEN
                    RAISE NOTICE 'TimescaleDB create_hypertable function not available';
                WHEN duplicate_object THEN
                    NULL;
                WHEN insufficient_privilege THEN
                    RAISE NOTICE 'Insufficient privilege to create hypertable for bars1m_option';
            END;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bars1m_option;")
    op.execute("DROP TABLE IF EXISTS option_chain_meta;")
