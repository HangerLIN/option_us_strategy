"""restructure risk limits schema"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251016122214"
down_revision = "20251015153738"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("risk_limits", "risk_limits_legacy")

    op.create_table(
        "risk_limits",
        sa.Column("limit_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("scope", sa.String(length=32), nullable=False, server_default=sa.text("'global'")),
        sa.Column("symbol", sa.String(length=32), nullable=True),
        sa.Column("bucket", sa.String(length=64), nullable=True),
        sa.Column(
            "effective_from",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("updated_by", sa.String(length=64), nullable=True, server_default=sa.text("'system'")),
        sa.CheckConstraint(
            "scope <> 'symbol' OR (symbol IS NOT NULL AND trim(symbol) <> '')",
            name="ck_risk_limits_symbol_scope",
        ),
        sa.CheckConstraint(
            "scope <> 'symbol_bucket' OR (bucket IS NOT NULL AND trim(bucket) <> '')",
            name="ck_risk_limits_bucket_scope",
        ),
    )
    op.create_unique_constraint(
        "uq_risk_limits_key_scope_target",
        "risk_limits",
        ["key", "scope", "symbol", "bucket", "effective_from"],
    )
    op.create_index(
        "ix_risk_limits_key_scope",
        "risk_limits",
        ["key", "scope", "effective_from"],
    )

    legacy_table = sa.table(
        "risk_limits_legacy",
        sa.column("symbol", sa.String(length=32)),
        sa.column("limit_code", sa.String(length=64)),
        sa.column("limit_value", sa.Numeric(18, 6)),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )

    connection = op.get_bind()
    rows = connection.execute(sa.select(legacy_table)).fetchall()

    insert_stmt = sa.text(
        """
        INSERT INTO risk_limits (key, value, scope, symbol, bucket, effective_from, updated_at, updated_by)
        VALUES (:key, :value, :scope, :symbol, :bucket, :effective_from, :updated_at, :updated_by)
        """
    )

    now_utc = datetime.now(timezone.utc)

    for row in rows:
        symbol = (row.symbol or "").upper()
        if symbol in {"GLOBAL", "ALL", ""}:
            scope = "global"
            symbol_value = ""
            bucket_value = ""
        elif symbol.startswith("BUCKET:"):
            scope = "symbol_bucket"
            bucket_value = symbol.split(":", 1)[1].upper()
            symbol_value = ""
        else:
            scope = "symbol"
            symbol_value = symbol
            bucket_value = ""

        key = (row.limit_code or "").upper()
        if not key:
            continue

        value = str(row.limit_value) if row.limit_value is not None else ""
        effective_from = row.created_at or row.updated_at or now_utc
        updated_at = row.updated_at or now_utc

        connection.execute(
            insert_stmt,
            {
                "key": key,
                "value": value,
                "scope": scope,
                "symbol": symbol_value,
                "bucket": bucket_value,
                "effective_from": effective_from,
                "updated_at": updated_at,
                "updated_by": "migration",
            },
        )

    op.drop_table("risk_limits_legacy")


def downgrade() -> None:
    op.create_table(
        "risk_limits_legacy",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("limit_code", sa.String(length=64), nullable=False),
        sa.Column("limit_value", sa.Numeric(18, 6), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_unique_constraint(
        "uq_risk_limits_symbol_code",
        "risk_limits_legacy",
        ["symbol", "limit_code"],
    )

    new_table = sa.table(
        "risk_limits",
        sa.column("key", sa.String(length=64)),
        sa.column("value", sa.Text()),
        sa.column("scope", sa.String(length=32)),
        sa.column("symbol", sa.String(length=32)),
        sa.column("bucket", sa.String(length=64)),
        sa.column("effective_from", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )

    connection = op.get_bind()
    rows = connection.execute(sa.select(new_table)).fetchall()

    insert_legacy = sa.text(
        """
        INSERT INTO risk_limits_legacy (symbol, limit_code, limit_value, created_at, updated_at)
        VALUES (:symbol, :limit_code, :limit_value, :created_at, :updated_at)
        """
    )

    for row in rows:
        scope = (row.scope or "global").lower()
        symbol = ""
        if scope == "global":
            symbol = "GLOBAL"
        elif scope == "symbol":
            symbol = row.symbol or ""
        elif scope == "symbol_bucket":
            symbol = f"BUCKET:{row.bucket or ''}"
        else:
            symbol = "GLOBAL"

        try:
            limit_value = Decimal(row.value)
        except Exception:  # noqa: BLE001
            continue

        connection.execute(
            insert_legacy,
            {
                "symbol": symbol,
                "limit_code": row.key,
                "limit_value": limit_value,
                "created_at": row.effective_from or datetime.now(timezone.utc),
                "updated_at": row.updated_at or datetime.now(timezone.utc),
            },
        )

    op.drop_index("ix_risk_limits_key_scope", table_name="risk_limits")
    op.drop_constraint("uq_risk_limits_key_scope_target", "risk_limits", type_="unique")
    op.drop_table("risk_limits")
    op.rename_table("risk_limits_legacy", "risk_limits")
