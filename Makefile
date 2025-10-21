PYTHON ?= python3
ENV_FILE ?= .env
COMPOSE ?= docker compose -f infra/docker-compose.yml

.PHONY: install dev-up dev-down db-migrate lint test format run-rt run-backtest verify-option-data

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"

dev-up:
	$(COMPOSE) up -d timescaledb redis prometheus

dev-down:
	$(COMPOSE) down

db-migrate:
	alembic upgrade head

lint:
	ruff check .
	mypy apps libs
	flake8 .

test:
	$(PYTHON) -m pytest tests

format:
	ruff format .

run-rt:
	uvicorn apps.rt_engine.main:app --env-file $(ENV_FILE)

run-backtest:
	uvicorn apps.backtest.main:app --env-file $(ENV_FILE)

verify-option-data:
	$(PYTHON) - <<'PY'
from sqlalchemy import create_engine, text
from libs.core import get_settings

settings = get_settings()
engine = create_engine(settings.database_url)

chain_sql = text(
    """
    SELECT trade_date, COUNT(*) AS contracts
    FROM option_chain_meta
    WHERE trade_date BETWEEN :start_date AND :end_date
    GROUP BY trade_date
    HAVING COUNT(*) > 0
    ORDER BY trade_date
    """
)

bars_sql = text(
    """
    SELECT COUNT(*)
    FROM bars1m_option
    WHERE ts_end >= :start_ts
      AND ts_end < :end_ts
    """
)

start_date = "2025-10-01"
end_date = "2025-10-07"
with engine.begin() as conn:
    chain_rows = conn.execute(chain_sql, {"start_date": start_date, "end_date": end_date}).fetchall()
    bars_count = conn.execute(
        bars_sql,
        {
            "start_ts": "2025-10-01 00:00:00-04:00",
            "end_ts": "2025-10-08 00:00:00-04:00",
        },
    ).scalar() or 0

if chain_rows:
    print("option_chain_meta coverage:")
    for trade_date, count in chain_rows:
        print(f"  {trade_date}: {count} contracts")
else:
    print("option_chain_meta coverage: none")

print(f"bars1m_option rows: {bars_count}")
PY
