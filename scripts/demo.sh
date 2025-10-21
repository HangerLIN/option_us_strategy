#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

LOG_DIR="${ROOT}/logs/demo"
ARTIFACT_DIR="${ROOT}/artifacts"
mkdir -p "$LOG_DIR" "$ARTIFACT_DIR"

if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

SERVICE_PIDS=()
cleanup() {
  local code=$?
  for pid in "${SERVICE_PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  make dev-down >/dev/null 2>&1 || true
  exit $code
}
trap cleanup EXIT

start_service() {
  local name=$1
  local module=$2
  local port=$3
  local log_file="$LOG_DIR/${name}.log"
  uvicorn "$module" \
    --port "$port" \
    --env-file .env \
    --log-level info \
    >"$log_file" 2>&1 &
  local pid=$!
  SERVICE_PIDS+=($pid)
}

wait_http() {
  local url=$1
  local retries=40
  until curl -fsSL "$url" >/dev/null; do
    ((retries--)) || { echo "[demo] Service not ready: $url" >&2; exit 1; }
    sleep 1
  done
}

printf '[demo] Starting infrastructure...\n'
make dev-up

printf '[demo] Waiting for database...\n'
python - <<'PY'
import time
from sqlalchemy import create_engine, text
from libs.core import get_settings
settings = get_settings()
engine = create_engine(settings.database_url, future=True)
for _ in range(30):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        break
    except Exception:
        time.sleep(2)
else:
    raise SystemExit("Database not reachable")
PY

printf '[demo] Applying migrations...\n'
alembic upgrade head

printf '[demo] Launching services...\n'
start_service signal apps.signal_svc.main:app 8000
start_service exec apps.exec_svc.main:app 8001
start_service risk apps.risk_svc.main:app 8002
start_service pnl apps.pnl_svc.main:app 8003
start_service backtest apps.backtest.api:app 8004
start_service mdgw apps.md_gw.main:app 8005

wait_http http://127.0.0.1:8000/healthz
wait_http http://127.0.0.1:8001/healthz
wait_http http://127.0.0.1:8002/healthz
wait_http http://127.0.0.1:8003/healthz
wait_http http://127.0.0.1:8004/healthz
wait_http http://127.0.0.1:8005/healthz

printf '[demo] Running end-to-end pipeline...\n'
DEMO_LOG_DIR="$LOG_DIR" DEMO_ARTIFACT_DIR="$ARTIFACT_DIR" python scripts/run_demo_pipeline.py

printf '\n[demo] Pipeline complete. Artifacts located in %s\n' "$ARTIFACT_DIR"
