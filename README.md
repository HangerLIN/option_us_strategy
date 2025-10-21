# Option US Strategy Monorepo

This repository hosts the live trading, risk, and analytics stack for the US options programme. It is designed to run exclusively against production-grade inputs—**No-Mock** means no simulated market data sources, CSV snapshots, or stubbed adapters. Services expect a reachable Interactive Brokers Gateway (IBGW/TWS), operational databases, and streaming infrastructure before boot.

## Key Principles

- Real connectivity only: if IBGW/TWS or upstream market data endpoints are not reachable, services must not start.
- Single source of truth: SQL databases and Redis capture canonical state (no CSV fallbacks).
- Observability and control: each FastAPI app exposes `/healthz` and supports Prometheus scraping.

## Layout

```
apps/
  md_gw/         # Market data gateway orchestrating IBKR subscriptions
  rt_engine/     # Real-time risk and rules engine
  signal_svc/    # Signal generation and portfolio construction
  risk_svc/      # Core risk service (uvicorn apps.risk_svc.main:app)
  exec_svc/      # Execution orchestrator
  backtest/      # Research/backtesting API against historical data
  pnl_svc/       # PnL aggregation and exposure tracking
libs/
  core/          # Shared config, logging, time utilities
  db/            # SQLAlchemy models and data access objects
  infra/         # External integrations (IBKR, PostgreSQL, Redis)
  schemas/       # Pydantic types for events and service contracts
infra/
  docker-compose.yml  # Postgres and Redis infrastructure services
tests/                 # Pytest suite
```

## Getting Started

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

Before running any service ensure:

1. IB Gateway or TWS is authenticated (`IB_HOST/IB_PORT/IB_CLIENT_ID/IB_ACCOUNT` in `.env`).
2. TimescaleDB and Redis endpoints are online (see `infra/docker-compose.yml` for local spin-up).
3. Network latency and permissions satisfy production requirements—this stack avoids mocked responses entirely.

Launch the risk service health endpoint:

```bash
uvicorn apps.risk_svc.main:app --env-file .env
```

## Development

- Bring up infra: `make dev-up`
- Tear down infra: `make dev-down`
- Apply database migrations: `make db-migrate`
- Format and lint: `make format` / `make lint`
- Run tests: `make test`
- Launch single services: `make run-rt` / `make run-backtest`

This repository does not include sample trades or snapshot data. Backtesting endpoints expect access-controlled historical datasets loaded via configured storage providers.

## Backtest Smoke Runner

To verify a full “ingest → backtest → metrics” loop against live IBKR/Timescale infrastructure, use the `scripts/run_backtest_smoke.py` helper.

### Prerequisites

1. IBKR TWS/Gateway在线，并允许 API 访问（取消 *Bind API clients to existing session IP*，必要时添加 Trusted IP）。  
2. `.env` 中 `DATABASE_URL / REDIS_URL / IB_HOST / IB_PORT / IB_CLIENT_ID / IB_ACCOUNT` 均已配置；Timescale/Redis 可访问。  
3. 可选：执行 `scripts/probe_ibkr_features.py` 验证 IBKR 历史行情接口无 162/10197 等错误。

### 运行示例

```bash
env PYTHONPATH=. .venv/bin/python scripts/run_backtest_smoke.py \
  --auto-ingest \
  --lookback-days 15 \
  --max-retries 2 \
  --symbols-limit 2
```

功能要点：

- 自动回退最近交易日；若当天行情不完整会尝试下一日。  
- 可根据需要调用 `ingest_equity_1m_ibkr.py`、`ingest_option_chain_meta_ibkr.py`、`ingest_option_l1_ibkr.py` 自动补数并缓存合约。  
- 成功后打印窗口、合约数、信号/成交统计、Sharpe/MaxDD、滑点分位以及合约缓存路径，同时调用 backtest API `/runs` `/metrics/{run_id}` 做抽检。

### 常见失败提示

- **缺少行情**：脚本会提示“Equity/Option L1 minutes insufficient...”并换下一交易日；如连续失败，请确认 Timescale 表是否存在对应日期。  
- **IBKR 162/10197**：表示 IP 绑定或会话冲突；退出其他 TWS/Gateway，重启后重试。  
- **No option parameters returned / no_quotes**：账号缺少目标期权市场数据或延迟行情无报价，需确认订阅或切换实时数据。

如仅需查看 IBKR 连接配置，可执行：

```bash
env PYTHONPATH=. .venv/bin/python scripts/run_backtest_smoke.py --print-ib-config
```
