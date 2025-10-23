-- =============================================================================
--  Quant Strategy Schema (TimescaleDB / PostgreSQL)
--  NOTE: All timestamps are stored in UTC; business windows in US/Eastern.
--  Requirements: TimescaleDB 2.x; recommended: timescaledb_toolkit (optional).
-- =============================================================================

-- -----------------------------
-- Extensions
-- -----------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- 可选：如需额外聚合函数与工具，可启用 toolkit
-- CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;

-- =============================================================================
-- 0) Dimension Tables
-- =============================================================================
-- 交易日历（RTH 开/收以 UTC 存储；业务判断以 ET）
CREATE TABLE IF NOT EXISTS dim_trading_calendar (
    trade_date         date PRIMARY KEY,
    is_trading_day     boolean NOT NULL DEFAULT true,
    rth_open_utc       timestamptz,
    rth_close_utc      timestamptz,
    is_half_day        boolean NOT NULL DEFAULT false,
    note               text
);

-- 财报日历（支撑 pre-earn 限制窗口）
CREATE TABLE IF NOT EXISTS earnings_calendar (
    symbol        text NOT NULL,
    earnings_date date NOT NULL,
    source        text,
    confidence    text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT earnings_calendar_pk PRIMARY KEY (symbol, earnings_date)
);

-- =============================================================================
-- 1) Data Pipeline (对齐《数据管道与预处理》)                                  -- fileciteturn0file4
-- =============================================================================

-- 1.1 1m Bar（正股）
CREATE TABLE IF NOT EXISTS bars1m_equity (
    symbol        text        NOT NULL,
    ts_end        timestamptz NOT NULL,   -- minute end timestamp (UTC)
    open          double precision,
    high          double precision,
    low           double precision,
    close         double precision,
    volume        bigint       DEFAULT 0,
    session       text,                   -- PRE/RTH/POST
    CONSTRAINT bars1m_equity_pk PRIMARY KEY (symbol, ts_end),
    CONSTRAINT bars1m_equity_minute_chk CHECK (date_trunc('minute', ts_end)=ts_end)
);
SELECT create_hypertable('bars1m_equity','ts_end', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bars1m_equity_ts_idx ON bars1m_equity (ts_end DESC);
ALTER TABLE bars1m_equity SET (timescaledb.compress, timescaledb.compress_segmentby = 'symbol');

-- compress & retention（使用 DO 块兼容不同版本）
DO $$ BEGIN
  PERFORM add_compression_policy('bars1m_equity', INTERVAL '7 days');
EXCEPTION WHEN others THEN NULL; END $$;
DO $$ BEGIN
  PERFORM add_retention_policy('bars1m_equity', INTERVAL '24 months');
EXCEPTION WHEN others THEN NULL; END $$;

-- 1.2 指标 1m（含 MFI/Stoch 供 E1/E2 可选过滤）
CREATE TABLE IF NOT EXISTS indicators_eq_1m (
    symbol        text        NOT NULL,
    ts_end        timestamptz NOT NULL,
    rsi6          double precision,
    rsi12         double precision,
    rsi24         double precision,
    boll_mid      double precision,
    boll_up       double precision,
    boll_dn       double precision,
    atr14         double precision,
    ao            double precision,
    stoch_k       double precision,
    stoch_d       double precision,
    stoch_rsi_k   double precision,
    stoch_rsi_d   double precision,
    cci14         double precision,
    cci6          double precision,
    sma5          double precision,
    lr_m5_slope   double precision,
    lr_boll_dn_slope double precision,
    lr_obv_slope  double precision,
    obv           double precision,
    obv_ma6       double precision,
    obv_ema20     double precision,
    mfi14         double precision,
    rvol6         double precision,
    CONSTRAINT indicators_eq_1m_pk PRIMARY KEY (symbol, ts_end),
    CONSTRAINT indicators_eq_1m_minute_chk CHECK (date_trunc('minute', ts_end)=ts_end)
);
SELECT create_hypertable('indicators_eq_1m','ts_end', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS indicators_eq_1m_ts_idx ON indicators_eq_1m (ts_end DESC);
ALTER TABLE indicators_eq_1m SET (timescaledb.compress, timescaledb.compress_segmentby = 'symbol');
DO $$ BEGIN
  PERFORM add_compression_policy('indicators_eq_1m', INTERVAL '7 days');
EXCEPTION WHEN others THEN NULL; END $$;
DO $$ BEGIN
  PERFORM add_retention_policy('indicators_eq_1m', INTERVAL '24 months');
EXCEPTION WHEN others THEN NULL; END $$;

-- 1.3 盘前 Top5（09:25–09:30 涨幅）
CREATE TABLE IF NOT EXISTS premarket_top5 (
    trade_date      date        NOT NULL,
    rank            smallint    NOT NULL CHECK (rank BETWEEN 1 AND 5),
    symbol          text        NOT NULL,
    ret_0925_0930   double precision NOT NULL,
    CONSTRAINT premarket_top5_pk PRIMARY KEY (trade_date, rank),
    CONSTRAINT premarket_top5_unq UNIQUE (trade_date, symbol)
);

-- 1.4 RVOL 基线（20D 分时均量）
CREATE TABLE IF NOT EXISTS rvol_baseline_eq (
    symbol        text      NOT NULL,
    minute_index  smallint  NOT NULL CHECK (minute_index BETWEEN 1 AND 390),
    mean_vol_20d  double precision NOT NULL,
    updated_at    timestamptz      DEFAULT now(),
    CONSTRAINT rvol_baseline_eq_pk PRIMARY KEY (symbol, minute_index)
);

-- 1.5（可选）Scanner Snapshot（调试/回补）
CREATE TABLE IF NOT EXISTS scanner_snapshot (
    ts       timestamptz NOT NULL,
    symbol   text        NOT NULL,
    rank     integer,
    source   text,
    CONSTRAINT scanner_snapshot_pk PRIMARY KEY (ts, symbol)
);

-- =============================================================================
-- 2) Day-level Aggregation (ET 口径，支撑 A1/M60/财报前判断)                  -- fileciteturn0file4 fileciteturn0file2
-- =============================================================================
-- 连续聚合（CAGG）：产出每日 RTH 首/末分钟与高低价边界
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_ohlcv_bounds
WITH (timescaledb.continuous) AS
SELECT
  symbol,
  time_bucket('1 day', ts_end, 'US/Eastern') AS bucket_start_et,
  MIN(CASE WHEN session='RTH' THEN ts_end END) AS first_ts_rth,
  MAX(CASE WHEN session='RTH' THEN ts_end END) AS last_ts_rth,
  MAX(high) FILTER (WHERE session='RTH')      AS high_rth,
  MIN(low)  FILTER (WHERE session='RTH')      AS low_rth
FROM bars1m_equity
GROUP BY symbol, time_bucket('1 day', ts_end, 'US/Eastern');
CREATE INDEX IF NOT EXISTS mv_daily_ohlcv_bounds_idx ON mv_daily_ohlcv_bounds (symbol, bucket_start_et DESC);

-- （可选）刷新策略
-- SELECT add_continuous_aggregate_policy('mv_daily_ohlcv_bounds',
--   start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '15 minutes');

-- 普通视图：以边界时间戳从原表取 open/close，并计算 prev_close
CREATE OR REPLACE VIEW v_daily_ohlcv AS
SELECT
  m.symbol,
  (m.bucket_start_et AT TIME ZONE 'US/Eastern')::date AS trade_date,
  b_open.close  AS open_rth,
  m.high_rth    AS high_rth,
  m.low_rth     AS low_rth,
  b_close.close AS close_rth,
  LAG(b_close.close) OVER (PARTITION BY m.symbol ORDER BY (m.bucket_start_et AT TIME ZONE 'US/Eastern')::date) AS prev_close_rth
FROM mv_daily_ohlcv_bounds m
LEFT JOIN bars1m_equity b_open  ON b_open.symbol=m.symbol AND b_open.ts_end=m.first_ts_rth
LEFT JOIN bars1m_equity b_close ON b_close.symbol=m.symbol AND b_close.ts_end=m.last_ts_rth;

-- 为 ATR% 提供“日末 ATR14”
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_ind_last
WITH (timescaledb.continuous) AS
SELECT
  symbol,
  time_bucket('1 day', ts_end, 'US/Eastern') AS bucket_start_et,
  MAX(ts_end) AS last_ts
FROM indicators_eq_1m
GROUP BY symbol, time_bucket('1 day', ts_end, 'US/Eastern');
CREATE INDEX IF NOT EXISTS mv_daily_ind_last_idx ON mv_daily_ind_last (symbol, bucket_start_et DESC);

CREATE OR REPLACE VIEW v_daily_atr_pct AS
SELECT
  d.symbol,
  (d.bucket_start_et AT TIME ZONE 'US/Eastern')::date AS trade_date,
  CASE WHEN o.close_rth > 0 THEN ind.atr14 / o.close_rth END AS atr_pct
FROM mv_daily_ind_last d
JOIN indicators_eq_1m ind ON ind.symbol=d.symbol AND ind.ts_end=d.last_ts
JOIN v_daily_ohlcv o      ON o.symbol=d.symbol AND o.trade_date=(d.bucket_start_et AT TIME ZONE 'US/Eastern')::date;

-- =============================================================================
-- 3) Signals (对齐《交易信号生成与组合构建》)                                  -- fileciteturn0file3
-- =============================================================================
CREATE TABLE IF NOT EXISTS signals (
    ts_emit        timestamptz NOT NULL,
    ts_end         timestamptz NOT NULL,
    symbol         text        NOT NULL,
    signal_code    text        NOT NULL,
    priority       smallint    NOT NULL DEFAULT 50,
    score          double precision,
    payload_json   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    state          text        NOT NULL DEFAULT 'NEW',   -- NEW/ACK/EXPIRED
    cooldown_until timestamptz,
    ttl_until      timestamptz,
    dedupe_key     text,
    CONSTRAINT signals_pk PRIMARY KEY (ts_emit, symbol, signal_code)
);
SELECT create_hypertable('signals','ts_emit', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
CREATE INDEX IF NOT EXISTS signals_sym_time_idx ON signals (symbol, ts_end);
-- 活动态去重：同一 dedupe_key 只允许一条活动信号
CREATE UNIQUE INDEX IF NOT EXISTS signals_dedupe_active_uniq
  ON signals (dedupe_key) WHERE state IN ('NEW','ACK');

-- =============================================================================
-- 4) Execution & Portfolio（对齐《执行与成本控制》）                          -- fileciteturn0file1
-- =============================================================================

-- 4.1 订单
CREATE TABLE IF NOT EXISTS orders (
    client_order_id   text PRIMARY KEY,      -- 幂等 id = trace_id
    ts_submit         timestamptz NOT NULL,
    ts_update         timestamptz,
    conid             bigint,
    symbol            text        NOT NULL,
    right             text,                 -- C/P（可空）
    strike            double precision,
    expiry            date,
    side              text        NOT NULL CHECK (side IN ('BUY','SELL','BUY_TO_COVER','SELL_SHORT','SELL_TO_CLOSE','BUY_TO_OPEN','SELL_TO_OPEN','BUY_TO_CLOSE')),
    qty               integer     NOT NULL CHECK (qty > 0),
    limit_price       double precision,
    tif               text        NOT NULL DEFAULT 'DAY',
    algo              text,
    status            text        NOT NULL DEFAULT 'Submitted',   -- Submitted/Filled/Cancelled/ApiCancelled/Rejected/Partial
    filled_qty        integer     NOT NULL DEFAULT 0,
    avg_fill_price    double precision,
    exchange          text,
    reason_code       text,                 -- BLOCK:* / OPEN_CHASE / REBOUND / AFTERNOON / EXIT / TIME_CLEAR / IV_CLEAR
    dedupe_key        text
);
CREATE INDEX IF NOT EXISTS orders_symbol_time_idx ON orders (symbol, ts_submit DESC);
CREATE INDEX IF NOT EXISTS orders_status_idx ON orders (status);
CREATE UNIQUE INDEX IF NOT EXISTS orders_dedupe_active_uniq
  ON orders (dedupe_key) WHERE status IN ('Submitted','Partial');

-- 4.2 成交
CREATE TABLE IF NOT EXISTS fills (
    fill_id          bigserial PRIMARY KEY,
    client_order_id  text NOT NULL REFERENCES orders(client_order_id) ON DELETE CASCADE,
    ts               timestamptz NOT NULL,
    qty              integer     NOT NULL CHECK (qty > 0),
    price            double precision NOT NULL,
    exchange         text,
    commission       double precision DEFAULT 0,
    fees             double precision DEFAULT 0
);
CREATE INDEX IF NOT EXISTS fills_order_idx ON fills (client_order_id, ts);

-- 4.3 持仓快照（合约粒度）
CREATE TABLE IF NOT EXISTS positions (
    ts          timestamptz NOT NULL,
    conid       bigint      NOT NULL,
    symbol      text        NOT NULL,
    qty         integer     NOT NULL,
    avg_px      double precision,
    tag         text,
    iv_at_entry double precision,
    greeks_json jsonb,
    CONSTRAINT positions_pk PRIMARY KEY (conid, ts)
);
SELECT create_hypertable('positions','ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
CREATE INDEX IF NOT EXISTS positions_symbol_ts_idx ON positions (symbol, ts DESC);

-- 4.4 日内 PnL（symbol 粒度）
CREATE TABLE IF NOT EXISTS pnl_intraday (
    ts              timestamptz NOT NULL,
    symbol          text        NOT NULL,
    realized_pnl    double precision DEFAULT 0,
    unrealized_pnl  double precision DEFAULT 0,
    fees            double precision DEFAULT 0,
    CONSTRAINT pnl_intraday_pk PRIMARY KEY (symbol, ts),
    CONSTRAINT pnl_intraday_minute_chk CHECK (date_trunc('minute', ts)=ts)
);
SELECT create_hypertable('pnl_intraday','ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');

-- 4.5 合约级日内 PnL（支持 -8% 单笔止损审计）
CREATE TABLE IF NOT EXISTS pnl_option_intraday (
    ts              timestamptz NOT NULL,
    conid           bigint      NOT NULL,
    symbol          text        NOT NULL,
    realized_pnl    double precision DEFAULT 0,
    unrealized_pnl  double precision DEFAULT 0,
    fees            double precision DEFAULT 0,
    PRIMARY KEY (conid, ts),
    CONSTRAINT pnl_opt_intraday_minute_chk CHECK (date_trunc('minute', ts)=ts)
);
SELECT create_hypertable('pnl_option_intraday','ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');

-- 4.6 日度 PnL
CREATE TABLE IF NOT EXISTS pnl_daily (
    trade_date      date        NOT NULL,
    symbol          text        NOT NULL,
    gross_pnl       double precision DEFAULT 0,
    net_pnl         double precision DEFAULT 0,
    num_trades      integer     DEFAULT 0,
    CONSTRAINT pnl_daily_pk PRIMARY KEY (trade_date, symbol)
);

-- 4.7 执行度量（滑点/成本）
CREATE TABLE IF NOT EXISTS exec_metrics (
    ts_submit          timestamptz NOT NULL,
    ts_fill            timestamptz,
    latency_ms         integer,
    client_order_id    text,
    conid              bigint,
    symbol             text NOT NULL,
    side               text,
    qty                integer,
    bid_at_submit      double precision,
    ask_at_submit      double precision,
    mid_at_submit      double precision,
    spread_at_submit   double precision,
    limit_px           double precision,
    fill_px            double precision,
    slip_abs           double precision,
    slip_sp_ratio      double precision,
    reason_code        text,
    CONSTRAINT exec_metrics_pk PRIMARY KEY (symbol, ts_submit, reason_code),
    CONSTRAINT exec_metrics_order_fk FOREIGN KEY (client_order_id) REFERENCES orders(client_order_id) ON DELETE SET NULL
);
SELECT create_hypertable('exec_metrics','ts_submit', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
CREATE INDEX IF NOT EXISTS exec_metrics_order_idx ON exec_metrics (client_order_id);

-- =============================================================================
-- 5) Risk Management（对齐《风险管理与动态调整》）                             -- fileciteturn0file0
-- =============================================================================
-- 5.1 风控阈值（热更）
CREATE TABLE IF NOT EXISTS risk_limits (
    limit_id        serial PRIMARY KEY,
    key             text NOT NULL,
    value           text NOT NULL,
    scope           text NOT NULL DEFAULT 'global',   -- global / symbol_bucket / symbol
    symbol          text,
    bucket          text,
    effective_from  timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    updated_by      text,
    CONSTRAINT ck_risk_limits_symbol_scope CHECK (scope <> 'symbol' OR (symbol IS NOT NULL AND trim(symbol) <> '')),
    CONSTRAINT ck_risk_limits_bucket_scope CHECK (scope <> 'symbol_bucket' OR (bucket IS NOT NULL AND trim(bucket) <> ''))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_risk_limits_key_scope_target
  ON risk_limits (key, scope, symbol, bucket, effective_from);
CREATE INDEX IF NOT EXISTS ix_risk_limits_key_scope
  ON risk_limits (key, scope, effective_from);

-- 5.2 风控状态（1m 快照）
CREATE TABLE IF NOT EXISTS risk_state (
    ts_end            timestamptz NOT NULL,
    equity            double precision,
    realized_pnl_day  double precision,
    unrealized_pnl    double precision,
    drawdown_r        double precision,
    used_R            double precision,
    num_positions     integer,
    num_symbols_top   integer,
    gamma_est         double precision,
    theta_est         double precision,
    vix               double precision,
    regime            text,            -- LOW/MID/HIGH
    gate_open_chase   boolean DEFAULT true,
    kill_switch       boolean DEFAULT false,
    CONSTRAINT risk_state_pk PRIMARY KEY (ts_end),
    CONSTRAINT risk_state_minute_chk CHECK (date_trunc('minute', ts_end)=ts_end)
);
SELECT create_hypertable('risk_state','ts_end', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');

-- 5.3 风控事件（审计）
CREATE TABLE IF NOT EXISTS risk_events (
    id            bigserial PRIMARY KEY,
    ts            timestamptz NOT NULL,
    symbol        text,
    event_code    text NOT NULL,      -- HARD_STOP / DAILY_CUTOFF / VIX_GATE_ON / COST_BREACH / REJECT / FILL_MISMATCH / MIDFAIL / MISSED_ENTRY ...
    action        text NOT NULL,      -- BLOCK / FORCE_CLOSE / WARN
    details_json  jsonb NOT NULL DEFAULT '{}'::jsonb,
    trace_id      text
);
SELECT create_hypertable('risk_events','ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
CREATE INDEX IF NOT EXISTS risk_events_time_idx ON risk_events (ts DESC);
CREATE INDEX IF NOT EXISTS risk_events_sym_event_idx ON risk_events (symbol, event_code, ts DESC);

-- =============================================================================
-- 6) Backtest（对齐《回测与绩效评估》）                                      -- fileciteturn0file2
-- =============================================================================
CREATE TABLE IF NOT EXISTS bt_runs (
    run_id        text PRIMARY KEY,
    mode          text NOT NULL,          -- logic / signal
    start_date    date NOT NULL,
    end_date      date NOT NULL,
    universe      text,
    params_json   jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bt_trades (
    run_id        text NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
    symbol        text NOT NULL,
    conid         bigint,
    open_ts       timestamptz NOT NULL,
    close_ts      timestamptz,
    side          text,                   -- CALL/PUT BUY/SELL etc.
    qty           integer,
    open_px       double precision,
    close_px      double precision,
    fees          double precision DEFAULT 0,
    pnl           double precision,
    mfe           double precision,
    mae           double precision,
    exit_reason   text,                   -- S1/S2/TIME/STOP/IV/OVERNIGHT
    signal_code   text,
    PRIMARY KEY (run_id, symbol, open_ts)
);

CREATE TABLE IF NOT EXISTS bt_signals (
    run_id        text NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
    ts_end        timestamptz NOT NULL,
    symbol        text NOT NULL,
    signal_code   text NOT NULL,
    accepted      boolean NOT NULL,
    reason        text,
    PRIMARY KEY (run_id, ts_end, symbol, signal_code)
);

CREATE TABLE IF NOT EXISTS bt_metrics_daily (
    run_id       text NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
    trade_date   date NOT NULL,
    gross_pnl    double precision,
    net_pnl      double precision,
    num_trades   integer,
    win_rate     double precision,
    avg_win      double precision,
    avg_loss     double precision,
    exp_value    double precision,
    max_dd       double precision,
    PRIMARY KEY (run_id, trade_date)
);

CREATE TABLE IF NOT EXISTS bt_metrics_total (
    run_id       text PRIMARY KEY REFERENCES bt_runs(run_id) ON DELETE CASCADE,
    gross_pnl    double precision,
    net_pnl      double precision,
    sharpe       double precision,
    sortino      double precision,
    max_dd       double precision,
    win_rate     double precision,
    pf           double precision
);

-- =============================================================================
-- 7) Optional：期权 1m（若你决定长期落库）                                   -- （可选项与执行/风控口径相关） fileciteturn0file1
-- =============================================================================
CREATE TABLE IF NOT EXISTS bars1m_option (
    conid             bigint       NOT NULL,
    underlying_symbol text         NOT NULL,
    expiry            date         NOT NULL,
    right             text         NOT NULL,
    strike            double precision NOT NULL,
    ts_end            timestamptz  NOT NULL,
    bid               double precision,
    ask               double precision,
    mid               double precision,
    volume            integer,
    open_interest     integer,
    implied_vol       double precision,
    delta             double precision,
    gamma             double precision,
    theta             double precision,
    vega              double precision,
    underlying_price  double precision,
    CONSTRAINT bars1m_option_pk PRIMARY KEY (conid, ts_end),
    CONSTRAINT bars1m_option_minute_chk CHECK (date_trunc('minute', ts_end)=ts_end)
);
SELECT create_hypertable('bars1m_option','ts_end', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');
CREATE INDEX IF NOT EXISTS bars1m_option_sym_idx ON bars1m_option (underlying_symbol, ts_end DESC);
ALTER TABLE bars1m_option SET (timescaledb.compress, timescaledb.compress_segmentby = 'conid');
DO $$ BEGIN
  PERFORM add_compression_policy('bars1m_option', INTERVAL '7 days');
EXCEPTION WHEN others THEN NULL; END $$;
DO $$ BEGIN
  PERFORM add_retention_policy('bars1m_option', INTERVAL '24 months');
EXCEPTION WHEN others THEN NULL; END $$;

-- =============================================================================
-- 8) Grants（按需修改）
-- =============================================================================
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO option_trader;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO option_trader;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO option_trader;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO option_trader;
