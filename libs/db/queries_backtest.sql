-- Backtest related SQL statements.

-- name: check_table_exists
SELECT to_regclass(:table_name) IS NOT NULL AS exists;

-- name: select_equity_bars
SELECT
    b.ts_end,
    b.symbol,
    b.open,
    b.high,
    b.low,
    b.close,
    b.volume,
    i.rsi6,
    i.rsi12,
    i.rsi24,
    i.atr14,
    i.ao,
    i.stoch_k,
    i.stoch_d,
    i.stoch_rsi_k,
    i.stoch_rsi_d,
    i.cci14,
    i.cci6,
    i.sma5,
    i.lr_m5_slope,
    i.lr_boll_dn_slope,
    i.lr_obv_slope,
    i.obv,
    i.obv_ma6,
    i.obv_ema20,
    i.mfi14,
    i.rvol6
FROM bars1m_equity AS b
JOIN indicators_eq_1m AS i
  ON b.symbol = i.symbol AND b.ts_end = i.ts_end
WHERE b.symbol = :symbol
  AND b.ts_end >= :start_ts
  AND b.ts_end < :end_ts
ORDER BY b.ts_end;

-- name: upsert_equity_bars
INSERT INTO bars1m_equity (
    symbol,
    ts_end,
    open,
    high,
    low,
    close,
    volume
) VALUES (
    :symbol,
    :ts_end,
    :open,
    :high,
    :low,
    :close,
    :volume
) ON CONFLICT (symbol, ts_end)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume;

-- name: upsert_equity_indicators
INSERT INTO indicators_eq_1m (
    symbol,
    ts_end,
    rsi6,
    rsi12,
    rsi24,
    boll_mid,
    boll_up,
    boll_dn,
    atr14,
    ao,
    stoch_k,
    stoch_d,
    stoch_rsi_k,
    stoch_rsi_d,
    cci14,
    cci6,
    sma5,
    lr_m5_slope,
    lr_boll_dn_slope,
    lr_obv_slope,
    obv,
    obv_ma6,
    obv_ema20,
    mfi14,
    rvol6
) VALUES (
    :symbol,
    :ts_end,
    :rsi6,
    :rsi12,
    :rsi24,
    :boll_mid,
    :boll_up,
    :boll_dn,
    :atr14,
    :ao,
    :stoch_k,
    :stoch_d,
    :stoch_rsi_k,
    :stoch_rsi_d,
    :cci14,
    :cci6,
    :sma5,
    :lr_m5_slope,
    :lr_boll_dn_slope,
    :lr_obv_slope,
    :obv,
    :obv_ma6,
    :obv_ema20,
    :mfi14,
    :rvol6
) ON CONFLICT (symbol, ts_end)
DO UPDATE SET
    rsi6 = EXCLUDED.rsi6,
    rsi12 = EXCLUDED.rsi12,
    rsi24 = EXCLUDED.rsi24,
    boll_mid = EXCLUDED.boll_mid,
    boll_up = EXCLUDED.boll_up,
    boll_dn = EXCLUDED.boll_dn,
    atr14 = EXCLUDED.atr14,
    ao = EXCLUDED.ao,
    stoch_k = EXCLUDED.stoch_k,
    stoch_d = EXCLUDED.stoch_d,
    stoch_rsi_k = EXCLUDED.stoch_rsi_k,
    stoch_rsi_d = EXCLUDED.stoch_rsi_d,
    cci14 = EXCLUDED.cci14,
    cci6 = EXCLUDED.cci6,
    sma5 = EXCLUDED.sma5,
    lr_m5_slope = EXCLUDED.lr_m5_slope,
    lr_boll_dn_slope = EXCLUDED.lr_boll_dn_slope,
    lr_obv_slope = EXCLUDED.lr_obv_slope,
    obv = EXCLUDED.obv,
    obv_ma6 = EXCLUDED.obv_ma6,
    obv_ema20 = EXCLUDED.obv_ema20,
    mfi14 = EXCLUDED.mfi14,
    rvol6 = EXCLUDED.rvol6;

-- name: select_option_bars
SELECT
    ts_end,
    conid,
    underlying_symbol AS symbol,
    expiry,
    strike,
    "right",
    bid,
    ask,
    mid,
    volume,
    open_interest
FROM {option_bar_table}
WHERE conid = :conid
  AND ts_end >= :start_ts
  AND ts_end < :end_ts
ORDER BY ts_end;

-- name: select_option_bars_by_attributes
SELECT
    ts_end,
    conid,
    underlying_symbol AS symbol,
    expiry,
    strike,
    "right",
    bid,
    ask,
    mid,
    volume,
    open_interest
FROM {option_bar_table}
WHERE underlying_symbol = :underlying_symbol
  AND expiry = :expiry
  AND strike = :strike
  AND "right" = :right
  AND ts_end >= :start_ts
  AND ts_end < :end_ts
ORDER BY ts_end;

-- name: select_option_candidates
SELECT
    conid,
    underlying_symbol,
    expiry,
    strike,
    "right" AS option_right,
    dte,
    delta,
    bid,
    ask,
    mid,
    open_interest,
    volume,
    min_tick
FROM {option_chain_table}
WHERE trade_date = :trade_date
  AND underlying_symbol = :underlying_symbol
  AND "right" = :option_right
  AND dte BETWEEN :dte_min AND :dte_max
ORDER BY dte ASC, strike ASC;

-- name: select_latest_option_quotes_for_symbol
WITH ranked AS (
    SELECT
        ts_end,
        conid,
        underlying_symbol AS symbol,
        expiry,
        strike,
        "right",
        bid,
        ask,
        mid,
        last,
        volume,
        open_interest,
        implied_vol,
        delta,
        gamma,
        theta,
        vega,
        underlying_price,
        ROW_NUMBER() OVER (PARTITION BY conid ORDER BY ts_end DESC) AS rn
    FROM {option_bar_table}
    WHERE underlying_symbol = :underlying_symbol
      AND "right" = :option_right
      AND ts_end >= :start_ts
      AND ts_end <= :end_ts
      AND bid IS NOT NULL
      AND ask IS NOT NULL
)
SELECT
    ts_end,
    conid,
    symbol,
    expiry,
    strike,
    "right",
    bid,
    ask,
    mid,
    last,
    volume,
    open_interest,
    implied_vol,
    delta,
    gamma,
    theta,
    vega,
    underlying_price
FROM ranked
WHERE rn = 1
ORDER BY ts_end DESC, (volume IS NULL) ASC, volume DESC, conid ASC;

-- name: select_latest_option_quote_by_conid
SELECT
    ts_end,
    conid,
    underlying_symbol AS symbol,
    expiry,
    strike,
    "right",
    bid,
    ask,
    mid,
    last,
    volume,
    open_interest,
    implied_vol,
    delta,
    gamma,
    theta,
    vega,
    underlying_price
FROM {option_bar_table}
WHERE conid = :conid
  AND ts_end >= :start_ts
  AND ts_end <= :end_ts
  AND bid IS NOT NULL
  AND ask IS NOT NULL
ORDER BY ts_end DESC
LIMIT 1;

-- name: insert_bt_run
INSERT INTO bt_runs (
    strategy_code,
    started_at,
    status,
    strategy_version,
    calibration_version,
    data_window_start,
    data_window_end,
    parameters,
    notes
) VALUES (
    :strategy_code,
    :started_at,
    :status,
    :strategy_version,
    :calibration_version,
    :data_window_start,
    :data_window_end,
    :parameters,
    :notes
) RETURNING run_id;

-- name: update_bt_run_status
UPDATE bt_runs
SET
    status = :status,
    completed_at = :completed_at,
    notes = :notes
WHERE run_id = :run_id;

-- name: fail_stale_running_runs
UPDATE bt_runs
SET
    status = 'FAILED',
    completed_at = :completed_at,
    notes = :notes
WHERE status = 'RUNNING'
  AND started_at < :cutoff;

-- name: ensure_bt_order_events
CREATE TABLE IF NOT EXISTS bt_order_events (
    event_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES bt_runs(run_id) ON DELETE CASCADE,
    trace_id TEXT,
    symbol TEXT NOT NULL,
    asset_type TEXT NOT NULL DEFAULT 'OPTION',
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

-- name: insert_bt_order_event
INSERT INTO bt_order_events (
    run_id,
    trace_id,
    symbol,
    asset_type,
    signal_code,
    side,
    event_type,
    reason_code,
    signal_time,
    order_submit_time,
    fill_time,
    signal_to_fill_seconds,
    order_ttl_seconds,
    is_stale_fill,
    order_attempts,
    execution_mode,
    original_limit_price,
    final_limit_price,
    fill_price,
    option_bid_at_signal,
    option_ask_at_signal,
    option_mid_at_signal,
    option_bid_at_fill,
    option_ask_at_fill,
    option_mid_at_fill,
    option_spread_abs_at_fill,
    option_spread_pct_at_fill,
    underlying_price_at_signal,
    underlying_price_at_fill,
    vwap_at_signal,
    vwap_at_fill,
    opening_range_high,
    opening_range_low,
    above_vwap_at_signal,
    above_vwap_at_fill,
    above_orh_at_signal,
    above_orh_at_fill,
    stale_reject_reason,
    option_right,
    strike,
    expiry,
    quantity,
    payload
) VALUES (
    :run_id,
    :trace_id,
    :symbol,
    :asset_type,
    :signal_code,
    :side,
    :event_type,
    :reason_code,
    :signal_time,
    :order_submit_time,
    :fill_time,
    :signal_to_fill_seconds,
    :order_ttl_seconds,
    :is_stale_fill,
    :order_attempts,
    :execution_mode,
    :original_limit_price,
    :final_limit_price,
    :fill_price,
    :option_bid_at_signal,
    :option_ask_at_signal,
    :option_mid_at_signal,
    :option_bid_at_fill,
    :option_ask_at_fill,
    :option_mid_at_fill,
    :option_spread_abs_at_fill,
    :option_spread_pct_at_fill,
    :underlying_price_at_signal,
    :underlying_price_at_fill,
    :vwap_at_signal,
    :vwap_at_fill,
    :opening_range_high,
    :opening_range_low,
    :above_vwap_at_signal,
    :above_vwap_at_fill,
    :above_orh_at_signal,
    :above_orh_at_fill,
    :stale_reject_reason,
    :option_right,
    :strike,
    :expiry,
    :quantity,
    :payload
);

-- name: insert_bt_trade
INSERT INTO bt_trades (
    run_id,
    symbol,
    asset_type,
    side,
    quantity,
    price,
    trade_ts,
    trace_id,
    option_right,
    strike,
    expiry,
    fees,
    slippage,
    reason_code
) VALUES (
    :run_id,
    :symbol,
    :asset_type,
    :side,
    :quantity,
    :price,
    :trade_ts,
    :trace_id,
    :option_right,
    :strike,
    :expiry,
    :fees,
    :slippage,
    :reason_code
);

-- name: insert_bt_signal
INSERT INTO bt_signals (
    run_id,
    ts_end,
    symbol,
    asset_type,
    signal_code,
    accepted,
    reason
) VALUES (
    :run_id,
    :ts_end,
    :symbol,
    :asset_type,
    :signal_code,
    :accepted,
    :reason
);

-- name: update_bt_signal_reason
UPDATE bt_signals
SET reason = :reason
WHERE run_id = :run_id
  AND ts_end = :ts_end
  AND symbol = :symbol
  AND signal_code = :signal_code;

-- name: insert_bt_metric_daily
INSERT INTO bt_metrics_daily (
    run_id,
    trade_date,
    metric_code,
    metric_value
) VALUES (
    :run_id,
    :trade_date,
    :metric_code,
    :metric_value
) ON CONFLICT (run_id, trade_date, metric_code)
DO UPDATE SET metric_value = EXCLUDED.metric_value;

-- name: insert_bt_metric_total
INSERT INTO bt_metrics_total (
    run_id,
    metric_code,
    metric_value
) VALUES (
    :run_id,
    :metric_code,
    :metric_value
) ON CONFLICT (run_id, metric_code)
DO UPDATE SET metric_value = EXCLUDED.metric_value;

-- name: select_bt_signals_window
SELECT
    ts_end,
    symbol,
    signal_code,
    accepted,
    reason
FROM bt_signals
WHERE symbol = :symbol
  AND ts_end >= :start_ts
  AND ts_end < :end_ts
ORDER BY ts_end;

-- name: select_risk_events_window
SELECT
    event_ts,
    symbol,
    event_code,
    severity,
    payload
FROM risk_events
WHERE event_ts >= :start_ts
  AND event_ts < :end_ts
  AND (:symbol IS NULL OR symbol = :symbol)
ORDER BY event_ts;

-- name: insert_risk_event
INSERT INTO risk_events (
    event_ts,
    symbol,
    event_code,
    severity,
    payload
) VALUES (
    :event_ts,
    :symbol,
    :event_code,
    :severity,
    CAST(:payload AS jsonb)
);

-- name: select_runs
SELECT
    run_id,
    strategy_code,
    started_at,
    completed_at,
    status,
    strategy_version,
    calibration_version,
    data_window_start,
    data_window_end,
    parameters,
    notes
FROM bt_runs
WHERE started_at >= :start_from
  AND started_at < :start_to
ORDER BY started_at DESC
LIMIT :limit OFFSET :offset;

-- name: select_metrics_daily
SELECT
    trade_date,
    metric_code,
    metric_value
FROM bt_metrics_daily
WHERE run_id = :run_id
ORDER BY trade_date, metric_code;

-- name: select_metrics_total
SELECT
    metric_code,
    metric_value
FROM bt_metrics_total
WHERE run_id = :run_id
ORDER BY metric_code;

-- name: select_rvol_baseline
SELECT
    minute_index,
    mean_vol_20d
FROM rvol_baseline_eq
WHERE symbol = :symbol;

-- name: select_recent_trade_dates
SELECT
    trade_date
FROM dim_trading_calendar
WHERE trade_date <= :as_of
ORDER BY trade_date DESC
LIMIT :limit;

-- name: select_trade_dates_between
SELECT
    trade_date
FROM dim_trading_calendar
WHERE trade_date BETWEEN :start_date AND :end_date
ORDER BY trade_date ASC;

-- name: select_equity_rth_minutes
SELECT
    COUNT(*) AS minute_count
FROM bars1m_equity
WHERE symbol = :symbol
  AND ts_end >= :start_ts
  AND ts_end < :end_ts;

-- name: select_equity_volume_top
SELECT
    symbol,
    SUM(volume) AS total_volume
FROM bars1m_equity
WHERE ts_end >= :start_ts
  AND ts_end < :end_ts
GROUP BY symbol
ORDER BY total_volume DESC
LIMIT :limit;

-- name: count_option_l1_minutes
SELECT
    COUNT(*) AS minute_count
FROM {option_bar_table}
WHERE conid = :conid
  AND ts_end >= :start_ts
  AND ts_end < :end_ts;

-- name: select_option_candidates_filtered
SELECT
    conid,
    underlying_symbol,
    expiry,
    strike,
    "right" AS option_right,
    dte,
    delta,
    bid,
    ask,
    mid,
    open_interest,
    volume,
    min_tick
FROM {option_chain_table}
WHERE trade_date = :trade_date
  AND underlying_symbol = :underlying_symbol
  AND "right" = :option_right
  AND dte BETWEEN :dte_min AND :dte_max
  AND strike BETWEEN :strike_min AND :strike_max
ORDER BY dte ASC, strike ASC;

-- name: select_premarket_top_symbols
SELECT
    symbol,
    rank
FROM premarket_top5
WHERE trade_date = :trade_date
ORDER BY rank ASC;

-- name: select_option_underlying_top
SELECT
    underlying_symbol,
    SUM(COALESCE(volume, 0)) AS total_volume
FROM {option_chain_table}
WHERE trade_date = :trade_date
GROUP BY underlying_symbol
ORDER BY total_volume DESC NULLS LAST
LIMIT :limit;

-- name: select_option_candidate_by_conid
SELECT
    conid,
    underlying_symbol,
    expiry,
    strike,
    "right" AS option_right,
    dte,
    delta,
    bid,
    ask,
    mid,
    open_interest,
    volume,
    min_tick
FROM {option_chain_table}
WHERE conid = :conid
LIMIT 1;

-- name: select_vix_value
SELECT
    metric_value
FROM risk_state
WHERE symbol = 'GLOBAL'
  AND metric_code = 'VIX'
  AND ts <= :ts
ORDER BY ts DESC
LIMIT 1;

-- name: check_relation_exists
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = COALESCE(:schema_name, 'public')
      AND c.relname = :relation_name
) AS exists;
