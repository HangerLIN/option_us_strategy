-- 1. 检查回测Run信息
SELECT 
    run_id,
    symbol,
    start_date,
    end_date,
    universe,
    track,
    created_at
FROM bt_runs
WHERE run_id BETWEEN 104 AND 116
ORDER BY run_id;

-- 2. 检查K线数据完整性（2025-10-01至2025-10-07）
SELECT 
    symbol,
    COUNT(DISTINCT DATE(ts_end AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')) as trading_days,
    COUNT(*) as total_bars,
    MIN(ts_end AT TIME ZONE 'America/New_York') as first_bar,
    MAX(ts_end AT TIME ZONE 'America/New_York') as last_bar,
    COUNT(CASE WHEN volume = 0 THEN 1 END) as zero_volume_bars
FROM bars1m_equity
WHERE symbol IN ('ASML', 'AVGO', 'AMD', 'NVDA', 'PLTR', 'TSM', 'GOOGL', 'CRM', 'ORCL', 'TSLA', 'AMZN', 'MSFT', 'AAPL')
  AND ts_end >= '2025-10-01 13:30:00+00'::timestamptz
  AND ts_end < '2025-10-08 21:00:00+00'::timestamptz
GROUP BY symbol
ORDER BY symbol;

-- 3. 检查指标数据完整性
SELECT 
    symbol,
    COUNT(*) as indicator_rows,
    COUNT(CASE WHEN rsi6 IS NULL THEN 1 END) as missing_rsi6,
    COUNT(CASE WHEN boll_mid IS NULL THEN 1 END) as missing_boll,
    COUNT(CASE WHEN atr14 IS NULL THEN 1 END) as missing_atr,
    COUNT(CASE WHEN ao IS NULL THEN 1 END) as missing_ao,
    COUNT(CASE WHEN cci14 IS NULL THEN 1 END) as missing_cci,
    COUNT(CASE WHEN obv IS NULL THEN 1 END) as missing_obv
FROM indicators_eq_1m
WHERE symbol IN ('ASML', 'AVGO', 'AMD', 'NVDA', 'PLTR', 'TSM', 'GOOGL', 'CRM', 'ORCL', 'TSLA', 'AMZN', 'MSFT', 'AAPL')
  AND ts_end >= '2025-10-01 13:30:00+00'::timestamptz
  AND ts_end < '2025-10-08 21:00:00+00'::timestamptz
GROUP BY symbol
ORDER BY symbol;
