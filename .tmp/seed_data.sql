DELETE FROM bt_trades;
DELETE FROM bt_runs;
DELETE FROM bt_signals;
DELETE FROM bt_metrics_daily;
DELETE FROM bt_metrics_total;
DELETE FROM bars1m_equity;
DELETE FROM indicators_eq_1m;
DELETE FROM option_chain_meta;
DELETE FROM bars1m_option;
DELETE FROM risk_state;
DELETE FROM risk_events;
DELETE FROM pnl_intraday;

INSERT INTO bars1m_equity (symbol, ts_end, open, high, low, close, volume, session) VALUES
 ('AAPL', '2024-01-02T19:30:00Z', 10.80, 10.90, 10.70, 10.85, 1200, 'RTH'),
 ('AAPL', '2024-01-02T19:31:00Z', 10.85, 10.90, 10.60, 10.70, 900, 'RTH'),
 ('AAPL', '2024-01-02T19:32:00Z', 10.70, 10.80, 9.90, 10.05, 1500, 'RTH'),
 ('AAPL', '2024-01-02T19:33:00Z', 10.05, 10.30, 9.80, 10.20, 1300, 'RTH'),
 ('AAPL', '2024-01-02T19:34:00Z', 10.20, 10.50, 10.00, 10.40, 1100, 'RTH'),
 ('AAPL', '2024-01-02T19:35:00Z', 10.40, 10.60, 10.20, 10.55, 1000, 'RTH');

INSERT INTO indicators_eq_1m (symbol, ts_end, boll_mid, boll_up, boll_dn, rsi6, rsi12, rsi24, atr14, ao, stoch_k, stoch_d, cci14, cci6, obv, obv_ema20, mfi14, rvol6) VALUES
 ('AAPL', '2024-01-02T19:30:00Z', 10.50, 10.80, 10.20, 55, 52, 50, 0.30, 0.10, 40, 38, 50, 48, 100000, 95000, 55, 1.2),
 ('AAPL', '2024-01-02T19:31:00Z', 10.55, 10.85, 10.25, 57, 53, 51, 0.32, 0.12, 45, 40, 52, 49, 101000, 95500, 56, 1.1),
 ('AAPL', '2024-01-02T19:32:00Z', 10.40, 10.70, 10.10, 59, 54, 52, 0.35, 0.15, 50, 46, 55, 51, 102000, 96000, 57, 1.0),
 ('AAPL', '2024-01-02T19:33:00Z', 10.30, 10.60, 10.00, 60, 55, 53, 0.36, 0.18, 52, 48, 56, 52, 103000, 96500, 58, 0.95),
 ('AAPL', '2024-01-02T19:34:00Z', 10.35, 10.65, 10.05, 61, 56, 54, 0.37, 0.19, 53, 49, 57, 53, 104000, 97000, 59, 0.90),
 ('AAPL', '2024-01-02T19:35:00Z', 10.45, 10.75, 10.15, 62, 57, 55, 0.38, 0.20, 54, 50, 58, 54, 105000, 97500, 60, 0.85);

INSERT INTO option_chain_meta (conid, trade_date, underlying_symbol, expiry, strike, "right", dte, delta, bid, ask, mid, open_interest, volume, min_tick) VALUES
 (1001, '2024-01-02', 'AAPL', '2024-01-05T21:00:00Z', 180.00, 'CALL', 3, 0.42, 9.80, 10.20, 10.00, 2000, 500, 0.10),
 (2001, '2024-01-02', 'AAPL', '2024-01-05T21:00:00Z', 175.00, 'PUT', 3, -0.38, 10.10, 10.50, 10.30, 1800, 450, 0.10);

INSERT INTO bars1m_option (ts_end, conid, symbol, expiry, strike, "right", bid, ask, mid, volume, open_interest) VALUES
 ('2024-01-02T19:30:00Z', 1001, 'AAPL', '2024-01-05T21:00:00Z', 180.00, 'CALL', 9.80, 10.20, 10.00, 200, 2000),
 ('2024-01-02T19:31:00Z', 1001, 'AAPL', '2024-01-05T21:00:00Z', 180.00, 'CALL', 9.80, 10.20, 10.00, 180, 2000),
 ('2024-01-02T19:32:00Z', 1001, 'AAPL', '2024-01-05T21:00:00Z', 180.00, 'CALL', 9.90, 10.30, 10.10, 220, 2000),
 ('2024-01-02T19:33:00Z', 1001, 'AAPL', '2024-01-05T21:00:00Z', 180.00, 'CALL', 9.95, 10.35, 10.15, 210, 2000),
 ('2024-01-02T19:34:00Z', 1001, 'AAPL', '2024-01-05T21:00:00Z', 180.00, 'CALL', 10.10, 10.50, 10.30, 190, 2000),
 ('2024-01-02T19:35:00Z', 1001, 'AAPL', '2024-01-05T21:00:00Z', 180.00, 'CALL', 10.20, 10.60, 10.40, 180, 2000),
 ('2024-01-02T19:33:00Z', 2001, 'AAPL', '2024-01-05T21:00:00Z', 175.00, 'PUT', 10.10, 10.50, 10.30, 160, 1800),
 ('2024-01-02T19:34:00Z', 2001, 'AAPL', '2024-01-05T21:00:00Z', 175.00, 'PUT', 10.15, 10.55, 10.35, 170, 1800),
 ('2024-01-02T19:35:00Z', 2001, 'AAPL', '2024-01-05T21:00:00Z', 175.00, 'PUT', 10.20, 10.60, 10.40, 175, 1800);

INSERT INTO bt_signals (run_id, ts_end, symbol, signal_code, accepted, reason) VALUES
 (NULL, '2024-01-02T19:30:00Z', 'AAPL', 'SIG_REBOUND_BUY', TRUE, 'seed-entry'),
 (NULL, '2024-01-02T19:34:00Z', 'AAPL', 'SIG_EXIT_BOX2MID', TRUE, 'seed-exit');
