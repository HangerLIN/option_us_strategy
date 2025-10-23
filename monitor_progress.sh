#!/bin/bash

# 9月数据拉取进度监控脚本

echo "======================================"
echo " 9月数据拉取进度监控"
echo "======================================"
echo ""

# 检查进程状态
echo "1️⃣ 进程状态:"
ps aux | grep "[p]ython.*prepare_backtest\|[p]ython.*ingest_equity" | awk '{print "   PID:", $2, "CMD:", $11, $12, $13}' || echo "   ❌ 没有运行中的进程"
echo ""

# 检查数据库中的数据量
echo "2️⃣ 数据库进度:"
psql "postgresql://option_user:option_pass@localhost:5433/option_us" -c "
SELECT 
  COUNT(DISTINCT symbol) as symbols,
  COUNT(DISTINCT ts_end::date) as days,
  COUNT(*) as total_bars,
  MIN(ts_end AT TIME ZONE 'America/New_York')::time as first_bar,
  MAX(ts_end AT TIME ZONE 'America/New_York')::time as last_bar
FROM bars1m_equity
WHERE ts_end >= '2025-09-01' AND ts_end < '2025-10-01';
" 2>/dev/null

echo ""
echo "3️⃣ 指标进度:"
psql "postgresql://option_user:option_pass@localhost:5433/option_us" -c "
SELECT 
  COUNT(DISTINCT symbol) as symbols_with_indicators,
  COUNT(DISTINCT ts_end::date) as days,
  COUNT(*) as total_indicators
FROM indicators_eq_1m
WHERE ts_end >= '2025-09-01' AND ts_end < '2025-10-01';
" 2>/dev/null

echo ""
echo "4️⃣ 最新日志（最后20行）:"
tail -20 sept_final_*.log 2>/dev/null | grep -E "Fetching|symbol_failed|✅|❌" | tail -10

echo ""
echo "======================================"
echo " 预期目标: 67 symbols × 22 days × 421 bars = 620,454 bars"
echo "======================================"






