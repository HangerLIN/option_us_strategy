#!/bin/bash
# 9月数据拉取进度监控脚本（修复版）

echo "========================================"
echo "  2025年9月数据拉取进度监控"
echo "  (修复Client ID冲突后)"
echo "========================================"
echo ""

# 1. 检查进程状态
echo "📊 进程状态:"
echo "--------"
if ps aux | grep -E "python.*prepare_backtest_data" | grep -v grep > /dev/null; then
    echo "✅ 主控进程运行中"
    ps aux | grep -E "python.*prepare_backtest_data" | grep -v grep | awk '{print "   PID:", $2}'
else
    echo "⚠️  主控进程未运行"
fi

if ps aux | grep -E "python.*backfill_premarket" | grep -v grep > /dev/null; then
    echo "✅ 盘前数据拉取进程运行中"
    ps aux | grep -E "python.*backfill_premarket" | grep -v grep | awk '{print "   PID:", $2}'
else
    echo "⏸️  盘前数据拉取进程未运行"
fi

if ps aux | grep -E "python.*ingest_equity_1m" | grep -v grep > /dev/null; then
    echo "✅ RTH数据拉取进程运行中"
    ps aux | grep -E "python.*ingest_equity_1m" | grep -v grep | awk '{print "   PID:", $2}'
else
    echo "⏸️  RTH数据拉取进程未运行"
fi
echo ""

# 2. 检查Client ID使用情况
echo "🔑 Client ID 使用情况:"
echo "--------"
LOG_FILE=$(ls -t sept_data_fixed_*.log 2>/dev/null | head -1)
if [ -n "$LOG_FILE" ]; then
    echo "日志文件: $LOG_FILE"
    echo ""
    echo "已发现的Client ID:"
    grep -o "clientId=[0-9]\+" "$LOG_FILE" | sort -u | sed 's/^/   /'
    echo ""
    echo "Client ID 冲突检查:"
    if grep -q "client id is already in use" "$LOG_FILE"; then
        echo "   ❌ 检测到 Client ID 冲突！"
        grep "client id is already in use" "$LOG_FILE" | tail -3
    else
        echo "   ✅ 无 Client ID 冲突"
    fi
else
    echo "   ⚠️  未找到日志文件"
fi
echo ""

# 3. 数据库入库进度
echo "💾 数据库入库进度:"
echo "--------"
PGPASSWORD=option_pass psql -h localhost -p 5433 -U option_user -d option_us -t -c "
SELECT 
  '总交易日: ' || COUNT(DISTINCT (ts_end AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date) || ' 天',
  '总K线数: ' || COUNT(*),
  '平均每天股票数: ' || ROUND(AVG(symbol_count), 1)
FROM (
  SELECT 
    (ts_end AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date as trade_date,
    COUNT(DISTINCT symbol) as symbol_count
  FROM bars1m_equity
  WHERE ts_end >= '2025-09-01' AND ts_end < '2025-10-01'
  GROUP BY 1
) sub;
"

echo ""
echo "最近5个交易日详情:"
PGPASSWORD=option_pass psql -h localhost -p 5433 -U option_user -d option_us -c "
SELECT 
  (ts_end AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date as 日期,
  COUNT(DISTINCT symbol) as 股票数,
  COUNT(*) as K线数,
  ROUND(COUNT(*)::numeric / COUNT(DISTINCT symbol), 0) as 平均每股K线数
FROM bars1m_equity
WHERE ts_end >= '2025-09-01' AND ts_end < '2025-10-01'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5;
"

echo ""

# 4. 最新日志输出
echo "========================================="
echo "📝 最新日志 (最后30行):"
echo "========================================="
if [ -n "$LOG_FILE" ]; then
    tail -30 "$LOG_FILE"
else
    echo "   ⚠️  未找到日志文件"
fi

echo ""
echo "========================================="
echo "💡 实用命令:"
echo "   实时日志: tail -f sept_data_fixed_*.log"
echo "   杀掉进程: kill -9 \$(ps aux | grep python.*prepare_backtest | awk '{print \$2}')"
echo "=========================================

"






