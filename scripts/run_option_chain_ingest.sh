#!/bin/bash
# 期权链数据补数脚本
# 用途：批量抓取股票池中所有标的的期权链分钟级K线数据
# 要求：DTE 2-7天，OTM第2-7档

set -e  # 遇到错误立即退出

# 切换到项目根目录
cd "$(dirname "$0")/.." || exit 1

# 激活虚拟环境
if [ -d ".venv" ]; then
    source .venv/bin/activate
else
    echo "❌ 错误：未找到虚拟环境 .venv"
    exit 1
fi

# 设置环境变量
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# 检查必要的参数
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "用法: $0 <开始日期> <结束日期> [其他参数]"
    echo "示例: $0 2025-10-01 2025-10-07"
    echo ""
    echo "可选参数："
    echo "  --symbols-from file|list|ref_universe|premarket_top5"
    echo "  --universe-file <文件路径> (默认: config/stock_universe.txt)"
    echo "  --symbol-list <标的列表> (当--symbols-from=list时使用)"
    echo "  --max-contracts-per-underlying <数量> (默认: 80)"
    echo "  --max-dte <天数> (默认: 7)"
    echo "  --max-workers <线程数> (默认: 4)"
    exit 1
fi

START_DATE=$1
END_DATE=$2
shift 2

# 默认参数
SYMBOLS_FROM=${SYMBOLS_FROM:-file}
UNIVERSE_FILE=${UNIVERSE_FILE:-config/stock_universe.txt}
MAX_CONTRACTS=${MAX_CONTRACTS:-80}
MAX_DTE=${MAX_DTE:-7}
MAX_WORKERS=${MAX_WORKERS:-4}

echo "========================================"
echo "期权链数据补数"
echo "========================================"
echo "开始日期: $START_DATE"
echo "结束日期: $END_DATE"
echo "股票池来源: $SYMBOLS_FROM"
if [ "$SYMBOLS_FROM" = "file" ]; then
    echo "股票池文件: $UNIVERSE_FILE"
fi
echo "DTE范围: 2-$MAX_DTE 天"
echo "OTM档位: 第2-7档"
echo "每标的最多合约数: $MAX_CONTRACTS"
echo "并发线程数: $MAX_WORKERS"
echo "========================================"
echo ""

# 执行补数命令
echo "🚀 开始执行..."
python -m apps.ingest.ingest_option_chain \
    --date-from "$START_DATE" \
    --date-to "$END_DATE" \
    --symbols-from "$SYMBOLS_FROM" \
    --universe-file "$UNIVERSE_FILE" \
    --max-contracts-per-underlying "$MAX_CONTRACTS" \
    --max-dte "$MAX_DTE" \
    --max-workers "$MAX_WORKERS" \
    "$@"

EXIT_CODE=$?

echo ""
echo "========================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ 补数完成！"
    echo ""
    echo "验证数据："
    echo "  SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM option_chain_meta;"
    echo "  SELECT COUNT(*), MIN(ts_end::date), MAX(ts_end::date) FROM bars1m_option;"
else
    echo "❌ 补数失败（退出码：$EXIT_CODE）"
    echo ""
    echo "检查日志："
    echo "  tail -100 logs/manual/*.log"
fi
echo "========================================"

exit $EXIT_CODE

