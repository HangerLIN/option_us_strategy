#!/bin/bash
# 测试改进后的 ingest_equity_1m_ibkr.py 脚本
# 用途：拉取少量数据验证改进效果

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  测试 ingest_equity_1m_ibkr.py 改进版本                        ║"
echo "║                                                                ║"
echo "║  测试范围：3个股票 × 2个交易日 = 6次请求                        ║"
echo "║  预期耗时：约 12-15秒 (6次请求 × 2秒延时)                      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# 设置测试参数
START_DATE="2025-10-01"
END_DATE="2025-10-02"
SYMBOLS="AAPL GOOGL MSFT"

echo "📋 测试参数："
echo "   开始日期: $START_DATE"
echo "   结束日期: $END_DATE"
echo "   股票列表: $SYMBOLS"
echo ""

echo "🚀 开始测试..."
echo ""

# 记录开始时间
start_time=$(date +%s)

# 运行脚本
python -m scripts.ingest_equity_1m_ibkr \
  --start "$START_DATE" \
  --end "$END_DATE" \
  --symbols $SYMBOLS \
  --rth-only 1

# 计算耗时
end_time=$(date +%s)
elapsed=$((end_time - start_time))

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  ✅ 测试完成！                                                  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "⏱️  总耗时: ${elapsed}秒"
echo ""
echo "📊 验证改进效果："
echo "   ✓ 是否看到了清晰的进度日志？(🔄 Fetching data...)"
echo "   ✓ 是否看到了成功完成日志？(✅ Successfully...)"
echo "   ✓ 是否没有长时间卡住？(每2-3秒有新日志)"
echo "   ✓ 是否没有触发 Pacing Violation？"
echo ""
echo "💡 下一步："
echo "   如果测试成功，可以运行完整的数据准备："
echo "   python scripts/prepare_backtest_data.py --start 2025-09-01 --end 2025-09-30"
echo ""




