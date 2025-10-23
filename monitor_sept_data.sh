#!/bin/bash
# 9月数据拉取进度监控脚本

echo "======================================"
echo "  2025年9月数据拉取进度监控"
echo "======================================"
echo ""

# 检查进程状态
if ps -p 37470 > /dev/null 2>&1; then
    echo "✅ 数据拉取进程运行中 (PID: 37470)"
else
    echo "⚠️  数据拉取进程已结束"
fi
echo ""

# 显示最新日志（最后30行）
echo "📊 最新进度 (最后30行):"
echo "--------------------------------------"
tail -30 sept_data_pull.log
echo ""

# 统计已完成的交易日
echo "--------------------------------------"
echo "📈 统计信息:"
grep -c "Fetching 1m bars" sept_data_pull.log | xargs -I {} echo "  - 已尝试拉取: {} 个symbol-day组合"
grep -c "symbol_failed" sept_data_pull.log | xargs -I {} echo "  - 失败数: {}"
grep -c "✅ 完成:" sept_data_pull.log | xargs -I {} echo "  - 成功步骤: {}"
echo ""

# 实时监控命令提示
echo "======================================"
echo "💡 实时监控命令:"
echo "  tail -f sept_data_pull.log"
echo ""
echo "💡 检查数据库:"
echo "  psql -U option_user -d option_us -c \"SELECT COUNT(*) FROM bars1m_equity WHERE ts_end::date = '2025-09-02';\""
echo "======================================"






