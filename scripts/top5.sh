#!/bin/bash
# 便捷运行Top5选股的包装脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"

# 检查虚拟环境
if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ 虚拟环境未找到: $VENV_PYTHON"
    exit 1
fi

# 运行Top5选股
"$VENV_PYTHON" "$SCRIPT_DIR/run_top5_selection.py" "$@"




