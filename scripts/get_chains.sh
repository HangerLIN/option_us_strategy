#!/bin/bash
# 便捷获取期权链的包装脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"

# 检查虚拟环境
if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ 虚拟环境未找到: $VENV_PYTHON"
    exit 1
fi

# 运行期权链获取
"$VENV_PYTHON" "$SCRIPT_DIR/get_top5_option_chains.py" "$@"




