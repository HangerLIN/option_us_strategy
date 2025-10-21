#!/usr/bin/env bash
# 服务管理脚本 - 启动、停止和查看系统服务状态

set -eo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

LOG_DIR="${ROOT}/logs/manual"
mkdir -p "$LOG_DIR"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 获取服务配置 (格式: module|port|client_id)
get_service_config() {
    local name=$1
    case "$name" in
        md_gw)
            echo "apps.md_gw.main:app|8005|88"
            ;;
        rt_engine)
            # 运行aggregator（包含Top5定时任务），不是API服务
            echo "apps.rt_engine.aggregator|8006|PYTHON"
            ;;
        rt_api)
            # RT Engine的API服务（提供手动触发Top5等接口）
            echo "apps.rt_engine.main:app|8007|97"
            ;;
        risk_svc)
            echo "apps.risk_svc.main:app|8002|"
            ;;
        signal_svc)
            echo "apps.signal_svc.main:app|8000|"
            ;;
        exec_svc)
            echo "apps.exec_svc.main:app|8001|89"
            ;;
        pnl_svc)
            echo "apps.pnl_svc.main:app|8003|"
            ;;
        *)
            echo ""
            ;;
    esac
}

# 启动单个服务
start_service() {
    local name=$1
    local config=$(get_service_config "$name")
    
    if [ -z "$config" ]; then
        echo -e "${RED}❌ 未知服务: $name${NC}"
        return 1
    fi
    
    IFS='|' read -r module port client_id <<< "$config"
    
    if [ -f "$LOG_DIR/${name}.pid" ]; then
        local pid=$(cat "$LOG_DIR/${name}.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}⚠️  ${name} 已在运行 (PID: $pid)${NC}"
            return 0
        fi
    fi
    
    echo -e "${GREEN}▶️  启动 ${name}...${NC}"
    
    # 检查是否为Python模块（aggregator等）
    if [ "$client_id" = "PYTHON" ]; then
        nohup .venv/bin/python -m "$module" \
            > "$LOG_DIR/${name}.log" 2>&1 &
    elif [ -n "$client_id" ]; then
        IB_CLIENT_ID=$client_id nohup .venv/bin/uvicorn "$module" \
            --port "$port" \
            --env-file .env \
            > "$LOG_DIR/${name}.log" 2>&1 &
    else
        nohup .venv/bin/uvicorn "$module" \
            --port "$port" \
            --env-file .env \
            > "$LOG_DIR/${name}.log" 2>&1 &
    fi
    
    echo $! > "$LOG_DIR/${name}.pid"
    echo -e "${GREEN}✅ ${name} 已启动 (PID: $(cat "$LOG_DIR/${name}.pid"))${NC}"
}

# 停止单个服务
stop_service() {
    local name=$1
    
    if [ ! -f "$LOG_DIR/${name}.pid" ]; then
        echo -e "${YELLOW}⚠️  ${name} 未运行${NC}"
        return 0
    fi
    
    local pid=$(cat "$LOG_DIR/${name}.pid")
    if ! ps -p "$pid" > /dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  ${name} 未运行${NC}"
        rm -f "$LOG_DIR/${name}.pid"
        return 0
    fi
    
    echo -e "${RED}⏹️  停止 ${name} (PID: $pid)...${NC}"
    kill "$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
    rm -f "$LOG_DIR/${name}.pid"
    echo -e "${GREEN}✅ ${name} 已停止${NC}"
}

# 检查服务状态
check_service() {
    local name=$1
    local config=$(get_service_config "$name")
    
    if [ -z "$config" ]; then
        return 1
    fi
    
    IFS='|' read -r module port client_id <<< "$config"
    
    if [ -f "$LOG_DIR/${name}.pid" ]; then
        local pid=$(cat "$LOG_DIR/${name}.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            # 检查健康接口
            if curl -s -m 1 "http://localhost:$port/healthz" > /dev/null 2>&1; then
                echo -e "${GREEN}✅${NC} $name (端口 $port, PID: $pid) - 运行正常"
            else
                echo -e "${YELLOW}⚠️${NC}  $name (端口 $port, PID: $pid) - 启动中"
            fi
            return 0
        fi
    fi
    echo -e "${RED}❌${NC} $name - 未运行"
    return 1
}

# 查看服务日志
logs_service() {
    local name=$1
    local lines=${2:-50}
    
    if [ ! -f "$LOG_DIR/${name}.log" ]; then
        echo -e "${RED}❌ ${name} 日志文件不存在${NC}"
        return 1
    fi
    
    echo -e "${GREEN}📋 ${name} 最新 ${lines} 行日志:${NC}"
    tail -n "$lines" "$LOG_DIR/${name}.log"
}

# 显示帮助
show_help() {
    cat << EOF
服务管理脚本

用法: $0 <command> [service_name] [options]

命令:
    start [service]     启动服务 (不指定则启动所有服务)
    stop [service]      停止服务 (不指定则停止所有服务)
    restart [service]   重启服务 (不指定则重启所有服务)
    status              显示所有服务状态
    logs <service> [n]  查看服务日志 (默认显示最后50行)

可用服务:
    md_gw       - 市场数据网关
    rt_engine   - 实时引擎（聚合器+Top5定时任务）
    rt_api      - 实时引擎API（手动触发Top5等）
    risk_svc    - 风险管理服务
    signal_svc  - 信号生成服务
    exec_svc    - 执行服务
    pnl_svc     - PnL 统计服务

示例:
    $0 start                    # 启动所有服务
    $0 start md_gw              # 只启动 md_gw
    $0 stop                     # 停止所有服务
    $0 restart exec_svc         # 重启 exec_svc
    $0 status                   # 查看所有服务状态
    $0 logs risk_svc 100        # 查看 risk_svc 最新100行日志

EOF
}

# 主逻辑
main() {
    local command=${1:-""}
    local service=${2:-""}
    local option=${3:-""}
    
    case "$command" in
        start)
            if [ -n "$service" ]; then
                start_service "$service"
            else
                echo "🚀 启动所有服务..."
                for svc in md_gw rt_engine rt_api risk_svc signal_svc exec_svc pnl_svc; do
                    start_service "$svc"
                    sleep 1
                done
                echo ""
                echo "等待服务启动..."
                sleep 3
                main status
            fi
            ;;
        stop)
            if [ -n "$service" ]; then
                stop_service "$service"
            else
                echo "🛑 停止所有服务..."
                for svc in pnl_svc exec_svc signal_svc risk_svc rt_api rt_engine md_gw; do
                    stop_service "$svc"
                done
            fi
            ;;
        restart)
            if [ -n "$service" ]; then
                stop_service "$service"
                sleep 2
                start_service "$service"
            else
                main stop
                sleep 2
                main start
            fi
            ;;
        status)
            echo "📊 系统服务状态:"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            for svc in md_gw rt_engine rt_api risk_svc signal_svc exec_svc pnl_svc; do
                check_service "$svc" || true
            done
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            ;;
        logs)
            if [ -z "$service" ]; then
                echo -e "${RED}❌ 请指定服务名称${NC}"
                show_help
                exit 1
            fi
            logs_service "$service" "${option:-50}"
            ;;
        help|--help|-h|"")
            show_help
            ;;
        *)
            echo -e "${RED}❌ 未知命令: $command${NC}"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
