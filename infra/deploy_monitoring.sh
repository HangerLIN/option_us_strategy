#!/bin/bash
# 监控系统快速部署脚本
# 使用方法: ./deploy_monitoring.sh [start|stop|restart|status]

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Docker是否运行
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        log_error "Docker未运行，请先启动Docker"
        exit 1
    fi
    log_info "Docker运行正常"
}

# 检查配置文件
check_configs() {
    log_info "检查配置文件..."
    
    local configs=(
        "prometheus/prometheus.yml"
        "prometheus/rules/critical.yml"
        "prometheus/rules/high.yml"
        "prometheus/rules/medium.yml"
        "prometheus/rules/low.yml"
        "alertmanager/alertmanager.yml"
        "grafana/provisioning/datasources/prometheus.yml"
        "grafana/provisioning/dashboards/default.yml"
        "grafana/dashboards/overview.json"
    )
    
    local missing=0
    for config in "${configs[@]}"; do
        if [ ! -f "$config" ]; then
            log_error "缺少配置文件: $config"
            missing=$((missing + 1))
        fi
    done
    
    if [ $missing -gt 0 ]; then
        log_error "有 $missing 个配置文件缺失，请先创建"
        exit 1
    fi
    
    log_info "所有配置文件检查通过"
}

# 验证YAML语法
validate_yaml() {
    log_info "验证YAML配置文件语法..."
    
    if command -v yamllint > /dev/null 2>&1; then
        yamllint prometheus/prometheus.yml alertmanager/alertmanager.yml || {
            log_warn "YAML语法检查发现问题，但继续执行"
        }
    else
        log_warn "yamllint未安装，跳过YAML语法检查"
    fi
}

# 启动监控栈
start_monitoring() {
    log_info "启动监控服务..."
    
    docker-compose up -d prometheus grafana alertmanager node_exporter redis_exporter postgres_exporter
    
    log_info "等待服务启动..."
    sleep 10
    
    check_services
}

# 停止监控栈
stop_monitoring() {
    log_info "停止监控服务..."
    
    docker-compose stop prometheus grafana alertmanager node_exporter redis_exporter postgres_exporter
    
    log_info "监控服务已停止"
}

# 重启监控栈
restart_monitoring() {
    log_info "重启监控服务..."
    
    stop_monitoring
    sleep 5
    start_monitoring
}

# 检查服务状态
check_services() {
    log_info "检查服务状态..."
    
    local services=(
        "prometheus:9090:-/healthy"
        "grafana:3000:/api/health"
        "alertmanager:9093:-/healthy"
        "node_exporter:9100:/metrics"
        "redis_exporter:9121:/metrics"
        "postgres_exporter:9187:/metrics"
    )
    
    echo ""
    echo "服务状态检查:"
    echo "----------------------------------------"
    
    for service in "${services[@]}"; do
        IFS=':' read -r name port endpoint <<< "$service"
        
        if curl -sf "http://localhost:${port}${endpoint}" > /dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} ${name} (http://localhost:${port}) - 运行正常"
        else
            echo -e "${RED}✗${NC} ${name} (http://localhost:${port}) - 无法访问"
        fi
    done
    
    echo "----------------------------------------"
    echo ""
}

# 显示访问信息
show_urls() {
    echo ""
    echo "================================================"
    echo "监控系统访问地址"
    echo "================================================"
    echo ""
    echo "📊 Prometheus:     http://localhost:9090"
    echo "📈 Grafana:        http://localhost:3000"
    echo "   └─ 默认账号: admin / admin123"
    echo "🔔 Alertmanager:   http://localhost:9093"
    echo "💻 Node Exporter:  http://localhost:9100/metrics"
    echo "🔴 Redis Exporter: http://localhost:9121/metrics"
    echo "🐘 PG Exporter:    http://localhost:9187/metrics"
    echo ""
    echo "================================================"
    echo "快速链接"
    echo "================================================"
    echo ""
    echo "查看告警规则:     http://localhost:9090/alerts"
    echo "查看告警:         http://localhost:9093/#/alerts"
    echo "Grafana看板:      http://localhost:3000/d/trading_overview"
    echo ""
}

# 热更新Prometheus配置
reload_prometheus() {
    log_info "热更新Prometheus配置..."
    
    if curl -X POST http://localhost:9090/-/reload > /dev/null 2>&1; then
        log_info "Prometheus配置已重新加载"
    else
        log_error "Prometheus配置重新加载失败"
        exit 1
    fi
}

# 查看日志
show_logs() {
    local service=${1:-all}
    
    if [ "$service" = "all" ]; then
        docker-compose logs -f prometheus grafana alertmanager
    else
        docker-compose logs -f "$service"
    fi
}

# 清理数据
cleanup_data() {
    log_warn "这将删除所有监控数据！"
    read -p "确认删除? (yes/no): " confirm
    
    if [ "$confirm" = "yes" ]; then
        log_info "停止服务..."
        docker-compose down
        
        log_info "删除数据卷..."
        docker volume rm infra_prometheus_data infra_grafana_data infra_alertmanager_data 2>/dev/null || true
        
        log_info "数据已清理"
    else
        log_info "取消清理操作"
    fi
}

# 显示帮助
show_help() {
    cat << EOF
监控系统部署脚本

使用方法: ./deploy_monitoring.sh [command]

命令:
  start          启动监控服务
  stop           停止监控服务
  restart        重启监控服务
  status         检查服务状态
  reload         热更新Prometheus配置
  logs [service] 查看日志 (不指定service则查看所有)
  cleanup        清理所有数据
  help           显示此帮助信息

示例:
  ./deploy_monitoring.sh start
  ./deploy_monitoring.sh logs prometheus
  ./deploy_monitoring.sh reload

EOF
}

# 主函数
main() {
    cd "$(dirname "$0")"
    
    local command=${1:-help}
    
    case $command in
        start)
            check_docker
            check_configs
            validate_yaml
            start_monitoring
            show_urls
            ;;
        stop)
            check_docker
            stop_monitoring
            ;;
        restart)
            check_docker
            restart_monitoring
            show_urls
            ;;
        status)
            check_services
            show_urls
            ;;
        reload)
            check_docker
            reload_prometheus
            ;;
        logs)
            show_logs "$2"
            ;;
        cleanup)
            check_docker
            cleanup_data
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "未知命令: $command"
            show_help
            exit 1
            ;;
    esac
}

main "$@"












