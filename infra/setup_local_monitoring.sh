#!/bin/bash
# 本地监控系统快速设置脚本 (使用 Homebrew)

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 检查Homebrew
check_homebrew() {
    if ! command -v brew &> /dev/null; then
        log_error "Homebrew未安装，请先安装: https://brew.sh"
        exit 1
    fi
    log_info "Homebrew已安装 ✓"
}

# 安装Prometheus
install_prometheus() {
    log_step "步骤1: 安装Prometheus"
    
    if command -v prometheus &> /dev/null; then
        log_info "Prometheus已安装 ✓"
        prometheus --version
    else
        log_info "正在安装Prometheus..."
        brew install prometheus
        log_info "Prometheus安装完成 ✓"
    fi
}

# 安装Grafana
install_grafana() {
    log_step "步骤2: 安装Grafana"
    
    if command -v grafana-server &> /dev/null; then
        log_info "Grafana已安装 ✓"
        grafana-server --version | head -1
    else
        log_info "正在安装Grafana..."
        brew install grafana
        log_info "Grafana安装完成 ✓"
    fi
}

# 安装Alertmanager
install_alertmanager() {
    log_step "步骤3: 安装Alertmanager (可选)"
    
    if command -v alertmanager &> /dev/null; then
        log_info "Alertmanager已安装 ✓"
        alertmanager --version | head -1
    else
        read -p "是否安装Alertmanager? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "正在安装Alertmanager..."
            brew install alertmanager
            log_info "Alertmanager安装完成 ✓"
        else
            log_warn "跳过Alertmanager安装"
        fi
    fi
}

# 设置Prometheus配置
setup_prometheus_config() {
    log_step "步骤4: 配置Prometheus"
    
    # 创建配置目录
    PROM_DIR="$HOME/prometheus"
    mkdir -p "$PROM_DIR/rules"
    mkdir -p "$PROM_DIR/data"
    
    # 复制配置文件
    log_info "复制Prometheus配置..."
    cp prometheus/prometheus_local.yml "$PROM_DIR/prometheus.yml"
    
    # 更新规则文件路径
    sed -i.bak "s|/Users/hangerlin/prometheus/rules|$PROM_DIR/rules|g" "$PROM_DIR/prometheus.yml"
    rm -f "$PROM_DIR/prometheus.yml.bak"
    
    # 复制告警规则
    log_info "复制告警规则..."
    cp prometheus/rules/*.yml "$PROM_DIR/rules/"
    
    log_info "Prometheus配置完成 ✓"
    log_info "配置路径: $PROM_DIR"
}

# 设置Grafana配置
setup_grafana_config() {
    log_step "步骤5: 配置Grafana"
    
    # 获取Grafana配置目录
    GRAFANA_ETC="/opt/homebrew/etc/grafana"
    if [ ! -d "$GRAFANA_ETC" ]; then
        GRAFANA_ETC="/usr/local/etc/grafana"
    fi
    
    # 创建provisioning目录
    mkdir -p "$GRAFANA_ETC/provisioning/datasources"
    mkdir -p "$GRAFANA_ETC/provisioning/dashboards"
    
    # 配置Prometheus数据源
    log_info "配置Prometheus数据源..."
    cat > "$GRAFANA_ETC/provisioning/datasources/prometheus.yml" << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://localhost:9090
    isDefault: true
    editable: false
    jsonData:
      httpMethod: POST
      timeInterval: 15s
      queryTimeout: 60s
EOF
    
    # 配置看板自动加载
    log_info "配置Grafana看板..."
    DASHBOARD_PATH="$(pwd)/grafana/dashboards"
    cat > "$GRAFANA_ETC/provisioning/dashboards/default.yml" << EOF
apiVersion: 1

providers:
  - name: 'Default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: $DASHBOARD_PATH
EOF
    
    log_info "Grafana配置完成 ✓"
    log_info "配置路径: $GRAFANA_ETC"
}

# 设置Alertmanager配置
setup_alertmanager_config() {
    if command -v alertmanager &> /dev/null; then
        log_step "步骤6: 配置Alertmanager"
        
        ALERT_DIR="$HOME/alertmanager"
        mkdir -p "$ALERT_DIR/data"
        
        log_info "复制Alertmanager配置..."
        cp alertmanager/alertmanager.yml "$ALERT_DIR/alertmanager.yml"
        
        # 更新配置中的localhost地址
        sed -i.bak 's/host\.docker\.internal/localhost/g' "$ALERT_DIR/alertmanager.yml"
        rm -f "$ALERT_DIR/alertmanager.yml.bak"
        
        log_info "Alertmanager配置完成 ✓"
        log_info "配置路径: $ALERT_DIR"
    fi
}

# 显示启动命令
show_start_commands() {
    echo ""
    echo "========================================"
    echo "✅ 配置完成！"
    echo "========================================"
    echo ""
    echo "🚀 启动命令："
    echo ""
    echo "1️⃣  启动Prometheus:"
    echo "   brew services start prometheus"
    echo "   或前台运行:"
    echo "   prometheus --config.file=$HOME/prometheus/prometheus.yml \\"
    echo "     --storage.tsdb.path=$HOME/prometheus/data \\"
    echo "     --web.enable-lifecycle"
    echo ""
    echo "2️⃣  启动Grafana:"
    echo "   brew services start grafana"
    echo ""
    if command -v alertmanager &> /dev/null; then
        echo "3️⃣  启动Alertmanager:"
        echo "   brew services start alertmanager"
        echo "   或前台运行:"
        echo "   alertmanager --config.file=$HOME/alertmanager/alertmanager.yml"
        echo ""
    fi
    echo "📊 访问地址："
    echo "   Prometheus:  http://localhost:9090"
    echo "   Grafana:     http://localhost:3000 (admin/admin)"
    if command -v alertmanager &> /dev/null; then
        echo "   Alertmanager: http://localhost:9093"
    fi
    echo ""
    echo "🔧 管理命令："
    echo "   查看状态: brew services list"
    echo "   重启服务: brew services restart prometheus"
    echo "   停止服务: brew services stop prometheus"
    echo ""
    echo "📖 详细文档: infra/LOCAL_SETUP.md"
    echo ""
}

# 主函数
main() {
    echo "========================================"
    echo "本地监控系统自动配置脚本"
    echo "========================================"
    echo ""
    
    # 切换到infra目录
    cd "$(dirname "$0")"
    
    # 执行安装步骤
    check_homebrew
    install_prometheus
    install_grafana
    install_alertmanager
    setup_prometheus_config
    setup_grafana_config
    setup_alertmanager_config
    
    # 显示启动命令
    show_start_commands
    
    # 询问是否立即启动
    read -p "是否立即启动所有服务? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "正在启动服务..."
        brew services start prometheus
        brew services start grafana
        if command -v alertmanager &> /dev/null; then
            brew services start alertmanager
        fi
        
        echo ""
        log_info "所有服务已启动！"
        echo ""
        log_info "等待5秒让服务启动..."
        sleep 5
        
        echo ""
        log_info "检查服务状态..."
        brew services list | grep -E "prometheus|grafana|alertmanager"
        
        echo ""
        log_info "打开Grafana..."
        open http://localhost:3000
    else
        log_info "请手动启动服务"
    fi
}

# 运行主函数
main





