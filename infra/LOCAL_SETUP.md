# 本地监控系统部署指南 (使用 Homebrew)

> **适用场景**: macOS开发环境  
> **部署方式**: Homebrew本地安装  
> **优势**: 无需Docker，直接在本机运行

---

## 📋 前置要求

- macOS操作系统
- 已安装Homebrew

---

## 🚀 第一步：安装Prometheus和Grafana

### 1.1 安装Prometheus

```bash
# 安装Prometheus
brew install prometheus

# 验证安装
prometheus --version
```

### 1.2 安装Grafana

```bash
# 安装Grafana
brew install grafana

# 验证安装
grafana-server --version
```

### 1.3 安装Alertmanager (可选)

```bash
# 安装Alertmanager
brew install alertmanager

# 验证安装
alertmanager --version
```

---

## 🔧 第二步：配置Prometheus

### 2.1 创建Prometheus配置目录

```bash
# 创建配置目录
mkdir -p ~/prometheus/rules
mkdir -p ~/prometheus/data

# 复制配置文件
cd /Users/hangerlin/Desktop/option_us_strategy/infra
cp prometheus/prometheus_local.yml ~/prometheus/prometheus.yml
cp prometheus/rules/*.yml ~/prometheus/rules/
```

### 2.2 创建本地配置文件

创建 `prometheus/prometheus_local.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: 'option-us-trading'
    environment: 'local'

# Alertmanager配置
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['localhost:9093']
      timeout: 10s

# 告警规则文件
rule_files:
  - '/Users/hangerlin/prometheus/rules/*.yml'

scrape_configs:
  # Prometheus自监控
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # 业务服务 - 实时数据引擎
  - job_name: 'rt_engine'
    static_configs:
      - targets: ['localhost:8002']
    metrics_path: '/metrics'
    scrape_interval: 10s

  # 业务服务 - 信号服务
  - job_name: 'signal_svc'
    static_configs:
      - targets: ['localhost:8003']
    metrics_path: '/metrics'
    scrape_interval: 15s

  # 业务服务 - 风控服务
  - job_name: 'risk_svc'
    static_configs:
      - targets: ['localhost:8004']
    metrics_path: '/metrics'
    scrape_interval: 10s

  # 业务服务 - 执行服务
  - job_name: 'exec_svc'
    static_configs:
      - targets: ['localhost:8001']
    metrics_path: '/metrics'
    scrape_interval: 15s

  # 业务服务 - 盈亏服务
  - job_name: 'pnl_svc'
    static_configs:
      - targets: ['localhost:8005']
    metrics_path: '/metrics'
    scrape_interval: 15s

  # 业务服务 - 回测API服务
  - job_name: 'backtest_api'
    static_configs:
      - targets: ['localhost:8006']
    metrics_path: '/metrics'
    scrape_interval: 30s

  # Grafana监控
  - job_name: 'grafana'
    static_configs:
      - targets: ['localhost:3000']
    metrics_path: '/metrics'
    scrape_interval: 30s
```

### 2.3 更新告警规则路径

需要确保告警规则文件中的路径正确。规则文件本身不需要修改，只需要复制到正确的位置。

---

## 🎨 第三步：配置Grafana

### 3.1 配置Grafana数据源

创建数据源配置文件：

```bash
# 创建Grafana provisioning目录
mkdir -p /opt/homebrew/etc/grafana/provisioning/datasources
mkdir -p /opt/homebrew/etc/grafana/provisioning/dashboards

# 创建数据源配置
cat > /opt/homebrew/etc/grafana/provisioning/datasources/prometheus.yml << 'EOF'
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
EOF

# 创建看板配置
cat > /opt/homebrew/etc/grafana/provisioning/dashboards/default.yml << 'EOF'
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
      path: /Users/hangerlin/Desktop/option_us_strategy/infra/grafana/dashboards
EOF
```

### 3.2 配置Grafana主配置

编辑Grafana配置（可选）：

```bash
# 编辑配置文件
nano /opt/homebrew/etc/grafana/grafana.ini

# 关键配置项（如果需要修改）：
# [server]
# http_port = 3000
# 
# [security]
# admin_user = admin
# admin_password = admin123
```

---

## 🚀 第四步：启动服务

### 4.1 启动Prometheus

```bash
# 方式1: 前台运行（查看日志）
prometheus --config.file=/Users/hangerlin/prometheus/prometheus.yml \
  --storage.tsdb.path=/Users/hangerlin/prometheus/data \
  --web.enable-lifecycle \
  --web.enable-admin-api

# 方式2: 作为后台服务运行
brew services start prometheus

# 查看状态
brew services list | grep prometheus
```

### 4.2 启动Grafana

```bash
# 方式1: 前台运行
grafana-server --config=/opt/homebrew/etc/grafana/grafana.ini \
  --homepath=/opt/homebrew/opt/grafana/share/grafana

# 方式2: 作为后台服务运行
brew services start grafana

# 查看状态
brew services list | grep grafana
```

### 4.3 启动Alertmanager（可选）

```bash
# 创建Alertmanager配置目录
mkdir -p ~/alertmanager

# 复制配置文件
cp /Users/hangerlin/Desktop/option_us_strategy/infra/alertmanager/alertmanager.yml \
   ~/alertmanager/alertmanager.yml

# 启动Alertmanager
alertmanager --config.file=/Users/hangerlin/alertmanager/alertmanager.yml \
  --storage.path=/Users/hangerlin/alertmanager/data

# 或作为服务运行
brew services start alertmanager
```

---

## 🎯 第五步：验证部署

### 5.1 检查服务状态

```bash
# 检查Prometheus
curl http://localhost:9090/-/healthy
open http://localhost:9090

# 检查Grafana
curl http://localhost:3000/api/health
open http://localhost:3000

# 检查Alertmanager
curl http://localhost:9093/-/healthy
open http://localhost:9093
```

### 5.2 登录Grafana

1. 访问: http://localhost:3000
2. 默认账号: `admin`
3. 默认密码: `admin` (首次登录会要求修改)

### 5.3 验证数据源

在Grafana中：
1. 点击 Configuration → Data Sources
2. 应该能看到 **Prometheus** 数据源
3. 点击 **Test** 按钮，应该显示 "Data source is working"

### 5.4 检查看板

1. 访问: http://localhost:3000/d/trading_overview
2. 应该能看到监控数据（前提是业务服务正在运行）

---

## 📊 服务URL汇总

| 服务 | URL | 用途 |
|------|-----|------|
| **Prometheus** | http://localhost:9090 | 指标查询和告警 |
| **Grafana** | http://localhost:3000 | 可视化看板 |
| **Alertmanager** | http://localhost:9093 | 告警管理 |
| - | | |
| **rt_engine** | http://localhost:8002/metrics | 实时引擎指标 |
| **signal_svc** | http://localhost:8003/metrics | 信号服务指标 |
| **risk_svc** | http://localhost:8004/metrics | 风控服务指标 |
| **exec_svc** | http://localhost:8001/metrics | 执行服务指标 |
| **pnl_svc** | http://localhost:8005/metrics | 盈亏服务指标 |
| **backtest_api** | http://localhost:8006/metrics | 回测API指标 |

---

## 🔄 服务管理命令

### 启动所有服务

```bash
# 启动Prometheus
brew services start prometheus

# 启动Grafana
brew services start grafana

# 启动Alertmanager
brew services start alertmanager

# 查看所有服务状态
brew services list
```

### 停止所有服务

```bash
brew services stop prometheus
brew services stop grafana
brew services stop alertmanager
```

### 重启服务

```bash
# 重启Prometheus（重新加载配置）
brew services restart prometheus

# 重启Grafana
brew services restart grafana
```

### 查看日志

```bash
# Prometheus日志
tail -f /opt/homebrew/var/log/prometheus.log

# Grafana日志
tail -f /opt/homebrew/var/log/grafana/grafana.log
```

---

## 🛠️ 常用操作

### 热更新Prometheus配置

```bash
# 修改配置后，发送reload信号
curl -X POST http://localhost:9090/-/reload

# 或重启服务
brew services restart prometheus
```

### 备份数据

```bash
# 备份Prometheus数据
tar -czf prometheus-backup-$(date +%Y%m%d).tar.gz ~/prometheus/data

# 备份Grafana数据
tar -czf grafana-backup-$(date +%Y%m%d).tar.gz /opt/homebrew/var/lib/grafana
```

### 清理数据

```bash
# 清理Prometheus数据（谨慎操作！）
brew services stop prometheus
rm -rf ~/prometheus/data/*
brew services start prometheus

# 清理Grafana数据
brew services stop grafana
rm -rf /opt/homebrew/var/lib/grafana/*
brew services start grafana
```

---

## 🐛 常见问题

### 问题1: Prometheus无法启动

**症状**: 启动失败或无法访问

**解决**:
```bash
# 检查配置文件语法
promtool check config /Users/hangerlin/prometheus/prometheus.yml

# 检查端口占用
lsof -i :9090

# 查看日志
tail -f /opt/homebrew/var/log/prometheus.log
```

### 问题2: Grafana无法访问

**症状**: 访问http://localhost:3000 无响应

**解决**:
```bash
# 检查Grafana是否运行
brew services list | grep grafana

# 查看日志
tail -f /opt/homebrew/var/log/grafana/grafana.log

# 检查端口
lsof -i :3000

# 重启Grafana
brew services restart grafana
```

### 问题3: Prometheus无法抓取业务服务指标

**症状**: Targets显示Down

**解决**:
```bash
# 1. 确认业务服务正在运行
curl http://localhost:8002/metrics
curl http://localhost:8003/metrics

# 2. 检查防火墙设置
# macOS通常不需要特殊配置

# 3. 检查Prometheus配置
promtool check config /Users/hangerlin/prometheus/prometheus.yml

# 4. 查看Prometheus日志
tail -f /opt/homebrew/var/log/prometheus.log | grep error
```

### 问题4: 看板无数据

**症状**: Grafana看板显示 "No Data"

**解决**:
1. 检查Prometheus能否访问: http://localhost:9090
2. 在Prometheus中测试查询: `up{job="rt_engine"}`
3. 在Grafana中测试数据源: Configuration → Data Sources → Test
4. 检查时间范围: 确保选择了有数据的时间段

---

## 🔐 安全建议

### 1. 修改Grafana默认密码

首次登录后立即修改:
- 默认账号: `admin`
- 默认密码: `admin`

### 2. 限制访问

如果需要，可以配置防火墙规则只允许本地访问。

### 3. 定期备份

设置定时任务自动备份监控数据:

```bash
# 创建备份脚本
cat > ~/backup-monitoring.sh << 'EOF'
#!/bin/bash
BACKUP_DIR=~/monitoring-backups
mkdir -p $BACKUP_DIR
DATE=$(date +%Y%m%d_%H%M%S)

# 备份Prometheus
tar -czf $BACKUP_DIR/prometheus-$DATE.tar.gz ~/prometheus/data

# 备份Grafana
tar -czf $BACKUP_DIR/grafana-$DATE.tar.gz /opt/homebrew/var/lib/grafana

# 保留最近7天的备份
find $BACKUP_DIR -name "*.tar.gz" -mtime +7 -delete
EOF

chmod +x ~/backup-monitoring.sh

# 添加到crontab（每天凌晨2点备份）
# crontab -e
# 0 2 * * * ~/backup-monitoring.sh
```

---

## 📝 配置文件位置汇总

| 文件 | 路径 |
|------|------|
| Prometheus配置 | `/Users/hangerlin/prometheus/prometheus.yml` |
| Prometheus数据 | `/Users/hangerlin/prometheus/data/` |
| 告警规则 | `/Users/hangerlin/prometheus/rules/` |
| Grafana配置 | `/opt/homebrew/etc/grafana/grafana.ini` |
| Grafana数据 | `/opt/homebrew/var/lib/grafana/` |
| Grafana日志 | `/opt/homebrew/var/log/grafana/` |
| Prometheus日志 | `/opt/homebrew/var/log/prometheus.log` |

---

## 🎉 完成！

现在你的监控系统已经在本地运行了！

**下一步**:
1. 启动你的业务服务（rt_engine, signal_svc等）
2. 访问 http://localhost:3000 查看Grafana看板
3. 访问 http://localhost:9090 查看Prometheus
4. 根据需要调整告警规则和看板

**有问题？** 参考上面的常见问题章节或查看日志文件。





