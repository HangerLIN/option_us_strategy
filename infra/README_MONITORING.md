# 监控系统完整配置

本目录包含完整的监控栈配置，包括Prometheus、Grafana、Alertmanager以及各种Exporter。

## 📁 目录结构

```
infra/
├── docker-compose.yml                 # Docker Compose配置文件
├── deploy_monitoring.sh               # 一键部署脚本
├── alert_webhook_example.py           # 告警接收服务示例
├── QUICK_START.md                     # 快速启动指南
├── README_MONITORING.md               # 本文件
│
├── prometheus/
│   ├── prometheus.yml                 # Prometheus主配置
│   └── rules/                         # 告警规则目录
│       ├── critical.yml               # P0 Critical告警
│       ├── high.yml                   # P1 High告警
│       ├── medium.yml                 # P2 Medium告警
│       └── low.yml                    # P3 Low告警
│
├── alertmanager/
│   └── alertmanager.yml               # Alertmanager配置
│
└── grafana/
    ├── provisioning/                  # Grafana自动配置
    │   ├── datasources/
    │   │   └── prometheus.yml         # Prometheus数据源配置
    │   └── dashboards/
    │       └── default.yml            # 看板自动加载配置
    └── dashboards/                    # 看板JSON文件
        └── overview.json              # 总览看板
```

## 🚀 快速开始

### 1. 启动监控栈

```bash
cd infra
./deploy_monitoring.sh start
```

### 2. 访问各服务

| 服务 | 地址 | 默认账号 |
|------|------|---------|
| Grafana | http://localhost:3000 | admin / admin123 |
| Prometheus | http://localhost:9090 | 无需登录 |
| Alertmanager | http://localhost:9093 | 无需登录 |

### 3. 验证监控

```bash
# 检查所有服务状态
./deploy_monitoring.sh status

# 查看Prometheus targets
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'

# 查询示例指标
curl -G http://localhost:9090/api/v1/query --data-urlencode 'query=up'
```

## 📊 监控指标体系

### 业务服务指标

#### rt_engine (实时数据引擎)
- `rt_ticks_received_total`: Tick数据接收总数
- `rt_subscribed_symbols_count`: 当前订阅股票数
- `rt_ibkr_connection_status`: IBKR连接状态
- `rt_indicator_calculation_seconds`: 指标计算耗时
- `rt_top5_selection_total`: Top5选股执行次数

#### signal_svc (信号服务)
- `signals_emitted_total`: 信号发出总数
- `signals_by_type_total`: 按类型分类的信号
- `signal_generation_latency_seconds`: 信号生成延迟
- `signals_active_count`: 当前活跃信号数

#### risk_svc (风控服务)
- `risk_kill_switch_state`: Kill Switch状态
- `risk_drawdown_r`: 当前回撤R值
- `risk_position_count`: 持仓数量
- `risk_account_equity_usd`: 账户权益
- `risk_capital_utilization_pct`: 资金使用率

#### exec_svc (执行服务)
- `orders_submitted_total`: 订单提交总数
- `orders_accepted_total`: 订单接受总数
- `orders_conversion_rate`: 订单转化率
- `order_slippage_bps`: 订单滑点
- `option_chain_fetch_latency_seconds`: 期权链获取延迟

#### pnl_svc (盈亏服务)
- `pnl_realized_today_usd`: 今日已实现盈亏
- `pnl_unrealized_today_usd`: 今日未实现盈亏
- `pnl_win_rate_pct`: 胜率
- `pnl_trades_count_today`: 今日交易数

#### backtest_api (回测服务) ✨ 新增
- `backtest_api_requests_total`: API请求总数
- `backtest_api_latency_seconds`: API请求延迟
- `backtest_runs_queried_total`: 回测查询次数
- `backtest_metrics_queried_total`: 指标查询次数
- `backtest_db_query_seconds`: 数据库查询延迟
- `backtest_total_runs`: 回测总数

### 基础设施指标

#### 系统资源 (Node Exporter)
- CPU使用率
- 内存使用率
- 磁盘IO
- 网络流量

#### 数据库 (Postgres Exporter)
- 连接数
- 查询延迟
- 缓存命中率
- 死锁次数

#### Redis (Redis Exporter)
- 内存使用
- Stream长度
- 连接数
- 命中率

## 🔔 告警规则

### 告警分级

| 级别 | 文件 | 响应时间 | 示例 |
|------|------|---------|------|
| P0 Critical | critical.yml | 立即 | Kill Switch激活、服务宕机 |
| P1 High | high.yml | 5分钟 | 订单转化率低、延迟高 |
| P2 Medium | medium.yml | 30分钟 | 内存使用高、慢查询 |
| P3 Low | low.yml | 1小时 | 磁盘空间低、无交易 |
| 回测专用 | backtest.yml | 1-10分钟 | 回测API异常、查询慢 |

### 关键告警

#### Critical (P0)
1. **KillSwitchActivated**: Kill Switch已激活
2. **DrawdownExceeded**: 回撤超过5R
3. **IBKRDisconnected**: IBKR连接断开
4. **DatabaseDown**: 数据库不可用
5. **CoreServiceDown**: 核心服务宕机

#### High (P1)
1. **LowOrderConversionRate**: 订单转化率<70%
2. **HighBarLatency**: Bar延迟P95>5s
3. **RedisStreamBacklog**: 消息队列积压>100
4. **FrequentForceClose**: 强平频繁触发
5. **HighSlippage**: 滑点P95>50bps

查看完整告警规则: `prometheus/rules/*.yml`

## 📈 Grafana看板

### Overview Dashboard (总览看板)

包含以下面板：
1. **系统健康状态**: 所有服务的运行状态
2. **关键业务指标**: 今日盈亏、持仓数、信号数、胜率
3. **活跃告警**: 当前告警按级别分类
4. **权益曲线**: 账户权益和回撤趋势
5. **风险指标**: 资金使用率、限额使用情况
6. **性能指标**: 各服务P95延迟

### 自定义看板

创建新看板：
1. 复制 `grafana/dashboards/overview.json`
2. 修改查询和面板配置
3. 保存后自动加载

## 🔧 配置修改

### 修改告警阈值

编辑对应的告警规则文件，例如修改回撤阈值：

```yaml
# prometheus/rules/critical.yml
- alert: DrawdownExceeded
  expr: risk_drawdown_r > 5  # 改为你想要的阈值
  for: 1m
```

应用修改：
```bash
./deploy_monitoring.sh reload
```

### 添加新的告警规则

在对应级别的文件中添加：

```yaml
# prometheus/rules/high.yml
- alert: MyNewAlert
  expr: my_metric > threshold
  for: 5m
  labels:
    severity: high
    service: my_service
    category: my_category
  annotations:
    summary: "告警摘要"
    description: "详细描述"
    runbook: "处理步骤"
```

### 配置通知渠道

#### 钉钉机器人

编辑 `alertmanager/alertmanager.yml`:

```yaml
receivers:
  - name: 'critical-alerts'
    webhook_configs:
      - url: 'https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN'
        send_resolved: true
```

#### 企业微信

```yaml
receivers:
  - name: 'critical-alerts'
    wechat_configs:
      - corp_id: 'YOUR_CORP_ID'
        to_party: '1'
        agent_id: 'YOUR_AGENT_ID'
        api_secret: 'YOUR_API_SECRET'
```

#### 自定义Webhook

使用提供的示例服务：

```bash
# 启动webhook接收服务
python3 alert_webhook_example.py

# 在alertmanager.yml中配置
webhook_configs:
  - url: 'http://host.docker.internal:5000/alerts/webhook'
```

## 🛠️ 运维操作

### 日常维护

```bash
# 查看服务状态
./deploy_monitoring.sh status

# 查看日志
./deploy_monitoring.sh logs prometheus
./deploy_monitoring.sh logs grafana
./deploy_monitoring.sh logs alertmanager

# 重启服务
./deploy_monitoring.sh restart

# 热更新配置
./deploy_monitoring.sh reload
```

### 备份与恢复

```bash
# 备份Prometheus数据
docker exec prometheus tar czf /tmp/prometheus-data.tar.gz /prometheus
docker cp prometheus:/tmp/prometheus-data.tar.gz ./backups/

# 备份Grafana配置
docker exec grafana tar czf /tmp/grafana-data.tar.gz /var/lib/grafana
docker cp grafana:/tmp/grafana-data.tar.gz ./backups/

# 备份告警规则
tar czf alerting-rules-$(date +%Y%m%d).tar.gz prometheus/rules/
```

### 性能优化

#### 1. 调整采集频率

编辑 `prometheus/prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'rt_engine'
    scrape_interval: 10s  # 高频服务
  
  - job_name: 'node_exporter'
    scrape_interval: 30s  # 系统指标可以降低频率
```

#### 2. 减少指标数量

在业务代码中：

```python
from prometheus_client import REGISTRY

# 取消注册不需要的指标
REGISTRY.unregister(some_metric)
```

#### 3. 使用Recording Rules

对于复杂查询，预计算结果：

```yaml
# prometheus/rules/recording.yml
groups:
  - name: recording_rules
    interval: 30s
    rules:
      - record: job:order_success_rate:5m
        expr: |
          sum(rate(orders_accepted_total[5m]))
          /
          sum(rate(orders_submitted_total[5m]))
```

### 故障排查

#### Prometheus无法抓取数据

```bash
# 1. 检查target状态
curl http://localhost:9090/api/v1/targets

# 2. 检查网络连通性
docker exec prometheus wget -O- http://host.docker.internal:8002/metrics

# 3. 查看Prometheus日志
docker-compose logs prometheus | grep ERROR
```

#### 告警未触发

```bash
# 1. 检查告警规则状态
curl http://localhost:9090/api/v1/rules | jq '.data.groups[].rules[] | select(.type=="alerting")'

# 2. 检查Alertmanager连接
curl http://localhost:9090/api/v1/alertmanagers

# 3. 查看告警历史
curl http://localhost:9093/api/v1/alerts
```

#### Grafana看板无数据

1. 检查数据源连接: Configuration > Data Sources > Test
2. 直接在Prometheus验证查询
3. 检查时间范围是否正确
4. 查看浏览器控制台错误

## 📚 相关文档

- **完整设计方案**: `../docs/监控系统设计方案.md`
- **快速启动指南**: `QUICK_START.md`
- **Prometheus文档**: https://prometheus.io/docs/
- **Grafana文档**: https://grafana.com/docs/
- **Alertmanager文档**: https://prometheus.io/docs/alerting/latest/alertmanager/

## 🎯 监控清单

### 启动前检查

- [ ] Docker已安装并运行
- [ ] 端口3000, 9090, 9093可用
- [ ] 所有配置文件已创建
- [ ] 业务服务已实现/metrics端点

### 运行后验证

- [ ] 所有服务健康检查通过
- [ ] Prometheus能抓取所有target
- [ ] Grafana能访问并显示数据
- [ ] 告警规则已加载
- [ ] 通知渠道已配置并测试

### 日常巡检

- [ ] 检查活跃告警
- [ ] 审查关键指标趋势
- [ ] 验证数据采集正常
- [ ] 检查磁盘空间
- [ ] 审查慢查询日志

## 💡 最佳实践

1. **告警疲劳预防**
   - 合理设置阈值（基于历史数据）
   - 使用`for`子句避免瞬时抖动
   - 配置告警抑制规则
   - 分级通知（Critical用电话，Low用邮件）

2. **性能优化**
   - 按需调整采集频率
   - 使用Recording Rules预计算复杂查询
   - 定期清理无用指标
   - 监控Prometheus自身性能

3. **可靠性保证**
   - 定期备份监控数据
   - 配置高可用部署（生产环境）
   - 监控监控系统本身
   - 准备应急预案

4. **持续改进**
   - 根据告警频率调整阈值
   - 定期审查告警有效性
   - 优化看板布局
   - 收集用户反馈

---

**有问题？** 查看 `QUICK_START.md` 或完整设计文档。

