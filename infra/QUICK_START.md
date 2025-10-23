# 监控系统快速启动指南

## 🚀 5分钟快速部署

### 前置要求

- ✅ Docker 和 Docker Compose 已安装
- ✅ 所有业务服务已配置 `/metrics` 端点
- ✅ 端口 3000, 9090, 9093 可用

### 第一步：启动监控栈

```bash
cd infra
./deploy_monitoring.sh start
```

这将启动以下服务：
- Prometheus (http://localhost:9090)
- Grafana (http://localhost:3000)
- Alertmanager (http://localhost:9093)
- Node Exporter
- Redis Exporter
- Postgres Exporter

**注意**: 确保以下业务服务正在运行（如果需要监控它们）：
- rt_engine (8002) - 实时数据引擎
- signal_svc (8003) - 信号服务
- risk_svc (8004) - 风控服务
- exec_svc (8001) - 执行服务
- pnl_svc (8005) - 盈亏服务
- backtest_api (8006) - 回测API服务 ✨ 新增

### 第二步：访问Grafana

1. 打开浏览器访问: http://localhost:3000
2. 使用默认账号登录: `admin` / `admin123`
3. 查看Overview看板: http://localhost:3000/d/trading_overview

### 第三步：验证数据采集

```bash
# 检查所有target状态
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'

# 查询业务指标
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=up{job="rt_engine"}'
```

### 第四步：配置告警通知

编辑 `alertmanager/alertmanager.yml`，配置你的通知渠道：

```yaml
receivers:
  - name: 'critical-alerts'
    # 钉钉通知
    webhook_configs:
      - url: 'https://oapi.dingtalk.com/robot/send?access_token=YOUR_TOKEN'
        send_resolved: true
    
    # 邮件通知
    email_configs:
      - to: 'oncall@yourdomain.com'
```

重新加载配置：
```bash
./deploy_monitoring.sh reload
```

## 📊 核心功能验证

### 验证告警规则

```bash
# 查看所有告警规则
curl http://localhost:9090/api/v1/rules | jq '.data.groups[].rules[] | {alert: .name, state: .state}'

# 触发测试告警（假设有Kill Switch端点）
curl -X POST http://localhost:8004/admin/kill-switch/activate
```

### 验证指标采集

访问任一业务服务的metrics端点：
```bash
curl http://localhost:8002/metrics | grep "rt_"
curl http://localhost:8003/metrics | grep "signals_"
curl http://localhost:8004/metrics | grep "risk_"
curl http://localhost:8006/metrics | grep "backtest_"  # 回测API
```

### 验证Grafana看板

在Grafana中应该能看到：
- ✅ 所有服务状态为 "Up"
- ✅ 今日盈亏数据
- ✅ 持仓数量
- ✅ 信号数量
- ✅ 权益曲线

## 🔧 常见问题

### 问题1: Prometheus无法抓取业务服务指标

**症状**: Targets显示为Down

**解决**:
```bash
# 检查服务是否运行
curl http://localhost:8002/metrics

# 检查Docker网络
docker network inspect infra_default

# 如果使用host.docker.internal无法访问，改用实际IP
# 编辑 prometheus/prometheus.yml:
#   - targets: ['192.168.1.100:8002']  # 替换为实际IP
```

### 问题2: Grafana看板无数据

**症状**: 看板显示 "No Data"

**解决**:
```bash
# 1. 检查数据源连接
# Grafana > Configuration > Data Sources > Prometheus > Test

# 2. 直接在Prometheus查询
# http://localhost:9090/graph
# 输入查询: up{job="rt_engine"}

# 3. 检查时间范围
# 确保Grafana的时间范围涵盖有数据的时段
```

### 问题3: 告警未触发

**症状**: 条件满足但没有收到告警

**解决**:
```bash
# 1. 检查告警规则状态
curl http://localhost:9090/api/v1/rules

# 2. 检查Alertmanager状态
curl http://localhost:9093/api/v1/alerts

# 3. 查看Alertmanager日志
docker-compose logs alertmanager | tail -n 50
```

## 📈 监控最佳实践

### 告警疲劳预防

1. **合理设置阈值**: 基于历史数据调整告警阈值
2. **使用for子句**: 避免瞬时抖动触发告警
3. **配置抑制规则**: 避免雪崩式告警
4. **分级通知**: Critical用电话/短信，Low用邮件

### 性能优化

1. **调整抓取间隔**:
   - 高频业务指标: 10s
   - 系统指标: 30s
   - 低频指标: 60s

2. **限制指标数量**:
   ```python
   # 只暴露必要的指标
   from prometheus_client import REGISTRY
   REGISTRY.unregister(unnecessary_metric)
   ```

3. **使用Recording Rules** (对于复杂查询):
   ```yaml
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

## 🎯 下一步

### 扩展监控

1. **添加自定义看板**:
   - 复制 `grafana/dashboards/overview.json`
   - 修改查询和布局
   - 刷新Grafana即可看到

2. **集成业务指标**:
   ```python
   # 在业务代码中添加
   from libs.infra.metrics import set_realized_pnl_today
   
   def update_pnl(amount):
       set_realized_pnl_today(float(amount))
   ```

3. **配置多渠道通知**:
   - Slack集成
   - PagerDuty集成
   - 企业微信集成

### 监控进阶

- 📚 阅读完整设计文档: `docs/监控系统设计方案.md`
- 🔍 学习PromQL: https://prometheus.io/docs/prometheus/latest/querying/basics/
- 📊 Grafana进阶: https://grafana.com/docs/grafana/latest/

## 🛠️ 维护命令

```bash
# 查看服务状态
./deploy_monitoring.sh status

# 查看日志
./deploy_monitoring.sh logs prometheus
./deploy_monitoring.sh logs grafana

# 重启服务
./deploy_monitoring.sh restart

# 热更新Prometheus配置
./deploy_monitoring.sh reload

# 完全清理数据
./deploy_monitoring.sh cleanup
```

## 📞 支持

遇到问题？

1. 查看日志: `docker-compose logs -f [service_name]`
2. 检查配置: 确保YAML文件格式正确
3. 验证网络: 确保服务间可以互相访问
4. 查阅文档: `docs/监控系统设计方案.md`

---

**祝监控顺利！** 🎉

