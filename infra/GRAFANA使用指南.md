# Grafana 监控使用指南 📊

## 🌐 访问地址

- **Grafana主页**: http://localhost:3000
- **Prometheus**: http://localhost:9090
- **Prometheus Targets**: http://localhost:9090/targets

## 🔑 登录信息

- **用户名**: `admin`
- **密码**: `admin` (首次登录会要求修改)

---

## ✅ 当前可用的监控服务

根据Prometheus监控状态，以下服务正常运行：

1. ✅ **backtest_api** (端口8006) - 回测API服务
2. ✅ **grafana** (端口3000) - Grafana本身
3. ✅ **prometheus** (端口9090) - Prometheus自监控

其他交易服务暂未启动（这是正常的）：
- ❌ rt_engine (8002)
- ❌ signal_svc (8003)
- ❌ risk_svc (8004)
- ❌ exec_svc (8001)
- ❌ pnl_svc (8005)

---

## 📊 在Grafana中查看监控数据

### 方式1: 使用Explore（推荐初学者）

1. 访问 http://localhost:3000/explore
2. 确保数据源选择为 **Prometheus**
3. 在查询框中输入以下任一查询：

**查看回测API状态**:
```promql
up{job="backtest_api"}
```
返回值为1表示服务正常运行

**查看回测API请求总数**:
```promql
backtest_api_requests_total
```

**查看回测API请求速率（过去5分钟）**:
```promql
rate(backtest_api_requests_total[5m])
```

**查看回测API响应延迟P95**:
```promql
histogram_quantile(0.95, rate(backtest_api_latency_seconds_bucket[5m]))
```

**查看回测查询次数**:
```promql
backtest_runs_queried_total
```

### 方式2: 查看预配置的Overview看板

访问: http://localhost:3000/d/trading_overview

你应该能看到：
- 系统健康状态（会显示backtest_api为UP）
- 其他指标暂时无数据（因为其他服务未启动）

---

## 🎯 回测监控指标速查

### 可用的回测指标

| 指标名称 | 说明 | 示例查询 |
|---------|------|---------|
| `up{job="backtest_api"}` | 服务状态 | 1=运行，0=停止 |
| `backtest_api_requests_total` | API请求总数 | 按endpoint/method/status_code分类 |
| `backtest_api_latency_seconds` | API响应延迟 | 直方图 |
| `backtest_runs_queried_total` | 回测列表查询次数 | 计数器 |
| `backtest_metrics_queried_total` | 回测指标查询次数 | 按found标签 |
| `backtest_db_query_seconds` | 数据库查询延迟 | 按operation分类 |
| `backtest_total_runs` | 数据库中的回测总数 | Gauge |

### 实用的PromQL查询

**回测API过去1小时的QPS**:
```promql
rate(backtest_api_requests_total[1h])
```

**回测API错误率**:
```promql
sum(rate(backtest_api_requests_total{status_code=~"5.."}[5m])) 
/ 
sum(rate(backtest_api_requests_total[5m]))
```

**数据库查询P95延迟**:
```promql
histogram_quantile(0.95, 
  rate(backtest_db_query_seconds_bucket{operation="list_runs"}[5m])
)
```

---

## 🎨 创建自定义看板

### 步骤1: 创建新看板

1. 点击左侧菜单 ➕ → Dashboard
2. 点击 "Add visualization"
3. 选择数据源: Prometheus

### 步骤2: 添加回测API状态面板

**配置**:
- Visualization: Stat
- Query: `up{job="backtest_api"}`
- Title: "回测API状态"
- 值映射: 0=Down, 1=Up
- 颜色: 0=红色, 1=绿色

### 步骤3: 添加请求速率图表

**配置**:
- Visualization: Time series
- Query: `rate(backtest_api_requests_total[5m])`
- Title: "回测API请求速率"
- Legend: `{{endpoint}} - {{method}}`

### 步骤4: 保存看板

点击右上角的 💾 Save 按钮

---

## 📈 监控数据实时更新

Grafana会自动刷新数据：
- 默认刷新间隔: 5秒
- 可在右上角调整刷新频率
- 点击右上角的 🔄 图标可手动刷新

---

## 🔍 故障排查

### 看板显示"No Data"

**原因**: 
1. 服务还没有产生指标数据
2. 时间范围选择不正确
3. Prometheus还没采集到数据

**解决**:
1. 等待30秒-1分钟让Prometheus采集数据
2. 调整时间范围为 "Last 5 minutes"
3. 触发一些API请求生成数据:
   ```bash
   curl http://localhost:8006/healthz
   curl http://localhost:8006/runs
   ```

### 数据源连接失败

**解决**:
1. 检查Prometheus是否运行: http://localhost:9090
2. 在Grafana中: Configuration → Data Sources → Prometheus → Test
3. 应该显示绿色的 "Data source is working"

---

## 💡 快速测试

在终端执行以下命令生成一些监控数据：

```bash
# 触发10次API请求
for i in {1..10}; do
  curl -s http://localhost:8006/healthz > /dev/null
  curl -s http://localhost:8006/runs > /dev/null
  sleep 1
done

echo "✅ 已生成测试数据，等待30秒后在Grafana中查询:"
echo "   rate(backtest_api_requests_total[5m])"
```

---

## 🎉 现在开始使用

1. **刷新Prometheus页面**: http://localhost:9090/targets
   - 应该看到 backtest_api 为 UP

2. **在Grafana Explore中测试查询**: http://localhost:3000/explore
   - 输入: `up{job="backtest_api"}`
   - 点击 Run Query
   - 应该看到值为 1

3. **查看监控指标**: 
   - 输入: `backtest_api_requests_total`
   - 切换到 Table 视图查看详细数据

祝监控愉快！ 🚀





