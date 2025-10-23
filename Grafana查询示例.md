# Grafana 真实回测数据查询示例

> **数据源**: PostgreSQL  
> **数据库**: option_us (localhost:5433)

---

## 🎯 快速开始 - 在Grafana Explore中使用

**访问**: http://localhost:3000/explore

1. 选择数据源: **PostgreSQL**
2. 切换到 **Code** 模式
3. 复制下面的SQL查询
4. 点击 **Run query**

---

## 📊 查询1: 查看最近的回测列表

```sql
SELECT 
  run_id as "回测ID",
  strategy_code as "策略",
  parameters->>'symbol' as "股票",
  status as "状态",
  started_at as "开始时间",
  completed_at as "完成时间"
FROM bt_runs
ORDER BY run_id DESC
LIMIT 20;
```

**作用**: 查看最近20个回测的基本信息

---

## 📈 查询2: Run #126 (TSLA) 的详细指标

```sql
SELECT 
  metric_code as "指标名称", 
  ROUND(metric_value::numeric, 6) as "值"
FROM bt_metrics_total
WHERE run_id = 126
ORDER BY metric_code;
```

**结果**: 
- COUNT_EXEC_SIG_OPEN_CHASE_BUY = 1（执行了1次信号）
- HIT_SIG_OPEN_CHASE_BUY_5M = 1（5分钟100%命中）
- RET_SIG_OPEN_CHASE_BUY_60M = 0.002261（60分钟收益+0.226%）

---

## 🏆 查询3: 所有回测的收益对比（60分钟）

```sql
SELECT 
  r.run_id as "Run ID",
  r.parameters->>'symbol' as "股票",
  r.status as "状态",
  ROUND(m.metric_value::numeric * 100, 2) as "收益率(%)"
FROM bt_runs r
JOIN bt_metrics_total m ON r.run_id = m.run_id
WHERE m.metric_code = 'RET_SIG_OPEN_CHASE_BUY_60M'
  AND r.status = 'COMPLETED'
ORDER BY m.metric_value DESC
LIMIT 20;
```

**作用**: 按收益率排序，找出表现最好的回测

---

## 📅 查询4: Run #126 的每日指标明细

```sql
SELECT 
  trade_date as "交易日",
  metric_code as "指标",
  ROUND(metric_value::numeric, 6) as "值"
FROM bt_metrics_daily
WHERE run_id = 126
ORDER BY trade_date, metric_code;
```

**作用**: 查看每个交易日的详细表现

---

## 📊 查询5: 有指标数据的回测统计

```sql
SELECT 
  r.run_id as "Run ID",
  r.parameters->>'symbol' as "股票",
  COUNT(DISTINCT m.metric_code) as "指标数量",
  r.started_at as "执行时间"
FROM bt_runs r
JOIN bt_metrics_total m ON r.run_id = m.run_id
WHERE r.status = 'COMPLETED'
GROUP BY r.run_id, r.parameters, r.started_at
ORDER BY r.run_id DESC
LIMIT 10;
```

**作用**: 查看哪些回测有完整的指标数据

---

## 🎨 查询6: 创建时间序列图表 - 收益趋势

```sql
SELECT 
  r.started_at as time,
  r.parameters->>'symbol' as metric,
  m.metric_value as value
FROM bt_runs r
JOIN bt_metrics_total m ON r.run_id = m.run_id
WHERE m.metric_code = 'RET_SIG_OPEN_CHASE_BUY_60M'
  AND r.status = 'COMPLETED'
ORDER BY time;
```

**作用**: 
- 在 **Graph** 视图中显示收益趋势线
- 每个股票一条线
- 横轴是时间，纵轴是收益率

---

## 💡 使用技巧

### 切换视图模式

查询结果下方可以切换：
- **Table**: 表格视图（适合查看详细数据）
- **Graph**: 图表视图（适合查看趋势）
- **Logs**: 日志视图

### 导出数据

1. 运行查询后
2. 点击右上角的 **Inspector** 按钮
3. 选择 **Data** 标签
4. 点击 **Download CSV** 导出数据

### 保存查询为看板

1. 运行查询确认无误
2. 点击右上角的 **Add to dashboard**
3. 选择或创建新看板
4. 保存

---

## 🎯 当前可用的真实回测数据

根据数据库查询，你有以下回测数据：

| Run ID | 股票 | 状态 | 指标数量 |
|--------|------|------|---------|
| 126 | TSLA | COMPLETED | 9个 |
| 125 | ORCL | COMPLETED | 9个 |
| 123 | GOOGL | COMPLETED | 9个 |
| 122 | TSM | COMPLETED | 9个 |
| 121 | PLTR | COMPLETED | 9个 |

**所有这些都是真实的回测结果！**

---

## 🚀 立即尝试

在Grafana Explore中：

1. 选择数据源: **PostgreSQL**
2. 输入查询2（Run #126的详细指标）
3. 点击 **Run query**
4. 查看真实的回测指标！

你现在应该能看到TSLA的真实回测结果了！ 🎉





