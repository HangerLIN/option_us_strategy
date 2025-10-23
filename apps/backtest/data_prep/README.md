# 回测数据准备模块

## 📁 目录结构

```
apps/backtest/data_prep/
├── __init__.py
├── README.md                        # 本文档
├── prepare_backtest_data.py        # 主入口：数据准备总调度器
├── ingest_equity_1m_ibkr.py        # IBKR数据拉取引擎
└── backfill_premarket_window.py    # 盘前数据回填工具
```

---

## 🎯 模块功能

### 1. `prepare_backtest_data.py` - 主入口

**作用**：回测数据准备的总调度器

**功能**：
- ✅ 加载市值数据 (`ref_market_cap`)
- ✅ 加载交易日历 (`dim_trading_calendar`)
- ✅ 调用盘前数据回填（可选，已废弃）
- ✅ 调用 RTH 数据拉取（09:00-16:00）

**使用方法**：
```bash
python -m apps.backtest.data_prep.prepare_backtest_data \
  --start 2025-09-01 \
  --end 2025-09-30 \
  --universe ref_market_cap:GLOBAL \
  --batch-size 5 \
  --skip-market-cap \
  --skip-calendar \
  --skip-premarket
```

**参数说明**：
- `--start`: 开始日期
- `--end`: 结束日期
- `--universe`: 股票池（ref_market_cap:GLOBAL 或文件路径）
- `--batch-size`: 批量拉取大小（默认5）
- `--skip-market-cap`: 跳过市值数据加载
- `--skip-calendar`: 跳过交易日历加载
- `--skip-premarket`: 跳过盘前数据回填（推荐）

---

### 2. `ingest_equity_1m_ibkr.py` - 数据拉取引擎

**作用**：从 IBKR 拉取 1 分钟 K 线数据并计算指标

**核心流程**：
```
1. 连接 IBKR API
   ↓
2. 请求历史 1 分钟 K 线数据（09:00-16:00）
   ↓
3. 写入 bars1m_equity 表
   ↓
4. 调用指标计算 (apps/rt_engine/indicators.py)
   ↓
5. 写入 indicators_eq_1m 表
```

**使用方法**：
```bash
python -m apps.backtest.data_prep.ingest_equity_1m_ibkr \
  --start 2025-09-01 \
  --end 2025-09-30 \
  --universe ref_market_cap:GLOBAL \
  --batch-size 5 \
  --rth-only 1
```

**关键函数**：
- `_request_equity_bars_with_retry()`: 从 IBKR 请求数据
- `_store_equity_rows()`: 存储 K 线到数据库
- `_recompute_indicators()`: 重新计算指标

---

### 3. `backfill_premarket_window.py` - 盘前数据回填

**作用**：回填指定时间窗口的数据（已废弃）

**状态**：⚠️ 已废弃，因为交易日历已改为 09:00-16:00，不再需要单独拉盘前数据

---

## 🔧 依赖关系

### 外部依赖
- `libs/infra/ibkr_client.py` - IBKR API 客户端
- `apps/rt_engine/indicators.py` - 技术指标计算
- `apps/backtest/dao.py` - 数据访问层
- `apps/backtest/universe.py` - 股票池解析

### 数据库表
- `bars1m_equity` - 1 分钟 K 线数据（OHLCV）
- `indicators_eq_1m` - 技术指标数据（25 列）
- `dim_trading_calendar` - 交易日历
- `ref_market_cap` - 市值数据

---

## 📊 数据流转

```
IBKR API (历史数据)
    ↓
ingest_equity_1m_ibkr.py
    ↓
bars1m_equity 表 (OHLCV)
    ↓
apps/rt_engine/indicators.py (计算指标)
    ↓
indicators_eq_1m 表 (25列指标)
```

---

## ⚙️ 配置要求

### 环境变量 (`.env`)
```bash
# IBKR 连接
IB_HOST=127.0.0.1
IB_PORT=7497
IB_CLIENT_ID=88

# 数据库
DATABASE_URL=postgresql://option_user:option_pass@localhost:5433/option_us
```

### 交易日历配置
- 文件：`data/dim_trading_calendar.csv`
- 格式：
  ```csv
  session_date,open_time,close_time,session_type
  2025-09-01,09:00,16:00,RTH
  ```
- **重要**：`open_time` 必须是 `09:00` 以确保 09:30 第一根信号 K 线有足够历史数据

---

## 🐛 常见问题

### 1. Client ID 冲突
**错误**：`Unable connect as the client id is already in use`

**解决**：
- 确保没有其他进程占用相同的 Client ID
- 或修改环境变量：`IB_CLIENT_ID=100 python -m apps.backtest.data_prep.prepare_backtest_data ...`

### 2. 指标计算失败
**错误**：`index 13 is out of bounds for axis 0 with size 1`

**原因**：数据不足 30 根 K 线

**解决**：已在 `apps/rt_engine/indicators.py` 添加 `MIN_BARS_REQUIRED=30` 检查

### 3. 缺少交易日历
**错误**：`No trading session configured for 2025-09-XX`

**解决**：
```bash
python -m scripts.seed_calendar data/dim_trading_calendar.csv
```

---

## 📈 监控进度

使用监控脚本：
```bash
./monitor_progress.sh
```

输出示例：
```
1️⃣ 进程状态: ✅ 正在运行
2️⃣ 数据库进度: 11,310 bars (5 symbols × 8 days)
3️⃣ 指标进度: 2,311 indicators
预期目标: 67 symbols × 22 days × 421 bars = 620,454 bars
```

---

## 📊 实时监控

数据拉取过程支持 Prometheus + Grafana 实时监控，可视化追踪进度。

### 启用监控

```bash
# 设置环境变量（可选，默认已启用）
export DATA_INGEST_METRICS_ENABLED=true
export DATA_INGEST_METRICS_PORT=9091

# 启动监控基础设施
cd infra
docker-compose up -d prometheus grafana

# 运行数据拉取（自动启动metrics server）
python -m apps.backtest.data_prep.ingest_equity_1m_ibkr \
  --start 2025-10-01 \
  --end 2025-10-07 \
  --universe ref_market_cap:GLOBAL \
  --batch-size 3
```

### 访问监控界面

- **📊 Grafana Dashboard**: http://localhost:3000
  - 导航到: Dashboards → Data Ingestion Progress
  - 实时查看：进度百分比、ETA、拉取速率、错误统计
  
- **📈 Prometheus**: http://localhost:9090
  - 查询示例：
    - `data_ingest_completion_percentage` - 完成度
    - `data_ingest_eta_seconds` - 预计完成时间
    - `rate(data_ingest_bars_stored_total[1m])` - K线存储速率
  
- **🔍 Metrics 原始端点**: http://localhost:9091/metrics
  - 直接查看所有Prometheus指标

### 关键监控指标

| 指标名称 | 说明 | 单位 |
|---------|------|------|
| `data_ingest_completion_percentage` | 总体完成度 | % (0-100) |
| `data_ingest_eta_seconds` | 预计完成时间 | 秒 |
| `data_ingest_rate_bars_per_second` | K线拉取速率 | bars/秒 |
| `data_ingest_symbols_completed` | 已完成股票数 | 个 |
| `data_ingest_symbols_total` | 总股票数 | 个 |
| `data_ingest_ibkr_connected` | IBKR连接状态 | 1=连接, 0=断开 |
| `data_ingest_bars_stored_total` | 已存储K线总数 | 累计 |
| `data_ingest_indicators_computed_total` | 已计算指标数 | 累计 |
| `data_ingest_ibkr_errors_total` | IBKR错误次数 | 累计 |
| `data_ingest_ibkr_pacing_violations_total` | 频率限制次数 | 累计 |

### 日志中的进度信息

启用监控后，日志会自动输出进度统计：

```
📊 Progress: 3/16 symbols (18.8%), ETA: 1245s (20.8min), Bars: 3156, Rate: 12.3 bars/s
```

### 自动告警

系统会监控以下情况并自动告警：

#### 🔴 Critical 级别
- IBKR连接断开
- 任务异常终止

#### ⚠️ Warning 级别
- 任务卡住（30分钟无进展）
- Pacing violations 过多
- 指标计算失败率>10%
- 股票处理失败率>50%
- IBKR错误率过高

#### ℹ️ Info 级别
- ETA超过2小时
- 拉取速率过慢(<5 bars/秒)
- 进度里程碑（50%, 90%）

### 禁用监控

如果不需要监控（例如一次性快速拉取）：

```bash
DATA_INGEST_METRICS_ENABLED=false \
python -m apps.backtest.data_prep.ingest_equity_1m_ibkr \
  --start 2025-10-02 \
  --end 2025-10-02 \
  --symbols AAPL MSFT
```

---

## 🔄 版本历史

### 2025-10-22 (v2.0)
- ✅ 新增 Prometheus + Grafana 实时监控
- ✅ 新增30+个监控指标
- ✅ 新增10个自动告警规则
- ✅ 新增 Grafana Dashboard（9个面板）
- ✅ 日志中输出进度统计（ETA、速率）

### 2025-10-22 (v1.0)
- ✅ 移动到 `apps/backtest/data_prep/` 目录
- ✅ 更新模块导入路径
- ✅ 添加 `MIN_BARS_REQUIRED=30` 指标检查
- ✅ 修改交易日历时间：09:30 → 09:00
- ✅ 修改信号生成时间：09:35 → 09:30



