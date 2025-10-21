# 数据流转完整路径：回测 vs Paper/Live

**生成时间:** 2025-10-21  
**适用版本:** option_us_strategy v1.0

---

## 一、三种模式对比总览

| 维度 | 回测 (Backtest) | Paper Trading | Live Trading |
|------|----------------|---------------|--------------|
| **触发方式** | 🔴 **手动触发** | 🟢 **自动运行** | 🟢 **自动运行** |
| **启动命令** | `python apps/backtest/main.py ...` | `uvicorn apps.rt_engine.main:app --port 8002` | `uvicorn apps.rt_engine.main:app --port 8002` |
| **数据来源** | TimescaleDB (历史) | IBKR实时流 + DB | IBKR实时流 + DB |
| **K线来源** | `bars1m_equity` 表 | IBKR tick聚合 → DB | IBKR tick聚合 → DB |
| **指标来源** | `indicators_eq_1m` 表 | 实时计算 → DB | 实时计算 → DB |
| **补数方式** | IBKR `reqHistoricalData` (手动触发) | IBKR `reqHistoricalData` (自动) | IBKR `reqHistoricalData` (自动) |
| **订单执行** | 模拟（不下单） | 模拟（不下单） | 🔴 **真实下单** |
| **IBKR连接** | 按需连接（补数时） | 持续连接 | 持续连接 |
| **运行模式** | 一次性任务 | 常驻服务 | 常驻服务 |
| **配置区分** | `--track equity/option` | `PAPER=1` in .env | `PAPER=0` in .env |

---

## 二、回测模式（Backtest）- 手动触发

### 2.1 启动方式
```bash
# 手动命令行启动
python apps/backtest/main.py \
  --start 2025-10-01T09:00:00-04:00 \
  --end 2025-10-08T17:00:00-04:00 \
  --signal-mode recompute \
  --track equity  # 或 option
```

### 2.2 完整文件路径流程

```
┌─────────────────────────────────────────────────────────────────┐
│  入口: apps/backtest/main.py                                    │
│  ├─ parse_args()           解析命令行参数                        │
│  ├─ Universe筛选           选择股票池                           │
│  └─ 循环每个symbol         调用 run_backtest()                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  执行: apps/backtest/bt_runner.py                               │
│  └─ run_backtest()                                              │
│     ├─ 创建 BacktestDAO                                         │
│     ├─ 写入 bt_runs 表                                          │
│     └─ 根据 track 调用:                                         │
│        ├─ _run_equity_track()   (equity模式)                    │
│        └─ _run_option_track()   (option模式)                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  数据获取: apps/backtest/dao.py                                 │
│  └─ BacktestDAO.fetch_equity_bars()                             │
│     ├─ 读取 libs/db/queries_backtest.sql                        │
│     ├─ 执行 SQL: select_equity_bars                             │
│     │  SELECT b.*, i.* FROM bars1m_equity b                     │
│     │  JOIN indicators_eq_1m i                                  │
│     │  ON b.symbol=i.symbol AND b.ts_end=i.ts_end              │
│     │  WHERE symbol=:symbol AND ts_end BETWEEN :start:end      │
│     └─ 返回 List[EquityBarRow]                                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  缺口检测: apps/backtest/datafeed/timescale_equity.py          │
│  └─ TimescaleEquityData.from_timescale()                        │
│     ├─ dao.fetch_equity_bars()     获取数据                     │
│     ├─ _scan_equity_gaps()         扫描缺口                     │
│     │  ├─ 检查price_gaps          价格K线缺失                   │
│     │  ├─ 检查indicator_gaps      指标NULL                     │
│     │  └─ 检查prev_close_missing  前日收盘价                   │
│     └─ 如有缺口 → 触发补数                                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                  有缺口 & auto_fill=True?
                    /          \
                  是             否
                  │              │
                  ▼              ▼
    ┌───────────────────────┐   直接使用
    │  补数流程（按需连接）   │   现有数据
    └──────┬────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│  IBKR连接: libs/infra/ibkr_client.py                            │
│  └─ build_ibkr_client(settings)                                 │
│     ├─ IBClient.__init__()                                      │
│     ├─ connect()                  连接IBKR                      │
│     │  ├─ super().connect(host, port, clientId)                │
│     │  ├─ 等待 nextValidId        (10秒超时)                    │
│     │  └─ 启动reader线程                                        │
│     └─ 返回已连接的IBClient实例                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  历史数据拉取: libs/infra/ibkr_client.py                        │
│  └─ IBClient.req_historical_1m()                                │
│     ├─ stock_contract()          构建股票合约                   │
│     ├─ req_historical_1m_contract()                             │
│     │  ├─ 计算duration          gap前后+60分钟                  │
│     │  ├─ 格式化end_str         "YYYYMMDD HH:MM:SS US/Eastern" │
│     │  ├─ reqHistoricalData()   IBKR API调用                   │
│     │  │  - barSizeSetting: "1 min"                            │
│     │  │  - whatToShow: "TRADES" 或 "MIDPOINT"                 │
│     │  │  - useRTH: True (仅RTH) 或 False (含盘前盘后)         │
│     │  └─ 等待 historicalData 回调                             │
│     └─ 返回 List[Dict] 历史K线                                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  数据入库: apps/backtest/dao.py                                 │
│  └─ BacktestDAO.upsert_equity_bars_from_df()                    │
│     ├─ 遍历DataFrame每一行                                      │
│     └─ 执行 SQL: upsert_equity_bars                             │
│        INSERT INTO bars1m_equity (symbol, ts_end, open, high,   │
│            low, close, volume)                                  │
│        VALUES (...) ON CONFLICT (symbol, ts_end) DO UPDATE      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  指标计算: apps/rt_engine/indicators.py                         │
│  └─ compute_indicators(df_bars, baseline_map)                   │
│     ├─ 读取 rvol_baseline_eq        获取RVOL基线                │
│     │  dao.fetch_rvol_baseline(symbol)                          │
│     ├─ 计算13个指标:                                             │
│     │  ├─ RSI(6/12/24)           ta.momentum.RSIIndicator       │
│     │  ├─ ATR(14)                ta.volatility.AverageTrueRange │
│     │  ├─ AO(5,34)               ta.momentum.AwesomeOscillator  │
│     │  ├─ Stoch(14,3)            ta.momentum.Stochastic         │
│     │  ├─ CCI(14/6)              ta.trend.CCIIndicator          │
│     │  ├─ OBV                    ta.volume.OnBalanceVolume      │
│     │  ├─ MFI(14)                ta.volume.MFIIndicator         │
│     │  └─ RVOL(6)                _compute_rvol_series() ⚠️      │
│     └─ 返回 DataFrame[indicators]                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  指标入库: apps/backtest/dao.py                                 │
│  └─ BacktestDAO.upsert_equity_indicators_from_df()              │
│     └─ 执行 SQL: upsert_equity_indicators                       │
│        INSERT INTO indicators_eq_1m (symbol, ts_end, rsi6, ...) │
│        VALUES (...) ON CONFLICT (symbol, ts_end) DO UPDATE      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  二次校验: apps/backtest/datafeed/timescale_equity.py          │
│  ├─ 重新读取 dao.fetch_equity_bars()                            │
│  ├─ _scan_equity_gaps()            再次检查                     │
│  ├─ 如仍有缺口 → 写入 DATA_GAP_PERSIST 风险事件                 │
│  └─ 如无缺口 → 写入 DATA_GAP_FILLED 成功事件                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  断开IBKR: libs/infra/ibkr_client.py                            │
│  └─ IBClient.disconnect_and_stop()                              │
│     ├─ disconnect()                关闭连接                     │
│     └─ 停止reader线程                                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  回测执行: apps/backtest/bt_runner.py                           │
│  └─ _run_equity_track() 或 _run_option_track()                  │
│     ├─ 加载信号 load_signals()                                  │
│     ├─ 遍历每个信号                                             │
│     ├─ 模拟交易（不实际下单）                                    │
│     ├─ 计算收益指标                                             │
│     ├─ 写入 bt_trades 表                                        │
│     ├─ 写入 bt_signals 表                                       │
│     └─ 写入 bt_metrics_* 表                                     │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 关键文件清单

**回测特有文件:**
```
apps/backtest/
├── main.py                          # 入口，解析参数
├── bt_runner.py                     # 回测执行器
├── dao.py                           # 数据访问层
├── signal_source.py                 # 信号加载
├── datafeed/
│   ├── timescale_equity.py          # K线+指标获取，缺口检测
│   └── timescale_option.py          # 期权数据获取
├── broker/
│   └── commission_ib.py             # 佣金模拟
├── strategy/
│   └── option_signal_strategy.py    # 策略逻辑
└── analyzers/
    └── metrics_writer.py            # 指标计算

libs/db/
├── queries_backtest.sql             # SQL查询模板
└── dao.py                           # 通用DAO

libs/infra/
└── ibkr_client.py                   # IBKR客户端（补数时用）

apps/rt_engine/
└── indicators.py                    # 指标计算库（共用）
```

---

## 三、Paper/Live模式 - 自动运行

### 3.1 启动方式
```bash
# Paper模式
PAPER=1 uvicorn apps.rt_engine.main:app --port 8002 --env-file .env

# Live模式
PAPER=0 uvicorn apps.rt_engine.main:app --port 8003 --env-file .env
```

**或使用Docker:**
```bash
docker-compose up rt-engine
```

### 3.2 完整文件路径流程

```
┌─────────────────────────────────────────────────────────────────┐
│  入口: apps/rt_engine/main.py                                   │
│  └─ FastAPI应用启动                                             │
│     ├─ @app.on_event("startup")                                 │
│     ├─ 读取配置 get_settings()                                  │
│     ├─ 连接数据库 create_engine()                               │
│     ├─ 连接Redis RedisBus()                                     │
│     └─ 启动后台任务 asyncio.create_task()                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  IBKR持续连接: libs/infra/ibkr_client.py                        │
│  └─ build_ibkr_client(settings)                                 │
│     ├─ connect() 并保持连接                                     │
│     ├─ 自动重连逻辑                                             │
│     └─ 心跳检测                                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  聚合器启动: apps/rt_engine/aggregator.py                       │
│  └─ MinuteAggregator.__init__()                                 │
│     ├─ 初始化缓冲区 _buffers                                    │
│     ├─ 初始化报价缓存 _quotes                                   │
│     ├─ 初始化RVOL基线缓存 _baseline_cache                       │
│     └─ 启动订阅任务                                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  实时订阅: apps/rt_engine/aggregator.py                         │
│  └─ MinuteAggregator._subscribe_symbols()                       │
│     ├─ 从watchlist读取活跃股票                                  │
│     ├─ 对每个symbol:                                            │
│     │  ├─ ib.reqMktData(contract, genericTicks="233")          │
│     │  │  - 订阅bid/ask/last/volume/exchange                   │
│     │  └─ 注册回调 handleTickPrice, handleTickString           │
│     └─ 订阅VIX: ib.reqMktData(vix_contract)                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Tick数据接收: libs/infra/ibkr_client.py                        │
│  └─ IBClient 回调方法:                                          │
│     ├─ tickPrice(tickerId, tickType, price, attrib)            │
│     │  - 处理bid/ask/last价格更新                              │
│     ├─ tickString(tickerId, tickType, value)                   │
│     │  - 处理RTVolume (58号tick)                               │
│     │  - 格式: "price;size;time;totalVolume;VWAP;single"       │
│     └─ 将tick推送到 _symbol_queues                              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  1分钟聚合: apps/rt_engine/aggregator.py                        │
│  └─ MinuteAggregator._aggregate_symbol()                        │
│     ├─ 从 _symbol_queues 读取tick                               │
│     ├─ 按分钟边界聚合:                                          │
│     │  ├─ open: 该分钟第一个tick价格                           │
│     │  ├─ high: 该分钟最高价                                   │
│     │  ├─ low: 该分钟最低价                                    │
│     │  ├─ close: 该分钟最后一个tick价格                        │
│     │  └─ volume: 该分钟累计成交量                             │
│     ├─ 如无成交:                                                │
│     │  └─ 使用NBBO mid填充 O/H/L/C (volume=0)                  │
│     └─ 生成 AggregatedBar                                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  K线入库: apps/rt_engine/aggregator.py                          │
│  └─ MinuteAggregator._persist_bar()                             │
│     └─ 执行 SQL: INSERT INTO bars1m_equity                      │
│        (symbol, ts_end, open, high, low, close, volume, session)│
│        VALUES (...) ON CONFLICT DO UPDATE                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  指标计算: apps/rt_engine/aggregator.py                         │
│  └─ MinuteAggregator._update_indicators()                       │
│     ├─ 从数据库读取最近120根K线                                 │
│     │  SELECT ts_end, open, high, low, close, volume            │
│     │  FROM bars1m_equity                                       │
│     │  WHERE symbol=:symbol AND ts_end<=:ts_end                 │
│     │  ORDER BY ts_end DESC LIMIT 120                           │
│     ├─ 转换为DataFrame                                          │
│     ├─ 获取RVOL基线                                             │
│     │  baseline_map = self._get_baseline_map(session, symbol)   │
│     ├─ 调用指标计算                                             │
│     │  indicators = compute_indicator_row(df, baseline_map)     │
│     │  # 位于 apps/rt_engine/indicators.py                      │
│     └─ 写入数据库                                               │
│        INSERT INTO indicators_eq_1m (symbol, ts_end, ...)       │
│        VALUES (...) ON CONFLICT DO UPDATE                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  事件发布: apps/rt_engine/aggregator.py                         │
│  └─ MinuteAggregator._broadcast_bar_closed()                    │
│     ├─ 构建 BarsClosed 事件                                     │
│     ├─ 发布到Redis: PUBLISH bars_closed                         │
│     └─ 下游服务订阅此事件触发信号生成                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  自动缺口回补: apps/rt_engine/aggregator.py                     │
│  └─ MinuteAggregator._check_and_backfill()                      │
│     ├─ 检测是否有分钟缺口                                       │
│     │  - 比较当前ts_end与上次ts_end                             │
│     │  - 如果间隔>1分钟 → 有缺口                                │
│     ├─ 如缺口≤15分钟:                                           │
│     │  ├─ ib.req_historical_1m()  获取历史数据                  │
│     │  ├─ 写入 bars1m_equity                                    │
│     │  └─ 计算并写入 indicators_eq_1m                           │
│     └─ 如缺口>15分钟:                                           │
│        ├─ 记录DQ告警                                            │
│        └─ 跳过（不补数）                                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  RVOL基线刷新: apps/rt_engine/aggregator.py                     │
│  └─ MinuteAggregator._maybe_refresh_rvol_baseline()             │
│     ├─ 每天16:10 ET自动触发                                     │
│     ├─ _refresh_rvol_baseline()                                 │
│     │  ├─ 对每个活跃symbol:                                     │
│     │  │  ├─ ib.req_historical_1m()  拉取近40天数据            │
│     │  │  ├─ 按分钟索引计算20日均量                             │
│     │  │  └─ UPSERT INTO rvol_baseline_eq                      │
│     │  └─ 更新 _baseline_cache                                  │
│     └─ 写入日志                                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  盘前Top5: apps/rt_engine/top5_service.py                       │
│  └─ Top5Service.compute_top5()                                  │
│     ├─ 每天09:30:01 ET自动触发                                  │
│     ├─ ib.reqScannerSubscription()  Scanner扫描候选             │
│     ├─ 对每个候选:                                              │
│     │  ├─ ib.req_historical_1m()  拉取09:25 bar               │
│     │  ├─ 计算盘前涨幅: (price_0925 - prev_close) / prev_close│
│     │  └─ 过滤: 市值≥阈值 && M60上行                            │
│     ├─ 排序取Top5                                               │
│     └─ INSERT INTO premarket_top5                               │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 关键文件清单

**Paper/Live共用文件:**
```
apps/rt_engine/
├── main.py                          # FastAPI入口
├── aggregator.py                    # K线聚合器（核心）
├── indicators.py                    # 指标计算
└── top5_service.py                  # 盘前Top5服务

apps/signal_svc/
├── main.py                          # 信号服务入口
└── engine.py                        # 信号生成引擎

apps/risk_svc/
├── main.py                          # 风控服务入口
└── rules.py                         # 风控规则

apps/exec_svc/
├── main.py                          # 执行服务入口
└── order_manager.py                 # 订单管理

libs/infra/
├── ibkr_client.py                   # IBKR客户端（持续连接）
└── redis_bus.py                     # Redis事件总线

libs/db/
├── models.py                        # SQLAlchemy模型
└── dao.py                           # 数据访问对象
```

---

## 四、关键区别详解

### 4.1 数据获取方式

#### 回测（Backtest）
```python
# 从数据库读取历史数据
rows = dao.fetch_equity_bars(symbol=symbol, start_ts=start, end_ts=end)

# SQL查询
SELECT b.*, i.* 
FROM bars1m_equity b
JOIN indicators_eq_1m i ON b.symbol=i.symbol AND b.ts_end=i.ts_end
WHERE b.symbol = 'AAPL' AND ts_end BETWEEN '2025-10-01' AND '2025-10-08'
```

#### Paper/Live
```python
# 实时订阅IBKR tick数据
ib.reqMktData(contract, genericTicks="233")

# tick → 1分钟聚合 → 写入数据库
bars1m_equity ← 实时聚合
indicators_eq_1m ← 实时计算
```

### 4.2 IBKR连接模式

#### 回测（Backtest）
```python
# 按需连接（仅在补数时）
if gap_report.has_any_gap():
    ib = build_ibkr_client(settings)  # 临时连接
    try:
        bars = ib.req_historical_1m(...)
        # ... 补数逻辑
    finally:
        ib.disconnect_and_stop()      # 立即断开
```

**特点:**
- 补数时才连接
- 用完立即断开
- 不占用ClientId

#### Paper/Live
```python
# 启动时连接，持续保持
ib = build_ibkr_client(settings)      # 启动时连接

# 自动重连逻辑
if not ib.is_connected():
    ib.connect()

# 持续订阅，实时接收数据
# ... 永不主动断开（除非服务停止）
```

**特点:**
- 服务启动时连接
- 持续保持连接
- 自动重连
- 长期占用ClientId

### 4.3 指标计算时机

#### 回测（Backtest）
```python
# 一次性批量计算
df_indicators = compute_indicators(
    df_bars,           # 所有历史K线
    baseline_map       # 从rvol_baseline_eq读取
)

# 写入数据库（批量）
dao.upsert_equity_indicators_from_df(symbol, df_indicators)
```

#### Paper/Live
```python
# 每分钟实时计算
def _update_indicators(session, symbol, ts_end):
    # 读取最近120根K线
    rows = session.execute("""
        SELECT * FROM bars1m_equity 
        WHERE symbol=:symbol AND ts_end<=:ts_end 
        ORDER BY ts_end DESC LIMIT 120
    """)
    
    # 计算最新一根的指标
    baseline_map = self._get_baseline_map(session, symbol)
    indicators = compute_indicator_row(df, baseline_map)
    
    # 写入数据库（单行）
    INSERT INTO indicators_eq_1m VALUES (...) 
    ON CONFLICT DO UPDATE
```

### 4.4 RVOL基线刷新

#### 回测（Backtest）
```python
# 手动运行刷新脚本
python refresh_rvol_baseline.py

# 或在补数时如果发现基线缺失，报错中断
baseline_map = dao.fetch_rvol_baseline(symbol)
if not baseline_map:
    LOGGER.warning("RVOL baseline missing for %s", symbol)
    # rvol6 将为 NaN
```

#### Paper/Live
```python
# 每天16:10自动刷新
def _maybe_refresh_rvol_baseline(ts_end):
    et = ts_end.astimezone(EASTERN)
    if et.hour == 16 and et.minute == 10:
        self._refresh_rvol_baseline(trade_date)

# 自动流程
def _refresh_rvol_baseline(trade_date):
    for symbol in active_symbols:
        # 拉取近40天历史数据
        bars = ib.req_historical_1m(symbol, start, end)
        # 计算分钟均量
        baseline_map = compute_baseline(bars)
        # 写入数据库
        UPSERT INTO rvol_baseline_eq (symbol, minute_index, mean_vol_20d)
```

### 4.5 缺口处理

#### 回测（Backtest）
```python
# 检测到缺口 → 手动补数
gap_report = _scan_equity_gaps(df, symbol)
if gap_report.has_any_gap():
    # 写入风险事件
    dao.write_risk_event("DATA_GAP_DETECTED", ...)
    
    # 尝试补数
    if auto_fill_missing:
        ib = build_ibkr_client(settings)
        _fill_equity_gaps_via_ibkr(dao, ib, symbol, gap_report)
        ib.disconnect_and_stop()
    else:
        raise RuntimeError("Data gaps detected")
```

#### Paper/Live
```python
# 每分钟自动检测并补数
def _check_and_backfill(symbol, current_ts):
    last_ts = self._last_persisted.get(symbol)
    
    if last_ts and (current_ts - last_ts).seconds > 60:
        # 检测到缺口
        gap_minutes = (current_ts - last_ts).seconds // 60 - 1
        
        if gap_minutes <= 15:
            # 自动补数
            bars = self._ib_client.req_historical_1m(
                symbol, last_ts, current_ts, use_rth=True
            )
            for bar in bars:
                self._persist_bar(bar)
                self._update_indicators(symbol, bar.ts_end)
        else:
            # 超出窗口，仅记录告警
            LOGGER.error("data_quality_gap_too_large", 
                        symbol=symbol, gap_minutes=gap_minutes)
```

---

## 五、配置文件区分

### 5.1 .env配置

```bash
# IBKR连接配置
IB_HOST=127.0.0.1
IB_PORT=7497                    # TWS: 7497, IB Gateway: 4001/4002
IB_CLIENT_ID=88                 # Paper和Live需要不同的ClientId

# Paper vs Live区分
PAPER=1                         # 1=Paper模式，0=Live模式

# 数据库配置
DATABASE_URL=postgresql://user:pass@localhost:5432/quant

# Redis配置（仅Paper/Live需要）
REDIS_HOST=localhost
REDIS_PORT=6379

# RVOL配置
RVOL_BASELINE_DAYS=20
HIST_BACKFILL_WINDOW_MIN=15     # 自动补数窗口（分钟）

# VIX配置
VIX_REQUIRED=true               # 是否必须有VIX数据
VIX_GATE=20                     # VIX阈值
```

### 5.2 启动脚本区分

```bash
# ============ 回测 ============
# 一次性手动任务
python apps/backtest/main.py \
  --start 2025-10-01T09:00:00-04:00 \
  --end 2025-10-08T17:00:00-04:00 \
  --signal-mode recompute \
  --track equity

# ============ Paper ============
# 常驻服务
export PAPER=1
export IB_CLIENT_ID=88

# 启动rt_engine（K线聚合）
uvicorn apps.rt_engine.main:app --port 8002 &

# 启动signal_svc（信号生成）
uvicorn apps.signal_svc.main:app --port 8003 &

# 启动risk_svc（风控）
uvicorn apps.risk_svc.main:app --port 8004 &

# 启动exec_svc（执行，paper模式不实际下单）
uvicorn apps.exec_svc.main:app --port 8001 &

# ============ Live ============
# 常驻服务（与Paper类似，但PAPER=0）
export PAPER=0
export IB_CLIENT_ID=89          # 使用不同的ClientId

# 启动相同的服务，但exec_svc会实际下单
uvicorn apps.rt_engine.main:app --port 8002 &
uvicorn apps.signal_svc.main:app --port 8003 &
uvicorn apps.risk_svc.main:app --port 8004 &
uvicorn apps.exec_svc.main:app --port 8001 &
```

---

## 六、数据流向对比图

### 6.1 回测模式

```
┌─────────────┐
│ 用户手动触发 │
└──────┬──────┘
       │ python apps/backtest/main.py
       ▼
┌─────────────┐
│ 读取数据库   │  bars1m_equity + indicators_eq_1m (历史)
└──────┬──────┘
       │ 检测缺口
       ▼
┌─────────────┐
│ 按需连IBKR   │  (仅补数时)
└──────┬──────┘
       │ req_historical_1m
       ▼
┌─────────────┐
│ 补数+计算指标│
└──────┬──────┘
       │ 写回DB
       ▼
┌─────────────┐
│ 模拟回测    │  (不下单)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ 生成报告    │  bt_trades, bt_signals, bt_metrics_*
└─────────────┘
```

### 6.2 Paper/Live模式

```
┌─────────────┐
│服务自动启动  │  systemd/supervisor/docker
└──────┬──────┘
       │
       ▼
┌─────────────┐
│持续连接IBKR  │  24/7保持连接
└──────┬──────┘
       │ reqMktData + 订阅
       ▼
┌─────────────┐
│实时tick数据  │  每秒N个tick
└──────┬──────┘
       │ 每分钟聚合
       ▼
┌─────────────┐
│ 写入bars1m  │  bars1m_equity
└──────┬──────┘
       │ 立即计算
       ▼
┌─────────────┐
│写入指标表    │  indicators_eq_1m
└──────┬──────┘
       │ 发布事件
       ▼
┌─────────────┐
│ bars_closed │  Redis事件
└──────┬──────┘
       │
       ├─────────────┐
       ▼             ▼
┌─────────────┐ ┌─────────────┐
│信号生成引擎  │ │盘前Top5服务  │
└──────┬──────┘ └─────────────┘
       │ signals事件
       ▼
┌─────────────┐
│  风控服务   │
└──────┬──────┘
       │ 风控通过
       ▼
┌─────────────┐
│  执行服务   │
└──────┬──────┘
       │
       ├────Paper: 不下单，仅记录
       └────Live: 实际下单到IBKR
```

---

## 七、文件依赖关系

### 7.1 回测模式依赖

```
apps/backtest/main.py
    ├─ apps/backtest/bt_runner.py
    │  ├─ apps/backtest/dao.py
    │  │  └─ libs/db/queries_backtest.sql
    │  ├─ apps/backtest/datafeed/timescale_equity.py
    │  │  ├─ apps/rt_engine/indicators.py  ← 指标计算
    │  │  └─ libs/infra/ibkr_client.py     ← IBKR补数
    │  └─ apps/backtest/signal_source.py
    │     └─ apps/signal_svc/engine.py
    └─ apps/backtest/universe.py
```

### 7.2 Paper/Live模式依赖

```
apps/rt_engine/main.py
    ├─ apps/rt_engine/aggregator.py        ← 核心聚合器
    │  ├─ libs/infra/ibkr_client.py        ← 持续IBKR连接
    │  ├─ apps/rt_engine/indicators.py     ← 指标计算
    │  └─ libs/infra/redis_bus.py          ← 事件总线
    └─ apps/rt_engine/top5_service.py      ← Top5服务

apps/signal_svc/main.py
    ├─ apps/signal_svc/engine.py           ← 信号引擎
    └─ libs/infra/redis_bus.py             ← 订阅bars_closed

apps/risk_svc/main.py
    ├─ apps/risk_svc/rules.py              ← 风控规则
    └─ libs/db/dao.py                      ← 数据访问

apps/exec_svc/main.py
    ├─ apps/exec_svc/order_manager.py      ← 订单管理
    ├─ libs/infra/ibkr_client.py           ← 下单API
    └─ libs/infra/redis_bus.py             ← 订阅signals
```

---

## 八、常见问题

### Q1: 为什么回测需要手动触发？
**A:** 回测是研究工具，需要指定明确的时间范围和参数。自动化没有意义，因为每次回测的目的和参数都不同。

### Q2: Paper和Live如何区分？
**A:** 通过`.env`中的`PAPER`环境变量：
- `PAPER=1`: Paper模式，模拟下单
- `PAPER=0`: Live模式，真实下单

两者使用相同的代码，仅在`exec_svc`中根据`PAPER`标志决定是否实际调用IBKR下单API。

### Q3: 回测能否使用Live的实时数据？
**A:** 不能。回测只能使用数据库中的历史数据。如果需要最新数据，必须先让rt_engine运行一段时间积累数据。

### Q4: RVOL基线必须手动刷新吗？
**A:** 
- **回测**: 必须手动运行`refresh_rvol_baseline.py`
- **Paper/Live**: 每天16:10自动刷新

### Q5: 如何同时运行Paper和Live？
**A:** 使用不同的ClientId和端口：
```bash
# Paper (ClientId=88, 端口8002-8004)
PAPER=1 IB_CLIENT_ID=88 uvicorn apps.rt_engine.main:app --port 8002 &

# Live (ClientId=89, 端口9002-9004)
PAPER=0 IB_CLIENT_ID=89 uvicorn apps.rt_engine.main:app --port 9002 &
```

### Q6: 补数时会占用多长时间？
**A:** 
- **回测**: 取决于缺口大小，通常1-5分钟
- **Paper/Live**: 自动补数，≤15分钟缺口实时补齐

### Q7: 为什么Paper/Live需要Redis？
**A:** 用于服务间事件通信（bars_closed、signals、executions等）。回测不需要因为是单进程运行。

---

## 九、监控和运维

### 9.1 回测监控
```bash
# 查看回测进度
tail -f backtest.log | grep "backtest.completed"

# 查看补数情况
grep "DATA_GAP" backtest.log

# 查看IBKR连接
grep "IBKR\|ibkr" backtest.log
```

### 9.2 Paper/Live监控
```bash
# 查看服务健康状态
curl http://localhost:8002/healthz  # rt_engine
curl http://localhost:8003/healthz  # signal_svc
curl http://localhost:8004/healthz  # risk_svc
curl http://localhost:8001/healthz  # exec_svc

# 查看实时日志
docker-compose logs -f rt-engine

# 查看Redis事件流
redis-cli MONITOR | grep bars_closed

# 查看数据库实时写入
watch -n 1 "psql -c 'SELECT MAX(ts_end) FROM bars1m_equity'"
```

---

**文档版本:** v1.0  
**最后更新:** 2025-10-21  
**维护者:** System  
**相关文档:**
- `K线和指标数据获取流程详解.md`
- `指标缺失问题分析与修复方案.md`
- `数据管道与预处理开发文档.md`

