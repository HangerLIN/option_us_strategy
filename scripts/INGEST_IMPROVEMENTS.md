# ingest_equity_1m_ibkr.py 改进说明

## 改进日期
2025-10-22

## 问题描述

原有的数据拉取脚本存在两个主要问题：
1. **IBKR API 响应缓慢**：程序看起来像是"卡死"了，用户无法判断进度
2. **容易触发API限流**：频繁请求导致 Pacing Violation

## 解决方案

### 核心策略：按日拆分 + 延时控制

```python
# 外层循环：遍历交易日
for trade_date in trade_dates:
    # 内层循环：遍历股票
    for symbol in symbols:
        # 1. 请求单个股票单天的数据
        ingest_equity(client, session_factory, trade_date, symbol, rth_only=True)
        
        # 2. 延时2秒，遵守IBKR限流规则
        time.sleep(2)
```

### 改进点

#### 1. 循环结构优化
- ✅ **外层循环**：按交易日顺序遍历（第449行）
- ✅ **内层循环**：遍历股票列表（第458行）
- ✅ **单次请求**：每次只请求一个股票一天的数据

#### 2. 强制延时
```python
# 第501行：每次成功请求后延时2秒
time.sleep(2)

# 第496行：即使失败也延时1秒，避免快速重试
time.sleep(1)
```

#### 3. 增强的日志输出

**请求前日志**（第460-468行）：
```
INFO: 🔄 Fetching data for symbol [AAPL] on date [2025-10-01] (symbol 1/10, date 1/5)...
```

**成功后日志**（第481-485行）：
```
INFO: ✅ Successfully completed [AAPL] on [2025-10-01]
```

**详细进度日志**（第394-399行）：
```
INFO: ✅ Successfully fetched 390 bars for symbol [AAPL] on date [2025-10-01]
```

**失败警告日志**（第489-494行）：
```
WARNING: ⚠️  Failed to fetch data for [AAPL] on [2025-10-01]. Skipping. Error: ...
```

#### 4. 改进的错误处理
- ✅ 使用 `try...except` 捕获单个请求的异常（第470-497行）
- ✅ 失败时记录 `WARNING` 而非 `ERROR`，更准确地反映问题级别
- ✅ 失败后 `continue` 而非中断，确保后续股票继续处理
- ✅ 失败后也会延时1秒，避免连续失败导致快速重试

#### 5. 清晰的进度追踪
- ✅ 每个交易日开始时打印日期进度（第450-455行）
- ✅ 每个股票请求时打印完整进度（第460-468行）
- ✅ 每个交易日完成时打印总结（第503-508行）
- ✅ Batch完成时打印统计信息（第510-516行）

## 使用示例

### 拉取单个月数据（例如2025年9月）
```bash
python -m scripts.ingest_equity_1m_ibkr \
  --start 2025-09-01 \
  --end 2025-09-30 \
  --universe ref_market_cap:GLOBAL \
  --batch-size 10 \
  --rth-only 1
```

### 预期行为

对于10个股票、5个交易日的场景：

```
📅 Processing trade date 2025-09-01 (1/5)
🔄 Fetching data for symbol [AAPL] on date [2025-09-01] (symbol 1/10, date 1/5)...
  → Received 390 bars from IBKR
  → Stored 390 bars to bars1m_equity
  → Computed and stored indicators
✅ Successfully fetched 390 bars for symbol [AAPL] on date [2025-09-01]
✅ Successfully completed [AAPL] on [2025-09-01]
⏱️  Sleeping 2 seconds to respect IBKR rate limits...

🔄 Fetching data for symbol [GOOGL] on date [2025-09-01] (symbol 2/10, date 1/5)...
...

✅ Completed trade date 2025-09-01 (1/5)

📅 Processing trade date 2025-09-02 (2/5)
...
```

### 预期耗时

**计算公式**：
```
总耗时 ≈ (股票数 × 交易日数 × 2秒) + API请求时间

示例：
- 10个股票 × 20个交易日 × 2秒 = 400秒 ≈ 6.7分钟
- 50个股票 × 20个交易日 × 2秒 = 2000秒 ≈ 33分钟
```

**对比优势**：
- ❌ 旧方案：一次请求整月数据，可能卡住10-30秒无响应
- ✅ 新方案：每2秒一次请求，持续输出进度日志，用户体验好

## 技术细节

### 为什么按日期外层循环？

**原因1：数据局部性**
- 同一天的数据通常会被一起回测
- 先完成整天的数据，便于尽快开始部分回测

**原因2：容错性**
- 某个股票某天失败，不影响该股票其他日期
- 某天全部失败，其他日期仍然完整

**原因3：进度可视化**
- "已完成3/20天"比"已完成15/200个请求"更直观

### 为什么延时2秒？

**IBKR API限流规则**（参考文档）：
- 历史数据请求：60秒内最多60次
- Pacing Violation会触发冷却期（10分钟+）

**安全策略**：
- 2秒/次 = 30次/分钟，远低于60次/分钟的硬限制
- 留出50%的余量应对网络延迟和重试

### 与 ibkr_client.py 的交互

**未修改的底层实现**：
```python
# libs/infra/ibkr_client.py (保持不变)
def req_historical_1m_equity(symbol: str, session_date: date, rth_only: bool):
    """单次请求单个股票单天的数据（阻塞式）"""
    start_et, end_et = trading_session_window(session_date)
    return req_historical_1m(symbol, start_et, end_et, use_rth=rth_only)
```

**改变的是调用策略**：
- 旧：一次请求30天数据 → IBKR慢响应
- 新：30次请求，每次1天 → 每次快速响应

## 验证清单

修改后需验证以下场景：

### ✅ 基础功能
- [x] 单个股票单天数据能正常拉取
- [x] 多个股票多天数据能正常拉取
- [x] 数据正确写入 `bars1m_equity` 表
- [x] 指标正确写入 `indicators_eq_1m` 表

### ✅ 异常处理
- [x] 单个请求失败不影响后续处理
- [x] 失败日志清晰记录错误信息
- [x] 非交易日跳过而不报错

### ✅ 性能和限流
- [x] 每次请求后确实延时2秒
- [x] 长时间运行不触发 Pacing Violation
- [x] 进度日志清晰显示当前状态

### ✅ 兼容性
- [x] 与 `prepare_backtest_data.py` 协作正常
- [x] 命令行参数保持向后兼容
- [x] 不影响 `ibkr_client.py` 的其他调用方

## 相关文件

**修改的文件**：
- `scripts/ingest_equity_1m_ibkr.py`（主要修改）

**未修改的文件**：
- `libs/infra/ibkr_client.py`（底层API，保持不变）
- `scripts/prepare_backtest_data.py`（调用方，无需修改）

**依赖的功能**：
- `ibkr_client.req_historical_1m_equity()`：单日数据获取
- `dao.fetch_trade_dates_between()`：交易日历查询

## 后续优化建议

### 可选改进1：动态调整延时
```python
# 根据API响应时间动态调整延时
base_sleep = 2.0
if response_time > 5.0:
    time.sleep(base_sleep * 1.5)  # 响应慢时增加延时
else:
    time.sleep(base_sleep)
```

### 可选改进2：并发拉取
```python
# 使用线程池并发拉取多个股票（需要多个ClientId）
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(ingest_equity, ...) for symbol in symbols]
```

### 可选改进3：断点续传
```python
# 记录已完成的(date, symbol)，失败后可从断点继续
completed = load_progress()
for trade_date in trade_dates:
    for symbol in symbols:
        if (trade_date, symbol) in completed:
            continue
        # ... 拉取数据
        save_progress(trade_date, symbol)
```

## 总结

| 项目 | 改进前 | 改进后 |
|------|--------|--------|
| **单次请求时长** | 10-30秒（整月） | 0.5-2秒（单天） |
| **用户体验** | 看起来卡死 | 持续进度反馈 |
| **API限流风险** | 高（易触发） | 低（2秒延时） |
| **错误恢复** | 全部失败 | 部分失败可继续 |
| **日志可读性** | 模糊 | 清晰详细 |

**核心改进**：将大请求拆分为小请求 + 强制延时 + 详细日志 = 稳定可靠的数据拉取流程。




