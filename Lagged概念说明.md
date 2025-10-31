# Lagged（滞后数据）概念说明

## 📖 什么是Lagged？

**Lagged** = **"滞后的"、"延迟的"**，指的是基于**历史数据**的信息，而不是当天的实时数据。

---

## 🗄️ Premarket Leaders Lagged表

### 数据结构

```sql
CREATE TABLE premarket_leaders_lagged (
    trade_date DATE,        -- 当前交易日（例如：2025-09-02）
    source_date DATE,       -- 历史交易日（例如：2025-08-29，前5天）
    direction VARCHAR,      -- GAIN（涨幅榜）或 LOSS（跌幅榜）
    rank INT,              -- 排名（1-10）
    symbol VARCHAR,        -- 股票代码
    ret_0928 DECIMAL,      -- 那一天的盘前涨幅（不是今天的！）
    volume_rth BIGINT       -- 那一天的成交量（不是今天的！）
);
```

### 实际数据示例（9月2日）

| trade_date | source_date | direction | rank | symbol | ret_0928 | volume_rth | 含义 |
|------------|-------------|-----------|------|--------|----------|------------|------|
| 2025-09-02 | 2025-08-29 | GAIN | 1 | AFRM | +14.45% | 326,037 | **8月29日**盘前涨幅第1名 |
| 2025-09-02 | 2025-08-29 | GAIN | 2 | GTLB | +1.79% | 30,890 | **8月29日**盘前涨幅第2名 |
| 2025-09-02 | 2025-08-29 | LOSS | 1 | MRVL | -16.19% | 706,792 | **8月29日**盘前跌幅第1名 |
| 2025-09-02 | 2025-08-28 | GAIN | 1 | SNOW | ... | ... | **8月28日**盘前涨幅第1名 |
| ... | ... | ... | ... | ... | ... | ... | 前5天每天都有10只GAIN + 10只LOSS |

**关键点**：
- `ret_0928` 是 **source_date那天的**盘前涨幅，不是trade_date的！
- `volume_rth` 是 **source_date那天的**成交量，不是trade_date的！

---

## 🎯 设计目的

### 1. 记录历史活跃股票

**目的**：记录**前5个交易日**中，每天盘前涨幅/跌幅排名前10的股票。

**逻辑**：
```
for 每一天 in 前5个交易日:
    计算当天的盘前涨幅榜（Top10涨幅 + Top10跌幅）
    记录到premarket_leaders_lagged表
    标记为trade_date的数据（供未来使用）
```

### 2. 预筛选候选股票池

**实盘逻辑**（`apps/rt_engine/top5_service.py`）：

```python
# 第144-153行
lagged_symbols = self._lagged_candidate_symbols(session, et_date)
if lagged_symbols:
    # ✅ 优先使用lagged pool（历史活跃股票）
    candidate_symbols = sorted(lagged_symbols)
else:
    # ❌ 如果没有lagged数据，fallback到scanner或fixed pool
    candidate_symbols = self._get_fixed_pool_symbols()  # 或scanner
```

**筛选条件**：
```python
# 第100-104行
selected = {
    row.symbol.upper()
    for row in rows
    if (row.volume_rth or 0) >= 800_000  # 成交量必须>=80万
}
```

**设计思路**：
- **优先关注最近5天表现活跃的股票**
- 这些股票更可能有**持续的波动性**
- 减少需要扫描的股票数量（性能优化）

---

## ❌ 问题：回测代码误用了Lagged

### 当前回测逻辑（错误）

```python
# apps/backtest/pipeline/premarket.py: 第85-86行

lagged_symbols = self._lagged_symbol_pool(trade_date, universe_symbols)
active_symbols = [s for s in universe_symbols if s in lagged_symbols] if lagged_symbols else universe_symbols
```

**问题**：
1. ✅ 如果lagged有数据 → 只选择**前5天在榜单上的股票**
2. ❌ 如果lagged为空 → 使用全部universe（正确）
3. ❌ **漏掉了当天突然大涨但前5天不在榜单上的股票**

### 示例：9月2日

**实际情况**：
- UVIX: 当天盘前+10.68%（大涨！）
- NEM: 当天盘前+1.05%
- ZS: 当天盘前+0.54%
- HUBS: 当天盘前+0.12%

**Lagged过滤结果**：
- UVIX: ❌ 不在前5天榜单 → 被过滤
- NEM: ❌ 不在前5天榜单 → 被过滤
- ZS: ❌ 不在前5天榜单 → 被过滤
- HUBS: ✅ 在8月29日LOSS榜（但volume=3,617 < 800k）→ 被过滤

**最终Top5**: 0只股票 ❌

---

## ✅ 正确的逻辑应该是

### 方案1：不使用Lagged（推荐用于回测）

```python
# Top5应该只根据当天涨幅选择
active_symbols = universe_symbols  # 直接使用全部股票池

# 然后计算当天盘前涨幅
for symbol in active_symbols:
    ret = (price_0928 - prev_close) / prev_close
    
# 按涨幅排序，取Top10
candidates.sort(key=lambda row: row.ret_preopen, reverse=True)
top10 = candidates[:10]
```

**优点**：
- ✅ 不会漏掉当天大涨的股票
- ✅ 逻辑简单清晰
- ✅ 符合"Top5 = 当天涨幅Top10"的设计意图

### 方案2：Lagged作为参考，不强制过滤（推荐用于实盘）

```python
# Lagged作为预筛选，但不是必须的
lagged_symbols = self._lagged_symbol_pool(trade_date, universe_symbols)

if lagged_symbols:
    # 优先使用lagged，但补充当天大涨的股票
    active_symbols = set(lagged_symbols)
    
    # 补充：计算全部股票的当天涨幅，添加涨幅>X%的股票
    for symbol in universe_symbols:
        ret = calculate_preopen_return(symbol)
        if ret > 0.05:  # 涨幅>5%的股票也加入
            active_symbols.add(symbol)
else:
    # 如果没有lagged数据，使用全部universe
    active_symbols = universe_symbols
```

**优点**：
- ✅ 优先关注历史活跃股票（性能优化）
- ✅ 不遗漏当天突然大涨的股票
- ✅ 适合实盘场景（需要快速响应）

---

## 📊 对比总结

| 维度 | Lagged的作用 | 当前回测逻辑 | 推荐逻辑 |
|------|-------------|------------|---------|
| **设计目的** | 预筛选候选池（性能优化） | ❌ 当作必须条件 | ✅ 作为参考，不强制 |
| **实盘场景** | ✅ 优先使用lagged pool | N/A | ✅ 优先lagged，补充大涨股 |
| **回测场景** | ❌ 不应该使用 | ❌ 错误使用，导致漏股 | ✅ 不使用lagged，直接用全部universe |
| **效果** | 提升性能，关注活跃股 | 漏掉当天大涨股 | 完整覆盖所有股票 |

---

## 🎯 结论

### Lagged的本质

**Lagged** = **历史数据预筛选机制**

- **作用**：记录前5天表现活跃的股票
- **目的**：缩小候选股票池，提升性能
- **适用场景**：实盘环境（需要快速响应）
- **不适用场景**：回测环境（需要完整数据）

### 回测应该怎么做？

**回测Top5生成应该**：
1. ✅ 直接使用全部universe股票
2. ✅ 计算每只股票的**当天盘前涨幅**
3. ✅ 按涨幅排序，取Top10-20
4. ❌ **不使用lagged过滤**

### 实盘应该怎么做？

**实盘Top5生成可以**：
1. ✅ 优先使用lagged pool（历史活跃股票）
2. ✅ 补充当天涨幅>X%的股票（不遗漏大涨股）
3. ✅ 如果没有lagged数据，fallback到scanner或fixed pool

---

**总结**：Lagged是一个**性能优化和预筛选机制**，不应该在回测中作为**必须条件**使用，否则会漏掉当天突然大涨的股票！

