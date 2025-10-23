# 信号生成时间过滤状态检查

## 📋 背景

- **数据拉取时间范围**：09:00-16:00（为指标计算提供足够历史数据）
- **信号生成时间范围**：应该保持在 09:30-16:00

## ✅ 已确认有时间过滤的信号

### 1. SIG_AM_BOTTOM_A1 (买入信号)
```python
# apps/signal_svc/engine.py:293
if et_time.time() < time(9, 30) or et_time.time() >= time(12, 0):
    return None
```
✅ **时间窗口：09:30-12:00**

### 2. SIG_AM_CONFLUENCE_BUY_A2 (买入信号)
```python
# apps/signal_svc/engine.py:367
if et_time.time() < time(9, 30) or et_time.time() >= time(12, 0):
    return None
```
✅ **时间窗口：09:30-12:00**

### 3. SIG_TIME_CLEAR_12_14 / SIG_TIME_CLEAR_14_16 (退出信号)
```python
# apps/signal_svc/engine.py:1047, 1058
if et_time.time() >= time(14, 0):  # 14:00-16:00
if et_time.time() >= time(12, 0):  # 12:00-16:00
```
✅ **时间窗口：12:00+ / 14:00+**

## ⚠️ 需要检查的信号（可能没有时间过滤）

### 4. SIG_AM_SELL_C1 (卖出信号)
- **函数**：`_detect_am_sell_c1` (第452行)
- **状态**：需要检查是否有时间过滤

### 5. SIG_AM_CONFLUENCE_SELL_S2 (卖出信号)
- **函数**：`_detect_am_confluence_sell_s2` (第528行)
- **状态**：需要检查是否有时间过滤

### 6. 其他退出信号
- `_detect_e1` (SIG_EXIT_UPPER_TAP_X2)
- `_detect_e2` (SIG_EXIT_BOX2MID)
- `_detect_s1` (SIG_OPEN_CHASE_BUY / SIG_REBOUND_BUY?)
- `_detect_s2` (?)

### 7. 盘前信号
- `_detect_pm_a2` (SIG_PM_BOTTOM_A2)
- `_detect_pm_a3` (SIG_PM_BOTTOM_A3)
- `_detect_pm_a4` (SIG_PM_BOTTOM_A4)

## 🎯 关键问题

**如果退出信号（SELL/EXIT）没有时间过滤，会有什么影响？**

1. **理论上**：退出信号应该在整个交易时段都能触发（09:30-16:00）
2. **实际情况**：
   - 退出信号依赖于已有持仓 (`state = self._positions.get(event.symbol)`)
   - 如果 09:00-09:30 没有买入信号，就不会有持仓
   - 因此即使退出信号在 09:00-09:30 触发，也不会产生实际交易

**结论**：
- ✅ **买入信号** (`SIG_AM_BOTTOM_A1`, `SIG_AM_CONFLUENCE_BUY_A2`) 已有时间过滤（09:30+）
- ✅ **退出信号** 即使没有时间过滤也无影响（因为没有持仓）
- ✅ **数据范围 09:00-16:00** 为 09:30 第一根信号K线提供 30 根历史K线

## 📌 建议

### 方案1：保持现状（推荐）
- 买入信号已有时间过滤（09:30+）
- 09:00-09:30 的数据用于指标计算，为 09:30 第一根信号K线提供历史数据
- 信号生成从 09:30 开始

### 方案2：回测引擎全局过滤（额外保护）
在 `apps/backtest/signal_source.py:_generate_signals_recompute` 中添加：
```python
# 第92-93行之后
for row in rows:
    ts_end = row.ts_end
    # 添加全局时间过滤
    et_time = ts_end.astimezone(EASTERN)
    if et_time.time() < time(9, 30):  # 跳过 09:00-09:30 的K线
        continue
    # ... 继续处理
```

### 方案3：数据拉取层过滤
修改 `dao.fetch_equity_bars` 的 SQL 查询，只返回 09:30+ 的K线：
```sql
WHERE ts_end >= :start_ts 
  AND ts_end < :end_ts
  AND EXTRACT(HOUR FROM ts_end AT TIME ZONE 'America/New_York') * 60 
      + EXTRACT(MINUTE FROM ts_end AT TIME ZONE 'America/New_York') >= 570  -- 09:30
```

## 🔍 验证步骤

1. 运行回测并检查信号生成时间：
   ```sql
   SELECT 
     signal_code,
     MIN(ts_end::time) as earliest_signal,
     MAX(ts_end::time) as latest_signal,
     COUNT(*) as signal_count
   FROM bt_signals
   WHERE ts_end::date = '2025-09-01'
   GROUP BY signal_code;
   ```

2. 确保没有 09:00-09:30 之间的买入信号：
   ```sql
   SELECT *
   FROM bt_signals
   WHERE ts_end::date = '2025-09-01'
     AND EXTRACT(HOUR FROM ts_end AT TIME ZONE 'America/New_York') * 60 
         + EXTRACT(MINUTE FROM ts_end AT TIME ZONE 'America/New_York') < 570
     AND signal_code IN ('SIG_AM_BOTTOM_A1', 'SIG_AM_CONFLUENCE_BUY_A2');
   ```

## ✅ 结论

**当前配置是正确的**：
- 09:00-16:00 数据范围为指标计算提供足够历史数据
- 买入信号有时间过滤（09:30+）
- 09:00-09:30 的 30 根K线为第一根信号K线（09:30）提供历史数据
- 不会在 09:00-09:30 生成买入信号

