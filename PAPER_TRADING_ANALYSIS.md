# Paper Trading 系统架构差异分析

生成时间: 2025-10-27 22:15 (北京时间)

## 关键发现：设计与实现的差异

###  文档中的设计（理想状态）

根据 `实盘交易完整链路详解.md`，系统应该是**全自动化**的：

```
rt_engine/aggregator
  ├─ 每天09:30:01自动触发Top5选股 → premarket_top5表
  ├─ 每30秒刷新watchlist (从premarket_top5读取)
  ├─ 订阅IBKR实时tick → 聚合K线 → 计算指标
  └─ 发布bars_closed事件到Redis Stream

signal_svc
  ├─ 启动时创建后台任务 _consume_bars_closed()
  ├─ 持续监听Redis Stream: bars_closed
  ├─ 自动消费事件 → 调用signal_engine.process_bar()
  └─ 发布生成的信号到Redis Stream: signals

exec_svc
  ├─ 监听Redis Stream: signals
  └─ 自动执行交易
```

### 🔴 实际实现的状态

检查当前代码后发现：

1. **rt_engine/aggregator** ✅ 已实现
   - 定时Top5选股：✅ 实现（第1758-1819行）
   - watchlist自动刷新：✅ 实现（第1516-1633行）
   - bars_closed事件发布：✅ 实现（第1192-1225行）

2. **signal_svc** ❌ **未实现自动消费**
   - 当前只有API端点：`/engine/process`
   - 没有启动后台任务 `_consume_bars_closed()`
   - **文档中的自动消费代码不存在于实际代码中**

3. **exec_svc** ？ 未检查

## 当前的工作流程（推测）

基于实际代码，可能的流程是：

### 方案A：缺失的自动消费者（需要实现）
```
rt_engine/aggregator → Redis Stream → ❌ 无人消费
```

### 方案B：可能的外部编排（未发现）
```
rt_engine/aggregator → Redis Stream → ？某个服务？ → 调用signal_svc API
```

### 方案C：聚合器直接调用（更符合"No-Mock"理念）
```
rt_engine/aggregator → 直接HTTP调用signal_svc API → 生成信号
```

## 你提出的问题完全正确

你指出的核心问题：

1. ✅ **盘前选股应该是自动的** - 代码中已实现（aggregator第1758行）
2. ✅ **不需要手动传入标的** - Top5从premarket_top5表自动读取
3. ❌ **signal_svc缺少自动消费逻辑** - 这是当前系统的缺陷

## 我之前的测试错误

我之前做的测试：
- ❌ 手动调用 `/top5/trigger` - 这是调试接口，不是正常流程
- ❌ 手动传入symbol测试 `/signals/preview` - 这是回测接口

这些都不符合系统的自动化设计理念。

## 需要补充的实现

### 1. signal_svc添加自动消费者

```python
# apps/signal_svc/main.py

@app.on_event("startup")
async def startup_event() -> None:
    if settings.app_env == "test":
        LOGGER.debug("Skipping redis ping in test environment")
        return
    await redis_bus.ping()
    
    # 🆕 添加：启动bars_closed消费者
    asyncio.create_task(_consume_bars_closed())


async def _consume_bars_closed() -> None:
    """后台任务：持续消费bars_closed事件"""
    try:
        # 创建消费者组
        await redis_bus.ensure_group('bars_closed', 'signal_svc', start_id='0')
        
        LOGGER.info("signal_svc.consumer_started", stream="bars_closed")
        
        while True:
            try:
                entries = await redis_bus.consume(
                    stream='bars_closed',
                    group='signal_svc',
                    consumer='signal-worker',
                    count=10,
                    block_ms=5000
                )
                
                for entry in entries:
                    try:
                        event_data = entry.get('payload', {})
                        event = BarsClosed(**event_data)
                        
                        # 调用引擎处理
                        signals = signal_engine.process_bar(event)
                        
                        # 广播到WebSocket
                        if signals:
                            await signal_stream.broadcast(
                                _build_signal_message(signals, source="bars_closed")
                            )
                        
                        # 发布生成的信号到Redis（给exec_svc消费）
                        for signal in signals:
                            await redis_bus.publish(
                                stream='signals',
                                payload=signal.model_dump(mode='json'),
                                trace_id=signal.trace_id
                            )
                        
                        # ACK消息
                        await redis_bus.ack(
                            stream='bars_closed',
                            group='signal_svc',
                            message_id=entry['id']
                        )
                        
                    except Exception:
                        LOGGER.exception("signal_svc.process_bar_failed", entry=entry)
                        
            except Exception:
                LOGGER.exception("signal_svc.consume_failed")
                await asyncio.sleep(5)  # 错误后等待重试
                
    except asyncio.CancelledError:
        LOGGER.info("signal_svc.consumer_stopped")
        raise
```

### 2. exec_svc添加自动消费者（类似）

### 3. 完整的自动化链路

```
实时数据 → aggregator → bars_closed → signal_svc → signals → exec_svc → 下单
   ↑            ↑            ↑             ↑           ↑         ↑
  IBKR      定时Top5      Redis        自动消费     Redis    自动消费
```

## 总结

你的理解是完全正确的！系统的**设计理念**是：

1. ✅ **自动化** - 不需要手动触发
2. ✅ **数据驱动** - 不需要手动传入标的
3. ❌ **实现不完整** - signal_svc缺少自动消费逻辑

当前系统的状态：
- rt_engine/aggregator：✅ 完整实现自动化
- signal_svc：❌ 缺少自动消费者
- 数据管道：⚠️ bars_closed事件发布了，但无人消费

建议下一步：
1. 补充signal_svc的Redis消费者
2. 补充exec_svc的Redis消费者
3. 测试完整的自动化链路


