# 🔄 Redis 连接实现指南

## 📋 项目中的 Redis 架构

### 1. 配置方式

**环境变量配置** (`.env` 文件)：
```bash
REDIS_URL=redis://localhost:6379/0
```

**代码配置** (`libs/core/config.py`)：
```python
class Settings(BaseSettings):
    redis_url: str = Field(..., validation_alias="REDIS_URL")
```

### 2. 核心实现 (`libs/infra/redis_bus.py`)

#### RedisBus 类特性
- ✅ **基于 Redis Streams** 的事件总线
- ✅ **异步支持** (redis.asyncio)
- ✅ **去重机制** (trace_id 防重复)
- ✅ **消费者组** 支持
- ✅ **自动重连** 和错误处理

#### 核心方法

```python
class RedisBus:
    def __init__(self, url: str, *, dedupe_ttl: int = 3600, maxlen: int = 10000):
        self._redis = Redis.from_url(url, encoding="utf-8", decode_responses=True)
    
    # 发布事件
    async def publish(self, event: str, payload: dict, *, trace_id: str) -> str
    
    # 消费事件  
    async def consume(self, event: str, group: str, consumer: str) -> list[dict]
    
    # 确保消费者组存在
    async def ensure_group(self, event: str, group: str) -> None
    
    # 连接测试
    async def ping(self) -> bool
    
    # 关闭连接
    async def close(self) -> None
```

## 🚀 使用示例

### 1. 基础连接测试

```python
from libs.core import get_settings
from libs.infra.redis_bus import RedisBus

async def test_connection():
    settings = get_settings()
    bus = RedisBus(settings.redis_url)
    
    # 测试连接
    is_connected = await bus.ping()
    print(f"Redis 连接状态: {is_connected}")
    
    await bus.close()
```

### 2. 发布事件

```python
async def publish_event():
    settings = get_settings()
    bus = RedisBus(settings.redis_url)
    
    # 发布风险参数重载事件
    event_data = {
        "trace_id": "risk-reload-12345",
        "triggered_by": "admin",
        "reload_at": "2025-10-16T15:00:00Z"
    }
    
    entry_id = await bus.publish(
        "risk_param_reload",
        event_data,
        trace_id="risk-reload-12345"
    )
    
    print(f"事件已发布: {entry_id}")
    await bus.close()
```

### 3. 消费事件 (服务端)

```python
async def consume_events():
    settings = get_settings()
    bus = RedisBus(settings.redis_url)
    
    # 确保消费者组存在
    await bus.ensure_group("risk_param_reload", "risk_svc")
    
    while True:
        # 消费事件
        entries = await bus.consume(
            event="risk_param_reload",
            group="risk_svc", 
            consumer="risk-params",
            count=10,
            block_ms=5000
        )
        
        for entry in entries:
            trace_id = entry["trace_id"]
            payload = entry["payload"]
            print(f"处理事件: {trace_id} -> {payload}")
            
            # 处理业务逻辑
            await handle_risk_reload(payload)
```

## 🏗️ 项目中的实际应用

### 1. 风险参数热更新

**发送端** (`scripts/send_risk_param_reload.py`)：
```python
async def _publish(triggered_by: str, trace_id: str):
    settings = get_settings()
    bus = RedisBus(settings.redis_url)
    
    event = RiskParamReload(
        trace_id=trace_id,
        triggered_by=triggered_by,
        reload_at=utc_now()
    )
    
    entry_id = await bus.publish(
        "risk_param_reload",
        event.model_dump(mode="json"),
        trace_id=event.trace_id
    )
    
    await bus.close()
```

**接收端** (`apps/risk_svc/subscribers.py`)：
```python
class RiskSubscribers:
    def __init__(self, bus: RedisBus, ...):
        self._bus = bus
    
    async def ensure_groups(self):
        await self._bus.ensure_group("risk_param_reload", "risk_svc")
    
    async def _handle_param_reload(self, entry: dict):
        # 重新加载风险参数
        await self._on_limits_reloaded()
```

### 2. 支持的事件类型

项目中定义的 Redis Streams：

| Stream | 用途 | 消费者组 |
|--------|------|----------|
| `risk_param_reload` | 风险参数热更新 | `risk_svc` |
| `bars_closed` | 分钟线数据更新 | `risk_svc` |
| `signals` | 交易信号 | `risk_svc` |
| `execution_fills` | 成交回报 | `risk_svc` |

## 🛠️ 配置和部署

### 1. 环境要求

```bash
# 安装 Redis
brew install redis

# 启动 Redis
brew services start redis

# 或者使用 Docker
docker run -d -p 6379:6379 redis:alpine
```

### 2. Python 依赖

```toml
# pyproject.toml
dependencies = [
    "redis>=5.0.3",  # Redis 异步客户端
]
```

### 3. 环境配置

```bash
# .env 文件
REDIS_URL=redis://localhost:6379/0

# 生产环境示例
REDIS_URL=redis://username:password@redis.example.com:6379/0
```

## 🔧 高级特性

### 1. 连接池配置

```python
# 自定义连接池
bus = RedisBus(
    url="redis://localhost:6379/0",
    dedupe_ttl=3600,    # 去重TTL (秒)
    maxlen=10000        # Stream 最大长度
)
```

### 2. 错误处理

```python
try:
    entry_id = await bus.publish("event", payload, trace_id="123")
    if not entry_id:
        print("事件重复，未发送")
except Exception as e:
    print(f"发布失败: {e}")
```

### 3. 监控和调试

```python
# 检查 Stream 状态
await redis_client.xinfo_stream("stream:risk_param_reload")

# 查看消费者组
await redis_client.xinfo_groups("stream:risk_param_reload")
```

## ✅ 最佳实践

1. **总是使用 trace_id** 防止重复事件
2. **正确关闭连接** 使用 `await bus.close()`
3. **异常处理** 包装所有 Redis 操作
4. **合理设置超时** 避免长时间阻塞
5. **监控 Stream 长度** 防止内存泄漏

## 🎯 总结

项目中的 Redis 实现提供了：
- ✅ **高可用事件总线**
- ✅ **自动去重机制**  
- ✅ **异步非阻塞**
- ✅ **生产就绪的错误处理**

这种设计确保了分布式服务间的可靠消息传递！
