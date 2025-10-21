#!/usr/bin/env python3
"""
Redis 连接使用示例
展示如何在项目中使用 Redis 进行事件发布和消费
"""

import asyncio
import json
from datetime import datetime, timezone
from libs.core import get_settings
from libs.infra.redis_bus import RedisBus


async def example_publisher():
    """发布者示例：发送事件到 Redis"""
    print("🚀 Redis 发布者示例")
    
    settings = get_settings()
    bus = RedisBus(settings.redis_url)
    
    try:
        # 1. 测试连接
        ping_result = await bus.ping()
        print(f"   Redis 连接: {'✅ 成功' if ping_result else '❌ 失败'}")
        
        if not ping_result:
            return
            
        # 2. 发布风险参数重载事件
        event_data = {
            "trace_id": f"demo-{int(datetime.now().timestamp())}",
            "triggered_by": "demo_script",
            "reload_at": datetime.now(timezone.utc).isoformat(),
            "reason": "演示用途"
        }
        
        entry_id = await bus.publish(
            "risk_param_reload",
            event_data,
            trace_id=event_data["trace_id"]
        )
        
        print(f"   📤 事件已发布: {entry_id}")
        print(f"   �� 事件内容: {json.dumps(event_data, indent=2)}")
        
        # 3. 发布自定义事件
        custom_event = {
            "trace_id": "custom-demo-123",
            "action": "test_action",
            "data": {"symbol": "AAPL", "value": 100}
        }
        
        custom_entry_id = await bus.publish(
            "demo_events",
            custom_event,
            trace_id="custom-demo-123"
        )
        
        print(f"   📤 自定义事件已发布: {custom_entry_id}")
        
    except Exception as e:
        print(f"   ❌ 发布失败: {e}")
    finally:
        await bus.close()


async def example_consumer():
    """消费者示例：监听和处理 Redis 事件"""
    print("📡 Redis 消费者示例")
    
    settings = get_settings()
    bus = RedisBus(settings.redis_url)
    
    try:
        # 1. 确保消费者组存在
        await bus.ensure_group("demo_events", "demo_service")
        print("   ✅ 消费者组已创建: demo_service")
        
        # 2. 消费事件 (模拟消费5秒)
        print("   🔄 开始监听事件...")
        timeout_count = 0
        max_timeouts = 3  # 最多等待3次超时
        
        while timeout_count < max_timeouts:
            entries = await bus.consume(
                event="demo_events",
                group="demo_service",
                consumer="demo-consumer-1",
                count=10,
                block_ms=2000  # 2秒超时
            )
            
            if entries:
                timeout_count = 0  # 重置超时计数
                for entry in entries:
                    print(f"   📥 收到事件:")
                    print(f"      ID: {entry['id']}")
                    print(f"      Trace ID: {entry['trace_id']}")
                    print(f"      内容: {json.dumps(entry['payload'], indent=6)}")
                    
                    # 模拟事件处理
                    await process_event(entry)
            else:
                timeout_count += 1
                print(f"   ⏳ 等待事件... ({timeout_count}/{max_timeouts})")
        
        print("   ✅ 消费者示例完成")
        
    except Exception as e:
        print(f"   ❌ 消费失败: {e}")
    finally:
        await bus.close()


async def process_event(entry):
    """处理接收到的事件"""
    payload = entry.get("payload", {})
    trace_id = entry.get("trace_id", "")
    
    # 模拟业务处理
    await asyncio.sleep(0.1)
    print(f"      ✅ 事件处理完成: {trace_id}")


async def redis_info_example():
    """Redis 信息查询示例"""
    print("📊 Redis 信息查询示例")
    
    settings = get_settings()
    bus = RedisBus(settings.redis_url)
    
    try:
        # 直接访问 Redis 客户端
        redis_client = bus._redis
        
        # 1. 基本信息
        info = await redis_client.info("server")
        print(f"   Redis 版本: {info.get('redis_version', 'Unknown')}")
        print(f"   运行时间: {info.get('uptime_in_seconds', 0)} 秒")
        
        # 2. 内存使用
        memory_info = await redis_client.info("memory")
        used_memory = memory_info.get('used_memory_human', 'Unknown')
        print(f"   内存使用: {used_memory}")
        
        # 3. 检查现有的 Streams
        streams = await redis_client.keys("stream:*")
        print(f"   现有 Streams: {len(streams)} 个")
        for stream in streams:
            length = await redis_client.xlen(stream)
            print(f"     - {stream}: {length} 条消息")
            
    except Exception as e:
        print(f"   ❌ 查询失败: {e}")
    finally:
        await bus.close()


async def main():
    """主函数：运行所有示例"""
    print("🔄 Redis 连接使用示例")
    print("=" * 50)
    
    # 1. 信息查询
    await redis_info_example()
    print()
    
    # 2. 发布事件
    await example_publisher()
    print()
    
    # 3. 消费事件
    await example_consumer()
    print()
    
    print("✅ 所有示例运行完成！")


if __name__ == "__main__":
    # 设置 PYTHONPATH
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    # 运行示例
    asyncio.run(main())
