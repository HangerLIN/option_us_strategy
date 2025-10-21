# 服务验证报告

## ✅ 已完成验证

### 1. signal_svc (端口 8000) - ✅ 正常运行
- **健康检查**: ✅ 通过
- **信号推送**: ✅ 成功
- **数据库修复**: ✅ 已修复 `bt_signals` 表字段约束
  - 将 `accepted` 和 `reason` 字段改为允许 NULL

#### 测试命令
```bash
# 健康检查
curl -s http://localhost:8000/healthz | python3 -m json.tool

# 信号推送测试
curl -X POST http://localhost:8000/signals/push \
  -H "Content-Type: application/json" \
  -d '{"signals":[{"signal":{"strategy_code":"core-vol","symbol":"GOOGL","signal_code":"SIG_OPEN_CHASE_BUY","side":"BUY","confidence":1.0,"reason":{"demo":true},"risk_hint":{"size":"1R"},"option_hint":{"dte":[2,7]},"ttl_seconds":180,"cooldown_seconds":600,"generated_at":"2025-10-10T11:00:00-05:00"},"force":true}]}'

# WebSocket 连接 (浏览器或工具)
# ws://localhost:8000/ws/signals
```

### 2. risk_svc (端口 8082) - ✅ 正常运行
- **健康检查**: ✅ 通过
- **数据库修复**: ✅ 已创建 `risk_limits` 表并授予权限

#### 测试命令
```bash
# 健康检查
curl -s http://localhost:8082/healthz | python3 -m json.tool

# 查看风控状态
curl -s http://localhost:8082/state | python3 -m json.tool
```

## ⚠️ 需要进一步配置

### 3. exec_svc (端口 8001) - ⚠️ IBKR 连接问题
- **状态**: 服务启动但 IBKR 连接失败
- **问题**: IBKR Gateway/TWS API 配置需要调整
- **当前配置**: 
  - IB_HOST=127.0.0.1
  - IB_PORT=7497 (已修复，原为 4002)
  - IB_CLIENT_ID=16

#### 需要在 IBKR TWS/Gateway 中配置:
1. 启用 Socket Clients: `Edit -> Global Configuration -> API -> Settings`
2. 勾选 "Enable ActiveX and Socket Clients"
3. 确认 Socket Port 为 7497 (Paper Trading)
4. 添加 Trusted IP: 127.0.0.1

#### 测试命令 (IBKR配置好后)
```bash
# 健康检查
curl -s http://localhost:8001/healthz | python3 -m json.tool

# 提交委托
curl -X POST http://localhost:8001/orders/submit \
  -H "Content-Type: application/json" \
  -d '{"strategy_code":"core-vol","symbol":"AAPL","side":"BUY","quantity":1,"limit_price":1.25,"tif":"DAY","signal_code":"SIG_OPEN_CHASE_BUY","execution_mode":"MARKETABLE","trace_id":"test-trace"}'

# WebSocket 订阅
# ws://localhost:8001/ws/orders
```

## 🔧 已修复的问题

1. **bt_signals 表结构不匹配**
   - 模型定义使用 `signal_ts` 但数据库使用 `ts_end`
   - 已统一为 `ts_end`
   - `accepted` 和 `reason` 字段 NOT NULL 约束已移除

2. **risk_limits 表缺失**
   - 已创建表结构
   - 已授予 option_user 访问权限

3. **IBKR 端口配置错误**
   - 从 4002 更新为 7497

## 📝 后续步骤

### 验证 WebSocket 实时推送
使用浏览器或 wscat 工具:

```bash
# 安装 wscat (如需)
npm install -g wscat

# 连接 signal_svc WebSocket
wscat -c ws://localhost:8000/ws/signals

# 在另一个终端推送信号,观察 WebSocket 收到的消息
curl -X POST http://localhost:8000/signals/push \
  -H "Content-Type: application/json" \
  -d '{"signals":[{"signal":{"strategy_code":"core-vol","symbol":"TEST","signal_code":"SIG_OPEN_CHASE_BUY","side":"BUY","confidence":1.0,"reason":{"test":true},"risk_hint":{"size":"1R"},"option_hint":{"dte":[2,7]},"ttl_seconds":180,"cooldown_seconds":600,"generated_at":"2025-10-10T12:00:00-05:00"},"force":true}]}'
```

### 验证风控热更链路

```bash
# 1. 准备配置文件 sample.json
cat > sample.json << 'EOF'
[
  {"symbol":"GLOBAL","limit_code":"RISK_NOTIONAL_CAP","limit_value":6000000},
  {"symbol":"GLOBAL","limit_code":"DAILY_LOSS_R","limit_value":4}
]
EOF

# 2. 加载风控限额
python scripts/load_risk_limits.py sample.json

# 3. 触发热更
python scripts/send_risk_param_reload.py --triggered-by ops

# 4. 查看风控状态确认更新
curl -s http://localhost:8082/state | python3 -m json.tool
```

## 🎯 总结

- ✅ **signal_svc**: 完全正常
- ✅ **risk_svc**: 完全正常  
- ⚠️ **exec_svc**: 需要配置 IBKR Gateway/TWS

所有核心服务已经可以运行，只需要完成 IBKR 的 API 配置即可进行完整的端到端测试。



