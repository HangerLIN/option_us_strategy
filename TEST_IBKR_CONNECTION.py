#!/usr/bin/env python3
"""快速测试 IBKR 连接"""

import sys
import time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.connected = False
        
    def nextValidId(self, orderId):
        print(f"✅ 连接成功! Next Valid Order ID: {orderId}")
        self.connected = True
        self.disconnect()

host = "127.0.0.1"
port = 7497
client_id = 999  # 使用不同的 Client ID 测试

print(f"测试连接到 IBKR: {host}:{port} (Client ID: {client_id})")
app = TestApp()

try:
    app.connect(host, port, client_id)
    time.sleep(2)
    
    if app.isConnected():
        print("✅ IBKR 连接正常")
        app.run()
        sys.exit(0)
    else:
        print("❌ IBKR 连接失败")
        print("\n请检查:")
        print("1. IBKR TWS/Gateway 是否正在运行")
        print("2. 端口是否为 7497 (Paper Trading)")
        print("3. TWS 配置:")
        print("   File -> Global Configuration -> API -> Settings")
        print("   ✓ Enable ActiveX and Socket Clients")
        print("   ✓ Socket port: 7497")
        print("   ✓ Read-Only API: 取消勾选")
        sys.exit(1)
except Exception as e:
    print(f"❌ 连接异常: {e}")
    sys.exit(1)






