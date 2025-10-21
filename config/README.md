# 股票池配置说明

## 文件说明

### stock_universe.txt
美股期权交易股票池，包含67个大盘科技股，按市值和流动性筛选。

**分类：**
- 超大盘股（7个）：市值 > 1T，如 NVDA, MSFT, AAPL, GOOGL, AMZN, META
- 大盘科技股（5个）：市值 500B - 1T，如 AVGO, TSM, TSLA, ORCL, NFLX
- 中大盘成长股（30个）：市值 100B - 500B
- 中盘成长股（25个）：市值 < 100B

**特点：**
- ✓ 期权流动性好
- ✓ 市值大，波动适中
- ✓ 覆盖主要科技板块

## 使用方式

### 1. 默认模式（推荐）

直接使用 `stock_universe.txt` 中的全部67个标的：

```bash
python -m apps.ingest.ingest_option_chain \
  --date-from 2025-10-01 \
  --date-to 2025-10-07
```

或使用快捷脚本：

```bash
./scripts/run_option_chain_ingest.sh 2025-10-01 2025-10-07
```

### 2. 自定义股票池文件

创建自己的股票池文件（如 `my_universe.txt`）：

```text
# 我的自定义股票池
AAPL
NVDA
MSFT
GOOGL
AMZN
```

然后使用：

```bash
python -m apps.ingest.ingest_option_chain \
  --date-from 2025-10-01 \
  --date-to 2025-10-07 \
  --symbols-from file \
  --universe-file my_universe.txt
```

### 3. 临时测试少量标的

```bash
python -m apps.ingest.ingest_option_chain \
  --date-from 2025-10-01 \
  --date-to 2025-10-01 \
  --symbols-from list \
  --symbol-list AAPL,NVDA,MSFT
```

### 4. 从数据库读取

```bash
python -m apps.ingest.ingest_option_chain \
  --date-from 2025-10-01 \
  --date-to 2025-10-07 \
  --symbols-from ref_universe
```

## 数据筛选规则

### DTE（到期天数）
- **范围**：2-7天
- **原因**：
  - 2天以上：避免过度激进
  - 7天以内：捕捉时间价值衰减最快的黄金期

### OTM档位（虚值档位）
- **范围**：第2-7档虚值期权
- **定义**：
  - CALL: strike > 标的价格
  - PUT: strike < 标的价格
- **原因**：
  - 跳过第1档：风险收益比不理想
  - 跳过第8档+：流动性差，太深度虚值

### 示例

假设 AAPL 现价 = $250：

**会选中的CALL期权：**
- ✓ 第2档：$255 (最接近ATM的第2个虚值)
- ✓ 第3档：$260
- ✓ 第4-7档：依次递增

**会选中的PUT期权：**
- ✓ 第2档：$245 (最接近ATM的第2个虚值)
- ✓ 第3档：$240
- ✓ 第4-7档：依次递减

**不会选中：**
- ✗ DTE=1天或8天+
- ✗ 第1档OTM（太接近ATM）
- ✗ 第8档+OTM（太深度虚值）

## 其他参数

```bash
--max-contracts-per-underlying 80    # 每个标的最多保留80个合约
--max-dte 7                           # 最大DTE=7天
--max-workers 4                       # 并发线程数
--what midpoint                       # 使用中间价（BID/ASK平均）
--useRTH 1                            # 只抓取常规交易时段数据
```

## 维护建议

1. **定期更新股票池**：
   - 移除退市或流动性下降的标的
   - 添加新上市的高流动性科技股

2. **监控数据质量**：
   ```sql
   -- 检查补数情况
   SELECT 
     underlying_symbol,
     COUNT(*) as contract_count,
     MIN(expiry) as earliest_expiry,
     MAX(expiry) as latest_expiry
   FROM option_chain_meta
   WHERE trade_date BETWEEN '2025-10-01' AND '2025-10-07'
   GROUP BY underlying_symbol
   ORDER BY contract_count DESC;
   ```

3. **调整档位范围**：
   - 根据回测结果调整 `min_otm` 和 `max_otm` 参数
   - 修改 `apps/ingest/ingest_option_chain.py` 中的 `_filter_otm_levels` 函数

## 故障排查

### 问题：secdef.empty（期权链为空）
**解决**：
- 检查 IBKR 连接是否正常
- 确认标的代码正确
- 查看日志中的 `after_dte` 和 `after_strike` 计数

### 问题：某些标的没有数据
**解决**：
- 检查该标的是否在补数日期有交易
- 检查是否满足DTE和strike过滤条件
- 查询 `risk_events` 表查看具体错误

### 问题：数据量太大
**解决**：
- 减少 `--max-contracts-per-underlying` 参数
- 缩小日期范围
- 使用更小的股票池文件

## 相关文件

- `apps/ingest/ingest_option_chain.py` - 补数主脚本
- `apps/backtest/universe.py` - 股票池解析器
- `libs/core/config.py` - 全局配置
- `scripts/run_option_chain_ingest.sh` - 快捷启动脚本

