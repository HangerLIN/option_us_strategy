# K线和指标数据获取流程详解

**生成时间:** 2025-10-21  
**适用版本:** option_us_strategy v1.0  
**回测模式:** equity track

---

## 一、整体流程概览

```
┌─────────────────────────────────────────────────────────────────┐
│                        回测启动                                  │
│  python apps/backtest/main.py --track equity                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  步骤1: 从TimescaleDB读取K线+指标                               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  SQL: SELECT b.*, i.* FROM bars1m_equity b              │   │
│  │       JOIN indicators_eq_1m i                           │   │
│  │       ON b.symbol=i.symbol AND b.ts_end=i.ts_end        │   │
│  │       WHERE symbol=:symbol AND ts_end BETWEEN :start:end│   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  步骤2: 数据完整性检查 (_scan_equity_gaps)                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ 检查价格缺口 (price_gaps)                            │   │
│  │  ✓ 检查指标缺口 (indicator_gaps)                        │   │
│  │  ✓ 检查prev_close是否缺失                               │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                    有缺口？
                    /      \
                  是         否
                  │          │
                  ▼          ▼
    ┌──────────────────┐   ┌──────────────────┐
    │ 步骤3: IBKR补数   │   │  直接使用数据   │
    │ (如果启用)       │   │  进入回测循环   │
    └──────┬───────────┘   └─────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│  步骤3.1: 连接IBKR获取历史数据                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ib = build_ibkr_client(settings)                       │   │
│  │  bars = ib.req_historical_1m(                           │   │
│  │      symbol=symbol,                                     │   │
│  │      start=gap_start - 60min,                           │   │
│  │      end=gap_end + 1min,                                │   │
│  │      use_rth=True,                                      │   │
│  │      what_to_show="TRADES"                              │   │
│  │  )                                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  步骤3.2: 写入bars1m_equity表                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  INSERT INTO bars1m_equity (symbol, ts_end, open, high, │   │
│  │      low, close, volume)                                │   │
│  │  VALUES (...)                                           │   │
│  │  ON CONFLICT (symbol, ts_end) DO UPDATE                 │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  步骤3.3: 计算指标                                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  baseline_map = dao.fetch_rvol_baseline(symbol)        │   │
│  │  df_inds = compute_indicators(df_bars, baseline_map)   │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  步骤3.4: 写入indicators_eq_1m表                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  INSERT INTO indicators_eq_1m (symbol, ts_end,          │   │
│  │      rsi6, rsi12, rsi24, atr14, ao, stoch_k, stoch_d,   │   │
│  │      cci14, cci6, obv, obv_ema20, mfi14, rvol6)         │   │
│  │  VALUES (...)                                           │   │
│  │  ON CONFLICT (symbol, ts_end) DO UPDATE                 │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  步骤4: 二次校验                                                 │
│  重新读取数据并检查缺口是否填补                                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  步骤5: 回测执行                                                 │
│  使用完整的K线和指标数据进行回测                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 二、核心代码路径

### 2.1 主入口
**文件:** `apps/backtest/main.py`
```python
def main():
    # ... 准备工作
    for symbol in final_symbols:
        run_id = run_backtest(
            symbol=symbol,
            start=args.start,
            end=args.end,
            signal_mode=args.signal_mode,
            track=args.track,
            # ...
        )
```

### 2.2 回测执行器
**文件:** `apps/backtest/bt_runner.py`
```python
def run_backtest(...):
    # ...
    if track_mode == "equity":
        _run_equity_track(
            dao=dao,
            symbol=symbol,
            start=start,
            end=end,
            # ...
        )
```

### 2.3 Equity Track执行
**文件:** `apps/backtest/bt_runner.py`
```python
def _run_equity_track(...):
    # 步骤1: 获取数据
    rows = dao.fetch_equity_bars(symbol=symbol, start_ts=start, end_ts=end)
    
    # 步骤2: 转换为DataFrame
    frame = pd.DataFrame([asdict(row) for row in rows])
    frame["ts_end"] = pd.to_datetime(frame["ts_end"], utc=True)
    
    # 步骤3: 数值化处理
    numeric_cols = ["open", "high", "low", "close", "volume",
                    "rsi6", "rsi12", "rsi24", "atr14", "ao",
                    "stoch_k", "stoch_d", "cci14", "cci6",
                    "obv", "obv_ema20", "mfi14", "rvol6"]
    for column in numeric_cols:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    
    # 步骤4: 设置索引并排序
    frame = frame.set_index("ts_end").sort_index()
    
    # 步骤5: 进入回测循环 ...
```

### 2.4 数据访问层 (DAO)
**文件:** `apps/backtest/dao.py`
```python
class BacktestDAO:
    def fetch_equity_bars(self, *, symbol: str, start_ts, end_ts) -> list[EquityBarRow]:
        """从TimescaleDB读取K线+指标"""
        sql = text(self._sql("select_equity_bars"))
        result = self._session.execute(
            sql,
            {"symbol": symbol, "start_ts": start_ts, "end_ts": end_ts},
        )
        rows = result.mappings().all()
        return [self._map_equity_bar(row) for row in rows]
    
    def fetch_rvol_baseline(self, *, symbol: str) -> dict[int, Decimal]:
        """读取RVOL基线数据"""
        sql = text(self._sql("select_rvol_baseline"))
        result = self._session.execute(sql, {"symbol": symbol})
        return {int(row.minute_index): Decimal(str(row.mean_vol_20d)) 
                for row in result}
```

---

## 三、SQL查询详解

### 3.1 查询K线和指标
**文件:** `libs/db/queries_backtest.sql`
```sql
-- name: select_equity_bars
SELECT
    b.ts_end,              -- 时间戳 (UTC)
    b.symbol,              -- 股票代码
    b.open,                -- 开盘价
    b.high,                -- 最高价
    b.low,                 -- 最低价
    b.close,               -- 收盘价
    b.volume,              -- 成交量
    -- 以下为指标
    i.rsi6,                -- RSI(6)
    i.rsi12,               -- RSI(12)
    i.rsi24,               -- RSI(24)
    i.atr14,               -- ATR(14)
    i.ao,                  -- Awesome Oscillator(5,34)
    i.stoch_k,             -- Stochastic %K
    i.stoch_d,             -- Stochastic %D
    i.cci14,               -- CCI(14)
    i.cci6,                -- CCI(6)
    i.obv,                 -- On Balance Volume
    i.obv_ema20,           -- OBV的EMA(20)
    i.mfi14,               -- Money Flow Index(14)
    i.rvol6                -- Relative Volume EMA(6)
FROM bars1m_equity AS b
JOIN indicators_eq_1m AS i
  ON b.symbol = i.symbol AND b.ts_end = i.ts_end
WHERE b.symbol = :symbol
  AND b.ts_end >= :start_ts
  AND b.ts_end < :end_ts
ORDER BY b.ts_end;
```

**关键点:**
- 使用`JOIN`确保K线和指标在同一行
- 如果某个K线没有对应指标，该行会被排除（INNER JOIN）
- 时间范围：`[start_ts, end_ts)` 左闭右开

### 3.2 查询RVOL基线
```sql
-- name: select_rvol_baseline
SELECT
    minute_index,          -- 分钟索引 (1-390)
    mean_vol_20d           -- 20日均量
FROM rvol_baseline_eq
WHERE symbol = :symbol;
```

**minute_index说明:**
- 1 = 09:30 ET
- 2 = 09:31 ET
- ...
- 390 = 16:00 ET

---

## 四、指标计算详解

### 4.1 指标计算入口
**文件:** `apps/rt_engine/indicators.py`
```python
def compute_indicators(
    df_bars: pd.DataFrame,
    baseline_map: Mapping[int, Decimal] | None = None,
) -> pd.DataFrame:
    """
    向量化指标计算
    
    参数:
        df_bars: K线DataFrame，包含 open/high/low/close/volume
        baseline_map: RVOL基线字典 {minute_index: mean_vol_20d}
    
    返回:
        指标DataFrame，包含所有13个指标
    """
```

### 4.2 各指标计算方法

#### RSI (Relative Strength Index)
```python
indicators["rsi6"] = RSIIndicator(close, window=6).rsi()
indicators["rsi12"] = RSIIndicator(close, window=12).rsi()
indicators["rsi24"] = RSIIndicator(close, window=24).rsi()
```
- **库:** `ta.momentum.RSIIndicator`
- **热身期:** 6/12/24根K线
- **公式:** RSI = 100 - (100 / (1 + RS)), RS = 平均上涨/平均下跌

#### ATR (Average True Range)
```python
indicators["atr14"] = AverageTrueRange(high, low, close, window=14).average_true_range()
```
- **库:** `ta.volatility.AverageTrueRange`
- **热身期:** 14根K线
- **公式:** ATR = EMA(TR, 14), TR = max(H-L, |H-PC|, |L-PC|)

#### AO (Awesome Oscillator)
```python
indicators["ao"] = AwesomeOscillator(high, low, window1=5, window2=34).awesome_oscillator()
```
- **库:** `ta.momentum.AwesomeOscillatorIndicator`
- **热身期:** 34根K线（最长！）
- **公式:** AO = SMA(median, 5) - SMA(median, 34)

#### Stochastic Oscillator
```python
stoch = StochasticOscillator(high, low, close, window=14, smooth_window=3)
indicators["stoch_k"] = stoch.stoch()
indicators["stoch_d"] = stoch.stoch_signal()
```
- **库:** `ta.momentum.StochasticOscillator`
- **热身期:** 14根K线
- **公式:** %K = (C-L14)/(H14-L14)*100, %D = SMA(%K, 3)

#### CCI (Commodity Channel Index)
```python
indicators["cci14"] = CCIIndicator(high, low, close, window=14).cci()
indicators["cci6"] = CCIIndicator(high, low, close, window=6).cci()
```
- **库:** `ta.trend.CCIIndicator`
- **热身期:** 14/6根K线
- **公式:** CCI = (TP - SMA(TP)) / (0.015 * MD)

#### OBV (On Balance Volume)
```python
obv_indicator = OnBalanceVolumeIndicator(close=close, volume=volume)
indicators["obv"] = obv_indicator.on_balance_volume()
indicators["obv_ema20"] = indicators["obv"].ewm(span=20, adjust=False).mean()
```
- **库:** `ta.volume.OnBalanceVolumeIndicator`
- **热身期:** 0根（累积型指标）
- **公式:** OBV_t = OBV_{t-1} + volume if close>prev_close else -volume

#### MFI (Money Flow Index)
```python
indicators["mfi14"] = MFIIndicator(high=high, low=low, close=close, volume=volume, window=14).money_flow_index()
```
- **库:** `ta.volume.MFIIndicator`
- **热身期:** 14根K线
- **公式:** MFI = 100 - 100/(1 + MFR), MFR = 正资金流/负资金流

#### RVOL (Relative Volume) ⚠️ 关键！
```python
indicators["rvol6"] = _compute_rvol_series(df, baseline_map or {})

def _compute_rvol_series(df: pd.DataFrame, baseline_map: Mapping[int, Decimal]) -> pd.Series:
    """
    计算RVOL6指标
    
    步骤:
    1. 如果baseline_map为空 → 返回全NaN
    2. 对每一根K线:
       a. 计算minute_index
       b. 查找baseline_map[minute_index]
       c. 计算ratio = volume / baseline
    3. 对ratio序列计算EMA(6)
    """
    if not baseline_map:
        return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    
    ratios = []
    for ts, row in df.iterrows():
        et = ts.astimezone(EASTERN)
        minute_index = et.hour * 60 + et.minute  # 例如 09:30 = 570
        baseline = baseline_map.get(minute_index)
        volume = row.get("volume")
        
        if baseline is None or baseline == 0 or volume is None:
            ratios.append(float("nan"))
        else:
            ratios.append(float(volume) / float(baseline))
    
    series = pd.Series(ratios, index=df.index, dtype="float64")
    return series.ewm(span=6, adjust=False).mean()
```

**RVOL6缺失原因分析:**
```
baseline_map为空
    ↓
所有ratio = NaN
    ↓
EMA(NaN序列) = NaN
    ↓
rvol6全部为NULL
```

---

## 五、IBKR历史数据获取

### 5.1 IBKR连接初始化
**文件:** `libs/infra/ibkr_client.py`
```python
def build_ibkr_client(settings: Settings) -> IBClient:
    """构建IBKR客户端"""
    client = IBClient(settings)
    if not client.is_connected():
        client.connect()
        # 等待nextValidId
        if not client._next_valid_id_ready.wait(timeout=10):
            raise TimeoutError("Timed out waiting for nextValidId from IBKR")
    return client
```

### 5.2 请求历史1分钟数据
```python
def req_historical_1m(
    self,
    symbol: str,
    start: datetime,
    end: datetime,
    use_rth: bool,
    *,
    what_to_show: str = "TRADES",
    exchange: str = "SMART",
    currency: str = "USD",
) -> list[Mapping[str, Any]]:
    """
    请求历史1分钟数据
    
    参数:
        symbol: 股票代码
        start: 开始时间（UTC）
        end: 结束时间（UTC）
        use_rth: 是否仅RTH (True=仅常规交易时段)
        what_to_show: "TRADES" 或 "MIDPOINT"
    
    返回:
        [{
            "datetime": datetime,
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": int,
            "wap": float,
            "count": int
        }, ...]
    """
    contract = self.stock_contract(symbol, exchange, currency)
    return self.req_historical_1m_contract(
        contract, start, end, use_rth, what_to_show=what_to_show
    )
```

### 5.3 IBKR API调用
```python
def req_historical_1m_contract(...):
    # 计算duration
    duration_minutes = max(1, int((end - start).total_seconds() // 60))
    duration_seconds = duration_minutes * 60
    duration_str = f"{duration_seconds} S"
    
    # 格式化结束时间
    end_str = end.strftime("%Y%m%d %H:%M:%S US/Eastern")
    
    # 发起请求
    self.reqHistoricalData(
        req_id,
        resolved_contract,
        end_str,           # endDateTime
        duration_str,      # durationStr (e.g., "7200 S" for 2 hours)
        "1 min",           # barSizeSetting
        what_to_show,      # whatToShow
        int(use_rth),      # useRTH (0 or 1)
        2,                 # formatDate (1=text, 2=timestamp)
        False,             # keepUpToDate
        [],                # chartOptions
    )
    
    # 等待回调
    bars = future.wait(timeout=30)
    return bars
```

---

## 六、数据缺口检测

### 6.1 缺口扫描
**文件:** `apps/backtest/datafeed/timescale_equity.py`
```python
def _scan_equity_gaps(df: pd.DataFrame, symbol: str) -> GapReport:
    """
    扫描数据缺口
    
    检查项:
    1. price_gaps: 价格K线缺失
    2. indicator_gaps: 指标NULL值
    3. prev_close_missing: 前一日收盘价缺失
    """
    report = GapReport()
    
    # 按交易日分组
    df_et = df.copy()
    df_et["trade_date"] = df_et.index.tz_convert(EASTERN).date
    
    for trade_date, day_df in df_et.groupby("trade_date"):
        # 检查价格缺口
        expected_minutes = _expected_rth_minutes(trade_date)
        if len(day_df) < expected_minutes:
            # 找出缺失的分钟
            missing = _find_missing_minutes(day_df, trade_date)
            report.price_gaps.append(MinuteGap(trade_date, missing))
        
        # 检查指标缺口
        for ts, row in day_df.iterrows():
            missing_cols = []
            for col in INDICATOR_COLUMNS:
                if pd.isna(row.get(col)):
                    missing_cols.append(col)
            if missing_cols:
                report.indicator_gaps[trade_date].append((ts, missing_cols))
    
    return report
```

### 6.2 缺口类型

#### 价格缺口 (Price Gap)
```python
@dataclass
class MinuteGap:
    trade_date: date
    missing_minutes: List[pd.Timestamp]
```
**示例:**
```
2025-10-01: [
    2025-10-01 09:30:00 ET,
    2025-10-01 16:01:00 ET,
    2025-10-01 16:02:00 ET,
    ...
]
```

#### 指标缺口 (Indicator Gap)
```python
@dataclass
class IndicatorGap:
    trade_date: date
    rows: List[Tuple[pd.Timestamp, List[str]]]
```
**示例:**
```
2025-10-01: [
    (2025-10-01 09:31:00 ET, ["boll_mid", "atr14", "ao"]),
    (2025-10-01 09:32:00 ET, ["ao"]),
    ...
]
```

---

## 七、补数流程详解

### 7.1 触发条件
```python
gap_report = _scan_equity_gaps(df=df, symbol=symbol)
if gap_report.has_any_gap():
    # 触发补数
    _fill_equity_gaps_via_ibkr(dao, ib, symbol, gap_report)
```

### 7.2 价格缺口补数
```python
for price_gap in gap_report.price_gaps:
    # 扩展窗口（前后各1小时缓冲）
    window_start = price_gap.missing_minutes[0] - pd.Timedelta(minutes=60)
    window_end = price_gap.missing_minutes[-1] + pd.Timedelta(minutes=1)
    
    # 从IBKR获取数据
    bars = ib.req_historical_1m(
        symbol=symbol,
        start=window_start_utc,
        end=window_end_utc,
        use_rth=True,
        what_to_show="TRADES"
    )
    
    # 写入数据库
    dao.upsert_equity_bars_from_df(symbol=symbol, df=df_bars)
    
    # 计算并写入指标
    baseline_map = dao.fetch_rvol_baseline(symbol=symbol)  # ⚠️ 关键！
    df_inds = indcalc.compute_indicators(df_bars, baseline_map=baseline_map)
    dao.upsert_equity_indicators_from_df(symbol=symbol, df=df_inds)
```

**关键点:**
- `baseline_map = dao.fetch_rvol_baseline(symbol)` 
- **如果rvol_baseline_eq表为空，baseline_map={}**
- **导致所有rvol6 = NaN！**

### 7.3 指标缺口补数
```python
for indicator_gap in gap_report.indicator_gaps:
    # 从数据库读取对应时间的价格数据
    rows = dao.fetch_equity_bars(symbol, start_utc, end_utc)
    
    # 重新计算指标
    df_prices = _rows_to_dataframe(rows)
    baseline_map = dao.fetch_rvol_baseline(symbol=symbol)  # ⚠️ 关键！
    df_inds = indcalc.compute_indicators(df_prices, baseline_map=baseline_map)
    
    # 更新指标
    dao.upsert_equity_indicators_from_df(symbol=symbol, df=df_inds)
```

---

## 八、数据表结构

### 8.1 bars1m_equity
```sql
CREATE TABLE bars1m_equity (
    symbol        text NOT NULL,
    ts_end        timestamptz NOT NULL,  -- 分钟结束时间戳（UTC）
    open          numeric NOT NULL,
    high          numeric NOT NULL,
    low           numeric NOT NULL,
    close         numeric NOT NULL,
    volume        bigint NOT NULL,
    session       text DEFAULT 'RTH',    -- RTH/PRE/POST
    PRIMARY KEY (symbol, ts_end)
);
```

### 8.2 indicators_eq_1m
```sql
CREATE TABLE indicators_eq_1m (
    symbol        text NOT NULL,
    ts_end        timestamptz NOT NULL,  -- 与bars1m_equity对齐
    rsi6          numeric,
    rsi12         numeric,
    rsi24         numeric,
    atr14         numeric,
    ao            numeric,
    stoch_k       numeric,
    stoch_d       numeric,
    cci14         numeric,
    cci6          numeric,
    obv           numeric,
    obv_ema20     numeric,
    mfi14         numeric,
    rvol6         numeric,               -- ⚠️ 依赖rvol_baseline_eq
    boll_mid      numeric,
    boll_up       numeric,
    boll_dn       numeric,
    PRIMARY KEY (symbol, ts_end)
);
```

### 8.3 rvol_baseline_eq ⚠️ 关键表
```sql
CREATE TABLE rvol_baseline_eq (
    symbol        text NOT NULL,
    minute_index  smallint NOT NULL CHECK (minute_index BETWEEN 1 AND 390),
    mean_vol_20d  double precision NOT NULL,  -- 20日平均成交量
    updated_at    timestamptz DEFAULT now(),
    PRIMARY KEY (symbol, minute_index)
);
```

**minute_index计算:**
```python
# 09:30 ET = 9*60 + 30 = 570 → 转换为相对索引 = 1
# 09:31 ET = 9*60 + 31 = 571 → 转换为相对索引 = 2
# 16:00 ET = 16*60 + 0 = 960 → 转换为相对索引 = 390

# 实际代码中的计算
minute_index = hour * 60 + minute  # 这是绝对索引
# 需要注意：有些代码可能用的是相对索引（从1开始）
```

---

## 九、当前问题诊断

### 9.1 问题症状
```
指标缺失统计:
- BOLL: 137处
- ATR: 57处
- AO: 301处（最多）
- CCI: 95处
- RSI/OBV: 0处（完整）
- RVOL6: ?处（可能全缺）

总计: 574处缺失
```

### 9.2 根本原因
```sql
SELECT COUNT(*) FROM rvol_baseline_eq;
-- 结果: 0

-- 导致：
baseline_map = {}
    ↓
compute_indicators(..., baseline_map={})
    ↓
_compute_rvol_series返回全NaN
    ↓
所有rvol6 = NULL
```

### 9.3 其他缺失原因
**热身期缺失（正常）:**
- 每天前20分钟BOLL缺失
- 每天前14分钟ATR缺失
- 每天前34分钟AO缺失
- **这是指标计算的固有特性，无法避免**

---

## 十、修复方案

### 方案1: 刷新RVOL基线（推荐）
```bash
# 1. 运行刷新脚本
python refresh_rvol_baseline.py

# 2. 验证
echo "SELECT COUNT(DISTINCT symbol), COUNT(*) FROM rvol_baseline_eq;" | psql $DATABASE_URL

# 3. 重新运行回测
python apps/backtest/main.py --start ... --end ... --track equity
```

### 方案2: 手动导入基线数据
```sql
-- 如果有现成的基线数据
COPY rvol_baseline_eq(symbol, minute_index, mean_vol_20d)
FROM '/path/to/rvol_baseline.csv'
WITH CSV HEADER;
```

### 方案3: 使用固定基线（测试用）
```python
# 临时方案：为所有分钟使用固定基线
def _compute_rvol_series_fixed_baseline(df: pd.DataFrame) -> pd.Series:
    """使用固定基线1000"""
    ratios = [row["volume"] / 1000.0 for _, row in df.iterrows()]
    series = pd.Series(ratios, index=df.index)
    return series.ewm(span=6, adjust=False).mean()
```

---

## 十一、监控和维护

### 11.1 数据健康检查
```sql
-- 检查最近7天的指标缺失
CREATE OR REPLACE VIEW v_indicator_health AS
SELECT 
    DATE(ts_end AT TIME ZONE 'America/New_York') as trade_date,
    symbol,
    COUNT(*) as total_rows,
    COUNT(rsi6) as rsi6_ok,
    COUNT(rvol6) as rvol6_ok,
    COUNT(boll_mid) as boll_ok,
    COUNT(*) - COUNT(rvol6) as rvol6_missing,
    ROUND(100.0 * COUNT(rvol6) / NULLIF(COUNT(*), 0), 2) as rvol6_pct
FROM indicators_eq_1m
WHERE ts_end >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY trade_date, symbol
ORDER BY trade_date DESC, rvol6_pct ASC;

-- 查询缺失最严重的
SELECT * FROM v_indicator_health WHERE rvol6_pct < 95;
```

### 11.2 RVOL基线监控
```sql
-- 检查基线覆盖率
SELECT 
    symbol,
    COUNT(*) as baseline_rows,
    MIN(minute_index) as min_idx,
    MAX(minute_index) as max_idx,
    updated_at
FROM rvol_baseline_eq
GROUP BY symbol, updated_at
HAVING COUNT(*) < 390  -- 期望390行
ORDER BY baseline_rows ASC;

-- 检查基线是否过期（>7天未更新）
SELECT symbol, MAX(updated_at) as last_update
FROM rvol_baseline_eq
GROUP BY symbol
HAVING MAX(updated_at) < CURRENT_DATE - INTERVAL '7 days';
```

### 11.3 回测前检查脚本
```bash
#!/bin/bash
# backtest_preflight_check.sh

echo "=== 回测前置检查 ==="

echo "1. 检查RVOL基线..."
psql $DATABASE_URL -c "SELECT COUNT(DISTINCT symbol) as symbols, COUNT(*) as rows FROM rvol_baseline_eq;"

echo "2. 检查最近7天的K线数据..."
psql $DATABASE_URL -c "SELECT DATE(ts_end AT TIME ZONE 'America/New_York') as date, COUNT(DISTINCT symbol) as symbols, COUNT(*) as bars FROM bars1m_equity WHERE ts_end >= CURRENT_DATE - 7 GROUP BY date ORDER BY date DESC;"

echo "3. 检查指标完整性..."
psql $DATABASE_URL -c "SELECT * FROM v_indicator_health WHERE rvol6_pct < 95 LIMIT 10;"

echo "4. 测试IBKR连接..."
python -c "from libs.infra import build_ibkr_client; from libs.core import get_settings; ib = build_ibkr_client(get_settings()); print('✓ IBKR连接成功' if ib.is_connected() else '✗ IBKR连接失败'); ib.disconnect_and_stop()"

echo "=== 检查完成 ==="
```

---

## 十二、常见问题

### Q1: 为什么不直接在compute_indicators中使用默认基线？
**A:** 这会导致RVOL值不准确，影响信号质量。RVOL必须基于真实的历史均量计算。

### Q2: 热身期缺失能避免吗？
**A:** 不能。这是技术指标的固有特性。但可以通过：
- 回测时跳过每天前34分钟
- 信号评估时检查指标是否为None

### Q3: IBKR补数为什么会失败？
**A:** 常见原因：
- ClientId被占用
- 网络连接问题
- IBKR API限流
- 历史数据订阅权限问题

### Q4: 可以从其他数据源获取K线吗？
**A:** 不推荐。文档要求"唯一行情来源：IBKR"（No-Mock Policy）。

---

**文档版本:** v1.0  
**最后更新:** 2025-10-21  
**维护者:** System  
**相关文档:** 
- `指标缺失问题分析与修复方案.md`
- `refresh_rvol_baseline.py`
- `数据管道与预处理开发文档.md`




