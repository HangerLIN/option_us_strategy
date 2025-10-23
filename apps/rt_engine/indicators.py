from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Mapping, Optional

import numpy as np

import pandas as pd
from ta.momentum import (
    RSIIndicator,
    StochRSIIndicator,
    StochasticOscillator,
    AwesomeOscillatorIndicator as AwesomeOscillator,
)
from ta.trend import CCIIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from ta.volume import MFIIndicator, OnBalanceVolumeIndicator

from libs.core import EASTERN


EQ_INDICATOR_COLUMNS = [
    "rsi6",
    "rsi12",
    "rsi24",
    "boll_mid",
    "boll_up",
    "boll_dn",
    "atr14",
    "ao",
    "stoch_k",
    "stoch_d",
    "stoch_rsi_k",
    "stoch_rsi_d",
    "cci14",
    "cci6",
    "sma5",
    "lr_m5_slope",
    "lr_boll_dn_slope",
    "lr_obv_slope",
    "obv",
    "obv_ma6",
    "obv_ema20",
    "mfi14",
    "rvol6",
]


def compute_indicators(
    df_bars: pd.DataFrame,
    baseline_map: Mapping[int, Decimal] | None = None,
) -> pd.DataFrame:
    """
    Vectorized indicator calculation for a minute-bar dataframe.

    Parameters
    ----------
    df_bars:
        DataFrame with index as timezone-aware timestamps (preferably ET) and
        columns open/high/low/close/volume.
    baseline_map:
        Optional mapping minute_index -> mean volume used for RVOL calculation.
        When absent, RVOL will be NaN.
    """
    if df_bars.empty:
        return pd.DataFrame(
            index=df_bars.index.copy(), columns=EQ_INDICATOR_COLUMNS, dtype="float64"
        )

    required_cols = {"open", "high", "low", "close", "volume"}
    missing = required_cols - set(df_bars.columns)
    if missing:
        raise KeyError(f"compute_indicators missing required columns: {sorted(missing)}")

    _validate_index(df_bars.index)

    df = df_bars.sort_index().copy()
    floats = df.loc[:, ["open", "high", "low", "close", "volume"]].astype("float64")
    close = floats["close"]
    high = floats["high"]
    low = floats["low"]
    volume = floats["volume"]

    indicators = pd.DataFrame(index=df.index, dtype="float64")

    indicators["rsi6"] = RSIIndicator(close, window=6).rsi()
    indicators["rsi12"] = RSIIndicator(close, window=12).rsi()
    indicators["rsi24"] = RSIIndicator(close, window=24).rsi()

    boll = BollingerBands(close, window=20, window_dev=2)
    indicators["boll_mid"] = boll.bollinger_mavg()
    indicators["boll_up"] = boll.bollinger_hband()
    indicators["boll_dn"] = boll.bollinger_lband()

    indicators["atr14"] = AverageTrueRange(high, low, close, window=14).average_true_range()
    indicators["ao"] = AwesomeOscillator(high, low, window1=5, window2=34).awesome_oscillator()

    stoch = StochasticOscillator(high, low, close, window=14, smooth_window=3)
    indicators["stoch_k"] = stoch.stoch()
    indicators["stoch_d"] = stoch.stoch_signal()

    stoch_rsi = StochRSIIndicator(close, window=14, smooth1=3, smooth2=3)
    indicators["stoch_rsi_k"] = stoch_rsi.stochrsi_k()
    indicators["stoch_rsi_d"] = stoch_rsi.stochrsi_d()

    indicators["cci14"] = CCIIndicator(high, low, close, window=14).cci()
    indicators["cci6"] = CCIIndicator(high, low, close, window=6).cci()

    obv_indicator = OnBalanceVolumeIndicator(close=close, volume=volume)
    indicators["obv"] = obv_indicator.on_balance_volume()
    indicators["obv_ma6"] = indicators["obv"].rolling(window=6, min_periods=1).mean()
    indicators["obv_ema20"] = indicators["obv"].ewm(span=20, adjust=False).mean()

    indicators["sma5"] = close.rolling(window=5, min_periods=1).mean()
    indicators["lr_m5_slope"] = _linear_regression_slope(indicators["sma5"], window=5)
    indicators["lr_boll_dn_slope"] = _linear_regression_slope(indicators["boll_dn"], window=5)
    indicators["lr_obv_slope"] = _linear_regression_slope(indicators["obv"], window=5)

    indicators["mfi14"] = MFIIndicator(high=high, low=low, close=close, volume=volume, window=14).money_flow_index()

    indicators["rvol6"] = _compute_rvol_series(df, baseline_map or {})

    return indicators.reindex(columns=EQ_INDICATOR_COLUMNS)


def compute_indicator_row(
    df: pd.DataFrame,
    baseline_map: Mapping[int, Decimal],
) -> Optional[Dict[str, Decimal | None]]:
    if df.empty:
        return None
    
    # 最小数据要求：确保有足够的历史数据来计算指标
    # Bollinger Bands(20) + StochRSI(14) + 缓冲 = 至少 30 个数据点
    MIN_BARS_REQUIRED = 30
    if len(df) < MIN_BARS_REQUIRED:
        return None

    df = df.copy()
    df.sort_index(inplace=True)
    _validate_index(df.index)

    indicators_df = compute_indicators(df, baseline_map=baseline_map)
    if indicators_df.empty:
        return None

    latest = indicators_df.iloc[-1]
    result: Dict[str, Decimal | None] = {}
    for column in EQ_INDICATOR_COLUMNS:
        value = latest.get(column)
        if value is None or pd.isna(value):
            result[column] = None
        else:
            result[column] = Decimal(str(float(value)))
    return result


def _compute_rvol_series(
    df: pd.DataFrame,
    baseline_map: Mapping[int, Decimal],
) -> pd.Series:
    """
    计算RVOL6（相对成交量的6期EMA）
    
    计算逻辑：
    1. 计算过去6分钟的成交量平均值作为baseline
    2. RVOL = 当前成交量 / baseline
    3. 对RVOL做6期EMA平滑
    
    如果提供了baseline_map，则使用历史基线；否则使用滚动窗口
    """
    if df.empty or 'volume' not in df.columns:
        return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    
    volume = df['volume'].astype('float64')
    
    if baseline_map:
        # 使用历史基线计算（如果有）
        ratios = []
        for ts, row in df.iterrows():
            if not isinstance(ts, datetime):
                ratios.append(float("nan"))
                continue
            et = ts.astimezone(EASTERN) if ts.tzinfo else ts.replace(tzinfo=EASTERN)
            minute_index = et.hour * 60 + et.minute
            baseline = baseline_map.get(minute_index)
            vol = row.get("volume")
            if baseline is None or baseline == 0 or vol is None:
                ratios.append(float("nan"))
                continue
            try:
                ratios.append(float(vol) / float(baseline))
            except (TypeError, ValueError):
                ratios.append(float("nan"))
        series = pd.Series(ratios, index=df.index, dtype="float64")
    else:
        # 使用滚动窗口计算（无需历史baseline）
        # baseline = 过去6分钟的平均成交量
        baseline = volume.rolling(window=6, min_periods=1).mean()
        # RVOL = 当前成交量 / baseline
        series = volume / baseline
    
    # 对RVOL做6期EMA平滑
    return series.ewm(span=6, adjust=False).mean()


def _compute_rvol(df: pd.DataFrame, baseline_map: Mapping[int, Decimal]) -> Optional[float]:
    ratios: List[float] = []
    for ts, row in df.iterrows():
        if not isinstance(ts, datetime):
            continue
        et = ts.astimezone(EASTERN) if ts.tzinfo else ts.replace(tzinfo=EASTERN)
        minute_index = et.hour * 60 + et.minute
        baseline = baseline_map.get(minute_index)
        if baseline is None or baseline == 0:
            ratios.append(float("nan"))
            continue
        ratios.append(float(row["volume"]) / float(baseline))

    if not ratios or all(pd.isna(r) for r in ratios):
        return None

    series = pd.Series(ratios)
    ema6 = series.ewm(span=6, adjust=False).mean()
    value = ema6.iloc[-1]
    if pd.isna(value):
        return None
    return float(value)


def _validate_index(index: pd.Index) -> None:
    if not isinstance(index, pd.DatetimeIndex):
        raise TypeError("Indicator calculations require DatetimeIndex")
    if index.tz is None:
        raise ValueError("Indicator calculations require timezone-aware timestamps")
    if not (index == index.floor("min")).all():
        raise ValueError("Indicator timestamps must align to minute-close boundaries")


def _linear_regression_slope(series: pd.Series, window: int) -> pd.Series:
    if window <= 1:
        return pd.Series([float("nan")] * len(series), index=series.index, dtype="float64")

    x = np.arange(window, dtype="float64")
    x_sum = x.sum()
    x_sq_sum = (x * x).sum()
    denom = window * x_sq_sum - x_sum * x_sum
    if denom == 0:
        return pd.Series([float("nan")] * len(series), index=series.index, dtype="float64")

    def _calc(values: np.ndarray) -> float:
        if np.any(np.isnan(values)):
            return float("nan")
        y_sum = values.sum()
        xy_sum = (x * values).sum()
        return (window * xy_sum - x_sum * y_sum) / denom

    slopes = series.rolling(window=window, min_periods=window).apply(_calc, raw=True)
    return slopes.reindex(series.index)
