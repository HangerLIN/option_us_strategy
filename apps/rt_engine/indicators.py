from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Mapping, Optional

import pandas as pd
from ta.momentum import RSIIndicator, StochasticOscillator, AwesomeOscillatorIndicator as AwesomeOscillator
from ta.trend import CCIIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from ta.volume import MFIIndicator, OnBalanceVolumeIndicator

from libs.core import EASTERN


EQ_INDICATOR_COLUMNS = [
    "rsi6",
    "rsi12",
    "rsi24",
    "atr14",
    "ao",
    "stoch_k",
    "stoch_d",
    "cci14",
    "cci6",
    "obv",
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
        return pd.DataFrame(index=df_bars.index.copy(), columns=EQ_INDICATOR_COLUMNS)

    required_cols = {"open", "high", "low", "close", "volume"}
    missing = required_cols - set(df_bars.columns)
    if missing:
        raise KeyError(f"compute_indicators missing required columns: {sorted(missing)}")

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

    indicators["atr14"] = AverageTrueRange(high, low, close, window=14).average_true_range()
    indicators["ao"] = AwesomeOscillator(high, low, window1=5, window2=34).awesome_oscillator()

    stoch = StochasticOscillator(high, low, close, window=14, smooth_window=3)
    indicators["stoch_k"] = stoch.stoch()
    indicators["stoch_d"] = stoch.stoch_signal()

    indicators["cci14"] = CCIIndicator(high, low, close, window=14).cci()
    indicators["cci6"] = CCIIndicator(high, low, close, window=6).cci()

    obv_indicator = OnBalanceVolumeIndicator(close=close, volume=volume)
    indicators["obv"] = obv_indicator.on_balance_volume()
    indicators["obv_ema20"] = indicators["obv"].ewm(span=20, adjust=False).mean()

    indicators["mfi14"] = MFIIndicator(high=high, low=low, close=close, volume=volume, window=14).money_flow_index()

    indicators["rvol6"] = _compute_rvol_series(df, baseline_map or {})

    return indicators.reindex(columns=EQ_INDICATOR_COLUMNS)


def compute_indicator_row(
    df: pd.DataFrame,
    baseline_map: Mapping[int, Decimal],
) -> Optional[Dict[str, Decimal | None]]:
    if df.empty:
        return None

    df = df.copy()
    df.sort_index(inplace=True)

    close = df["close"].astype("float64")
    high = df["high"].astype("float64")
    low = df["low"].astype("float64")
    volume = df["volume"].astype("float64")

    result: Dict[str, Decimal | None] = {
        key: None
        for key in (
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
            "cci14",
            "cci6",
            "obv",
            "obv_ema20",
            "mfi14",
            "rvol6",
        )
    }

    if len(close) >= 6:
        result["rsi6"] = Decimal(str(RSIIndicator(close, window=6).rsi().iloc[-1]))
    if len(close) >= 12:
        result["rsi12"] = Decimal(str(RSIIndicator(close, window=12).rsi().iloc[-1]))
    if len(close) >= 24:
        result["rsi24"] = Decimal(str(RSIIndicator(close, window=24).rsi().iloc[-1]))

    if len(close) >= 20:
        boll = BollingerBands(close, window=20, window_dev=2)
        result["boll_mid"] = Decimal(str(boll.bollinger_mavg().iloc[-1]))
        result["boll_up"] = Decimal(str(boll.bollinger_hband().iloc[-1]))
        result["boll_dn"] = Decimal(str(boll.bollinger_lband().iloc[-1]))

    if len(close) >= 14:
        atr = AverageTrueRange(high, low, close, window=14)
        result["atr14"] = Decimal(str(atr.average_true_range().iloc[-1]))

    if len(close) >= 34:
        ao = AwesomeOscillator(high, low, window1=5, window2=34)
        result["ao"] = Decimal(str(ao.awesome_oscillator().iloc[-1]))

    if len(close) >= 14:
        stoch = StochasticOscillator(high, low, close, window=14, smooth_window=3)
        result["stoch_k"] = Decimal(str(stoch.stoch().iloc[-1]))
        result["stoch_d"] = Decimal(str(stoch.stoch_signal().iloc[-1]))

    if len(close) >= 14:
        cci14 = CCIIndicator(high, low, close, window=14)
        result["cci14"] = Decimal(str(cci14.cci().iloc[-1]))
    if len(close) >= 6:
        cci6 = CCIIndicator(high, low, close, window=6)
        result["cci6"] = Decimal(str(cci6.cci().iloc[-1]))

    if len(close) >= 2:
        obv_indicator = OnBalanceVolumeIndicator(close=close, volume=volume)
        obv_series = obv_indicator.on_balance_volume()
        result["obv"] = Decimal(str(obv_series.iloc[-1]))
        obv_ema20 = obv_series.ewm(span=20, adjust=False).mean().iloc[-1]
        result["obv_ema20"] = Decimal(str(obv_ema20))

    if len(close) >= 14:
        mfi = MFIIndicator(high=high, low=low, close=close, volume=volume, window=14)
        result["mfi14"] = Decimal(str(mfi.money_flow_index().iloc[-1]))

    rvol = _compute_rvol(df, baseline_map)
    if rvol is not None:
        result["rvol6"] = Decimal(str(rvol))

    return result


def _compute_rvol_series(
    df: pd.DataFrame,
    baseline_map: Mapping[int, Decimal],
) -> pd.Series:
    if not baseline_map:
        return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")

    ratios = []
    for ts, row in df.iterrows():
        if not isinstance(ts, datetime):
            ratios.append(float("nan"))
            continue
        et = ts.astimezone(EASTERN) if ts.tzinfo else ts.replace(tzinfo=EASTERN)
        minute_index = et.hour * 60 + et.minute
        baseline = baseline_map.get(minute_index)
        volume = row.get("volume")
        if baseline is None or baseline == 0 or volume is None:
            ratios.append(float("nan"))
            continue
        try:
            ratios.append(float(volume) / float(baseline))
        except (TypeError, ValueError):
            ratios.append(float("nan"))
    series = pd.Series(ratios, index=df.index, dtype="float64")
    return series.ewm(span=6, adjust=False).mean()


def _compute_rvol(df: pd.DataFrame, baseline_map: Mapping[int, Decimal]) -> Optional[float]:
    ratios: List[float] = []
    for ts, row in df.iterrows():
        if not isinstance(ts, datetime):
            continue
        et = ts.astimezone(EASTERN)
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
