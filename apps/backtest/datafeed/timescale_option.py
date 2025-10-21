from __future__ import annotations

from datetime import datetime
from dataclasses import asdict
from decimal import Decimal
from typing import Any, Mapping

import pandas as pd
from backtrader.feeds import PandasData

from apps.backtest.dao import BacktestDAO, OptionBarRow


class TimescaleOptionData(PandasData):
    lines = ("bid", "ask")
    params = (
        ("datetime", None),
        ("high", -1),
        ("low", -1),
        ("open", -1),
        ("close", -1),
        ("volume", -1),
        ("openinterest", -1),
        ("bid", -1),
        ("ask", -1),
    )

    @classmethod
    def from_timescale(
        cls,
        *,
        dao: BacktestDAO,
        contract: Mapping[str, Any],
        start: datetime,
        end: datetime,
    ) -> "TimescaleOptionData":
        conid = contract.get("conid")
        if isinstance(conid, (int, str)) and str(conid).isdigit():
            rows: list[OptionBarRow] = dao.fetch_option_bars_by_conid(
                conid=int(conid), start_ts=start, end_ts=end
            )
        else:
            rows = dao.fetch_option_bars_by_attributes(
                underlying_symbol=str(contract["symbol"]),
                expiry=contract["expiry"],
                strike=contract["strike"],
                right=str(contract["right"]),
                start_ts=start,
                end_ts=end,
            )

        if not rows:
            raise RuntimeError("No option L1 data for contract")

        df = pd.DataFrame([asdict(row) for row in rows])
        required_cols = {"ts_end", "bid", "ask", "mid"}
        missing = required_cols - set(df.columns)
        if missing:
            raise RuntimeError(f"Option feed missing columns: {missing}")

        if df[["bid", "ask"]].isna().any().any():
            raise RuntimeError("Option feed contains null bid/ask")

        df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True).dt.tz_convert("US/Eastern")
        numeric_cols = ["bid", "ask", "mid", "volume", "open_interest", "strike"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].apply(_decimal_to_numeric), errors="coerce")
        df = df.set_index("ts_end").sort_index()
        expected_minutes = int((df.index[-1] - df.index[0]).total_seconds() / 60) + 1
        if len(df) != expected_minutes:
            raise RuntimeError("Option feed has missing minutes")

        data = cls(dataname=df, name=f"OPT-{contract.get('conid', '')}")
        return data


def _decimal_to_numeric(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    return value
