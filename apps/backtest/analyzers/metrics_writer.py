from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from statistics import mean
from typing import Dict, List, Tuple

import backtrader as bt

from apps.backtest.dao import BacktestDAO


@dataclass
class MetricsResult:
    total: Dict[str, float]
    daily: Dict[date, Dict[str, float]]


class MetricsWriter(bt.Analyzer):
    params = dict(dao=None, run_id=None)

    _SPECIAL_SIGNALS = [
        "AM_BOTTOM_A1",
        "AM_SELL_C1",
        "AM_CONFLUENCE_BUY_A2",
        "AM_CONFLUENCE_SELL_S2",
    ]
    _RET_SUFFIXES = ["TFE_MEAN", "TFE_P50", "TFE_P90"]

    def __init__(self) -> None:
        if self.p.dao is None or self.p.run_id is None:
            raise ValueError("MetricsWriter requires dao and run_id parameters")
        self.dao: BacktestDAO = self.p.dao
        self.run_id: int = self.p.run_id
        self._daily_pnl: Dict[date, float] = defaultdict(float)
        self._daily_trades: Dict[date, List[Tuple[float, float]]] = defaultdict(list)
        self._fees: List[float] = []
        self._slippage: List[float] = []
        self._pnl_series: List[float] = []

    def notify_trade(self, trade: bt.Trade) -> None:
        if trade.isclosed:
            pnl = trade.pnl
            fees = trade.commission
            self._fees.append(fees)
            self._slippage.append(trade.price - trade.order.executed.price)
            end_date = trade.data.datetime.date(0)
            self._daily_pnl[end_date] += pnl
            self._daily_trades[end_date].append((pnl, fees))
            self._pnl_series.append(pnl)

    def stop(self) -> None:
        total_pnl = sum(self._pnl_series)
        gross_wins = sum(p for p in self._pnl_series if p > 0)
        gross_losses = sum(abs(p) for p in self._pnl_series if p < 0)
        num_trades = len(self._pnl_series)
        win_trades = len([p for p in self._pnl_series if p > 0])
        win_rate = win_trades / num_trades if num_trades else 0
        total_fees = sum(self._fees)
        total_slippage = sum(self._slippage)

        sharpe = self._compute_sharpe()
        max_drawdown = self._compute_max_drawdown()

        total_metrics = [
            {"run_id": self.run_id, "metric_code": "total_pnl", "metric_value": total_pnl},
            {"run_id": self.run_id, "metric_code": "gross_wins", "metric_value": gross_wins},
            {"run_id": self.run_id, "metric_code": "gross_losses", "metric_value": gross_losses},
            {"run_id": self.run_id, "metric_code": "win_rate", "metric_value": win_rate},
            {"run_id": self.run_id, "metric_code": "num_trades", "metric_value": num_trades},
            {"run_id": self.run_id, "metric_code": "fees_total", "metric_value": total_fees},
            {
                "run_id": self.run_id,
                "metric_code": "slippage_total",
                "metric_value": total_slippage,
            },
        ]
        if sharpe is not None:
            total_metrics.append(
                {"run_id": self.run_id, "metric_code": "sharpe", "metric_value": sharpe}
            )
        if max_drawdown is not None:
            total_metrics.append(
                {"run_id": self.run_id, "metric_code": "max_drawdown", "metric_value": max_drawdown}
            )

        existing_codes = {entry["metric_code"] for entry in total_metrics}
        for signal in self._SPECIAL_SIGNALS:
            for suffix in self._RET_SUFFIXES:
                metric_code = f"RET_SIG_{signal}_{suffix}"
                if metric_code not in existing_codes:
                    total_metrics.append(
                        {"run_id": self.run_id, "metric_code": metric_code, "metric_value": 0.0}
                    )
                    existing_codes.add(metric_code)

        self.dao.record_metrics_total(total_metrics)

        rows = []
        for trade_date, values in sorted(self._daily_pnl.items()):
            rows.append(
                {
                    "run_id": self.run_id,
                    "trade_date": trade_date,
                    "metric_code": "daily_pnl",
                    "metric_value": values,
                }
            )
        self.dao.record_metrics_daily(rows)

    def _compute_sharpe(self) -> float | None:
        pnl_series = self._pnl_series
        if len(pnl_series) < 2:
            return None
        avg = mean(pnl_series)
        variance = mean((x - avg) ** 2 for x in pnl_series)
        if variance == 0:
            return None
        return avg / variance**0.5

    def _compute_max_drawdown(self) -> float | None:
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for pnl in self._pnl_series:
            cumulative += pnl
            peak = max(peak, cumulative)
            drawdown = peak - cumulative
            max_dd = max(max_dd, drawdown)
        return max_dd
