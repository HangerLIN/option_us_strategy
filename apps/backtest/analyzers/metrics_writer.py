from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date
from statistics import mean, median
from typing import Deque, Dict, Iterable, List, Mapping, Tuple

import backtrader as bt
from sqlalchemy import text

from apps.backtest.dao import BacktestDAO
from libs.schemas.signals import BUY_SIGNAL_CODES


@dataclass
class MetricsResult:
    total: Dict[str, float]
    daily: Dict[date, Dict[str, float]]


@dataclass
class _OpenLeg:
    quantity: float
    price: float
    signal_code: str
    fees: float


class MetricsWriter(bt.Analyzer):
    params = dict(dao=None, run_id=None)

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
            executed_price = getattr(getattr(trade, "order", None), "executed", None)
            executed_price_value = getattr(executed_price, "price", trade.price)
            self._slippage.append(trade.price - executed_price_value)
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

        total_metrics.extend(self._build_signal_metrics())
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

    def _build_signal_metrics(self) -> list[dict[str, float | int]]:
        """Attribute closed option PnL back to the entry signal that opened the leg.

        bt_trades stores one row per execution with reason_code equal to the triggering
        signal. We reconstruct FIFO round trips per option contract and aggregate the
        realized PnL by entry signal code. This answers: each signal is making or losing
        money, how often it wins, and how many executions/opportunities it had.
        """
        rows = self._fetch_bt_trades()
        opportunities = self._fetch_signal_opportunities()
        realized: dict[str, list[float]] = defaultdict(list)
        open_legs: dict[tuple[object, ...], Deque[_OpenLeg]] = defaultdict(deque)

        for row in rows:
            side = str(row.get("side") or "").upper()
            signal_code = str(row.get("reason_code") or "")
            quantity = abs(float(row.get("quantity") or 0))
            price = float(row.get("price") or 0)
            fees = float(row.get("fees") or 0)
            if quantity <= 0 or price <= 0:
                continue
            key = (
                str(row.get("symbol") or "").upper(),
                str(row.get("option_right") or "").upper(),
                str(row.get("strike") or ""),
                str(row.get("expiry") or ""),
            )
            if side == "BUY" and signal_code in BUY_SIGNAL_CODES:
                open_legs[key].append(
                    _OpenLeg(
                        quantity=quantity,
                        price=price,
                        signal_code=signal_code,
                        fees=fees,
                    )
                )
                continue
            if side != "SELL":
                continue

            remaining = quantity
            while remaining > 0 and open_legs[key]:
                leg = open_legs[key][0]
                closed_qty = min(remaining, leg.quantity)
                entry_fee = leg.fees * (closed_qty / leg.quantity) if leg.quantity else 0.0
                exit_fee = fees * (closed_qty / quantity) if quantity else 0.0
                pnl = (price - leg.price) * closed_qty * 100.0 - entry_fee - exit_fee
                realized[leg.signal_code].append(pnl)
                leg.quantity -= closed_qty
                remaining -= closed_qty
                if leg.quantity <= 1e-9:
                    open_legs[key].popleft()

        metrics: list[dict[str, float | int]] = []
        all_codes = sorted(set(BUY_SIGNAL_CODES) | set(opportunities) | set(realized))
        for code in all_codes:
            values = realized.get(code, [])
            count = len(values)
            pnl_total = sum(values)
            wins = [v for v in values if v > 0]
            losses = [v for v in values if v < 0]
            avg_pnl = pnl_total / count if count else 0.0
            win_rate = len(wins) / count if count else 0.0
            loss_rate = len(losses) / count if count else 0.0
            opp_count = opportunities.get(code, 0)
            suffix = code.removeprefix("SIG_")
            metrics.extend(
                [
                    {"run_id": self.run_id, "metric_code": f"PNL_SIG_{suffix}", "metric_value": pnl_total},
                    {"run_id": self.run_id, "metric_code": f"AVG_PNL_SIG_{suffix}", "metric_value": avg_pnl},
                    {"run_id": self.run_id, "metric_code": f"WIN_RATE_SIG_{suffix}", "metric_value": win_rate},
                    {"run_id": self.run_id, "metric_code": f"LOSS_RATE_SIG_{suffix}", "metric_value": loss_rate},
                    {"run_id": self.run_id, "metric_code": f"COUNT_EXEC_SIG_{suffix}", "metric_value": count},
                    {"run_id": self.run_id, "metric_code": f"COUNT_OPP_SIG_{suffix}", "metric_value": opp_count},
                    {"run_id": self.run_id, "metric_code": f"GROSS_WIN_SIG_{suffix}", "metric_value": sum(wins)},
                    {"run_id": self.run_id, "metric_code": f"GROSS_LOSS_SIG_{suffix}", "metric_value": abs(sum(losses))},
                    # Backward-compatible names used by the earlier completion report.
                    {"run_id": self.run_id, "metric_code": f"RET_SIG_{suffix}_TFE_MEAN", "metric_value": avg_pnl},
                    {"run_id": self.run_id, "metric_code": f"RET_SIG_{suffix}_TFE_P50", "metric_value": median(values) if values else 0.0},
                    {"run_id": self.run_id, "metric_code": f"RET_SIG_{suffix}_TFE_P90", "metric_value": self._percentile(values, 0.90)},
                    {"run_id": self.run_id, "metric_code": f"COUNT_EXEC_SIG_{suffix}_FIXED", "metric_value": count},
                    {"run_id": self.run_id, "metric_code": f"COUNT_OPP_SIG_{suffix}_TFE", "metric_value": opp_count},
                ]
            )
        return metrics

    def _fetch_bt_trades(self) -> list[Mapping[str, object]]:
        session = self.dao._session  # BacktestDAO owns the run transaction.
        rows = session.execute(
            text(
                """
                SELECT symbol, side, quantity, price, trade_ts, option_right,
                       strike, expiry, fees, reason_code
                FROM bt_trades
                WHERE run_id = :run_id
                ORDER BY trade_ts ASC, side ASC
                """
            ),
            {"run_id": self.run_id},
        ).mappings().all()
        return list(rows)

    def _fetch_signal_opportunities(self) -> dict[str, int]:
        session = self.dao._session
        rows = session.execute(
            text(
                """
                SELECT signal_code, COUNT(*) AS cnt
                FROM bt_signals
                WHERE run_id = :run_id
                  AND accepted = TRUE
                GROUP BY signal_code
                """
            ),
            {"run_id": self.run_id},
        ).all()
        return {str(code): int(cnt or 0) for code, cnt in rows}

    @staticmethod
    def _percentile(values: Iterable[float], q: float) -> float:
        ordered = sorted(values)
        if not ordered:
            return 0.0
        if len(ordered) == 1:
            return ordered[0]
        idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
        return ordered[idx]

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
