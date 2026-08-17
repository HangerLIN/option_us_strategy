from __future__ import annotations

import math
from typing import Any

from backtrader.brokers.bbroker import BackBroker


class QuoteAwareOptionBroker(BackBroker):
    """Execute option limit orders against bid/ask quotes when available."""

    def _try_exec_limit(self, order: Any, popen: float, phigh: float, plow: float, plimit: float):
        bid, ask = self._current_quote(order.data)
        if bid is None or ask is None:
            # Invalid or stale L1 on an option feed must remain unfilled. Falling
            # back to its synthetic mid OHLC would manufacture an execution.
            if hasattr(order.data, "bid") or hasattr(order.data, "ask"):
                return
            return super()._try_exec_limit(order, popen, phigh, plow, plimit)

        if order.isbuy():
            if plimit >= ask:
                if self._reject_invalid_fill(order, ask):
                    return
                self._execute(order, ago=0, price=ask)
            return

        if plimit <= bid:
            if self._reject_invalid_fill(order, bid):
                return
            self._execute(order, ago=0, price=bid)

    def _reject_invalid_fill(self, order: Any, price: float) -> bool:
        validator = getattr(getattr(order, "owner", None), "_validate_order_fill", None)
        if not callable(validator):
            return False
        reason = validator(order, price)
        if not reason:
            return False
        order.addinfo(fill_reject_reason=str(reason), rejected_fill_price=price)
        order.reject(self)
        self.notify(order)
        return True

    @staticmethod
    def _current_quote(data: Any) -> tuple[float | None, float | None]:
        if not hasattr(data, "bid") or not hasattr(data, "ask"):
            return None, None
        try:
            bid = float(data.bid[0])
            ask = float(data.ask[0])
        except (TypeError, ValueError, IndexError):
            return None, None
        if not math.isfinite(bid) or not math.isfinite(ask):
            return None, None
        if bid <= 0 or ask <= bid:
            return None, None
        if hasattr(data, "quote_age_seconds"):
            try:
                quote_age_seconds = float(data.quote_age_seconds[0])
            except (TypeError, ValueError, IndexError):
                return None, None
            if not math.isfinite(quote_age_seconds) or quote_age_seconds > 60:
                return None, None
        return bid, ask
