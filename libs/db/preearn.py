from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Dict, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

def evaluate_preearn_guard(
    session: Session,
    *,
    symbol: str,
    trade_date: date,
    days_min: int,
    days_max: int,
    atr_pct_max: Decimal,
) -> Tuple[bool, Dict[str, object]]:
    """Evaluate whether a symbol is allowed to open new exposure during the pre-earn window.

    Returns (allowed, context).
    """
    symbol_key = symbol.upper()
    earnings_row = session.execute(
        text(
            """
            SELECT earnings_date
            FROM earnings_calendar
            WHERE symbol = :symbol
              AND earnings_date >= :trade_date
            ORDER BY earnings_date ASC
            LIMIT 1
            """
        ),
        {"symbol": symbol_key, "trade_date": trade_date},
    ).fetchone()

    if earnings_row is None:
        return True, {"pre_earn": False}

    earnings_value = earnings_row[0] if hasattr(earnings_row, "__iter__") else None
    if isinstance(earnings_value, date):
        earnings_date = earnings_value
    else:
        earnings_date = earnings_row.earnings_date if hasattr(earnings_row, "earnings_date") else earnings_value
    if not isinstance(earnings_date, date):
        return True, {"pre_earn": False}
    if earnings_date is None:
        return True, {"pre_earn": False}

    days_to = (earnings_date - trade_date).days
    context: Dict[str, object] = {
        "pre_earn": True,
        "earnings_date": earnings_date.isoformat(),
        "days_to": days_to,
    }

    if days_to < days_min or days_to > days_max:
        return True, context

    ma_row = session.execute(
        text(
            """
            SELECT close_rth, sma60, sma60_slope
            FROM v_daily_ma60
            WHERE symbol = :symbol
              AND trade_date_et::date = :trade_date
            """
        ),
        {"symbol": symbol_key, "trade_date": trade_date},
    ).fetchone()

    close_rth_val = ma_row[0] if ma_row else None
    sma60_val = ma_row[1] if ma_row else None
    sma60_slope_val = ma_row[2] if ma_row else None

    close_rth = Decimal(str(close_rth_val)) if close_rth_val is not None else None
    sma60 = Decimal(str(sma60_val)) if sma60_val is not None else None
    sma60_slope = Decimal(str(sma60_slope_val)) if sma60_slope_val is not None else None

    m60_up = (
        close_rth is not None
        and sma60 is not None
        and sma60_slope is not None
        and close_rth > sma60
        and sma60_slope > Decimal("0")
    )
    context["m60_up"] = m60_up
    if close_rth is not None:
        context["close_rth"] = str(close_rth)
    if sma60 is not None:
        context["sma60"] = str(sma60)
    if sma60_slope is not None:
        context["sma60_slope"] = str(sma60_slope)

    atr_row = session.execute(
        text(
            """
            SELECT atr_pct
            FROM v_daily_atr_pct
            WHERE symbol = :symbol
              AND trade_date = :trade_date
            """
        ),
        {"symbol": symbol_key, "trade_date": trade_date},
    ).fetchone()

    atr_val = atr_row[0] if atr_row else None
    atr_pct = Decimal(str(atr_val)) if atr_val is not None else None
    atr_pass = atr_pct is not None and atr_pct < atr_pct_max
    context["atr_pass"] = atr_pass
    if atr_pct is not None:
        context["atr_pct"] = str(atr_pct)
    context["atr_threshold"] = str(atr_pct_max)

    allowed = m60_up or atr_pass
    context["allowed"] = allowed
    return allowed, context
