from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd

from apps.backtest.dao import BacktestDAO


def test_upsert_equity_indicators_from_df_persists_bollinger_columns() -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.statement = ""
            self.payloads = []

        def execute(self, statement, payloads):
            self.statement = str(statement)
            self.payloads = payloads

    ts = pd.Timestamp(datetime(2026, 5, 22, 13, 29, tzinfo=timezone.utc))
    df = pd.DataFrame(
        [
            {
                "rsi6": Decimal("55"),
                "boll_mid": Decimal("100.5"),
                "boll_up": Decimal("105.5"),
                "boll_dn": Decimal("95.5"),
                "ao": Decimal("1.25"),
            }
        ],
        index=pd.DatetimeIndex([ts]),
    )

    session = FakeSession()
    dao = BacktestDAO(
        session,  # type: ignore[arg-type]
        option_bar_table="bars1m_option",
        option_chain_table="option_chain_meta",
    )
    dao.upsert_equity_indicators_from_df(symbol="AMD", df=df)

    assert "boll_mid" in session.statement
    assert "boll_up" in session.statement
    assert "boll_dn" in session.statement
    assert session.payloads[0]["boll_mid"] == Decimal("100.5")
    assert session.payloads[0]["boll_up"] == Decimal("105.5")
    assert session.payloads[0]["boll_dn"] == Decimal("95.5")
