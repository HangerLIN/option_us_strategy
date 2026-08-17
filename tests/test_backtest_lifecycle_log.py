from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from apps.backtest.bt_runner import _EquityMetricsAccumulator
from apps.backtest.lifecycle_log import BacktestLifecycleLogger, resolve_lifecycle_log_path


def test_lifecycle_logger_appends_jsonl_with_shared_sequence(tmp_path) -> None:
    path = tmp_path / "nested" / "lifecycle.jsonl"
    logger = BacktestLifecycleLogger(path, context={"run_id": 7, "symbol": "AAPL"})

    logger.emit(
        "order_submitted",
        event_time=datetime(2026, 1, 2, 14, 31, tzinfo=timezone.utc),
        limit_price=Decimal("1.25"),
    )
    logger.bind(trace_id="trace-1").emit(
        "order_filled",
        fill_price=Decimal("1.30"),
    )

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [row["event"] for row in rows] == ["ORDER_SUBMITTED", "ORDER_FILLED"]
    assert [row["sequence"] for row in rows] == [1, 2]
    assert rows[0]["limit_price"] == "1.25"
    assert rows[1]["fill_price"] == "1.30"
    assert rows[1]["trace_id"] == "trace-1"
    assert logger.snapshot_counts() == {"ORDER_FILLED": 1, "ORDER_SUBMITTED": 1}


def test_default_lifecycle_path_is_sanitised() -> None:
    path = resolve_lifecycle_log_path(batch_id="batch/with spaces")

    assert path.name == "batch_with_spaces.jsonl"
    assert path.parent.name == "backtest"


def test_equity_metric_names_do_not_duplicate_signal_prefix() -> None:
    recorded: list[dict[str, object]] = []
    dao = SimpleNamespace(
        record_metrics_total=lambda metrics: recorded.extend(metrics),
    )
    metrics = _EquityMetricsAccumulator()
    metrics.add_execution(signal_code="SIG_TEST_ENTRY")
    metrics.add_opportunity("SIG_TEST_ENTRY", None)
    metrics.add_tfe("SIG_TEST_ENTRY", Decimal("0.01"))

    metrics.flush(dao, run_id=7)

    codes = {str(row["metric_code"]) for row in recorded}
    assert "COUNT_EXEC_SIG_TEST_ENTRY" in codes
    assert "COUNT_OPP_SIG_TEST_ENTRY" in codes
    assert "RET_SIG_TEST_ENTRY_TFE_MEAN" in codes
    assert not any("SIG_SIG" in code for code in codes)
