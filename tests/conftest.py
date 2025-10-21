from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APPS_DIR = ROOT / "apps"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if str(APPS_DIR) not in sys.path:
    sys.path.insert(0, str(APPS_DIR))

if "prometheus_client" not in sys.modules:
    prometheus_module = types.ModuleType("prometheus_client")

    class _MetricStub:
        def labels(self, *args: object, **kwargs: object) -> "_MetricStub":
            return self

        def inc(self, *args: object, **kwargs: object) -> None:
            return None

        def set(self, *args: object, **kwargs: object) -> None:
            return None

        def observe(self, *args: object, **kwargs: object) -> None:
            return None

    def _metric_factory(*args: object, **kwargs: object) -> _MetricStub:
        return _MetricStub()

    prometheus_module.Counter = _metric_factory  # type: ignore[attr-defined]
    prometheus_module.Gauge = _metric_factory  # type: ignore[attr-defined]
    prometheus_module.Histogram = _metric_factory  # type: ignore[attr-defined]
    prometheus_module.CONTENT_TYPE_LATEST = "text/plain"  # type: ignore[attr-defined]
    prometheus_module.generate_latest = lambda: b""  # type: ignore[attr-defined]

    sys.modules["prometheus_client"] = prometheus_module
