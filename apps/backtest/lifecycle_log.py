from __future__ import annotations

import json
import math
import re
import threading
import warnings
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_LOG_DIR = _PROJECT_ROOT / "logs" / "backtest"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_.-]+")


def resolve_lifecycle_log_path(
    path: str | Path | None = None,
    *,
    batch_id: str | None = None,
    run_id: int | None = None,
) -> Path:
    """Return a stable JSONL path for one backtest batch or standalone run."""

    if path is not None:
        return Path(path).expanduser().resolve()
    raw_name = batch_id or (f"run-{run_id}" if run_id is not None else "backtest")
    safe_name = _SAFE_NAME.sub("_", raw_name).strip("._") or "backtest"
    return _DEFAULT_LOG_DIR / f"{safe_name}.jsonl"


@dataclass
class _SinkState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    counts: Counter[str] = field(default_factory=Counter)
    sequence: int = 0
    disabled_reason: str | None = None
    warned: bool = False


class BacktestLifecycleLogger:
    """Append-only, machine-readable audit log for a backtest lifecycle.

    The logger deliberately stays independent of the process-wide structlog setup:
    CLI, API, and direct test runners therefore produce the same JSONL schema. A
    logging failure is reported once and never changes trading behaviour.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        context: Mapping[str, Any] | None = None,
        _state: _SinkState | None = None,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        self._context = dict(context or {})
        self._state = _state or _SinkState()

    def bind(self, **context: Any) -> "BacktestLifecycleLogger":
        merged = {**self._context, **context}
        return BacktestLifecycleLogger(self.path, context=merged, _state=self._state)

    def emit(self, event: str, **fields: Any) -> None:
        event_name = str(event).strip().upper()
        if not event_name or self._state.disabled_reason is not None:
            return

        with self._state.lock:
            self._state.sequence += 1
            payload = {
                "schema_version": 1,
                "sequence": self._state.sequence,
                "logged_at": datetime.now(timezone.utc),
                "event": event_name,
                **self._context,
                **fields,
            }
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                line = json.dumps(
                    _normalise(payload),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(line)
                    handle.write("\n")
                self._state.counts[event_name] += 1
            except (OSError, TypeError, ValueError) as exc:
                self._state.disabled_reason = f"{type(exc).__name__}: {exc}"
                if not self._state.warned:
                    self._state.warned = True
                    warnings.warn(
                        f"Backtest lifecycle logging disabled for {self.path}: {exc}",
                        RuntimeWarning,
                        stacklevel=2,
                    )

    def snapshot_counts(self) -> dict[str, int]:
        with self._state.lock:
            return dict(sorted(self._state.counts.items()))

    @property
    def disabled_reason(self) -> str | None:
        return self._state.disabled_reason


def _normalise(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return _normalise(value.value)
    if isinstance(value, Mapping):
        return {str(key): _normalise(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_normalise(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _normalise(value.item())
        except (TypeError, ValueError):
            pass
    return str(value)
