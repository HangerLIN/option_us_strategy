from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from itertools import product
from typing import Any, Callable, Mapping, Protocol, Sequence


@dataclass(frozen=True, slots=True)
class WalkForwardSplit:
    train_start: date
    train_end: date
    validation_start: date
    validation_end: date
    test_start: date | None = None
    test_end: date | None = None


@dataclass(frozen=True, slots=True)
class CalibrationParameter:
    name: str
    value: Any
    group: str = "strategy"


@dataclass(frozen=True, slots=True)
class CalibrationMetric:
    name: str
    value: Decimal
    scope: str = "validation"


@dataclass(frozen=True, slots=True)
class CalibrationArtifact:
    artifact_type: str
    uri: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CalibrationResult:
    strategy_code: str
    calibration_version: str
    parameters: list[CalibrationParameter]
    metrics: list[CalibrationMetric]
    artifacts: list[CalibrationArtifact] = field(default_factory=list)
    split: WalkForwardSplit | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class CalibrationJob(Protocol):
    def run(self) -> CalibrationResult:
        ...


def build_walk_forward_splits(
    windows: Sequence[tuple[date, date, date, date]],
) -> list[WalkForwardSplit]:
    return [
        WalkForwardSplit(
            train_start=train_start,
            train_end=train_end,
            validation_start=validation_start,
            validation_end=validation_end,
        )
        for train_start, train_end, validation_start, validation_end in windows
    ]


class GridSearchCalibrationJob:
    def __init__(
        self,
        *,
        strategy_code: str,
        calibration_version: str,
        param_grid: Mapping[str, Sequence[Any]],
        objective: Callable[[dict[str, Any]], Mapping[str, Any]],
        metric_name: str,
        maximize: bool = True,
        split: WalkForwardSplit | None = None,
    ) -> None:
        if not param_grid:
            raise ValueError("param_grid must not be empty")
        self.strategy_code = strategy_code
        self.calibration_version = calibration_version
        self.param_grid = param_grid
        self.objective = objective
        self.metric_name = metric_name
        self.maximize = maximize
        self.split = split

    def run(self) -> CalibrationResult:
        names = list(self.param_grid)
        best_params: dict[str, Any] | None = None
        best_metrics: Mapping[str, Any] | None = None
        best_score: Decimal | None = None

        for values in product(*(self.param_grid[name] for name in names)):
            params = dict(zip(names, values))
            metrics = self.objective(params)
            if self.metric_name not in metrics:
                raise ValueError(f"objective did not return metric {self.metric_name!r}")
            score = Decimal(str(metrics[self.metric_name]))
            is_better = best_score is None or (
                score > best_score if self.maximize else score < best_score
            )
            if is_better:
                best_score = score
                best_params = params
                best_metrics = metrics

        assert best_params is not None and best_metrics is not None
        return CalibrationResult(
            strategy_code=self.strategy_code,
            calibration_version=self.calibration_version,
            parameters=[CalibrationParameter(name=name, value=value) for name, value in best_params.items()],
            metrics=[
                CalibrationMetric(name=name, value=Decimal(str(value)))
                for name, value in best_metrics.items()
                if isinstance(value, (int, float, str, Decimal))
            ],
            split=self.split,
            metadata={"method": "grid_search", "objective": self.metric_name},
        )
