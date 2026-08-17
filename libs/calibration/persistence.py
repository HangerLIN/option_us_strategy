from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from libs.db.models import (
    CalibrationArtifact as CalibrationArtifactModel,
    CalibrationMetric as CalibrationMetricModel,
    CalibrationParam as CalibrationParamModel,
    CalibrationRun as CalibrationRunModel,
)

from .registry import CalibrationResult


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


def persist_calibration_result(
    session: Session,
    result: CalibrationResult,
    *,
    status: str = "COMPLETED",
    completed_at: datetime | None = None,
) -> CalibrationRunModel:
    split = result.split
    objective_metric = result.metadata.get("objective")
    run = CalibrationRunModel(
        strategy_code=result.strategy_code,
        calibration_version=result.calibration_version,
        status=status,
        train_start=split.train_start if split else None,
        train_end=split.train_end if split else None,
        validation_start=split.validation_start if split else None,
        validation_end=split.validation_end if split else None,
        test_start=split.test_start if split else None,
        test_end=split.test_end if split else None,
        objective_metric=str(objective_metric) if objective_metric is not None else None,
        maximize=bool(result.metadata.get("maximize", True)),
        metadata_json=_jsonable(result.metadata),
        completed_at=completed_at or datetime.now(timezone.utc),
    )
    session.add(run)
    session.flush()

    for param in result.parameters:
        session.add(
            CalibrationParamModel(
                calibration_id=run.calibration_id,
                param_name=param.name,
                param_value={"value": _jsonable(param.value)},
                param_group=param.group,
            )
        )
    for metric in result.metrics:
        session.add(
            CalibrationMetricModel(
                calibration_id=run.calibration_id,
                metric_name=metric.name,
                metric_value=metric.value,
                metric_scope=metric.scope,
            )
        )
    for artifact in result.artifacts:
        session.add(
            CalibrationArtifactModel(
                calibration_id=run.calibration_id,
                artifact_type=artifact.artifact_type,
                uri=artifact.uri,
                metadata_json=_jsonable(artifact.metadata),
            )
        )
    session.flush()
    return run
