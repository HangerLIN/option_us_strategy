from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class BacktestRunRequest(BaseModel):
    strategy_code: str = Field(..., max_length=32)
    parquet_path: str = Field(..., description="Filesystem or mounted path to Parquet dataset.")

    def dataset_path(self) -> Path:
        return Path(self.parquet_path)


class BacktestRunRecord(BaseModel):
    run_id: str
    strategy_code: str
    parquet_path: str
    status: str
    submitted_at: datetime
    metrics_ready: bool = False


class BacktestMetrics(BaseModel):
    run_id: str
    status: str
    metrics: Optional[dict[str, float]] = None
    updated_at: datetime
