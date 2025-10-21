from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, Tuple

from pydantic import BaseModel, Field, ValidationInfo, field_validator


class ServiceHealth(BaseModel):
    status: str
    service: str
    timestamp: datetime


class ApiResult(BaseModel):
    """Standardised API envelope for service responses."""

    ERROR_PREFIXES: ClassVar[Tuple[str, ...]] = ("BLOCK:", "REJECT:", "ERR:")

    ok: bool = Field(..., description="Indicates whether the request was processed successfully.")
    code: str = Field(
        ..., description="Machine-readable status code. Use BLOCK:/REJECT:/ERR: for failures."
    )
    message: str | None = Field(
        default=None, description="Human-readable message describing the outcome."
    )
    data: Any | None = Field(default=None, description="Payload when applicable.")

    @field_validator("code")
    @classmethod
    def validate_code(cls, value: str, info: ValidationInfo) -> str:
        """Enforce error code families for failed results."""
        ok = info.data.get("ok")
        if ok:
            return value
        if not value.startswith(cls.ERROR_PREFIXES):
            raise ValueError("Failure codes must begin with BLOCK:, REJECT:, or ERR:.")
        return value
