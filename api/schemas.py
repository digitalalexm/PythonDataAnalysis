"""Pydantic models for the API's request/response boundary.

Kept separate from pipeline.models.MetricRow deliberately: that dataclass is
the internal domain representation the pipeline produces, this is the wire
format promised to API clients. Changing one shouldn't force changing the
other.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from pipeline.models import MetricRow


class MetricResponse(BaseModel):
    service: str
    minute: datetime
    request_count: int
    avg_latency_ms: float | None
    error_rate: float | None

    @classmethod
    def from_metric_row(cls, row: MetricRow) -> "MetricResponse":
        return cls(
            service=row.service,
            minute=row.minute,
            request_count=row.request_count,
            avg_latency_ms=row.avg_latency_ms,
            error_rate=row.error_rate,
        )
