"""Shared data shapes for the events pipeline.

Each type owns its own `to_dict()` so serialization format (e.g. how a
datetime is stringified) is decided once here, instead of being re-derived
in every writer that touches these records.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class CleanEvent:
    """A validated, normalized event ready for aggregation or export.

    event_id, timestamp, and service are guaranteed non-null - a record
    missing any of them never becomes a CleanEvent (see clean.clean_record).
    The remaining fields are best-effort: present when they could be parsed,
    None when they couldn't, rather than causing a rejection.
    """

    event_id: str
    timestamp: datetime
    service: str
    event_type: str | None
    latency_ms: int | None
    status_code: int | None
    user_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "service": self.service,
            "event_type": self.event_type,
            "latency_ms": self.latency_ms,
            "status_code": self.status_code,
            "user_id": self.user_id,
        }


@dataclass(frozen=True, slots=True)
class RejectedEvent:
    """A record that was dropped, with a machine-readable reason.

    `record` holds whatever was available at the point of rejection: the raw
    input dict for field-validation failures, or a CleanEvent for records
    rejected later (e.g. as duplicates) - kept loosely typed deliberately, so
    every rejection stage can attach whatever context it has without forcing
    a shared schema on inputs that don't share one.
    """

    record: Any
    reason: str

    def to_dict(self) -> dict[str, Any]:
        record = self.record
        if isinstance(record, CleanEvent):
            record = record.to_dict()
        return {"reason": self.reason, "record": record}


@dataclass(frozen=True, slots=True)
class MetricRow:
    """Aggregated metrics for one service in one one-minute window."""

    service: str
    minute: datetime
    request_count: int
    avg_latency_ms: float | None
    error_rate: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "minute": self.minute.isoformat(),
            "request_count": self.request_count,
            "avg_latency_ms": self.avg_latency_ms,
            "error_rate": self.error_rate,
        }
