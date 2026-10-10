"""Per-record validation and normalization.

Every function here is pure: given the same input it always returns the same
output, with no I/O and no dependency on other records. That's deliberate -
these are the units that map directly onto a Beam DoFn later, and the units
that test_clean.py exercises as simple input/output tables.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from .models import CleanEvent, RejectedEvent

_SPACE_SEP_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}Z?$")
_DMY_RE = re.compile(r"^(\d{2})/(\d{2})/(\d{4}) (\d{2}):(\d{2}):(\d{2})$")
_LEADING_INT_RE = re.compile(r"-?\d+")


def normalize_timestamp(raw: Any) -> datetime | None:
    """Parse a raw timestamp into a UTC-aware datetime, or None if unrecoverable.

    Handles the formats observed in the sample data: standard ISO-8601 with a
    trailing 'Z' (the common case), the same format with a space instead of
    'T', and a DD/MM/YYYY variant from at least one upstream producer.
    Anything else - missing, empty, or genuinely unparseable - returns None;
    we don't guess at a timestamp, because a wrong guess would silently
    corrupt per-minute aggregation.
    """
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        pass

    if _SPACE_SEP_RE.match(value):
        iso_value = value.replace(" ", "T")
        try:
            return datetime.fromisoformat(iso_value.replace("Z", "+00:00"))
        except ValueError:
            pass

    match = _DMY_RE.match(value)
    if match:
        day, month, year, hour, minute, second = (int(g) for g in match.groups())
        try:
            return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        except ValueError:
            return None

    return None


def coerce_status_code(raw: Any) -> int | None:
    """Coerce a status code to int, or None if missing/invalid.

    Some producers send status codes as numeric strings (e.g. "200"); those
    are coerced. Non-numeric sentinels (e.g. "ERR") and missing values become
    None rather than being guessed at - callers computing error rates treat
    None as "unknown" and exclude it from the denominator, rather than
    silently assuming success or failure.
    """
    if isinstance(raw, bool):  # bool is a subclass of int - exclude explicitly
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return None


def coerce_latency_ms(raw: Any) -> int | None:
    """Coerce latency to a non-negative int, or None if missing/invalid.

    Some producers append units (e.g. "123ms"); the leading numeric portion
    is extracted. Negative latencies are physically impossible, so they're
    treated as invalid (None) rather than kept at face value - a negative
    number would silently skew an average latency downward.
    """
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw >= 0 else None
    if isinstance(raw, float):
        return int(raw) if raw >= 0 else None
    if isinstance(raw, str):
        match = _LEADING_INT_RE.match(raw.strip())
        if match:
            value = int(match.group())
            return value if value >= 0 else None
    return None


def _clean_optional_str(raw: Any) -> str | None:
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return None


def clean_record(raw: dict[str, Any]) -> CleanEvent | RejectedEvent:
    """Validate and normalize a single raw event.

    event_id, timestamp, and service are required: they're what the
    requested per-service-per-minute aggregation is keyed on (plus event_id
    for dedup), so a record missing any of them can't be placed anywhere
    meaningful and is rejected outright with a reason code.

    status_code, latency_ms, user_id, and event_type are optional: coerced
    where possible, set to None when invalid or missing. These don't cause a
    rejection because the record is still useful without them (e.g. it still
    counts toward request volume even if latency is unknown).
    """
    event_id = raw.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        return RejectedEvent(record=raw, reason="missing_event_id")

    timestamp = normalize_timestamp(raw.get("timestamp"))
    if timestamp is None:
        return RejectedEvent(record=raw, reason="missing_or_invalid_timestamp")

    service = _clean_optional_str(raw.get("service"))
    if service is None:
        return RejectedEvent(record=raw, reason="missing_service")

    return CleanEvent(
        event_id=event_id.strip(),
        timestamp=timestamp,
        service=service,
        event_type=_clean_optional_str(raw.get("event_type")),
        latency_ms=coerce_latency_ms(raw.get("latency_ms")),
        status_code=coerce_status_code(raw.get("status_code")),
        user_id=_clean_optional_str(raw.get("user_id")),
    )
