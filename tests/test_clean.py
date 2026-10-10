from datetime import datetime, timezone

import pytest

from pipeline.clean import (
    clean_record,
    coerce_latency_ms,
    coerce_status_code,
    normalize_timestamp,
)
from pipeline.models import CleanEvent, RejectedEvent


# Each case here is taken directly from events.jsonl, not invented - the
# dataset is what told us these formats existed in the first place.
@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2025-01-12T10:32:14Z", datetime(2025, 1, 12, 10, 32, 14, tzinfo=timezone.utc)),
        ("2025-01-12 10:00:00Z", datetime(2025, 1, 12, 10, 0, 0, tzinfo=timezone.utc)),
        ("12/01/2025 10:00:00", datetime(2025, 1, 12, 10, 0, 0, tzinfo=timezone.utc)),
        ("", None),
        (None, None),
        ("not a timestamp", None),
        (12345, None),  # wrong type entirely
    ],
)
def test_normalize_timestamp(raw, expected):
    assert normalize_timestamp(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        (200, 200),
        ("200", 200),
        ("ERR", None),
        (None, None),
        (True, None),  # bool is an int subclass - must not be coerced to 1
    ],
)
def test_coerce_status_code(raw, expected):
    assert coerce_status_code(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        (123, 123),
        ("123ms", 123),
        ("", None),
        (-5, None),
        ("-5", None),
        (None, None),
        (12.0, 12),
    ],
)
def test_coerce_latency_ms(raw, expected):
    assert coerce_latency_ms(raw) == expected


def test_clean_record_accepts_fully_valid_event():
    raw = {
        "event_id": "9f1c2b3e",
        "timestamp": "2025-01-12T10:32:14Z",
        "service": "checkout",
        "event_type": "request_completed",
        "latency_ms": 123,
        "status_code": 200,
        "user_id": "u123",
    }
    result = clean_record(raw)
    assert isinstance(result, CleanEvent)
    assert result.event_id == "9f1c2b3e"
    assert result.service == "checkout"
    assert result.latency_ms == 123
    assert result.status_code == 200


def test_clean_record_coerces_optional_fields_without_rejecting():
    raw = {
        "event_id": "abc123",
        "timestamp": "2025-01-12T10:32:14Z",
        "service": "checkout",
        "latency_ms": "123ms",
        "status_code": "ERR",
        # user_id and event_type missing entirely
    }
    result = clean_record(raw)
    assert isinstance(result, CleanEvent)
    assert result.latency_ms == 123
    assert result.status_code is None
    assert result.user_id is None
    assert result.event_type is None


@pytest.mark.parametrize(
    "missing_field, reason",
    [
        ("event_id", "missing_event_id"),
        ("timestamp", "missing_or_invalid_timestamp"),
        ("service", "missing_service"),
    ],
)
def test_clean_record_rejects_when_required_field_missing(missing_field, reason):
    raw = {
        "event_id": "abc123",
        "timestamp": "2025-01-12T10:32:14Z",
        "service": "checkout",
        "status_code": 200,
        "latency_ms": 10,
    }
    del raw[missing_field]
    result = clean_record(raw)
    assert isinstance(result, RejectedEvent)
    assert result.reason == reason
    assert result.record is raw


def test_clean_record_rejects_unparseable_timestamp():
    raw = {"event_id": "abc123", "timestamp": "garbage", "service": "checkout"}
    result = clean_record(raw)
    assert isinstance(result, RejectedEvent)
    assert result.reason == "missing_or_invalid_timestamp"
