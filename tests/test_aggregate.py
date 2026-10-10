from datetime import datetime, timezone

from pipeline.aggregate import aggregate_events
from pipeline.models import CleanEvent


def _event(
    service="checkout",
    minute=10,
    second=0,
    latency_ms=100,
    status_code=200,
) -> CleanEvent:
    return CleanEvent(
        event_id="id",
        timestamp=datetime(2025, 1, 12, 10, minute, second, tzinfo=timezone.utc),
        service=service,
        event_type="request_completed",
        latency_ms=latency_ms,
        status_code=status_code,
        user_id="u1",
    )


def test_groups_by_service_and_minute_floor():
    events = [
        _event(minute=10, second=5),
        _event(minute=10, second=59),  # same minute bucket as above
        _event(minute=11, second=0),  # different minute
    ]
    rows = list(aggregate_events(events))
    assert len(rows) == 2
    assert rows[0].minute == datetime(2025, 1, 12, 10, 10, 0, tzinfo=timezone.utc)
    assert rows[0].request_count == 2
    assert rows[1].minute == datetime(2025, 1, 12, 10, 11, 0, tzinfo=timezone.utc)
    assert rows[1].request_count == 1


def test_separates_services_sharing_the_same_minute():
    events = [_event(service="checkout"), _event(service="payments")]
    rows = list(aggregate_events(events))
    assert {r.service for r in rows} == {"checkout", "payments"}
    assert all(r.request_count == 1 for r in rows)


def test_avg_latency_ignores_unknown_values():
    events = [
        _event(latency_ms=100),
        _event(latency_ms=200),
        _event(latency_ms=None),
    ]
    row = next(iter(aggregate_events(events)))
    assert row.request_count == 3
    assert row.avg_latency_ms == 150  # the None is excluded, not treated as 0


def test_error_rate_excludes_unknown_status_from_denominator():
    events = [
        _event(status_code=200),
        _event(status_code=500),
        _event(status_code=None),
    ]
    row = next(iter(aggregate_events(events)))
    assert row.request_count == 3
    assert row.error_rate == 0.5  # 1 error out of 2 *known* statuses


def test_error_rate_is_none_when_no_status_known():
    row = next(iter(aggregate_events([_event(status_code=None)])))
    assert row.error_rate is None


def test_avg_latency_is_none_when_no_latency_known():
    row = next(iter(aggregate_events([_event(latency_ms=None)])))
    assert row.avg_latency_ms is None


def test_empty_input_yields_no_rows():
    assert list(aggregate_events([])) == []
