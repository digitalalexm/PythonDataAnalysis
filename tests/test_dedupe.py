from datetime import datetime, timezone

from pipeline.dedupe import dedupe_events
from pipeline.models import CleanEvent, RejectedEvent


def _event(event_id: str, second: int = 0) -> CleanEvent:
    return CleanEvent(
        event_id=event_id,
        timestamp=datetime(2025, 1, 12, 10, 0, second, tzinfo=timezone.utc),
        service="checkout",
        event_type="request_completed",
        latency_ms=100,
        status_code=200,
        user_id="u1",
    )


def test_keeps_first_occurrence_and_rejects_the_rest():
    events = [_event("a"), _event("a", second=2), _event("b")]
    results = list(dedupe_events(events))

    kept = [r for r in results if isinstance(r, CleanEvent)]
    rejected = [r for r in results if isinstance(r, RejectedEvent)]

    assert [e.event_id for e in kept] == ["a", "b"]
    assert len(rejected) == 1
    assert rejected[0].reason == "duplicate_event_id"
    assert rejected[0].record.event_id == "a"


def test_already_rejected_records_pass_through_untouched():
    raw = {"event_id": None}
    pre_rejected = RejectedEvent(record=raw, reason="missing_event_id")
    results = list(dedupe_events([pre_rejected, _event("a")]))

    assert results[0] is pre_rejected
    assert isinstance(results[1], CleanEvent)


def test_no_duplicates_passes_everything_through():
    events = [_event("a"), _event("b"), _event("c")]
    results = list(dedupe_events(events))
    assert results == events
