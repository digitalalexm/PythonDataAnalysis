"""Duplicate-event filtering.

Retried/at-least-once-delivered events share an event_id but can have
slightly different timestamps (observed up to ~2 seconds apart in the sample
data) - a classic at-least-once delivery signature, not a different event.
This module keeps the first-seen record per event_id and routes the rest to
the rejected-events log rather than dropping them silently, so "why did my
count drop" is always answerable from the output files alone.
"""
from __future__ import annotations

from typing import Iterable, Iterator, Protocol

from .models import CleanEvent, RejectedEvent


class SeenStore(Protocol):
    """Tracks which event_ids have already been processed.

    Isolated behind a protocol so the in-memory implementation below - fine
    for this dataset's scale - can be swapped for a time-bounded store (e.g.
    a TTL'd cache that only remembers the last N minutes of IDs, matching how
    Beam/Dataflow bound deduplication state) without changing dedupe_events
    itself. See README for the scaling discussion.
    """

    def seen(self, event_id: str) -> bool: ...

    def mark(self, event_id: str) -> None: ...


class InMemorySeenStore:
    """Unbounded in-memory SeenStore.

    Memory grows with the number of distinct event_ids ever seen, not
    bounded by time - adequate here, but see README for why this wouldn't
    be the production choice at high-volume, long-running scale.
    """

    def __init__(self) -> None:
        self._ids: set[str] = set()

    def seen(self, event_id: str) -> bool:
        return event_id in self._ids

    def mark(self, event_id: str) -> None:
        self._ids.add(event_id)


def dedupe_events(
    records: Iterable[CleanEvent | RejectedEvent],
    store: SeenStore | None = None,
) -> Iterator[CleanEvent | RejectedEvent]:
    """Drop repeat CleanEvents by event_id, keeping the first occurrence.

    RejectedEvents pass through untouched - a record already rejected for a
    data-quality reason doesn't need a dedup decision, and routing it through
    here would overwrite a more specific reason with a less useful one.
    """
    store = store if store is not None else InMemorySeenStore()
    for record in records:
        if isinstance(record, RejectedEvent):
            yield record
            continue
        if store.seen(record.event_id):
            yield RejectedEvent(record=record, reason="duplicate_event_id")
            continue
        store.mark(record.event_id)
        yield record
