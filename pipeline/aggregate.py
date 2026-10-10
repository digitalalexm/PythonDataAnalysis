"""Per-service, per-minute metric aggregation.

Aggregation is an incremental accumulator keyed by (service, minute), not a
collect-everything-then-groupby: memory scales with the number of distinct
(service, minute) keys, not the number of input records, so this stays
bounded in memory regardless of how large the input stream is. This mirrors
Beam's windowed CombinePerKey and is meant to port to it directly.

This is still a batch-style aggregation in one respect: it only yields rows
after the whole input is exhausted, because a late-arriving record could
still belong to an already-seen minute. A true streaming version would emit
a window's row once a watermark declares that window closed (with an
"allowed lateness" policy for stragglers) - see README for the trade-off.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator

from .models import CleanEvent, MetricRow


def _floor_to_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


@dataclass
class _Accumulator:
    request_count: int = 0
    latency_sum: int = 0
    latency_count: int = 0
    status_known_count: int = 0
    error_count: int = 0

    def add(self, event: CleanEvent) -> None:
        self.request_count += 1
        if event.latency_ms is not None:
            self.latency_sum += event.latency_ms
            self.latency_count += 1
        if event.status_code is not None:
            self.status_known_count += 1
            if not (200 <= event.status_code < 300):
                self.error_count += 1

    def to_row(self, service: str, minute: datetime) -> MetricRow:
        avg_latency = (
            self.latency_sum / self.latency_count if self.latency_count else None
        )
        # Error rate is computed over records with a known status_code only.
        # Records with an unknown status aren't counted as errors or
        # successes - mixing them into either bucket would make the rate
        # look more confident than the data actually supports.
        error_rate = (
            self.error_count / self.status_known_count
            if self.status_known_count
            else None
        )
        return MetricRow(
            service=service,
            minute=minute,
            request_count=self.request_count,
            avg_latency_ms=avg_latency,
            error_rate=error_rate,
        )


def aggregate_events(events: Iterable[CleanEvent]) -> Iterator[MetricRow]:
    """Compute request count, average latency, and error rate per (service, minute).

    Rows are yielded sorted by (service, minute) for deterministic,
    diffable output - useful both for tests and for anyone reviewing
    metrics.csv by eye.
    """
    buckets: dict[tuple[str, datetime], _Accumulator] = {}
    for event in events:
        key = (event.service, _floor_to_minute(event.timestamp))
        buckets.setdefault(key, _Accumulator()).add(event)

    for (service, minute), acc in sorted(buckets.items(), key=lambda kv: kv[0]):
        yield acc.to_row(service, minute)
