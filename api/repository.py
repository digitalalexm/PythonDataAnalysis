"""Data access for aggregated metrics.

Route handlers talk to MetricsRepository, never to the CSV file directly -
that's what lets the data source change (CSV today, a Bigtable/BigQuery
client per the Part 2 design tomorrow) without touching api/main.py.
"""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from pipeline.models import MetricRow


def _parse_optional_float(value: str) -> float | None:
    return float(value) if value else None


class MetricsRepository:
    """In-memory metrics store, queryable by service and time range."""

    def __init__(self, records: list[MetricRow]) -> None:
        self._records = records

    @classmethod
    def from_csv(cls, path: str | Path) -> "MetricsRepository":
        """Load metrics.csv (as produced by pipeline.run_pipeline) into memory.

        Loaded once at API startup, not per-request - see api/main.py.
        """
        records: list[MetricRow] = []
        with open(path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                records.append(
                    MetricRow(
                        service=row["service"],
                        minute=datetime.fromisoformat(row["minute"]),
                        request_count=int(row["request_count"]),
                        avg_latency_ms=_parse_optional_float(row["avg_latency_ms"]),
                        error_rate=_parse_optional_float(row["error_rate"]),
                    )
                )
        return cls(records)

    def query(
        self,
        service: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[MetricRow]:
        """Return rows matching all given filters, sorted by (service, minute).

        A filter left as None is not applied - e.g. omitting `service`
        returns rows for every service, not zero rows.
        """
        results = self._records
        if service is not None:
            results = [r for r in results if r.service == service]
        if start is not None:
            results = [r for r in results if r.minute >= start]
        if end is not None:
            results = [r for r in results if r.minute <= end]
        return sorted(results, key=lambda r: (r.service, r.minute))
