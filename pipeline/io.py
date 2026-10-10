"""Streaming I/O helpers for the events pipeline.

Every function here is a generator or writes incrementally - none of them
build a full list of records in memory. That's what lets the same code path
handle the 2k-line sample file and a multi-gigabyte production file without
changes.
"""
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any, Iterable, Iterator

logger = logging.getLogger(__name__)


def read_jsonl(path: str | Path) -> Iterator[dict[str, Any]]:
    """Yield one parsed JSON object per line from a JSON Lines file.

    Lines that aren't valid JSON are logged and skipped, not raised - a
    single corrupt line shouldn't abort a multi-gigabyte run. This is a
    different failure class from a well-formed-but-invalid record (e.g.
    missing a required field): that's a data-quality problem handled by
    pipeline.clean, and shows up in the rejected-events log. A line that
    isn't JSON at all is a transport/corruption problem, so it's logged here
    instead and doesn't get a reason code in that log.
    """
    with open(path, "r", encoding="utf-8") as f:
        for line_number, raw_line in enumerate(f, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSON at %s:%d", path, line_number)


def write_jsonl(path: str | Path, rows: Iterable[Any]) -> int:
    """Write objects exposing `to_dict()` as JSON Lines. Returns the row count.

    Writes one line at a time rather than building a list of serialized
    strings first, so memory use doesn't grow with input size.
    """
    count = 0
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row.to_dict()))
            f.write("\n")
            count += 1
    return count


def write_csv(path: str | Path, rows: Iterable[Any]) -> int:
    """Write objects exposing `to_dict()` as CSV. Returns the row count.

    The header is inferred from the first row's keys. If `rows` is empty, an
    empty file is written (no header) - there's nothing to infer a schema
    from, and writing an empty file is less surprising than writing nothing
    at all when a file at that path was expected.
    """
    count = 0
    writer: csv.DictWriter | None = None
    with open(path, "w", encoding="utf-8", newline="") as f:
        for row in rows:
            row_dict = row.to_dict()
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(row_dict.keys()))
                writer.writeheader()
            writer.writerow(row_dict)
            count += 1
    return count
