"""Pipeline entry point: read -> clean -> dedupe -> write cleaned events +
rejected events + aggregated metrics.

Usage:
    python -m pipeline.run_pipeline events.jsonl --output-dir output
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .aggregate import aggregate_events
from .clean import clean_record
from .dedupe import dedupe_events
from .io import read_jsonl, write_csv, write_jsonl
from .models import CleanEvent, RejectedEvent

logger = logging.getLogger(__name__)


def run(input_path: str | Path, output_dir: str | Path) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_records = read_jsonl(input_path)
    cleaned_or_rejected = (clean_record(r) for r in raw_records)
    deduped = dedupe_events(cleaned_or_rejected)

    # The one place this pipeline isn't fully streaming: clean_events needs
    # to feed two independent sinks (the cleaned-events file, and
    # aggregation), but a generator can only be consumed once. Materializing
    # it as a list is fine at this dataset's size. At 10GB scale, the fix
    # isn't to avoid branching - it's to drive both sinks from one pass
    # (write each record to disk *and* update an in-memory aggregation
    # accumulator in the same loop iteration), which never holds more than
    # one record plus the bounded aggregation state in memory. See README.
    clean_events: list[CleanEvent] = []
    rejected_events: list[RejectedEvent] = []
    for record in deduped:
        if isinstance(record, RejectedEvent):
            rejected_events.append(record)
        else:
            clean_events.append(record)

    clean_count = write_jsonl(output_dir / "cleaned_events.jsonl", clean_events)
    reject_count = write_jsonl(output_dir / "rejected_events.jsonl", rejected_events)
    metric_rows = list(aggregate_events(clean_events))
    metric_count = write_csv(output_dir / "metrics.csv", metric_rows)

    logger.info(
        "Pipeline complete: %d cleaned, %d rejected, %d metric rows -> %s",
        clean_count,
        reject_count,
        metric_count,
        output_dir,
    )

    if rejected_events:
        reasons: dict[str, int] = {}
        for rejected in rejected_events:
            reasons[rejected.reason] = reasons.get(rejected.reason, 0) + 1
        for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
            logger.info("  rejected (%s): %d", reason, count)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Clean events and compute metrics.")
    parser.add_argument("input", help="Path to the input events.jsonl file")
    parser.add_argument(
        "--output-dir", default="output", help="Directory to write outputs to"
    )
    args = parser.parse_args()
    run(args.input, args.output_dir)


if __name__ == "__main__":
    main()
