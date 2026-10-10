import json
import logging

from pipeline.run_pipeline import run

# Deliberately constructed, not sampled from events.jsonl: a1 is duplicated
# (1 second apart, like the real dataset's retries), a2 is missing optional
# fields but valid, a3 is missing a required field (service), a4 is missing
# a required field (timestamp), and the last line isn't JSON at all.
RAW_LINES = [
    '{"event_id": "a1", "timestamp": "2025-01-12T10:00:00Z", "service": "checkout", "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"}',
    '{"event_id": "a1", "timestamp": "2025-01-12T10:00:01Z", "service": "checkout", "event_type": "request_completed", "latency_ms": 100, "status_code": 200, "user_id": "u1"}',
    '{"event_id": "a2", "timestamp": "2025-01-12T10:00:30Z", "service": "checkout", "status_code": 500}',
    '{"event_id": "a3", "timestamp": "2025-01-12T10:01:00Z", "latency_ms": 50, "status_code": 200}',
    '{"event_id": "a4", "service": "payments", "latency_ms": 20, "status_code": 200}',
    "not valid json at all",
]


def _write_input(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text("\n".join(RAW_LINES) + "\n", encoding="utf-8")
    return path


def test_run_produces_expected_cleaned_rejected_and_metric_outputs(tmp_path, caplog):
    input_path = _write_input(tmp_path)
    output_dir = tmp_path / "output"

    with caplog.at_level(logging.WARNING):
        run(input_path, output_dir)

    # The malformed line is logged and skipped before it ever reaches
    # clean_record - it shouldn't show up in either output file.
    assert "malformed JSON" in caplog.text

    cleaned = [
        json.loads(line)
        for line in (output_dir / "cleaned_events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert {e["event_id"] for e in cleaned} == {"a1", "a2"}

    rejected = [
        json.loads(line)
        for line in (output_dir / "rejected_events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    reject_reasons = {r["record"]["event_id"]: r["reason"] for r in rejected}
    assert reject_reasons == {
        "a1": "duplicate_event_id",
        "a3": "missing_service",
        "a4": "missing_or_invalid_timestamp",
    }

    # Every raw line is accounted for: 2 cleaned + 3 rejected + 1 malformed
    # (never reaches either file) == 6 raw lines.
    assert len(cleaned) + len(rejected) == len(RAW_LINES) - 1

    metrics_lines = (output_dir / "metrics.csv").read_text(encoding="utf-8").splitlines()
    assert metrics_lines[0] == "service,minute,request_count,avg_latency_ms,error_rate"
    # a1 (10:00:00) and a2 (10:00:30) both floor to the same checkout/10:00
    # minute bucket - one row expected, combining both.
    assert len(metrics_lines) == 2
    assert metrics_lines[1] == "checkout,2025-01-12T10:00:00+00:00,2,100.0,0.5"


def test_run_creates_nested_output_dir_if_missing(tmp_path):
    input_path = _write_input(tmp_path)
    output_dir = tmp_path / "nested" / "output"
    assert not output_dir.exists()

    run(input_path, output_dir)

    assert (output_dir / "cleaned_events.jsonl").exists()
    assert (output_dir / "rejected_events.jsonl").exists()
    assert (output_dir / "metrics.csv").exists()
