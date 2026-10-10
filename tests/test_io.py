import json
import logging

from pipeline.io import read_jsonl, write_csv, write_jsonl


class _Row:
    """Minimal stand-in for CleanEvent/MetricRow - anything exposing to_dict()."""

    def __init__(self, **kwargs):
        self._data = kwargs

    def to_dict(self):
        return self._data


def test_read_jsonl_yields_valid_objects_and_skips_malformed_lines(tmp_path, caplog):
    path = tmp_path / "events.jsonl"
    # Line 1: valid. Line 2: blank (silently skipped, not malformed). Line 3:
    # not JSON at all. Line 4: valid again - proves a bad line doesn't abort
    # the rest of the file.
    path.write_text(
        '{"a": 1}\n' "\n" "{not valid json\n" '{"a": 2}\n',
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING):
        results = list(read_jsonl(path))

    assert results == [{"a": 1}, {"a": 2}]
    assert "malformed JSON" in caplog.text
    assert ":3" in caplog.text  # the broken line is line 3


def test_read_jsonl_on_empty_file_yields_nothing(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")
    assert list(read_jsonl(path)) == []


def test_write_jsonl_writes_one_json_object_per_line(tmp_path):
    path = tmp_path / "out.jsonl"
    count = write_jsonl(path, [_Row(a=1), _Row(a=2)])
    assert count == 2
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == [{"a": 1}, {"a": 2}]


def test_write_csv_writes_header_then_rows(tmp_path):
    path = tmp_path / "out.csv"
    count = write_csv(path, [_Row(a=1, b="x"), _Row(a=2, b="y")])
    assert count == 2
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "a,b"
    assert lines[1:] == ["1,x", "2,y"]


def test_write_csv_with_no_rows_writes_empty_file(tmp_path):
    path = tmp_path / "empty.csv"
    count = write_csv(path, [])
    assert count == 0
    assert path.read_text(encoding="utf-8") == ""
