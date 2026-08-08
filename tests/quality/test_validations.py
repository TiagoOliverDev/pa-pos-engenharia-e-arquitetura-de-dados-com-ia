import pytest

from src.quality.validations import ValidationError, find_duplicate_rows, validate_records


def test_validate_records_detects_missing_fields() -> None:
    report = validate_records([{"id": 1, "name": "A"}, {"id": 2, "name": ""}], required_fields=["id", "name"])

    assert report.valid is False
    assert "name" in report.missing_fields


def test_validate_records_allows_empty_when_requested() -> None:
    report = validate_records([], allow_empty=True)

    assert report.valid is True
    assert report.record_count == 0


def test_find_duplicate_rows() -> None:
    records = [{"id": 1}, {"id": 1}, {"id": 2}]

    duplicates = find_duplicate_rows(records, ["id"])

    assert len(duplicates) == 1


def test_ensure_not_empty_raises() -> None:
    from src.quality.validations import ensure_not_empty

    with pytest.raises(ValidationError):
        ensure_not_empty([])

