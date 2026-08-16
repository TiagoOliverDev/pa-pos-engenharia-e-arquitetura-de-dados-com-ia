import pytest

from src.quality.validations import ValidationError
from src.quality.validations import ensure_positive_fields
from src.quality.validations import find_duplicate_rows
from src.quality.validations import validate_records


def test_validate_records_detects_missing_fields() -> None:
    report = validate_records(
        [{"id": 1, "name": "A"}, {"id": 2, "name": ""}],
        required_fields=["id", "name"],
    )

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


@pytest.mark.parametrize("value", [0, -1, None, "1", True])
def test_ensure_positive_fields_rejects_invalid_counters(value) -> None:
    with pytest.raises(ValidationError):
        ensure_positive_fields([{"row_count": value}], ["row_count"])


def test_ensure_positive_fields_accepts_positive_counters() -> None:
    ensure_positive_fields([{"source_row_count": 10, "row_count": 9}], [
        "source_row_count",
        "row_count",
    ])
