from src.silver.transformations import normalize_records


def test_normalize_records_returns_copies() -> None:
    records = [{"a": 1}]

    normalized = normalize_records(records)

    assert normalized == records
    assert normalized is not records

