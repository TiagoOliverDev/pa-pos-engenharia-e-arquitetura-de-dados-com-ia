from src.ingestion.scope import FEFCSourceScope


def test_fefc_scope_includes_last_three_elections() -> None:
    scope = FEFCSourceScope()

    assert scope.election_years == (2020, 2022, 2024)
    assert scope.contains(2020) is True
    assert scope.contains(2022) is True
    assert scope.contains(2024) is True
    assert scope.contains(2018) is False

