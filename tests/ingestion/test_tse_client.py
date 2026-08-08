from src.ingestion.tse_client import TSEClient


def test_build_context_uses_default_year(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_ELECTION_YEAR", "2022")
    client = TSEClient()

    context = client.build_context()

    assert context.election_year == 2022
    assert context.source_name == "fundo_eleitoral"
    assert context.metadata["source_name"] == "fundo_eleitoral"
    assert context.metadata["source_url"].endswith("fundo+eleitoral")


def test_client_lists_last_three_elections() -> None:
    client = TSEClient()

    assert client.list_election_years() == (2020, 2022, 2024)


def test_build_context_rejects_out_of_scope_year() -> None:
    client = TSEClient()

    try:
        client.build_context(2018)
    except ValueError as exc:
        assert "fora do escopo" in str(exc)
    else:
        raise AssertionError("Era esperado ValueError para ano fora do escopo")
