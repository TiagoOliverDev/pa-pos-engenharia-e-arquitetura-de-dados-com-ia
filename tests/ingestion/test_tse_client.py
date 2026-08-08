from src.ingestion.tse_client import TSEClient


def test_build_context_uses_default_year(monkeypatch) -> None:
    monkeypatch.setenv("DEFAULT_ELECTION_YEAR", "2022")
    client = TSEClient()

    context = client.build_context()

    assert context.election_year == 2022
    assert context.source_name == "fundo_eleitoral"

