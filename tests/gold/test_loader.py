from src.gold.loader import PostgresWarehouse


def test_warehouse_builds_url(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "dw")
    monkeypatch.setenv("POSTGRES_USER", "user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "pass")

    warehouse = PostgresWarehouse()

    assert warehouse.settings.postgres_sqlalchemy_url == (
        "postgresql+psycopg2://user:pass@localhost:5432/dw"
    )
