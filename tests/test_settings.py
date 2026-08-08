from src.config.settings import get_settings


def test_settings_reads_environment(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localstack:4566")

    settings = get_settings()

    assert settings.postgres_host == "db"
    assert settings.postgres_port == 5433
    assert settings.use_localstack is True

