from src.bronze.storage import S3PathBuilder
from src.ingestion.tse_client import TSEClient


class _FakeStorage:
    def __init__(self) -> None:
        self.bucket_name = "fefc-data-lake"
        self.paths = S3PathBuilder(bucket_name=self.bucket_name)
        self.uploaded_bytes: list[tuple[str, bytes, str]] = []
        self.uploaded_text: list[tuple[str, str, str]] = []
        self.bucket_created = False

    def ensure_bucket(self) -> None:
        self.bucket_created = True

    def upload_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> None:
        self.uploaded_bytes.append((key, payload, content_type))

    def upload_text(self, key: str, content: str, content_type: str = "text/plain; charset=utf-8") -> None:
        self.uploaded_text.append((key, content, content_type))


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


def test_download_archive_uses_official_url(monkeypatch) -> None:
    client = TSEClient()
    spec = client.scope.archive_spec_for_year(2024)

    called = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"zip-bytes"

    def fake_urlopen(request, timeout):
        called["url"] = request.full_url
        called["headers"] = dict(request.headers)
        called["timeout"] = timeout
        return _Response()

    monkeypatch.setattr("src.ingestion.tse_client.urlopen", fake_urlopen)

    archive = client.download_archive(spec)

    assert archive.content == b"zip-bytes"
    assert called["url"] == "https://cdn.tse.jus.br/estatistica/sead/odsele/fefc_fp/fefc_fp_2024.zip"
    assert called["timeout"] == 120


def test_ingest_to_bronze_uploads_manifest_and_archives(monkeypatch) -> None:
    client = TSEClient()
    storage = _FakeStorage()

    class _Response:
        def __init__(self, content: bytes) -> None:
            self._content = content

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return self._content

    def fake_urlopen(request, timeout):
        year = int(request.full_url.rsplit("_", 1)[-1].replace(".zip", ""))
        return _Response(f"zip-{year}".encode("utf-8"))

    monkeypatch.setattr("src.ingestion.tse_client.urlopen", fake_urlopen)

    manifest = client.ingest_to_bronze(storage)

    assert storage.bucket_created is True
    assert len(manifest) == 3
    assert len(storage.uploaded_bytes) == 3
    assert len(storage.uploaded_text) == 1
    assert storage.uploaded_bytes[0][0] == "bronze/fundo_eleitoral/ano_eleicao=2020/fefc_fp_2020.zip"
    assert storage.uploaded_text[0][0] == "bronze/fundo_eleitoral/ano_eleicao=2020/_manifest.json"
    assert manifest[0]["download_url"].endswith("fefc_fp_2020.zip")
