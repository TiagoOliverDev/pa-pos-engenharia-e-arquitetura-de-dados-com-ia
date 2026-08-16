"""Cliente de ingestao para a fonte oficial do TSE."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.bronze.storage import S3Storage
from src.config import Settings, get_settings
from src.ingestion.scope import FEFCArchiveSpec, FEFCSourceScope
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/zip, application/octet-stream, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


@dataclass(frozen=True, slots=True)
class IngestionContext:
    """Contexto minimo da ingestao."""

    election_year: int
    source_name: str = "fundo_eleitoral"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DownloadedArchive:
    """Representa um arquivo oficial baixado do TSE."""

    spec: FEFCArchiveSpec
    content: bytes

    @property
    def size_bytes(self) -> int:
        """Nao recebe parametros adicionais e retorna o tamanho do conteudo em bytes."""

        return len(self.content)


class TSEClient:
    """Cliente responsavel por obter os arquivos oficiais do FEFC."""

    def __init__(self, settings: Settings | None = None) -> None:
        """Recebe configuracoes opcionais, inicializa o cliente e nao retorna valor."""

        self._settings = settings or get_settings()
        self._scope = FEFCSourceScope()

    @property
    def settings(self) -> Settings:
        """Nao recebe parametros adicionais e retorna as configuracoes do cliente."""

        return self._settings

    @property
    def scope(self) -> FEFCSourceScope:
        """Nao recebe parametros adicionais e retorna a fonte e os anos do MVP."""

        return self._scope

    def list_election_years(self) -> tuple[int, ...]:
        """Nao recebe parametros e retorna os anos das tres eleicoes do MVP."""

        return self.scope.election_years

    def list_archive_specs(self) -> tuple[FEFCArchiveSpec, ...]:
        """Nao recebe parametros e retorna os metadados dos arquivos oficiais do MVP."""

        return self.scope.archive_specs

    def build_context(self, election_year: int | None = None) -> IngestionContext:
        """Recebe um ano opcional, valida o escopo e retorna o contexto da ingestao."""

        year = election_year or self.settings.default_election_year
        if not self.scope.contains(year):
            raise ValueError(
                f"Ano eleitoral fora do escopo do MVP: {year}. "
                f"Use um dos anos {self.scope.election_years}."
            )
        return IngestionContext(
            election_year=year,
            metadata={
                "app_env": self.settings.app_env,
                "source_name": self.scope.source_name,
                "source_url": self.scope.source_url,
            },
        )

    def download_archive(self, spec: FEFCArchiveSpec, timeout_seconds: int = 120) -> DownloadedArchive:
        """Recebe os metadados e o timeout e retorna o arquivo baixado do TSE."""

        LOGGER.info(
            "Baixando FEFC %s diretamente da fonte oficial do TSE.",
            spec.election_year,
        )
        try:
            request = Request(spec.download_url, headers=REQUEST_HEADERS)
            with urlopen(request, timeout=timeout_seconds) as response:
                return DownloadedArchive(spec=spec, content=response.read())
        except (HTTPError, URLError) as exc:
            raise RuntimeError(
                f"Falha ao baixar o arquivo oficial do TSE para {spec.election_year}."
            ) from exc

    def ingest_to_bronze(
        self,
        storage: S3Storage,
        election_years: tuple[int, ...] | None = None,
    ) -> list[dict[str, Any]]:
        """Recebe o storage e anos opcionais, grava os ZIPs na Bronze e retorna o manifesto."""

        years = election_years or self.list_election_years()
        archives = [self.scope.archive_spec_for_year(year) for year in years]
        storage.ensure_bucket()

        manifest: list[dict[str, Any]] = []
        for spec in archives:
            archive = self.download_archive(spec)
            key = storage.paths.build_raw_key(spec.election_year, spec.filename)
            storage.upload_bytes(key, archive.content, content_type="application/zip")
            manifest.append(
                {
                    "election_year": spec.election_year,
                    "source_name": self.scope.source_name,
                    "dataset_url": spec.dataset_url,
                    "resource_url": spec.resource_url,
                    "download_url": spec.download_url,
                    "package_id": spec.package_id,
                    "resource_id": spec.resource_id,
                    "bucket": storage.bucket_name,
                    "s3_key": key,
                    "filename": spec.filename,
                    "size_bytes": archive.size_bytes,
                }
            )

        manifest_key = storage.paths.build_raw_key(years[0], "_manifest.json")
        storage.upload_text(
            manifest_key,
            content=json.dumps(manifest, ensure_ascii=False, indent=2),
            content_type="application/json; charset=utf-8",
        )
        LOGGER.info("Ingestao FEFC concluida com %s arquivos oficiais.", len(manifest))
        return manifest
