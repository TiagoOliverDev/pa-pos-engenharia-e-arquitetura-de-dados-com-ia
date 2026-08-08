"""Cliente de ingestao para a futura fonte do TSE."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.config import Settings, get_settings
from src.ingestion.scope import FEFCSourceScope
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class IngestionContext:
    """Contexto minimo da ingestao."""

    election_year: int
    source_name: str = "fundo_eleitoral"
    metadata: dict[str, Any] = field(default_factory=dict)


class TSEClient:
    """Placeholder do cliente de origem.

    A Sprint 1 vai definir a fonte real e os detalhes da extracao. Por enquanto
    o cliente apenas organiza o contexto da execucao.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._scope = FEFCSourceScope()

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def scope(self) -> FEFCSourceScope:
        return self._scope

    def list_election_years(self) -> tuple[int, ...]:
        """Return the fixed MVP scope for the last three elections."""

        return self.scope.election_years

    def build_context(self, election_year: int | None = None) -> IngestionContext:
        """Build the run context without touching the real source."""

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

    def fetch_records(self, context: IngestionContext) -> list[dict[str, Any]]:
        """Placeholder that keeps the project executable without real extraction."""

        LOGGER.info(
            "Ingestao ainda nao implementada para o ano %s. Sprint 1 fara a extracao real.",
            context.election_year,
        )
        return []
