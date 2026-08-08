"""Cliente de ingestao para a futura fonte do TSE."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.config import Settings, get_settings
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

    @property
    def settings(self) -> Settings:
        return self._settings

    def build_context(self, election_year: int | None = None) -> IngestionContext:
        """Build the run context without touching the real source."""

        year = election_year or self.settings.default_election_year
        return IngestionContext(
            election_year=year,
            metadata={"app_env": self.settings.app_env},
        )

    def fetch_records(self, context: IngestionContext) -> list[dict[str, Any]]:
        """Placeholder that keeps the project executable without real extraction."""

        LOGGER.info(
            "Ingestao ainda nao implementada para o ano %s. Sprint 1 fara a extracao real.",
            context.election_year,
        )
        return []

