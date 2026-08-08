"""Definicao de fonte e escopo dos dados do FEFC."""

from __future__ import annotations

from dataclasses import dataclass

FEFC_ELECTION_YEARS: tuple[int, ...] = (2020, 2022, 2024)
FEFC_SOURCE_NAME = "fundo_eleitoral"
FEFC_SOURCE_URL = "https://dadosabertos.tse.jus.br/dataset/?q=fundo+eleitoral"


@dataclass(frozen=True, slots=True)
class FEFCSourceScope:
    """Escopo minimo do MVP para as tres ultimas eleicoes."""

    source_name: str = FEFC_SOURCE_NAME
    source_url: str = FEFC_SOURCE_URL
    election_years: tuple[int, ...] = FEFC_ELECTION_YEARS

    def contains(self, election_year: int) -> bool:
        return election_year in self.election_years

