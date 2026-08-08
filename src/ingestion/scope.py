"""Definicao de fonte e escopo dos dados do FEFC."""

from __future__ import annotations

from dataclasses import dataclass

FEFC_ELECTION_YEARS: tuple[int, ...] = (2020, 2022, 2024)
FEFC_SOURCE_NAME = "fundo_eleitoral"
FEFC_SOURCE_URL = "https://dadosabertos.tse.jus.br/dataset/?q=fundo+eleitoral"


@dataclass(frozen=True, slots=True)
class FEFCArchiveSpec:
    """Metadata for one official FEFC archive published by the TSE."""

    election_year: int
    dataset_url: str
    resource_url: str
    download_url: str
    package_id: str
    resource_id: str
    filename: str


FEFC_ARCHIVE_SPECS: tuple[FEFCArchiveSpec, ...] = (
    FEFCArchiveSpec(
        election_year=2020,
        dataset_url="https://dadosabertos.tse.jus.br/dataset/prestacao-de-contas-eleitorais-2020",
        resource_url=(
            "https://dadosabertos.tse.jus.br/dataset/prestacao-de-contas-eleitorais-2020/"
            "resource/9925398b-e067-47f8-88b6-b16e4ec59bed"
        ),
        download_url="https://cdn.tse.jus.br/estatistica/sead/odsele/fefc_fp/fefc_fp_2020.zip",
        package_id="75841dbc-7279-4550-9569-6d7c12164a7b",
        resource_id="9925398b-e067-47f8-88b6-b16e4ec59bed",
        filename="fefc_fp_2020.zip",
    ),
    FEFCArchiveSpec(
        election_year=2022,
        dataset_url=(
            "https://dadosabertos.tse.jus.br/dataset/"
            "dadosabertos-tse-jus-br-dataset-prestacao-de-contas-eleitorais-2022"
        ),
        resource_url=(
            "https://dadosabertos.tse.jus.br/dataset/"
            "dadosabertos-tse-jus-br-dataset-prestacao-de-contas-eleitorais-2022/"
            "resource/ef15e41c-a39a-4dba-bb12-864713a33b7b"
        ),
        download_url="https://cdn.tse.jus.br/estatistica/sead/odsele/fefc_fp/fefc_fp_2022.zip",
        package_id="50805cdc-a983-49a1-b76e-2a89e0e61580",
        resource_id="ef15e41c-a39a-4dba-bb12-864713a33b7b",
        filename="fefc_fp_2022.zip",
    ),
    FEFCArchiveSpec(
        election_year=2024,
        dataset_url="https://dadosabertos.tse.jus.br/dataset/prestacao-de-contas-eleitorais-2024",
        resource_url=(
            "https://dadosabertos.tse.jus.br/dataset/prestacao-de-contas-eleitorais-2024/"
            "resource/0ab5db94-6aeb-4d1b-b214-e6f9cb5ca712"
        ),
        download_url="https://cdn.tse.jus.br/estatistica/sead/odsele/fefc_fp/fefc_fp_2024.zip",
        package_id="380e5337-a918-4013-a987-b92920527087",
        resource_id="0ab5db94-6aeb-4d1b-b214-e6f9cb5ca712",
        filename="fefc_fp_2024.zip",
    ),
)


@dataclass(frozen=True, slots=True)
class FEFCSourceScope:
    """Escopo minimo do MVP para as tres ultimas eleicoes."""

    source_name: str = FEFC_SOURCE_NAME
    source_url: str = FEFC_SOURCE_URL
    election_years: tuple[int, ...] = FEFC_ELECTION_YEARS
    archive_specs: tuple[FEFCArchiveSpec, ...] = FEFC_ARCHIVE_SPECS

    def contains(self, election_year: int) -> bool:
        return election_year in self.election_years

    def archive_spec_for_year(self, election_year: int) -> FEFCArchiveSpec:
        for spec in self.archive_specs:
            if spec.election_year == election_year:
                return spec
        raise ValueError(f"Ano eleitoral fora do escopo do MVP: {election_year}")

