from __future__ import annotations

import json
from decimal import Decimal
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from src.bronze.storage import S3PathBuilder
from src.silver.transformations import normalize_records
from src.silver.transformations import transform_bronze_manifest


FEFC_GENERO_HEADER = (
    "AA_ELEICAO;SG_PARTIDO;NR_PARTIDO;DS_GENERO;QT_CANDIDATO;"
    "VR_PARTIDO_FEFC;PE_CAND_PARTIDO_GENERO;VR_REPASSE_MINIMO_COTA;"
    "VR_TOTAL_RECEBIDO_FEFC;PE_VALOR_FEFC_GENERO;ST_RENUNCIA;"
    "DT_GERACAO;HH_GERACAO"
)
FP_GENERO_HEADER = (
    "AA_ELEICAO;SG_PARTIDO;NR_PARTIDO;DS_ESFERA_PARTIDARIA;SG_UF;"
    "SG_UE;DS_MUNICIPIO;DS_GENERO;QT_CANDIDATO;VR_DESPESA_DIRETORIO_FP;"
    "PE_CAND_PARTIDO_GENERO;VR_DESPESA_MINIMO_COTA;VR_TOTAL_RECEBIDO_FP;"
    "PE_VALOR_FP_GENERO;DT_GERACAO;HH_GERACAO"
)


def _with_cor_raca(header: str) -> str:
    columns = header.split(";")
    columns.insert(columns.index("DS_GENERO") + 1, "DS_COR_RACA")
    return ";".join(columns)


def _build_archive(
    year: int,
    *,
    fp_percentage: str = "10,00",
    empty_dataset: str | None = None,
    duplicate_dataset: str | None = None,
) -> bytes:
    election_scope = "Municipal" if year in (2020, 2024) else "Estadual"
    uf = "SP"
    ue = "71072" if year in (2020, 2024) else ""
    city = "SAO PAULO" if year in (2020, 2024) else ""
    fefc_row = (
        f"{year}; pdt ;12; Feminino ;10;1.234,56;50,00;617,28;700,00;56,70;"
        "0;16/08/2026;13:21"
    )
    fp_row = (
        f"{year};pdt;12;{election_scope};{uf};{ue};{city};Feminino;10;"
        f"1.000,00;50,00;500,00;600,00;{fp_percentage};16/08/2026;13:21"
    )

    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        members = {
            f"fefc_genero_{year}.csv": FEFC_GENERO_HEADER + "\n" + fefc_row,
            f"fefc_cor_raca_{year}.csv": (
                _with_cor_raca(FEFC_GENERO_HEADER)
                + "\n"
                + fefc_row.replace("; Feminino ;", "; Feminino ;NEGRA;")
            ),
            f"fp_genero_{year}.csv": FP_GENERO_HEADER + "\n" + fp_row,
            f"fp_cor_raca_{year}.csv": (
                _with_cor_raca(FP_GENERO_HEADER)
                + "\n"
                + fp_row.replace(";Feminino;", ";Feminino;NEGRA;")
            ),
        }
        for member_name, content in members.items():
            dataset_name = member_name.removesuffix(f"_{year}.csv")
            archive.writestr(
                member_name,
                content.splitlines()[0] + "\n" if dataset_name == empty_dataset else content,
            )
        if duplicate_dataset:
            member_name = f"{duplicate_dataset}_{year}.csv"
            archive.writestr(f"duplicado/{member_name}", members[member_name])
    return buffer.getvalue()


class _FakeStorage:
    def __init__(self, archives: dict[str, bytes]) -> None:
        self.bucket_name = "fefc-data-lake"
        self.paths = S3PathBuilder(bucket_name=self.bucket_name)
        self._archives = archives
        self.uploaded_bytes: list[tuple[str, bytes, str]] = []
        self.uploaded_text: list[tuple[str, str, str]] = []

    def ensure_bucket(self) -> None:
        return None

    def download_bytes(self, key: str) -> bytes:
        return self._archives[key]

    def upload_bytes(
        self, key: str, payload: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        self.uploaded_bytes.append((key, payload, content_type))
        self._archives[key] = payload

    def upload_text(
        self, key: str, content: str, content_type: str = "text/plain; charset=utf-8"
    ) -> None:
        self.uploaded_text.append((key, content, content_type))
        self._archives[key] = content.encode("utf-8")


def _transform_year(year: int, *, fp_percentage: str = "10,00") -> tuple[dict, _FakeStorage]:
    archive_key = f"bronze/fundo_eleitoral/ano_eleicao={year}/raw/fefc_fp_{year}.zip"
    storage = _FakeStorage({archive_key: _build_archive(year, fp_percentage=fp_percentage)})
    result = transform_bronze_manifest(
        storage,
        [{"election_year": year, "s3_key": archive_key}],
        collect_records=True,
    )
    return result, storage


def test_normalize_records_standardizes_keys_and_values() -> None:
    records = [{"Nome Completo": "  Maria  ", "Campo vazio": "-"}]

    assert normalize_records(records) == [
        {"nome_completo": "Maria", "campo_vazio": None}
    ]


def test_treatment_is_isolated_by_election_year() -> None:
    for year, election_type in ((2020, "municipal"), (2022, "geral"), (2024, "municipal")):
        result, storage = _transform_year(year)

        assert result["silver_record_count"] == 4
        assert len(result["silver_artifacts"]) == 4
        assert {artifact["dataset_name"] for artifact in result["silver_artifacts"]} == {
            "fefc_cor_raca",
            "fefc_genero",
            "fp_cor_raca",
            "fp_genero",
        }
        assert {artifact["election_type"] for artifact in result["silver_artifacts"]} == {
            election_type
        }
        assert len(storage.uploaded_bytes) == 4
        assert storage.uploaded_text[0][0] == (
            f"silver/fundo_eleitoral/ano_eleicao={year}/tratado/_manifest.json"
        )


def test_values_are_typed_standardized_and_traceable() -> None:
    result, _ = _transform_year(2024)
    record = next(
        row for row in result["silver_records"] if row["source_member"] == "fefc_genero_2024.csv"
    )

    assert record["source_row_number"] == 2
    assert record["ano_eleicao"] == 2024
    assert record["tipo_eleicao"] == "municipal"
    assert record["tipo_fundo"] == "fefc"
    assert record["dimensao_agregacao"] == "genero"
    assert record["sigla_partido"] == "PDT"
    assert record["genero"] == "FEMININO"
    assert record["numero_partido"] == 12
    assert record["valor_partido_fefc"] == Decimal("1234.56")
    assert record["data_geracao"] == "2026-08-16"
    assert record["hora_geracao"] == "13:21:00"
    assert record["data_hora_geracao"] == "2026-08-16T13:21:00"


def test_expected_missing_geography_is_preserved_for_2022() -> None:
    result, _ = _transform_year(2022)
    record = next(
        row for row in result["silver_records"] if row["source_member"] == "fp_genero_2022.csv"
    )

    assert record["tipo_eleicao"] == "geral"
    assert record["sigla_ue"] is None
    assert record["municipio"] is None


def test_invalid_numeric_placeholder_becomes_null_and_is_reported() -> None:
    result, storage = _transform_year(2020, fp_percentage="#########")
    record = next(
        row for row in result["silver_records"] if row["source_member"] == "fp_genero_2020.csv"
    )
    artifact = next(
        item for item in result["silver_artifacts"] if item["dataset_name"] == "fp_genero"
    )
    manifest = json.loads(storage.uploaded_text[0][1])

    assert record["percentual_valor_fp_genero"] is None
    assert artifact["invalid_value_counts"] == {"percentual_valor_fp_genero": 1}
    assert manifest["row_count"] == 4


def test_financial_values_keep_decimal_precision() -> None:
    result, storage = _transform_year(2024)
    record = next(
        row for row in result["silver_records"] if row["source_member"] == "fefc_genero_2024.csv"
    )
    output = next(
        payload.decode("utf-8")
        for key, payload, _ in storage.uploaded_bytes
        if key.endswith("fefc_genero_2024_tratado.csv")
    )

    assert record["valor_partido_fefc"] == Decimal("1234.56")
    assert ";1234.56;" in output


def test_duplicate_dataset_is_rejected_before_upload() -> None:
    year = 2024
    archive_key = f"bronze/fundo_eleitoral/ano_eleicao={year}/raw/fefc_fp_{year}.zip"
    storage = _FakeStorage(
        {archive_key: _build_archive(year, duplicate_dataset="fp_genero")}
    )

    with pytest.raises(ValueError, match="CSVs duplicados"):
        transform_bronze_manifest(
            storage,
            [{"election_year": year, "s3_key": archive_key}],
        )

    assert storage.uploaded_bytes == []
    assert storage.uploaded_text == []


def test_empty_dataset_is_rejected_before_upload() -> None:
    year = 2024
    archive_key = f"bronze/fundo_eleitoral/ano_eleicao={year}/raw/fefc_fp_{year}.zip"
    storage = _FakeStorage(
        {archive_key: _build_archive(year, empty_dataset="fefc_genero")}
    )

    with pytest.raises(ValueError, match="nao possui registros"):
        transform_bronze_manifest(
            storage,
            [{"election_year": year, "s3_key": archive_key}],
        )

    assert storage.uploaded_bytes == []
    assert storage.uploaded_text == []


def test_duplicate_election_year_is_rejected() -> None:
    year = 2024
    archive_key = f"bronze/fundo_eleitoral/ano_eleicao={year}/raw/fefc_fp_{year}.zip"
    storage = _FakeStorage({archive_key: _build_archive(year)})
    manifest_item = {"election_year": year, "s3_key": archive_key}

    with pytest.raises(ValueError, match="Eleicoes duplicadas"):
        transform_bronze_manifest(storage, [manifest_item, manifest_item])

    assert storage.uploaded_bytes == []
