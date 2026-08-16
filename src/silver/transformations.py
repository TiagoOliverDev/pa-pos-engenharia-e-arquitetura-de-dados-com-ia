"""Limpeza e padronizacao da camada Silver dos dados FEFC."""

from __future__ import annotations

import csv
import json
import re
import unicodedata
import zipfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.bronze.storage import S3Storage
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)

_EMPTY_TOKENS = {"", "-", "na", "n/a", "null", "none", "nan"}
_INVALID_NUMERIC_TOKENS = {"#########", "#div/0!", "#n/a", "#valor!"}
_MEMBER_PATTERN = re.compile(
    r"^(?P<fund_type>fefc|fp)_(?P<dimension>genero|cor_raca)_(?P<year>\d{4})\.csv$",
    flags=re.IGNORECASE,
)

_COLUMN_ALIASES = {
    "AA_ELEICAO": "ano_eleicao",
    "SG_PARTIDO": "sigla_partido",
    "NR_PARTIDO": "numero_partido",
    "DS_ESFERA_PARTIDARIA": "esfera_partidaria",
    "SG_UF": "sigla_uf",
    "SG_UE": "sigla_ue",
    "DS_MUNICIPIO": "municipio",
    "DS_GENERO": "genero",
    "DS_COR_RACA": "cor_raca",
    "QT_CANDIDATO": "quantidade_candidatos",
    "VR_PARTIDO_FEFC": "valor_partido_fefc",
    "PE_CAND_PARTIDO_GENERO": "percentual_candidatos_partido_genero",
    "VR_REPASSE_MINIMO_COTA": "valor_repasse_minimo_cota",
    "VR_TOTAL_RECEBIDO_FEFC": "valor_total_recebido_fefc",
    "PE_VALOR_FEFC_GENERO": "percentual_valor_fefc_genero",
    "ST_RENUNCIA": "status_renuncia",
    "VR_DESPESA_DIRETORIO_FP": "valor_despesa_diretorio_fp",
    "VR_DESPESA_MINIMO_COTA": "valor_despesa_minimo_cota",
    "VR_TOTAL_RECEBIDO_FP": "valor_total_recebido_fp",
    "PE_VALOR_FP_GENERO": "percentual_valor_fp_genero",
    "DT_GERACAO": "data_geracao",
    "HH_GERACAO": "hora_geracao",
}

_FEFC_GENERO_COLUMNS = (
    "AA_ELEICAO",
    "SG_PARTIDO",
    "NR_PARTIDO",
    "DS_GENERO",
    "QT_CANDIDATO",
    "VR_PARTIDO_FEFC",
    "PE_CAND_PARTIDO_GENERO",
    "VR_REPASSE_MINIMO_COTA",
    "VR_TOTAL_RECEBIDO_FEFC",
    "PE_VALOR_FEFC_GENERO",
    "ST_RENUNCIA",
    "DT_GERACAO",
    "HH_GERACAO",
)
_FP_GENERO_COLUMNS = (
    "AA_ELEICAO",
    "SG_PARTIDO",
    "NR_PARTIDO",
    "DS_ESFERA_PARTIDARIA",
    "SG_UF",
    "SG_UE",
    "DS_MUNICIPIO",
    "DS_GENERO",
    "QT_CANDIDATO",
    "VR_DESPESA_DIRETORIO_FP",
    "PE_CAND_PARTIDO_GENERO",
    "VR_DESPESA_MINIMO_COTA",
    "VR_TOTAL_RECEBIDO_FP",
    "PE_VALOR_FP_GENERO",
    "DT_GERACAO",
    "HH_GERACAO",
)
_SOURCE_SCHEMAS = {
    "fefc_genero": _FEFC_GENERO_COLUMNS,
    "fefc_cor_raca": _FEFC_GENERO_COLUMNS[:4]
    + ("DS_COR_RACA",)
    + _FEFC_GENERO_COLUMNS[4:],
    "fp_genero": _FP_GENERO_COLUMNS,
    "fp_cor_raca": _FP_GENERO_COLUMNS[:8]
    + ("DS_COR_RACA",)
    + _FP_GENERO_COLUMNS[8:],
}

_INTEGER_COLUMNS = {
    "ano_eleicao",
    "numero_partido",
    "quantidade_candidatos",
    "status_renuncia",
}
_DECIMAL_COLUMNS = {
    "valor_partido_fefc",
    "percentual_candidatos_partido_genero",
    "valor_repasse_minimo_cota",
    "valor_total_recebido_fefc",
    "percentual_valor_fefc_genero",
    "valor_despesa_diretorio_fp",
    "valor_despesa_minimo_cota",
    "valor_total_recebido_fp",
    "percentual_valor_fp_genero",
}
_UPPERCASE_COLUMNS = {
    "sigla_partido",
    "sigla_uf",
    "sigla_ue",
    "esfera_partidaria",
    "municipio",
    "genero",
    "cor_raca",
}


@dataclass(frozen=True, slots=True)
class ElectionTreatment:
    """Regras explicitas de tratamento para uma eleicao."""

    election_year: int
    election_type: str
    expected_datasets: tuple[str, ...] = (
        "fefc_cor_raca",
        "fefc_genero",
        "fp_cor_raca",
        "fp_genero",
    )


_ELECTION_TREATMENTS = {
    2020: ElectionTreatment(2020, "municipal"),
    2022: ElectionTreatment(2022, "geral"),
    2024: ElectionTreatment(2024, "municipal"),
}


@dataclass(frozen=True, slots=True)
class MemberIdentity:
    dataset_name: str
    fund_type: str
    aggregation_dimension: str


@dataclass(frozen=True, slots=True)
class SilverArtifact:
    """Metadados e indicadores de qualidade de um CSV Silver."""

    election_year: int
    election_type: str
    dataset_name: str
    fund_type: str
    aggregation_dimension: str
    source_archive_key: str
    source_member: str
    source_encoding: str
    output_key: str
    source_row_count: int
    row_count: int
    invalid_value_counts: dict[str, int]
    columns: tuple[str, ...]


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _normalize_column_name(value: str) -> str:
    slug = _strip_accents(str(value)).lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return re.sub(r"_+", "_", slug).strip("_") or "coluna"


def _clean_scalar(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    cleaned = re.sub(r"\s+", " ", value).strip()
    if cleaned.casefold() in _EMPTY_TOKENS:
        return None
    return cleaned


def _parse_integer(value: Any) -> tuple[int | None, bool]:
    cleaned = _clean_scalar(value)
    if cleaned is None:
        return None, False
    text = str(cleaned)
    if not re.fullmatch(r"-?\d+", text):
        return None, True
    return int(text), False


def _parse_brazilian_decimal(value: Any) -> tuple[Decimal | None, bool]:
    cleaned = _clean_scalar(value)
    if cleaned is None:
        return None, False
    text = str(cleaned)
    if text.casefold() in _INVALID_NUMERIC_TOKENS:
        return None, True
    normalized = text.replace(".", "").replace(",", ".")
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
        return None, True
    try:
        return Decimal(normalized), False
    except InvalidOperation:
        return None, True


def _parse_date(value: Any) -> tuple[str | None, bool]:
    cleaned = _clean_scalar(value)
    if cleaned is None:
        return None, False
    try:
        return datetime.strptime(str(cleaned), "%d/%m/%Y").date().isoformat(), False
    except ValueError:
        return None, True


def _parse_time(value: Any) -> tuple[str | None, bool]:
    cleaned = _clean_scalar(value)
    if cleaned is None:
        return None, False
    for date_format in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(str(cleaned), date_format).time().isoformat(), False
        except ValueError:
            continue
    return None, True


def _classify_member(member_name: str, election_year: int) -> MemberIdentity:
    filename = Path(member_name).name
    match = _MEMBER_PATTERN.fullmatch(filename)
    if match is None:
        raise ValueError(f"Nome de CSV FEFC nao reconhecido: {member_name}")
    member_year = int(match.group("year"))
    if member_year != election_year:
        raise ValueError(
            f"Ano {member_year} do arquivo {member_name} difere da particao {election_year}."
        )
    fund_type = match.group("fund_type").lower()
    dimension = match.group("dimension").lower()
    return MemberIdentity(
        dataset_name=f"{fund_type}_{dimension}",
        fund_type=fund_type,
        aggregation_dimension=dimension,
    )


def _read_csv_from_member(
    zip_file: zipfile.ZipFile, member_name: str
) -> tuple[list[str], list[list[str]], str]:
    member_bytes = zip_file.read(member_name)
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            content = member_bytes.decode(encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
        reader = csv.reader(StringIO(content), delimiter=";")
        headers = next(reader, [])
        return headers, list(reader), encoding
    raise RuntimeError(f"Falha ao ler o arquivo CSV {member_name}.") from last_error


def _convert_value(column: str, value: Any) -> tuple[Any, bool]:
    if column in _INTEGER_COLUMNS:
        return _parse_integer(value)
    if column in _DECIMAL_COLUMNS:
        return _parse_brazilian_decimal(value)
    if column == "data_geracao":
        return _parse_date(value)
    if column == "hora_geracao":
        return _parse_time(value)

    cleaned = _clean_scalar(value)
    if cleaned is not None and column in _UPPERCASE_COLUMNS:
        cleaned = str(cleaned).upper()
    return cleaned, False


def _clean_rows(
    headers: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    treatment: ElectionTreatment,
    identity: MemberIdentity,
    source_archive: str,
    source_member: str,
) -> tuple[list[dict[str, Any]], list[str], dict[str, int]]:
    expected_headers = _SOURCE_SCHEMAS[identity.dataset_name]
    if tuple(headers) != expected_headers:
        raise ValueError(
            f"Schema inesperado em {source_member}. "
            f"Esperado={expected_headers}; recebido={tuple(headers)}"
        )

    canonical_headers = [_COLUMN_ALIASES[header] for header in headers]
    invalid_counts: Counter[str] = Counter()
    cleaned_rows: list[dict[str, Any]] = []

    for source_row_number, row_values in enumerate(rows, start=2):
        if len(row_values) != len(headers):
            raise ValueError(
                f"Linha {source_row_number} de {source_member} possui "
                f"{len(row_values)} campos; esperado={len(headers)}."
            )
        if all(_clean_scalar(value) is None for value in row_values):
            continue

        row: dict[str, Any] = {
            "silver_layer": "silver",
            "source_name": "fundo_eleitoral",
            "source_row_number": source_row_number,
            "source_archive": source_archive,
            "source_member": Path(source_member).name,
            "tipo_eleicao": treatment.election_type,
            "tipo_fundo": identity.fund_type,
            "dimensao_agregacao": identity.aggregation_dimension,
        }
        for column, value in zip(canonical_headers, row_values):
            converted, invalid = _convert_value(column, value)
            row[column] = converted
            if invalid:
                invalid_counts[column] += 1

        if row["ano_eleicao"] != treatment.election_year:
            raise ValueError(
                f"Ano invalido na linha {source_row_number} de {source_member}: "
                f"{row['ano_eleicao']}"
            )
        data_geracao = row.get("data_geracao")
        hora_geracao = row.get("hora_geracao")
        row["data_hora_geracao"] = (
            f"{data_geracao}T{hora_geracao}" if data_geracao and hora_geracao else None
        )
        cleaned_rows.append(row)

    metadata_columns = [
        "silver_layer",
        "source_name",
        "source_row_number",
        "source_archive",
        "source_member",
        "ano_eleicao",
        "tipo_eleicao",
        "tipo_fundo",
        "dimensao_agregacao",
    ]
    data_columns = [column for column in canonical_headers if column != "ano_eleicao"]
    columns = metadata_columns + data_columns + ["data_hora_geracao"]
    return cleaned_rows, columns, dict(invalid_counts)


def _rows_to_csv(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> str:
    buffer = StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(columns), delimiter=";")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column) for column in columns})
    return buffer.getvalue()


def normalize_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Normaliza chaves e valores de registros genericos."""

    return [
        {_normalize_column_name(key): _clean_scalar(value) for key, value in record.items()}
        for record in records
    ]


def _validate_archive_members(
    treatment: ElectionTreatment, identities: Sequence[MemberIdentity]
) -> None:
    dataset_counts = Counter(identity.dataset_name for identity in identities)
    duplicated = sorted(name for name, count in dataset_counts.items() if count > 1)
    if duplicated:
        raise ValueError(
            f"CSVs duplicados no ZIP de {treatment.election_year}: {duplicated}"
        )

    received = set(dataset_counts)
    expected = set(treatment.expected_datasets)
    if received != expected:
        raise ValueError(
            f"Conteudo inesperado no ZIP de {treatment.election_year}. "
            f"Ausentes={sorted(expected - received)}; extras={sorted(received - expected)}"
        )


def transform_bronze_manifest(
    storage: S3Storage,
    manifest: Sequence[Mapping[str, Any]],
    *,
    collect_records: bool = False,
) -> dict[str, Any]:
    """Transforma os ZIPs Bronze e grava os CSVs padronizados na Silver."""

    storage.ensure_bucket()
    silver_records: list[dict[str, Any]] = []
    silver_artifacts: list[dict[str, Any]] = []
    total_records = 0

    manifest_years = [int(item["election_year"]) for item in manifest]
    duplicated_years = sorted(
        year for year, count in Counter(manifest_years).items() if count > 1
    )
    if duplicated_years:
        raise ValueError(f"Eleicoes duplicadas no manifesto Bronze: {duplicated_years}")

    for item in manifest:
        election_year = int(item["election_year"])
        try:
            treatment = _ELECTION_TREATMENTS[election_year]
        except KeyError as exc:
            raise ValueError(f"Eleicao {election_year} sem contrato de tratamento Silver.") from exc

        archive_key = str(item["s3_key"])
        archive_bytes = storage.download_bytes(archive_key)
        year_artifacts: list[SilverArtifact] = []
        prepared_uploads: list[tuple[str, bytes, SilverArtifact]] = []

        with zipfile.ZipFile(BytesIO(archive_bytes)) as zip_file:
            csv_members = sorted(
                member
                for member in zip_file.namelist()
                if member.lower().endswith(".csv") and not member.endswith("/")
            )
            identities = [_classify_member(member, election_year) for member in csv_members]
            _validate_archive_members(treatment, identities)

            for member_name, identity in zip(csv_members, identities):
                headers, raw_rows, encoding = _read_csv_from_member(zip_file, member_name)
                rows, columns, invalid_counts = _clean_rows(
                    headers,
                    raw_rows,
                    treatment=treatment,
                    identity=identity,
                    source_archive=archive_key,
                    source_member=member_name,
                )
                if not rows:
                    raise ValueError(
                        f"CSV {member_name} da eleicao {election_year} nao possui registros."
                    )
                output_name = f"{Path(member_name).stem}_tratado.csv"
                output_key = storage.paths.build_treated_silver_key(
                    election_year, output_name
                )
                artifact = SilverArtifact(
                    election_year=election_year,
                    election_type=treatment.election_type,
                    dataset_name=identity.dataset_name,
                    fund_type=identity.fund_type,
                    aggregation_dimension=identity.aggregation_dimension,
                    source_archive_key=archive_key,
                    source_member=Path(member_name).name,
                    source_encoding=encoding,
                    output_key=output_key,
                    source_row_count=len(raw_rows),
                    row_count=len(rows),
                    invalid_value_counts=invalid_counts,
                    columns=tuple(columns),
                )
                year_artifacts.append(artifact)
                prepared_uploads.append(
                    (output_key, _rows_to_csv(rows, columns).encode("utf-8"), artifact)
                )
                if collect_records:
                    silver_records.extend(rows)

        # Evita publicar um ano parcialmente quando algum CSV possui erro de dados.
        for output_key, output_payload, artifact in prepared_uploads:
            storage.upload_bytes(
                output_key,
                output_payload,
                content_type="text/csv; charset=utf-8",
            )
            silver_artifacts.append(asdict(artifact))
            total_records += artifact.row_count

        manifest_key = f"{storage.paths.silver_treated_prefix(election_year)}_manifest.json"
        storage.upload_text(
            manifest_key,
            content=json.dumps(
                {
                    "election_year": election_year,
                    "election_type": treatment.election_type,
                    "source_archive_key": archive_key,
                    "row_count": sum(artifact.row_count for artifact in year_artifacts),
                    "artifacts": [asdict(artifact) for artifact in year_artifacts],
                },
                ensure_ascii=False,
                indent=2,
            ),
            content_type="application/json; charset=utf-8",
        )

    LOGGER.info(
        "Tratamento Silver concluido: %s registros em %s arquivos.",
        total_records,
        len(silver_artifacts),
    )
    result: dict[str, Any] = {
        "silver_record_count": total_records,
        "silver_artifacts": silver_artifacts,
    }
    if collect_records:
        result["silver_records"] = silver_records
    return result
