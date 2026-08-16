"""Validacoes de qualidade especificas para os arquivos Silver do FEFC."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from io import StringIO
from typing import Any, Mapping, Sequence

from src.bronze.storage import S3Storage
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)

_EXPECTED_DATASETS = {
    "fefc_cor_raca",
    "fefc_genero",
    "fp_cor_raca",
    "fp_genero",
}
_ELECTION_TYPES = {2020: "municipal", 2022: "geral", 2024: "municipal"}
_VALID_GENDERS = {"FEMININO", "MASCULINO"}
_VALID_RACES = {"NEGRA", "NAO NEGRA", "NÃO NEGRA"}
_VALID_PARTY_LEVELS = {"NACIONAL", "ESTADUAL", "MUNICIPAL"}
_MAX_SAMPLES = 5

_COMMON_REQUIRED_FIELDS = {
    "silver_layer",
    "source_name",
    "source_row_number",
    "source_archive",
    "source_member",
    "ano_eleicao",
    "tipo_eleicao",
    "tipo_fundo",
    "dimensao_agregacao",
    "sigla_partido",
    "numero_partido",
    "genero",
    "quantidade_candidatos",
    "data_geracao",
    "hora_geracao",
    "data_hora_geracao",
}
_DATASET_REQUIRED_FIELDS = {
    "fefc_genero": {
        "valor_partido_fefc",
        "percentual_candidatos_partido_genero",
        "valor_repasse_minimo_cota",
        "valor_total_recebido_fefc",
        "percentual_valor_fefc_genero",
    },
    "fefc_cor_raca": {
        "cor_raca",
        "valor_partido_fefc",
        "percentual_candidatos_partido_genero",
        "valor_repasse_minimo_cota",
        "valor_total_recebido_fefc",
        "percentual_valor_fefc_genero",
    },
    "fp_genero": {
        "esfera_partidaria",
        "valor_despesa_diretorio_fp",
        "percentual_candidatos_partido_genero",
        "valor_despesa_minimo_cota",
        "valor_total_recebido_fp",
    },
    "fp_cor_raca": {
        "esfera_partidaria",
        "cor_raca",
        "valor_despesa_diretorio_fp",
        "percentual_candidatos_partido_genero",
        "valor_despesa_minimo_cota",
        "valor_total_recebido_fp",
    },
}
_INTEGER_FIELDS = {
    "source_row_number",
    "ano_eleicao",
    "numero_partido",
    "quantidade_candidatos",
    "status_renuncia",
}
_DECIMAL_FIELDS = {
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
_GRAIN_FIELDS = {
    "fefc_genero": ("ano_eleicao", "sigla_partido", "numero_partido", "genero"),
    "fefc_cor_raca": (
        "ano_eleicao",
        "sigla_partido",
        "numero_partido",
        "genero",
        "cor_raca",
    ),
    "fp_genero": (
        "ano_eleicao",
        "sigla_partido",
        "numero_partido",
        "esfera_partidaria",
        "sigla_uf",
        "sigla_ue",
        "municipio",
        "genero",
    ),
    "fp_cor_raca": (
        "ano_eleicao",
        "sigla_partido",
        "numero_partido",
        "esfera_partidaria",
        "sigla_uf",
        "sigla_ue",
        "municipio",
        "genero",
        "cor_raca",
    ),
}


@dataclass(frozen=True, slots=True)
class QualityIssue:
    """Descreve uma inconsistencia encontrada, sua gravidade e exemplos de linhas."""

    rule: str
    severity: str
    election_year: int
    dataset_name: str
    output_key: str
    column: str | None
    count: int
    rate: float
    message: str
    sample_rows: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ArtifactQualityResult:
    """Resume a validacao de um artefato Silver e suas inconsistencias."""

    election_year: int
    dataset_name: str
    output_key: str
    expected_row_count: int
    observed_row_count: int
    duplicate_count: int
    null_counts: dict[str, int]
    error_count: int
    warning_count: int


@dataclass(frozen=True, slots=True)
class QualityReport:
    """Consolida os resultados e relatorios persistidos de uma execucao de qualidade."""

    valid: bool
    generated_at: str
    artifact_count: int
    row_count: int
    error_count: int
    warning_count: int
    artifacts: tuple[ArtifactQualityResult, ...]
    issues: tuple[QualityIssue, ...]
    report_keys: tuple[str, ...]


@dataclass(slots=True)
class _IssueState:
    """Acumula a contagem e exemplos de linhas de uma regra durante a validacao."""

    rule: str
    severity: str
    column: str | None
    message: str
    count: int = 0
    sample_rows: list[int] | None = None

    def add(self, source_row: int | None = None, count: int = 1) -> None:
        """Recebe uma linha e quantidade e as incorpora ao estado; nao retorna valor."""

        self.count += count
        if self.sample_rows is None:
            self.sample_rows = []
        if source_row is not None and len(self.sample_rows) < _MAX_SAMPLES:
            self.sample_rows.append(source_row)


@dataclass(slots=True)
class _ArtifactProfile:
    """Agrupa o resultado e as chaves naturais observadas em um artefato."""

    result: ArtifactQualityResult
    issues: list[QualityIssue]
    parent_keys: set[tuple[str, ...]]


def _is_null(value: Any) -> bool:
    """Recebe um valor e retorna se ele deve ser considerado nulo."""

    return value is None or str(value).strip() == ""


def _source_row(row: Mapping[str, str], fallback: int) -> int:
    """Recebe um registro e uma linha alternativa e retorna a linha de origem valida."""

    try:
        return int(row.get("source_row_number") or fallback)
    except ValueError:
        return fallback


def _issue(
    states: dict[tuple[str, str, str | None], _IssueState],
    *,
    rule: str,
    severity: str,
    message: str,
    column: str | None = None,
    source_row: int | None = None,
    count: int = 1,
) -> None:
    """Recebe os dados de uma ocorrencia e a acumula por regra; nao retorna valor."""

    key = (rule, severity, column)
    if key not in states:
        states[key] = _IssueState(rule, severity, column, message)
    states[key].add(source_row, count=count)


def _parse_integer(value: str) -> int | None:
    """Recebe um texto e retorna o inteiro convertido ou nulo quando invalido."""

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_decimal(value: str) -> Decimal | None:
    """Recebe um texto e retorna o decimal convertido ou nulo quando invalido."""

    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None


def _validate_geography(
    row: Mapping[str, str],
    states: dict[tuple[str, str, str | None], _IssueState],
    source_row: int,
) -> None:
    """Recebe uma linha FP, registra inconsistencias geograficas e nao retorna valor."""

    level = row.get("esfera_partidaria", "")
    uf = row.get("sigla_uf", "")
    ue = row.get("sigla_ue", "")
    city = row.get("municipio", "")

    required: tuple[tuple[str, str], ...] = ()
    forbidden: tuple[tuple[str, str], ...] = ()
    if level == "MUNICIPAL":
        required = (("sigla_uf", uf), ("sigla_ue", ue), ("municipio", city))
    elif level == "ESTADUAL":
        required = (("sigla_uf", uf),)
        forbidden = (("sigla_ue", ue), ("municipio", city))
    elif level == "NACIONAL":
        forbidden = (("sigla_uf", uf), ("sigla_ue", ue), ("municipio", city))

    for column, value in required:
        if _is_null(value):
            _issue(
                states,
                rule="geographic_integrity",
                severity="error",
                column=column,
                message=f"{column} obrigatorio para esfera {level}.",
                source_row=source_row,
            )
    for column, value in forbidden:
        if not _is_null(value):
            _issue(
                states,
                rule="geographic_integrity",
                severity="error",
                column=column,
                message=f"{column} deve ser nulo para esfera {level}.",
                source_row=source_row,
            )


def _validate_row(
    row: Mapping[str, str],
    *,
    artifact: Mapping[str, Any],
    required_fields: set[str],
    states: dict[tuple[str, str, str | None], _IssueState],
    fallback_row: int,
) -> None:
    """Recebe uma linha Silver e seu contrato, registra violacoes e nao retorna valor."""

    source_row = _source_row(row, fallback_row)
    election_year = int(artifact["election_year"])
    dataset_name = str(artifact["dataset_name"])

    for field in required_fields:
        if _is_null(row.get(field)):
            _issue(
                states,
                rule="required_not_null",
                severity="error",
                column=field,
                message=f"Campo obrigatorio {field} esta nulo.",
                source_row=source_row,
            )

    if _is_null(row.get("percentual_valor_fp_genero")) and dataset_name.startswith("fp_"):
        _issue(
            states,
            rule="source_numeric_null",
            severity="warning",
            column="percentual_valor_fp_genero",
            message="Percentual nulo apos valor invalido na fonte TSE.",
            source_row=source_row,
        )

    for field in _INTEGER_FIELDS.intersection(row):
        value = row.get(field, "")
        if _is_null(value):
            continue
        parsed = _parse_integer(value)
        if parsed is None:
            _issue(
                states,
                rule="invalid_integer",
                severity="error",
                column=field,
                message=f"Campo {field} nao e um inteiro valido.",
                source_row=source_row,
            )
        elif field in {"numero_partido", "quantidade_candidatos"} and parsed < 0:
            _issue(
                states,
                rule="negative_value",
                severity="error",
                column=field,
                message=f"Campo {field} nao pode ser negativo.",
                source_row=source_row,
            )

    for field in _DECIMAL_FIELDS.intersection(row):
        value = row.get(field, "")
        if _is_null(value):
            continue
        parsed = _parse_decimal(value)
        if parsed is None:
            _issue(
                states,
                rule="invalid_decimal",
                severity="error",
                column=field,
                message=f"Campo {field} nao e um decimal valido.",
                source_row=source_row,
            )
            continue
        if field.startswith("percentual_") and (parsed < 0 or parsed > 100):
            _issue(
                states,
                rule="percentage_outlier",
                severity="warning",
                column=field,
                message=f"Campo {field} esta fora da faixa usual de 0 a 100.",
                source_row=source_row,
            )
        elif parsed < 0:
            _issue(
                states,
                rule="negative_financial_value",
                severity="warning",
                column=field,
                message=f"Campo financeiro {field} possui ajuste negativo.",
                source_row=source_row,
            )

    expected_metadata = {
        "ano_eleicao": str(election_year),
        "tipo_eleicao": _ELECTION_TYPES.get(election_year, ""),
        "tipo_fundo": str(artifact["fund_type"]),
        "dimensao_agregacao": str(artifact["aggregation_dimension"]),
        "silver_layer": "silver",
        "source_name": "fundo_eleitoral",
    }
    for field, expected in expected_metadata.items():
        if row.get(field) != expected:
            _issue(
                states,
                rule="metadata_integrity",
                severity="error",
                column=field,
                message=f"Campo {field} diverge do valor esperado {expected!r}.",
                source_row=source_row,
            )

    if row.get("genero") not in _VALID_GENDERS:
        _issue(
            states,
            rule="accepted_values",
            severity="error",
            column="genero",
            message="Genero fora do dominio esperado.",
            source_row=source_row,
        )
    if dataset_name.endswith("cor_raca") and row.get("cor_raca") not in _VALID_RACES:
        _issue(
            states,
            rule="accepted_values",
            severity="error",
            column="cor_raca",
            message="Cor/raca fora do dominio esperado.",
            source_row=source_row,
        )
    if dataset_name.startswith("fp_"):
        if row.get("esfera_partidaria") not in _VALID_PARTY_LEVELS:
            _issue(
                states,
                rule="accepted_values",
                severity="error",
                column="esfera_partidaria",
                message="Esfera partidaria fora do dominio esperado.",
                source_row=source_row,
            )
        _validate_geography(row, states, source_row)

    try:
        generated_at = datetime.fromisoformat(row.get("data_hora_geracao", ""))
        if generated_at.date().isoformat() != row.get("data_geracao"):
            raise ValueError
        if generated_at.time().isoformat() != row.get("hora_geracao"):
            raise ValueError
    except ValueError:
        _issue(
            states,
            rule="datetime_integrity",
            severity="error",
            column="data_hora_geracao",
            message="Data/hora de geracao invalida ou inconsistente.",
            source_row=source_row,
        )


def _finalize_issues(
    states: Mapping[tuple[str, str, str | None], _IssueState],
    *,
    artifact: Mapping[str, Any],
    row_count: int,
) -> list[QualityIssue]:
    """Recebe estados acumulados e retorna as inconsistencias consolidadas do artefato."""

    return [
        QualityIssue(
            rule=state.rule,
            severity=state.severity,
            election_year=int(artifact["election_year"]),
            dataset_name=str(artifact["dataset_name"]),
            output_key=str(artifact["output_key"]),
            column=state.column,
            count=state.count,
            rate=round(state.count / row_count, 6) if row_count else 1.0,
            message=state.message,
            sample_rows=tuple(state.sample_rows or ()),
        )
        for state in states.values()
    ]


def _validate_artifact(
    storage: S3Storage,
    artifact: Mapping[str, Any],
) -> _ArtifactProfile:
    """Recebe storage e metadados Silver e retorna o perfil de qualidade do CSV."""

    dataset_name = str(artifact["dataset_name"])
    output_key = str(artifact["output_key"])
    content = storage.download_bytes(output_key).decode("utf-8-sig")
    reader = csv.DictReader(StringIO(content), delimiter=";")
    headers = tuple(reader.fieldnames or ())
    expected_headers = tuple(artifact.get("columns", ()))
    states: dict[tuple[str, str, str | None], _IssueState] = {}

    if headers != expected_headers:
        _issue(
            states,
            rule="schema_integrity",
            severity="error",
            message="Schema do objeto S3 diverge do manifesto Silver.",
        )

    required_fields = _COMMON_REQUIRED_FIELDS | _DATASET_REQUIRED_FIELDS.get(
        dataset_name, set()
    )
    missing_columns = sorted(required_fields - set(headers))
    for column in missing_columns:
        _issue(
            states,
            rule="missing_column",
            severity="error",
            column=column,
            message=f"Coluna obrigatoria {column} nao existe no CSV.",
        )

    null_counts: Counter[str] = Counter()
    seen_grain: set[tuple[str, ...]] = set()
    seen_source_rows: set[str] = set()
    parent_keys: set[tuple[str, ...]] = set()
    duplicate_count = 0
    row_count = 0
    grain_fields = _GRAIN_FIELDS.get(dataset_name, ())
    parent_fields = tuple(field for field in grain_fields if field != "cor_raca")

    for fallback_row, row in enumerate(reader, start=2):
        row_count += 1
        source_row = _source_row(row, fallback_row)
        for column in headers:
            if _is_null(row.get(column)):
                null_counts[column] += 1

        _validate_row(
            row,
            artifact=artifact,
            required_fields=required_fields,
            states=states,
            fallback_row=fallback_row,
        )

        grain_key = tuple(row.get(field, "") for field in grain_fields)
        if grain_key in seen_grain:
            duplicate_count += 1
            _issue(
                states,
                rule="duplicate_grain",
                severity="error",
                message="Registro duplicado no grao analitico esperado.",
                source_row=source_row,
            )
        seen_grain.add(grain_key)
        parent_keys.add(tuple(row.get(field, "") for field in parent_fields))

        source_row_value = row.get("source_row_number", "")
        if source_row_value in seen_source_rows:
            _issue(
                states,
                rule="duplicate_source_row",
                severity="error",
                column="source_row_number",
                message="Numero de linha de origem duplicado no arquivo.",
                source_row=source_row,
            )
        seen_source_rows.add(source_row_value)

    expected_count = int(artifact["row_count"])
    if row_count != expected_count:
        _issue(
            states,
            rule="row_count_integrity",
            severity="error",
            message=(
                f"Contagem observada {row_count} difere do manifesto {expected_count}."
            ),
        )

    issues = _finalize_issues(states, artifact=artifact, row_count=row_count)
    result = ArtifactQualityResult(
        election_year=int(artifact["election_year"]),
        dataset_name=dataset_name,
        output_key=output_key,
        expected_row_count=expected_count,
        observed_row_count=row_count,
        duplicate_count=duplicate_count,
        null_counts=dict(sorted(null_counts.items())),
        error_count=sum(issue.count for issue in issues if issue.severity == "error"),
        warning_count=sum(
            issue.count for issue in issues if issue.severity == "warning"
        ),
    )
    return _ArtifactProfile(result=result, issues=issues, parent_keys=parent_keys)


def _cross_artifact_issues(
    artifacts: Sequence[Mapping[str, Any]],
    profiles: Mapping[tuple[int, str], _ArtifactProfile],
) -> list[QualityIssue]:
    """Recebe artefatos e perfis e retorna inconsistencias entre arquivos relacionados."""

    issues: list[QualityIssue] = []
    by_year: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for artifact in artifacts:
        by_year[int(artifact["election_year"])].append(artifact)

    for year, year_artifacts in by_year.items():
        dataset_counts = Counter(str(item["dataset_name"]) for item in year_artifacts)
        missing = sorted(_EXPECTED_DATASETS - set(dataset_counts))
        duplicated = sorted(name for name, count in dataset_counts.items() if count > 1)
        if missing or duplicated:
            issues.append(
                QualityIssue(
                    rule="artifact_integrity",
                    severity="error",
                    election_year=year,
                    dataset_name="_year_partition",
                    output_key="",
                    column=None,
                    count=len(missing) + len(duplicated),
                    rate=1.0,
                    message=f"Datasets ausentes={missing}; duplicados={duplicated}.",
                    sample_rows=(),
                )
            )

        for fund_type in ("fefc", "fp"):
            parent = profiles.get((year, f"{fund_type}_genero"))
            child = profiles.get((year, f"{fund_type}_cor_raca"))
            if parent is None or child is None:
                continue
            missing_children = parent.parent_keys - child.parent_keys
            orphan_children = child.parent_keys - parent.parent_keys
            if missing_children or orphan_children:
                issues.append(
                    QualityIssue(
                        rule="cross_dataset_integrity",
                        severity="error",
                        election_year=year,
                        dataset_name=fund_type,
                        output_key="",
                        column=None,
                        count=len(missing_children) + len(orphan_children),
                        rate=round(
                            (len(missing_children) + len(orphan_children))
                            / max(len(parent.parent_keys), 1),
                            6,
                        ),
                        message=(
                            "Cobertura entre arquivos de genero e cor/raca inconsistente: "
                            f"sem cor/raca={len(missing_children)}, "
                            f"orfaos={len(orphan_children)}."
                        ),
                        sample_rows=(),
                    )
                )
    return issues


def _build_report(
    profiles: Sequence[_ArtifactProfile],
    issues: Sequence[QualityIssue],
    report_keys: Sequence[str],
) -> QualityReport:
    """Recebe perfis, inconsistencias e chaves e retorna o relatorio consolidado."""

    artifacts = tuple(profile.result for profile in profiles)
    return QualityReport(
        valid=not any(issue.severity == "error" for issue in issues),
        generated_at=datetime.now(timezone.utc).isoformat(),
        artifact_count=len(artifacts),
        row_count=sum(item.observed_row_count for item in artifacts),
        error_count=sum(issue.count for issue in issues if issue.severity == "error"),
        warning_count=sum(
            issue.count for issue in issues if issue.severity == "warning"
        ),
        artifacts=artifacts,
        issues=tuple(issues),
        report_keys=tuple(report_keys),
    )


def validate_silver_artifacts(
    storage: S3Storage,
    artifacts: Sequence[Mapping[str, Any]],
    *,
    persist_reports: bool = True,
) -> QualityReport:
    """Recebe storage e artefatos Silver, valida e retorna o relatorio de qualidade."""

    if not artifacts:
        raise ValueError("Nenhum artefato Silver recebido para validacao.")

    profiles: dict[tuple[int, str], _ArtifactProfile] = {}
    ordered_profiles: list[_ArtifactProfile] = []
    for artifact in artifacts:
        profile = _validate_artifact(storage, artifact)
        key = (profile.result.election_year, profile.result.dataset_name)
        profiles[key] = profile
        ordered_profiles.append(profile)

    issues = [issue for profile in ordered_profiles for issue in profile.issues]
    issues.extend(_cross_artifact_issues(artifacts, profiles))
    report_keys: list[str] = []

    if persist_reports:
        years = sorted({profile.result.election_year for profile in ordered_profiles})
        for year in years:
            year_profiles = [
                profile
                for profile in ordered_profiles
                if profile.result.election_year == year
            ]
            year_issues = [issue for issue in issues if issue.election_year == year]
            report_key = storage.paths.build_quality_report_key(year)
            year_report = _build_report(year_profiles, year_issues, (report_key,))
            storage.upload_text(
                report_key,
                json.dumps(asdict(year_report), ensure_ascii=False, indent=2),
                content_type="application/json; charset=utf-8",
            )
            report_keys.append(report_key)

    report = _build_report(ordered_profiles, issues, report_keys)
    LOGGER.info(
        "Qualidade Silver: valid=%s, linhas=%s, erros=%s, alertas=%s.",
        report.valid,
        report.row_count,
        report.error_count,
        report.warning_count,
    )
    return report
