"""Carga dos CSVs Silver no modelo analitico PostgreSQL."""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import asdict, dataclass
from io import StringIO
from typing import Any, Mapping, Sequence

from src.bronze.storage import S3Storage
from src.gold.loader import PostgresWarehouse
from src.ingestion.scope import FEFC_ELECTION_YEARS
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)
_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


class WarehouseLoadError(RuntimeError):
    """Raised when a Silver artifact cannot be loaded safely."""


@dataclass(frozen=True, slots=True)
class DatasetLoadSpec:
    target_table: str
    uses_location: bool
    uses_race: bool


@dataclass(frozen=True, slots=True)
class ArtifactLoadResult:
    election_year: int
    dataset_name: str
    source_member: str
    target_table: str
    staged_rows: int
    deleted_rows: int
    inserted_rows: int


@dataclass(frozen=True, slots=True)
class WarehouseLoadReport:
    artifact_count: int
    staged_rows: int
    deleted_rows: int
    inserted_rows: int
    results: tuple[ArtifactLoadResult, ...]


_LOAD_SPECS = {
    "fefc_genero": DatasetLoadSpec("dw.fato_fefc_genero", False, False),
    "fefc_cor_raca": DatasetLoadSpec("dw.fato_fefc_cor_raca", False, True),
    "fp_genero": DatasetLoadSpec("dw.fato_fp_genero", True, False),
    "fp_cor_raca": DatasetLoadSpec("dw.fato_fp_cor_raca", True, True),
}


def _quote_identifier(identifier: str) -> str:
    if _IDENTIFIER_PATTERN.fullmatch(identifier) is None:
        raise WarehouseLoadError(f"Identificador SQL invalido: {identifier!r}")
    return f'"{identifier}"'


def _stage_name(dataset_name: str) -> str:
    if dataset_name not in _LOAD_SPECS:
        raise WarehouseLoadError(f"Dataset Silver nao suportado: {dataset_name}")
    return f"stage_{dataset_name}"


def _read_silver_csv(payload: bytes) -> tuple[tuple[str, ...], str]:
    content = payload.decode("utf-8-sig")
    reader = csv.reader(StringIO(content), delimiter=";")
    headers = tuple(next(reader, ()))
    if not headers:
        raise WarehouseLoadError("CSV Silver sem cabecalho.")
    for header in headers:
        _quote_identifier(header)
    return headers, content


def _create_stage(cursor, stage_name: str, columns: Sequence[str]) -> None:
    quoted_stage = _quote_identifier(stage_name)
    definitions = ", ".join(f"{_quote_identifier(column)} TEXT" for column in columns)
    cursor.execute(f"DROP TABLE IF EXISTS {quoted_stage}")
    cursor.execute(
        f"CREATE TEMP TABLE {quoted_stage} ({definitions}) ON COMMIT DROP"
    )


def _copy_stage(
    cursor,
    stage_name: str,
    columns: Sequence[str],
    content: str,
) -> int:
    quoted_stage = _quote_identifier(stage_name)
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    cursor.copy_expert(
        (
            f"COPY {quoted_stage} ({quoted_columns}) FROM STDIN "
            "WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', NULL '', ENCODING 'UTF8')"
        ),
        StringIO(content),
    )
    cursor.execute(f"SELECT COUNT(*) FROM {quoted_stage}")
    return int(cursor.fetchone()[0])


def _upsert_dimensions(cursor, stage_name: str, spec: DatasetLoadSpec) -> None:
    stage = _quote_identifier(stage_name)
    cursor.execute(
        f"""
        INSERT INTO dw.dim_partido (ano_eleicao, numero_partido, sigla_partido)
        SELECT DISTINCT
            ano_eleicao::SMALLINT,
            numero_partido::INTEGER,
            sigla_partido
        FROM {stage}
        ON CONFLICT (ano_eleicao, numero_partido) DO UPDATE
        SET sigla_partido = EXCLUDED.sigla_partido
        """
    )
    cursor.execute(
        f"""
        INSERT INTO dw.dim_genero (genero)
        SELECT DISTINCT genero FROM {stage}
        ON CONFLICT (genero) DO NOTHING
        """
    )
    if spec.uses_race:
        cursor.execute(
            f"""
            INSERT INTO dw.dim_cor_raca (cor_raca)
            SELECT DISTINCT cor_raca FROM {stage}
            ON CONFLICT (cor_raca) DO NOTHING
            """
        )
    if spec.uses_location:
        cursor.execute(
            f"""
            INSERT INTO dw.dim_localidade (
                ano_eleicao,
                esfera_partidaria,
                sigla_uf,
                sigla_ue,
                municipio
            )
            SELECT DISTINCT
                ano_eleicao::SMALLINT,
                esfera_partidaria,
                NULLIF(sigla_uf, ''),
                NULLIF(sigla_ue, ''),
                NULLIF(municipio, '')
            FROM {stage}
            ON CONFLICT (
                chave_natural
            ) DO NOTHING
            """
        )


def _dimension_joins(spec: DatasetLoadSpec) -> str:
    joins = """
        JOIN dw.dim_partido p
          ON p.ano_eleicao = s.ano_eleicao::SMALLINT
         AND p.numero_partido = s.numero_partido::INTEGER
        JOIN dw.dim_genero g ON g.genero = s.genero
    """
    if spec.uses_location:
        joins += """
        JOIN dw.dim_localidade l
          ON l.chave_natural = (
                s.ano_eleicao
                || '|'
                || s.esfera_partidaria
                || '|'
                || COALESCE(s.sigla_uf, '')
                || '|'
                || COALESCE(s.sigla_ue, '')
                || '|'
                || COALESCE(s.municipio, '')
             )
        """
    if spec.uses_race:
        joins += """
        JOIN dw.dim_cor_raca c ON c.cor_raca = s.cor_raca
        """
    return joins


def _fact_insert_sql(dataset_name: str, stage_name: str) -> str:
    spec = _LOAD_SPECS[dataset_name]
    stage = _quote_identifier(stage_name)
    joins = _dimension_joins(spec)

    if dataset_name == "fefc_genero":
        columns = """
            ano_eleicao, partido_id, genero_id, quantidade_candidatos,
            valor_partido_fefc, percentual_candidatos_partido_genero,
            valor_repasse_minimo_cota, valor_total_recebido_fefc,
            percentual_valor_fefc_genero, status_renuncia,
            data_hora_geracao, source_archive, source_member, source_row_number
        """
        values = """
            s.ano_eleicao::SMALLINT, p.partido_id, g.genero_id,
            s.quantidade_candidatos::INTEGER, s.valor_partido_fefc::NUMERIC,
            s.percentual_candidatos_partido_genero::NUMERIC,
            s.valor_repasse_minimo_cota::NUMERIC,
            s.valor_total_recebido_fefc::NUMERIC,
            s.percentual_valor_fefc_genero::NUMERIC,
            NULLIF(s.status_renuncia, '')::SMALLINT,
            s.data_hora_geracao::TIMESTAMP,
            s.source_archive, s.source_member, s.source_row_number::INTEGER
        """
    elif dataset_name == "fefc_cor_raca":
        columns = """
            ano_eleicao, partido_id, genero_id, cor_raca_id,
            quantidade_candidatos, valor_partido_fefc,
            percentual_candidatos_partido_genero, valor_repasse_minimo_cota,
            valor_total_recebido_fefc, percentual_valor_fefc_genero,
            status_renuncia, data_hora_geracao,
            source_archive, source_member, source_row_number
        """
        values = """
            s.ano_eleicao::SMALLINT, p.partido_id, g.genero_id, c.cor_raca_id,
            s.quantidade_candidatos::INTEGER, s.valor_partido_fefc::NUMERIC,
            s.percentual_candidatos_partido_genero::NUMERIC,
            s.valor_repasse_minimo_cota::NUMERIC,
            s.valor_total_recebido_fefc::NUMERIC,
            s.percentual_valor_fefc_genero::NUMERIC,
            NULLIF(s.status_renuncia, '')::SMALLINT,
            s.data_hora_geracao::TIMESTAMP,
            s.source_archive, s.source_member, s.source_row_number::INTEGER
        """
    elif dataset_name == "fp_genero":
        columns = """
            ano_eleicao, partido_id, localidade_id, genero_id,
            quantidade_candidatos, valor_despesa_diretorio_fp,
            percentual_candidatos_partido_genero, valor_despesa_minimo_cota,
            valor_total_recebido_fp, percentual_valor_fp_genero,
            data_hora_geracao, source_archive, source_member, source_row_number
        """
        values = """
            s.ano_eleicao::SMALLINT, p.partido_id, l.localidade_id, g.genero_id,
            s.quantidade_candidatos::INTEGER,
            s.valor_despesa_diretorio_fp::NUMERIC,
            s.percentual_candidatos_partido_genero::NUMERIC,
            s.valor_despesa_minimo_cota::NUMERIC,
            s.valor_total_recebido_fp::NUMERIC,
            NULLIF(s.percentual_valor_fp_genero, '')::NUMERIC,
            s.data_hora_geracao::TIMESTAMP,
            s.source_archive, s.source_member, s.source_row_number::INTEGER
        """
    elif dataset_name == "fp_cor_raca":
        columns = """
            ano_eleicao, partido_id, localidade_id, genero_id, cor_raca_id,
            quantidade_candidatos, valor_despesa_diretorio_fp,
            percentual_candidatos_partido_genero, valor_despesa_minimo_cota,
            valor_total_recebido_fp, percentual_valor_fp_genero,
            data_hora_geracao, source_archive, source_member, source_row_number
        """
        values = """
            s.ano_eleicao::SMALLINT, p.partido_id, l.localidade_id,
            g.genero_id, c.cor_raca_id, s.quantidade_candidatos::INTEGER,
            s.valor_despesa_diretorio_fp::NUMERIC,
            s.percentual_candidatos_partido_genero::NUMERIC,
            s.valor_despesa_minimo_cota::NUMERIC,
            s.valor_total_recebido_fp::NUMERIC,
            NULLIF(s.percentual_valor_fp_genero, '')::NUMERIC,
            s.data_hora_geracao::TIMESTAMP,
            s.source_archive, s.source_member, s.source_row_number::INTEGER
        """
    else:  # pragma: no cover - guarded by _LOAD_SPECS
        raise WarehouseLoadError(f"Dataset Silver nao suportado: {dataset_name}")

    return f"INSERT INTO {spec.target_table} ({columns}) SELECT {values} FROM {stage} s {joins}"


def _record_load_audit(
    cursor,
    artifact: Mapping[str, Any],
    result: ArtifactLoadResult,
) -> None:
    cursor.execute(
        """
        INSERT INTO dw.carga_arquivo (
            ano_eleicao,
            dataset_name,
            source_member,
            output_key,
            linhas_origem,
            linhas_removidas,
            linhas_inseridas,
            carregado_em
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (ano_eleicao, dataset_name) DO UPDATE
        SET source_member = EXCLUDED.source_member,
            output_key = EXCLUDED.output_key,
            linhas_origem = EXCLUDED.linhas_origem,
            linhas_removidas = EXCLUDED.linhas_removidas,
            linhas_inseridas = EXCLUDED.linhas_inseridas,
            carregado_em = CURRENT_TIMESTAMP
        """,
        (
            result.election_year,
            result.dataset_name,
            result.source_member,
            str(artifact["output_key"]),
            result.staged_rows,
            result.deleted_rows,
            result.inserted_rows,
        ),
    )


def _load_artifact(cursor, storage: S3Storage, artifact: Mapping[str, Any]) -> ArtifactLoadResult:
    dataset_name = str(artifact["dataset_name"])
    spec = _LOAD_SPECS.get(dataset_name)
    if spec is None:
        raise WarehouseLoadError(f"Dataset Silver nao suportado: {dataset_name}")

    stage_name = _stage_name(dataset_name)
    headers, content = _read_silver_csv(storage.download_bytes(str(artifact["output_key"])))
    expected_headers = tuple(artifact.get("columns", ()))
    if headers != expected_headers:
        raise WarehouseLoadError(
            f"Schema de {artifact['output_key']} diverge do manifesto Silver."
        )

    _create_stage(cursor, stage_name, headers)
    staged_rows = _copy_stage(cursor, stage_name, headers, content)
    expected_rows = int(artifact["row_count"])
    if staged_rows != expected_rows:
        raise WarehouseLoadError(
            f"Contagem de {artifact['output_key']} diverge do manifesto: "
            f"observado={staged_rows}, esperado={expected_rows}."
        )

    _upsert_dimensions(cursor, stage_name, spec)
    cursor.execute(
        f"DELETE FROM {spec.target_table} WHERE ano_eleicao = %s AND source_member = %s",
        (int(artifact["election_year"]), str(artifact["source_member"])),
    )
    deleted_rows = max(cursor.rowcount, 0)

    cursor.execute(_fact_insert_sql(dataset_name, stage_name))
    inserted_rows = max(cursor.rowcount, 0)
    if inserted_rows != staged_rows:
        raise WarehouseLoadError(
            f"Carga incompleta em {spec.target_table}: "
            f"inserido={inserted_rows}, esperado={staged_rows}."
        )

    result = ArtifactLoadResult(
        election_year=int(artifact["election_year"]),
        dataset_name=dataset_name,
        source_member=str(artifact["source_member"]),
        target_table=spec.target_table,
        staged_rows=staged_rows,
        deleted_rows=deleted_rows,
        inserted_rows=inserted_rows,
    )
    _record_load_audit(cursor, artifact, result)
    cursor.execute(f"DROP TABLE {_quote_identifier(stage_name)}")
    return result


def load_silver_artifacts(
    storage: S3Storage,
    artifacts: Sequence[Mapping[str, Any]],
    warehouse: PostgresWarehouse | None = None,
) -> WarehouseLoadReport:
    """Load validated Silver artifacts into the partitioned analytical model."""

    if not artifacts:
        raise WarehouseLoadError("Nenhum artefato Silver recebido para carga.")
    resolved_warehouse = warehouse or PostgresWarehouse()
    raw_connection = resolved_warehouse.engine.raw_connection()
    cursor = raw_connection.cursor()
    results: list[ArtifactLoadResult] = []

    try:
        cursor.execute("SELECT to_regclass('dw.carga_arquivo')")
        if cursor.fetchone()[0] is None:
            raise WarehouseLoadError(
                "Modelo analitico nao esta migrado. Execute as migrations antes da carga."
            )

        for artifact in sorted(
            artifacts,
            key=lambda item: (int(item["election_year"]), str(item["dataset_name"])),
        ):
            result = _load_artifact(cursor, storage, artifact)
            results.append(result)
            LOGGER.info(
                "Carga Gold %s/%s: %s linhas.",
                result.election_year,
                result.dataset_name,
                result.inserted_rows,
            )
        raw_connection.commit()
    except Exception:
        raw_connection.rollback()
        raise
    finally:
        cursor.close()
        raw_connection.close()

    return WarehouseLoadReport(
        artifact_count=len(results),
        staged_rows=sum(item.staged_rows for item in results),
        deleted_rows=sum(item.deleted_rows for item in results),
        inserted_rows=sum(item.inserted_rows for item in results),
        results=tuple(results),
    )


def load_from_s3_manifests(
    election_years: Sequence[int] = FEFC_ELECTION_YEARS,
) -> WarehouseLoadReport:
    """Manual entrypoint that requires valid persisted quality reports."""

    storage = S3Storage()
    artifacts: list[dict[str, Any]] = []
    for year in election_years:
        quality_key = storage.paths.build_quality_report_key(year)
        quality_report = json.loads(storage.download_bytes(quality_key))
        if not quality_report.get("valid"):
            raise WarehouseLoadError(
                f"Relatorio de qualidade invalido ou ausente para {year}: {quality_key}"
            )
        manifest_key = f"{storage.paths.silver_treated_prefix(year)}_manifest.json"
        manifest = json.loads(storage.download_bytes(manifest_key))
        artifacts.extend(manifest["artifacts"])
    return load_silver_artifacts(storage, artifacts)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Carga Silver para o DW FEFC")
    parser.add_argument("--years", nargs="+", type=int, default=list(FEFC_ELECTION_YEARS))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = load_from_s3_manifests(args.years)
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
