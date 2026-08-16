from __future__ import annotations

import csv
from io import StringIO

from src.quality.fefc_validations import validate_silver_artifacts
from tests.silver.test_transformations import _transform_year


def _change_first_row(storage, key: str, **changes: str) -> None:
    content = storage._archives[key].decode("utf-8")
    reader = csv.DictReader(StringIO(content), delimiter=";")
    rows = list(reader)
    rows[0].update(changes)

    output = StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=reader.fieldnames, delimiter=";")
    writer.writeheader()
    writer.writerows(rows)
    storage._archives[key] = output.getvalue().encode("utf-8")


def test_valid_silver_artifacts_generate_partitioned_quality_report() -> None:
    transformed, storage = _transform_year(2024)

    report = validate_silver_artifacts(storage, transformed["silver_artifacts"])

    assert report.valid is True
    assert report.artifact_count == 4
    assert report.row_count == 4
    assert report.error_count == 0
    assert report.warning_count == 0
    assert report.report_keys == (
        "quality/fundo_eleitoral/ano_eleicao=2024/_quality_report.json",
    )
    assert storage.uploaded_text[-1][0] == report.report_keys[0]


def test_required_null_and_geographic_inconsistency_are_errors() -> None:
    transformed, storage = _transform_year(2024)
    artifact = next(
        item
        for item in transformed["silver_artifacts"]
        if item["dataset_name"] == "fp_genero"
    )
    _change_first_row(
        storage,
        artifact["output_key"],
        sigla_partido="",
        municipio="",
    )

    report = validate_silver_artifacts(
        storage,
        transformed["silver_artifacts"],
        persist_reports=False,
    )

    assert report.valid is False
    assert {issue.rule for issue in report.issues} >= {
        "required_not_null",
        "geographic_integrity",
        "cross_dataset_integrity",
    }


def test_duplicate_grain_is_reported() -> None:
    transformed, storage = _transform_year(2024)
    artifact = next(
        item
        for item in transformed["silver_artifacts"]
        if item["dataset_name"] == "fefc_genero"
    )
    content = storage._archives[artifact["output_key"]].decode("utf-8")
    header, row = content.strip().splitlines()
    storage._archives[artifact["output_key"]] = f"{header}\n{row}\n{row}\n".encode()

    report = validate_silver_artifacts(
        storage,
        transformed["silver_artifacts"],
        persist_reports=False,
    )

    assert report.valid is False
    assert any(issue.rule == "duplicate_grain" for issue in report.issues)
    assert any(issue.rule == "row_count_integrity" for issue in report.issues)


def test_known_invalid_source_percentage_is_a_non_blocking_warning() -> None:
    transformed, storage = _transform_year(2020, fp_percentage="#########")

    report = validate_silver_artifacts(
        storage,
        transformed["silver_artifacts"],
        persist_reports=False,
    )

    assert report.valid is True
    assert report.error_count == 0
    assert report.warning_count == 2
    assert {issue.rule for issue in report.issues} == {"source_numeric_null"}


def test_cross_dataset_orphan_is_reported() -> None:
    transformed, storage = _transform_year(2024)
    artifact = next(
        item
        for item in transformed["silver_artifacts"]
        if item["dataset_name"] == "fefc_cor_raca"
    )
    _change_first_row(storage, artifact["output_key"], sigla_partido="OUTRO")

    report = validate_silver_artifacts(
        storage,
        transformed["silver_artifacts"],
        persist_reports=False,
    )

    assert report.valid is False
    assert any(issue.rule == "cross_dataset_integrity" for issue in report.issues)


def test_financial_outliers_are_non_blocking_warnings() -> None:
    transformed, storage = _transform_year(2024)
    artifact = next(
        item
        for item in transformed["silver_artifacts"]
        if item["dataset_name"] == "fp_genero"
    )
    _change_first_row(
        storage,
        artifact["output_key"],
        percentual_valor_fp_genero="125.50",
        valor_total_recebido_fp="-10.25",
    )

    report = validate_silver_artifacts(
        storage,
        transformed["silver_artifacts"],
        persist_reports=False,
    )

    assert report.valid is True
    assert report.warning_count == 2
    assert {issue.rule for issue in report.issues} == {
        "negative_financial_value",
        "percentage_outlier",
    }
