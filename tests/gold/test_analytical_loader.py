from __future__ import annotations

import pytest

from src.gold.analytical_loader import WarehouseLoadError
from src.gold.analytical_loader import _fact_insert_sql
from src.gold.analytical_loader import _quote_identifier
from src.gold.analytical_loader import load_silver_artifacts
from tests.silver.test_transformations import _transform_year


class _FakeCursor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []
        self.rowcount = 0
        self.stage_count = 0
        self._fetchone = (None,)

    def execute(self, sql, params=None) -> None:
        normalized = " ".join(str(sql).split())
        self.statements.append((normalized, params))
        if "to_regclass" in normalized:
            self._fetchone = ("dw.carga_arquivo",)
        elif normalized.startswith("SELECT COUNT(*)"):
            self._fetchone = (self.stage_count,)
        elif normalized.startswith("DELETE FROM dw.fato"):
            self.rowcount = 0
        elif normalized.startswith("INSERT INTO dw.fato"):
            self.rowcount = self.stage_count
        else:
            self.rowcount = 1

    def copy_expert(self, sql, stream) -> None:
        self.statements.append((sql, None))
        self.stage_count = max(len(stream.read().splitlines()) - 1, 0)

    def fetchone(self):
        return self._fetchone

    def close(self) -> None:
        return None


class _FakeRawConnection:
    def __init__(self) -> None:
        self.cursor_instance = _FakeCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class _FakeEngine:
    def __init__(self, connection: _FakeRawConnection) -> None:
        self.connection = connection

    def raw_connection(self) -> _FakeRawConnection:
        return self.connection


class _FakeWarehouse:
    def __init__(self, connection: _FakeRawConnection) -> None:
        self.engine = _FakeEngine(connection)


def test_fact_sql_targets_parent_tables_and_expected_dimensions() -> None:
    fefc_sql = _fact_insert_sql("fefc_cor_raca", "stage_fefc_cor_raca")
    fp_sql = _fact_insert_sql("fp_genero", "stage_fp_genero")

    assert "INSERT INTO dw.fato_fefc_cor_raca" in fefc_sql
    assert "JOIN dw.dim_cor_raca" in fefc_sql
    assert "INSERT INTO dw.fato_fp_genero" in fp_sql
    assert "JOIN dw.dim_localidade" in fp_sql
    assert "l.chave_natural" in fp_sql


def test_identifier_validation_rejects_sql_injection() -> None:
    with pytest.raises(WarehouseLoadError, match="invalido"):
        _quote_identifier("column; DROP TABLE dw.dim_eleicao")


def test_load_silver_artifact_commits_transaction() -> None:
    transformed, storage = _transform_year(2024)
    artifact = next(
        item
        for item in transformed["silver_artifacts"]
        if item["dataset_name"] == "fefc_genero"
    )
    connection = _FakeRawConnection()

    report = load_silver_artifacts(
        storage,
        [artifact],
        _FakeWarehouse(connection),
    )

    assert report.artifact_count == 1
    assert report.staged_rows == 1
    assert report.inserted_rows == 1
    assert report.deleted_rows == 0
    assert connection.committed is True
    assert connection.rolled_back is False
    assert connection.closed is True


def test_load_rolls_back_when_manifest_count_diverges() -> None:
    transformed, storage = _transform_year(2024)
    artifact = next(
        item
        for item in transformed["silver_artifacts"]
        if item["dataset_name"] == "fefc_genero"
    )
    invalid_artifact = {**artifact, "row_count": 2}
    connection = _FakeRawConnection()

    with pytest.raises(WarehouseLoadError, match="Contagem"):
        load_silver_artifacts(
            storage,
            [invalid_artifact],
            _FakeWarehouse(connection),
        )

    assert connection.committed is False
    assert connection.rolled_back is True
    assert connection.closed is True
