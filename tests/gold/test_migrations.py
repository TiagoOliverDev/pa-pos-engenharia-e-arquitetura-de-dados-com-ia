from __future__ import annotations

from pathlib import Path

import pytest

from src.gold.migrations import discover_migrations


def _write_migration(directory: Path, filename: str, sql: str = "SELECT 1;") -> Path:
    path = directory / filename
    path.write_text(sql, encoding="utf-8")
    return path


def test_discover_migrations_orders_versions_and_calculates_checksum(tmp_path) -> None:
    _write_migration(tmp_path, "002_second.sql", "SELECT 2;")
    _write_migration(tmp_path, "001_first.sql", "SELECT 1;")

    migrations = discover_migrations(tmp_path)

    assert [migration.version for migration in migrations] == [1, 2]
    assert [migration.name for migration in migrations] == ["first", "second"]
    assert all(len(migration.checksum) == 64 for migration in migrations)


def test_discover_migrations_rejects_duplicate_versions(tmp_path) -> None:
    _write_migration(tmp_path, "001_first.sql")
    _write_migration(tmp_path, "001_duplicate.sql")

    with pytest.raises(ValueError, match="duplicada"):
        discover_migrations(tmp_path)


def test_discover_migrations_rejects_invalid_filename(tmp_path) -> None:
    _write_migration(tmp_path, "create_model.sql")

    with pytest.raises(ValueError, match="invalido"):
        discover_migrations(tmp_path)


def test_discover_migrations_requires_directory(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        discover_migrations(tmp_path / "missing")
