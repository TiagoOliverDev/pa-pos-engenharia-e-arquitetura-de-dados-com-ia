"""Executor simples de migrations SQL do Data Warehouse."""

from __future__ import annotations

import argparse
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from sqlalchemy import text

from src.config import Settings, get_settings
from src.gold.loader import PostgresWarehouse
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)
_MIGRATION_PATTERN = re.compile(r"^(?P<version>\d+)_(?P<name>[a-z0-9_]+)\.sql$")


@dataclass(frozen=True, slots=True)
class Migration:
    """Representa uma migration local com versao, nome, arquivo e checksum."""

    version: int
    name: str
    path: Path
    checksum: str


@dataclass(frozen=True, slots=True)
class MigrationStatus:
    """Representa uma migration e informa se ela ja foi aplicada ao banco."""

    version: int
    name: str
    applied: bool


def discover_migrations(migrations_dir: Path) -> tuple[Migration, ...]:
    """Recebe o diretorio, valida os arquivos e retorna as migrations ordenadas."""

    migrations: list[Migration] = []
    versions: set[int] = set()
    if not migrations_dir.exists():
        raise FileNotFoundError(f"Diretorio de migrations nao existe: {migrations_dir}")

    for path in sorted(migrations_dir.glob("*.sql")):
        match = _MIGRATION_PATTERN.fullmatch(path.name)
        if match is None:
            raise ValueError(f"Nome de migration invalido: {path.name}")
        version = int(match.group("version"))
        if version in versions:
            raise ValueError(f"Versao de migration duplicada: {version}")
        versions.add(version)
        payload = path.read_bytes()
        migrations.append(
            Migration(
                version=version,
                name=match.group("name"),
                path=path,
                checksum=hashlib.sha256(payload).hexdigest(),
            )
        )
    return tuple(sorted(migrations, key=lambda item: item.version))


def _migration_dir(settings: Settings, migrations_dir: Path | None) -> Path:
    """Recebe configuracoes e diretorio opcional e retorna o caminho efetivo."""

    return migrations_dir or settings.project_root / "migrations"


def _ensure_history_table(connection) -> None:
    """Recebe uma conexao e garante a tabela de historico; nao retorna valor."""

    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS public.dw_schema_migrations (
                version INTEGER PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                checksum CHAR(64) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )


def migration_status(
    settings: Settings | None = None,
    migrations_dir: Path | None = None,
) -> tuple[MigrationStatus, ...]:
    """Recebe configuracoes e diretorio opcionais e retorna o status das migrations."""

    resolved_settings = settings or get_settings()
    migrations = discover_migrations(_migration_dir(resolved_settings, migrations_dir))
    warehouse = PostgresWarehouse(resolved_settings)
    with warehouse.engine.begin() as connection:
        _ensure_history_table(connection)
        rows = connection.execute(
            text("SELECT version, checksum FROM public.dw_schema_migrations")
        )
        applied = {int(row.version): str(row.checksum) for row in rows}

    for migration in migrations:
        applied_checksum = applied.get(migration.version)
        if applied_checksum is not None and applied_checksum != migration.checksum:
            raise RuntimeError(
                f"Migration {migration.version} foi alterada depois de aplicada."
            )
    return tuple(
        MigrationStatus(
            version=migration.version,
            name=migration.name,
            applied=migration.version in applied,
        )
        for migration in migrations
    )


def apply_migrations(
    settings: Settings | None = None,
    migrations_dir: Path | None = None,
) -> tuple[Migration, ...]:
    """Recebe configuracoes e diretorio opcionais e retorna as migrations aplicadas."""

    resolved_settings = settings or get_settings()
    migrations = discover_migrations(_migration_dir(resolved_settings, migrations_dir))
    warehouse = PostgresWarehouse(resolved_settings)
    applied_now: list[Migration] = []

    with warehouse.engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(hashtext('fefc_dw_migrations'))"))
        _ensure_history_table(connection)
        rows = connection.execute(
            text("SELECT version, checksum FROM public.dw_schema_migrations")
        )
        applied = {int(row.version): str(row.checksum) for row in rows}

        for migration in migrations:
            applied_checksum = applied.get(migration.version)
            if applied_checksum is not None:
                if applied_checksum != migration.checksum:
                    raise RuntimeError(
                        f"Migration {migration.version} foi alterada depois de aplicada."
                    )
                continue

            LOGGER.info("Aplicando migration %03d_%s.", migration.version, migration.name)
            connection.exec_driver_sql(migration.path.read_text(encoding="utf-8"))
            connection.execute(
                text(
                    """
                    INSERT INTO public.dw_schema_migrations (version, name, checksum)
                    VALUES (:version, :name, :checksum)
                    """
                ),
                {
                    "version": migration.version,
                    "name": migration.name,
                    "checksum": migration.checksum,
                },
            )
            applied_now.append(migration)

    return tuple(applied_now)


def _build_parser() -> argparse.ArgumentParser:
    """Nao recebe parametros e retorna o parser dos comandos manuais de migration."""

    parser = argparse.ArgumentParser(description="Migrations do Data Warehouse FEFC")
    parser.add_argument("command", choices=("up", "status"))
    parser.add_argument("--migrations-dir", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Recebe argumentos opcionais, executa o comando de migration e retorna o codigo de saida."""

    args = _build_parser().parse_args(argv)
    if args.command == "up":
        applied = apply_migrations(migrations_dir=args.migrations_dir)
        if applied:
            for migration in applied:
                print(f"APPLIED {migration.version:03d}_{migration.name}")
        else:
            print("Nenhuma migration pendente.")
        return 0

    for status in migration_status(migrations_dir=args.migrations_dir):
        state = "APPLIED" if status.applied else "PENDING"
        print(f"{state} {status.version:03d}_{status.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
