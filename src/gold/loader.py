"""Acesso ao data warehouse local em PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import Settings, get_settings
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass(slots=True)
class PostgresWarehouse:
    """Thin wrapper around SQLAlchemy for the future Gold layer."""

    settings: Settings | None = field(default=None)

    def __post_init__(self) -> None:
        if self.settings is None:
            self.settings = get_settings()
        self._engine: Engine = create_engine(
            self.settings.postgres_sqlalchemy_url,
            pool_pre_ping=True,
        )

    @property
    def engine(self) -> Engine:
        return self._engine

    def healthcheck(self) -> bool:
        with self.engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True

    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        schema: str | None = None,
        if_exists: str = "append",
        index: bool = False,
    ) -> None:
        LOGGER.info("Carga Gold placeholder para a tabela %s.", table_name)
        dataframe.to_sql(
            table_name,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=index,
            method="multi",
        )

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        with self.engine.begin() as connection:
            connection.execute(text(sql), params or {})
