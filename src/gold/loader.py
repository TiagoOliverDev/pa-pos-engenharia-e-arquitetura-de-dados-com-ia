"""Acesso ao data warehouse local em PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

try:
    import pandas as pd
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
except ImportError:  # pragma: no cover - fallback for minimal local environments
    pd = None
    create_engine = None
    text = None
    Engine = Any

from src.config import Settings, get_settings
from src.utils.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass(slots=True)
class PostgresWarehouse:
    """Centraliza a conexao e as operacoes da camada Gold no PostgreSQL."""

    settings: Settings | None = field(default=None)
    _engine: Engine | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Recebe as configuracoes do dataclass e inicializa a engine; nao retorna valor."""

        if self.settings is None:
            self.settings = get_settings()
        if create_engine is not None:
            self._engine = create_engine(
                self.settings.postgres_sqlalchemy_url,
                pool_pre_ping=True,
            )

    @property
    def engine(self) -> Engine:
        """Nao recebe parametros adicionais e retorna a engine SQLAlchemy disponivel."""

        if self._engine is None:
            raise RuntimeError(
                "SQLAlchemy nao esta instalado neste ambiente; a camada Gold exige a dependencia."
            )
        return self._engine

    def healthcheck(self) -> bool:
        """Nao recebe parametros, consulta o banco e retorna se ele esta acessivel."""

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
        """Recebe um DataFrame e opcoes, grava-o no PostgreSQL e nao retorna valor."""

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
        """Recebe SQL e parametros opcionais e os executa em transacao; nao retorna valor."""

        with self.engine.begin() as connection:
            connection.execute(text(sql), params or {})
