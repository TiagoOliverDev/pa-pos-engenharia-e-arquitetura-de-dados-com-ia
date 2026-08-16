"""Configuracao padrao de logging."""

from __future__ import annotations

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """Recebe o nivel desejado e configura o logger raiz; nao retorna valor."""

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def get_logger(name: str) -> logging.Logger:
    """Recebe o nome do modulo e retorna seu logger com a configuracao padrao."""

    configure_logging()
    return logging.getLogger(name)
