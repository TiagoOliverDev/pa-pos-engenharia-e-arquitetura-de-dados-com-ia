"""Configuracao padrao de logging."""

from __future__ import annotations

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the root logger once."""

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def get_logger(name: str) -> logging.Logger:
    """Return a module logger after ensuring the root logger is ready."""

    configure_logging()
    return logging.getLogger(name)

