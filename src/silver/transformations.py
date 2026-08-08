"""Transformacoes iniciais da camada Silver."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from src.utils.logging import get_logger

LOGGER = get_logger(__name__)


def normalize_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return a shallow copy of the records for now.

    The real cleaning rules will be added in later sprints.
    """

    LOGGER.info("Transformacao Silver ainda em modo placeholder.")
    return [dict(record) for record in records]

