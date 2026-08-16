"""Validacoes basicas e genericas de dados."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from src.utils.logging import get_logger

LOGGER = get_logger(__name__)


class ValidationError(ValueError):
    """Indica que um conjunto de dados violou uma regra basica de qualidade."""


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Resume a validacao com quantidade de registros, duplicidades e campos nulos."""

    valid: bool
    record_count: int
    duplicate_count: int
    missing_fields: tuple[str, ...]
    messages: tuple[str, ...]


def ensure_not_empty(records: Sequence[Mapping[str, Any]], allow_empty: bool = False) -> None:
    """Recebe registros e a permissao de vazio e valida se ha dados; nao retorna valor."""

    if records or allow_empty:
        return
    raise ValidationError("O conjunto de registros esta vazio.")


def ensure_positive_fields(
    records: Sequence[Mapping[str, Any]],
    fields: Sequence[str],
) -> None:
    """Recebe registros e campos, valida valores positivos e nao retorna valor."""

    for record_index, record in enumerate(records):
        for field in fields:
            value = record.get(field)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                raise ValidationError(
                    f"Campo {field} deve ser numerico e maior que zero "
                    f"no registro {record_index}. Valor recebido: {value!r}"
                )


def find_duplicate_rows(
    records: Sequence[Mapping[str, Any]],
    key_fields: Sequence[str],
) -> list[dict[str, Any]]:
    """Recebe registros e campos-chave e retorna as linhas com chaves duplicadas."""

    seen: set[tuple[Any, ...]] = set()
    duplicates: list[dict[str, Any]] = []
    for record in records:
        key = tuple(record.get(field) for field in key_fields)
        if key in seen:
            duplicates.append(dict(record))
        else:
            seen.add(key)
    return duplicates


def validate_records(
    records: Sequence[Mapping[str, Any]],
    required_fields: Sequence[str] = (),
    unique_fields: Sequence[str] = (),
    allow_empty: bool = False,
) -> ValidationReport:
    """Recebe registros e regras genericas e retorna o relatorio de validacao."""

    ensure_not_empty(records, allow_empty=allow_empty)
    missing_fields: set[str] = set()
    messages: list[str] = []

    for field in required_fields:
        if any(field not in record or record.get(field) in (None, "") for record in records):
            missing_fields.add(field)
            messages.append(f"Campo obrigatorio ausente ou vazio: {field}")

    duplicate_count = len(find_duplicate_rows(records, unique_fields)) if unique_fields else 0
    if duplicate_count:
        messages.append(f"Encontradas {duplicate_count} linhas duplicadas.")

    valid = not missing_fields and duplicate_count == 0
    if allow_empty and not records:
        valid = True

    LOGGER.info(
        "Validacao concluida: valid=%s, registros=%s, duplicados=%s",
        valid,
        len(records),
        duplicate_count,
    )

    return ValidationReport(
        valid=valid,
        record_count=len(records),
        duplicate_count=duplicate_count,
        missing_fields=tuple(sorted(missing_fields)),
        messages=tuple(messages),
    )
