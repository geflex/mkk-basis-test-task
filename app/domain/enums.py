"""Доменные перечисления, не зависящие от ORM и API."""

from __future__ import annotations

import enum


class PaymentStatus(str, enum.Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Currency(str, enum.Enum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


def enum_values(enum_type: type[enum.Enum]) -> list[str]:
    """Возвращает *значения* членов enum для хранения в БД."""

    return [member.value for member in enum_type]