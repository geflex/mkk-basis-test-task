"""Тесты идемпотентности на уровне репозитория (без реальной БД)."""

from __future__ import annotations

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import Currency, Payment
from app.repositories import create_payment
from app.schemas import PaymentCreate


def _session() -> AsyncMock:
    session = AsyncMock()
    # ``add`` синхронный у AsyncSession; мок тоже делаем синхронным.
    session.add = MagicMock()
    return session


def _payload() -> PaymentCreate:
    return PaymentCreate(
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="order",
        metadata={"order": 1},
        webhook_url="https://example.com/hook",
    )


def _existing() -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        idempotency_key="key",
        webhook_url="https://example.com/hook",
    )


async def test_create_payment_returns_new_payment() -> None:
    session = _session()
    with patch(
        "app.repositories.get_by_idempotency_key", new=AsyncMock(return_value=None)
    ):
        payment, created = await create_payment(session, _payload(), "key")

    assert created is True
    assert payment.id is not None
    session.commit.assert_awaited_once()


async def test_create_payment_returns_existing_without_insert() -> None:
    session = _session()
    existing = _existing()
    with patch(
        "app.repositories.get_by_idempotency_key",
        new=AsyncMock(return_value=existing),
    ):
        payment, created = await create_payment(session, _payload(), "key")

    assert payment is existing
    assert created is False
    session.commit.assert_not_awaited()


async def test_create_payment_recovers_from_race_on_commit() -> None:
    """Конкурентный запрос, закоммитившийся первым, не должен приводить к 500."""

    session = _session()
    session.commit.side_effect = IntegrityError("INSERT", {}, Exception("duplicate"))
    existing = _existing()
    lookup = AsyncMock(side_effect=[None, existing])

    with patch("app.repositories.get_by_idempotency_key", new=lookup):
        payment, created = await create_payment(session, _payload(), "key")

    assert payment is existing
    assert created is False
    session.rollback.assert_awaited_once()
    assert lookup.await_count == 2


async def test_create_payment_reraises_when_existing_not_found() -> None:
    session = _session()
    session.commit.side_effect = IntegrityError("INSERT", {}, Exception("duplicate"))

    with patch(
        "app.repositories.get_by_idempotency_key", new=AsyncMock(return_value=None)
    ):
        with pytest.raises(IntegrityError):
            await create_payment(session, _payload(), "key")

    session.rollback.assert_awaited_once()
