"""Тесты создания платежа и идемпотентности в сервисном слое (без реальной БД)."""

from __future__ import annotations

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from app.db.models import Payment
from app.domain.contracts import PaymentCreate
from app.domain.enums import Currency
from app.services.payments import PaymentService


def _session() -> AsyncMock:
    session = AsyncMock()
    # ``add_all`` синхронный у AsyncSession; мок тоже делаем синхронным.
    session.add_all = MagicMock()
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
    service = PaymentService(session)
    with patch(
        "app.services.payments.get_by_idempotency_key",
        new=AsyncMock(return_value=None),
    ):
        payment, created = await service.create(_payload(), "key")

    assert created is True
    assert payment.id is not None
    session.add_all.assert_called_once()
    session.commit.assert_awaited_once()


async def test_create_payment_returns_existing_without_insert() -> None:
    session = _session()
    service = PaymentService(session)
    existing = _existing()
    with patch(
        "app.services.payments.get_by_idempotency_key",
        new=AsyncMock(return_value=existing),
    ):
        payment, created = await service.create(_payload(), "key")

    assert payment is existing
    assert created is False
    session.add_all.assert_not_called()
    session.commit.assert_not_awaited()


async def test_create_payment_recovers_from_race_on_commit() -> None:
    """Конкурентный запрос, закоммитившийся первым, не должен приводить к 500."""

    session = _session()
    session.commit.side_effect = IntegrityError("INSERT", {}, Exception("duplicate"))
    existing = _existing()
    lookup = AsyncMock(side_effect=[None, existing])
    service = PaymentService(session)

    with patch("app.services.payments.get_by_idempotency_key", new=lookup):
        payment, created = await service.create(_payload(), "key")

    assert payment is existing
    assert created is False
    session.rollback.assert_awaited_once()
    assert lookup.await_count == 2


async def test_create_payment_reraises_when_existing_not_found() -> None:
    session = _session()
    session.commit.side_effect = IntegrityError("INSERT", {}, Exception("duplicate"))
    service = PaymentService(session)

    with patch(
        "app.services.payments.get_by_idempotency_key",
        new=AsyncMock(return_value=None),
    ):
        with pytest.raises(IntegrityError):
            await service.create(_payload(), "key")

    session.rollback.assert_awaited_once()