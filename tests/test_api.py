"""Тесты проверки idempotency-запроса в сервисе платежей."""

from __future__ import annotations

import uuid
from decimal import Decimal

from app.db.models import Payment
from app.domain.contracts import PaymentCreate
from app.domain.enums import Currency
from app.services.payments import PaymentService


def _payment() -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="order",
        meta={"order": 1},
        idempotency_key="key",
        webhook_url="https://example.com/hook",
    )


def _payload(**overrides: object) -> PaymentCreate:
    data: dict[str, object] = {
        "amount": Decimal("10.00"),
        "currency": Currency.USD,
        "description": "order",
        "metadata": {"order": 1},
        "webhook_url": "https://example.com/hook",
    }
    data.update(overrides)
    return PaymentCreate(**data)


def test_matches_identical_request() -> None:
    assert PaymentService.matches(_payment(), _payload()) is True


def test_mismatch_on_amount() -> None:
    assert PaymentService.matches(_payment(), _payload(amount=Decimal("20.00"))) is False


def test_mismatch_on_metadata() -> None:
    assert PaymentService.matches(_payment(), _payload(metadata={"order": 2})) is False


def test_mismatch_on_webhook() -> None:
    assert (
        PaymentService.matches(
            _payment(), _payload(webhook_url="https://example.com/other")
        )
        is False
    )