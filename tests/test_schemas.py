"""Schema validation tests."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from app.models import Currency, PaymentStatus
from app.schemas import PaymentCreate, PaymentDetail


def _payload(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "amount": "10.00",
        "currency": "USD",
        "description": "test",
        "metadata": {"order": 1},
        "webhook_url": "https://example.com/hook",
    }
    data.update(overrides)
    return data


def test_payment_create_ok() -> None:
    payload = PaymentCreate(**_payload())
    assert payload.amount == Decimal("10.00")
    assert payload.currency is Currency.USD


@pytest.mark.parametrize("amount", ["0", "-1", "0.001"])
def test_payment_create_rejects_bad_amount(amount: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**_payload(amount=amount))


def test_payment_create_rejects_bad_currency() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**_payload(currency="GBP"))


def test_payment_create_rejects_bad_webhook() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**_payload(webhook_url="not-a-url"))


def test_payment_detail_serialises_metadata() -> None:
    detail = PaymentDetail(
        id=uuid.uuid4(),
        amount=Decimal("5.00"),
        currency=Currency.RUB,
        description=None,
        metadata={"a": 1},
        status=PaymentStatus.pending,
        idempotency_key="k",
        webhook_url="https://example.com/hook",
        created_at="2026-01-01T00:00:00Z",
        processed_at=None,
    )
    dumped = detail.model_dump(mode="json")
    assert dumped["metadata"] == {"a": 1}
    assert dumped["amount"] == "5.00"
