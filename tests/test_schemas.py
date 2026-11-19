"""Тесты валидации схем."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

import app.schemas as schemas
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


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/hook",
        "http://localhost./hook",
        "http://127.0.0.1/hook",
        "http://10.0.0.5/hook",
        "http://192.168.1.10/hook",
        "http://169.254.169.254/latest/meta-data",
        "http://0.0.0.0/hook",
        "http://[::1]/hook",
        "http://payments.internal/hook",
        "http://host.local/hook",
    ],
)
def test_payment_create_rejects_private_webhook(url: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**_payload(webhook_url=url))


def test_payment_create_allows_private_webhook_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(schemas.settings, "allow_private_webhook_hosts", True)
    payload = PaymentCreate(**_payload(webhook_url="http://localhost:9000/webhook"))
    assert payload.webhook_url.host == "localhost"


def test_payment_detail_serialises_metadata() -> None:
    detail = PaymentDetail(
        id=uuid.uuid4(),
        amount=Decimal("5.00"),
        currency=Currency.RUB,
        description=None,
        metadata={"a": 1},
        status=PaymentStatus.PENDING,
        idempotency_key="k",
        webhook_url="https://example.com/hook",
        created_at="2026-01-01T00:00:00Z",
        processed_at=None,
    )
    dumped = detail.model_dump(mode="json")
    assert dumped["metadata"] == {"a": 1}
    assert dumped["amount"] == "5.00"
