"""Webhook delivery tests."""

from __future__ import annotations

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.models import Currency, Payment, PaymentStatus, utcnow
from app.webhook import WebhookDeliveryError, build_payload, send_webhook


def _payment() -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="order",
        status=PaymentStatus.succeeded,
        processed_at=utcnow(),
        webhook_url="https://example.com/hook",
    )


def test_build_payload() -> None:
    payment = _payment()
    payload = build_payload(payment)
    assert payload["payment_id"] == str(payment.id)
    assert payload["status"] == "succeeded"
    assert payload["amount"] == "10.00"
    assert payload["currency"] == "USD"


async def test_send_webhook_retries_then_raises() -> None:
    payment = _payment()
    client = AsyncMock()
    client.post.side_effect = httpx.ConnectError("nope")

    with (
        patch("app.webhook.httpx.AsyncClient") as client_cls,
        patch("app.webhook.asyncio.sleep", new=AsyncMock()) as sleep,
    ):
        client_cls.return_value.__aenter__.return_value = client
        with pytest.raises(WebhookDeliveryError):
            await send_webhook(payment)

    assert client.post.await_count == 3
    assert sleep.await_count == 2


async def test_send_webhook_success_without_retry() -> None:
    payment = _payment()
    client = AsyncMock()
    client.post.return_value = httpx.Response(200, request=httpx.Request("POST", "http://x"))

    with patch("app.webhook.httpx.AsyncClient") as client_cls:
        client_cls.return_value.__aenter__.return_value = client
        await send_webhook(payment)

    assert client.post.await_count == 1
