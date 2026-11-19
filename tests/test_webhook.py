"""Webhook delivery tests."""

from __future__ import annotations

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import app.webhook as webhook
from app.models import Currency, Payment, PaymentStatus, utcnow
from app.webhook import (
    WebhookDeliveryError,
    build_payload,
    send_webhook,
    sign_payload,
)


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


def test_sign_payload_is_stable() -> None:
    body = b'{"a":1}'
    assert sign_payload(body, "secret") == sign_payload(body, "secret")
    assert sign_payload(body, "secret").startswith("sha256=")
    assert sign_payload(body, "other") != sign_payload(body, "secret")


async def test_send_webhook_sends_idempotency_and_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(webhook.settings, "webhook_secret", "topsecret")
    payment = _payment()
    client = AsyncMock()
    client.post.return_value = httpx.Response(
        200, request=httpx.Request("POST", "http://x")
    )

    with patch("app.webhook.httpx.AsyncClient") as client_cls:
        client_cls.return_value.__aenter__.return_value = client
        await send_webhook(payment)

    _, kwargs = client.post.call_args
    headers = kwargs["headers"]
    body = kwargs["content"]
    assert headers["X-Webhook-Id"] == f"{payment.id}:succeeded"
    assert headers["X-Webhook-Signature"] == sign_payload(body, "topsecret")

