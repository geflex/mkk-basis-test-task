"""Тесты доставки webhook."""

from __future__ import annotations

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import app.services.webhook as webhook
from app.core.config import WebhookSettings
from app.core.net import UnsafeWebhookTarget
from app.db.models import Payment
from app.domain.enums import Currency, PaymentStatus
from app.domain.time import utcnow
from app.services.webhook import (
    WebhookDeliveryError,
    WebhookNotification,
    WebhookSender,
    build_payload,
    sign_payload,
)
from pydantic import SecretStr


def _notification() -> WebhookNotification:
    return WebhookNotification(
        payment_id=uuid.uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="order",
        processed_at=utcnow(),
        webhook_url="https://example.com/hook",
    )


def _settings(**overrides: object) -> WebhookSettings:
    data: dict[str, object] = {
        "timeout": 3,
        "max_retries": 3,
        "retry_delay": 1.0,
        "allow_private_hosts": False,
    }
    data.update(overrides)
    return WebhookSettings(**data)  # type: ignore[arg-type]


def _response(status_code: int = 200, content: bytes = b'{"ok":true}') -> httpx.Response:
    return httpx.Response(
        status_code,
        content=content,
        request=httpx.Request("POST", "https://example.com/hook"),
    )


def test_build_payload_from_model() -> None:
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="order",
        status=PaymentStatus.SUCCEEDED,
        processed_at=utcnow(),
        webhook_url="https://example.com/hook",
    )
    payload = build_payload(WebhookNotification.from_payment(payment))
    assert payload["payment_id"] == str(payment.id)
    assert payload["status"] == "succeeded"
    assert payload["amount"] == "10.00"
    assert payload["currency"] == "USD"


async def test_send_webhook_retries_then_raises() -> None:
    sender = WebhookSender(settings=_settings(), client=AsyncMock())
    sender.client.post.side_effect = httpx.ConnectError("nope")

    with (
        patch.object(
            webhook, "validate_webhook_target", new=AsyncMock()
        ),
        patch.object(webhook.asyncio, "sleep", new=AsyncMock()) as sleep,
    ):
        with pytest.raises(WebhookDeliveryError):
            await sender.send(_notification())

    assert sender.client.post.await_count == 3
    assert sleep.await_count == 2


async def test_send_webhook_success_without_retry() -> None:
    client = AsyncMock()
    client.post.return_value = _response()
    sender = WebhookSender(settings=_settings(), client=client)

    with patch.object(webhook, "validate_webhook_target", new=AsyncMock()):
        await sender.send(_notification())

    assert client.post.await_count == 1


async def test_send_webhook_rejects_unsafe_target_before_request() -> None:
    client = AsyncMock()
    sender = WebhookSender(settings=_settings(), client=client)

    with patch.object(
        webhook,
        "validate_webhook_target",
        new=AsyncMock(side_effect=UnsafeWebhookTarget("bad")),
    ):
        with pytest.raises(UnsafeWebhookTarget):
            await sender.send(_notification())

    client.post.assert_not_awaited()


def test_sign_payload_is_stable() -> None:
    body = b'{"a":1}'
    assert sign_payload(body, "secret") == sign_payload(body, "secret")
    assert sign_payload(body, "secret").startswith("sha256=")
    assert sign_payload(body, "other") != sign_payload(body, "secret")


async def test_send_webhook_sends_idempotency_and_signature() -> None:
    client = AsyncMock()
    client.post.return_value = _response()
    sender = WebhookSender(
        settings=_settings(secret=SecretStr("topsecret")), client=client
    )
    notification = _notification()

    with patch.object(webhook, "validate_webhook_target", new=AsyncMock()):
        await sender.send(notification)

    _, kwargs = client.post.call_args
    headers = kwargs["headers"]
    body = kwargs["content"]
    assert headers["X-Webhook-Id"] == f"{notification.payment_id}:succeeded"
    assert headers["X-Webhook-Signature"] == sign_payload(body, "topsecret")