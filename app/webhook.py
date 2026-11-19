"""Webhook delivery with its own retry/backoff.

Notifications are sent as a deterministic JSON body so that an optional HMAC
signature over the exact bytes can be verified by the receiver. Every request
also carries an ``X-Webhook-Id`` header that is stable for a given
payment/status, letting the receiver deduplicate redeliveries.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging

import httpx

from app.config import settings
from app.models import Payment

logger = logging.getLogger(__name__)


class WebhookDeliveryError(RuntimeError):
    """Raised when a webhook could not be delivered after all local retries."""


def build_payload(payment: Payment) -> dict:
    return {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "description": payment.description,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }


def serialize_payload(payload: dict) -> bytes:
    """Canonical JSON encoding of a webhook payload."""

    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sign_payload(body: bytes, secret: str) -> str:
    """Return the ``sha256=<hex>`` HMAC signature for ``body``."""

    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def build_headers(payment: Payment, body: bytes) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Id": f"{payment.id}:{payment.status.value}",
    }
    if settings.webhook_secret:
        headers["X-Webhook-Signature"] = sign_payload(body, settings.webhook_secret)
    return headers


async def send_webhook(payment: Payment) -> None:
    """POST the payment result to ``payment.webhook_url``.

    Retries ``webhook_max_retries`` times with a linear backoff. If every
    attempt fails the error is re-raised so the broker-level retry can kick in.
    """

    body = serialize_payload(build_payload(payment))
    headers = build_headers(payment, body)
    last_error: Exception | None = None

    for attempt in range(1, settings.webhook_max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=settings.webhook_timeout) as client:
                response = await client.post(
                    payment.webhook_url, content=body, headers=headers
                )
                response.raise_for_status()
            logger.info("Webhook delivered for payment %s (attempt %s)", payment.id, attempt)
            return
        except Exception as exc:  # noqa: BLE001 - retried below
            last_error = exc
            logger.warning(
                "Webhook attempt %s/%s failed for payment %s: %s",
                attempt,
                settings.webhook_max_retries,
                payment.id,
                exc,
            )
            if attempt < settings.webhook_max_retries:
                await asyncio.sleep(settings.webhook_retry_delay * attempt)

    raise WebhookDeliveryError(
        f"webhook for payment {payment.id} failed after "
        f"{settings.webhook_max_retries} attempts"
    ) from last_error
