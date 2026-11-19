"""Webhook delivery with its own retry/backoff."""

from __future__ import annotations

import asyncio
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


async def send_webhook(payment: Payment) -> None:
    """POST the payment result to ``payment.webhook_url``.

    Retries ``webhook_max_retries`` times with a linear backoff. If every
    attempt fails the error is re-raised so the broker-level retry can kick in.
    """

    payload = build_payload(payment)
    last_error: Exception | None = None

    for attempt in range(1, settings.webhook_max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=settings.webhook_timeout) as client:
                response = await client.post(payment.webhook_url, json=payload)
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
