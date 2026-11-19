"""Доставка webhook с собственными повторами/backoff.

Уведомления отправляются с детерминированным JSON-телом, чтобы получатель мог
проверить необязательную HMAC-подпись по точным байтам. Каждый запрос также
несёт заголовок ``X-Webhook-Id``, стабильный для пары «платёж/статус», что
позволяет получателю дедуплицировать повторные доставки.
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
    """Возбуждается, когда webhook не удалось доставить после всех локальных попыток."""


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
    """Каноническое JSON-кодирование payload'а webhook."""

    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sign_payload(body: bytes, secret: str) -> str:
    """Возвращает HMAC-подпись ``sha256=<hex>`` для ``body``."""

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
    """Отправляет результат платежа методом POST на ``payment.webhook_url``.

    Делает ``webhook_max_retries`` попыток с линейной задержкой. Если все
    попытки провалились, ошибка пробрасывается дальше, чтобы включился retry
    на уровне брокера.
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
            logger.info("Webhook доставлен для платежа %s (попытка %s)", payment.id, attempt)
            return
        except Exception as exc:  # noqa: BLE001 - повторяется ниже
            last_error = exc
            logger.warning(
                "Попытка webhook %s/%s для платежа %s провалилась: %s",
                attempt,
                settings.webhook_max_retries,
                payment.id,
                exc,
            )
            if attempt < settings.webhook_max_retries:
                await asyncio.sleep(settings.webhook_retry_delay * attempt)

    raise WebhookDeliveryError(
        f"webhook для платежа {payment.id} не доставлен после "
        f"{settings.webhook_max_retries} попыток"
    ) from last_error
