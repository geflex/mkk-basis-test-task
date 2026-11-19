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
import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import httpx

from app.core.config import WebhookSettings, get_webhook_settings
from app.core.net import validate_webhook_target
from app.db.models import Payment
from app.domain.enums import Currency, PaymentStatus

logger = logging.getLogger(__name__)


class WebhookDeliveryError(RuntimeError):
    """Webhook не удалось доставить после всех локальных попыток."""


@dataclass(frozen=True, slots=True)
class WebhookNotification:
    """Снимок платежа, достаточный для построения webhook-уведомления.

    Даёт возможность отправлять уведомление уже после закрытия DB-сессии и не
    держать ORM-объект живым.
    """

    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str | None
    processed_at: datetime | None
    webhook_url: str

    @classmethod
    def from_payment(cls, payment: Payment) -> "WebhookNotification":
        return cls(
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            processed_at=payment.processed_at,
            webhook_url=payment.webhook_url,
        )


def build_payload(notification: WebhookNotification) -> dict:
    return {
        "payment_id": str(notification.payment_id),
        "status": notification.status.value,
        "amount": str(notification.amount),
        "currency": notification.currency.value,
        "description": notification.description,
        "processed_at": (
            notification.processed_at.isoformat()
            if notification.processed_at
            else None
        ),
    }


def serialize_payload(payload: dict) -> bytes:
    """Каноническое JSON-кодирование payload'а webhook."""

    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sign_payload(body: bytes, secret: str) -> str:
    """Возвращает HMAC-подпись ``sha256=<hex>`` для ``body``."""

    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def build_headers(
    notification: WebhookNotification, body: bytes, secret: str | None
) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Id": f"{notification.payment_id}:{notification.status.value}",
    }
    if secret:
        headers["X-Webhook-Signature"] = sign_payload(body, secret)
    return headers


class WebhookSender:
    """Отправляет webhook через общий ``httpx.AsyncClient``.

    Клиент создаётся один раз в lifespan процесса, чтобы не открывать новое
    соединение на каждую попытку и каждый платёж. В тестах клиент можно
    подставить через конструктор.
    """

    def __init__(
        self,
        settings: WebhookSettings | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings or get_webhook_settings()
        self._client = client
        self._owns_client = client is None

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._settings.timeout,
                follow_redirects=False,
            )

    async def stop(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("WebhookSender.start() не был вызван")
        return self._client

    async def send(self, notification: WebhookNotification) -> None:
        """Отправляет результат платежа методом POST на webhook-адрес.

        Делает ``webhook_max_retries`` попыток с экспоненциальной задержкой и
        jitter. Если все попытки провалились, ошибка пробрасывается дальше,
        чтобы включился retry на уровне брокера.
        """

        settings = self._settings
        await validate_webhook_target(
            notification.webhook_url, settings.allow_private_hosts
        )

        body = serialize_payload(build_payload(notification))
        secret = settings.secret.get_secret_value() if settings.secret else None
        headers = build_headers(notification, body, secret)
        last_error: Exception | None = None

        for attempt in range(1, settings.max_retries + 1):
            try:
                response = await self.client.post(
                    notification.webhook_url, content=body, headers=headers
                )
                if len(response.content) > settings.max_response_bytes:
                    raise WebhookDeliveryError(
                        f"ответ webhook превышает {settings.max_response_bytes} байт"
                    )
                response.raise_for_status()
                logger.info(
                    "Webhook доставлен для платежа %s (попытка %s)",
                    notification.payment_id,
                    attempt,
                )
                return
            except Exception as exc:  # noqa: BLE001 - повторяется ниже
                last_error = exc
                logger.warning(
                    "Попытка webhook %s/%s для платежа %s провалилась: %s",
                    attempt,
                    settings.max_retries,
                    notification.payment_id,
                    exc,
                )
                if attempt < settings.max_retries:
                    delay = settings.retry_delay * (2 ** (attempt - 1))
                    delay *= random.uniform(0.5, 1.5)
                    await asyncio.sleep(delay)

        raise WebhookDeliveryError(
            f"webhook для платежа {notification.payment_id} не доставлен после "
            f"{settings.max_retries} попыток"
        ) from last_error