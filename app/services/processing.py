"""Сценарий обработки платежа consumer'ом."""

from __future__ import annotations

import logging
import uuid

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import ProcessingSettings, get_processing_settings
from app.db.models import Payment
from app.domain.enums import PaymentStatus
from app.domain.time import utcnow
from app.services.gateway import EmulatedPaymentGateway, PaymentGateway
from app.services.webhook import WebhookNotification, WebhookSender

logger = logging.getLogger(__name__)


class PaymentNotFoundError(LookupError):
    """Событие пришло по платежу, которого нет в БД.

    Не глотается в consumer'е: сообщение уходит в retry, а после исчерпания
    попыток — в DLQ, вместо тихой потери.
    """


class PaymentProcessor:
    """Загружает платёж, проводит его через шлюз и отправляет webhook."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        gateway: PaymentGateway,
        webhooks: WebhookSender,
        settings: ProcessingSettings | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks
        self._settings = settings or get_processing_settings()

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session:
            # ``FOR UPDATE`` сериализует конкурентные доставки одного
            # сообщения: второй worker блокируется до коммита первого, затем
            # видит финальный статус и только повторно отправляет webhook.
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None:
                # Платёж обязан существовать: событие пишется в outbox в одной
                # транзакции с платежом. Если его нет — это сигнал о проблеме,
                # поэтому сообщение должно уйти в retry/DLQ, а не потеряться.
                raise PaymentNotFoundError(
                    f"Платёж {payment_id} не найден в БД"
                )

            if payment.status == PaymentStatus.PENDING:
                payment.status = await self._gateway.process(payment_id)
                payment.processed_at = utcnow()
                await session.commit()
                logger.info("Платёж %s -> %s", payment_id, payment.status.value)
            else:
                logger.info(
                    "Платёж %s уже %s, повторно отправляю webhook",
                    payment_id,
                    payment.status.value,
                )

            notification = WebhookNotification.from_payment(payment)

        # Сессия закрыта до сетевого вызова: webhook не держит DB-соединение.
        await self._webhooks.send(notification)


def build_processor(
    session_factory: async_sessionmaker,
    webhooks: WebhookSender,
    gateway: PaymentGateway | None = None,
) -> PaymentProcessor:
    return PaymentProcessor(
        session_factory=session_factory,
        gateway=gateway or EmulatedPaymentGateway(),
        webhooks=webhooks,
    )