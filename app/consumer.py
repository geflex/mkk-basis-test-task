"""Worker обработки платежей.

Одно приложение FastStream, читающее ``payments.new``. Для каждого сообщения:

1. загружает платёж и пропускает эмуляцию, если он уже в финальном статусе
   (идемпотентная повторная доставка),
2. эмулирует обработку шлюзом (2–5 с, ~90% успеха),
3. сохраняет финальный статус,
4. доставляет webhook.

Любая ошибка приводит к повтору через очереди задержки с экспоненциальным
backoff; после ``max_attempts`` сообщение уходит в dead letter queue.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from typing import Annotated

from faststream import FastStream, Header
from faststream.rabbit import RabbitBroker

from app.broker import (
    DLQ,
    EXCHANGE,
    NEW_QUEUE,
    broker,
    declare_topology,
    retry_routing_key,
)
from app.config import settings
from app.db import SessionFactory
from app.logging_setup import configure_logging
from app.models import Payment, PaymentStatus, utcnow
from app.schemas import PaymentCreatedEvent
from app.webhook import send_webhook

configure_logging()
logger = logging.getLogger("payments.consumer")


async def process_payment(payment_id: uuid.UUID) -> None:
    """Эмулирует обработку шлюзом и уведомляет клиента."""

    async with SessionFactory() as session:
        # ``FOR UPDATE`` сериализует конкурентные доставки одного сообщения:
        # второй worker блокируется до коммита первого, затем видит финальный
        # статус и только повторно отправляет webhook. Это сохраняет
        # идемпотентность обработки даже при нескольких репликах consumer'а.
        payment = await session.get(Payment, payment_id, with_for_update=True)
        if payment is None:
            logger.warning("Платёж %s не найден, нечего делать", payment_id)
            return

        if payment.status == PaymentStatus.PENDING:
            delay = random.uniform(
                settings.processing_min_seconds, settings.processing_max_seconds
            )
            logger.info("Обрабатываю платёж %s (эмуляция %.2f с)", payment_id, delay)
            await asyncio.sleep(delay)

            success = random.random() < settings.success_rate
            payment.status = (
                PaymentStatus.SUCCEEDED if success else PaymentStatus.FAILED
            )
            payment.processed_at = utcnow()
            await session.commit()
            await session.refresh(payment)
            logger.info("Платёж %s -> %s", payment_id, payment.status.value)
        else:
            logger.info(
                "Платёж %s уже %s, повторно отправляю webhook",
                payment_id,
                payment.status.value,
            )

        await send_webhook(payment)


async def _schedule_retry(
    event: PaymentCreatedEvent, attempt: int, error: Exception
) -> None:
    """Переставляет сообщение в отложенный retry или отправляет его в DLQ."""

    if attempt < settings.max_attempts:
        next_attempt = attempt + 1
        logger.warning(
            "Платёж %s упал на попытке %s/%s, повторяю: %s",
            event.payment_id,
            attempt,
            settings.max_attempts,
            error,
        )
        await broker.publish(
            event,
            exchange=EXCHANGE,
            routing_key=retry_routing_key(attempt),
            headers={"x-attempt": next_attempt},
            message_id=str(event.payment_id),
            persist=True,
        )
    else:
        logger.error(
            "Платёж %s упал после %s попыток, отправляю в DLQ: %s",
            event.payment_id,
            settings.max_attempts,
            error,
        )
        await broker.publish(
            event,
            exchange=EXCHANGE,
            routing_key=DLQ.routing_key,
            headers={
                "x-attempt": attempt,
                "x-error": str(error)[:500],
                "x-failed-at": utcnow().isoformat(),
            },
            message_id=str(event.payment_id),
            persist=True,
        )


@broker.subscriber(NEW_QUEUE, EXCHANGE)
async def handle_payment_created(
    event: PaymentCreatedEvent,
    x_attempt: Annotated[int, Header("x-attempt")] = 1,
) -> None:
    logger.info("Получен платёж %s (попытка %s)", event.payment_id, x_attempt)
    try:
        await process_payment(event.payment_id)
    except Exception as exc:  # noqa: BLE001 - retry/DLQ обрабатываются явно
        await _schedule_retry(event, x_attempt, exc)


app = FastStream(broker, after_startup=(declare_topology,))


if __name__ == "__main__":  # pragma: no cover
    app.run()
