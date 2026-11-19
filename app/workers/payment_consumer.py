"""Worker обработки платежей.

Одно приложение FastStream, читающее ``payments.new``. Для каждого сообщения
сервисный слой загружает платёж, проводит его через эмуляцию шлюза и
доставляет webhook. Любая ошибка приводит к повтору через очереди задержки с
экспоненциальным backoff; после ``max_attempts`` сообщение уходит в DLQ.
"""

from __future__ import annotations

import logging
from typing import Annotated

from faststream import FastStream, Header
from faststream.rabbit import RabbitBroker

from app.core.config import get_processing_settings
from app.core.logging import configure_logging, payment_id_var
from app.db.session import SessionFactory
from app.domain.contracts import PaymentCreatedEvent
from app.domain.time import utcnow
from app.messaging.broker import (
    DLQ,
    EXCHANGE,
    NEW_QUEUE,
    broker,
    declare_topology,
    retry_routing_key,
)
from app.services.processing import build_processor
from app.services.webhook import WebhookSender

configure_logging()
logger = logging.getLogger("payments.consumer")

webhook_sender = WebhookSender()
processor = build_processor(SessionFactory, webhook_sender)


async def _startup() -> None:
    await declare_topology(broker, get_processing_settings().max_attempts)
    await webhook_sender.start()


async def _shutdown() -> None:
    await webhook_sender.stop()


async def _schedule_retry(
    event: PaymentCreatedEvent, attempt: int, error: Exception
) -> None:
    """Переставляет сообщение в отложенный retry или отправляет его в DLQ."""

    settings = get_processing_settings()
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
    token = payment_id_var.set(str(event.payment_id))
    try:
        await processor.process(event.payment_id)
    except Exception as exc:  # noqa: BLE001 - retry/DLQ обрабатываются явно
        await _schedule_retry(event, x_attempt, exc)
    finally:
        payment_id_var.reset(token)


app = FastStream(
    broker,
    after_startup=(_startup,),
    after_shutdown=(_shutdown,),
)


if __name__ == "__main__":  # pragma: no cover
    app.run()