"""Payment processing worker.

One FastStream app consuming ``payments.new``. For every message it:

1. loads the payment and skips emulation when it is already terminal
   (idempotent redelivery),
2. emulates gateway processing (2-5s, ~90% success),
3. persists the final status,
4. delivers the webhook.

Any failure is retried via per-attempt delay queues with exponential backoff;
after ``max_attempts`` the message is routed to the dead letter queue.
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
    """Emulate gateway processing and notify the client."""

    async with SessionFactory() as session:
        # ``FOR UPDATE`` serialises concurrent deliveries of the same message:
        # a second worker blocks until the first commits, then observes the
        # terminal status and only re-sends the webhook. This keeps processing
        # idempotent even if the consumer is scaled to several replicas.
        payment = await session.get(Payment, payment_id, with_for_update=True)
        if payment is None:
            logger.warning("Payment %s not found, nothing to do", payment_id)
            return

        if payment.status == PaymentStatus.PENDING:
            delay = random.uniform(
                settings.processing_min_seconds, settings.processing_max_seconds
            )
            logger.info("Processing payment %s (emulating %.2fs)", payment_id, delay)
            await asyncio.sleep(delay)

            success = random.random() < settings.success_rate
            payment.status = (
                PaymentStatus.SUCCEEDED if success else PaymentStatus.FAILED
            )
            payment.processed_at = utcnow()
            await session.commit()
            await session.refresh(payment)
            logger.info("Payment %s -> %s", payment_id, payment.status.value)
        else:
            logger.info(
                "Payment %s already %s, resending webhook",
                payment_id,
                payment.status.value,
            )

        await send_webhook(payment)


async def _schedule_retry(
    event: PaymentCreatedEvent, attempt: int, error: Exception
) -> None:
    """Requeue the message for a delayed retry or move it to the DLQ."""

    if attempt < settings.max_attempts:
        next_attempt = attempt + 1
        logger.warning(
            "Payment %s failed on attempt %s/%s, retrying: %s",
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
            "Payment %s failed after %s attempts, sending to DLQ: %s",
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
    logger.info("Received payment %s (attempt %s)", event.payment_id, x_attempt)
    try:
        await process_payment(event.payment_id)
    except Exception as exc:  # noqa: BLE001 - retry/DLQ handled explicitly
        await _schedule_retry(event, x_attempt, exc)


app = FastStream(broker, after_startup=(declare_topology,))


if __name__ == "__main__":  # pragma: no cover
    app.run()
