"""Data-access helpers for payments and outbox events."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def get_by_idempotency_key(
    session: AsyncSession, idempotency_key: str
) -> Payment | None:
    result = await session.execute(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


async def create_payment(
    session: AsyncSession,
    payload: PaymentCreate,
    idempotency_key: str,
) -> tuple[Payment, bool]:
    """Create a payment and its outbox event atomically.

    Returns ``(payment, created)`` where ``created`` is ``False`` when the
    idempotency key was already used. Both the lookup and the unique constraint
    are used to stay correct under concurrent duplicate requests.
    """

    existing = await get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return existing, False

    payment = Payment(
        amount=payload.amount,
        currency=payload.currency,
        description=payload.description,
        meta=payload.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(payload.webhook_url),
    )
    session.add(payment)
    await session.flush()

    session.add(
        OutboxEvent(
            event_type="payment.created",
            routing_key=settings.payments_new_queue,
            payload={"payment_id": str(payment.id)},
        )
    )

    try:
        await session.commit()
    except IntegrityError:
        # A concurrent request won the race for this idempotency key.
        await session.rollback()
        existing = await get_by_idempotency_key(session, idempotency_key)
        if existing is None:  # pragma: no cover - extremely unlikely
            raise
        return existing, False

    await session.refresh(payment)
    return payment, True
