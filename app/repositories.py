"""Хелперы доступа к данным платежей и событий outbox."""

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
    """Атомарно создаёт платёж и его событие outbox.

    Возвращает ``(payment, created)``, где ``created`` равно ``False``, если
    idempotency-ключ уже использовался. Используются и поиск, и уникальное
    ограничение, чтобы оставаться корректным при конкурентных дублирующих
    запросах.
    """

    existing = await get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return existing, False

    # Первичный ключ генерируется на клиенте, чтобы payload outbox мог
    # ссылаться на него без дополнительного обращения к БД. Именно коммит
    # обеих строк в одной транзакции делает паттерн outbox атомарным.
    payment = Payment(
        id=uuid.uuid4(),
        amount=payload.amount,
        currency=payload.currency,
        description=payload.description,
        meta=payload.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(payload.webhook_url),
    )
    session.add(payment)
    session.add(
        OutboxEvent(
            event_type="payment.created",
            routing_key=settings.payments_new_queue,
            payload={"payment_id": str(payment.id)},
        )
    )

    try:
        # ``commit`` выполняет отложенные INSERT'ы, поэтому конкурентный
        # запрос, закоммитившийся первым, проявляется здесь как
        # ``IntegrityError`` и перехватывается.
        await session.commit()
    except IntegrityError:
        # Конкурентный запрос выиграл гонку за этот idempotency-ключ.
        await session.rollback()
        existing = await get_by_idempotency_key(session, idempotency_key)
        if existing is None:  # pragma: no cover - крайне маловероятно
            raise
        return existing, False

    return payment, True
