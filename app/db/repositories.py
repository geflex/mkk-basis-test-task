"""Доступ к данным платежей и outbox.

Здесь только операции с БД: транзакции, бизнес-правила идемпотентности и
формирование outbox-события живут в сервисном слое.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent, Payment


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def get_by_idempotency_key(
    session: AsyncSession, idempotency_key: str
) -> Payment | None:
    result = await session.execute(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


def add_payment_with_event(
    session: AsyncSession, payment: Payment, event: OutboxEvent
) -> None:
    """Добавляет платёж и событие outbox в текущую транзакцию."""

    session.add_all([payment, event])


async def get_unpublished_events(
    session: AsyncSession, limit: int
) -> list[OutboxEvent]:
    """Блокирует батч неопубликованных событий для публикатора."""

    result = await session.execute(
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(result.scalars())