"""Сервис платежей: создание, чтение и проверка идемпотентности."""

from __future__ import annotations

import uuid

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_broker_settings
from app.db.models import OutboxEvent, Payment
from app.db.repositories import (
    add_payment_with_event,
    get_by_idempotency_key,
    get_payment,
)
from app.domain.contracts import PaymentCreate, PaymentDetail


class PaymentService:
    """Операции над платежами в рамках одной DB-сессии."""

    def __init__(
        self, session: AsyncSession, new_queue_routing_key: str | None = None
    ) -> None:
        self._session = session
        self._routing_key = (
            new_queue_routing_key or get_broker_settings().payments_new_queue
        )

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await get_payment(self._session, payment_id)

    async def create(
        self, payload: PaymentCreate, idempotency_key: str
    ) -> tuple[Payment, bool]:
        """Атомарно создаёт платёж и его событие outbox.

        Возвращает ``(payment, created)``, где ``created`` равно ``False``,
        если idempotency-ключ уже использовался. Используются и поиск, и
        уникальное ограничение, чтобы оставаться корректным при конкурентных
        дублирующих запросах.
        """

        existing = await get_by_idempotency_key(self._session, idempotency_key)
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
        add_payment_with_event(
            self._session,
            payment,
            OutboxEvent(
                event_type="payment.created",
                routing_key=self._routing_key,
                payload={"payment_id": str(payment.id)},
            ),
        )

        try:
            # ``commit`` выполняет отложенные INSERT'ы, поэтому конкурентный
            # запрос, закоммитившийся первым, проявляется здесь как
            # ``IntegrityError`` и перехватывается.
            await self._session.commit()
        except IntegrityError:
            # Конкурентный запрос выиграл гонку за этот idempotency-ключ.
            await self._session.rollback()
            existing = await get_by_idempotency_key(self._session, idempotency_key)
            if existing is None:  # pragma: no cover - крайне маловероятно
                raise
            return existing, False

        return payment, True

    @staticmethod
    def matches(payment: Payment, payload: PaymentCreate) -> bool:
        """``True``, если сохранённый платёж создан из ``payload``.

        Повторное использование ``Idempotency-Key`` для *другого* запроса —
        ошибка клиента, поэтому вызывающий код не должен молча возвращать
        исходный платёж.
        """

        return (
            payment.amount == payload.amount
            and payment.currency == payload.currency
            and payment.description == payload.description
            and payment.meta == payload.metadata
            and payment.webhook_url == str(payload.webhook_url)
        )

    @staticmethod
    def to_detail(payment: Payment) -> PaymentDetail:
        return PaymentDetail(
            id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.meta,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )