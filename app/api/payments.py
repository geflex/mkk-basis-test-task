"""Эндпоинты платежей."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models import Payment
from app.repositories import create_payment, get_payment
from app.schemas import PaymentCreate, PaymentCreated, PaymentDetail
from app.security import require_api_key

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


def _matches_request(payment: Payment, payload: PaymentCreate) -> bool:
    """Возвращает ``True``, если сохранённый платёж создан из ``payload``.

    Повторное использование ``Idempotency-Key`` для *другого* запроса — ошибка
    клиента, поэтому вызывающий код не должен молча возвращать исходный платёж.
    """

    return (
        payment.amount == payload.amount
        and payment.currency == payload.currency
        and payment.description == payload.description
        and payment.meta == payload.metadata
        and payment.webhook_url == str(payload.webhook_url)
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreated,
    summary="Создать платёж",
)
async def create_payment_endpoint(
    payload: PaymentCreate,
    response: Response,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentCreated:
    """Принимает платёж в асинхронную обработку.

    Заголовок ``Idempotency-Key`` делает операцию безопасной для повторов:
    повторная отправка того же ключа возвращает уже созданный платёж вместо
    создания нового. Повторы помечаются заголовком ``Idempotency-Replayed``,
    а повторное использование ключа с другим телом возвращает ``409 Conflict``.
    """

    payment, created = await create_payment(session, payload, idempotency_key)
    if not created:
        if not _matches_request(payment, payload):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "Idempotency-Key уже использовался с другим запросом"
                ),
            )
        response.headers["Idempotency-Replayed"] = "true"

    return PaymentCreated(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    summary="Получить платёж",
)
async def get_payment_endpoint(
    payment_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentDetail:
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Платёж не найден"
        )
    return PaymentDetail.from_model(payment)
