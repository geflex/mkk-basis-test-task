"""Эндпоинты платежей."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.dependencies import get_payment_service
from app.core.security import require_api_key
from app.domain.contracts import PaymentCreate, PaymentCreated, PaymentDetail
from app.services.payments import PaymentService

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
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
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentCreated:
    """Принимает платёж в асинхронную обработку.

    Заголовок ``Idempotency-Key`` делает операцию безопасной для повторов:
    повторная отправка того же ключа возвращает уже созданный платёж вместо
    создания нового. Повторы помечаются заголовком ``Idempotency-Replayed``,
    а повторное использование ключа с другим телом возвращает ``409 Conflict``.
    """

    payment, created = await service.create(payload, idempotency_key)
    if not created:
        if not service.matches(payment, payload):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency-Key уже использовался с другим запросом",
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
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentDetail:
    payment = await service.get(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Платёж не найден"
        )
    return service.to_detail(payment)