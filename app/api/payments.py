"""Payment endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.repositories import create_payment, get_payment
from app.schemas import PaymentCreate, PaymentCreated, PaymentDetail
from app.security import require_api_key

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreated,
    summary="Create a payment",
)
async def create_payment_endpoint(
    payload: PaymentCreate,
    response: Response,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentCreated:
    """Accept a payment for asynchronous processing.

    The ``Idempotency-Key`` header makes the operation safe to repeat: sending
    the same key again returns the already created payment instead of creating
    a new one. Replays are flagged with the ``Idempotency-Replayed`` header.
    """

    payment, created = await create_payment(session, payload, idempotency_key)
    if not created:
        response.headers["Idempotency-Replayed"] = "true"

    return PaymentCreated(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    summary="Get a payment",
)
async def get_payment_endpoint(
    payment_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentDetail:
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        )
    return PaymentDetail.from_model(payment)
