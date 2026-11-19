"""Зависимости FastAPI."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.services.payments import PaymentService


async def get_payment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentService:
    """Собирает сервис платежей на время запроса."""

    return PaymentService(session)