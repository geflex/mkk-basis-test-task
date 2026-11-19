"""Контракты приложения: входные DTO, ответы API и сообщения брокера.

Модуль не зависит от SQLAlchemy — ORM-модели, в свою очередь, не зависят от
API-схем. Это позволяет использовать контракты и в HTTP-слое, и в consumer'е.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.core.config import get_webhook_settings
from app.core.net import is_private_host
from app.domain.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    """Тело запроса для ``POST /api/v1/payments``."""

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str | None = Field(default=None, max_length=1024)
    metadata: dict = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("metadata")
    @classmethod
    def _limit_metadata(cls, value: dict) -> dict:
        if len(value) > 50:
            raise ValueError("metadata должен содержать не более 50 ключей")
        return value

    @field_validator("webhook_url")
    @classmethod
    def _reject_private_webhook_host(cls, value: HttpUrl) -> HttpUrl:
        if get_webhook_settings().allow_private_hosts:
            return value
        host = value.host
        if not host or is_private_host(host):
            raise ValueError(
                "webhook_url не должен указывать на приватный, loopback- "
                "или link-local хост"
            )
        return value


class PaymentCreated(BaseModel):
    """Ответ, возвращаемый, когда платёж принят в обработку."""

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    """Полное представление платежа."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentCreatedEvent(BaseModel):
    """Событие, публикуемое в ``payments.new`` и потребляемое воркером."""

    payment_id: uuid.UUID