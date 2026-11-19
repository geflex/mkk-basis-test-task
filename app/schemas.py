"""Схемы Pydantic v2, используемые API и брокером сообщений."""

from __future__ import annotations

import ipaddress
import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.config import settings
from app.models import Currency, Payment, PaymentStatus

_LOCAL_HOSTNAMES = ("localhost",)
_LOCAL_SUFFIXES = (".localhost", ".local", ".internal")


def _is_private_host(host: str) -> bool:
    """Возвращает ``True`` для хостов, которым нельзя отправлять запросы с сервера."""

    host = host.strip().lower().rstrip(".")
    if host in _LOCAL_HOSTNAMES or host.endswith(_LOCAL_SUFFIXES):
        return True
    try:
        address = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return False
    return (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_reserved
        or address.is_multicast
        or address.is_unspecified
    )


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
        if settings.allow_private_webhook_hosts:
            return value
        host = value.host
        if not host or _is_private_host(host):
            raise ValueError(
                "webhook_url не должен указывать на приватный, loopback- "
                "или link-local хост"
            )
        return value


class PaymentCreated(BaseModel):
    """Ответ, возвращаемый когда платёж принят в обработку."""

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

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentDetail":
        return cls(
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


class PaymentCreatedEvent(BaseModel):
    """Событие, публикуемое в ``payments.new`` и потребляемое воркером."""

    payment_id: uuid.UUID
