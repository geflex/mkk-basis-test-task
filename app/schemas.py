"""Pydantic v2 schemas used by the API and the message broker."""

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
    """Return ``True`` for hosts that must not receive server-side requests."""

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
    """Request body for ``POST /api/v1/payments``."""

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str | None = Field(default=None, max_length=1024)
    metadata: dict = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("metadata")
    @classmethod
    def _limit_metadata(cls, value: dict) -> dict:
        if len(value) > 50:
            raise ValueError("metadata must contain at most 50 keys")
        return value

    @field_validator("webhook_url")
    @classmethod
    def _reject_private_webhook_host(cls, value: HttpUrl) -> HttpUrl:
        if settings.allow_private_webhook_hosts:
            return value
        host = value.host
        if not host or _is_private_host(host):
            raise ValueError(
                "webhook_url must not target a private, loopback or link-local host"
            )
        return value


class PaymentCreated(BaseModel):
    """Response returned when a payment has been accepted for processing."""

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    """Full payment representation."""

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
    """Event published to ``payments.new`` and consumed by the worker."""

    payment_id: uuid.UUID
