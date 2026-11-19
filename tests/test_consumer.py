"""Consumer retry / DLQ routing tests (no broker required)."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest

import app.consumer as consumer
from app.schemas import PaymentCreatedEvent


@pytest.fixture
def published() -> AsyncMock:
    mock = AsyncMock()
    with patch.object(consumer.broker, "publish", new=mock):
        yield mock


async def test_failure_schedules_delayed_retry(published: AsyncMock) -> None:
    event = PaymentCreatedEvent(payment_id=uuid.uuid4())
    with patch.object(
        consumer, "process_payment", new=AsyncMock(side_effect=RuntimeError("boom"))
    ):
        await consumer.handle_payment_created(event, 1)

    _, kwargs = published.call_args
    assert kwargs["routing_key"] == "payments.retry.1"
    assert kwargs["headers"]["x-attempt"] == 2


async def test_second_failure_schedules_second_retry(published: AsyncMock) -> None:
    event = PaymentCreatedEvent(payment_id=uuid.uuid4())
    with patch.object(
        consumer, "process_payment", new=AsyncMock(side_effect=RuntimeError("boom"))
    ):
        await consumer.handle_payment_created(event, 2)

    _, kwargs = published.call_args
    assert kwargs["routing_key"] == "payments.retry.2"
    assert kwargs["headers"]["x-attempt"] == 3


async def test_exhausted_attempts_go_to_dlq(published: AsyncMock) -> None:
    event = PaymentCreatedEvent(payment_id=uuid.uuid4())
    with patch.object(
        consumer, "process_payment", new=AsyncMock(side_effect=RuntimeError("boom"))
    ):
        await consumer.handle_payment_created(event, consumer.settings.max_attempts)

    _, kwargs = published.call_args
    assert kwargs["routing_key"] == "payments.dlq"
    assert kwargs["headers"]["x-error"] == "boom"


async def test_success_does_not_republish(published: AsyncMock) -> None:
    event = PaymentCreatedEvent(payment_id=uuid.uuid4())
    with patch.object(consumer, "process_payment", new=AsyncMock()) as process:
        await consumer.handle_payment_created(event, 1)

    process.assert_awaited_once_with(event.payment_id)
    published.assert_not_awaited()
