"""Тесты сценария обработки платежа consumer'ом (без реальной БД)."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.processing import PaymentNotFoundError, PaymentProcessor


def _processor(session: AsyncMock) -> PaymentProcessor:
    session_factory = MagicMock()
    session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
    session_factory.return_value.__aexit__ = AsyncMock(return_value=False)
    return PaymentProcessor(
        session_factory=session_factory,
        gateway=AsyncMock(),
        webhooks=AsyncMock(),
    )


async def test_missing_payment_is_not_silently_ignored() -> None:
    """Событие без платежа должно уйти в retry/DLQ, а не подтвердиться молча."""

    session = AsyncMock()
    session.get.return_value = None
    processor = _processor(session)

    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid.uuid4())

    session.commit.assert_not_awaited()