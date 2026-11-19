"""Абстракция платёжного шлюза и его эмуляция."""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from typing import Protocol

from app.core.config import ProcessingSettings, get_processing_settings
from app.domain.enums import PaymentStatus

logger = logging.getLogger(__name__)


class PaymentGateway(Protocol):
    """Внешний платёжный шлюз."""

    async def process(self, payment_id: uuid.UUID) -> PaymentStatus:
        """Проводит платёж и возвращает финальный статус."""
        ...


class EmulatedPaymentGateway:
    """Эмуляция шлюза: задержка 2–5 с и заданная вероятность успеха."""

    def __init__(self, settings: ProcessingSettings | None = None) -> None:
        self._settings = settings or get_processing_settings()

    async def process(self, payment_id: uuid.UUID) -> PaymentStatus:
        settings = self._settings
        delay = random.uniform(
            settings.processing_min_seconds, settings.processing_max_seconds
        )
        logger.info("Обрабатываю платёж %s (эмуляция %.2f с)", payment_id, delay)
        await asyncio.sleep(delay)

        return (
            PaymentStatus.SUCCEEDED
            if random.random() < settings.success_rate
            else PaymentStatus.FAILED
        )