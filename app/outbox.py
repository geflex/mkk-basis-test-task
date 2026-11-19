"""Публикатор транзакционного outbox.

Фоновая задача опрашивает таблицу ``outbox`` на предмет ещё не опубликованных
событий, пересылает их в RabbitMQ и помечает опубликованными. Использование
``SELECT ... FOR UPDATE SKIP LOCKED`` позволяет безопасно запускать несколько
публикаторов одновременно.
"""

from __future__ import annotations

import asyncio
import logging

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.broker import EXCHANGE, broker as default_broker
from app.config import settings
from app.models import OutboxEvent, utcnow

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(
        self,
        session_factory: async_sessionmaker,
        broker: RabbitBroker = default_broker,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker

    async def publish_pending(self) -> int:
        """Публикует один батч ожидающих событий; возвращает число отправленных."""

        published = 0
        async with self._session_factory() as session:
            result = await session.execute(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars())

            for event in events:
                await self._broker.publish(
                    event.payload,
                    exchange=EXCHANGE,
                    routing_key=event.routing_key,
                    message_id=str(event.id),
                    persist=True,
                )
                event.published_at = utcnow()
                published += 1

            if events:
                await session.commit()

        if published:
            logger.info("Outbox: опубликовано событий: %s", published)
        return published

    async def run(self) -> None:
        """Непрерывно пересылает ожидающие события до отмены."""

        logger.info("Публикатор outbox запущен (интервал=%.2f с)", settings.outbox_poll_interval)
        while True:
            try:
                await self.publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - устойчивость
                logger.exception("Итерация публикатора outbox завершилась ошибкой")
            await asyncio.sleep(settings.outbox_poll_interval)
