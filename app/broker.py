"""Топология RabbitMQ и общий брокер FastStream.

Топология (direct-exchange ``payments``)::

    payments.new            -> основная очередь обработки, её читает worker
    payments.retry.<n>      -> очереди задержки на каждую попытку (TTL + DLX обратно в payments.new)
    payments.dlq            -> dead letter queue для сообщений, исчерпавших попытки

Retry-очереди — обычные очереди с TTL сообщения и dead-letter exchange,
указывающим обратно в ``payments.new``. Отдельная очередь на каждую попытку
сохраняет экспоненциальный backoff без эффекта «head-of-line blocking».
"""

from __future__ import annotations

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

broker: RabbitBroker = RabbitBroker(settings.rabbitmq_url)

EXCHANGE = RabbitExchange(
    settings.payments_exchange,
    type=ExchangeType.DIRECT,
    durable=True,
)

NEW_QUEUE = RabbitQueue(
    settings.payments_new_queue,
    durable=True,
    routing_key=settings.payments_new_queue,
    # Отклонённые сообщения (например, если обработчик упал при планировании
    # retry) уходят в DLQ, а не отбрасываются молча, так как подписчик
    # использует стандартную политику подтверждения REJECT_ON_ERROR.
    arguments={
        "x-dead-letter-exchange": settings.payments_exchange,
        "x-dead-letter-routing-key": settings.payments_dlq_queue,
    },
)

DLQ = RabbitQueue(
    settings.payments_dlq_queue,
    durable=True,
    routing_key=settings.payments_dlq_queue,
)


def retry_routing_key(attempt: int) -> str:
    """Routing key очереди задержки, следующей за попыткой ``attempt``."""

    return f"{settings.retry_routing_prefix}.{attempt}"


def _build_retry_queues() -> list[RabbitQueue]:
    queues: list[RabbitQueue] = []
    # attempt == 1 — первая доставка; повторы выполняются для попыток 2..max_attempts
    for attempt in range(1, settings.max_attempts):
        delay_ms = settings.base_retry_delay_ms * (2 ** (attempt - 1))
        queues.append(
            RabbitQueue(
                retry_routing_key(attempt),
                durable=True,
                routing_key=retry_routing_key(attempt),
                arguments={
                    "x-message-ttl": delay_ms,
                    "x-dead-letter-exchange": settings.payments_exchange,
                    "x-dead-letter-routing-key": settings.payments_new_queue,
                },
            )
        )
    return queues


RETRY_QUEUES: list[RabbitQueue] = _build_retry_queues()

ALL_QUEUES: list[RabbitQueue] = [NEW_QUEUE, *RETRY_QUEUES, DLQ]


async def declare_topology(broker_: RabbitBroker = broker) -> None:
    """Объявляет exchange, все очереди и их привязки.

    Вызывается при старте и API, и worker'ом, чтобы сообщения не терялись
    из-за того, что очередь ещё не была привязана.
    """

    exchange = await broker_.declare_exchange(EXCHANGE)
    for queue in ALL_QUEUES:
        declared = await broker_.declare_queue(queue)
        await declared.bind(exchange, queue.routing_key)
