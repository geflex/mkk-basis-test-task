"""Топология RabbitMQ и общий брокер FastStream.

Топология (direct-exchange ``payments``)::

    payments.new            -> основная очередь обработки, её читает worker
    payments.retry.<n>      -> очереди задержки на каждую попытку
    payments.dlq            -> dead letter queue для исчерпавших попытки

Retry-очереди — обычные очереди с TTL сообщения и dead-letter exchange,
указывающим обратно в ``payments.new``. Отдельная очередь на каждую попытку
сохраняет экспоненциальный backoff без эффекта «head-of-line blocking».
"""

from __future__ import annotations

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import get_broker_settings

SETTINGS = get_broker_settings()

broker: RabbitBroker = RabbitBroker(SETTINGS.rabbitmq_url)

EXCHANGE = RabbitExchange(
    SETTINGS.payments_exchange,
    type=ExchangeType.DIRECT,
    durable=True,
)

NEW_QUEUE = RabbitQueue(
    SETTINGS.payments_new_queue,
    durable=True,
    routing_key=SETTINGS.payments_new_queue,
    # Отклонённые сообщения (например, если обработчик упал при планировании
    # retry) уходят в DLQ, а не отбрасываются молча.
    arguments={
        "x-dead-letter-exchange": SETTINGS.payments_exchange,
        "x-dead-letter-routing-key": SETTINGS.payments_dlq_queue,
    },
)

DLQ = RabbitQueue(
    SETTINGS.payments_dlq_queue,
    durable=True,
    routing_key=SETTINGS.payments_dlq_queue,
)


def retry_routing_key(attempt: int) -> str:
    """Routing key очереди задержки, следующей за попыткой ``attempt``."""

    return f"{SETTINGS.retry_routing_prefix}.{attempt}"


def build_retry_queues(max_attempts: int) -> list[RabbitQueue]:
    """Строит retry-очереди для попыток 2..``max_attempts``."""

    queues: list[RabbitQueue] = []
    # attempt == 1 — первая доставка; повторы выполняются для попыток 2..N.
    for attempt in range(1, max_attempts):
        delay_ms = SETTINGS.base_retry_delay_ms * (2 ** (attempt - 1))
        queues.append(
            RabbitQueue(
                retry_routing_key(attempt),
                durable=True,
                routing_key=retry_routing_key(attempt),
                arguments={
                    "x-message-ttl": delay_ms,
                    "x-dead-letter-exchange": SETTINGS.payments_exchange,
                    "x-dead-letter-routing-key": SETTINGS.payments_new_queue,
                },
            )
        )
    return queues


def all_queues(max_attempts: int) -> list[RabbitQueue]:
    return [NEW_QUEUE, *build_retry_queues(max_attempts), DLQ]


async def declare_topology(
    broker_: RabbitBroker = broker, max_attempts: int = 3
) -> None:
    """Объявляет exchange, все очереди и их привязки.

    Вызывается при старте и API, и worker'ом, чтобы сообщения не терялись
    из-за того, что очередь ещё не была привязана.
    """

    exchange = await broker_.declare_exchange(EXCHANGE)
    for queue in all_queues(max_attempts):
        declared = await broker_.declare_queue(queue)
        await declared.bind(exchange, queue.routing_key)