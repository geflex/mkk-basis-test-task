"""RabbitMQ topology and the shared FastStream broker.

Topology (direct exchange ``payments``)::

    payments.new            -> main work queue consumed by the worker
    payments.retry.<n>      -> per-attempt delay queues (TTL + DLX back to payments.new)
    payments.dlq            -> dead letter queue for messages that exhausted retries

The retry queues are plain queues with a message TTL and a dead-letter
exchange pointing back at ``payments.new``. Using a dedicated queue per retry
attempt keeps exponential backoff without head-of-line blocking.
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
)

DLQ = RabbitQueue(
    settings.payments_dlq_queue,
    durable=True,
    routing_key=settings.payments_dlq_queue,
)


def retry_routing_key(attempt: int) -> str:
    """Routing key of the delay queue that follows ``attempt``."""

    return f"{settings.retry_routing_prefix}.{attempt}"


def _build_retry_queues() -> list[RabbitQueue]:
    queues: list[RabbitQueue] = []
    # attempt == 1 is the first delivery; retries happen for attempts 2..max_attempts
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
    """Declare the exchange, every queue and their bindings.

    Called on startup by both the API and the worker so that messages are
    never lost because a queue was not bound yet.
    """

    exchange = await broker_.declare_exchange(EXCHANGE)
    for queue in ALL_QUEUES:
        declared = await broker_.declare_queue(queue)
        await declared.bind(exchange, queue.routing_key)
