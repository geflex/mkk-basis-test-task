"""Тесты топологии RabbitMQ (без брокера)."""

from __future__ import annotations

from app.core.config import get_broker_settings, get_processing_settings
from app.messaging.broker import NEW_QUEUE, build_retry_queues, retry_routing_key

settings = get_broker_settings()


def test_new_queue_dead_letters_rejected_messages_to_dlq() -> None:
    assert NEW_QUEUE.arguments["x-dead-letter-exchange"] == settings.payments_exchange
    assert (
        NEW_QUEUE.arguments["x-dead-letter-routing-key"]
        == settings.payments_dlq_queue
    )


def test_retry_queues_have_exponential_backoff() -> None:
    queues = build_retry_queues(get_processing_settings().max_attempts)
    delays = [queue.arguments["x-message-ttl"] for queue in queues]

    assert len(delays) == get_processing_settings().max_attempts - 1
    assert delays == sorted(delays)
    assert delays[0] == settings.base_retry_delay_ms
    for queue in queues:
        assert queue.arguments["x-dead-letter-routing-key"] == settings.payments_new_queue


def test_retry_routing_key_is_attempt_scoped() -> None:
    assert retry_routing_key(1) == f"{settings.retry_routing_prefix}.1"
    assert retry_routing_key(2) == f"{settings.retry_routing_prefix}.2"