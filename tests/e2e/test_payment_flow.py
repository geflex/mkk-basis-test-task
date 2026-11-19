"""End-to-end тесты жизненного цикла платежа.

Запуск с реальной инфраструктурой:

    pytest -m e2e tests/e2e
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from conftest import Stack

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


def _payload(stack: Stack, **overrides: object) -> dict:
    body: dict[str, object] = {
        "amount": "10.00",
        "currency": "USD",
        "description": "payload",
        "metadata": {},
        "webhook_url": f"{stack.receiver.base_url}/ok",
    }
    body.update(overrides)
    return body


async def test_missing_api_key_is_rejected(stack: Stack) -> None:
    response = await stack.client.post(
        "/api/v1/payments",
        json=_payload(stack),
        headers={"X-API-Key": "", "Idempotency-Key": f"auth-{uuid.uuid4()}"},
    )
    assert response.status_code == 401


async def test_wrong_api_key_is_rejected(stack: Stack) -> None:
    response = await stack.client.post(
        "/api/v1/payments",
        json=_payload(stack),
        headers={"X-API-Key": "nope", "Idempotency-Key": f"auth-{uuid.uuid4()}"},
    )
    assert response.status_code == 401


async def test_unknown_payment_returns_404(stack: Stack) -> None:
    response = await stack.client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_successful_payment_reaches_webhook(stack: Stack) -> None:
    response = await stack.create_payment(key=f"ok-{uuid.uuid4()}")
    assert response.status_code == 202
    created = response.json()
    assert created["status"] == "pending"

    payment_id = created["payment_id"]
    detail = await stack.wait_for_status(payment_id, "succeeded")
    assert detail["processed_at"] is not None
    assert detail["amount"] == "149.90"
    assert detail["metadata"] == {"order_id": 1}

    webhook = await stack.receiver.wait_for_payment(payment_id)
    assert webhook is not None
    assert webhook.path == "/ok"
    assert webhook.headers.get("x-webhook-id") == f"{payment_id}:succeeded"

    body = webhook.json()
    assert body["payment_id"] == payment_id
    assert body["status"] == "succeeded"
    assert body["amount"] == "149.90"
    assert body["currency"] == "RUB"


async def test_idempotent_replay_returns_same_payment(stack: Stack) -> None:
    key = f"idem-{uuid.uuid4()}"
    first = await stack.create_payment(key=key)
    second = await stack.create_payment(key=key)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert "Idempotency-Replayed" not in first.headers
    assert second.headers.get("Idempotency-Replayed") == "true"

    # Должны быть созданы только одна строка платежа и одно событие.
    payment_id = first.json()["payment_id"]
    detail = await stack.wait_for_status(payment_id, "succeeded")
    assert detail["idempotency_key"] == key


async def test_idempotency_key_reuse_with_other_body_conflicts(stack: Stack) -> None:
    key = f"conflict-{uuid.uuid4()}"
    first = await stack.create_payment(key=key, amount="10.00")
    assert first.status_code == 202

    second = await stack.create_payment(key=key, amount="20.00")
    assert second.status_code == 409


async def test_failed_processing_marks_payment_failed(
    stack: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.consumer as consumer

    monkeypatch.setattr(consumer.settings, "success_rate", 0.0)

    response = await stack.create_payment(key=f"fail-{uuid.uuid4()}")
    payment_id = response.json()["payment_id"]

    detail = await stack.wait_for_status(payment_id, "failed")
    assert detail["processed_at"] is not None

    webhook = await stack.receiver.wait_for_payment(payment_id)
    assert webhook is not None
    assert webhook.json()["status"] == "failed"


async def test_webhook_failure_retries_then_dead_letters(stack: Stack) -> None:
    baseline = await stack.queue_depth("payments.dlq")

    response = await stack.create_payment(
        key=f"dlq-{uuid.uuid4()}", webhook_path="/fail"
    )
    payment_id = response.json()["payment_id"]

    # Сама обработка успешна; падает только webhook, поэтому сообщение
    # должно исчерпать попытки и попасть в DLQ.
    await stack.wait_for_status(payment_id, "succeeded")

    deadline = time.monotonic() + 25
    depth = baseline
    while time.monotonic() < deadline:
        depth = await stack.queue_depth("payments.dlq")
        if depth > baseline:
            break
        await asyncio.sleep(0.2)

    assert depth > baseline, "message never reached the dead letter queue"


async def test_webhook_signature_when_secret_configured(
    stack: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.webhook as webhook

    monkeypatch.setattr(webhook.settings, "webhook_secret", "e2e-secret")

    response = await stack.create_payment(key=f"sig-{uuid.uuid4()}")
    payment_id = response.json()["payment_id"]

    webhook_request = await stack.receiver.wait_for_payment(payment_id)
    assert webhook_request is not None
    assert webhook_request.headers.get("x-webhook-signature") == webhook.sign_payload(
        webhook_request.body, "e2e-secret"
    )
