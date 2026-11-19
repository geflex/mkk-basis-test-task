"""Фикстуры end-to-end тестов.

Эти тесты проверяют весь сервис на реальных PostgreSQL и RabbitMQ, поднятых
через testcontainers, плюс реальный HTTP-приёмник webhook. API работает
внутрипроцессно на ASGI-транспорте, а worker потребляет сообщения через общий
брокер, поэтому покрывается полный цикл
create -> outbox -> broker -> process -> webhook.

Они опциональны и исключены из обычного прогона (``addopts = -m "not e2e"``):

    pytest -m e2e tests/e2e
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest_asyncio

ROOT = Path(__file__).resolve().parents[2]
API_KEY = "e2e-api-key"
POSTGRES_PORT = 5432
RABBITMQ_PORT = 5672


@dataclass
class WebhookRequest:
    path: str
    body: bytes
    headers: dict[str, str]

    def json(self) -> dict:
        return json.loads(self.body)


class WebhookReceiver:
    """HTTP-сервер в отдельном потоке, записывающий доставки webhook.

    ``/fail`` всегда отвечает ``500``, чтобы можно было проверить путь
    retry/DLQ.
    """

    def __init__(self) -> None:
        self.requests: list[WebhookRequest] = []
        self.fail_paths: set[str] = {"/fail"}
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self.port = 0

    def start(self) -> None:
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - именование из stdlib
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                with receiver._lock:
                    receiver.requests.append(
                        WebhookRequest(
                            self.path,
                            body,
                            {key.lower(): value for key, value in self.headers.items()},
                        )
                    )
                self.send_response(500 if self.path in receiver.fail_paths else 200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok": true}')

            def log_message(self, *args: object) -> None:
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def wait_for_payment(
        self, payment_id: str, timeout: float = 20.0
    ) -> WebhookRequest | None:
        needle = payment_id.encode()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                for request in self.requests:
                    if needle in request.body:
                        return request
            # Нужно уступать управление: worker работает в том же event loop,
            # поэтому блокировка здесь не дала бы доставить webhook.
            await asyncio.sleep(0.05)
        return None


@dataclass
class Stack:
    client: httpx.AsyncClient
    receiver: WebhookReceiver
    database_url: str
    rabbitmq_url: str

    async def create_payment(
        self, *, key: str, webhook_path: str = "/ok", **overrides: object
    ) -> httpx.Response:
        body: dict[str, object] = {
            "amount": "149.90",
            "currency": "RUB",
            "description": "e2e order",
            "metadata": {"order_id": 1},
            "webhook_url": f"{self.receiver.base_url}{webhook_path}",
        }
        body.update(overrides)
        return await self.client.post(
            "/api/v1/payments", json=body, headers={"Idempotency-Key": key}
        )

    async def wait_for_status(
        self, payment_id: str, status: str, timeout: float = 20.0
    ) -> dict:
        deadline = time.monotonic() + timeout
        last: dict = {}
        while time.monotonic() < deadline:
            response = await self.client.get(f"/api/v1/payments/{payment_id}")
            last = response.json()
            if last.get("status") == status:
                return last
            await asyncio.sleep(0.1)
        raise AssertionError(f"payment {payment_id} never reached {status!r}: {last}")

    async def queue_depth(self, queue_name: str) -> int:
        import aio_pika

        connection = await aio_pika.connect_robust(self.rabbitmq_url)
        try:
            channel = await connection.channel()
            queue = await channel.declare_queue(queue_name, durable=True, passive=True)
            return queue.declaration_result.message_count
        finally:
            await connection.close()


def _run_migrations() -> None:
    from alembic import command
    from alembic.config import Config

    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    command.upgrade(config, "head")


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def stack() -> AsyncIterator[Stack]:
    from testcontainers.community.postgres import PostgresContainer
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    postgres = PostgresContainer(
        "postgres:16-alpine",
        username="payments",
        password="payments",
        dbname="payments",
    )
    # Обычный контейнер с проверкой готовности по логам избегает нестабильного
    # pika-проба, которая идёт в community-образе RabbitMQ.
    rabbit = (
        DockerContainer("rabbitmq:3.13-management-alpine")
        .with_exposed_ports(RABBITMQ_PORT)
        .with_env("RABBITMQ_DEFAULT_USER", "guest")
        .with_env("RABBITMQ_DEFAULT_PASS", "guest")
        .waiting_for(LogMessageWaitStrategy("Server startup complete"))
    )

    await asyncio.to_thread(postgres.start)
    await asyncio.to_thread(rabbit.start)

    database_url = (
        f"postgresql+asyncpg://payments:payments@"
        f"{postgres.get_container_host_ip()}:{postgres.get_exposed_port(POSTGRES_PORT)}"
        f"/payments"
    )
    rabbitmq_url = (
        f"amqp://guest:guest@"
        f"{rabbit.get_container_host_ip()}:{rabbit.get_exposed_port(RABBITMQ_PORT)}/"
    )

    # Должно быть задано до импорта любого модуля ``app.*``: настройки читаются
    # во время импорта, а движок привязывается к ``DATABASE_URL`` при первом
    # импорте.
    os.environ.update(
        DATABASE_URL=database_url,
        RABBITMQ_URL=rabbitmq_url,
        API_KEY=API_KEY,
        ALLOW_PRIVATE_WEBHOOK_HOSTS="true",
        PROCESSING_MIN_SECONDS="0",
        PROCESSING_MAX_SECONDS="0",
        SUCCESS_RATE="1.0",
        OUTBOX_POLL_INTERVAL="0.05",
        BASE_RETRY_DELAY_MS="150",
        WEBHOOK_RETRY_DELAY="0.05",
        WEBHOOK_MAX_RETRIES="2",
        WEBHOOK_TIMEOUT="3",
    )

    await asyncio.to_thread(_run_migrations)

    import app.workers.payment_consumer as consumer  # noqa: F401  - регистрирует подписчика
    from app.api.app import app as fastapi_app

    receiver = WebhookReceiver()
    receiver.start()

    lifespan = fastapi_app.router.lifespan_context(fastapi_app)
    await lifespan.__aenter__()
    # В реальном деплое WebhookSender стартует в lifespan consumer'а; здесь
    # worker и API живут в одном процессе, поэтому запускаем его вручную.
    await consumer.webhook_sender.start()

    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=fastapi_app),
        base_url="http://e2e",
        headers={"X-API-Key": API_KEY},
    )

    try:
        yield Stack(
            client=client,
            receiver=receiver,
            database_url=database_url,
            rabbitmq_url=rabbitmq_url,
        )
    finally:
        await consumer.webhook_sender.stop()
        await client.aclose()
        await lifespan.__aexit__(None, None, None)
        receiver.stop()
        await asyncio.to_thread(rabbit.stop)
        await asyncio.to_thread(postgres.stop)
