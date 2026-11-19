"""End-to-end fixtures.

These tests exercise the whole service against real PostgreSQL and RabbitMQ
instances started with testcontainers, plus a real HTTP webhook receiver. The
API runs in-process on an ASGI transport while the worker consumes through the
shared broker, so a full create -> outbox -> broker -> process -> webhook
round-trip is covered.

They are opt-in and excluded from the default run (``addopts = -m "not e2e"``):

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
    """Threaded HTTP server that records webhook deliveries.

    ``/fail`` always answers ``500`` so the retry/DLQ path can be exercised.
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
            def do_POST(self) -> None:  # noqa: N802 - stdlib naming
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
            # Must yield: the worker runs in the same event loop, so blocking
            # here would prevent the webhook from ever being delivered.
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
    # A plain container with a log-based readiness check avoids the flaky
    # pika probe that the community RabbitMQ image ships with.
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

    # Must be set before any ``app.*`` module is imported: settings are read at
    # import time and the engine is bound to ``DATABASE_URL`` on first import.
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

    import app.consumer  # noqa: F401  - registers the subscriber on the broker
    from app.main import app as fastapi_app

    receiver = WebhookReceiver()
    receiver.start()

    lifespan = fastapi_app.router.lifespan_context(fastapi_app)
    await lifespan.__aenter__()

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
        await client.aclose()
        await lifespan.__aexit__(None, None, None)
        receiver.stop()
        await asyncio.to_thread(rabbit.stop)
        await asyncio.to_thread(postgres.stop)
