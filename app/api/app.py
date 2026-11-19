"""Сборка FastAPI-приложения."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.routers import payments_router
from app.core.config import get_api_key_settings, get_processing_settings
from app.core.logging import configure_logging, request_id_var
from app.db.session import SessionFactory, dispose_engine
from app.messaging.broker import broker, declare_topology
from app.messaging.outbox import OutboxPublisher

logger = logging.getLogger("payments.api")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    # Падаем на старте, если API_KEY не задан, вместо 401 на каждый запрос.
    get_api_key_settings()

    await broker.start()
    await declare_topology(broker, get_processing_settings().max_attempts)

    publisher = OutboxPublisher(SessionFactory, broker)
    task = asyncio.create_task(publisher.run())

    logger.info("API запущен")
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await broker.stop()
        await dispose_engine()
        logger.info("API остановлен")


async def _database_ready() -> bool:
    """Проверяет доступность БД простым запросом ``SELECT 1``."""

    try:
        async with SessionFactory() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001 - readiness не должен падать сам
        logger.exception("Readiness: проверка БД не прошла")
        return False


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title="Сервис процессинга платежей",
        version="1.0.0",
        description=(
            "Асинхронная обработка платежей с транзакционным outbox, "
            "повторами и dead letter queue."
        ),
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def request_context(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        token = request_id_var.set(request_id)
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            request_id_var.reset(token)

    app.include_router(payments_router)

    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/live", tags=["system"])
    async def health_live() -> dict[str, str]:
        """Liveness: процесс жив и отвечает (без обращения к зависимостям)."""

        return {"status": "ok"}

    @app.get("/health/ready", tags=["system"])
    async def health_ready() -> JSONResponse:
        """Readiness: БД и брокер доступны, можно принимать трафик."""

        database_ok = await _database_ready()
        broker_ok = await broker.ping(timeout=2.0)
        ready = database_ok and broker_ok
        return JSONResponse(
            {
                "status": "ok" if ready else "unavailable",
                "database": "ok" if database_ok else "fail",
                "broker": "ok" if broker_ok else "fail",
            },
            status_code=200 if ready else 503,
        )

    return app


app = create_app()