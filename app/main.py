"""FastAPI application entrypoint."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import payments_router
from app.broker import broker, declare_topology
from app.db import SessionFactory
from app.logging_setup import configure_logging
from app.outbox import OutboxPublisher

configure_logging()
logger = logging.getLogger("payments.api")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await broker.start()
    await declare_topology(broker)

    publisher = OutboxPublisher(SessionFactory, broker)
    task = asyncio.create_task(publisher.run())

    logger.info("API started")
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await broker.stop()
        logger.info("API stopped")


app = FastAPI(
    title="Payment Processing Service",
    version="1.0.0",
    description=(
        "Asynchronous payment processing with a transactional outbox, "
        "retries and a dead letter queue."
    ),
    lifespan=lifespan,
)

app.include_router(payments_router)


@app.get("/health", tags=["system"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
