"""Асинхронный движок БД и управление сессиями."""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_database_settings

_db = get_database_settings()

engine: AsyncEngine = create_async_engine(
    _db.url,
    pool_pre_ping=True,
    pool_size=_db.pool_size,
    max_overflow=_db.max_overflow,
    pool_recycle=_db.pool_recycle,
    connect_args={"command_timeout": _db.command_timeout},
)

SessionFactory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Зависимость FastAPI, возвращающая сессию в рамках запроса."""

    async with SessionFactory() as session:
        yield session


async def dispose_engine() -> None:
    """Закрывает пул соединений при остановке процесса."""

    await engine.dispose()