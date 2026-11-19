"""Аутентификация по статическому API-ключу."""

from __future__ import annotations

import secrets

from fastapi import Header, HTTPException, status

from app.core.config import get_api_key_settings


async def require_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    """Зависимость FastAPI, проверяющая заголовок ``X-API-Key``."""

    expected = get_api_key_settings().api_key.get_secret_value()
    if not x_api_key or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Отсутствует или неверный заголовок X-API-Key",
        )