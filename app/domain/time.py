"""Утилиты времени."""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    """Текущее время UTC с часовым поясом."""

    return datetime.now(timezone.utc)