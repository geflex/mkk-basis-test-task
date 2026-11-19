"""Логирование с контекстом запроса/платежа и опциональным JSON-форматом."""

from __future__ import annotations

import json
import logging
from contextvars import ContextVar

from app.core.config import LoggingSettings, get_logging_settings

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")
payment_id_var: ContextVar[str] = ContextVar("payment_id", default="-")

_TEXT_FORMAT = (
    "%(asctime)s %(levelname)-8s [req=%(request_id)s pay=%(payment_id)s] "
    "%(name)s: %(message)s"
)


class ContextFilter(logging.Filter):
    """Добавляет в каждую запись идентификаторы из contextvars."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        record.payment_id = payment_id_var.get()
        return True


class JsonFormatter(logging.Formatter):
    """Формат логов, пригодный для сборщиков (ELK/Loki)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", "-"),
            "payment_id": getattr(record, "payment_id", "-"),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(settings: LoggingSettings | None = None) -> None:
    """Настраивает корневой логгер для точки входа процесса.

    Повторный вызов (например, из тестов) заменяет только собственный handler
    приложения, не трогая обработчики, установленные снаружи.
    """

    settings = settings or get_logging_settings()

    handler = logging.StreamHandler()
    handler.addFilter(ContextFilter())
    if settings.log_json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(_TEXT_FORMAT))

    root = logging.getLogger()
    for existing in list(root.handlers):
        if getattr(existing, "_payments_handler", False):
            root.removeHandler(existing)

    handler._payments_handler = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())