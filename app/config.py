"""Конфигурация приложения, загружаемая из переменных окружения."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки времени выполнения.

    Любое значение можно переопределить одноимённой переменной окружения
    (в верхнем регистре) или через локальный файл ``.env``.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- HTTP API ---------------------------------------------------------
    #: Статический ключ, обязательный в заголовке ``X-API-Key``. Значение по
    #: умолчанию намеренно не задано, чтобы неправильно настроенный деплой
    #: падал сразу, а не стартовал с общеизвестным ключом.
    api_key: str = Field(description="Статический API-ключ для заголовка X-API-Key")

    # --- Инфраструктура ---------------------------------------------------
    database_url: str = Field(
        default="postgresql+asyncpg://payments:payments@localhost:5432/payments"
    )
    rabbitmq_url: str = Field(default="amqp://guest:guest@localhost:5672/")

    # --- Топология брокера --------------------------------------------------
    payments_exchange: str = Field(default="payments")
    payments_new_queue: str = Field(default="payments.new")
    payments_dlq_queue: str = Field(default="payments.dlq")
    retry_routing_prefix: str = Field(default="payments.retry")
    #: Задержка перед первым повтором; каждая следующая попытка удваивает её.
    base_retry_delay_ms: int = Field(default=2000)

    # --- Outbox -----------------------------------------------------------
    outbox_poll_interval: float = Field(default=1.0)
    outbox_batch_size: int = Field(default=100)

    # --- Эмуляция обработки --------------------------------------------
    max_attempts: int = Field(default=3)
    processing_min_seconds: float = Field(default=2.0)
    processing_max_seconds: float = Field(default=5.0)
    success_rate: float = Field(default=0.9)

    # --- Webhook'и ---------------------------------------------------------
    webhook_timeout: float = Field(default=10.0)
    webhook_max_retries: int = Field(default=3)
    webhook_retry_delay: float = Field(default=1.0)
    #: Необязательный общий секрет. Если задан, уведомления несут HMAC
    #: ``X-Webhook-Signature``, чтобы получатель мог проверить подлинность.
    webhook_secret: str | None = Field(default=None)
    #: Отклонять webhook-адреса, указывающие на приватные, loopback- или
    #: link-local хосты. Включайте только в локальной разработке, где
    #: уведомления адресованы хосту на той же машине или в локальной сети.
    allow_private_webhook_hosts: bool = Field(default=False)


@lru_cache
def get_settings() -> Settings:
    """Возвращает закэшированный экземпляр настроек."""

    return Settings()


settings = get_settings()
