"""Application configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings.

    Every value can be overridden with an environment variable of the same
    (upper-case) name or via a local ``.env`` file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- HTTP API ---------------------------------------------------------
    api_key: str = Field(default="super-secret-api-key")

    # --- Infrastructure ---------------------------------------------------
    database_url: str = Field(
        default="postgresql+asyncpg://payments:payments@localhost:5432/payments"
    )
    rabbitmq_url: str = Field(default="amqp://guest:guest@localhost:5672/")

    # --- Broker topology --------------------------------------------------
    payments_exchange: str = Field(default="payments")
    payments_new_queue: str = Field(default="payments.new")
    payments_dlq_queue: str = Field(default="payments.dlq")
    retry_routing_prefix: str = Field(default="payments.retry")
    #: Delay before the first retry; each next retry doubles it.
    base_retry_delay_ms: int = Field(default=2000)

    # --- Outbox -----------------------------------------------------------
    outbox_poll_interval: float = Field(default=1.0)
    outbox_batch_size: int = Field(default=100)

    # --- Processing emulation --------------------------------------------
    max_attempts: int = Field(default=3)
    processing_min_seconds: float = Field(default=2.0)
    processing_max_seconds: float = Field(default=5.0)
    success_rate: float = Field(default=0.9)

    # --- Webhooks ---------------------------------------------------------
    webhook_timeout: float = Field(default=10.0)
    webhook_max_retries: int = Field(default=3)
    webhook_retry_delay: float = Field(default=1.0)


@lru_cache
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()


settings = get_settings()
