"""Настройки приложения, сгруппированные по зонам ответственности.

Каждая группа читает свои переменные окружения независимо. Это позволяет
процессу API не требовать настройки consumer'а и наоборот, а также не тащить
глобальный мутабельный ``Settings`` через все модули.

Имена переменных окружения сохранены прежними (``DATABASE_URL``,
``RABBITMQ_URL``, ``WEBHOOK_TIMEOUT`` и т.д.).
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _model_config(env_prefix: str = "") -> SettingsConfigDict:
    return SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        env_prefix=env_prefix,
        populate_by_name=True,
    )


class DatabaseSettings(BaseSettings):
    """Параметры подключения к PostgreSQL."""

    model_config = _model_config(env_prefix="database_")

    url: str = Field(
        default="postgresql+asyncpg://payments:payments@localhost:5432/payments"
    )
    pool_size: int = Field(default=10, ge=1)
    max_overflow: int = Field(default=20, ge=0)
    pool_recycle: float = Field(default=1800, gt=0)
    command_timeout: float = Field(default=30, gt=0)


class BrokerSettings(BaseSettings):
    """Подключение к RabbitMQ и имена сущностей топологии."""

    model_config = _model_config()

    rabbitmq_url: str = Field(default="amqp://guest:guest@localhost:5672/")
    payments_exchange: str = Field(default="payments")
    payments_new_queue: str = Field(default="payments.new")
    payments_dlq_queue: str = Field(default="payments.dlq")
    retry_routing_prefix: str = Field(default="payments.retry")
    #: Задержка перед первым повтором; каждая следующая попытка удваивает её.
    base_retry_delay_ms: int = Field(default=2000, ge=1)


class OutboxSettings(BaseSettings):
    """Параметры фонового публикатора outbox."""

    model_config = _model_config(env_prefix="outbox_")

    poll_interval: float = Field(default=1.0, gt=0)
    batch_size: int = Field(default=100, ge=1, le=1000)


class ProcessingSettings(BaseSettings):
    """Эмуляция обработки платежа и параметры retry."""

    model_config = _model_config()

    max_attempts: int = Field(default=3, ge=1, le=20)
    processing_min_seconds: float = Field(default=2.0, ge=0)
    processing_max_seconds: float = Field(default=5.0, ge=0)
    success_rate: float = Field(default=0.9, ge=0, le=1)

    @model_validator(mode="after")
    def _validate_processing_window(self) -> "ProcessingSettings":
        if self.processing_min_seconds > self.processing_max_seconds:
            raise ValueError(
                "processing_min_seconds не может быть больше processing_max_seconds"
            )
        return self


class WebhookSettings(BaseSettings):
    """Параметры доставки webhook-уведомлений."""

    model_config = _model_config(env_prefix="webhook_")

    timeout: float = Field(default=10.0, gt=0)
    max_retries: int = Field(default=3, ge=1, le=10)
    retry_delay: float = Field(default=1.0, gt=0)
    max_response_bytes: int = Field(default=1_048_576, ge=1)
    secret: SecretStr | None = Field(default=None)
    #: Отклонять webhook-адреса на приватные/loopback/link-local хосты.
    allow_private_hosts: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "ALLOW_PRIVATE_WEBHOOK_HOSTS", "allow_private_hosts"
        ),
    )

    @field_validator("secret", mode="before")
    @classmethod
    def _empty_secret_to_none(cls, value: object) -> object:
        # ``WEBHOOK_SECRET=`` в .env приходит пустой строкой, а не None.
        if value == "":
            return None
        return value


class ApiKeySettings(BaseSettings):
    """Статический ключ доступа к HTTP API."""

    model_config = _model_config()

    api_key: SecretStr

    @field_validator("api_key")
    @classmethod
    def _reject_empty_key(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value():
            raise ValueError("API_KEY не может быть пустым")
        return value


class LoggingSettings(BaseSettings):
    """Формат и уровень логирования."""

    model_config = _model_config()

    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=False)


@lru_cache
def get_database_settings() -> DatabaseSettings:
    return DatabaseSettings()


@lru_cache
def get_broker_settings() -> BrokerSettings:
    return BrokerSettings()


@lru_cache
def get_outbox_settings() -> OutboxSettings:
    return OutboxSettings()


@lru_cache
def get_processing_settings() -> ProcessingSettings:
    return ProcessingSettings()


@lru_cache
def get_webhook_settings() -> WebhookSettings:
    return WebhookSettings()


@lru_cache
def get_api_key_settings() -> ApiKeySettings:
    return ApiKeySettings()


@lru_cache
def get_logging_settings() -> LoggingSettings:
    return LoggingSettings()
