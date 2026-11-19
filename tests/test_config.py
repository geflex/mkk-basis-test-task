"""Тесты валидации конфигурации."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.core.config import ApiKeySettings, ProcessingSettings, WebhookSettings


def test_processing_rejects_inverted_window() -> None:
    with pytest.raises(ValidationError):
        ProcessingSettings(processing_min_seconds=5, processing_max_seconds=1)


def test_processing_rejects_success_rate_out_of_range() -> None:
    with pytest.raises(ValidationError):
        ProcessingSettings(success_rate=1.5)


def test_webhook_empty_secret_becomes_none() -> None:
    assert WebhookSettings(secret="").secret is None


def test_api_key_rejects_empty_value() -> None:
    with pytest.raises(ValidationError):
        ApiKeySettings(api_key="")