"""Общая конфигурация pytest.

Настройки валидируются во время импорта, а у ``API_KEY`` нет значения по
умолчанию, поэтому тестовое значение должно быть задано до импорта любого
модуля ``app.*``.
"""

from __future__ import annotations

import os

os.environ.setdefault("API_KEY", "test-api-key")
