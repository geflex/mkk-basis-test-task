"""Shared pytest configuration.

Settings are validated at import time and ``API_KEY`` has no default, so make
sure a test value is present before any ``app.*`` module is imported.
"""

from __future__ import annotations

import os

os.environ.setdefault("API_KEY", "test-api-key")
