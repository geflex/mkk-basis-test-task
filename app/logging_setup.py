"""Единая настройка логирования для точек входа процессов."""

from __future__ import annotations

import logging

_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Настраивает корневой логгер один раз для точки входа процесса."""

    logging.basicConfig(level=level, format=_LOG_FORMAT)
