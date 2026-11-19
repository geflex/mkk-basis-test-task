"""Central logging configuration for the process entrypoints."""

from __future__ import annotations

import logging

_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the root logger once for a process entrypoint."""

    logging.basicConfig(level=level, format=_LOG_FORMAT)
