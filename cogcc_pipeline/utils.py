"""Shared utilities: config loader, logger factory."""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path) -> dict[str, Any]:
    """Load a YAML configuration file and return the parsed dictionary.

    Resolves relative paths against the current working directory.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    path = Path(config_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)  # type: ignore[no-any-return]


def get_logger(
    name: str,
    log_dir: str,
    log_file: str,
    level: str = "INFO",
) -> logging.Logger:
    """Create and return a logger that writes to stdout and a rotating file.

    Args:
        name: Logger name (typically the module name).
        log_dir: Directory for the log file; created if absent.
        log_file: Log file name within log_dir.
        level: Logging level string (e.g. "INFO", "DEBUG").

    Returns:
        Configured :class:`logging.Logger` instance.
    """
    os.makedirs(log_dir, exist_ok=True)
    log_level = getattr(logging, level.upper(), logging.INFO)
    fmt = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    formatter = logging.Formatter(fmt)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Avoid adding duplicate handlers when called multiple times
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(log_level)
        logger.addHandler(stream_handler)

        file_handler = RotatingFileHandler(
            Path(log_dir) / log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)

    return logger
