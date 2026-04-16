"""Shared utility functions used across all pipeline stages."""

import datetime
import logging
import logging.handlers
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_REQUIRED_SECTIONS = ("acquire", "dask", "logging")


def load_config(config_path: str) -> dict:
    """Load config/config.yaml and return the full configuration dictionary.

    Resolves acquire.end_year from null to the current calendar year.

    Raises:
        FileNotFoundError: if config_path does not exist.
        KeyError: if any required top-level section is missing.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path) as fh:
        cfg: dict = yaml.safe_load(fh) or {}

    for section in _REQUIRED_SECTIONS:
        if section not in cfg:
            raise KeyError(section)

    if cfg.get("acquire", {}).get("end_year") is None:
        cfg["acquire"]["end_year"] = datetime.date.today().year

    return cfg


def setup_logging(log_file: str, level: str) -> None:
    """Configure the root logger with console and file handlers.

    Idempotent — calling twice does not add duplicate handlers.
    Creates the log directory if it does not exist.

    Raises:
        OSError: if the log directory is unwritable.
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root.setLevel(numeric_level)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s")

    # Avoid adding duplicate handlers (idempotency guard)
    handler_files = {
        getattr(h, "baseFilename", None)
        for h in root.handlers
        if isinstance(h, logging.FileHandler)
    }
    has_stream = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in root.handlers
    )

    if not has_stream:
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)

    abs_log = str(log_path.resolve())
    if abs_log not in handler_files:
        fh = logging.FileHandler(abs_log)
        fh.setFormatter(fmt)
        root.addHandler(fh)


def get_partition_count(n: int) -> int:
    """Return max(10, min(n, 50)) — the standard Parquet partition count for this pipeline."""
    return max(10, min(n, 50))
