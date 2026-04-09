"""Acquire stage: download COGCC/ECMC production data files."""

from __future__ import annotations

import logging
import sys
import time
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path

import dask
import requests
import yaml

logger = logging.getLogger(__name__)

REQUIRED_CONFIG_KEYS = ["acquire", "ingest", "transform", "features", "dask", "logging"]


class AcquireError(RuntimeError):
    """Raised on unrecoverable download or extraction errors."""


def load_config(config_path: str = "config.yaml") -> dict:
    """Read and return the parsed YAML config dict.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If any required top-level key is absent.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with path.open("r") as f:
        config: dict = yaml.safe_load(f)
    missing = [k for k in REQUIRED_CONFIG_KEYS if k not in config]
    if missing:
        raise ValueError(f"Config missing required sections: {missing}")
    return config


def is_valid_file(path: Path) -> bool:
    """Return True if path exists, is non-empty, is UTF-8 readable, and has ≥2 lines."""
    try:
        if not path.exists():
            return False
        if path.stat().st_size == 0:
            return False
        text = path.read_text(encoding="utf-8")
        lines = [ln for ln in text.splitlines() if ln.strip()]
        return len(lines) >= 2
    except (UnicodeDecodeError, OSError):
        return False


def download_year_zip(year: int, config: dict) -> Path:
    """Download the annual production ZIP for a historical year and extract its CSV.

    Returns:
        Path to the extracted CSV file.

    Raises:
        AcquireError: On network or ZIP extraction failures.
    """
    raw_dir = Path(config["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_path = raw_dir / f"{year}_prod_reports.csv"

    if is_valid_file(output_path):
        logger.debug("Skipping %s — already valid", output_path)
        return output_path

    url = config["acquire"]["zip_url_template"].format(year=year)
    logger.info("Downloading ZIP for year %d from %s", year, url)

    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        content = response.content
    except requests.RequestException as exc:
        logger.error("Download failed for year %d: %s", year, exc)
        raise AcquireError(f"Download failed for year {year}: {exc}") from exc

    try:
        with zipfile.ZipFile(BytesIO(content)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise AcquireError(f"No CSV found in ZIP for year {year}")
            with zf.open(csv_names[0]) as src, output_path.open("wb") as dst:
                dst.write(src.read())
    except zipfile.BadZipFile as exc:
        logger.error("Bad ZIP file for year %d: %s", year, exc)
        raise AcquireError(f"Bad ZIP file for year {year}: {exc}") from exc

    time.sleep(config["acquire"]["sleep_seconds"])

    if not is_valid_file(output_path):
        logger.warning("Downloaded file for year %d failed validity check: %s", year, output_path)

    return output_path


def download_monthly_csv(config: dict) -> Path:
    """Download the current-year monthly production CSV directly (no ZIP).

    Returns:
        Path to the downloaded CSV file.

    Raises:
        AcquireError: On network failures.
    """
    raw_dir = Path(config["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_path = raw_dir / "monthly_prod.csv"

    if is_valid_file(output_path):
        logger.debug("Skipping monthly_prod.csv — already valid")
        return output_path

    url = config["acquire"]["monthly_url"]
    logger.info("Downloading monthly CSV from %s", url)

    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        output_path.write_bytes(response.content)
    except requests.RequestException as exc:
        logger.error("Download failed for monthly CSV: %s", exc)
        raise AcquireError(f"Download failed for monthly CSV: {exc}") from exc

    time.sleep(config["acquire"]["sleep_seconds"])

    if not is_valid_file(output_path):
        logger.warning("Downloaded monthly CSV failed validity check: %s", output_path)

    return output_path


def run_acquire(config: dict) -> list[Path]:
    """Orchestrate parallel downloads for all target years using the Dask threaded scheduler.

    Returns:
        List of Path objects for all downloaded CSV files.
    """
    current_year = datetime.now().year
    start_year = config["acquire"]["start_year"]
    max_workers = config["acquire"]["max_workers"]

    historical_years = list(range(start_year, current_year))
    delayed_list = [dask.delayed(download_year_zip)(year, config) for year in historical_years]
    delayed_list.append(dask.delayed(download_monthly_csv)(config))

    results = dask.compute(*delayed_list, scheduler="threads", num_workers=max_workers)
    paths: list[Path] = list(results)

    logger.info("Acquired %d files: %s", len(paths), [str(p) for p in paths])
    return paths


def setup_logging(config: dict) -> logging.Logger:
    """Configure root logger with stdout and file handlers.

    Safe to call multiple times — will not duplicate handlers.

    Returns:
        The configured root logger.
    """
    log_cfg = config["logging"]
    level_str: str = log_cfg.get("level", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)
    log_file = log_cfg.get("log_file", "logs/pipeline.log")

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if len(root_logger.handlers) >= 2:
        return root_logger

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    if not any(isinstance(h, logging.StreamHandler) and h.stream is sys.stdout for h in root_logger.handlers):
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(fmt)
        root_logger.addHandler(sh)

    if not any(isinstance(h, logging.FileHandler) for h in root_logger.handlers):
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        fh.setFormatter(fmt)
        root_logger.addHandler(fh)

    return root_logger
