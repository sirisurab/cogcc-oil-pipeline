"""Acquire stage: download COGCC production data files."""

import logging
import time
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path

import dask
import requests
import yaml

logger = logging.getLogger(__name__)

REQUIRED_ACQUIRE_KEYS = [
    "zip_url_template",
    "monthly_url",
    "start_year",
    "raw_dir",
    "max_workers",
    "sleep_seconds",
]


def load_config(config_path: str) -> dict:
    """Read config.yaml and return the acquire section."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    acquire_cfg: dict = cfg.get("acquire", {})
    for key in REQUIRED_ACQUIRE_KEYS:
        if key not in acquire_cfg:
            raise KeyError(f"Missing required acquire config key: '{key}'")
    return acquire_cfg


def build_download_targets(config: dict) -> list[dict]:
    """Build list of download target descriptors for all configured years."""
    current_year = datetime.now().year
    start_year: int = config["start_year"]
    zip_template: str = config["zip_url_template"]
    monthly_url: str = config["monthly_url"]
    raw_dir: str = config["raw_dir"]

    targets: list[dict] = []
    for year in range(start_year, current_year + 1):
        if year == current_year:
            url = monthly_url
            dest = str(Path(raw_dir) / "monthly_prod.csv")
        else:
            url = zip_template.format(year=year)
            dest = str(Path(raw_dir) / f"{year}_prod_reports.csv")
        targets.append({"url": url, "dest": dest, "year": year})
    return targets


def download_file(target: dict, sleep_seconds: float) -> Path | None:
    """Download a single file described by target to its destination path.

    For zip targets, extracts CSV in-memory. For monthly CSV targets, writes
    response body directly. Returns None on any failure; never leaves a partial
    file at the destination.
    """
    dest = Path(target["dest"])
    url: str = target["url"]
    year: int = target["year"]

    if dest.exists():
        time.sleep(sleep_seconds)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()

        content = response.content
        if len(content) == 0:
            logger.warning("Empty response body for year=%s url=%s", year, url)
            time.sleep(sleep_seconds)
            return None

        if url.endswith(".zip"):
            with zipfile.ZipFile(BytesIO(content)) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    logger.warning("No CSV found in zip for year=%s url=%s", year, url)
                    time.sleep(sleep_seconds)
                    return None
                csv_data = zf.read(csv_names[0])
            dest.write_bytes(csv_data)
        else:
            dest.write_bytes(content)

    except Exception as exc:
        logger.warning("Download failed for year=%s url=%s: %s", year, url, exc)
        if dest.exists():
            dest.unlink()
        time.sleep(sleep_seconds)
        return None

    time.sleep(sleep_seconds)
    return dest


def acquire(config: dict) -> list[Path | None]:
    """Orchestrate the full acquire stage using Dask threaded scheduler (ADR-001)."""
    acquire_cfg = config if "raw_dir" in config else config.get("acquire", config)
    raw_dir = Path(acquire_cfg["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    targets = build_download_targets(acquire_cfg)
    sleep_seconds: float = float(acquire_cfg["sleep_seconds"])
    max_workers: int = int(acquire_cfg["max_workers"])

    delayed_tasks = [
        dask.delayed(download_file)(target, sleep_seconds) for target in targets
    ]
    # ADR-001: acquire is I/O-bound — use threaded scheduler
    results: list[Path | None] = dask.compute(
        *delayed_tasks,
        scheduler="threads",
        num_workers=max_workers,
    )
    return list(results)
