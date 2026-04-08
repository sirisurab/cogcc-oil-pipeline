"""Acquire stage: download COGCC annual production ZIP/CSV files."""

from __future__ import annotations

import hashlib
import json
import logging
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import dask
import requests
import yaml

logger = logging.getLogger(__name__)

REQUIRED_CONFIG_SECTIONS = ["acquire", "ingest", "transform", "features", "dask", "logging"]


# ---------------------------------------------------------------------------
# A-02: Logging setup
# ---------------------------------------------------------------------------

def setup_logging(config: dict) -> logging.Logger:
    """Configure root logger writing to stdout and a log file.

    Args:
        config: Full config dict loaded from config.yaml.

    Returns:
        Configured Logger instance.

    Raises:
        OSError: If the logs directory cannot be created.
    """
    log_cfg = config["logging"]
    log_file = Path(log_cfg["log_file"])
    level_str: str = log_cfg.get("level", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)

    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        raise OSError(f"Cannot create logs directory {log_file.parent}: {exc}") from exc

    root = logging.getLogger()
    root.setLevel(level)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s")

    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
               for h in root.handlers):
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)

    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) ==
               str(log_file.resolve()) for h in root.handlers):
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        root.addHandler(fh)

    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# A-03: Configuration loader
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
    """Load config.yaml and validate required sections exist.

    Args:
        config_path: Path to config.yaml.

    Returns:
        Dict with full configuration.

    Raises:
        FileNotFoundError: If file does not exist.
        ValueError: On YAML parse error.
        KeyError: If a required section is missing.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    try:
        with config_path.open() as f:
            cfg: dict = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ValueError(f"YAML parse error in {config_path}: {exc}") from exc

    for section in REQUIRED_CONFIG_SECTIONS:
        if section not in cfg:
            raise KeyError(f"Missing required config section: '{section}'")
    return cfg


# ---------------------------------------------------------------------------
# A-04: MD5 checksum
# ---------------------------------------------------------------------------

def compute_md5(file_path: Path) -> str:
    """Compute MD5 hexdigest of a file using 8 KB chunks.

    Args:
        file_path: Path to the file.

    Returns:
        Lowercase hex MD5 string.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    h = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# A-05: HTTP file downloader with retry
# ---------------------------------------------------------------------------

def download_file(
    url: str,
    dest_path: Path,
    retry_max_attempts: int,
    retry_backoff_factor: float,
) -> Path:
    """Download a file from url to dest_path with exponential backoff retry.

    Args:
        url: HTTP URL to download.
        dest_path: Local path where file will be written.
        retry_max_attempts: Maximum number of download attempts.
        retry_backoff_factor: Base multiplier for exponential backoff.

    Returns:
        dest_path after successful write.

    Raises:
        RuntimeError: If all attempts fail or HTTP status >= 400 on final attempt.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception = RuntimeError("No attempts made")

    for attempt in range(retry_max_attempts):
        try:
            resp = requests.get(url, stream=True, timeout=60)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code} for {url}")
            with dest_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            time.sleep(0.5)
            logger.info("Downloaded %s → %s", url, dest_path)
            return dest_path
        except RuntimeError:
            raise
        except Exception as exc:
            last_exc = exc
            wait = retry_backoff_factor * (2 ** attempt)
            logger.warning("Attempt %d/%d failed for %s: %s. Retrying in %.1fs",
                           attempt + 1, retry_max_attempts, url, exc, wait)
            time.sleep(wait)

    raise RuntimeError(f"All {retry_max_attempts} attempts failed for {url}: {last_exc}")


# ---------------------------------------------------------------------------
# A-06: ZIP extractor
# ---------------------------------------------------------------------------

def extract_csv_from_zip(zip_path: Path, dest_dir: Path) -> Path:
    """Extract the first CSV file from a ZIP archive, then delete the ZIP.

    Args:
        zip_path: Path to the ZIP file.
        dest_dir: Directory where the CSV should be placed.

    Returns:
        Path to the extracted CSV file.

    Raises:
        ValueError: If the ZIP contains no CSV files.
        zipfile.BadZipFile: If the ZIP is corrupted.
    """
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV file found in ZIP: {zip_path.name}")
        csv_name = csv_names[0]
        zf.extract(csv_name, dest_dir)
        extracted = dest_dir / csv_name

    zip_path.unlink()
    logger.info("Extracted %s from %s", extracted, zip_path.name)
    return extracted


# ---------------------------------------------------------------------------
# A-07: Download manifest writer
# ---------------------------------------------------------------------------

def update_manifest(manifest_path: Path, entry: dict) -> None:
    """Read the manifest JSON, upsert entry by filename, and write back.

    Args:
        manifest_path: Path to the JSON manifest file.
        entry: Dict containing url, filename, size_bytes, download_timestamp, md5_checksum.
    """
    manifest: dict = {}
    if manifest_path.exists():
        try:
            with manifest_path.open() as f:
                manifest = json.load(f)
        except json.JSONDecodeError:
            logger.warning("Corrupt manifest at %s — overwriting.", manifest_path)
            manifest = {}

    manifest[entry["filename"]] = entry
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w") as f:
        json.dump(manifest, f, indent=2)
    logger.debug("Manifest updated: %s", manifest_path)


# ---------------------------------------------------------------------------
# A-08: Per-year acquire worker
# ---------------------------------------------------------------------------

def acquire_year(year: int, config: dict) -> dict:
    """Acquire production data for a single calendar year.

    Skips download if the target CSV already exists (idempotent).

    Args:
        year: The year to acquire.
        config: Full config dict.

    Returns:
        Manifest entry dict.
    """
    current_year = datetime.now(tz=timezone.utc).year
    acfg = config["acquire"]
    raw_dir = Path(acfg["raw_dir"])
    manifest_path = raw_dir / "download_manifest.json"

    if year == current_year:
        url: str = acfg["monthly_url"]
        dest_csv = raw_dir / "monthly_prod.csv"
    else:
        url = acfg["zip_url_template"].format(year=year)
        dest_csv = raw_dir / f"{year}_prod_reports.csv"

    filename = dest_csv.name

    if dest_csv.exists():
        logger.info("Skipping %s, already exists", filename)
        return {
            "url": url,
            "filename": filename,
            "size_bytes": dest_csv.stat().st_size,
            "download_timestamp": None,
            "md5_checksum": None,
            "skipped": True,
        }

    raw_dir.mkdir(parents=True, exist_ok=True)

    try:
        if year == current_year:
            download_file(
                url, dest_csv,
                acfg["retry_max_attempts"],
                acfg["retry_backoff_factor"],
            )
        else:
            zip_dest = raw_dir / f"{year}_prod_reports.zip"
            download_file(
                url, zip_dest,
                acfg["retry_max_attempts"],
                acfg["retry_backoff_factor"],
            )
            dest_csv = extract_csv_from_zip(zip_dest, raw_dir)
            filename = dest_csv.name
    except Exception:
        logger.exception("Failed to acquire year %d", year)
        raise

    md5 = compute_md5(dest_csv)
    entry = {
        "url": url,
        "filename": filename,
        "size_bytes": dest_csv.stat().st_size,
        "download_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "md5_checksum": md5,
        "skipped": False,
    }
    update_manifest(manifest_path, entry)
    time.sleep(0.5)
    return entry


# ---------------------------------------------------------------------------
# A-09: Acquire stage runner
# ---------------------------------------------------------------------------

def run_acquire(config: dict) -> list[dict]:
    """Run the acquire stage for all years from year_start to current year.

    Uses Dask threaded scheduler for parallel downloads.

    Args:
        config: Full config dict.

    Returns:
        List of manifest entry dicts (one per year).
    """
    acfg = config["acquire"]
    current_year = datetime.now(tz=timezone.utc).year
    years = list(range(acfg["year_start"], current_year + 1))
    max_workers = min(int(acfg["max_workers"]), 5)

    raw_dir = Path(acfg["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    delayed_tasks = [dask.delayed(acquire_year)(yr, config) for yr in years]

    logger.info("Acquiring %d year(s): %s … %s", len(years), years[0], years[-1])
    results_tuple = dask.compute(
        *delayed_tasks,
        scheduler="threads",
        num_workers=max_workers,
    )

    entries: list[dict] = []
    failed: list[int] = []
    skipped = 0
    downloaded = 0

    for yr, result in zip(years, results_tuple):
        if isinstance(result, Exception):
            logger.error("Year %d failed: %s", yr, result)
            failed.append(yr)
        else:
            entries.append(result)
            if result.get("skipped"):
                skipped += 1
            else:
                downloaded += 1

    logger.info(
        "Acquire complete: %d attempted, %d downloaded, %d skipped, %d failed",
        len(years), downloaded, skipped, len(failed),
    )
    return entries
