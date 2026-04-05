"""Acquire stage — download COGCC/ECMC production data files."""

from __future__ import annotations

import logging
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import IO

import requests
import yaml
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """Return a configured logger with a single StreamHandler (no duplicates)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


_log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_REQUIRED_KEYS = {"acquire", "ingest", "transform", "features"}


def load_config(config_path: str = "config.yaml") -> dict:
    """Load and return the full pipeline configuration from *config_path*."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    try:
        with path.open() as fh:
            cfg = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"YAML parse error in {config_path}: {exc}") from exc
    missing = _REQUIRED_KEYS - set(cfg or {})
    if missing:
        raise KeyError(f"Config missing required top-level keys: {missing}")
    return cfg


# ---------------------------------------------------------------------------
# HTTP downloader with retry
# ---------------------------------------------------------------------------

def download_with_retry(url: str, dest_path: Path, cfg: dict) -> bool:
    """Download *url* to *dest_path* with exponential back-off retry.

    Returns True on success, False after all retries are exhausted.
    If *dest_path* already exists and has size > 0, skip download (idempotent).
    """
    if dest_path.exists() and dest_path.stat().st_size > 0:
        _log.info("Skip (exists): %s", dest_path)
        return True

    attempts = int(cfg.get("retry_attempts", 3))
    backoff_base = float(cfg.get("retry_backoff_base", 1))

    for attempt in range(attempts):
        try:
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with dest_path.open("wb") as fh:
                    _write_stream(resp, fh)
            except Exception:
                if dest_path.exists():
                    dest_path.unlink(missing_ok=True)
                raise
            _log.info("Downloaded: %s → %s", url, dest_path)
            return True
        except requests.RequestException as exc:
            wait = backoff_base * (2**attempt)
            _log.warning("Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                         attempt + 1, attempts, url, exc, wait)
            time.sleep(wait)

    _log.error("Could not download %s after %d attempts", url, attempts)
    if dest_path.exists():
        dest_path.unlink(missing_ok=True)
    return False


def _write_stream(resp: requests.Response, fh: IO[bytes]) -> None:
    for chunk in resp.iter_content(chunk_size=8192):
        if chunk:
            fh.write(chunk)


# ---------------------------------------------------------------------------
# Data dictionary
# ---------------------------------------------------------------------------

def fetch_data_dictionary(cfg: dict) -> Path:
    """Fetch the ECMC data dictionary HTML and write a Markdown reference file."""
    url = cfg["data_dict_url"]
    refs_dir = Path(cfg.get("references_dir", "references"))
    refs_dir.mkdir(parents=True, exist_ok=True)
    out_path = refs_dir / "production-data-dictionary.md"

    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            raise requests.HTTPError(f"HTTP {resp.status_code}")
    except requests.RequestException as exc:
        _log.error("Failed to fetch data dictionary from %s: %s", url, exc)
        raise RuntimeError("Failed to fetch data dictionary") from exc

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")

    lines = ["# COGCC Production Data Dictionary\n",
             "| Column Name | Description |\n",
             "|-------------|-------------|\n"]

    if table:
        for row in table.find_all("tr"):
            cols = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cols) >= 2:
                lines.append(f"| {cols[0]} | {cols[1]} |\n")

    with out_path.open("w", encoding="utf-8") as fh:
        fh.writelines(lines)

    _log.info("Data dictionary written to %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# Annual zip downloader
# ---------------------------------------------------------------------------

def download_annual_zip(year: int, cfg: dict) -> Path | None:
    """Download and extract annual production zip for *year*.

    Returns path to extracted CSV, or None on failure.
    """
    raw_dir = Path(cfg["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    csv_path = raw_dir / f"{year}_prod_reports.csv"
    if csv_path.exists() and csv_path.stat().st_size > 0:
        _log.info("Skip (exists): %s", csv_path)
        return csv_path

    zip_url = f"{cfg['base_url']}/{year}_prod_reports.zip"
    zip_path = raw_dir / f"{year}_prod_reports.zip"

    success = download_with_retry(zip_url, zip_path, cfg)
    if not success:
        _log.error("Failed to download zip for year %d", year)
        return None

    time.sleep(float(cfg.get("sleep_seconds", 0.5)))

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_member = _find_csv_member(zf, year)
            zf.extract(csv_member, raw_dir)
            extracted = raw_dir / csv_member
            if extracted != csv_path:
                extracted.rename(csv_path)
        _log.info("Extracted CSV for year %d → %s", year, csv_path)
    except (zipfile.BadZipFile, ValueError) as exc:
        _log.error("Zip extraction failed for year %d: %s", year, exc)
        return None
    finally:
        if zip_path.exists():
            zip_path.unlink(missing_ok=True)

    return csv_path


def _find_csv_member(zf: zipfile.ZipFile, year: int) -> str:
    """Return the name of the CSV member matching *prod_reports* (case-insensitive)."""
    matches = [
        m for m in zf.namelist()
        if m.lower().endswith(".csv") and "prod_reports" in m.lower()
    ]
    if not matches:
        raise ValueError(f"No CSV found in zip for {year}")
    return matches[0]


# ---------------------------------------------------------------------------
# Monthly CSV downloader (current year)
# ---------------------------------------------------------------------------

def download_monthly_csv(cfg: dict) -> Path | None:
    """Download the live monthly production CSV (always re-downloaded)."""
    raw_dir = Path(cfg["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / "monthly_prod.csv"

    # Force re-download by temporarily removing the file
    if dest.exists():
        dest.unlink()

    success = download_with_retry(cfg["monthly_url"], dest, cfg)
    if not success:
        _log.error("Failed to download monthly CSV")
        return None

    time.sleep(float(cfg.get("sleep_seconds", 0.5)))
    return dest


# ---------------------------------------------------------------------------
# Parallel orchestrator
# ---------------------------------------------------------------------------

def run_acquire(config_path: str = "config.yaml") -> list[Path]:
    """Download all ECMC production files in parallel. Returns list of CSV paths."""
    cfg = load_config(config_path)
    acq = cfg["acquire"]

    raw_dir = Path(acq["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Fetch data dictionary first (sequential, once)
    try:
        fetch_data_dictionary(acq)
    except RuntimeError as exc:
        _log.warning("Data dictionary fetch failed: %s — continuing", exc)

    current_year = datetime.now().year
    historical_years = list(range(int(acq["start_year"]), current_year))
    max_workers = int(acq.get("max_workers", 5))

    results: list[Path] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(download_annual_zip, yr, acq): yr
            for yr in historical_years
        }
        futures[pool.submit(download_monthly_csv, acq)] = current_year

        for fut in as_completed(futures):
            yr = futures[fut]
            try:
                path = fut.result()
                if path is not None:
                    results.append(path)
                    _log.info("Acquired: %s (year %s)", path, yr)
                else:
                    _log.warning("Acquire returned None for year %s", yr)
            except Exception as exc:  # noqa: BLE001
                _log.error("Acquire failed for year %s: %s", yr, exc)

    _log.info("Acquire complete: %d/%d files acquired",
              len(results), len(historical_years) + 1)
    return results


if __name__ == "__main__":
    run_acquire()
