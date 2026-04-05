"""Acquire stage: download COGCC/ECMC production data files."""

from __future__ import annotations

import io
import logging
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

from cogcc_pipeline.utils import get_logger, load_config


def fetch_data_dictionary(
    url: str,
    output_path: Path,
    logger: logging.Logger,
) -> Path:
    """Download and parse the ECMC data dictionary HTML page into a CSV file.

    If the output file already exists the download is skipped.

    Args:
        url: URL of the HTML data dictionary page.
        output_path: Destination path for the output CSV.
        logger: Configured logger instance.

    Returns:
        Path to the written (or pre-existing) CSV file.

    Raises:
        requests.HTTPError: If the HTTP response status is not 200.
        ValueError: If no HTML table is found on the page.
    """
    if output_path.exists():
        logger.info("Data dictionary already exists, skipping: %s", output_path)
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise requests.HTTPError(
            f"Failed to fetch data dictionary from {url}: HTTP {response.status_code}"
        )

    soup = BeautifulSoup(response.text, "lxml")
    table = soup.find("table")
    if table is None:
        raise ValueError("No table found on data dictionary page")

    headers = [th.get_text(strip=True) for th in table.find_all("th")]  # type: ignore[union-attr]
    rows = []
    for tr in table.find_all("tr")[1:]:  # type: ignore[union-attr]
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if cells:
            rows.append(cells)

    df = pd.DataFrame(rows, columns=headers if headers else None)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Data dictionary written to %s (%d rows)", output_path, len(df))
    return output_path


def download_annual_zip(
    year: int,
    url_template: str,
    raw_dir: Path,
    sleep_seconds: float,
    logger: logging.Logger,
) -> Path | None:
    """Download the annual production ZIP for a given year and extract the CSV.

    Args:
        year: Year to download.
        url_template: URL template with ``{year}`` placeholder.
        raw_dir: Directory where the extracted CSV will be saved.
        sleep_seconds: Seconds to sleep before issuing the HTTP request.
        logger: Configured logger instance.

    Returns:
        Path to the extracted CSV file, or ``None`` on failure.

    Raises:
        OSError: If ``raw_dir`` cannot be created.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    target = raw_dir / f"{year}_prod_reports.csv"

    if target.exists():
        logger.info("Annual CSV already exists, skipping: %s", target)
        return target

    url = url_template.format(year=year)
    logger.info("Downloading annual ZIP for %d from %s", year, url)
    time.sleep(sleep_seconds)

    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        logger.warning(
            "Failed to download %d ZIP: HTTP %d from %s", year, response.status_code, url
        )
        return None

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            logger.warning("ZIP for year %d contains no CSV files", year)
            return None
        with zf.open(csv_names[0]) as csv_fh, target.open("wb") as out_fh:
            out_fh.write(csv_fh.read())

    logger.info("Extracted %s (%d bytes)", target, target.stat().st_size)
    return target


def download_monthly_csv(
    url: str,
    raw_dir: Path,
    sleep_seconds: float,
    logger: logging.Logger,
    force_refresh: bool = False,
) -> Path | None:
    """Download the current-year monthly production CSV.

    Args:
        url: Direct URL to the monthly CSV file.
        raw_dir: Directory where the CSV will be saved.
        sleep_seconds: Seconds to sleep before issuing the HTTP request.
        logger: Configured logger instance.
        force_refresh: When ``True``, re-download even if the file exists.

    Returns:
        Path to the written CSV file, or ``None`` on failure.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    target = raw_dir / "monthly_prod.csv"

    if target.exists() and not force_refresh:
        logger.info("Monthly CSV already exists, skipping: %s", target)
        return target

    logger.info("Downloading monthly CSV from %s", url)
    time.sleep(sleep_seconds)

    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        logger.warning("Failed to download monthly CSV: HTTP %d from %s", response.status_code, url)
        return None

    target.write_bytes(response.content)
    logger.info("Monthly CSV written to %s (%d bytes)", target, target.stat().st_size)
    return target


def run_acquire(config: dict[str, Any], logger: logging.Logger) -> list[Path]:
    """Run the full acquire stage: download all annual ZIPs and the monthly CSV.

    Uses :class:`concurrent.futures.ThreadPoolExecutor` with at most 5 workers.
    Downloads the ECMC data dictionary after all file downloads complete.

    Args:
        config: Pipeline configuration dictionary (top-level).
        logger: Configured logger instance.

    Returns:
        List of successfully downloaded file Paths (None results excluded).
    """
    acfg = config["acquire"]
    raw_dir = Path(acfg["raw_dir"])
    url_template: str = acfg["base_url_zip"]
    monthly_url: str = acfg["base_url_monthly"]
    dict_url: str = acfg["data_dictionary_url"]
    references_dir = Path(acfg["references_dir"])
    start_year: int = acfg["start_year"]
    sleep_seconds: float = acfg["sleep_seconds"]
    max_workers: int = min(int(acfg["max_workers"]), 5)

    current_year = datetime.now().year
    annual_years = list(range(start_year, current_year))

    futures_map = {}
    results: list[Path] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for year in annual_years:
            fut = executor.submit(
                download_annual_zip, year, url_template, raw_dir, sleep_seconds, logger
            )
            futures_map[fut] = f"annual_{year}"

        monthly_fut = executor.submit(
            download_monthly_csv, monthly_url, raw_dir, sleep_seconds, logger
        )
        futures_map[monthly_fut] = "monthly"

        for fut in as_completed(futures_map):
            label = futures_map[fut]
            try:
                path = fut.result()
            except Exception as exc:
                logger.warning("Download task %s raised: %s", label, exc)
                path = None
            if path is None:
                logger.warning("Download task %s returned no file", label)
            else:
                results.append(path)

    # Data dictionary — single-threaded after all downloads
    dict_output = references_dir / "production-data-dictionary.csv"
    try:
        fetch_data_dictionary(dict_url, dict_output, logger)
    except Exception as exc:
        logger.warning("Could not fetch data dictionary: %s", exc)

    return results


def validate_raw_files(raw_dir: Path, logger: logging.Logger) -> list[str]:
    """Validate every CSV file in raw_dir for integrity.

    Checks:
    1. File size > 0 bytes.
    2. Readable as UTF-8 (fallback to Latin-1).
    3. At least one data row beyond the header.

    Args:
        raw_dir: Directory containing raw CSV files.
        logger: Configured logger instance.

    Returns:
        List of validation error strings (empty if all files pass).
    """
    errors: list[str] = []
    csv_files = sorted(raw_dir.glob("*.csv"))

    for path in csv_files:
        if path.stat().st_size == 0:
            msg = f"{path}: file is empty (0 bytes)"
            logger.warning(msg)
            errors.append(msg)
            continue

        content: str | None = None
        for enc in ("utf-8", "latin-1"):
            try:
                with path.open(encoding=enc) as fh:
                    content = fh.read(4096)
                break
            except UnicodeDecodeError:
                continue

        if content is None:
            msg = f"{path}: cannot be decoded as UTF-8 or Latin-1"
            logger.warning(msg)
            errors.append(msg)
            continue

        line_count = content.count("\n")
        if line_count < 1:
            msg = f"{path}: contains only a header line with no data rows"
            logger.warning(msg)
            errors.append(msg)

    return errors


if __name__ == "__main__":
    _config = load_config("config/pipeline_config.yaml")
    _log_cfg = _config["logging"]
    _logger = get_logger(
        "cogcc_pipeline.acquire",
        _log_cfg["log_dir"],
        _log_cfg["log_file"],
        _log_cfg["level"],
    )
    run_acquire(_config, _logger)
    _raw_dir = Path(_config["acquire"]["raw_dir"])
    _errors = validate_raw_files(_raw_dir, _logger)
    for _err in _errors:
        _logger.error(_err)
    sys.exit(1 if _errors else 0)
