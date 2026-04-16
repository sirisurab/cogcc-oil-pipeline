"""Acquire stage: download COGCC/ECMC production data files from external URLs."""

import logging
import time
import zipfile
from pathlib import Path

import dask.bag as db
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from cogcc_pipeline.utils import load_config

logger = logging.getLogger(__name__)

# Module-level default sleep; overridden at runtime from config
_SLEEP_PER_WORKER: float = 0.5


def build_download_targets(config: dict) -> list[dict]:
    """Return ordered list of download target descriptors for all configured years.

    Each descriptor has keys: year, url, output_path, is_current_year.

    Raises:
        ValueError: if start_year > end_year.
    """
    start_year: int = config["start_year"]
    end_year: int = config["end_year"]

    if start_year > end_year:
        raise ValueError(f"Invalid year range: start_year={start_year} > end_year={end_year}")

    raw_dir = Path(config["raw_dir"]).resolve()
    targets: list[dict] = []

    for year in range(start_year, end_year + 1):
        is_current = year == end_year
        if is_current:
            url: str = config["base_url_current"]
            output_path = raw_dir / "monthly_prod.csv"
        else:
            url = config["base_url_historical"].format(year=year)
            # Output is the extracted CSV (zip downloaded then extracted)
            output_path = raw_dir / f"{year}_prod_reports.csv"

        targets.append(
            {
                "year": year,
                "url": url,
                "output_path": output_path,
                "is_current_year": is_current,
            }
        )

    return targets


def is_valid_download(file_path: Path) -> bool:
    """Return True if file exists, is non-empty, and readable as UTF-8 text."""
    if not file_path.exists():
        return False
    if file_path.stat().st_size == 0:
        return False
    try:
        with open(file_path, "rb") as fh:
            chunk = fh.read(1024)
        chunk.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def _make_download_attempt(
    target: dict,
    timeout: int,
    max_retries: int,
    backoff_multiplier: int,
) -> Path | None:
    """Inner implementation with tenacity retry wrapping."""

    @retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=backoff_multiplier, min=1, max=60),
        retry=retry_if_exception_type((requests.exceptions.RequestException, OSError)),
        reraise=True,
    )
    def _attempt() -> Path:
        output_path: Path = target["output_path"]
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")

        time.sleep(_SLEEP_PER_WORKER)

        headers = {"User-Agent": "COGCC-Pipeline/1.0"}
        response = requests.get(target["url"], timeout=timeout, headers=headers, stream=True)

        if response.status_code != 200:
            raise requests.exceptions.HTTPError(f"HTTP {response.status_code} for {target['url']}")

        try:
            with open(tmp_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=65536):
                    fh.write(chunk)
        except OSError:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        if target["is_current_year"]:
            # Rename tmp → final path directly
            tmp_path.rename(output_path)
        else:
            # Extract first CSV from the zip tmp file
            try:
                with zipfile.ZipFile(tmp_path, "r") as zf:
                    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                    if not csv_names:
                        raise ValueError(f"No CSV found inside zip from {target['url']}")
                    with zf.open(csv_names[0]) as csv_in, open(output_path, "wb") as csv_out:
                        csv_out.write(csv_in.read())
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()

        if not is_valid_download(output_path):
            if output_path.exists():
                output_path.unlink()
            raise RuntimeError(f"Downloaded file failed validity check: {output_path}")

        size = output_path.stat().st_size
        logger.info("Downloaded %s (%d bytes)", output_path.name, size)
        return output_path

    return _attempt()


def download_file(
    target: dict,
    timeout: int = 60,
    max_retries: int = 3,
    backoff_multiplier: int = 2,
) -> Path | None:
    """Download a single file described by target. Returns Path on success, None if skipped.

    Returns None without making any HTTP request if a valid file already exists.
    After max_retries failures, logs a WARNING and returns None.
    """
    output_path: Path = target["output_path"]

    if is_valid_download(output_path):
        logger.info("Skipping already-present file: %s", output_path.name)
        return None

    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        return _make_download_attempt(target, timeout, max_retries, backoff_multiplier)
    except Exception as exc:
        logger.warning(
            "Failed to download %s after %d attempts: %s", target["url"], max_retries, exc
        )
        return None


def run_acquire(config_path: str = "config/config.yaml") -> list[Path]:
    """Top-level entry point for the acquire stage.

    Downloads all configured year files in parallel using the Dask threaded scheduler.
    Returns a list of Path objects for files that were newly downloaded.
    """
    global _SLEEP_PER_WORKER

    config = load_config(config_path)
    acq_cfg = config["acquire"]

    _SLEEP_PER_WORKER = float(acq_cfg.get("sleep_per_worker", 0.5))
    timeout: int = int(acq_cfg.get("timeout", 60))
    max_retries: int = int(acq_cfg.get("max_retries", 3))
    backoff_multiplier: int = int(acq_cfg.get("backoff_multiplier", 2))
    max_workers: int = int(acq_cfg.get("max_workers", 5))

    raw_dir = Path(acq_cfg["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    targets = build_download_targets(acq_cfg)
    n_partitions = min(len(targets), max_workers)

    def _download(t: dict) -> Path | None:
        return download_file(
            t, timeout=timeout, max_retries=max_retries, backoff_multiplier=backoff_multiplier
        )

    bag = db.from_sequence(targets, npartitions=n_partitions)
    results: list[Path | None] = bag.map(_download).compute(scheduler="threads")

    downloaded = [p for p in results if p is not None]
    skipped = len(results) - len(downloaded)

    logger.info(
        "Acquire complete: %d total targets, %d downloaded, %d skipped",
        len(targets),
        len(downloaded),
        skipped,
    )
    return downloaded
