"""Acquire stage — download raw COGCC production CSVs from ECMC."""

from __future__ import annotations

import io
import logging
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


class AcquireConfigError(ValueError):
    """Raised when the acquire configuration is invalid."""


class DownloadError(RuntimeError):
    """Raised when a download cannot be completed."""


# ---------------------------------------------------------------------------
# Task A1 — Build the list of download targets
# ---------------------------------------------------------------------------


def build_download_targets(config: dict, current_year: int | None = None) -> list[dict]:
    """Produce one download target dict per year in the configured range.

    Args:
        config: The ``acquire`` section of config.yaml.
        current_year: Override for the current year (used in tests).

    Returns:
        List of target dicts with keys: year, url, kind, target_path.

    Raises:
        AcquireConfigError: If the year range is missing, empty, or inverted.
    """
    if current_year is None:
        current_year = datetime.now().year

    start_year: int | None = config.get("start_year")
    if start_year is None:
        raise AcquireConfigError("acquire.start_year is not configured")
    if start_year > current_year:
        raise AcquireConfigError(f"start_year {start_year} is after current_year {current_year}")

    base_url: str = config.get("base_url", "")
    raw_dir: str = config.get("raw_dir", "data/raw")
    zip_url_tpl: str = config.get("zip_url_template", "{base_url}/{year}_prod_reports.zip")
    monthly_url: str = config.get("monthly_url", "{base_url}/monthly_prod.csv")

    # Resolve template placeholders
    monthly_url = monthly_url.format(base_url=base_url)

    targets: list[dict] = []
    for year in range(start_year, current_year + 1):
        if year == current_year:
            url = monthly_url
            kind = "csv"
            filename = "monthly_prod.csv"
        else:
            url = zip_url_tpl.format(base_url=base_url, year=year)
            kind = "zip"
            filename = f"{year}_prod_reports.csv"

        targets.append(
            {
                "year": year,
                "url": url,
                "kind": kind,
                "target_path": str(Path(raw_dir) / filename),
            }
        )

    return targets


# ---------------------------------------------------------------------------
# Task A2 — Download a single target with retries and validation
# ---------------------------------------------------------------------------


def _is_valid_csv(path: Path) -> bool:
    """Return True if the file is non-empty, UTF-8, and has >1 line."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        content = path.read_text(encoding="utf-8")
        lines = [ln for ln in content.splitlines() if ln.strip()]
        return len(lines) > 1
    except UnicodeDecodeError:
        return False


def download_target(target: dict, config: dict) -> dict:
    """Download one target, write to disk, and return a result record.

    Args:
        target: A target dict produced by :func:`build_download_targets`.
        config: The ``acquire`` section of config.yaml.

    Returns:
        Result record with keys: year, url, final_path, size_bytes,
        downloaded_at, status.
    """
    year: int = target["year"]
    url: str = target["url"]
    kind: str = target["kind"]
    target_path = Path(target["target_path"])
    retry_count: int = int(config.get("retry_count", 3))
    sleep_s: float = float(config.get("sleep_between_downloads", 0.5))

    result_base: dict = {
        "year": year,
        "url": url,
        "final_path": str(target_path),
        "size_bytes": 0,
        "downloaded_at": datetime.utcnow().isoformat(),
        "status": "failed",
    }

    # Idempotency: skip if already valid
    if _is_valid_csv(target_path):
        logger.info("Skipping %s — already downloaded", target_path)
        return {
            **result_base,
            "final_path": str(target_path),
            "size_bytes": target_path.stat().st_size,
            "status": "skipped",
        }

    target_path.parent.mkdir(parents=True, exist_ok=True)

    # Rate-limit sleep per worker
    time.sleep(sleep_s)

    last_exc: Exception | None = None
    for attempt in range(1, retry_count + 1):
        try:
            resp = requests.get(url, timeout=60, stream=True)
            resp.raise_for_status()
            raw_bytes = resp.content

            if not raw_bytes:
                logger.warning("Empty response for %s (attempt %d)", url, attempt)
                last_exc = DownloadError(f"Empty response from {url}")
                continue

            if kind == "zip":
                # Extract inner CSV from ZIP
                with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                    if not csv_names:
                        raise DownloadError(f"No CSV found in ZIP from {url}")
                    csv_bytes = zf.read(csv_names[0])

                # Validate extracted CSV
                try:
                    text = csv_bytes.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise DownloadError(f"ZIP inner CSV is not UTF-8: {url}") from exc

                lines = [ln for ln in text.splitlines() if ln.strip()]
                if len(lines) <= 1:
                    raise DownloadError(f"ZIP inner CSV has no data rows: {url}")

                target_path.write_bytes(csv_bytes)

            else:  # kind == "csv"
                try:
                    text = raw_bytes.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise DownloadError(f"Downloaded CSV is not UTF-8: {url}") from exc

                lines = [ln for ln in text.splitlines() if ln.strip()]
                if len(lines) <= 1:
                    raise DownloadError(f"Downloaded CSV has no data rows: {url}")

                target_path.write_bytes(raw_bytes)

            size = target_path.stat().st_size
            logger.info("Downloaded %s → %s (%d bytes)", url, target_path, size)
            return {
                **result_base,
                "final_path": str(target_path),
                "size_bytes": size,
                "downloaded_at": datetime.utcnow().isoformat(),
                "status": "downloaded",
            }

        except (requests.RequestException, DownloadError) as exc:
            last_exc = exc
            logger.warning(
                "Download attempt %d/%d failed for %s: %s",
                attempt,
                retry_count,
                url,
                exc,
            )
            if attempt < retry_count:
                time.sleep(2**attempt)  # exponential backoff

    # Cleanup any partial file
    if target_path.exists():
        target_path.unlink()
        logger.debug("Removed partial file %s", target_path)

    logger.error("All %d attempts failed for %s: %s", retry_count, url, last_exc)
    return result_base


# ---------------------------------------------------------------------------
# Task A3 — Parallel acquire orchestration
# ---------------------------------------------------------------------------


def acquire(config: dict) -> list[dict]:
    """Entry point for the acquire stage.

    Downloads all configured year targets in parallel using a thread pool
    (I/O-bound per ADR-001). Returns one result record per target.

    Args:
        config: The ``acquire`` section of config.yaml.

    Returns:
        List of per-target result records (downloaded / skipped / failed).
    """
    raw_dir = Path(config.get("raw_dir", "data/raw"))
    raw_dir.mkdir(parents=True, exist_ok=True)

    max_workers: int = min(int(config.get("workers", 5)), 5)  # cap at 5 per task-writer

    targets = build_download_targets(config)
    logger.info("Acquire stage: %d targets, %d workers", len(targets), max_workers)

    results: list[dict] = [{}] * len(targets)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(download_target, t, config): i for i, t in enumerate(targets)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.error("Unexpected error for target index %d: %s", idx, exc)
                results[idx] = {
                    "year": targets[idx]["year"],
                    "url": targets[idx]["url"],
                    "final_path": targets[idx]["target_path"],
                    "size_bytes": 0,
                    "downloaded_at": datetime.utcnow().isoformat(),
                    "status": "failed",
                }

    downloaded = sum(1 for r in results if r.get("status") == "downloaded")
    skipped = sum(1 for r in results if r.get("status") == "skipped")
    failed = sum(1 for r in results if r.get("status") == "failed")
    logger.info(
        "Acquire complete — downloaded=%d skipped=%d failed=%d",
        downloaded,
        skipped,
        failed,
    )
    return results
