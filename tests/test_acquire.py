"""Tests for cogcc_pipeline.acquire module."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cogcc_pipeline.acquire import (
    AcquireError,
    download_monthly_csv,
    download_year_zip,
    is_valid_file,
    load_config,
    run_acquire,
    setup_logging,
)
from tests.conftest import make_valid_zip_bytes


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_load_config_valid(tmp_path: Path) -> None:
    """Valid config file returns dict with all required top-level keys."""
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "acquire:\n  start_year: 2020\n"
        "ingest:\n  raw_dir: data/raw\n"
        "transform:\n  interim_dir: data/interim\n"
        "features:\n  processed_dir: data/processed\n"
        "dask:\n  scheduler: local\n"
        "logging:\n  level: INFO\n"
    )
    result = load_config(str(cfg))
    assert set(result.keys()) >= {"acquire", "ingest", "transform", "features", "dask", "logging"}


@pytest.mark.unit
def test_load_config_missing_section(tmp_path: Path) -> None:
    """Config missing required section raises ValueError."""
    cfg = tmp_path / "config.yaml"
    cfg.write_text("acquire:\n  start_year: 2020\ningest:\n  raw_dir: x\ntransform:\n  a: 1\nfeatures:\n  b: 2\nlogging:\n  level: INFO\n")
    with pytest.raises(ValueError, match="dask"):
        load_config(str(cfg))


@pytest.mark.unit
def test_load_config_missing_file() -> None:
    """Non-existent config path raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yaml")


# ---------------------------------------------------------------------------
# is_valid_file
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_is_valid_file_existing(tmp_path: Path) -> None:
    """File with header + data row → True."""
    f = tmp_path / "test.csv"
    f.write_text("Header\nDataRow\n", encoding="utf-8")
    assert is_valid_file(f) is True


@pytest.mark.unit
def test_is_valid_file_empty(tmp_path: Path) -> None:
    """Empty file → False."""
    f = tmp_path / "empty.csv"
    f.write_bytes(b"")
    assert is_valid_file(f) is False


@pytest.mark.unit
def test_is_valid_file_header_only(tmp_path: Path) -> None:
    """File with only a header and no data rows → False."""
    f = tmp_path / "header_only.csv"
    f.write_text("HeaderRow\n", encoding="utf-8")
    assert is_valid_file(f) is False


@pytest.mark.unit
def test_is_valid_file_missing() -> None:
    """Non-existent path → False."""
    assert is_valid_file(Path("/nonexistent/file.csv")) is False


@pytest.mark.unit
def test_is_valid_file_invalid_utf8(tmp_path: Path) -> None:
    """File with invalid UTF-8 bytes → False."""
    f = tmp_path / "bad.csv"
    f.write_bytes(b"Header\nData\xff\xfe\n")
    assert is_valid_file(f) is False


# ---------------------------------------------------------------------------
# download_year_zip
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_download_year_zip_success(tmp_path: Path, config_fixture: dict) -> None:
    """Mock requests.get returns valid ZIP → returns Path to .csv file."""
    csv_content = "Header\nRow1\n"
    zip_bytes = make_valid_zip_bytes(csv_content)

    mock_response = MagicMock()
    mock_response.content = zip_bytes
    mock_response.raise_for_status = MagicMock()

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_response):
        result = download_year_zip(2021, config_fixture)

    assert isinstance(result, Path)
    assert result.suffix == ".csv"
    assert result.exists()


@pytest.mark.unit
def test_download_year_zip_skips_valid(tmp_path: Path, config_fixture: dict) -> None:
    """If output CSV already exists and is valid, requests.get is never called."""
    raw_dir = Path(config_fixture["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    output = raw_dir / "2021_prod_reports.csv"
    output.write_text("Header\nDataRow\n", encoding="utf-8")

    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        result = download_year_zip(2021, config_fixture)
        mock_get.assert_not_called()

    assert result == output


@pytest.mark.unit
def test_download_year_zip_connection_error(config_fixture: dict) -> None:
    """Mock requests.get raises ConnectionError → AcquireError is raised."""
    import requests as req

    with patch("cogcc_pipeline.acquire.requests.get", side_effect=req.ConnectionError("conn error")):
        with pytest.raises(AcquireError):
            download_year_zip(2021, config_fixture)


@pytest.mark.unit
def test_download_year_zip_bad_zip(config_fixture: dict) -> None:
    """Mock requests.get returns non-ZIP content → AcquireError is raised."""
    mock_response = MagicMock()
    mock_response.content = b"This is not a zip file"
    mock_response.raise_for_status = MagicMock()

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_response):
        with pytest.raises(AcquireError):
            download_year_zip(2021, config_fixture)


# ---------------------------------------------------------------------------
# download_monthly_csv
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_download_monthly_csv_success(config_fixture: dict) -> None:
    """Mock returns CSV text → returns Path with filename monthly_prod.csv."""
    mock_response = MagicMock()
    mock_response.content = b"Header\nRow1\n"
    mock_response.raise_for_status = MagicMock()

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_response):
        result = download_monthly_csv(config_fixture)

    assert result.name == "monthly_prod.csv"
    assert result.exists()


@pytest.mark.unit
def test_download_monthly_csv_skips_valid(config_fixture: dict) -> None:
    """If monthly_prod.csv is already valid, requests.get is never called."""
    raw_dir = Path(config_fixture["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    output = raw_dir / "monthly_prod.csv"
    output.write_text("Header\nDataRow\n", encoding="utf-8")

    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        result = download_monthly_csv(config_fixture)
        mock_get.assert_not_called()

    assert result == output


@pytest.mark.unit
def test_download_monthly_csv_timeout(config_fixture: dict) -> None:
    """Mock raises Timeout → AcquireError is raised."""
    import requests as req

    with patch("cogcc_pipeline.acquire.requests.get", side_effect=req.Timeout("timeout")):
        with pytest.raises(AcquireError):
            download_monthly_csv(config_fixture)


# ---------------------------------------------------------------------------
# run_acquire
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_run_acquire_returns_paths(config_fixture: dict) -> None:
    """Mock downloaders; assert run_acquire returns list of Paths."""
    from datetime import datetime
    current_year = datetime.now().year
    start_year = config_fixture["acquire"]["start_year"]
    expected_count = (current_year - start_year) + 1  # historical + monthly

    fake_path = Path("/fake/path.csv")
    with (
        patch("cogcc_pipeline.acquire.download_year_zip", return_value=fake_path),
        patch("cogcc_pipeline.acquire.download_monthly_csv", return_value=fake_path),
    ):
        result = run_acquire(config_fixture)

    assert isinstance(result, list)
    assert len(result) == expected_count
    assert all(isinstance(p, Path) for p in result)


@pytest.mark.unit
def test_run_acquire_uses_threads_scheduler(config_fixture: dict) -> None:
    """dask.compute is called with scheduler='threads'."""
    fake_path = Path("/fake/path.csv")
    with (
        patch("cogcc_pipeline.acquire.download_year_zip", return_value=fake_path),
        patch("cogcc_pipeline.acquire.download_monthly_csv", return_value=fake_path),
        patch("cogcc_pipeline.acquire.dask.compute", return_value=(fake_path,) * 10) as mock_compute,
    ):
        run_acquire(config_fixture)
        call_kwargs = mock_compute.call_args[1]
        assert call_kwargs.get("scheduler") == "threads"


@pytest.mark.unit
def test_run_acquire_propagates_acquire_error(config_fixture: dict) -> None:
    """If one download raises AcquireError, it propagates from run_acquire."""
    with patch("cogcc_pipeline.acquire.download_year_zip", side_effect=AcquireError("fail")):
        with pytest.raises((AcquireError, Exception)):
            run_acquire(config_fixture)


# ---------------------------------------------------------------------------
# setup_logging
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_setup_logging_no_duplicate_handlers(tmp_path: Path, config_fixture: dict) -> None:
    """Calling setup_logging twice must not duplicate handlers."""
    import logging
    # Clear handlers first
    root = logging.getLogger()
    root.handlers.clear()

    setup_logging(config_fixture)
    setup_logging(config_fixture)

    assert len(root.handlers) == 2


@pytest.mark.unit
def test_setup_logging_creates_log_dir(tmp_path: Path, config_fixture: dict) -> None:
    """setup_logging creates the logs/ directory if absent."""
    import logging
    log_dir = tmp_path / "newlogs"
    cfg = dict(config_fixture)
    cfg["logging"] = {"log_file": str(log_dir / "pipeline.log"), "level": "INFO"}

    root = logging.getLogger()
    root.handlers.clear()
    setup_logging(cfg)
    assert log_dir.exists()


@pytest.mark.unit
def test_setup_logging_level(config_fixture: dict) -> None:
    """Returned logger level matches config logging.level."""
    import logging
    root = logging.getLogger()
    root.handlers.clear()
    result = setup_logging(config_fixture)
    assert result.level == logging.INFO
