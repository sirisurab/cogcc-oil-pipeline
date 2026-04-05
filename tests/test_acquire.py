"""Tests for cogcc_pipeline.acquire and cogcc_pipeline.utils."""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from cogcc_pipeline.acquire import (
    download_annual_zip,
    download_monthly_csv,
    fetch_data_dictionary,
    run_acquire,
    validate_raw_files,
)
from cogcc_pipeline.utils import get_logger, load_config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_zip_bytes(csv_content: str, filename: str = "data.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, csv_content)
    return buf.getvalue()


def _make_response(status_code: int = 200, content: bytes = b"") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.text = content.decode("utf-8", errors="replace")
    return resp


@pytest.fixture()
def logger() -> logging.Logger:
    return logging.getLogger("test_acquire")


# ---------------------------------------------------------------------------
# utils.load_config tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_config_returns_dict_with_top_level_keys(tmp_path: Path) -> None:
    yaml_content = (
        "acquire:\n  start_year: 2020\n"
        "ingest:\n  raw_dir: data/raw\n"
        "transform:\n  processed_dir: data/processed\n"
        "features:\n  features_file: feat.parquet\n"
        "logging:\n  level: INFO\n"
    )
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml_content)
    config = load_config(cfg_file)
    for key in ("acquire", "ingest", "transform", "features", "logging"):
        assert key in config


@pytest.mark.unit
def test_load_config_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nonexistent.yaml")


@pytest.mark.unit
def test_get_logger_returns_logger_with_two_handlers(tmp_path: Path) -> None:
    log_dir = str(tmp_path / "logs")
    lg = get_logger("test_utils", log_dir, "test.log", "INFO")
    assert isinstance(lg, logging.Logger)
    assert len(lg.handlers) >= 2


# ---------------------------------------------------------------------------
# fetch_data_dictionary tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fetch_data_dictionary_writes_csv(tmp_path: Path, logger: logging.Logger) -> None:
    html = (
        "<html><body><table>"
        "<tr><th>Field</th><th>Type</th></tr>"
        "<tr><td>OilProduced</td><td>float</td></tr>"
        "</table></body></html>"
    )
    resp = _make_response(200, html.encode())
    output_path = tmp_path / "dict.csv"
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        result = fetch_data_dictionary("http://example.com/dict", output_path, logger)
    assert result == output_path
    assert output_path.exists()
    text = output_path.read_text()
    assert "Field" in text


@pytest.mark.unit
def test_fetch_data_dictionary_raises_http_error(tmp_path: Path, logger: logging.Logger) -> None:
    resp = _make_response(404)
    output_path = tmp_path / "dict.csv"
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with pytest.raises(requests.HTTPError):
            fetch_data_dictionary("http://example.com/dict", output_path, logger)


@pytest.mark.unit
def test_fetch_data_dictionary_raises_value_error_no_table(
    tmp_path: Path, logger: logging.Logger
) -> None:
    resp = _make_response(200, b"<html><body>No table here</body></html>")
    output_path = tmp_path / "dict.csv"
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with pytest.raises(ValueError, match="No table found"):
            fetch_data_dictionary("http://example.com/dict", output_path, logger)


@pytest.mark.unit
def test_fetch_data_dictionary_skips_if_exists(tmp_path: Path, logger: logging.Logger) -> None:
    output_path = tmp_path / "dict.csv"
    output_path.write_text("existing content")
    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        fetch_data_dictionary("http://example.com/dict", output_path, logger)
        assert mock_get.call_count == 0


# ---------------------------------------------------------------------------
# download_annual_zip tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_annual_zip_extracts_csv(tmp_path: Path, logger: logging.Logger) -> None:
    csv_data = "APICountyCode,APISeqNum\n1,2\n"
    zip_bytes = _make_zip_bytes(csv_data)
    resp = _make_response(200, zip_bytes)
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_annual_zip(2022, "http://x.com/{year}.zip", tmp_path, 0.0, logger)
    assert result is not None
    assert result.exists()
    assert result.name == "2022_prod_reports.csv"


@pytest.mark.unit
def test_download_annual_zip_skips_if_exists(tmp_path: Path, logger: logging.Logger) -> None:
    target = tmp_path / "2022_prod_reports.csv"
    target.write_text("existing")
    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        result = download_annual_zip(2022, "http://x.com/{year}.zip", tmp_path, 0.0, logger)
    assert mock_get.call_count == 0
    assert result == target


@pytest.mark.unit
def test_download_annual_zip_returns_none_on_http_error(
    tmp_path: Path, logger: logging.Logger
) -> None:
    resp = _make_response(503)
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_annual_zip(2022, "http://x.com/{year}.zip", tmp_path, 0.0, logger)
    assert result is None


@pytest.mark.unit
def test_download_annual_zip_returns_none_when_no_csv_in_zip(
    tmp_path: Path, logger: logging.Logger
) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "no csv")
    resp = _make_response(200, buf.getvalue())
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_annual_zip(2022, "http://x.com/{year}.zip", tmp_path, 0.0, logger)
    assert result is None


@pytest.mark.unit
def test_download_annual_zip_idempotent(tmp_path: Path, logger: logging.Logger) -> None:
    csv_data = "col1\nval1\n"
    zip_bytes = _make_zip_bytes(csv_data)
    resp = _make_response(200, zip_bytes)
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            p1 = download_annual_zip(2022, "http://x.com/{year}.zip", tmp_path, 0.0, logger)
    count_after_first = len(list(tmp_path.glob("*.csv")))
    # Second call — file exists, should skip
    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        p2 = download_annual_zip(2022, "http://x.com/{year}.zip", tmp_path, 0.0, logger)
        assert mock_get.call_count == 0
    count_after_second = len(list(tmp_path.glob("*.csv")))
    assert count_after_first == count_after_second
    assert p1 is not None
    assert p1.read_text() == p2.read_text()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# download_monthly_csv tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_monthly_csv_writes_file(tmp_path: Path, logger: logging.Logger) -> None:
    resp = _make_response(200, b"header\nval\n")
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_monthly_csv("http://x.com/monthly.csv", tmp_path, 0.0, logger)
    assert result is not None
    assert result.name == "monthly_prod.csv"
    assert result.exists()


@pytest.mark.unit
def test_download_monthly_csv_force_refresh(tmp_path: Path, logger: logging.Logger) -> None:
    target = tmp_path / "monthly_prod.csv"
    target.write_text("old content")
    new_content = b"new content\n"
    resp = _make_response(200, new_content)
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_monthly_csv(
                "http://x.com/monthly.csv", tmp_path, 0.0, logger, force_refresh=True
            )
    assert result is not None
    assert result.read_bytes() == new_content


# ---------------------------------------------------------------------------
# run_acquire tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def minimal_config(tmp_path: Path) -> dict:
    return {
        "acquire": {
            "base_url_zip": "http://example.com/{year}.zip",
            "base_url_monthly": "http://example.com/monthly.csv",
            "data_dictionary_url": "http://example.com/dict.htm",
            "start_year": 2020,
            "raw_dir": str(tmp_path / "raw"),
            "references_dir": str(tmp_path / "refs"),
            "max_workers": 2,
            "sleep_seconds": 0.0,
        }
    }


@pytest.mark.unit
def test_run_acquire_returns_successful_paths(tmp_path: Path, logger: logging.Logger) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    fake_path = raw_dir / "2020_prod_reports.csv"
    fake_path.write_text("col\nval\n")

    config: dict = {
        "acquire": {
            "base_url_zip": "http://x.com/{year}.zip",
            "base_url_monthly": "http://x.com/monthly.csv",
            "data_dictionary_url": "http://x.com/dict.htm",
            "start_year": 2020,
            "raw_dir": str(raw_dir),
            "references_dir": str(tmp_path / "refs"),
            "max_workers": 2,
            "sleep_seconds": 0.0,
        }
    }

    with patch("cogcc_pipeline.acquire.download_annual_zip", return_value=fake_path):
        with patch(
            "cogcc_pipeline.acquire.download_monthly_csv",
            return_value=raw_dir / "monthly_prod.csv",
        ):
            with patch("cogcc_pipeline.acquire.fetch_data_dictionary"):
                result = run_acquire(config, logger)

    assert all(isinstance(p, Path) for p in result)


@pytest.mark.unit
def test_run_acquire_excludes_none_results(tmp_path: Path, logger: logging.Logger) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    config: dict = {
        "acquire": {
            "base_url_zip": "http://x.com/{year}.zip",
            "base_url_monthly": "http://x.com/monthly.csv",
            "data_dictionary_url": "http://x.com/dict.htm",
            "start_year": 2020,
            "raw_dir": str(raw_dir),
            "references_dir": str(tmp_path / "refs"),
            "max_workers": 2,
            "sleep_seconds": 0.0,
        }
    }

    with patch("cogcc_pipeline.acquire.download_annual_zip", return_value=None):
        with patch("cogcc_pipeline.acquire.download_monthly_csv", return_value=None):
            with patch("cogcc_pipeline.acquire.fetch_data_dictionary"):
                result = run_acquire(config, logger)

    assert result == []


@pytest.mark.unit
def test_run_acquire_files_have_positive_size(tmp_path: Path, logger: logging.Logger) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    fake = raw_dir / "2020_prod_reports.csv"
    fake.write_text("col\nval\n")
    config: dict = {
        "acquire": {
            "base_url_zip": "http://x.com/{year}.zip",
            "base_url_monthly": "http://x.com/monthly.csv",
            "data_dictionary_url": "http://x.com/dict.htm",
            "start_year": 2020,
            "raw_dir": str(raw_dir),
            "references_dir": str(tmp_path / "refs"),
            "max_workers": 2,
            "sleep_seconds": 0.0,
        }
    }

    with patch("cogcc_pipeline.acquire.download_annual_zip", return_value=fake):
        with patch("cogcc_pipeline.acquire.download_monthly_csv", return_value=fake):
            with patch("cogcc_pipeline.acquire.fetch_data_dictionary"):
                result = run_acquire(config, logger)

    for p in result:
        assert p.stat().st_size > 0


# ---------------------------------------------------------------------------
# validate_raw_files tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_raw_files_valid_file(tmp_path: Path, logger: logging.Logger) -> None:
    f = tmp_path / "2020_prod_reports.csv"
    f.write_text("col1,col2\nval1,val2\n")
    errors = validate_raw_files(tmp_path, logger)
    assert errors == []


@pytest.mark.unit
def test_validate_raw_files_empty_file(tmp_path: Path, logger: logging.Logger) -> None:
    f = tmp_path / "empty.csv"
    f.write_bytes(b"")
    errors = validate_raw_files(tmp_path, logger)
    assert len(errors) == 1
    assert "empty" in errors[0].lower() or "0 bytes" in errors[0]


@pytest.mark.unit
def test_validate_raw_files_header_only(tmp_path: Path, logger: logging.Logger) -> None:
    f = tmp_path / "header_only.csv"
    f.write_text("col1,col2")  # no trailing newline → 0 newlines
    errors = validate_raw_files(tmp_path, logger)
    assert len(errors) == 1


@pytest.mark.unit
def test_validate_raw_files_latin1_fallback(tmp_path: Path, logger: logging.Logger) -> None:
    # Write bytes that are invalid UTF-8 but valid Latin-1
    f = tmp_path / "latin.csv"
    content = b"col1,col2\nval\xe9,2\n"  # \xe9 is 'é' in latin-1
    f.write_bytes(content)
    errors = validate_raw_files(tmp_path, logger)
    assert errors == []
