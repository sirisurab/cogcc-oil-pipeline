"""Tests for cogcc_pipeline.acquire — unit + idempotency + integrity suites."""

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
    download_with_retry,
    fetch_data_dictionary,
    get_logger,
    load_config,
    run_acquire,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINIMAL_CONFIG = {
    "acquire": {
        "base_url": "https://example.com/data",
        "data_dict_url": "https://example.com/dict.htm",
        "monthly_url": "https://example.com/monthly.csv",
        "start_year": 2020,
        "raw_dir": "data/raw",
        "references_dir": "references",
        "max_workers": 2,
        "sleep_seconds": 0.0,
        "retry_attempts": 3,
        "retry_backoff_base": 0,
    },
    "ingest": {},
    "transform": {},
    "features": {},
}


def _make_zip_bytes(filename: str, content: bytes = b"col1,col2\nval1,val2\n") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, content)
    return buf.getvalue()


def _mock_response(content: bytes = b"data", status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.raise_for_status = MagicMock(
        side_effect=None if status < 400 else requests.HTTPError(f"HTTP {status}")
    )
    resp.iter_content = MagicMock(return_value=iter([content]))
    resp.text = content.decode("utf-8", errors="replace")
    return resp


# ---------------------------------------------------------------------------
# Task 01: config.yaml tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_config_yaml_loadable(tmp_path):
    """config.yaml is loadable and contains required top-level keys."""
    cfg_path = Path("config.yaml")
    if cfg_path.exists():
        import yaml
        with cfg_path.open() as f:
            cfg = yaml.safe_load(f)
        for key in ("acquire", "ingest", "transform", "features"):
            assert key in cfg


@pytest.mark.unit
def test_pyproject_build_backend():
    """pyproject.toml specifies setuptools.build_meta."""
    p = Path("pyproject.toml")
    if p.exists():
        content = p.read_text()
        assert "setuptools.build_meta" in content


@pytest.mark.unit
def test_pyproject_dev_deps():
    """Dev optional-deps include pandas-stubs and types-requests."""
    p = Path("pyproject.toml")
    if p.exists():
        content = p.read_text()
        assert "pandas-stubs" in content
        assert "types-requests" in content


# ---------------------------------------------------------------------------
# Task 02: load_config tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_config_returns_all_keys(tmp_path):
    import yaml
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(_MINIMAL_CONFIG))
    cfg = load_config(str(cfg_file))
    for key in ("acquire", "ingest", "transform", "features"):
        assert key in cfg


@pytest.mark.unit
def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yaml")


@pytest.mark.unit
def test_load_config_invalid_yaml(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(": invalid: yaml: {{{{")
    with pytest.raises(ValueError):
        load_config(str(bad))


# ---------------------------------------------------------------------------
# Task 03: get_logger tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_get_logger_returns_logger():
    logger = get_logger("test_logger")
    assert isinstance(logger, logging.Logger)


@pytest.mark.unit
def test_get_logger_no_duplicate_handlers():
    get_logger("dedup_test")
    logger = get_logger("dedup_test")
    assert len(logger.handlers) == 1


@pytest.mark.unit
def test_get_logger_level_info():
    logger = get_logger("level_test")
    assert logger.level == logging.INFO


# ---------------------------------------------------------------------------
# Task 04: download_with_retry tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_with_retry_success(tmp_path):
    dest = tmp_path / "file.csv"
    cfg = {"retry_attempts": 3, "retry_backoff_base": 0}
    resp = _mock_response(b"col1,col2\nval1,val2")
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        result = download_with_retry("http://example.com/file.csv", dest, cfg)
    assert result is True
    assert dest.exists()
    assert dest.stat().st_size > 0


@pytest.mark.unit
def test_download_with_retry_skips_existing(tmp_path):
    dest = tmp_path / "existing.csv"
    dest.write_bytes(b"already downloaded content")
    cfg = {"retry_attempts": 3, "retry_backoff_base": 0}
    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        result = download_with_retry("http://example.com/file.csv", dest, cfg)
    assert result is True
    assert mock_get.call_count == 0


@pytest.mark.unit
def test_download_with_retry_all_fail(tmp_path):
    dest = tmp_path / "fail.csv"
    cfg = {"retry_attempts": 3, "retry_backoff_base": 0}
    with patch("cogcc_pipeline.acquire.requests.get",
               side_effect=requests.ConnectionError("connection refused")):
        result = download_with_retry("http://example.com/fail.csv", dest, cfg)
    assert result is False
    assert not dest.exists()


@pytest.mark.unit
def test_download_with_retry_recovers_on_third_attempt(tmp_path):
    dest = tmp_path / "recover.csv"
    cfg = {"retry_attempts": 3, "retry_backoff_base": 0}
    good_resp = _mock_response(b"content")
    side_effects = [
        requests.ConnectionError("fail 1"),
        requests.ConnectionError("fail 2"),
        good_resp,
    ]
    with patch("cogcc_pipeline.acquire.requests.get", side_effect=side_effects):
        result = download_with_retry("http://example.com/recover.csv", dest, cfg)
    assert result is True
    assert dest.exists()


# ---------------------------------------------------------------------------
# Task 05: fetch_data_dictionary tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fetch_data_dictionary_writes_markdown(tmp_path):
    html = (
        "<html><body><table>"
        "<tr><th>Column Name</th><th>Description</th></tr>"
        "<tr><td>ApiCountyCode</td><td>API county code</td></tr>"
        "</table></body></html>"
    )
    cfg = {
        "data_dict_url": "http://example.com/dict.htm",
        "references_dir": str(tmp_path / "refs"),
    }
    resp = _mock_response(html.encode())
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        out = fetch_data_dictionary(cfg)
    assert out.exists()
    content = out.read_text()
    assert "| Column Name | Description |" in content


@pytest.mark.unit
def test_fetch_data_dictionary_404_raises(tmp_path):
    cfg = {
        "data_dict_url": "http://example.com/dict.htm",
        "references_dir": str(tmp_path / "refs"),
    }
    resp = _mock_response(b"not found", status=404)
    resp.raise_for_status.side_effect = requests.HTTPError("HTTP 404")
    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with pytest.raises(RuntimeError, match="Failed to fetch data dictionary"):
            fetch_data_dictionary(cfg)


# ---------------------------------------------------------------------------
# Task 06: download_annual_zip tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_annual_zip_success(tmp_path):
    zip_bytes = _make_zip_bytes("2022_prod_reports.csv", b"col1,col2\n1,2\n")
    cfg = {
        "base_url": "http://example.com",
        "raw_dir": str(tmp_path),
        "sleep_seconds": 0.0,
        "retry_attempts": 1,
        "retry_backoff_base": 0,
    }
    good_resp = _mock_response(zip_bytes)
    with patch("cogcc_pipeline.acquire.requests.get", return_value=good_resp):
        result = download_annual_zip(2022, cfg)
    assert result is not None
    assert result.name == "2022_prod_reports.csv"
    assert result.exists()
    # Zip file should be deleted
    assert not (tmp_path / "2022_prod_reports.zip").exists()


@pytest.mark.unit
def test_download_annual_zip_download_failure(tmp_path):
    cfg = {
        "base_url": "http://example.com",
        "raw_dir": str(tmp_path),
        "sleep_seconds": 0.0,
        "retry_attempts": 1,
        "retry_backoff_base": 0,
    }
    with patch("cogcc_pipeline.acquire.requests.get",
               side_effect=requests.ConnectionError("fail")):
        result = download_annual_zip(2022, cfg)
    assert result is None


@pytest.mark.unit
def test_download_annual_zip_no_matching_csv(tmp_path):
    zip_bytes = _make_zip_bytes("unrelated_file.txt", b"text content")
    cfg = {
        "base_url": "http://example.com",
        "raw_dir": str(tmp_path),
        "sleep_seconds": 0.0,
        "retry_attempts": 1,
        "retry_backoff_base": 0,
    }
    with patch("cogcc_pipeline.acquire.requests.get", return_value=_mock_response(zip_bytes)):
        result = download_annual_zip(2022, cfg)
    # Should return None (ValueError caught internally)
    assert result is None


@pytest.mark.unit
def test_download_annual_zip_existing_skips(tmp_path):
    existing = tmp_path / "2022_prod_reports.csv"
    existing.write_bytes(b"col1,col2\nval1,val2\n")
    cfg = {
        "base_url": "http://example.com",
        "raw_dir": str(tmp_path),
        "sleep_seconds": 0.0,
        "retry_attempts": 1,
        "retry_backoff_base": 0,
    }
    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        result = download_annual_zip(2022, cfg)
    assert result == existing
    assert mock_get.call_count == 0


# ---------------------------------------------------------------------------
# Task 07: download_monthly_csv tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_monthly_csv_success(tmp_path):
    cfg = {
        "monthly_url": "http://example.com/monthly.csv",
        "raw_dir": str(tmp_path),
        "sleep_seconds": 0.0,
        "retry_attempts": 1,
        "retry_backoff_base": 0,
    }
    with patch("cogcc_pipeline.acquire.requests.get",
               return_value=_mock_response(b"col1,col2\nval1,val2")):
        result = download_monthly_csv(cfg)
    assert result is not None
    assert result.name == "monthly_prod.csv"


@pytest.mark.unit
def test_download_monthly_csv_failure(tmp_path):
    cfg = {
        "monthly_url": "http://example.com/monthly.csv",
        "raw_dir": str(tmp_path),
        "sleep_seconds": 0.0,
        "retry_attempts": 1,
        "retry_backoff_base": 0,
    }
    with patch("cogcc_pipeline.acquire.requests.get",
               side_effect=requests.ConnectionError("fail")):
        result = download_monthly_csv(cfg)
    assert result is None


# ---------------------------------------------------------------------------
# Task 08: run_acquire tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_acquire_returns_paths(tmp_path):
    import yaml

    cfg = dict(_MINIMAL_CONFIG)
    cfg["acquire"] = dict(cfg["acquire"])
    cfg["acquire"]["raw_dir"] = str(tmp_path / "raw")
    cfg["acquire"]["references_dir"] = str(tmp_path / "refs")
    cfg["acquire"]["start_year"] = 2022
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg))

    fake_paths = [
        tmp_path / "raw" / "2022_prod_reports.csv",
        tmp_path / "raw" / "2023_prod_reports.csv",
        tmp_path / "raw" / "monthly_prod.csv",
    ]
    for p in fake_paths:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"col\nval")

    with (
        patch("cogcc_pipeline.acquire.download_annual_zip", side_effect=fake_paths[:2]),
        patch("cogcc_pipeline.acquire.download_monthly_csv", return_value=fake_paths[2]),
        patch("cogcc_pipeline.acquire.fetch_data_dictionary"),
    ):
        results = run_acquire(str(cfg_file))

    assert len(results) == 3
    for r in results:
        assert isinstance(r, Path)


@pytest.mark.unit
def test_run_acquire_none_result_excluded(tmp_path):
    import yaml

    cfg = dict(_MINIMAL_CONFIG)
    cfg["acquire"] = dict(cfg["acquire"])
    cfg["acquire"]["raw_dir"] = str(tmp_path / "raw")
    cfg["acquire"]["references_dir"] = str(tmp_path / "refs")
    cfg["acquire"]["start_year"] = 2023
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg))

    monthly = tmp_path / "raw" / "monthly_prod.csv"
    monthly.parent.mkdir(parents=True, exist_ok=True)
    monthly.write_bytes(b"col\nval")

    with (
        patch("cogcc_pipeline.acquire.download_annual_zip", return_value=None),
        patch("cogcc_pipeline.acquire.download_monthly_csv", return_value=monthly),
        patch("cogcc_pipeline.acquire.fetch_data_dictionary"),
    ):
        results = run_acquire(str(cfg_file))

    assert monthly in results
    assert len(results) == 1


@pytest.mark.unit
def test_run_acquire_respects_max_workers(tmp_path):
    import yaml
    from concurrent.futures import ThreadPoolExecutor

    cfg = dict(_MINIMAL_CONFIG)
    cfg["acquire"] = dict(cfg["acquire"])
    cfg["acquire"]["raw_dir"] = str(tmp_path / "raw")
    cfg["acquire"]["references_dir"] = str(tmp_path / "refs")
    cfg["acquire"]["max_workers"] = 2
    cfg["acquire"]["start_year"] = 2024
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg))

    monthly = tmp_path / "raw" / "monthly_prod.csv"
    monthly.parent.mkdir(parents=True, exist_ok=True)
    monthly.write_bytes(b"col\nval")

    captured_workers = []
    orig_init = ThreadPoolExecutor.__init__

    def patched_init(self, max_workers=None, **kwargs):
        captured_workers.append(max_workers)
        orig_init(self, max_workers=max_workers, **kwargs)

    with (
        patch("cogcc_pipeline.acquire.download_annual_zip", return_value=None),
        patch("cogcc_pipeline.acquire.download_monthly_csv", return_value=monthly),
        patch("cogcc_pipeline.acquire.fetch_data_dictionary"),
        patch.object(ThreadPoolExecutor, "__init__", patched_init),
    ):
        run_acquire(str(cfg_file))

    assert 2 in captured_workers


# ---------------------------------------------------------------------------
# Task 09: Idempotency tests (TR-20)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_idempotency_file_count_unchanged(tmp_path):
    """Calling download_with_retry on an existing file does not create duplicates."""
    dest = tmp_path / "file.csv"
    dest.write_bytes(b"col\nval")
    cfg = {"retry_attempts": 3, "retry_backoff_base": 0}
    before = len(list(tmp_path.iterdir()))
    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        download_with_retry("http://example.com/file.csv", dest, cfg)
    after = len(list(tmp_path.iterdir()))
    assert before == after
    assert mock_get.call_count == 0


@pytest.mark.unit
def test_idempotency_content_unchanged(tmp_path):
    """Second call to download_with_retry leaves file content unchanged."""
    dest = tmp_path / "file.csv"
    original_content = b"original data content"
    dest.write_bytes(original_content)
    cfg = {"retry_attempts": 3, "retry_backoff_base": 0}
    with patch("cogcc_pipeline.acquire.requests.get"):
        download_with_retry("http://example.com/file.csv", dest, cfg)
    assert dest.read_bytes() == original_content


@pytest.mark.unit
def test_idempotency_no_exception_on_existing(tmp_path):
    """download_with_retry does not raise when dest_path already exists."""
    dest = tmp_path / "file.csv"
    dest.write_bytes(b"existing content")
    cfg = {"retry_attempts": 3, "retry_backoff_base": 0}
    with patch("cogcc_pipeline.acquire.requests.get"):
        result = download_with_retry("http://example.com/file.csv", dest, cfg)
    assert result is True


# ---------------------------------------------------------------------------
# Task 10: File integrity tests (TR-21) — integration, require real data/mock
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_integrity_all_files_nonempty(tmp_path):
    """Every file in data/raw/ has size > 0."""
    raw_dir = Path("data/raw")
    if not raw_dir.exists():
        pytest.skip("data/raw not found — run acquire first")
    for f in raw_dir.glob("*.csv"):
        assert f.stat().st_size > 0, f"Empty file: {f}"


@pytest.mark.integration
def test_integrity_all_files_utf8(tmp_path):
    """Every file in data/raw/ is readable as UTF-8."""
    raw_dir = Path("data/raw")
    if not raw_dir.exists():
        pytest.skip("data/raw not found — run acquire first")
    for f in raw_dir.glob("*.csv"):
        try:
            f.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            pytest.fail(f"UnicodeDecodeError in {f}: {exc}")


@pytest.mark.integration
def test_integrity_files_have_header_and_data():
    """Every file in data/raw/ has at least 2 lines (header + 1 data row)."""
    raw_dir = Path("data/raw")
    if not raw_dir.exists():
        pytest.skip("data/raw not found — run acquire first")
    for f in raw_dir.glob("*.csv"):
        lines = f.read_text(encoding="utf-8", errors="replace").splitlines()
        assert len(lines) >= 2, f"File has fewer than 2 lines: {f}"
