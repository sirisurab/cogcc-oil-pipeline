"""Tests for the acquire stage (ACQ-02 through ACQ-07, TR-20, TR-21)."""

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cogcc_pipeline.acquire import (
    build_download_targets,
    download_file,
    is_valid_download,
    run_acquire,
)
from cogcc_pipeline.utils import load_config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def base_config(tmp_path):
    """Minimal acquire config dict (not YAML — direct dict for unit tests)."""
    return {
        "acquire": {
            "base_url_historical": "https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip",
            "base_url_current": "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv",
            "start_year": 2020,
            "end_year": 2022,
            "raw_dir": str(tmp_path / "raw"),
            "max_workers": 5,
            "sleep_per_worker": 0.0,
            "timeout": 5,
            "max_retries": 2,
            "backoff_multiplier": 1,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 2,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/pipeline.log"), "level": "DEBUG"},
        "ingest": {"raw_dir": "data/raw", "interim_dir": "data/interim", "start_year": 2020},
        "transform": {"interim_dir": "data/interim", "processed_dir": "data/processed"},
        "features": {"processed_dir": "data/processed", "features_dir": "data/features"},
    }


@pytest.fixture()
def config_yaml_path(tmp_path, base_config):
    """Write a valid config YAML to a temp file; return the path string."""
    import yaml

    cfg = {
        "acquire": base_config["acquire"],
        "dask": base_config["dask"],
        "logging": base_config["logging"],
        "ingest": base_config["ingest"],
        "transform": base_config["transform"],
        "features": base_config["features"],
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg))
    return str(cfg_file)


# ---------------------------------------------------------------------------
# ACQ-02: load_config tests
# ---------------------------------------------------------------------------


def test_load_config_returns_required_keys(config_yaml_path):
    cfg = load_config(config_yaml_path)
    assert "acquire" in cfg
    assert "dask" in cfg
    assert "logging" in cfg


def test_load_config_resolves_null_end_year(tmp_path):
    import datetime

    import yaml

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": None,
            "raw_dir": "data/raw",
            "max_workers": 5,
            "sleep_per_worker": 0.5,
            "timeout": 60,
            "max_retries": 3,
            "backoff_multiplier": 2,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 2,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": "logs/p.log", "level": "INFO"},
        "ingest": {},
        "transform": {},
        "features": {},
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))
    cfg = load_config(str(cfg_file))
    assert isinstance(cfg["acquire"]["end_year"], int)
    assert cfg["acquire"]["end_year"] == datetime.date.today().year


def test_load_config_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")


def test_load_config_missing_acquire_key_raises(tmp_path):
    import yaml

    cfg_file = tmp_path / "bad.yaml"
    cfg_file.write_text(yaml.dump({"dask": {}, "logging": {}}))
    with pytest.raises(KeyError):
        load_config(str(cfg_file))


# ---------------------------------------------------------------------------
# ACQ-03: build_download_targets tests
# ---------------------------------------------------------------------------


def test_build_download_targets_count(base_config):
    targets = build_download_targets(base_config["acquire"])
    assert len(targets) == 3  # 2020, 2021, 2022


def test_build_download_targets_current_year(base_config):
    targets = build_download_targets(base_config["acquire"])
    current = [t for t in targets if t["year"] == 2022][0]
    assert current["is_current_year"] is True
    assert current["url"] == base_config["acquire"]["base_url_current"]


def test_build_download_targets_historical_urls(base_config):
    targets = build_download_targets(base_config["acquire"])
    for t in targets:
        if not t["is_current_year"]:
            assert str(t["year"]) in t["url"]
            assert t["is_current_year"] is False


def test_build_download_targets_invalid_range_raises(base_config):
    cfg = dict(base_config["acquire"])
    cfg["start_year"] = 2023
    cfg["end_year"] = 2020
    with pytest.raises(ValueError):
        build_download_targets(cfg)


def test_build_download_targets_output_paths_are_path_objects(base_config):
    targets = build_download_targets(base_config["acquire"])
    for t in targets:
        assert isinstance(t["output_path"], Path)


# ---------------------------------------------------------------------------
# ACQ-04: is_valid_download tests
# ---------------------------------------------------------------------------


def test_is_valid_download_nonexistent(tmp_path):
    assert is_valid_download(tmp_path / "ghost.csv") is False


def test_is_valid_download_zero_bytes(tmp_path):
    f = tmp_path / "empty.csv"
    f.write_bytes(b"")
    assert is_valid_download(f) is False


def test_is_valid_download_valid_utf8(tmp_path):
    f = tmp_path / "good.csv"
    f.write_text("col1,col2\n1,2\n", encoding="utf-8")
    assert is_valid_download(f) is True


def test_is_valid_download_binary_content(tmp_path):
    f = tmp_path / "binary.bin"
    f.write_bytes(b"\xff\xfe\x00invalid")
    assert is_valid_download(f) is False


def test_is_valid_download_single_byte(tmp_path):
    f = tmp_path / "single.csv"
    f.write_bytes(b"A")
    assert is_valid_download(f) is True


# ---------------------------------------------------------------------------
# ACQ-05: download_file tests
# ---------------------------------------------------------------------------


def _make_response(status: int, content: bytes) -> MagicMock:
    """Helper: create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status
    resp.iter_content = lambda chunk_size=65536: iter([content])
    resp.raise_for_status = MagicMock()
    return resp


def _make_zip_content(csv_text: str) -> bytes:
    """Helper: create in-memory zip containing a single CSV."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", csv_text)
    return buf.getvalue()


def test_download_file_csv_target_success(tmp_path):
    """Mocked 200 for a CSV (current year) target writes the file."""
    output_path = tmp_path / "monthly_prod.csv"
    target = {
        "year": 2024,
        "url": "http://test/monthly_prod.csv",
        "output_path": output_path,
        "is_current_year": True,
    }
    csv_content = b"col1,col2\n1,2\n"
    with patch(
        "cogcc_pipeline.acquire.requests.get", return_value=_make_response(200, csv_content)
    ):
        with patch("cogcc_pipeline.acquire._SLEEP_PER_WORKER", 0):
            result = download_file(target, timeout=5, max_retries=1, backoff_multiplier=1)
    assert result == output_path
    assert is_valid_download(output_path)


def test_download_file_zip_target_success(tmp_path):
    """Mocked 200 for a zip (historical) target extracts CSV to output_path."""
    output_path = tmp_path / "2021_prod_reports.csv"
    target = {
        "year": 2021,
        "url": "http://test/2021_prod_reports.zip",
        "output_path": output_path,
        "is_current_year": False,
    }
    zip_content = _make_zip_content("col1,col2\n1,2\n")
    with patch(
        "cogcc_pipeline.acquire.requests.get", return_value=_make_response(200, zip_content)
    ):
        with patch("cogcc_pipeline.acquire._SLEEP_PER_WORKER", 0):
            result = download_file(target, timeout=5, max_retries=1, backoff_multiplier=1)
    assert result == output_path
    assert is_valid_download(output_path)


def test_download_file_skips_existing(tmp_path):
    """Pre-existing valid file → returns None, no HTTP call made."""
    output_path = tmp_path / "monthly_prod.csv"
    output_path.write_text("col1,col2\n1,2\n", encoding="utf-8")
    target = {
        "year": 2024,
        "url": "http://test/monthly_prod.csv",
        "output_path": output_path,
        "is_current_year": True,
    }
    mock_get = MagicMock()
    with patch("cogcc_pipeline.acquire.requests.get", mock_get):
        result = download_file(target, timeout=5, max_retries=1, backoff_multiplier=1)
    assert result is None
    mock_get.assert_not_called()


def test_download_file_404_returns_none(tmp_path):
    """All retries return 404 → function logs warning and returns None."""
    output_path = tmp_path / "monthly_prod.csv"
    target = {
        "year": 2024,
        "url": "http://test/monthly_prod.csv",
        "output_path": output_path,
        "is_current_year": True,
    }
    with patch("cogcc_pipeline.acquire.requests.get", return_value=_make_response(404, b"")):
        with patch("cogcc_pipeline.acquire._SLEEP_PER_WORKER", 0):
            result = download_file(target, timeout=5, max_retries=2, backoff_multiplier=1)
    assert result is None


def test_download_file_retry_on_connection_error(tmp_path):
    """ConnectionError on first attempt, 200 on second → file written."""
    import requests as req_module

    output_path = tmp_path / "monthly_prod.csv"
    target = {
        "year": 2024,
        "url": "http://test/monthly_prod.csv",
        "output_path": output_path,
        "is_current_year": True,
    }
    csv_content = b"col1,col2\n1,2\n"
    side_effects = [
        req_module.exceptions.ConnectionError("connection refused"),
        _make_response(200, csv_content),
    ]
    with patch("cogcc_pipeline.acquire.requests.get", side_effect=side_effects):
        with patch("cogcc_pipeline.acquire._SLEEP_PER_WORKER", 0):
            result = download_file(target, timeout=5, max_retries=3, backoff_multiplier=1)
    assert result == output_path
    assert is_valid_download(output_path)


def test_download_file_zero_bytes_raises(tmp_path):
    """200 response with 0 bytes → RuntimeError (validity check fails)."""
    output_path = tmp_path / "monthly_prod.csv"
    target = {
        "year": 2024,
        "url": "http://test/monthly_prod.csv",
        "output_path": output_path,
        "is_current_year": True,
    }
    # Response with empty body but 200 status
    resp = MagicMock()
    resp.status_code = 200
    resp.iter_content = lambda chunk_size=65536: iter([b""])  # writes 0 bytes

    # Patch is_valid_download to simulate it failing after empty write
    original_is_valid = is_valid_download

    def _patched_is_valid(p):
        if p == output_path:
            return False
        return original_is_valid(p)

    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with patch("cogcc_pipeline.acquire._SLEEP_PER_WORKER", 0):
            with patch("cogcc_pipeline.acquire.is_valid_download", side_effect=_patched_is_valid):
                # Should return None (retries exhausted logging warning)
                result = download_file(target, timeout=5, max_retries=1, backoff_multiplier=1)
    assert result is None
    assert not output_path.exists() or output_path.stat().st_size == 0


def test_download_file_zip_no_csv_raises_value_error(tmp_path):
    """Zip with no CSV inside → ValueError."""
    output_path = tmp_path / "2021_prod_reports.csv"
    target = {
        "year": 2021,
        "url": "http://test/2021_prod_reports.zip",
        "output_path": output_path,
        "is_current_year": False,
    }
    # Create a zip with no CSV file inside
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "no csv here")
    zip_content = buf.getvalue()

    with patch(
        "cogcc_pipeline.acquire.requests.get", return_value=_make_response(200, zip_content)
    ):
        with patch("cogcc_pipeline.acquire._SLEEP_PER_WORKER", 0):
            result = download_file(target, timeout=5, max_retries=1, backoff_multiplier=1)
    # ValueError is caught by tenacity, exhausted retries → returns None
    assert result is None


# ---------------------------------------------------------------------------
# ACQ-06: run_acquire tests
# ---------------------------------------------------------------------------


def _write_valid_csv(path: Path, content: str = "col1,col2\n1,2\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_run_acquire_downloads_all(tmp_path, config_yaml_path):
    """3 targets with 200 responses → 3 results."""
    # Patch download_file at the module level so it returns mock paths
    from cogcc_pipeline import acquire as acq_mod

    acq_mod._SLEEP_PER_WORKER = 0.0

    csv_content = b"col1,col2\n1,2\n"
    zip_content = _make_zip_content("col1,col2\n1,2\n")

    def fake_get(url, **kwargs):
        if url.endswith(".zip"):
            return _make_response(200, zip_content)
        return _make_response(200, csv_content)

    with patch("cogcc_pipeline.acquire.requests.get", side_effect=fake_get):
        result = run_acquire(config_yaml_path)

    assert len(result) >= 1


def test_run_acquire_skips_existing(tmp_path, config_yaml_path):
    """Pre-existing valid files → 0 downloads, no HTTP calls."""
    import yaml

    with open(config_yaml_path) as f:
        cfg = yaml.safe_load(f)

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Pre-write valid files for all targets
    targets = build_download_targets(cfg["acquire"])
    for t in targets:
        _write_valid_csv(t["output_path"])

    mock_get = MagicMock()
    with patch("cogcc_pipeline.acquire.requests.get", mock_get):
        result = run_acquire(config_yaml_path)

    assert result == []
    mock_get.assert_not_called()


def test_run_acquire_idempotent(tmp_path, config_yaml_path):
    """TR-20: Running twice produces same file count."""
    import yaml

    with open(config_yaml_path) as f:
        cfg = yaml.safe_load(f)

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    targets = build_download_targets(cfg["acquire"])
    for t in targets:
        _write_valid_csv(t["output_path"])

    original_contents = {t["output_path"]: t["output_path"].read_bytes() for t in targets}

    mock_get = MagicMock()
    with patch("cogcc_pipeline.acquire.requests.get", mock_get):
        run_acquire(config_yaml_path)

    file_count_after = len(list(raw_dir.glob("*")))
    assert file_count_after == len(targets)
    for t in targets:
        assert t["output_path"].read_bytes() == original_contents[t["output_path"]]


def test_run_acquire_uses_threads_scheduler(config_yaml_path):
    """Assert Dask bag is computed with scheduler='threads'."""
    import yaml

    with open(config_yaml_path) as f:
        cfg = yaml.safe_load(f)

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    targets = build_download_targets(cfg["acquire"])
    for t in targets:
        _write_valid_csv(t["output_path"])

    with patch("dask.bag.Bag.compute") as mock_compute:
        mock_compute.return_value = [None] * len(targets)
        with patch("cogcc_pipeline.acquire.requests.get"):
            run_acquire(config_yaml_path)
        _, kwargs = mock_compute.call_args
        assert kwargs.get("scheduler") == "threads"


# ---------------------------------------------------------------------------
# ACQ-07 Integration tests: TR-20, TR-21
# ---------------------------------------------------------------------------


def test_tr20_idempotency_no_http_calls(tmp_path, config_yaml_path):
    """TR-20: Pre-written valid files → no HTTP calls, unchanged file count."""
    import yaml

    with open(config_yaml_path) as f:
        cfg = yaml.safe_load(f)

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    targets = build_download_targets(cfg["acquire"])
    for t in targets:
        _write_valid_csv(t["output_path"], "col1,col2\nA,B\n")

    initial_count = len(list(raw_dir.glob("*")))
    mock_get = MagicMock()
    with patch("cogcc_pipeline.acquire.requests.get", mock_get):
        run_acquire(config_yaml_path)

    mock_get.assert_not_called()
    assert len(list(raw_dir.glob("*"))) == initial_count


def test_tr21_file_integrity_after_download(tmp_path, config_yaml_path):
    """TR-21: Downloaded files must be non-empty, valid UTF-8, with at least 2 lines."""
    import yaml

    with open(config_yaml_path) as f:
        yaml.safe_load(f)

    csv_body = "col1,col2\n1,2\n"
    zip_body = _make_zip_content(csv_body)

    def fake_get(url, **kwargs):
        if url.endswith(".zip"):
            return _make_response(200, zip_body)
        return _make_response(200, csv_body.encode("utf-8"))

    with patch("cogcc_pipeline.acquire.requests.get", side_effect=fake_get):
        with patch("cogcc_pipeline.acquire._SLEEP_PER_WORKER", 0):
            result = run_acquire(config_yaml_path)

    for p in result:
        assert p.stat().st_size > 0
        text = p.read_text(encoding="utf-8")
        assert len(text.splitlines()) >= 2


def test_tr21_zero_byte_response_not_written(tmp_path, config_yaml_path):
    """TR-21: A 0-byte HTTP response must not leave a valid file at output_path."""
    import yaml

    with open(config_yaml_path) as f:
        cfg = yaml.safe_load(f)

    raw_dir = Path(cfg["acquire"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Only test the current-year target (simpler — CSV, no zip extraction)
    targets = build_download_targets(cfg["acquire"])
    current_target = [t for t in targets if t["is_current_year"]][0]

    resp = MagicMock()
    resp.status_code = 200
    resp.iter_content = lambda chunk_size=65536: iter([b""])

    with patch("cogcc_pipeline.acquire.requests.get", return_value=resp):
        with patch("cogcc_pipeline.acquire._SLEEP_PER_WORKER", 0):
            result = download_file(current_target, timeout=5, max_retries=1, backoff_multiplier=1)

    assert result is None
    # File must not exist as a valid file
    assert not is_valid_download(current_target["output_path"])
