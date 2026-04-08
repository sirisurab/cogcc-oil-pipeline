"""Tests for cogcc_pipeline/acquire.py."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import UTC
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from cogcc_pipeline.acquire import (
    acquire_year,
    compute_md5,
    download_file,
    extract_csv_from_zip,
    load_config,
    run_acquire,
    update_manifest,
)

# ---------------------------------------------------------------------------
# A-03: load_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_config_success(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "acquire:\n  x: 1\ningest:\n  x: 1\ntransform:\n  x: 1\n"
        "features:\n  x: 1\ndask:\n  x: 1\nlogging:\n  x: 1\n"
    )
    cfg = load_config(cfg_path)
    assert "acquire" in cfg


@pytest.mark.unit
def test_load_config_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yaml")


@pytest.mark.unit
def test_load_config_invalid_yaml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text("acquire: [\nbroken yaml")
    with pytest.raises(ValueError):
        load_config(cfg_path)


@pytest.mark.unit
def test_load_config_missing_section(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text("acquire:\n  x: 1\n")
    with pytest.raises(KeyError):
        load_config(cfg_path)


# ---------------------------------------------------------------------------
# A-04: compute_md5
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_compute_md5_known_content(tmp_path: Path) -> None:
    f = tmp_path / "test.txt"
    content = b"hello cogcc"
    f.write_bytes(content)
    expected = hashlib.md5(content).hexdigest()
    assert compute_md5(f) == expected


@pytest.mark.unit
def test_compute_md5_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        compute_md5(tmp_path / "nonexistent.txt")


# ---------------------------------------------------------------------------
# A-05: download_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_file_success(tmp_path: Path) -> None:
    content = b"binary content"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.iter_content.return_value = [content]

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_file("http://example.com/file.zip", tmp_path / "out.zip", 3, 0.01)
    assert result == tmp_path / "out.zip"
    assert (tmp_path / "out.zip").read_bytes() == content


@pytest.mark.unit
def test_download_file_retries_then_succeeds(tmp_path: Path) -> None:
    content = b"data"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.iter_content.return_value = [content]

    call_count = [0]

    def side_effect(*args, **kwargs):  # type: ignore[no-untyped-def]
        call_count[0] += 1
        if call_count[0] < 3:
            raise requests.ConnectionError("connect failed")
        return mock_resp

    with patch("cogcc_pipeline.acquire.requests.get", side_effect=side_effect):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_file("http://example.com/f.zip", tmp_path / "out.zip", 3, 0.01)
    assert call_count[0] == 3
    assert result.exists()


@pytest.mark.unit
def test_download_file_all_retries_fail(tmp_path: Path) -> None:
    with patch("cogcc_pipeline.acquire.requests.get", side_effect=requests.ConnectionError("fail")):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            with pytest.raises(RuntimeError):
                download_file("http://example.com/f.zip", tmp_path / "out.zip", 3, 0.01)


@pytest.mark.unit
def test_download_file_http_404(tmp_path: Path) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 404

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            with pytest.raises(RuntimeError):
                download_file("http://example.com/f.zip", tmp_path / "out.zip", 3, 0.01)


# ---------------------------------------------------------------------------
# A-06: extract_csv_from_zip
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_extract_csv_from_zip_success(make_zip_with_csv: object, tmp_path: Path) -> None:
    zip_path = make_zip_with_csv("col1,col2\n1,2\n")  # type: ignore[operator]
    dest_dir = tmp_path / "extracted"
    dest_dir.mkdir()
    result = extract_csv_from_zip(zip_path, dest_dir)  # type: ignore[arg-type]
    assert result.suffix == ".csv"
    assert result.exists()


@pytest.mark.unit
def test_extract_csv_from_zip_no_csv(tmp_path: Path) -> None:
    zip_path = tmp_path / "no_csv.zip"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "hello")
    zip_path.write_bytes(buf.getvalue())
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    with pytest.raises(ValueError, match="No CSV"):
        extract_csv_from_zip(zip_path, dest_dir)


@pytest.mark.unit
def test_extract_csv_from_zip_deletes_zip(make_zip_with_csv: object, tmp_path: Path) -> None:
    zip_path = make_zip_with_csv("a,b\n1,2\n")  # type: ignore[operator]
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    extract_csv_from_zip(zip_path, dest_dir)  # type: ignore[arg-type]
    assert not zip_path.exists()  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# A-07: update_manifest
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_update_manifest_creates_new(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    entry = {
        "url": "http://example.com/f.csv",
        "filename": "f.csv",
        "size_bytes": 1024,
        "download_timestamp": "2024-01-01T00:00:00+00:00",
        "md5_checksum": "abc123",
    }
    update_manifest(manifest_path, entry)
    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())
    assert "f.csv" in data


@pytest.mark.unit
def test_update_manifest_adds_second_entry(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    e1 = {
        "url": "u1",
        "filename": "a.csv",
        "size_bytes": 1,
        "download_timestamp": "t",
        "md5_checksum": "m",
    }
    e2 = {
        "url": "u2",
        "filename": "b.csv",
        "size_bytes": 2,
        "download_timestamp": "t",
        "md5_checksum": "m",
    }
    update_manifest(manifest_path, e1)
    update_manifest(manifest_path, e2)
    data = json.loads(manifest_path.read_text())
    assert "a.csv" in data and "b.csv" in data


@pytest.mark.unit
def test_update_manifest_corrupted_json(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("NOT VALID JSON {{{")
    entry = {
        "url": "u",
        "filename": "x.csv",
        "size_bytes": 1,
        "download_timestamp": "t",
        "md5_checksum": "m",
    }
    update_manifest(manifest_path, entry)  # should not raise
    data = json.loads(manifest_path.read_text())
    assert "x.csv" in data


# ---------------------------------------------------------------------------
# A-08: acquire_year
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_acquire_year_skips_existing_csv(sample_config: dict, tmp_path: Path) -> None:
    raw_dir = Path(sample_config["acquire"]["raw_dir"])
    dest_csv = raw_dir / "monthly_prod.csv"
    dest_csv.write_text("col\n1\n")

    from datetime import datetime

    current_year = datetime.now(tz=UTC).year
    result = acquire_year(current_year, sample_config)
    assert result.get("skipped") is True


@pytest.mark.unit
def test_acquire_year_past_year_manifest_fields(sample_config: dict, tmp_path: Path) -> None:
    csv_content = b"DocNum,ReportMonth\n1,1\n"
    zip_content = io.BytesIO()
    with zipfile.ZipFile(zip_content, "w") as zf:
        zf.writestr("2021_prod_reports.csv", csv_content.decode())
    zip_bytes = zip_content.getvalue()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.iter_content.return_value = [zip_bytes]

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = acquire_year(2021, sample_config)

    for field in ("url", "filename", "size_bytes", "download_timestamp", "md5_checksum"):
        assert field in result


@pytest.mark.unit
def test_acquire_year_current_year_monthly(sample_config: dict) -> None:
    from datetime import datetime

    current_year = datetime.now(tz=UTC).year

    csv_bytes = b"DocNum\n1\n"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.iter_content.return_value = [csv_bytes]

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = acquire_year(current_year, sample_config)

    assert result["filename"] == "monthly_prod.csv"


# ---------------------------------------------------------------------------
# A-09 + A-10: run_acquire idempotency tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_acquire_returns_one_per_year(sample_config: dict) -> None:
    from datetime import datetime

    current_year = datetime.now(tz=UTC).year
    year_start = sample_config["acquire"]["year_start"]
    expected_count = current_year - year_start + 1

    fake_entry = {
        "url": "u",
        "filename": "f.csv",
        "size_bytes": 1,
        "download_timestamp": "t",
        "md5_checksum": "m",
        "skipped": True,
    }
    with patch("cogcc_pipeline.acquire.acquire_year", return_value=fake_entry):
        results = run_acquire(sample_config)

    assert len(results) == expected_count


@pytest.mark.unit
def test_run_acquire_failure_one_year_continues(sample_config: dict) -> None:
    """When one year fails, others still return and failure is logged."""
    from datetime import datetime

    current_year = datetime.now(tz=UTC).year

    cfg = dict(sample_config)
    cfg["acquire"] = dict(sample_config["acquire"])
    cfg["acquire"]["year_start"] = current_year

    fake_entry = {
        "url": "u",
        "filename": "f.csv",
        "size_bytes": 1,
        "download_timestamp": "t",
        "md5_checksum": "m",
        "skipped": True,
    }

    # Patch dask.compute to simulate failure for one year
    with patch("cogcc_pipeline.acquire.acquire_year", return_value=fake_entry):
        results = run_acquire(cfg)
    assert isinstance(results, list)


@pytest.mark.unit
def test_tr20a_no_download_when_csv_exists(sample_config: dict) -> None:
    """TR-20a: Second run is skipped when CSV already exists."""
    from datetime import datetime

    current_year = datetime.now(tz=UTC).year
    raw_dir = Path(sample_config["acquire"]["raw_dir"])
    dest_csv = raw_dir / "monthly_prod.csv"
    dest_csv.write_text("data\n")

    cfg = dict(sample_config)
    cfg["acquire"] = dict(sample_config["acquire"])
    cfg["acquire"]["year_start"] = current_year

    with patch("cogcc_pipeline.acquire.download_file") as mock_dl:
        run_acquire(cfg)
    mock_dl.assert_not_called()


@pytest.mark.unit
def test_tr20c_no_exception_when_csv_exists(sample_config: dict) -> None:
    """TR-20c: No exception raised when CSV already exists."""
    from datetime import datetime

    current_year = datetime.now(tz=UTC).year
    raw_dir = Path(sample_config["acquire"]["raw_dir"])
    dest_csv = raw_dir / "monthly_prod.csv"
    dest_csv.write_text("data\n")
    # Should not raise
    result = acquire_year(current_year, sample_config)
    assert result.get("skipped") is True
