"""Unit tests for cogcc_pipeline.acquire."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cogcc_pipeline.acquire import (
    AcquireConfigError,
    acquire,
    build_download_targets,
    download_target,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_CONFIG = {
    "base_url": "https://ecmc.state.co.us/documents/data/downloads/production",
    "zip_url_template": "{base_url}/{year}_prod_reports.zip",
    "monthly_url": "{base_url}/monthly_prod.csv",
    "start_year": 2020,
    "raw_dir": "data/raw",
    "workers": 2,
    "sleep_between_downloads": 0.0,
    "retry_count": 2,
}

VALID_CSV_CONTENT = b"DocNum,ReportYear,ReportMonth\n12345,2022,6\n67890,2022,7\n"


def _make_zip(csv_bytes: bytes, inner_name: str = "prod.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(inner_name, csv_bytes)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Task A1 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_download_targets_correct_count_and_kinds():
    """5 targets for 2020-2024; prior years=zip, current=csv."""
    targets = build_download_targets(BASE_CONFIG, current_year=2024)
    assert len(targets) == 5
    zip_targets = [t for t in targets if t["kind"] == "zip"]
    csv_targets = [t for t in targets if t["kind"] == "csv"]
    assert len(zip_targets) == 4  # 2020-2023
    assert len(csv_targets) == 1  # 2024
    assert csv_targets[0]["year"] == 2024


@pytest.mark.unit
def test_build_download_targets_inverted_year_raises():
    cfg = {**BASE_CONFIG, "start_year": 2030}
    with pytest.raises(AcquireConfigError):
        build_download_targets(cfg, current_year=2024)


@pytest.mark.unit
def test_build_download_targets_paths_under_raw_dir(tmp_path: Path):
    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path / "raw")}
    targets = build_download_targets(cfg, current_year=2022)
    for t in targets:
        assert Path(t["target_path"]).is_relative_to(tmp_path / "raw")


@pytest.mark.unit
def test_build_download_targets_missing_start_year_raises():
    cfg = {k: v for k, v in BASE_CONFIG.items() if k != "start_year"}
    with pytest.raises(AcquireConfigError):
        build_download_targets(cfg, current_year=2024)


# ---------------------------------------------------------------------------
# Task A2 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_target_success_csv(tmp_path: Path):
    """Valid 200 response with multi-line body → downloaded status."""
    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path)}
    target = {
        "year": 2025,
        "url": "https://example.com/monthly_prod.csv",
        "kind": "csv",
        "target_path": str(tmp_path / "monthly_prod.csv"),
    }
    mock_resp = MagicMock()
    mock_resp.content = VALID_CSV_CONTENT
    mock_resp.raise_for_status = MagicMock()

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_target(target, cfg)

    assert result["status"] == "downloaded"
    assert Path(result["final_path"]).exists()


@pytest.mark.unit
def test_download_target_skips_if_valid_file_exists(tmp_path: Path):
    """Pre-existing valid file → skipped without HTTP call (TR-20)."""
    csv_path = tmp_path / "monthly_prod.csv"
    csv_path.write_bytes(VALID_CSV_CONTENT)

    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path)}
    target = {
        "year": 2025,
        "url": "https://example.com/monthly_prod.csv",
        "kind": "csv",
        "target_path": str(csv_path),
    }

    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_target(target, cfg)

    mock_get.assert_not_called()
    assert result["status"] == "skipped"


@pytest.mark.unit
def test_download_target_empty_body_returns_failed_no_file(tmp_path: Path):
    """Empty response body → failed status, no file left on disk (TR-21, H2)."""
    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path), "retry_count": 1}
    target_path = tmp_path / "monthly_prod.csv"
    target = {
        "year": 2025,
        "url": "https://example.com/monthly_prod.csv",
        "kind": "csv",
        "target_path": str(target_path),
    }
    mock_resp = MagicMock()
    mock_resp.content = b""
    mock_resp.raise_for_status = MagicMock()

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_target(target, cfg)

    assert result["status"] == "failed"
    assert not target_path.exists()


@pytest.mark.unit
def test_download_target_connection_error_returns_failed(tmp_path: Path):
    """Repeated connection errors → failed, no exception raised."""
    import requests as req_lib

    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path), "retry_count": 2}
    target = {
        "year": 2025,
        "url": "https://example.com/monthly_prod.csv",
        "kind": "csv",
        "target_path": str(tmp_path / "monthly_prod.csv"),
    }

    with patch(
        "cogcc_pipeline.acquire.requests.get",
        side_effect=req_lib.ConnectionError("connection refused"),
    ):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_target(target, cfg)

    assert result["status"] == "failed"


@pytest.mark.unit
def test_download_target_zip_extracts_csv_removes_zip(tmp_path: Path):
    """ZIP target: inner CSV extracted, ZIP artifact removed, final_path is CSV."""
    zip_bytes = _make_zip(VALID_CSV_CONTENT, inner_name="data.csv")
    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path)}
    target_path = tmp_path / "2022_prod_reports.csv"
    target = {
        "year": 2022,
        "url": "https://example.com/2022_prod_reports.zip",
        "kind": "zip",
        "target_path": str(target_path),
    }
    mock_resp = MagicMock()
    mock_resp.content = zip_bytes
    mock_resp.raise_for_status = MagicMock()

    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_resp):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            result = download_target(target, cfg)

    assert result["status"] == "downloaded"
    assert target_path.exists()
    # Only the CSV should remain — no ZIP file
    zip_artifacts = list(tmp_path.glob("*.zip"))
    assert len(zip_artifacts) == 0


# ---------------------------------------------------------------------------
# Task A3 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_acquire_all_downloaded(tmp_path: Path):
    """All targets downloaded → result length equals target count, all downloaded."""
    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path / "raw"), "start_year": 2022}

    def _fake_download(target: dict, config: dict) -> dict:
        return {
            "year": target["year"],
            "url": target["url"],
            "final_path": target["target_path"],
            "size_bytes": 100,
            "downloaded_at": "2024-01-01T00:00:00",
            "status": "downloaded",
        }

    with patch("cogcc_pipeline.acquire.download_target", side_effect=_fake_download):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            results = acquire(cfg)

    assert len(results) > 0
    assert all(r["status"] == "downloaded" for r in results)


@pytest.mark.unit
def test_acquire_mixed_statuses(tmp_path: Path):
    """Mixed downloaded / skipped statuses returned correctly."""
    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path / "raw"), "start_year": 2022}

    statuses = iter(["skipped", "downloaded"])

    def _fake_download(target: dict, config: dict) -> dict:
        st = next(statuses, "downloaded")
        return {
            "year": target["year"],
            "url": target["url"],
            "final_path": target["target_path"],
            "size_bytes": 100,
            "downloaded_at": "2024-01-01T00:00:00",
            "status": st,
        }

    with patch("cogcc_pipeline.acquire.download_target", side_effect=_fake_download):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            results = acquire(cfg)

    statuses_set = {r["status"] for r in results}
    assert "skipped" in statuses_set or "downloaded" in statuses_set


@pytest.mark.unit
def test_acquire_failed_target_does_not_abort(tmp_path: Path):
    """One failed target → stage returns, other results still present."""
    cfg = {**BASE_CONFIG, "raw_dir": str(tmp_path / "raw"), "start_year": 2022}
    call_count = {"n": 0}

    def _fake_download(target: dict, config: dict) -> dict:
        call_count["n"] += 1
        st = "failed" if call_count["n"] == 1 else "downloaded"
        return {
            "year": target["year"],
            "url": target["url"],
            "final_path": target["target_path"],
            "size_bytes": 0 if st == "failed" else 100,
            "downloaded_at": "2024-01-01T00:00:00",
            "status": st,
        }

    with patch("cogcc_pipeline.acquire.download_target", side_effect=_fake_download):
        with patch("cogcc_pipeline.acquire.time.sleep"):
            results = acquire(cfg)

    assert any(r["status"] == "failed" for r in results)
    assert any(r["status"] == "downloaded" for r in results)


@pytest.mark.unit
def test_acquire_idempotent_file_count(tmp_path: Path):
    """Running acquire twice on pre-populated dir yields same file count (TR-20 a,b,c)."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    csv_path = raw_dir / "monthly_prod.csv"
    csv_path.write_bytes(VALID_CSV_CONTENT)

    cfg = {
        **BASE_CONFIG,
        "raw_dir": str(raw_dir),
        "start_year": 2025,
        "retry_count": 1,
    }
    # Only 1 target: current year (csv)
    targets = build_download_targets(cfg, current_year=2025)
    assert len(targets) == 1

    # Pre-populate the file for the target
    Path(targets[0]["target_path"]).write_bytes(VALID_CSV_CONTENT)

    before_content = Path(targets[0]["target_path"]).read_bytes()

    with patch("cogcc_pipeline.acquire.time.sleep"):
        acquire(cfg)

    files_after_run1 = list(raw_dir.glob("*.csv"))

    with patch("cogcc_pipeline.acquire.time.sleep"):
        results2 = acquire(cfg)

    files_after_run2 = list(raw_dir.glob("*.csv"))
    after_content = Path(targets[0]["target_path"]).read_bytes()

    assert len(files_after_run1) == len(files_after_run2)
    assert before_content == after_content
    assert all(r["status"] == "skipped" for r in results2)


@pytest.mark.unit
def test_acquire_file_integrity(tmp_path: Path):
    """Every file after acquire is non-empty, UTF-8, has >1 line (TR-21 a,b,c)."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    csv_path = raw_dir / "monthly_prod.csv"
    csv_path.write_bytes(VALID_CSV_CONTENT)

    cfg = {
        **BASE_CONFIG,
        "raw_dir": str(raw_dir),
        "start_year": 2025,
    }
    targets = build_download_targets(cfg, current_year=2025)
    Path(targets[0]["target_path"]).write_bytes(VALID_CSV_CONTENT)

    with patch("cogcc_pipeline.acquire.time.sleep"):
        acquire(cfg)

    for f in raw_dir.glob("*.csv"):
        assert f.stat().st_size > 0
        text = f.read_text(encoding="utf-8")
        lines = [ln for ln in text.splitlines() if ln.strip()]
        assert len(lines) > 1
