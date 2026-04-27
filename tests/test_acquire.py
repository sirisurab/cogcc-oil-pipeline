"""Tests for the acquire stage."""

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from cogcc_pipeline.acquire import (
    acquire,
    build_download_targets,
    download_file,
    load_config,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_ACQUIRE_CONFIG = {
    "zip_url_template": "https://example.com/{year}_prod_reports.zip",
    "monthly_url": "https://example.com/monthly_prod.csv",
    "start_year": 2020,
    "raw_dir": "data/raw",
    "max_workers": 2,
    "sleep_seconds": 0.0,
}


def _make_config_file(tmp_path: Path, acquire_cfg: dict) -> str:
    cfg = {"acquire": acquire_cfg}
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(cfg))
    return str(p)


def _make_zip_bytes(csv_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", csv_content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Task 01: load_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_config_returns_all_required_keys(tmp_path: Path) -> None:
    cfg_path = _make_config_file(tmp_path, VALID_ACQUIRE_CONFIG)
    result = load_config(cfg_path)
    for key in [
        "zip_url_template",
        "monthly_url",
        "start_year",
        "raw_dir",
        "max_workers",
        "sleep_seconds",
    ]:
        assert key in result


@pytest.mark.unit
def test_load_config_raises_on_missing_key(tmp_path: Path) -> None:
    bad_cfg = {k: v for k, v in VALID_ACQUIRE_CONFIG.items() if k != "max_workers"}
    cfg_path = _make_config_file(tmp_path, bad_cfg)
    with pytest.raises(KeyError, match="max_workers"):
        load_config(cfg_path)


# ---------------------------------------------------------------------------
# Task 02: build_download_targets
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_download_targets_returns_correct_count() -> None:
    cfg = dict(VALID_ACQUIRE_CONFIG, start_year=2020)
    with patch("cogcc_pipeline.acquire.datetime") as mock_dt:
        mock_dt.now.return_value.year = 2023
        targets = build_download_targets(cfg)
    # 2020, 2021, 2022, 2023 — current year uses monthly URL
    assert len(targets) == 4
    for t in targets:
        assert "data/raw" in t["dest"]


@pytest.mark.unit
def test_build_download_targets_current_year_uses_monthly_url() -> None:
    cfg = dict(VALID_ACQUIRE_CONFIG, start_year=2023)
    with patch("cogcc_pipeline.acquire.datetime") as mock_dt:
        mock_dt.now.return_value.year = 2023
        targets = build_download_targets(cfg)
    current = [t for t in targets if t["year"] == 2023]
    assert len(current) == 1
    assert current[0]["url"] == cfg["monthly_url"]
    assert "monthly_prod.csv" in current[0]["dest"]


@pytest.mark.unit
def test_build_download_targets_no_unresolved_placeholders() -> None:
    cfg = dict(VALID_ACQUIRE_CONFIG, start_year=2020)
    with patch("cogcc_pipeline.acquire.datetime") as mock_dt:
        mock_dt.now.return_value.year = 2023
        targets = build_download_targets(cfg)
    for t in targets:
        assert "{year}" not in t["url"]


# ---------------------------------------------------------------------------
# Task 03: download_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_download_file_skips_existing_file(tmp_path: Path) -> None:
    dest = tmp_path / "2021_prod_reports.csv"
    dest.write_text("header\nrow")
    target = {"url": "https://example.com/2021.zip", "dest": str(dest), "year": 2021}
    with patch("cogcc_pipeline.acquire.requests.get") as mock_get:
        result = download_file(target, 0.0)
    mock_get.assert_not_called()
    assert result == dest


@pytest.mark.unit
def test_download_file_extracts_csv_from_zip(tmp_path: Path) -> None:
    dest = tmp_path / "2021_prod_reports.csv"
    zip_bytes = _make_zip_bytes("col1,col2\n1,2\n")
    target = {
        "url": "https://example.com/2021_prod_reports.zip",
        "dest": str(dest),
        "year": 2021,
    }
    mock_response = MagicMock()
    mock_response.content = zip_bytes
    mock_response.raise_for_status = MagicMock()
    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_response):
        result = download_file(target, 0.0)
    assert result is not None
    assert result.exists()
    assert result.suffix == ".csv"


@pytest.mark.unit
def test_download_file_writes_monthly_csv(tmp_path: Path) -> None:
    dest = tmp_path / "monthly_prod.csv"
    csv_bytes = b"col1,col2\n1,2\n"
    target = {
        "url": "https://example.com/monthly_prod.csv",
        "dest": str(dest),
        "year": 2024,
    }
    mock_response = MagicMock()
    mock_response.content = csv_bytes
    mock_response.raise_for_status = MagicMock()
    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_response):
        result = download_file(target, 0.0)
    assert result is not None
    assert result.read_bytes() == csv_bytes


@pytest.mark.unit
def test_download_file_returns_none_on_network_error(tmp_path: Path) -> None:
    dest = tmp_path / "2021_prod_reports.csv"
    target = {"url": "https://example.com/2021.zip", "dest": str(dest), "year": 2021}
    with patch(
        "cogcc_pipeline.acquire.requests.get",
        side_effect=__import__("requests").exceptions.RequestException("timeout"),
    ):
        result = download_file(target, 0.0)
    assert result is None
    assert not dest.exists()


@pytest.mark.unit
def test_download_file_returns_none_on_empty_response(tmp_path: Path) -> None:
    dest = tmp_path / "2021_prod_reports.csv"
    target = {"url": "https://example.com/2021.zip", "dest": str(dest), "year": 2021}
    mock_response = MagicMock()
    mock_response.content = b""
    mock_response.raise_for_status = MagicMock()
    with patch("cogcc_pipeline.acquire.requests.get", return_value=mock_response):
        result = download_file(target, 0.0)
    assert result is None
    assert not dest.exists()


@pytest.mark.integration
@pytest.mark.withoutresponses
def test_download_file_real_download_produces_valid_file(tmp_path: Path) -> None:
    """TR-21(a)(b)(c): real download produces non-empty readable UTF-8 file."""
    dest = tmp_path / "monthly_prod.csv"
    target = {
        "url": "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv",
        "dest": str(dest),
        "year": 2024,
    }
    result = download_file(target, 0.0)
    assert result is not None
    assert result.stat().st_size > 0
    text = result.read_text(encoding="utf-8")
    lines = text.splitlines()
    assert len(lines) >= 2


# ---------------------------------------------------------------------------
# Task 04: acquire
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_acquire_returns_list_of_paths(tmp_path: Path) -> None:
    cfg = dict(VALID_ACQUIRE_CONFIG, raw_dir=str(tmp_path))
    fake_paths = [tmp_path / f"{y}.csv" for y in range(2020, 2023)]
    for p in fake_paths:
        p.write_text("x")

    with (
        patch("cogcc_pipeline.acquire.build_download_targets") as mock_targets,
        patch("cogcc_pipeline.acquire.download_file") as mock_dl,
    ):
        mock_targets.return_value = [
            {"url": "u", "dest": str(p), "year": 2020 + i}
            for i, p in enumerate(fake_paths)
        ]
        mock_dl.side_effect = fake_paths
        results = acquire(cfg)

    assert len(results) == 3
    assert all(isinstance(r, Path) for r in results)


@pytest.mark.unit
def test_acquire_tolerates_none_result(tmp_path: Path) -> None:
    cfg = dict(VALID_ACQUIRE_CONFIG, raw_dir=str(tmp_path))
    targets = [
        {"url": "u1", "dest": str(tmp_path / "a.csv"), "year": 2020},
        {"url": "u2", "dest": str(tmp_path / "b.csv"), "year": 2021},
    ]
    # Map by URL so the result is deterministic regardless of thread execution order
    url_to_result = {
        "u1": tmp_path / "a.csv",
        "u2": None,
    }

    def _mock_download(target: dict, sleep_seconds: float) -> Path | None:
        return url_to_result[target["url"]]

    with (
        patch("cogcc_pipeline.acquire.build_download_targets", return_value=targets),
        patch("cogcc_pipeline.acquire.download_file", side_effect=_mock_download),
    ):
        results = acquire(cfg)
    # results list preserves target order
    url_to_idx = {t["url"]: i for i, t in enumerate(targets)}
    assert results[url_to_idx["u2"]] is None
    assert results[url_to_idx["u1"]] is not None


@pytest.mark.integration
def test_acquire_idempotency(tmp_path: Path) -> None:
    """TR-20: second run produces same file count, no content change, no error."""
    cfg = dict(VALID_ACQUIRE_CONFIG, raw_dir=str(tmp_path / "raw"), start_year=2024)
    # Patch to current year so only the monthly file is targeted
    with patch("cogcc_pipeline.acquire.datetime") as mock_dt:
        mock_dt.now.return_value.year = 2024
        acquire(cfg)
        files_after_first = {
            p.name: p.read_bytes() for p in (tmp_path / "raw").glob("*.csv")
        }
        acquire(cfg)
        files_after_second = {
            p.name: p.read_bytes() for p in (tmp_path / "raw").glob("*.csv")
        }

    assert len(files_after_first) == len(files_after_second)
    for name in files_after_first:
        assert files_after_first[name] == files_after_second[name]
