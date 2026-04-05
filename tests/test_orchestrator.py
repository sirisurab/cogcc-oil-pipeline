"""Tests for cogcc_pipeline.orchestrator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cogcc_pipeline.orchestrator import orchestrate


@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    yaml_content = (
        "acquire:\n"
        "  base_url_zip: 'http://x.com/{year}.zip'\n"
        "  base_url_monthly: 'http://x.com/monthly.csv'\n"
        "  data_dictionary_url: 'http://x.com/dict.htm'\n"
        "  start_year: 2020\n"
        "  raw_dir: 'data/raw'\n"
        "  references_dir: 'references'\n"
        "  max_workers: 2\n"
        "  sleep_seconds: 0.0\n"
        "ingest:\n"
        "  raw_dir: 'data/raw'\n"
        "  interim_dir: 'data/interim'\n"
        "  start_year: 2020\n"
        "transform:\n"
        "  interim_dir: 'data/interim'\n"
        "  processed_dir: 'data/processed'\n"
        "  cleaned_file: 'cogcc_production_cleaned.parquet'\n"
        "features:\n"
        "  processed_dir: 'data/processed'\n"
        "  cleaned_file: 'cogcc_production_cleaned.parquet'\n"
        "  features_file: 'cogcc_production_features.parquet'\n"
        "  quality_report: 'data_quality_report.json'\n"
        "  rolling_windows: [3, 6, 12]\n"
        "logging:\n"
        "  log_dir: '" + str(tmp_path / "logs") + "'\n"
        "  log_file: 'pipeline.log'\n"
        "  level: 'INFO'\n"
    )
    cfg = tmp_path / "config.yaml"
    cfg.write_text(yaml_content)
    return cfg


@pytest.mark.unit
def test_orchestrate_calls_all_stages_in_order(config_file: Path, tmp_path: Path) -> None:
    """All four run_* stages should be called exactly once in correct order."""
    call_order: list[str] = []

    def _acquire(cfg: dict, lg: object) -> list:
        call_order.append("acquire")
        return []

    def _ingest(cfg: dict, lg: object) -> Path:
        call_order.append("ingest")
        return tmp_path / "interim"

    def _transform(cfg: dict, lg: object) -> Path:
        call_order.append("transform")
        return tmp_path / "cleaned"

    def _features(cfg: dict, lg: object) -> Path:
        call_order.append("features")
        return tmp_path / "features"

    with (
        patch("cogcc_pipeline.orchestrator.run_acquire", side_effect=_acquire),
        patch("cogcc_pipeline.orchestrator.validate_raw_files", return_value=[]),
        patch("cogcc_pipeline.orchestrator.run_ingest", side_effect=_ingest),
        patch("cogcc_pipeline.orchestrator.run_transform", side_effect=_transform),
        patch("cogcc_pipeline.orchestrator.run_features", side_effect=_features),
    ):
        orchestrate(config_file)

    assert call_order == ["acquire", "ingest", "transform", "features"]


@pytest.mark.unit
def test_orchestrate_stops_on_ingest_failure(config_file: Path, tmp_path: Path) -> None:
    """If ingest raises, transform and features should NOT be called."""
    transform_mock = MagicMock()
    features_mock = MagicMock()

    with (
        patch("cogcc_pipeline.orchestrator.run_acquire", return_value=[]),
        patch("cogcc_pipeline.orchestrator.validate_raw_files", return_value=[]),
        patch(
            "cogcc_pipeline.orchestrator.run_ingest",
            side_effect=RuntimeError("ingest failed"),
        ),
        patch("cogcc_pipeline.orchestrator.run_transform", transform_mock),
        patch("cogcc_pipeline.orchestrator.run_features", features_mock),
    ):
        with pytest.raises(RuntimeError, match="ingest failed"):
            orchestrate(config_file)

    transform_mock.assert_not_called()
    features_mock.assert_not_called()


@pytest.mark.unit
def test_orchestrate_returns_none_on_success(config_file: Path, tmp_path: Path) -> None:
    """orchestrate should return None when all stages succeed."""
    with (
        patch("cogcc_pipeline.orchestrator.run_acquire", return_value=[]),
        patch("cogcc_pipeline.orchestrator.validate_raw_files", return_value=[]),
        patch("cogcc_pipeline.orchestrator.run_ingest", return_value=tmp_path / "interim"),
        patch("cogcc_pipeline.orchestrator.run_transform", return_value=tmp_path / "cleaned"),
        patch("cogcc_pipeline.orchestrator.run_features", return_value=tmp_path / "features"),
    ):
        orchestrate(config_file)


@pytest.mark.unit
def test_orchestrate_propagates_transform_exception(config_file: Path, tmp_path: Path) -> None:
    """If transform raises, features should NOT be called."""
    features_mock = MagicMock()

    with (
        patch("cogcc_pipeline.orchestrator.run_acquire", return_value=[]),
        patch("cogcc_pipeline.orchestrator.validate_raw_files", return_value=[]),
        patch("cogcc_pipeline.orchestrator.run_ingest", return_value=tmp_path / "interim"),
        patch(
            "cogcc_pipeline.orchestrator.run_transform",
            side_effect=ValueError("transform failed"),
        ),
        patch("cogcc_pipeline.orchestrator.run_features", features_mock),
    ):
        with pytest.raises(ValueError, match="transform failed"):
            orchestrate(config_file)

    features_mock.assert_not_called()


@pytest.mark.unit
def test_orchestrate_logs_validation_errors(config_file: Path, tmp_path: Path) -> None:
    """Validation errors from validate_raw_files should be logged but not abort."""
    with (
        patch("cogcc_pipeline.orchestrator.run_acquire", return_value=[]),
        patch("cogcc_pipeline.orchestrator.validate_raw_files", return_value=["error1"]),
        patch("cogcc_pipeline.orchestrator.run_ingest", return_value=tmp_path / "interim"),
        patch("cogcc_pipeline.orchestrator.run_transform", return_value=tmp_path / "cleaned"),
        patch("cogcc_pipeline.orchestrator.run_features", return_value=tmp_path / "features"),
    ):
        # Should not raise even with validation errors
        orchestrate(config_file)
