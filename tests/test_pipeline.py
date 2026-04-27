"""Tests for the pipeline entry point and orchestrator."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINIMAL_HEADER = (
    "DocNum,ReportMonth,ReportYear,DaysProduced,AcceptedDate,Revised,OpName,OpNumber,"
    "FacilityId,ApiCountyCode,ApiSequenceNumber,ApiSidetrack,Well,WellStatus,"
    "FormationCode,OilProduced,OilSales,OilAdjustment,OilGravity,GasProduced,GasSales,"
    "GasBtuSales,GasUsedOnLease,GasShrinkage,GasPressureTubing,GasPressureCasing,"
    "WaterProduced,WaterPressureTubing,WaterPressureCasing,FlaredVented,BomInvent,EomInvent"
)
_MINIMAL_ROW = (
    "1,3,2021,28,2021-04-01,False,OpA,100,200,05,12345,00,Well A,AC,SandA,"
    "500.0,490.0,0.0,35.0,1000.0,980.0,1100.0,10.0,5.0,100.0,90.0,200.0,50.0,45.0,2.0,10.0,8.0"
)


def _write_config(
    tmp_path: Path,
    raw_dir: str,
    interim_dir: str,
    processed_dir: str,
    features_dir: str,
) -> str:
    cfg = {
        "acquire": {
            "zip_url_template": "https://example.com/{year}_prod_reports.zip",
            "monthly_url": "https://example.com/monthly_prod.csv",
            "start_year": 2024,
            "raw_dir": raw_dir,
            "max_workers": 1,
            "sleep_seconds": 0.0,
        },
        "ingest": {
            "raw_dir": raw_dir,
            "interim_dir": interim_dir,
            "data_dictionary": "references/production-data-dictionary.csv",
            "min_year": 2020,
        },
        "transform": {
            "interim_dir": interim_dir,
            "processed_dir": processed_dir,
            "entity_col": "ApiSequenceNumber",
            "data_dictionary": "references/production-data-dictionary.csv",
        },
        "features": {
            "processed_dir": processed_dir,
            "output_dir": features_dir,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8799,
        },
        "logging": {
            "log_file": str(tmp_path / "logs" / "pipeline.log"),
            "level": "INFO",
        },
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(cfg))
    return str(p)


# ---------------------------------------------------------------------------
# Task 03: main() — unit tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_main_acquire_only_does_not_init_distributed(tmp_path: Path) -> None:
    """Given acquire-only, distributed Client must NOT be initialized."""
    cfg_path = _write_config(
        tmp_path,
        raw_dir=str(tmp_path / "raw"),
        interim_dir=str(tmp_path / "interim"),
        processed_dir=str(tmp_path / "processed"),
        features_dir=str(tmp_path / "features"),
    )
    (tmp_path / "raw").mkdir(parents=True)

    with (
        patch("cogcc_pipeline.pipeline._init_dask_client") as mock_init,
        patch("cogcc_pipeline.acquire.acquire", return_value=[]) as mock_acq,
    ):
        from cogcc_pipeline.pipeline import main

        main(["--config", cfg_path, "acquire"])

    mock_init.assert_not_called()
    mock_acq.assert_called_once()


@pytest.mark.unit
def test_main_ingest_initializes_distributed(tmp_path: Path) -> None:
    """Given ingest stage, distributed Client IS initialized."""
    cfg_path = _write_config(
        tmp_path,
        raw_dir=str(tmp_path / "raw"),
        interim_dir=str(tmp_path / "interim"),
        processed_dir=str(tmp_path / "processed"),
        features_dir=str(tmp_path / "features"),
    )
    (tmp_path / "raw").mkdir(parents=True)

    mock_client = MagicMock()
    with (
        patch(
            "cogcc_pipeline.pipeline._init_dask_client", return_value=mock_client
        ) as mock_init,
        patch("cogcc_pipeline.ingest.ingest") as mock_ingest,
    ):
        from cogcc_pipeline.pipeline import main

        main(["--config", cfg_path, "ingest"])

    mock_init.assert_called_once()
    mock_ingest.assert_called_once()


@pytest.mark.unit
def test_main_stops_on_stage_failure(tmp_path: Path) -> None:
    """If a stage raises, subsequent stages are not called."""
    cfg_path = _write_config(
        tmp_path,
        raw_dir=str(tmp_path / "raw"),
        interim_dir=str(tmp_path / "interim"),
        processed_dir=str(tmp_path / "processed"),
        features_dir=str(tmp_path / "features"),
    )
    (tmp_path / "raw").mkdir(parents=True)

    mock_client = MagicMock()
    with (
        patch("cogcc_pipeline.pipeline._init_dask_client", return_value=mock_client),
        patch(
            "cogcc_pipeline.ingest.ingest", side_effect=RuntimeError("ingest failed")
        ),
        patch("cogcc_pipeline.transform.transform") as mock_transform,
    ):
        from cogcc_pipeline.pipeline import main

        with pytest.raises(SystemExit):
            main(["--config", cfg_path, "ingest", "transform"])

    mock_transform.assert_not_called()


@pytest.mark.unit
def test_main_sets_up_logging_before_stages(tmp_path: Path) -> None:
    """Log handlers must be set up before any stage runs."""
    cfg_path = _write_config(
        tmp_path,
        raw_dir=str(tmp_path / "raw"),
        interim_dir=str(tmp_path / "interim"),
        processed_dir=str(tmp_path / "processed"),
        features_dir=str(tmp_path / "features"),
    )
    (tmp_path / "raw").mkdir(parents=True)

    setup_called_before_stage: list[bool] = []

    def fake_acquire(config: dict) -> list:
        root = logging.getLogger()
        setup_called_before_stage.append(len(root.handlers) > 0)
        return []

    with (
        patch("cogcc_pipeline.pipeline._init_dask_client", return_value=MagicMock()),
        patch("cogcc_pipeline.acquire.acquire", side_effect=fake_acquire),
    ):
        from cogcc_pipeline.pipeline import main

        main(["--config", cfg_path, "acquire"])

    assert setup_called_before_stage == [True]


# ---------------------------------------------------------------------------
# Integration: end-to-end pipeline (TR-27)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pipeline_end_to_end(tmp_path: Path) -> None:
    """TR-27: ingest → transform → features on minimal real data, no unhandled exceptions."""
    import dask.dataframe as dd
    import pandas as pd

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    features_dir = tmp_path / "features"

    # Write 2 minimal CSV files to raw
    for i in range(2):
        (raw_dir / f"file{i}.csv").write_text(_MINIMAL_HEADER + "\n" + _MINIMAL_ROW)

    cfg_path = _write_config(
        tmp_path,
        raw_dir=str(raw_dir),
        interim_dir=str(interim_dir),
        processed_dir=str(processed_dir),
        features_dir=str(features_dir),
    )

    from cogcc_pipeline.pipeline import main

    # Run ingest, transform, features without acquire (no network in integration test)
    # Use a local distributed client (1 worker to keep CI fast)
    main(["--config", cfg_path, "ingest", "transform", "features"])

    # (a) ingest output satisfies boundary-ingest-transform.md
    interim_ddf = dd.read_parquet(str(interim_dir))
    assert "ReportYear" in interim_ddf.columns
    assert "OilProduced" in interim_ddf.columns

    # (b) transform output satisfies boundary-transform-features.md
    processed_df = pd.read_parquet(str(processed_dir))
    assert processed_df.index.name == "ApiSequenceNumber"
    assert "production_date" in processed_df.columns

    # (c) features output satisfies TR-26
    features_df = pd.read_parquet(str(features_dir))
    for col in [
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate_oil",
    ]:
        assert col in features_df.columns

    # (d) no unhandled exceptions — reaching here means success
