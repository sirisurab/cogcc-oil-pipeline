"""Shared pytest fixtures for COGCC pipeline tests."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture()
def sample_config(tmp_path: Path) -> dict:
    """Return a synthetic config dict pointing to tmp_path directories."""
    raw_dir = tmp_path / "data" / "raw"
    interim_dir = tmp_path / "data" / "interim"
    processed_dir = tmp_path / "data" / "processed"
    log_file = tmp_path / "logs" / "pipeline.log"
    raw_dir.mkdir(parents=True, exist_ok=True)
    interim_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    return {
        "acquire": {
            "year_start": 2020,
            "max_workers": 2,
            "retry_max_attempts": 2,
            "retry_backoff_factor": 0.01,
            "zip_url_template": "https://example.com/{year}_prod_reports.zip",
            "monthly_url": "https://example.com/monthly_prod.csv",
            "raw_dir": str(raw_dir),
        },
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "year_start": 2020,
        },
        "transform": {
            "interim_dir": str(interim_dir),
            "processed_dir": str(processed_dir),
            "date_start": "2020-01-01",
        },
        "features": {
            "interim_dir": str(interim_dir),
            "processed_dir": str(processed_dir),
            "rolling_windows": [3, 6],
            "decline_rate_clip_min": -1.0,
            "decline_rate_clip_max": 10.0,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "512MB",
            "dashboard_port": 8788,
        },
        "logging": {
            "log_file": str(log_file),
            "level": "WARNING",
        },
    }


@pytest.fixture()
def raw_csv_columns() -> list[str]:
    """Return the canonical raw CSV column list."""
    return [
        "DocNum",
        "ReportMonth",
        "ReportYear",
        "DaysProduced",
        "AcceptedDate",
        "Revised",
        "OpName",
        "OpNumber",
        "FacilityId",
        "ApiCountyCode",
        "ApiSequenceNumber",
        "ApiSidetrack",
        "Well",
        "WellStatus",
        "FormationCode",
        "OilProduced",
        "OilSales",
        "OilAdjustment",
        "OilGravity",
        "GasProduced",
        "GasSales",
        "GasBtuSales",
        "GasUsedOnLease",
        "GasShrinkage",
        "GasPressureTubing",
        "GasPressureCasing",
        "WaterProduced",
        "WaterPressureTubing",
        "WaterPressureCasing",
        "FlaredVented",
        "BomInvent",
        "EomInvent",
    ]


@pytest.fixture()
def sample_raw_df() -> pd.DataFrame:
    """Return a small synthetic raw production DataFrame."""
    return pd.DataFrame(
        {
            "DocNum": pd.array([1, 2, 3], dtype="int64"),
            "ReportMonth": pd.array([1, 2, 3], dtype="int64"),
            "ReportYear": pd.array([2021, 2021, 2021], dtype="int64"),
            "DaysProduced": pd.array([31, 28, 31], dtype=pd.Int64Dtype()),
            "AcceptedDate": pd.to_datetime(["2021-02-01", "2021-03-01", "2021-04-01"]),
            "Revised": pd.array([False, True, False], dtype=pd.BooleanDtype()),
            "OpName": pd.array(
                ["CHEVRON USA", "BP AMERICA", "CHEVRON USA"], dtype=pd.StringDtype()
            ),
            "OpNumber": pd.array([100, 200, 100], dtype=pd.Int64Dtype()),
            "FacilityId": pd.array([10, 20, 30], dtype=pd.Int64Dtype()),
            "ApiCountyCode": pd.array(["05", "05", "05"], dtype=pd.StringDtype()),
            "ApiSequenceNumber": pd.array(["00123", "00456", "00789"], dtype=pd.StringDtype()),
            "ApiSidetrack": pd.array(["00", "00", "00"], dtype=pd.StringDtype()),
            "Well": pd.array(["WELL A", "WELL B", "WELL C"], dtype=pd.StringDtype()),
            "WellStatus": pd.array(["PR", "AB", "PR"], dtype=pd.StringDtype()),
            "FormationCode": pd.array(["NIO", "COD", "NIO"], dtype=pd.StringDtype()),
            "OilProduced": pd.array([100.0, 200.0, 150.0], dtype=pd.Float64Dtype()),
            "OilSales": pd.array([95.0, 195.0, 145.0], dtype=pd.Float64Dtype()),
            "OilAdjustment": pd.array([0.0, 0.0, 0.0], dtype=pd.Float64Dtype()),
            "OilGravity": pd.array([35.0, 38.0, 36.0], dtype=pd.Float64Dtype()),
            "GasProduced": pd.array([500.0, 1000.0, 750.0], dtype=pd.Float64Dtype()),
            "GasSales": pd.array([480.0, 980.0, 730.0], dtype=pd.Float64Dtype()),
            "GasBtuSales": pd.array([480000.0, 980000.0, 730000.0], dtype=pd.Float64Dtype()),
            "GasUsedOnLease": pd.array([20.0, 20.0, 20.0], dtype=pd.Float64Dtype()),
            "GasShrinkage": pd.array([0.0, 0.0, 0.0], dtype=pd.Float64Dtype()),
            "GasPressureTubing": pd.array([100.0, 110.0, 105.0], dtype=pd.Float64Dtype()),
            "GasPressureCasing": pd.array([95.0, 105.0, 100.0], dtype=pd.Float64Dtype()),
            "WaterProduced": pd.array([50.0, 80.0, 60.0], dtype=pd.Float64Dtype()),
            "WaterPressureTubing": pd.array([50.0, 55.0, 52.0], dtype=pd.Float64Dtype()),
            "WaterPressureCasing": pd.array([45.0, 50.0, 47.0], dtype=pd.Float64Dtype()),
            "FlaredVented": pd.array([0.0, 0.0, 0.0], dtype=pd.Float64Dtype()),
            "BomInvent": pd.array([0.0, 0.0, 0.0], dtype=pd.Float64Dtype()),
            "EomInvent": pd.array([0.0, 0.0, 0.0], dtype=pd.Float64Dtype()),
        }
    )


@pytest.fixture()
def sample_cleaned_df() -> pd.DataFrame:
    """Return a synthetic cleaned production DataFrame with derived columns."""
    df = pd.DataFrame(
        {
            "DocNum": pd.array([1, 2, 3, 4, 5, 6], dtype="int64"),
            "ReportMonth": pd.array([1, 2, 3, 1, 2, 3], dtype="int64"),
            "ReportYear": pd.array([2021, 2021, 2021, 2021, 2021, 2021], dtype="int64"),
            "DaysProduced": pd.array([31, 28, 31, 31, 28, 31], dtype=pd.Int64Dtype()),
            "AcceptedDate": pd.to_datetime(
                [
                    "2021-02-01",
                    "2021-03-01",
                    "2021-04-01",
                    "2021-02-01",
                    "2021-03-01",
                    "2021-04-01",
                ]
            ),
            "Revised": pd.array(
                [False, False, False, False, False, False], dtype=pd.BooleanDtype()
            ),
            "OpName": pd.array(["CHEVRON USA"] * 3 + ["BP AMERICA"] * 3, dtype=pd.StringDtype()),  # type: ignore[arg-type]
            "OpNumber": pd.array([100, 100, 100, 200, 200, 200], dtype=pd.Int64Dtype()),
            "FacilityId": pd.array([10, 10, 10, 20, 20, 20], dtype=pd.Int64Dtype()),
            "ApiCountyCode": pd.array(["05"] * 6, dtype=pd.StringDtype()),
            "ApiSequenceNumber": pd.array(["00123"] * 3 + ["00456"] * 3, dtype=pd.StringDtype()),  # type: ignore[arg-type]
            "ApiSidetrack": pd.array(["00"] * 6, dtype=pd.StringDtype()),
            "Well": pd.array(["WELL A"] * 3 + ["WELL B"] * 3, dtype=pd.StringDtype()),  # type: ignore[arg-type]
            "WellStatus": pd.array(["PR"] * 6, dtype=pd.StringDtype()),
            "FormationCode": pd.array(["NIO"] * 3 + ["COD"] * 3, dtype=pd.StringDtype()),  # type: ignore[arg-type]
            "OilProduced": pd.array(
                [100.0, 200.0, 150.0, 80.0, 90.0, 70.0], dtype=pd.Float64Dtype()
            ),
            "OilSales": pd.array([95.0] * 6, dtype=pd.Float64Dtype()),
            "OilAdjustment": pd.array([0.0] * 6, dtype=pd.Float64Dtype()),
            "OilGravity": pd.array([35.0] * 6, dtype=pd.Float64Dtype()),
            "GasProduced": pd.array(
                [500.0, 1000.0, 750.0, 400.0, 450.0, 350.0], dtype=pd.Float64Dtype()
            ),
            "GasSales": pd.array([480.0] * 6, dtype=pd.Float64Dtype()),
            "GasBtuSales": pd.array([480000.0] * 6, dtype=pd.Float64Dtype()),
            "GasUsedOnLease": pd.array([20.0] * 6, dtype=pd.Float64Dtype()),
            "GasShrinkage": pd.array([0.0] * 6, dtype=pd.Float64Dtype()),
            "GasPressureTubing": pd.array([100.0] * 6, dtype=pd.Float64Dtype()),
            "GasPressureCasing": pd.array([95.0] * 6, dtype=pd.Float64Dtype()),
            "WaterProduced": pd.array(
                [50.0, 80.0, 60.0, 30.0, 40.0, 35.0], dtype=pd.Float64Dtype()
            ),
            "WaterPressureTubing": pd.array([50.0] * 6, dtype=pd.Float64Dtype()),
            "WaterPressureCasing": pd.array([45.0] * 6, dtype=pd.Float64Dtype()),
            "FlaredVented": pd.array([0.0] * 6, dtype=pd.Float64Dtype()),
            "BomInvent": pd.array([0.0] * 6, dtype=pd.Float64Dtype()),
            "EomInvent": pd.array([0.0] * 6, dtype=pd.Float64Dtype()),
            "production_date": pd.to_datetime(
                [
                    "2021-01-01",
                    "2021-02-01",
                    "2021-03-01",
                    "2021-01-01",
                    "2021-02-01",
                    "2021-03-01",
                ]
            ),
            "well_id": pd.array(["05-00123-00"] * 3 + ["05-00456-00"] * 3, dtype=pd.StringDtype()),  # type: ignore[arg-type]
        }
    )
    return df


@pytest.fixture()
def make_zip_with_csv(tmp_path: Path):
    """Factory fixture to create a ZIP file containing a CSV in tmp_path."""

    def _factory(csv_content: str, csv_name: str = "data.csv") -> Path:
        zip_path = tmp_path / "test.zip"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(csv_name, csv_content)
        zip_path.write_bytes(buf.getvalue())
        return zip_path

    return _factory
