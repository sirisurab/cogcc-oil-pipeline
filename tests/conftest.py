"""Shared pytest fixtures for cogcc_pipeline tests."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Config fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def config_fixture(tmp_path: Path) -> dict:
    """Return a Config-like dict with temp directory paths."""
    raw_dir = tmp_path / "raw"
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    features_dir = tmp_path / "features"
    logs_dir = tmp_path / "logs"

    for d in (raw_dir, interim_dir, processed_dir, features_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    return {
        "acquire": {
            "start_year": 2020,
            "raw_dir": str(raw_dir),
            "max_workers": 2,
            "zip_url_template": "https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip",
            "monthly_url": "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv",
            "sleep_seconds": 0.0,
        },
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "start_year": 2020,
        },
        "transform": {
            "interim_dir": str(interim_dir),
            "processed_dir": str(processed_dir),
        },
        "features": {
            "processed_dir": str(processed_dir),
            "output_dir": str(features_dir),
            "rolling_windows": [3, 6],
            "lag_periods": [1, 3],
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "512MB",
            "dashboard_port": 8787,
        },
        "logging": {
            "log_file": str(logs_dir / "pipeline.log"),
            "level": "INFO",
        },
    }


# ---------------------------------------------------------------------------
# Raw / canonical column lists
# ---------------------------------------------------------------------------

CANONICAL_COLUMNS = [
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


def _make_raw_csv_content(n_rows: int = 5, start_year: int = 2021) -> str:
    """Generate a minimal valid COGCC CSV string."""
    header = ",".join(CANONICAL_COLUMNS)
    rows = []
    for i in range(n_rows):
        month = (i % 12) + 1
        rows.append(
            f"{1000 + i},{month},{start_year},28,2021-01-15,False,"
            f"Operator {i},{100 + i},{200 + i},"
            f"0{i % 9 + 1},1234{i:01d},00,Well {i},AC,NIOBRARA,"
            f"{100.0 + i},{90.0 + i},0.0,40.0,"
            f"{500.0 + i},{450.0 + i},1000.0,10.0,5.0,200.0,100.0,"
            f"{200.0 + i},50.0,30.0,"
            f"2.0,10.0,5.0"
        )
    return header + "\n" + "\n".join(rows) + "\n"


@pytest.fixture()
def sample_raw_csv(tmp_path: Path) -> Path:
    """Write a small canonical CSV and return its path."""
    csv_path = tmp_path / "2021_prod_reports.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=10), encoding="utf-8")
    return csv_path


@pytest.fixture()
def sample_raw_df() -> pd.DataFrame:
    """Return a DataFrame mimicking raw COGCC data."""
    import pandas as pd

    content = _make_raw_csv_content(n_rows=20)
    from cogcc_pipeline.ingest import DTYPE_MAP

    df = pd.read_csv(
        io.StringIO(content),
        dtype=cast(dict[str, Any], {k: v for k, v in DTYPE_MAP.items()}),  # type: ignore[arg-type]
        parse_dates=["AcceptedDate"],
    )
    return df


@pytest.fixture()
def sample_clean_df() -> pd.DataFrame:
    """Return a cleaned DataFrame with proper column names and well_id/production_date."""
    n = 24  # two wells × 12 months
    well_ids = ["05-01-12345-00"] * 12 + ["05-02-67890-00"] * 12
    dates = pd.date_range("2021-01-01", periods=12, freq="MS").tolist() * 2

    df = pd.DataFrame(
        {
            "well_id": pd.Series(well_ids, dtype="string").values,
            "production_date": pd.to_datetime(dates),
            "OilProduced": pd.array(
                [float(50 + i % 12) for i in range(n)], dtype=pd.Float64Dtype()
            ),
            "GasProduced": pd.array(
                [float(200 + i % 12) for i in range(n)], dtype=pd.Float64Dtype()
            ),
            "WaterProduced": pd.array(
                [float(100 + i % 12) for i in range(n)], dtype=pd.Float64Dtype()
            ),
            "DaysProduced": pd.array([28] * n, dtype=pd.Int64Dtype()),
            "WellStatus": pd.Categorical(
                ["AC"] * n,
                categories=["AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"],
            ),
        }
    )
    return df


@pytest.fixture()
def tmp_parquet(tmp_path: Path, sample_clean_df: pd.DataFrame) -> Path:
    """Write sample_clean_df to a temp parquet and return the path."""
    pq_path = tmp_path / "sample.parquet"
    sample_clean_df.to_parquet(pq_path, engine="pyarrow", index=False)
    return pq_path


def make_valid_zip_bytes(csv_content: str) -> bytes:
    """Create an in-memory ZIP containing a single CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("production.csv", csv_content)
    buf.seek(0)
    return buf.read()
