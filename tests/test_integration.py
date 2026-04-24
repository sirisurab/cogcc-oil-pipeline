"""Integration tests — marked @pytest.mark.integration.

All outputs write to pytest's tmp_path — never to the project's data/ directories.
ADR-008: integration tests accept a config dict with output paths overridden to tmp_path.
These tests require no network access; they use synthetic CSV files as input.
"""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from cogcc_pipeline.features import features
from cogcc_pipeline.ingest import ingest, load_canonical_schema
from cogcc_pipeline.transform import transform

DICT_PATH = "references/production-data-dictionary.csv"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_synthetic_csv(path: Path, year: int = 2022, n_rows: int = 20) -> None:
    """Write a synthetic production CSV with realistic data."""
    rows = []
    for i in range(n_rows):
        rows.append(
            {
                "DocNum": str(1000 + i),
                "ReportMonth": str((i % 12) + 1),
                "ReportYear": str(year),
                "DaysProduced": "30",
                "AcceptedDate": f"{year}-07-01",
                "Revised": "false",
                "OpName": "TEST OPERATOR",
                "OpNumber": "99",
                "FacilityId": str(100 + i),
                "ApiCountyCode": "01",
                "ApiSequenceNumber": str(10000 + i % 5),  # 5 unique wells
                "ApiSidetrack": "00",
                "Well": f"WELL {i % 5}",
                "WellStatus": "AC",
                "FormationCode": "CODELL",
                "OilProduced": str(float(100 + i)),
                "OilSales": str(float(95 + i)),
                "OilAdjustment": "0.0",
                "OilGravity": "42.0",
                "GasProduced": str(float(500 + i * 5)),
                "GasSales": str(float(490 + i * 5)),
                "GasBtuSales": "1000.0",
                "GasUsedOnLease": "10.0",
                "GasShrinkage": "5.0",
                "GasPressureTubing": "200.0",
                "GasPressureCasing": "210.0",
                "WaterProduced": str(float(50 + i)),
                "WaterPressureTubing": "100.0",
                "WaterPressureCasing": "105.0",
                "FlaredVented": "0.0",
                "BomInvent": "0.0",
                "EomInvent": "0.0",
            }
        )
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)


INGEST_CFG_TEMPLATE = {
    "raw_dir": "",
    "interim_dir": "",
    "dictionary_path": DICT_PATH,
    "partition_min": 10,
    "partition_max": 50,
    "min_report_year": 2020,
}

TRANSFORM_CFG_TEMPLATE = {
    "interim_dir": "",
    "processed_dir": "",
    "entity_col": "well_id",
    "date_col": "production_date",
    "api_county_col": "ApiCountyCode",
    "api_sequence_col": "ApiSequenceNumber",
    "api_sidetrack_col": "ApiSidetrack",
    "min_report_year": 2020,
    "partition_min": 10,
    "partition_max": 50,
    "physical_bounds": {
        "OilProduced": {"min": 0.0, "max": None},
        "GasProduced": {"min": 0.0, "max": None},
        "WaterProduced": {"min": 0.0, "max": None},
        "OilSales": {"min": 0.0, "max": None},
        "GasSales": {"min": 0.0, "max": None},
        "WaterPressureTubing": {"min": 0.0, "max": None},
        "WaterPressureCasing": {"min": 0.0, "max": None},
        "GasPressureTubing": {"min": 0.0, "max": None},
        "GasPressureCasing": {"min": 0.0, "max": None},
    },
    "unit_consistency": {"OilProduced_max_bbl_month": 50000.0},
}

FEATURES_CFG_TEMPLATE = {
    "processed_dir": "",
    "features_dir": "",
    "rolling_windows": [3, 6, 12],
    "lag_sizes": [1],
    "decline_rate_lower_bound": -1.0,
    "decline_rate_upper_bound": 10.0,
    "partition_min": 10,
    "partition_max": 50,
}


# ---------------------------------------------------------------------------
# TR-24: Ingest stage integration test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_ingest_integration(tmp_path: Path):
    """TR-24: run ingest on synthetic raw files; assert interim Parquet satisfies
    boundary-ingest-transform guarantees."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"

    # Write 2 synthetic raw CSVs
    _make_synthetic_csv(raw_dir / "file1.csv", year=2022, n_rows=20)
    _make_synthetic_csv(raw_dir / "file2.csv", year=2023, n_rows=15)

    cfg = {
        "ingest": {
            **INGEST_CFG_TEMPLATE,
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
        }
    }
    ingest(cfg)

    # (a) Interim Parquet is readable
    ddf = dd.read_parquet(str(interim_dir), engine="pyarrow")
    df = ddf.compute()
    assert len(df) > 0

    # (b) All columns carry data-dictionary dtypes
    schema = load_canonical_schema(DICT_PATH)
    for col, meta in schema.items():
        if col in df.columns:
            expected_token = meta["token"]
            actual_dtype = df[col].dtype
            if expected_token == "float":
                assert pd.api.types.is_float_dtype(actual_dtype), (
                    f"{col}: expected float, got {actual_dtype}"
                )

    # (c) Upstream guarantees from boundary-ingest-transform:
    # - Canonical column names present
    for col in ["ApiCountyCode", "ApiSequenceNumber", "ReportYear", "OilProduced"]:
        assert col in df.columns

    # - Partition count in contract range
    assert 10 <= ddf.npartitions <= 50

    # (d) All schema-required columns present (TR-22)
    for col in [
        "ApiCountyCode",
        "ApiSequenceNumber",
        "ReportYear",
        "ReportMonth",
        "OilProduced",
        "OilSales",
        "GasProduced",
        "GasSales",
        "FlaredVented",
        "GasUsedOnLease",
        "WaterProduced",
        "DaysProduced",
        "Well",
        "OpName",
        "DocNum",
        "OpNumber",
    ]:
        assert col in df.columns, f"TR-22 column missing: {col}"


# ---------------------------------------------------------------------------
# TR-25: Transform stage integration test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_transform_integration(tmp_path: Path):
    """TR-25: run transform on ingest output; assert processed Parquet satisfies
    boundary-transform-features guarantees."""
    # First run ingest to produce interim
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"

    _make_synthetic_csv(raw_dir / "file1.csv", year=2022, n_rows=20)
    _make_synthetic_csv(raw_dir / "file2.csv", year=2023, n_rows=15)

    ingest(
        {
            "ingest": {
                **INGEST_CFG_TEMPLATE,
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
            }
        }
    )

    transform(
        {
            "transform": {
                **TRANSFORM_CFG_TEMPLATE,
                "interim_dir": str(interim_dir),
                "processed_dir": str(processed_dir),
            }
        }
    )

    # (a) Processed Parquet is readable
    ddf = dd.read_parquet(str(processed_dir), engine="pyarrow")
    df = ddf.compute()
    assert len(df) > 0

    # (b) boundary-transform-features guarantees:
    # - Entity-indexed
    assert ddf.index.name == "well_id"

    # - Sorted by production_date within each partition
    for pf in list(Path(processed_dir).glob("**/*.parquet"))[:3]:
        part = pd.read_parquet(pf)
        if len(part) > 1 and "production_date" in part.columns:
            assert part["production_date"].is_monotonic_increasing

    # - Partition count in contract range
    assert 10 <= ddf.npartitions <= 50

    # - WellStatus carries only declared values
    if "WellStatus" in df.columns:
        valid_values = {"AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"}
        non_null = df["WellStatus"].dropna()
        assert set(non_null.astype(str)).issubset(valid_values)


# ---------------------------------------------------------------------------
# TR-26: Features stage integration test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_features_integration(tmp_path: Path):
    """TR-26: run features on transform output; assert features Parquet is correct."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    features_dir = tmp_path / "features"

    _make_synthetic_csv(raw_dir / "file1.csv", year=2022, n_rows=20)
    _make_synthetic_csv(raw_dir / "file2.csv", year=2023, n_rows=15)

    ingest(
        {
            "ingest": {
                **INGEST_CFG_TEMPLATE,
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
            }
        }
    )

    transform(
        {
            "transform": {
                **TRANSFORM_CFG_TEMPLATE,
                "interim_dir": str(interim_dir),
                "processed_dir": str(processed_dir),
            }
        }
    )

    features(
        {
            "features": {
                **FEATURES_CFG_TEMPLATE,
                "processed_dir": str(processed_dir),
                "features_dir": str(features_dir),
            }
        }
    )

    # (a) Features Parquet is readable
    ddf = dd.read_parquet(str(features_dir), engine="pyarrow")
    df = ddf.compute()
    assert len(df) > 0

    # (b) All derived feature columns are present
    required = [
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "OilProduced_rolling_3m",
        "OilProduced_lag_1",
        "is_peak_oil_month",
    ]
    for col in required:
        assert col in df.columns, f"Missing derived feature: {col}"

    # (c) Schema consistent across sampled partitions
    parquet_files = list(Path(features_dir).glob("**/*.parquet"))
    if len(parquet_files) >= 2:
        df1 = pd.read_parquet(parquet_files[0])
        df2 = pd.read_parquet(parquet_files[1])
        assert list(df1.columns) == list(df2.columns)


# ---------------------------------------------------------------------------
# TR-27: End-to-end pipeline integration test
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_end_to_end_pipeline(tmp_path: Path):
    """TR-27: full pipeline ingest→transform→features on synthetic data.

    Asserts all inter-stage boundary guarantees and no unhandled exceptions.
    All output paths use tmp_path (ADR-008).
    """
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    features_dir = tmp_path / "features"

    _make_synthetic_csv(raw_dir / "file1.csv", year=2022, n_rows=25)
    _make_synthetic_csv(raw_dir / "file2.csv", year=2023, n_rows=25)
    _make_synthetic_csv(raw_dir / "file3.csv", year=2021, n_rows=25)

    # --- ingest ---
    ingest(
        {
            "ingest": {
                **INGEST_CFG_TEMPLATE,
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
            }
        }
    )

    # (a) boundary-ingest-transform
    ddf_ingest = dd.read_parquet(str(interim_dir), engine="pyarrow")
    df_ingest = ddf_ingest.compute()
    assert len(df_ingest) > 0
    assert 10 <= ddf_ingest.npartitions <= 50
    for col in ["ApiCountyCode", "ApiSequenceNumber", "OilProduced", "ReportYear"]:
        assert col in df_ingest.columns

    # --- transform ---
    transform(
        {
            "transform": {
                **TRANSFORM_CFG_TEMPLATE,
                "interim_dir": str(interim_dir),
                "processed_dir": str(processed_dir),
            }
        }
    )

    # (b) boundary-transform-features
    ddf_transform = dd.read_parquet(str(processed_dir), engine="pyarrow")
    df_transform = ddf_transform.compute()
    assert len(df_transform) > 0
    assert ddf_transform.index.name == "well_id"
    assert 10 <= ddf_transform.npartitions <= 50

    # --- features ---
    features(
        {
            "features": {
                **FEATURES_CFG_TEMPLATE,
                "processed_dir": str(processed_dir),
                "features_dir": str(features_dir),
            }
        }
    )

    # (c) TR-26 features guarantees
    ddf_features = dd.read_parquet(str(features_dir), engine="pyarrow")
    df_features = ddf_features.compute()
    assert len(df_features) > 0
    for col in ["cum_oil", "cum_gas", "gor", "water_cut", "decline_rate"]:
        assert col in df_features.columns

    # (d) No unhandled exceptions (if we got here, we're good)
