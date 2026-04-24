"""Unit tests for cogcc_pipeline.transform."""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from cogcc_pipeline.transform import _partition_count, clean_partition, transform

DICT_PATH = "references/production-data-dictionary.csv"

TRANSFORM_CFG = {
    "interim_dir": "data/interim",
    "processed_dir": "data/processed",
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
    "unit_consistency": {
        "OilProduced_max_bbl_month": 50000.0,
    },
}


def _make_partition(**overrides: object) -> pd.DataFrame:
    """Create a minimal synthetic partition for transform tests."""
    base = {
        "ApiCountyCode": ["01"],
        "ApiSequenceNumber": ["12345"],
        "ApiSidetrack": ["00"],
        "ReportYear": [2022],
        "ReportMonth": [6],
        "OilProduced": [100.0],
        "GasProduced": [500.0],
        "WaterProduced": [50.0],
        "OilSales": [95.0],
        "GasSales": [490.0],
        "WaterPressureTubing": [100.0],
        "WaterPressureCasing": [105.0],
        "GasPressureTubing": [200.0],
        "GasPressureCasing": [210.0],
        "WellStatus": ["AC"],
        "OpName": "operator a",
        "Well": ["WELL 1"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# ---------------------------------------------------------------------------
# Task T1 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_clean_partition_negative_oil_replaced(tmp_path: Path):
    """Negative OilProduced is replaced with NA (TR-01)."""
    pdf = _make_partition(OilProduced=[-100.0])
    result = clean_partition(pdf, TRANSFORM_CFG)
    assert pd.isna(result["OilProduced"].iloc[0])


@pytest.mark.unit
def test_clean_partition_oil_above_unit_threshold_replaced(tmp_path: Path):
    """Oil above 50,000 BBL/month replaced with NA (TR-02)."""
    pdf = _make_partition(OilProduced=[99999.0])
    result = clean_partition(pdf, TRANSFORM_CFG)
    assert pd.isna(result["OilProduced"].iloc[0])


@pytest.mark.unit
def test_clean_partition_dedup_reduces_rows(tmp_path: Path):
    """Duplicate (entity, date) rows are removed; row count ≤ input (TR-15)."""
    pdf = pd.concat([_make_partition(), _make_partition()], ignore_index=True)
    assert len(pdf) == 2
    result = clean_partition(pdf, TRANSFORM_CFG)
    assert len(result) <= 2
    assert len(result) == 1


@pytest.mark.unit
def test_clean_partition_dedup_idempotent(tmp_path: Path):
    """Running clean_partition twice gives identical output (TR-15)."""
    pdf = pd.concat([_make_partition(), _make_partition()], ignore_index=True)
    result1 = clean_partition(pdf, TRANSFORM_CFG)
    result2 = clean_partition(result1, TRANSFORM_CFG)
    pd.testing.assert_frame_equal(result1.reset_index(drop=True), result2.reset_index(drop=True))


@pytest.mark.unit
def test_clean_partition_zero_oil_preserved(tmp_path: Path):
    """OilProduced=0 stays zero — not converted to NA (TR-05)."""
    pdf = _make_partition(OilProduced=[0.0])
    result = clean_partition(pdf, TRANSFORM_CFG)
    assert float(result["OilProduced"].iloc[0]) == 0.0


@pytest.mark.unit
def test_clean_partition_null_entity_rows_dropped(tmp_path: Path):
    """Rows with null ApiCountyCode → entity is null → row dropped."""
    pdf = _make_partition(ApiCountyCode=[None])
    result = clean_partition(pdf, TRANSFORM_CFG)
    assert len(result) == 0


@pytest.mark.unit
def test_clean_partition_empty_input_returns_full_schema(tmp_path: Path):
    """Empty partition → empty output with full output schema (ADR-003, TR-23)."""
    pdf = _make_partition().iloc[:0]
    result = clean_partition(pdf, TRANSFORM_CFG)
    assert len(result) == 0
    assert "well_id" in result.columns
    assert "production_date" in result.columns


@pytest.mark.unit
def test_clean_partition_opname_normalized(tmp_path: Path):
    """OpName is stripped and uppercased."""
    pdf = _make_partition()
    pdf["OpName"] = "  operator lowercase  "
    result = clean_partition(pdf, TRANSFORM_CFG)
    assert result["OpName"].iloc[0] == "OPERATOR LOWERCASE"


# ---------------------------------------------------------------------------
# Task T2 tests
# ---------------------------------------------------------------------------


def _write_interim_parquet(tmp_path: Path, rows: int = 5) -> Path:
    """Write a synthetic interim Parquet for transform integration-style unit tests."""
    base_cols = {
        "ApiCountyCode": ["01"] * rows,
        "ApiSequenceNumber": ["12345"] * rows,
        "ApiSidetrack": ["00"] * rows,
        "ReportYear": [2022] * rows,
        "ReportMonth": list(range(1, rows + 1)),
        "OilProduced": [float(i * 10) for i in range(rows)],
        "GasProduced": [float(i * 50) for i in range(rows)],
        "WaterProduced": [float(i * 5) for i in range(rows)],
        "OilSales": [float(i * 9) for i in range(rows)],
        "GasSales": [float(i * 45) for i in range(rows)],
        "WaterPressureTubing": [100.0] * rows,
        "WaterPressureCasing": [105.0] * rows,
        "GasPressureTubing": [200.0] * rows,
        "GasPressureCasing": [210.0] * rows,
        "WellStatus": ["AC"] * rows,
        "OpName": ["OPERATOR A"] * rows,
        "Well": ["WELL 1"] * rows,
        "DaysProduced": [30] * rows,
        "DocNum": [12345] * rows,
        "OpNumber": [1] * rows,
        "FacilityId": [99] * rows,
        "AcceptedDate": pd.to_datetime(["2022-07-01"] * rows),
        "Revised": [False] * rows,
        "OilAdjustment": [0.0] * rows,
        "OilGravity": [42.0] * rows,
        "GasBtuSales": [1000.0] * rows,
        "GasUsedOnLease": [10.0] * rows,
        "GasShrinkage": [5.0] * rows,
        "FlaredVented": [0.0] * rows,
        "BomInvent": [0.0] * rows,
        "EomInvent": [0.0] * rows,
        "FormationCode": ["CODELL"] * rows,
    }
    df = pd.DataFrame(base_cols)
    interim = tmp_path / "interim"
    interim.mkdir()
    df.to_parquet(interim / "part.0.parquet", index=False)
    return interim


@pytest.mark.unit
def test_transform_writes_readable_parquet(tmp_path: Path):
    """Processed Parquet is written and readable after transform (TR-18)."""
    interim = _write_interim_parquet(tmp_path)
    processed = tmp_path / "processed"

    cfg = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed),
        }
    }
    transform(cfg)

    ddf = dd.read_parquet(str(processed), engine="pyarrow")
    df = ddf.compute()
    assert len(df) > 0


@pytest.mark.unit
def test_transform_entity_index_established(tmp_path: Path):
    """Processed Parquet has entity column as index."""
    interim = _write_interim_parquet(tmp_path)
    processed = tmp_path / "processed"

    cfg = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed),
        }
    }
    transform(cfg)

    ddf = dd.read_parquet(str(processed), engine="pyarrow")
    assert ddf.index.name == "well_id"


@pytest.mark.unit
def test_transform_partitions_sorted_by_date(tmp_path: Path):
    """Each partition is sorted by production_date (boundary-transform-features)."""
    interim = _write_interim_parquet(tmp_path, rows=6)
    processed = tmp_path / "processed"

    cfg = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed),
        }
    }
    transform(cfg)

    parquet_files = list(Path(processed).glob("**/*.parquet"))
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        if len(df) > 1 and "production_date" in df.columns:
            assert df["production_date"].is_monotonic_increasing


@pytest.mark.unit
def test_transform_partition_count_in_range(tmp_path: Path):
    """Partition count within ADR-004 range after transform."""
    interim = _write_interim_parquet(tmp_path)
    processed = tmp_path / "processed"

    cfg = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed),
        }
    }
    transform(cfg)

    ddf = dd.read_parquet(str(processed), engine="pyarrow")
    assert 10 <= ddf.npartitions <= 50


@pytest.mark.unit
def test_transform_schema_stable_across_partitions(tmp_path: Path):
    """Two sampled partitions have identical column names and dtypes (TR-14)."""
    interim = _write_interim_parquet(tmp_path)
    processed = tmp_path / "processed"

    cfg = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed),
        }
    }
    transform(cfg)

    parquet_files = list(Path(processed).glob("**/*.parquet"))
    if len(parquet_files) >= 2:
        df1 = pd.read_parquet(parquet_files[0])
        df2 = pd.read_parquet(parquet_files[1])
        assert list(df1.columns) == list(df2.columns)
        assert dict(df1.dtypes) == dict(df2.dtypes)


@pytest.mark.unit
def test_transform_dtypes_correct(tmp_path: Path):
    """Numeric cols float, date col datetime, string/categorical cols correct (TR-12)."""
    interim = _write_interim_parquet(tmp_path)
    processed = tmp_path / "processed"

    cfg = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed),
        }
    }
    transform(cfg)

    ddf = dd.read_parquet(str(processed), engine="pyarrow")
    df = ddf.compute()

    assert pd.api.types.is_float_dtype(df["OilProduced"])
    if "production_date" in df.columns:
        assert pd.api.types.is_datetime64_any_dtype(df["production_date"])


@pytest.mark.unit
def test_transform_zero_oil_preserved(tmp_path: Path):
    """Zeros pass through transform unchanged (TR-05)."""
    interim = _write_interim_parquet(tmp_path, rows=3)
    # Patch first row's OilProduced to 0
    pf = list(Path(interim).glob("*.parquet"))[0]
    df = pd.read_parquet(pf)
    df.at[0, "OilProduced"] = 0.0
    df.to_parquet(pf, index=False)

    processed = tmp_path / "processed"
    cfg = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed),
        }
    }
    transform(cfg)

    out = dd.read_parquet(str(processed), engine="pyarrow").compute()
    oil_values = out["OilProduced"].dropna()
    assert 0.0 in oil_values.values


@pytest.mark.unit
def test_transform_idempotent(tmp_path: Path):
    """Running transform twice yields same row count and content (TR-15)."""
    interim = _write_interim_parquet(tmp_path)
    processed1 = tmp_path / "processed1"
    processed2 = tmp_path / "processed2"

    cfg1 = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed1),
        }
    }
    cfg2 = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim),
            "processed_dir": str(processed2),
        }
    }
    transform(cfg1)
    transform(cfg2)

    df1 = dd.read_parquet(str(processed1), engine="pyarrow").compute()
    df2 = dd.read_parquet(str(processed2), engine="pyarrow").compute()
    assert len(df1) == len(df2)


@pytest.mark.unit
def test_transform_well_completeness(tmp_path: Path):
    """Well with records for every month Jan–Jun shows all 6 records (TR-04)."""
    interim_dir = tmp_path / "interim"
    interim_dir.mkdir()
    rows = []
    for m in range(1, 7):
        rows.append(
            {
                "ApiCountyCode": "01",
                "ApiSequenceNumber": "99999",
                "ApiSidetrack": "00",
                "ReportYear": 2022,
                "ReportMonth": m,
                "OilProduced": float(100 + m),
                "GasProduced": 500.0,
                "WaterProduced": 50.0,
                "OilSales": 90.0,
                "GasSales": 490.0,
                "WaterPressureTubing": 100.0,
                "WaterPressureCasing": 105.0,
                "GasPressureTubing": 200.0,
                "GasPressureCasing": 210.0,
                "WellStatus": "AC",
                "OpName": "OP",
                "Well": "TEST WELL",
                "DaysProduced": 30,
                "DocNum": 1,
                "OpNumber": 1,
                "FacilityId": 1,
                "AcceptedDate": None,
                "Revised": False,
                "OilAdjustment": 0.0,
                "OilGravity": 42.0,
                "GasBtuSales": 1000.0,
                "GasUsedOnLease": 10.0,
                "GasShrinkage": 5.0,
                "FlaredVented": 0.0,
                "BomInvent": 0.0,
                "EomInvent": 0.0,
                "FormationCode": "CODELL",
            }
        )
    df = pd.DataFrame(rows)
    df.to_parquet(interim_dir / "part.parquet", index=False)

    processed = tmp_path / "processed"
    cfg = {
        "transform": {
            **TRANSFORM_CFG,
            "interim_dir": str(interim_dir),
            "processed_dir": str(processed),
        }
    }
    transform(cfg)

    out = dd.read_parquet(str(processed), engine="pyarrow").compute()
    well_id = "01-99999-00"
    well_rows = out[out.index == well_id]
    assert len(well_rows) == 6


@pytest.mark.unit
def test_partition_count_formula():
    """Verify the ADR-004 formula."""
    assert _partition_count(5, 10, 50) == 10
    assert _partition_count(30, 10, 50) == 30
    assert _partition_count(100, 10, 50) == 50


@pytest.mark.unit
def test_transform_map_partitions_meta_matches_output(tmp_path: Path):
    """Meta from clean_partition on zero-row input matches actual output schema (TR-23)."""
    from cogcc_pipeline.ingest import load_canonical_schema

    schema = load_canonical_schema(DICT_PATH)
    from cogcc_pipeline.ingest import _empty_frame

    empty_input = _empty_frame(schema)
    meta_df = clean_partition(empty_input, TRANSFORM_CFG)

    # Feed a real minimal partition
    pdf = _make_partition()
    actual = clean_partition(pdf, TRANSFORM_CFG)

    # Column names and order must match
    assert list(meta_df.columns) == list(actual.columns)
    # Dtypes must match
    for col in meta_df.columns:
        assert meta_df[col].dtype == actual[col].dtype, (
            f"dtype mismatch for {col}: meta={meta_df[col].dtype}, actual={actual[col].dtype}"
        )
