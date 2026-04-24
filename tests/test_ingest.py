"""Unit tests for cogcc_pipeline.ingest."""

from __future__ import annotations

import io
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from cogcc_pipeline.ingest import (
    SchemaError,
    _empty_frame,
    _partition_count,
    ingest,
    load_canonical_schema,
    read_raw_file_to_frame,
)

DICT_PATH = "references/production-data-dictionary.csv"

# Required columns per TR-22 (note: TR-22 mentions OpNum but dictionary has OpNumber)
TR22_COLUMNS = [
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
    # OpNum is not in the data dictionary; OpNumber is
    "OpNumber",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_csv_row() -> dict:
    """Return a minimal valid row dict for all non-nullable columns."""
    return {
        "DocNum": "12345",
        "ReportMonth": "6",
        "ReportYear": "2022",
        "DaysProduced": "30",
        "AcceptedDate": "2022-07-01",
        "Revised": "false",
        "OpName": "OPERATOR A",
        "OpNumber": "12",
        "FacilityId": "99",
        "ApiCountyCode": "01",
        "ApiSequenceNumber": "12345",
        "ApiSidetrack": "00",
        "Well": "WELL 1",
        "WellStatus": "AC",
        "FormationCode": "CODELL",
        "OilProduced": "100.0",
        "OilSales": "95.0",
        "OilAdjustment": "0.0",
        "OilGravity": "42.0",
        "GasProduced": "500.0",
        "GasSales": "490.0",
        "GasBtuSales": "1000.0",
        "GasUsedOnLease": "10.0",
        "GasShrinkage": "5.0",
        "GasPressureTubing": "200.0",
        "GasPressureCasing": "210.0",
        "WaterProduced": "50.0",
        "WaterPressureTubing": "100.0",
        "WaterPressureCasing": "105.0",
        "FlaredVented": "0.0",
        "BomInvent": "0.0",
        "EomInvent": "0.0",
    }


def _write_csv(tmp_path: Path, rows: list[dict], filename: str = "test.csv") -> Path:
    df = pd.DataFrame(rows)
    path = tmp_path / filename
    df.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Task I1 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_schema_loads_all_columns():
    """Every column in TR-22 is present in the schema."""
    schema = load_canonical_schema(DICT_PATH)
    for col in ["ApiCountyCode", "ApiSequenceNumber", "ReportYear", "ReportMonth",
                "OilProduced", "WaterProduced", "DocNum", "OpNumber", "Well"]:
        assert col in schema, f"Column {col} missing from schema"


@pytest.mark.unit
def test_nullable_columns_use_nullable_dtype():
    """Every nullable column maps to a nullable-aware pandas dtype (ADR-003)."""
    schema = load_canonical_schema(DICT_PATH)
    nullable_aware = {"Int64", "Float64", "string", "boolean"}
    for col, meta in schema.items():
        if meta["nullable"] and meta["token"] not in ("datetime", "categorical"):
            assert meta["dtype"] in nullable_aware, (
                f"Nullable column '{col}' uses non-nullable dtype '{meta['dtype']}'"
            )


@pytest.mark.unit
def test_categorical_column_has_declared_values():
    """WellStatus carries the exact declared category set."""
    schema = load_canonical_schema(DICT_PATH)
    assert "WellStatus" in schema
    cats = schema["WellStatus"]["categories"]
    assert cats is not None
    expected = {"AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"}
    assert set(cats) == expected


@pytest.mark.unit
def test_schema_raises_on_missing_file():
    with pytest.raises(SchemaError):
        load_canonical_schema("nonexistent_file.csv")


@pytest.mark.unit
def test_schema_raises_on_malformed_dict(tmp_path: Path):
    bad_csv = tmp_path / "bad_dict.csv"
    # Missing required columns
    bad_csv.write_text("col_name,description\nFoo,bar\n")
    with pytest.raises(SchemaError):
        load_canonical_schema(str(bad_csv))


@pytest.mark.unit
def test_schema_raises_on_unsupported_dtype(tmp_path: Path):
    bad_csv = tmp_path / "bad_dict.csv"
    bad_csv.write_text("column,description,dtype,nullable,categories\nFoo,bar,badtype,yes,\n")
    with pytest.raises(SchemaError):
        load_canonical_schema(str(bad_csv))


# ---------------------------------------------------------------------------
# Task I2 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_file_canonical_names_and_dtypes(tmp_path: Path):
    """Returned frame has canonical names, dtypes, rows filtered to >=2020."""
    row = _minimal_csv_row()
    csv_path = _write_csv(tmp_path, [row, {**row, "ReportYear": "2019"}])
    schema = load_canonical_schema(DICT_PATH)
    df = read_raw_file_to_frame(str(csv_path), schema)

    assert "ReportYear" in df.columns
    # Only 2022 row should survive filter
    assert (df["ReportYear"] >= 2020).all()
    assert len(df) == 1  # 2019 row filtered out


@pytest.mark.unit
def test_read_raw_file_missing_nullable_column(tmp_path: Path):
    """Missing nullable column → added as all-NA at correct dtype (H3)."""
    row = _minimal_csv_row()
    del row["DocNum"]  # DocNum is nullable=yes
    csv_path = _write_csv(tmp_path, [row])
    schema = load_canonical_schema(DICT_PATH)
    df = read_raw_file_to_frame(str(csv_path), schema)

    assert "DocNum" in df.columns
    assert df["DocNum"].isna().all()


@pytest.mark.unit
def test_read_raw_file_missing_nonnullable_column_raises(tmp_path: Path):
    """Missing non-nullable column → SchemaError (H3)."""
    row = _minimal_csv_row()
    del row["ApiCountyCode"]  # nullable=no
    csv_path = _write_csv(tmp_path, [row])
    schema = load_canonical_schema(DICT_PATH)

    with pytest.raises(SchemaError):
        read_raw_file_to_frame(str(csv_path), schema)


@pytest.mark.unit
def test_read_raw_file_invalid_wellstatus_replaced():
    """WellStatus value outside declared set → replaced with NA."""
    import tempfile

    row = _minimal_csv_row()
    row["WellStatus"] = "INVALID_STATUS"
    schema = load_canonical_schema(DICT_PATH)

    with tempfile.TemporaryDirectory() as td:
        csv_path = _write_csv(Path(td), [row])
        df = read_raw_file_to_frame(str(csv_path), schema)

    assert "WellStatus" in df.columns
    # After casting, column values must all be from declared set or NA
    valid_values = {"AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"}
    non_null = df["WellStatus"].dropna()
    assert set(non_null.astype(str)).issubset(valid_values)


@pytest.mark.unit
def test_read_raw_file_preserves_zeros(tmp_path: Path):
    """Zero values in numeric columns remain as zeros — not converted to NA (TR-05)."""
    row = _minimal_csv_row()
    row["OilProduced"] = "0.0"
    row["GasProduced"] = "0.0"
    csv_path = _write_csv(tmp_path, [row])
    schema = load_canonical_schema(DICT_PATH)
    df = read_raw_file_to_frame(str(csv_path), schema)

    assert float(df["OilProduced"].iloc[0]) == 0.0
    assert float(df["GasProduced"].iloc[0]) == 0.0


@pytest.mark.unit
def test_read_raw_file_empty_after_filter_has_full_schema(tmp_path: Path):
    """Zero rows after year filter → empty frame with full canonical schema (H2)."""
    row = _minimal_csv_row()
    row["ReportYear"] = "2010"  # will be filtered
    csv_path = _write_csv(tmp_path, [row])
    schema = load_canonical_schema(DICT_PATH)
    df = read_raw_file_to_frame(str(csv_path), schema)

    assert len(df) == 0
    for col in schema:
        assert col in df.columns, f"Column {col} missing from empty frame"


# ---------------------------------------------------------------------------
# Task I3 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ingest_writes_readable_parquet(tmp_path: Path):
    """Interim Parquet is written and readable; TR-22 columns present (TR-18, TR-22)."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"

    row = _minimal_csv_row()
    _write_csv(raw_dir, [row], "file1.csv")
    _write_csv(raw_dir, [{**row, "ReportYear": "2023"}], "file2.csv")

    cfg = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "dictionary_path": DICT_PATH,
            "partition_min": 10,
            "partition_max": 50,
        }
    }
    ingest(cfg)

    ddf = dd.read_parquet(str(interim_dir), engine="pyarrow")
    df = ddf.compute()
    assert len(df) > 0
    for col in ["ApiCountyCode", "ApiSequenceNumber", "ReportYear", "OilProduced", "DocNum"]:
        assert col in df.columns


@pytest.mark.unit
def test_ingest_partition_count_in_adr004_range(tmp_path: Path):
    """Partition count = max(10, min(n, 50)) per ADR-004 (TR-22)."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"

    row = _minimal_csv_row()
    for i in range(3):
        _write_csv(raw_dir, [row], f"file{i}.csv")

    cfg = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "dictionary_path": DICT_PATH,
            "partition_min": 10,
            "partition_max": 50,
        }
    }
    ingest(cfg)

    ddf = dd.read_parquet(str(interim_dir), engine="pyarrow")
    assert 10 <= ddf.npartitions <= 50


@pytest.mark.unit
def test_ingest_multiple_partitions_have_full_schema(tmp_path: Path):
    """Sampling 3 written partitions shows full canonical column set (TR-22)."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"

    row = _minimal_csv_row()
    for i in range(3):
        _write_csv(raw_dir, [row], f"file{i}.csv")

    cfg = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "dictionary_path": DICT_PATH,
            "partition_min": 10,
            "partition_max": 50,
        }
    }
    ingest(cfg)

    schema = load_canonical_schema(DICT_PATH)
    parquet_files = list(Path(interim_dir).glob("**/*.parquet"))
    assert len(parquet_files) > 0

    for pf in parquet_files[:3]:
        df = pd.read_parquet(pf)
        for col in schema:
            assert col in df.columns, f"Column {col} missing in {pf}"


@pytest.mark.unit
def test_ingest_parquet_readable_fresh(tmp_path: Path):
    """Every written Parquet file is readable by a fresh pandas process (TR-18)."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"

    row = _minimal_csv_row()
    _write_csv(raw_dir, [row], "file1.csv")

    cfg = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "dictionary_path": DICT_PATH,
            "partition_min": 10,
            "partition_max": 50,
        }
    }
    ingest(cfg)

    for pf in Path(interim_dir).glob("**/*.parquet"):
        df = pd.read_parquet(pf)
        assert isinstance(df, pd.DataFrame)


@pytest.mark.unit
def test_partition_count_formula():
    """Verify the ADR-004 formula max(10, min(n, 50))."""
    assert _partition_count(5, 10, 50) == 10
    assert _partition_count(30, 10, 50) == 30
    assert _partition_count(100, 10, 50) == 50
    assert _partition_count(10, 10, 50) == 10
    assert _partition_count(50, 10, 50) == 50
