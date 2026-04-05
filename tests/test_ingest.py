"""Tests for cogcc_pipeline.ingest."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from cogcc_pipeline.ingest import (
    CANONICAL_COLUMNS,
    ingest_raw_files,
    normalise_columns,
    read_raw_csv,
    run_ingest,
    validate_interim_schema,
    write_interim_parquet,
)


@pytest.fixture()
def logger() -> logging.Logger:
    return logging.getLogger("test_ingest")


def _make_canonical_df(nrows: int = 3) -> pd.DataFrame:
    """Return a small canonical-schema DataFrame."""
    return pd.DataFrame(
        {
            "APICountyCode": pd.array(["001"] * nrows, dtype=pd.StringDtype()),
            "APISeqNum": pd.array(["00001"] * nrows, dtype=pd.StringDtype()),
            "ReportYear": pd.array([2021] * nrows, dtype=pd.Int32Dtype()),
            "ReportMonth": pd.array([1] * nrows, dtype=pd.Int32Dtype()),
            "OilProduced": pd.array([100.0] * nrows, dtype=pd.Float64Dtype()),
            "OilSold": pd.array([90.0] * nrows, dtype=pd.Float64Dtype()),
            "GasProduced": pd.array([500.0] * nrows, dtype=pd.Float64Dtype()),
            "GasSold": pd.array([490.0] * nrows, dtype=pd.Float64Dtype()),
            "GasFlared": pd.array([0.0] * nrows, dtype=pd.Float64Dtype()),
            "GasUsed": pd.array([10.0] * nrows, dtype=pd.Float64Dtype()),
            "WaterProduced": pd.array([200.0] * nrows, dtype=pd.Float64Dtype()),
            "DaysProduced": pd.array([28.0] * nrows, dtype=pd.Float64Dtype()),
            "Well": pd.array(["Well A"] * nrows, dtype=pd.StringDtype()),
            "OpName": pd.array(["Operator X"] * nrows, dtype=pd.StringDtype()),
            "OpNum": pd.array(["123"] * nrows, dtype=pd.StringDtype()),
            "DocNum": pd.array(["456"] * nrows, dtype=pd.StringDtype()),
        }
    )


# ---------------------------------------------------------------------------
# normalise_columns tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_normalise_columns_canonical_input_unchanged() -> None:
    df = _make_canonical_df()
    result = normalise_columns(df)
    assert list(result.columns) == CANONICAL_COLUMNS
    assert len(result) == 3


@pytest.mark.unit
def test_normalise_columns_variant_names() -> None:
    df = pd.DataFrame(
        {
            "Oil_Produced": [100.0],
            "Gas_Produced": [500.0],
            "Water_Produced": [200.0],
            "Report_Year": [2021],
            "Report_Month": [3],
            "API_County_Code": ["001"],
            "API_Seq_Num": ["00001"],
            "Oil_Sold": [90.0],
            "Gas_Sold": [490.0],
            "Gas_Flared": [0.0],
            "Gas_Used": [10.0],
            "Days_Produced": [28.0],
            "Well": ["W"],
            "Op_Name": ["Ops"],
            "Op_Num": ["1"],
            "Doc_Num": ["2"],
        }
    )
    result = normalise_columns(df)
    assert "OilProduced" in result.columns
    assert "GasProduced" in result.columns
    assert "WaterProduced" in result.columns


@pytest.mark.unit
def test_normalise_columns_adds_missing_columns() -> None:
    df = pd.DataFrame({"APICountyCode": ["001"], "APISeqNum": ["00001"], "ReportYear": [2021]})
    result = normalise_columns(df)
    assert "GasFlared" in result.columns
    assert "GasUsed" in result.columns
    # Missing columns should be null
    assert result["GasFlared"].isna().all()


@pytest.mark.unit
def test_normalise_columns_drops_extra_columns() -> None:
    df = _make_canonical_df()
    df["ExtraColumn"] = "should_be_dropped"
    result = normalise_columns(df)
    assert "ExtraColumn" not in result.columns
    assert list(result.columns) == CANONICAL_COLUMNS


@pytest.mark.unit
def test_normalise_columns_strips_whitespace_from_names() -> None:
    df = pd.DataFrame(
        {
            "  OilProduced  ": [100.0],
            "  ReportYear  ": [2021],
        }
    )
    result = normalise_columns(df)
    assert "OilProduced" in result.columns
    assert "ReportYear" in result.columns


# ---------------------------------------------------------------------------
# read_raw_csv tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_csv_filters_to_2020_plus(tmp_path: Path, logger: logging.Logger) -> None:
    csv_content = (
        "APICountyCode,APISeqNum,ReportYear,ReportMonth,OilProduced,OilSold,"
        "GasProduced,GasSold,GasFlared,GasUsed,WaterProduced,DaysProduced,"
        "Well,OpName,OpNum,DocNum\n"
        "001,00001,2019,1,100.0,90.0,500.0,490.0,0.0,10.0,200.0,28.0,W,Op,1,2\n"
        "001,00001,2020,1,100.0,90.0,500.0,490.0,0.0,10.0,200.0,28.0,W,Op,1,2\n"
        "001,00001,2021,3,150.0,140.0,600.0,590.0,0.0,10.0,220.0,30.0,W,Op,1,2\n"
    )
    f = tmp_path / "test.csv"
    f.write_text(csv_content, encoding="utf-8")
    df = read_raw_csv(f, logger)
    assert len(df) == 2
    assert all(df["ReportYear"].astype(int) >= 2020)


@pytest.mark.unit
def test_read_raw_csv_utf8_no_warning(tmp_path: Path, logger: logging.Logger) -> None:
    csv_content = "APICountyCode,APISeqNum,ReportYear,ReportMonth\n001,00001,2021,1\n"
    f = tmp_path / "utf8.csv"
    f.write_text(csv_content, encoding="utf-8")
    # Should not raise
    df = read_raw_csv(f, logger)
    assert len(df) == 1


@pytest.mark.unit
def test_read_raw_csv_latin1_fallback(tmp_path: Path, logger: logging.Logger) -> None:
    header = "APICountyCode,APISeqNum,ReportYear,ReportMonth,OpName\n"
    row = "001,00001,2021,1,Op\xe9rateur\n"  # é in latin-1
    f = tmp_path / "latin1.csv"
    f.write_bytes((header + row).encode("latin-1"))
    df = read_raw_csv(f, logger)
    assert len(df) == 1


@pytest.mark.unit
def test_read_raw_csv_empty_returns_zero_rows(tmp_path: Path, logger: logging.Logger) -> None:
    header = (
        "APICountyCode,APISeqNum,ReportYear,ReportMonth,OilProduced,OilSold,"
        "GasProduced,GasSold,GasFlared,GasUsed,WaterProduced,DaysProduced,"
        "Well,OpName,OpNum,DocNum\n"
    )
    f = tmp_path / "empty.csv"
    f.write_text(header, encoding="utf-8")
    df = read_raw_csv(f, logger)
    assert len(df) == 0
    assert list(df.columns) == CANONICAL_COLUMNS


@pytest.mark.unit
def test_read_raw_csv_raises_for_binary(tmp_path: Path, logger: logging.Logger) -> None:
    f = tmp_path / "binary.csv"
    f.write_bytes(bytes(range(256)))
    with pytest.raises(ValueError):
        read_raw_csv(f, logger)


# ---------------------------------------------------------------------------
# ingest_raw_files tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ingest_raw_files_returns_dask_dataframe(tmp_path: Path, logger: logging.Logger) -> None:
    csv_content = (
        "APICountyCode,APISeqNum,ReportYear,ReportMonth,OilProduced,OilSold,"
        "GasProduced,GasSold,GasFlared,GasUsed,WaterProduced,DaysProduced,"
        "Well,OpName,OpNum,DocNum\n"
        "001,00001,2021,1,100.0,90.0,500.0,490.0,0.0,10.0,200.0,28.0,W,Op,1,2\n"
    )
    f = tmp_path / "2021_prod_reports.csv"
    f.write_text(csv_content)
    ddf = ingest_raw_files(tmp_path, logger)
    assert isinstance(ddf, dd.DataFrame)


@pytest.mark.unit
def test_ingest_raw_files_meta_has_canonical_schema(tmp_path: Path, logger: logging.Logger) -> None:
    csv_content = (
        "APICountyCode,APISeqNum,ReportYear,ReportMonth,OilProduced,OilSold,"
        "GasProduced,GasSold,GasFlared,GasUsed,WaterProduced,DaysProduced,"
        "Well,OpName,OpNum,DocNum\n"
        "001,00001,2021,1,100.0,90.0,500.0,490.0,0.0,10.0,200.0,28.0,W,Op,1,2\n"
    )
    for name in ("2021_prod_reports.csv", "monthly_prod.csv"):
        (tmp_path / name).write_text(csv_content)
    ddf = ingest_raw_files(tmp_path, logger)
    # Check meta without compute()
    assert list(ddf._meta.columns) == CANONICAL_COLUMNS


@pytest.mark.unit
def test_ingest_raw_files_raises_when_no_csvs(tmp_path: Path, logger: logging.Logger) -> None:
    with pytest.raises(FileNotFoundError):
        ingest_raw_files(tmp_path, logger)


# ---------------------------------------------------------------------------
# write_interim_parquet tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_interim_parquet_returns_correct_path(tmp_path: Path, logger: logging.Logger) -> None:
    df = _make_canonical_df(5)
    ddf = dd.from_pandas(df, npartitions=1)
    result = write_interim_parquet(ddf, tmp_path, logger)
    assert result == tmp_path / "cogcc_production_interim.parquet"


@pytest.mark.unit
def test_write_interim_parquet_readable(tmp_path: Path, logger: logging.Logger) -> None:
    df = _make_canonical_df(10)
    ddf = dd.from_pandas(df, npartitions=1)
    out = write_interim_parquet(ddf, tmp_path, logger)
    result = pd.read_parquet(out)
    assert len(result) == 10
    for col in CANONICAL_COLUMNS:
        assert col in result.columns


@pytest.mark.unit
def test_write_interim_parquet_creates_dir(tmp_path: Path, logger: logging.Logger) -> None:
    new_dir = tmp_path / "new" / "interim"
    df = _make_canonical_df(3)
    ddf = dd.from_pandas(df, npartitions=1)
    write_interim_parquet(ddf, new_dir, logger)
    assert new_dir.exists()


# ---------------------------------------------------------------------------
# validate_interim_schema tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_interim_schema_all_good(tmp_path: Path, logger: logging.Logger) -> None:
    df = _make_canonical_df(5)
    for i in range(3):
        df.to_parquet(tmp_path / f"part.{i}.parquet", index=False)
    errors = validate_interim_schema(tmp_path, logger)
    assert errors == []


@pytest.mark.unit
def test_validate_interim_schema_missing_column(tmp_path: Path, logger: logging.Logger) -> None:
    df = _make_canonical_df(5)
    bad_df = df.drop(columns=["GasFlared"])
    for i in range(3):
        bad_df.to_parquet(tmp_path / f"part.{i}.parquet", index=False)
    errors = validate_interim_schema(tmp_path, logger)
    assert any("GasFlared" in e for e in errors)


@pytest.mark.unit
def test_validate_interim_schema_single_file_no_raise(
    tmp_path: Path, logger: logging.Logger
) -> None:
    df = _make_canonical_df(5)
    df.to_parquet(tmp_path / "part.0.parquet", index=False)
    # Should not raise
    errors = validate_interim_schema(tmp_path, logger)
    assert isinstance(errors, list)


# ---------------------------------------------------------------------------
# run_ingest integration-style unit test
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_ingest_returns_path(tmp_path: Path, logger: logging.Logger) -> None:

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    csv_content = (
        "APICountyCode,APISeqNum,ReportYear,ReportMonth,OilProduced,OilSold,"
        "GasProduced,GasSold,GasFlared,GasUsed,WaterProduced,DaysProduced,"
        "Well,OpName,OpNum,DocNum\n"
        "001,00001,2021,1,100.0,90.0,500.0,490.0,0.0,10.0,200.0,28.0,W,Op,1,2\n"
    )
    (raw_dir / "2021_prod_reports.csv").write_text(csv_content)

    config = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "start_year": 2020,
        }
    }
    result = run_ingest(config, logger)
    assert isinstance(result, Path)
    assert result.name == "cogcc_production_interim.parquet"


@pytest.mark.unit
def test_run_ingest_logs_validation_warnings(tmp_path: Path) -> None:
    """run_ingest should not raise even with schema validation errors."""
    from unittest.mock import patch

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    csv_content = (
        "APICountyCode,APISeqNum,ReportYear,ReportMonth,OilProduced,OilSold,"
        "GasProduced,GasSold,GasFlared,GasUsed,WaterProduced,DaysProduced,"
        "Well,OpName,OpNum,DocNum\n"
        "001,00001,2021,1,100.0,90.0,500.0,490.0,0.0,10.0,200.0,28.0,W,Op,1,2\n"
    )
    (raw_dir / "2021_prod_reports.csv").write_text(csv_content)

    config = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "start_year": 2020,
        }
    }
    lg = logging.getLogger("test_run_ingest_warn")
    with patch(
        "cogcc_pipeline.ingest.validate_interim_schema",
        return_value=["error1", "error2"],
    ):
        result = run_ingest(config, lg)
    assert isinstance(result, Path)
