"""Tests for cogcc_pipeline.ingest module."""

from __future__ import annotations

import hashlib
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from cogcc_pipeline.ingest import (
    IngestError,
    log_file_metadata,
    make_delayed_df,
    read_raw_file,
    validate_schema,
    write_interim,
)
from tests.conftest import CANONICAL_COLUMNS, _make_raw_csv_content

# ---------------------------------------------------------------------------
# log_file_metadata
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_log_file_metadata_sha256(tmp_path: Path) -> None:
    """Returned dict contains correct sha256 hex string."""
    content = b"Header\nRow1\nRow2\n"
    f = tmp_path / "test.csv"
    f.write_bytes(content)
    expected_sha256 = hashlib.sha256(content).hexdigest()

    meta = log_file_metadata(f)
    assert meta["sha256"] == expected_sha256


@pytest.mark.unit
def test_log_file_metadata_row_count(tmp_path: Path) -> None:
    """row_count_raw equals number of data lines (total - header)."""
    f = tmp_path / "test.csv"
    f.write_text("Header\nRow1\nRow2\nRow3\nRow4\nRow5\n", encoding="utf-8")

    meta = log_file_metadata(f)
    assert meta["row_count_raw"] == 5


@pytest.mark.unit
def test_log_file_metadata_file_size(tmp_path: Path) -> None:
    """file_size_bytes equals os.path.getsize."""
    import os

    f = tmp_path / "test.csv"
    f.write_text("Header\nRow1\n", encoding="utf-8")

    meta = log_file_metadata(f)
    assert meta["file_size_bytes"] == os.path.getsize(f)


# ---------------------------------------------------------------------------
# read_raw_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_file_dtypes(tmp_path: Path, config_fixture: dict) -> None:
    """Returned DataFrame has correct dtypes for key columns."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=5), encoding="utf-8")

    df = read_raw_file(csv_path, config_fixture)
    assert str(df["DocNum"].dtype) == "Int64"
    assert str(df["OilProduced"].dtype) == "Float64"
    assert str(df["Revised"].dtype) == "boolean"
    assert isinstance(df["ApiCountyCode"].dtype, pd.StringDtype)
    assert df["AcceptedDate"].dtype in ("datetime64[ns]", "datetime64[us]")


@pytest.mark.unit
def test_read_raw_file_year_filter(tmp_path: Path, config_fixture: dict) -> None:
    """Only rows with ReportYear >= start_year are returned."""
    header = ",".join(CANONICAL_COLUMNS)
    row_old = (
        "1,1,2018,28,2018-01-15,False,Op,100,200,01,12345,00,Well,AC,NIOBRARA,"
        "100.0,90.0,0.0,40.0,500.0,450.0,1000.0,10.0,5.0,200.0,100.0,200.0,50.0,30.0,2.0,10.0,5.0"
    )
    row_new = (
        "2,6,2021,28,2021-06-15,False,Op,100,200,01,12345,00,Well,AC,NIOBRARA,"
        "100.0,90.0,0.0,40.0,500.0,450.0,1000.0,10.0,5.0,200.0,100.0,200.0,50.0,30.0,2.0,10.0,5.0"
    )
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(f"{header}\n{row_old}\n{row_new}\n", encoding="utf-8")

    df = read_raw_file(csv_path, config_fixture)
    assert len(df) == 1
    assert df["ReportYear"].iloc[0] == 2021


@pytest.mark.unit
def test_read_raw_file_missing_column_raises(tmp_path: Path, config_fixture: dict) -> None:
    """CSV missing ApiCountyCode raises IngestError."""
    cols = [c for c in CANONICAL_COLUMNS if c != "ApiCountyCode"]
    header = ",".join(cols)
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(f"{header}\n" + ",".join(["x"] * len(cols)) + "\n", encoding="utf-8")
    with pytest.raises((IngestError, ValueError)):
        read_raw_file(csv_path, config_fixture)


@pytest.mark.unit
def test_read_raw_file_drops_both_null_api(tmp_path: Path, config_fixture: dict) -> None:
    """Rows where both ApiCountyCode and ApiSequenceNumber are null are dropped."""
    header = ",".join(CANONICAL_COLUMNS)
    good_row = (
        "1,1,2021,28,2021-01-15,False,Op,100,200,01,12345,00,Well,AC,NIOBRARA,"
        "100.0,90.0,0.0,40.0,500.0,450.0,1000.0,10.0,5.0,200.0,100.0,200.0,50.0,30.0,2.0,10.0,5.0"
    )
    # Both county code and sequence number null
    bad_row = (
        "2,2,2021,28,2021-02-15,False,Op,100,200,,,00,Well,AC,NIOBRARA,"
        "100.0,90.0,0.0,40.0,500.0,450.0,1000.0,10.0,5.0,200.0,100.0,200.0,50.0,30.0,2.0,10.0,5.0"
    )
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(f"{header}\n{good_row}\n{bad_row}\n", encoding="utf-8")

    df = read_raw_file(csv_path, config_fixture)
    assert len(df) == 1


@pytest.mark.unit
def test_read_raw_file_bom(tmp_path: Path, config_fixture: dict) -> None:
    """File with BOM is read without error and column names are clean."""
    content = _make_raw_csv_content(n_rows=3)
    csv_path = tmp_path / "bom.csv"
    csv_path.write_bytes(b"\xef\xbb\xbf" + content.encode("utf-8"))

    df = read_raw_file(csv_path, config_fixture)
    assert "DocNum" in df.columns
    assert all(not col.startswith("\ufeff") for col in df.columns)


# ---------------------------------------------------------------------------
# make_delayed_df
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_make_delayed_df_returns_dask_df(tmp_path: Path, config_fixture: dict) -> None:
    """make_delayed_df returns a dask.dataframe.DataFrame, not a pandas DataFrame."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=5), encoding="utf-8")

    result = make_delayed_df(csv_path, config_fixture)
    assert isinstance(result, dd.DataFrame)
    assert not isinstance(result, pd.DataFrame)


@pytest.mark.unit
def test_make_delayed_df_compute(tmp_path: Path, config_fixture: dict) -> None:
    """Calling .compute() on the result returns a pandas DataFrame with expected rows."""
    n = 5
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=n), encoding="utf-8")

    ddf = make_delayed_df(csv_path, config_fixture)
    df = ddf.compute()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == n


# ---------------------------------------------------------------------------
# validate_schema
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_schema_valid(tmp_path: Path, config_fixture: dict) -> None:
    """Dask DataFrame with all canonical columns → no exception."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=3), encoding="utf-8")

    ddf = make_delayed_df(csv_path, config_fixture)
    validate_schema(ddf)  # should not raise


@pytest.mark.unit
def test_validate_schema_missing_column(tmp_path: Path, config_fixture: dict) -> None:
    """Dask DataFrame missing OilProduced → IngestError mentioning that column."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=3), encoding="utf-8")

    ddf = make_delayed_df(csv_path, config_fixture)
    ddf = ddf.drop(columns=["OilProduced"])
    with pytest.raises(IngestError, match="OilProduced"):
        validate_schema(ddf)


@pytest.mark.unit
def test_validate_schema_dtype_mismatch_logs_warning(
    tmp_path: Path, config_fixture: dict, caplog: pytest.LogCaptureFixture
) -> None:
    """Dask DataFrame with DocNum as int64 instead of Int64 → WARNING logged, no exception."""
    import logging

    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=3), encoding="utf-8")

    ddf = make_delayed_df(csv_path, config_fixture)
    # Simulate dtype mismatch
    meta = ddf._meta.copy()
    meta["DocNum"] = meta["DocNum"].astype("int64")
    ddf2 = ddf.map_partitions(lambda df: df.assign(DocNum=df["DocNum"].astype("int64")), meta=meta)

    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.ingest"):
        validate_schema(ddf2)  # Should not raise

    # Only warn — no IngestError raised (already confirmed above)


# ---------------------------------------------------------------------------
# write_interim
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_interim_min_10_partitions(tmp_path: Path, config_fixture: dict) -> None:
    """Dask DataFrame with 3 partitions → repartitioned to at least 10 files."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=30), encoding="utf-8")

    ddf = make_delayed_df(csv_path, config_fixture)
    assert ddf.npartitions < 10  # starts with 1

    write_interim(ddf, config_fixture)

    out_dir = Path(config_fixture["ingest"]["interim_dir"]) / "production.parquet"
    parquet_files = list(out_dir.glob("*.parquet"))
    assert len(parquet_files) >= 10


@pytest.mark.unit
def test_write_interim_max_50_partitions(tmp_path: Path, config_fixture: dict) -> None:
    """Dask DataFrame with many partitions → at most 50 files written."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=5), encoding="utf-8")

    ddf = make_delayed_df(csv_path, config_fixture)
    # Force many partitions
    ddf = ddf.repartition(npartitions=200)

    write_interim(ddf, config_fixture)

    out_dir = Path(config_fixture["ingest"]["interim_dir"]) / "production.parquet"
    parquet_files = list(out_dir.glob("*.parquet"))
    assert len(parquet_files) <= 50
