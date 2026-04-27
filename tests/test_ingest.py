"""Tests for the ingest stage."""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from cogcc_pipeline.ingest import (
    CANONICAL_COLUMNS,
    build_dask_dataframe,
    filter_year,
    ingest,
    load_data_dictionary,
    read_raw_file,
    write_parquet,
)

DD_PATH = "references/production-data-dictionary.csv"

# Minimal valid CSV row with all required non-nullable and some nullable columns
_MINIMAL_HEADER = ",".join(CANONICAL_COLUMNS)
_MINIMAL_ROW = (
    "1,3,2021,28,2021-04-01,False,OpA,100,200,05,12345,00,Well A,AC,SandA,"
    "500.0,490.0,0.0,35.0,1000.0,980.0,1100.0,10.0,5.0,100.0,90.0,200.0,50.0,45.0,2.0,10.0,8.0"
)


def _make_csv(tmp_path: Path, filename: str, rows: list[str] | None = None) -> str:
    lines = [_MINIMAL_HEADER]
    if rows is not None:
        lines.extend(rows)
    else:
        lines.append(_MINIMAL_ROW)
    p = tmp_path / filename
    p.write_text("\n".join(lines))
    return str(p)


def _make_config(tmp_path: Path, raw_dir: str, interim_dir: str) -> dict:
    return {
        "ingest": {
            "raw_dir": raw_dir,
            "interim_dir": interim_dir,
            "data_dictionary": DD_PATH,
            "min_year": 2020,
        }
    }


# ---------------------------------------------------------------------------
# Task 01: load_data_dictionary
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_data_dictionary_contains_all_32_columns() -> None:
    schema = load_data_dictionary(DD_PATH)
    for col in CANONICAL_COLUMNS:
        assert col in schema, f"Missing column: {col}"
    assert len(schema) == 32


@pytest.mark.unit
def test_load_data_dictionary_oil_produced_float64_nullable() -> None:
    schema = load_data_dictionary(DD_PATH)
    assert schema["OilProduced"]["dtype"] == "float64"
    assert schema["OilProduced"]["nullable"] == True  # noqa: E712


@pytest.mark.unit
def test_load_data_dictionary_report_year_int64_not_nullable() -> None:
    schema = load_data_dictionary(DD_PATH)
    assert schema["ReportYear"]["dtype"] == "int64"
    assert schema["ReportYear"]["nullable"] == False  # noqa: E712


@pytest.mark.unit
def test_load_data_dictionary_well_status_categorical() -> None:
    schema = load_data_dictionary(DD_PATH)
    dtype = schema["WellStatus"]["dtype"]
    assert isinstance(dtype, pd.CategoricalDtype)
    expected_cats = {"AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"}
    assert expected_cats.issubset(set(dtype.categories))


@pytest.mark.unit
def test_load_data_dictionary_docnum_is_nullable_int64() -> None:
    schema = load_data_dictionary(DD_PATH)
    assert isinstance(schema["DocNum"]["dtype"], pd.Int64Dtype)
    assert schema["DocNum"]["nullable"] == True  # noqa: E712


@pytest.mark.unit
def test_load_data_dictionary_accepted_date_is_datetime() -> None:
    schema = load_data_dictionary(DD_PATH)
    assert schema["AcceptedDate"]["dtype"] == "datetime64[ns]"


# ---------------------------------------------------------------------------
# Task 02: read_raw_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_file_correct_dtypes_spot_check(tmp_path: Path) -> None:
    fp = _make_csv(tmp_path, "test.csv")
    schema = load_data_dictionary(DD_PATH)
    df = read_raw_file(fp, schema)
    assert df["ReportYear"].dtype == np.dtype("int64")
    assert df["OilProduced"].dtype == np.dtype("float64")
    assert df["Well"].dtype == pd.StringDtype()
    assert isinstance(df["WellStatus"].dtype, pd.CategoricalDtype)
    assert df["AcceptedDate"].dtype == np.dtype("datetime64[ns]")


@pytest.mark.unit
def test_read_raw_file_nullable_absent_column_is_all_na(tmp_path: Path) -> None:
    """Given CSV missing DocNum (nullable=yes), output has DocNum as all-NA Int64."""
    cols_without_docnum = [c for c in CANONICAL_COLUMNS if c != "DocNum"]
    row_values = _MINIMAL_ROW.split(",")
    # DocNum is the first column; remove it
    row_without_docnum = ",".join(row_values[1:])
    p = tmp_path / "no_docnum.csv"
    p.write_text(",".join(cols_without_docnum) + "\n" + row_without_docnum)
    schema = load_data_dictionary(DD_PATH)
    df = read_raw_file(str(p), schema)
    assert "DocNum" in df.columns
    assert isinstance(df["DocNum"].dtype, pd.Int64Dtype)
    assert df["DocNum"].isna().all()


@pytest.mark.unit
def test_read_raw_file_raises_on_missing_nonnullable_column(tmp_path: Path) -> None:
    """Given CSV missing ApiCountyCode (nullable=no), raises ValueError."""
    cols_without = [c for c in CANONICAL_COLUMNS if c != "ApiCountyCode"]
    row_values = _MINIMAL_ROW.split(",")
    # ApiCountyCode is at index 9
    row_without = ",".join(
        row_values[i] for i, c in enumerate(CANONICAL_COLUMNS) if c != "ApiCountyCode"
    )
    p = tmp_path / "no_county.csv"
    p.write_text(",".join(cols_without) + "\n" + row_without)
    schema = load_data_dictionary(DD_PATH)
    with pytest.raises(ValueError, match="ApiCountyCode"):
        read_raw_file(str(p), schema)


@pytest.mark.unit
def test_read_raw_file_zero_rows_returns_typed_empty_df(tmp_path: Path) -> None:
    """Header-only CSV yields zero-row DataFrame with correct dtypes."""
    p = tmp_path / "header_only.csv"
    p.write_text(_MINIMAL_HEADER)
    schema = load_data_dictionary(DD_PATH)
    df = read_raw_file(str(p), schema)
    assert len(df) == 0
    assert list(df.columns) == CANONICAL_COLUMNS
    assert df["OilProduced"].dtype == np.dtype("float64")
    assert df["ReportYear"].dtype == np.dtype("int64")


@pytest.mark.unit
def test_read_raw_file_float_cols_use_nan_not_pdNA(tmp_path: Path) -> None:
    """Float columns (OilProduced, GasProduced, WaterProduced) use np.nan sentinel."""
    # Create row with empty float cols
    row_parts = _MINIMAL_ROW.split(",")
    # Set OilProduced (index 15), GasProduced (19), WaterProduced (26) to empty
    float_indices = [15, 19, 26]
    for idx in float_indices:
        row_parts[idx] = ""
    row = ",".join(row_parts)
    p = tmp_path / "null_floats.csv"
    p.write_text(_MINIMAL_HEADER + "\n" + row)
    schema = load_data_dictionary(DD_PATH)
    df = read_raw_file(str(p), schema)
    for col in ["OilProduced", "GasProduced", "WaterProduced"]:
        val = df[col].iloc[0]
        assert np.isnan(float(val))


# ---------------------------------------------------------------------------
# Task 03: build_dask_dataframe
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_dask_dataframe_returns_dask_df(tmp_path: Path) -> None:
    fps = [_make_csv(tmp_path, f"f{i}.csv") for i in range(3)]
    schema = load_data_dictionary(DD_PATH)
    ddf = build_dask_dataframe(fps, schema)
    assert isinstance(ddf, dd.DataFrame)  # TR-17


@pytest.mark.unit
def test_build_dask_dataframe_has_canonical_columns(tmp_path: Path) -> None:
    fps = [_make_csv(tmp_path, f"g{i}.csv") for i in range(2)]
    schema = load_data_dictionary(DD_PATH)
    ddf = build_dask_dataframe(fps, schema)
    assert list(ddf.columns) == CANONICAL_COLUMNS


@pytest.mark.unit
def test_build_dask_dataframe_meta_dtypes_match_data_dict(tmp_path: Path) -> None:
    fps = [_make_csv(tmp_path, f"h{i}.csv") for i in range(2)]
    schema = load_data_dictionary(DD_PATH)
    ddf = build_dask_dataframe(fps, schema)
    meta = ddf._meta
    assert meta["OilProduced"].dtype == np.dtype("float64")
    assert meta["ReportYear"].dtype == np.dtype("int64")


# ---------------------------------------------------------------------------
# Task 04: filter_year
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_filter_year_keeps_only_target_years(tmp_path: Path) -> None:
    schema = load_data_dictionary(DD_PATH)
    rows_2018 = _MINIMAL_ROW.replace(",2021,", ",2018,")
    rows_2020 = _MINIMAL_ROW.replace(",2021,", ",2020,")
    rows_2021 = _MINIMAL_ROW
    rows_2019 = _MINIMAL_ROW.replace(",2021,", ",2019,")
    fp = _make_csv(tmp_path, "mixed.csv", [rows_2018, rows_2019, rows_2020, rows_2021])
    ddf = build_dask_dataframe([fp], schema)
    filtered = filter_year(ddf, 2020)
    result = filtered.compute()
    assert set(result["ReportYear"].unique()) == {2020, 2021}


@pytest.mark.unit
def test_filter_year_all_before_min_returns_empty_dask_df(tmp_path: Path) -> None:
    schema = load_data_dictionary(DD_PATH)
    row_2019 = _MINIMAL_ROW.replace(",2021,", ",2019,")
    fp = _make_csv(tmp_path, "old.csv", [row_2019])
    ddf = build_dask_dataframe([fp], schema)
    filtered = filter_year(ddf, 2020)
    assert isinstance(filtered, dd.DataFrame)
    result = filtered.compute()
    assert len(result) == 0


@pytest.mark.unit
def test_filter_year_returns_dask_dataframe(tmp_path: Path) -> None:
    schema = load_data_dictionary(DD_PATH)
    fp = _make_csv(tmp_path, "t.csv")
    ddf = build_dask_dataframe([fp], schema)
    filtered = filter_year(ddf, 2020)
    assert isinstance(filtered, dd.DataFrame)  # TR-17


# ---------------------------------------------------------------------------
# Task 05: write_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_parquet_creates_readable_files(tmp_path: Path) -> None:
    schema = load_data_dictionary(DD_PATH)
    fps = [_make_csv(tmp_path, f"wp{i}.csv") for i in range(3)]
    ddf = build_dask_dataframe(fps, schema)
    out_dir = str(tmp_path / "interim")
    write_parquet(ddf, out_dir)
    pq_files = list(Path(out_dir).glob("*.parquet"))
    assert len(pq_files) > 0
    result = dd.read_parquet(out_dir)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.integration
def test_write_parquet_readable_by_pandas_and_dask(tmp_path: Path) -> None:
    """TR-18: written Parquet is readable by both dask and pandas."""
    schema = load_data_dictionary(DD_PATH)
    fp = _make_csv(tmp_path, "int.csv")
    ddf = build_dask_dataframe([fp], schema)
    out_dir = str(tmp_path / "interim2")
    write_parquet(ddf, out_dir)
    dask_result = dd.read_parquet(out_dir)
    assert len(dask_result.columns) > 0
    pandas_result = pd.read_parquet(out_dir)
    assert len(pandas_result.columns) > 0


# ---------------------------------------------------------------------------
# Task 06: ingest orchestrator
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_ingest_warns_on_empty_raw_dir(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    cfg = _make_config(tmp_path, str(raw), str(tmp_path / "interim"))
    import logging

    with caplog.at_level(logging.WARNING):
        ingest(cfg)
    assert any("No CSV" in r.message for r in caplog.records)


@pytest.mark.integration
def test_ingest_writes_parquet_with_correct_schema(tmp_path: Path) -> None:
    """TR-24: ingest on real-ish CSVs writes readable Parquet with correct dtypes."""
    raw = tmp_path / "raw"
    raw.mkdir()
    interim = tmp_path / "interim"
    # Create 2 minimal CSV files
    for i in range(2):
        (raw / f"file{i}.csv").write_text(_MINIMAL_HEADER + "\n" + _MINIMAL_ROW)
    cfg = _make_config(tmp_path, str(raw), str(interim))
    ingest(cfg)
    pq_files = list(interim.glob("*.parquet")) + list(interim.glob("**/*.parquet"))
    assert len(pq_files) > 0
    df = pd.read_parquet(str(interim))
    assert "OilProduced" in df.columns
    assert df["OilProduced"].dtype == np.dtype("float64")


@pytest.mark.integration
def test_ingest_all_expected_columns_present(tmp_path: Path) -> None:
    """TR-22: all expected columns present in every sampled partition."""
    raw = tmp_path / "raw"
    raw.mkdir()
    interim = tmp_path / "interim3"
    for i in range(3):
        (raw / f"f{i}.csv").write_text(_MINIMAL_HEADER + "\n" + _MINIMAL_ROW)
    cfg = _make_config(tmp_path, str(raw), str(interim))
    ingest(cfg)
    expected = [
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
        "OpNumber",
        "DocNum",
    ]
    ddf = dd.read_parquet(str(interim))
    for col in expected:
        assert col in ddf.columns, f"Missing column: {col}"
