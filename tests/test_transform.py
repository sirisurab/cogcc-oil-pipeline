"""Tests for cogcc_pipeline.transform."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from cogcc_pipeline.transform import (
    build_production_date,
    build_well_id,
    check_well_completeness,
    generate_quality_report,
    remove_duplicates,
    replace_sentinels,
    run_transform,
    validate_physical_bounds,
    write_cleaned_parquet,
)


@pytest.fixture()
def logger() -> logging.Logger:
    return logging.getLogger("test_transform")


def _base_df(**kwargs: object) -> pd.DataFrame:
    """Build a minimal canonical DataFrame for transform tests."""
    defaults: dict[str, object] = {
        "APICountyCode": pd.array(["001"], dtype=pd.StringDtype()),
        "APISeqNum": pd.array(["00023"], dtype=pd.StringDtype()),
        "ReportYear": pd.array([2022], dtype=pd.Int32Dtype()),
        "ReportMonth": pd.array([3], dtype=pd.Int32Dtype()),
        "OilProduced": pd.array([100.0], dtype=pd.Float64Dtype()),
        "OilSold": pd.array([90.0], dtype=pd.Float64Dtype()),
        "GasProduced": pd.array([500.0], dtype=pd.Float64Dtype()),
        "GasSold": pd.array([490.0], dtype=pd.Float64Dtype()),
        "GasFlared": pd.array([0.0], dtype=pd.Float64Dtype()),
        "GasUsed": pd.array([10.0], dtype=pd.Float64Dtype()),
        "WaterProduced": pd.array([200.0], dtype=pd.Float64Dtype()),
        "DaysProduced": pd.array([28.0], dtype=pd.Float64Dtype()),
        "Well": pd.array(["Well A"], dtype=pd.StringDtype()),
        "OpName": pd.array(["Operator X"], dtype=pd.StringDtype()),
        "OpNum": pd.array(["123"], dtype=pd.StringDtype()),
        "DocNum": pd.array(["456"], dtype=pd.StringDtype()),
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


# ---------------------------------------------------------------------------
# replace_sentinels tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_replace_sentinels_preserves_zeros(logger: logging.Logger) -> None:
    df = _base_df(OilProduced=pd.array([0.0], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    result = replace_sentinels(ddf, logger).compute()
    assert float(result["OilProduced"].iloc[0]) == 0.0


@pytest.mark.unit
def test_replace_sentinels_replaces_negative_9999(logger: logging.Logger) -> None:
    df = _base_df(OilProduced=pd.array([-9999.0], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    result = replace_sentinels(ddf, logger).compute()
    assert pd.isna(result["OilProduced"].iloc[0])


@pytest.mark.unit
def test_replace_sentinels_replaces_positive_9999(logger: logging.Logger) -> None:
    df = _base_df(OilProduced=pd.array([9999.0], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    result = replace_sentinels(ddf, logger).compute()
    assert pd.isna(result["OilProduced"].iloc[0])


@pytest.mark.unit
def test_replace_sentinels_leaves_string_columns_unchanged(logger: logging.Logger) -> None:
    df = _base_df(OpName=pd.array(["-9999"], dtype=pd.StringDtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    result = replace_sentinels(ddf, logger).compute()
    assert result["OpName"].iloc[0] == "-9999"


@pytest.mark.unit
def test_replace_sentinels_returns_dask_dataframe(logger: logging.Logger) -> None:
    ddf = dd.from_pandas(_base_df(), npartitions=1)
    result = replace_sentinels(ddf, logger)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# build_well_id tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_well_id_single_digit_county(logger: logging.Logger) -> None:
    df = _base_df(
        APICountyCode=pd.array(["1"], dtype=pd.StringDtype()),
        APISeqNum=pd.array(["23"], dtype=pd.StringDtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = build_well_id(ddf, logger).compute()
    assert result["well_id"].iloc[0] == "0500100023"


@pytest.mark.unit
def test_build_well_id_full_digits(logger: logging.Logger) -> None:
    df = _base_df(
        APICountyCode=pd.array(["123"], dtype=pd.StringDtype()),
        APISeqNum=pd.array(["45678"], dtype=pd.StringDtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = build_well_id(ddf, logger).compute()
    assert result["well_id"].iloc[0] == "0512345678"


@pytest.mark.unit
def test_build_well_id_invalid_county_over_125(logger: logging.Logger) -> None:
    df = _base_df(
        APICountyCode=pd.array(["200"], dtype=pd.StringDtype()),
        APISeqNum=pd.array(["00001"], dtype=pd.StringDtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = build_well_id(ddf, logger).compute()
    assert pd.isna(result["well_id"].iloc[0])


@pytest.mark.unit
def test_build_well_id_zero_sequence_invalid(logger: logging.Logger) -> None:
    df = _base_df(
        APICountyCode=pd.array(["0"], dtype=pd.StringDtype()),
        APISeqNum=pd.array(["0"], dtype=pd.StringDtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = build_well_id(ddf, logger).compute()
    assert pd.isna(result["well_id"].iloc[0])


@pytest.mark.unit
def test_build_well_id_null_county_no_exception(logger: logging.Logger) -> None:
    df = _base_df(
        APICountyCode=pd.array([pd.NA], dtype=pd.StringDtype()),
        APISeqNum=pd.array(["00001"], dtype=pd.StringDtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = build_well_id(ddf, logger).compute()
    assert pd.isna(result["well_id"].iloc[0])


@pytest.mark.unit
def test_build_well_id_returns_dask_dataframe(logger: logging.Logger) -> None:
    ddf = dd.from_pandas(_base_df(), npartitions=1)
    result = build_well_id(ddf, logger)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# build_production_date tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_production_date_correct_date(logger: logging.Logger) -> None:
    df = _base_df(
        ReportYear=pd.array([2022], dtype=pd.Int32Dtype()),
        ReportMonth=pd.array([3], dtype=pd.Int32Dtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    result = build_production_date(ddf, logger).compute()
    assert result["production_date"].iloc[0] == pd.Timestamp("2022-03-01")


@pytest.mark.unit
def test_build_production_date_invalid_month(logger: logging.Logger) -> None:
    df = _base_df(
        ReportYear=pd.array([2022], dtype=pd.Int32Dtype()),
        ReportMonth=pd.array([13], dtype=pd.Int32Dtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    result = build_production_date(ddf, logger).compute()
    assert pd.isna(result["production_date"].iloc[0])


@pytest.mark.unit
def test_build_production_date_year_below_2020(logger: logging.Logger) -> None:
    df = _base_df(
        ReportYear=pd.array([2019], dtype=pd.Int32Dtype()),
        ReportMonth=pd.array([6], dtype=pd.Int32Dtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    result = build_production_date(ddf, logger).compute()
    assert pd.isna(result["production_date"].iloc[0])


@pytest.mark.unit
def test_build_production_date_null_year_no_exception(logger: logging.Logger) -> None:
    df = _base_df(
        ReportYear=pd.array([pd.NA], dtype=pd.Int32Dtype()),
        ReportMonth=pd.array([3], dtype=pd.Int32Dtype()),
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    result = build_production_date(ddf, logger).compute()
    assert pd.isna(result["production_date"].iloc[0])


@pytest.mark.unit
def test_build_production_date_returns_dask_dataframe(logger: logging.Logger) -> None:
    ddf = dd.from_pandas(_base_df(), npartitions=1)
    ddf = build_well_id(ddf, logger)
    result = build_production_date(ddf, logger)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# validate_physical_bounds tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_bounds_negative_oil_becomes_null(logger: logging.Logger) -> None:
    df = _base_df(OilProduced=pd.array([-100.0], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    ddf = build_production_date(ddf, logger)
    result = validate_physical_bounds(ddf, logger).compute()
    assert pd.isna(result["OilProduced"].iloc[0])
    assert not result["outlier_flag"].iloc[0]


@pytest.mark.unit
def test_validate_bounds_negative_water_becomes_null(logger: logging.Logger) -> None:
    df = _base_df(WaterProduced=pd.array([-0.001], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    ddf = build_production_date(ddf, logger)
    result = validate_physical_bounds(ddf, logger).compute()
    assert pd.isna(result["WaterProduced"].iloc[0])


@pytest.mark.unit
def test_validate_bounds_high_oil_sets_outlier_flag(logger: logging.Logger) -> None:
    df = _base_df(OilProduced=pd.array([75_000.0], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    ddf = build_production_date(ddf, logger)
    result = validate_physical_bounds(ddf, logger).compute()
    assert result["outlier_flag"].iloc[0]
    # Value is retained
    assert float(result["OilProduced"].iloc[0]) == 75_000.0


@pytest.mark.unit
def test_validate_bounds_oil_below_threshold_no_flag(logger: logging.Logger) -> None:
    df = _base_df(OilProduced=pd.array([49_999.0], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    ddf = build_production_date(ddf, logger)
    result = validate_physical_bounds(ddf, logger).compute()
    assert not result["outlier_flag"].iloc[0]


@pytest.mark.unit
def test_validate_bounds_days_over_31_becomes_null(logger: logging.Logger) -> None:
    df = _base_df(DaysProduced=pd.array([32.0], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    ddf = build_production_date(ddf, logger)
    result = validate_physical_bounds(ddf, logger).compute()
    assert pd.isna(result["DaysProduced"].iloc[0])


@pytest.mark.unit
def test_validate_bounds_zero_oil_preserved(logger: logging.Logger) -> None:
    df = _base_df(OilProduced=pd.array([0.0], dtype=pd.Float64Dtype()))
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = build_well_id(ddf, logger)
    ddf = build_production_date(ddf, logger)
    result = validate_physical_bounds(ddf, logger).compute()
    assert float(result["OilProduced"].iloc[0]) == 0.0


@pytest.mark.unit
def test_validate_bounds_returns_dask_dataframe(logger: logging.Logger) -> None:
    ddf = dd.from_pandas(_base_df(), npartitions=1)
    ddf = build_well_id(ddf, logger)
    ddf = build_production_date(ddf, logger)
    result = validate_physical_bounds(ddf, logger)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# remove_duplicates tests
# ---------------------------------------------------------------------------


def _well_df_with_dates(well_id: str, dates: list[str], logger: logging.Logger) -> dd.DataFrame:
    rows = len(dates)
    df = pd.DataFrame(
        {
            "well_id": pd.array([well_id] * rows, dtype=pd.StringDtype()),
            "production_date": pd.to_datetime(dates),
            "OilProduced": pd.array([100.0] * rows, dtype=pd.Float64Dtype()),
        }
    )
    return dd.from_pandas(df, npartitions=1)


@pytest.mark.unit
def test_remove_duplicates_keeps_first(logger: logging.Logger) -> None:
    dates = ["2022-01-01", "2022-01-01", "2022-02-01"]
    ddf = _well_df_with_dates("0500100001", dates, logger)
    result = remove_duplicates(ddf, logger).compute()
    assert len(result) == 2


@pytest.mark.unit
def test_remove_duplicates_idempotent(logger: logging.Logger) -> None:
    dates = ["2022-01-01", "2022-01-01", "2022-02-01"]
    ddf = _well_df_with_dates("0500100001", dates, logger)
    result1 = remove_duplicates(ddf, logger).compute()
    ddf2 = dd.from_pandas(result1, npartitions=1)
    result2 = remove_duplicates(ddf2, logger).compute()
    assert len(result1) == len(result2)


@pytest.mark.unit
def test_remove_duplicates_retains_null_well_ids(logger: logging.Logger) -> None:
    df = pd.DataFrame(
        {
            "well_id": pd.array([pd.NA, pd.NA], dtype=pd.StringDtype()),
            "production_date": pd.to_datetime(["2022-01-01", "2022-02-01"]),
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = remove_duplicates(ddf, logger).compute()
    assert len(result) == 2


@pytest.mark.unit
def test_remove_duplicates_returns_dask_dataframe(logger: logging.Logger) -> None:
    ddf = _well_df_with_dates("0500100001", ["2022-01-01"], logger)
    result = remove_duplicates(ddf, logger)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# generate_quality_report tests
# ---------------------------------------------------------------------------


def _cleaned_ddf(nrows: int = 5) -> dd.DataFrame:
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001"] * nrows, dtype=pd.StringDtype()),
            "production_date": pd.date_range("2022-01-01", periods=nrows, freq="MS"),
            "OilProduced": pd.array([100.0] * nrows, dtype=pd.Float64Dtype()),
            "outlier_flag": pd.array([False] * nrows, dtype=pd.BooleanDtype()),
        }
    )
    return dd.from_pandas(df, npartitions=1)


@pytest.mark.unit
def test_generate_quality_report_null_counts(tmp_path: Path, logger: logging.Logger) -> None:
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001", None], dtype=pd.StringDtype()),
            "production_date": [pd.Timestamp("2022-01-01"), None],
            "OilProduced": pd.array([100.0, None], dtype=pd.Float64Dtype()),
            "outlier_flag": pd.array([False, False], dtype=pd.BooleanDtype()),
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    report = generate_quality_report(ddf, tmp_path / "report.json", logger)
    assert report["null_counts"]["well_id"] == 1
    assert report["total_rows"] == 2


@pytest.mark.unit
def test_generate_quality_report_json_is_written(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _cleaned_ddf()
    out = tmp_path / "report.json"
    generate_quality_report(ddf, out, logger)
    assert out.exists()
    data = json.loads(out.read_text())
    assert "total_rows" in data


@pytest.mark.unit
def test_generate_quality_report_outlier_count(tmp_path: Path, logger: logging.Logger) -> None:
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001"] * 3, dtype=pd.StringDtype()),
            "production_date": pd.date_range("2022-01-01", periods=3, freq="MS"),
            "OilProduced": pd.array([100.0, 200.0, 300.0], dtype=pd.Float64Dtype()),
            "outlier_flag": pd.array([True, True, False], dtype=pd.BooleanDtype()),
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    report = generate_quality_report(ddf, tmp_path / "rep.json", logger)
    assert report["outlier_flag_count"] == 2


# ---------------------------------------------------------------------------
# check_well_completeness tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_check_well_completeness_no_gaps(logger: logging.Logger) -> None:
    dates = pd.date_range("2022-01-01", periods=12, freq="MS")
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001"] * 12, dtype=pd.StringDtype()),
            "production_date": dates,
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    gaps = check_well_completeness(ddf, logger)
    assert gaps["0500100001"] == []


@pytest.mark.unit
def test_check_well_completeness_missing_april(logger: logging.Logger) -> None:
    dates = list(pd.date_range("2022-01-01", "2022-03-01", freq="MS")) + list(
        pd.date_range("2022-05-01", "2022-12-01", freq="MS")
    )
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001"] * len(dates), dtype=pd.StringDtype()),
            "production_date": dates,
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    gaps = check_well_completeness(ddf, logger)
    assert "2022-04-01" in gaps["0500100001"]


@pytest.mark.unit
def test_check_well_completeness_single_record(logger: logging.Logger) -> None:
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001"], dtype=pd.StringDtype()),
            "production_date": [pd.Timestamp("2022-01-01")],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    gaps = check_well_completeness(ddf, logger)
    assert gaps.get("0500100001", []) == []


# ---------------------------------------------------------------------------
# write_cleaned_parquet tests
# ---------------------------------------------------------------------------


def _full_cleaned_ddf(nrows: int = 10) -> dd.DataFrame:
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001"] * nrows, dtype=pd.StringDtype()),
            "production_date": pd.date_range("2022-01-01", periods=nrows, freq="MS"),
            "OilProduced": pd.array([100.0] * nrows, dtype=pd.Float64Dtype()),
            "outlier_flag": pd.array([False] * nrows, dtype=pd.BooleanDtype()),
        }
    )
    return dd.from_pandas(df, npartitions=1)


@pytest.mark.unit
def test_write_cleaned_parquet_readable(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _full_cleaned_ddf()
    out = write_cleaned_parquet(ddf, tmp_path, "cleaned.parquet", logger)
    df = pd.read_parquet(out)
    assert len(df) == 10


@pytest.mark.unit
def test_write_cleaned_parquet_returns_correct_path(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _full_cleaned_ddf()
    out = write_cleaned_parquet(ddf, tmp_path, "cleaned.parquet", logger)
    assert out == tmp_path / "cleaned.parquet"


@pytest.mark.unit
def test_write_cleaned_parquet_file_count_at_most_50(
    tmp_path: Path, logger: logging.Logger
) -> None:
    ddf = _full_cleaned_ddf(5)
    out = write_cleaned_parquet(ddf, tmp_path, "cleaned.parquet", logger)
    parquet_files = list(out.glob("*.parquet"))
    assert len(parquet_files) <= 50


@pytest.mark.unit
def test_run_transform_returns_path(tmp_path: Path, logger: logging.Logger) -> None:
    """run_transform should return the expected cleaned parquet path."""

    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    interim_dir.mkdir()

    # Create a synthetic interim Parquet
    df = pd.DataFrame(
        {
            "APICountyCode": pd.array(["001"], dtype=pd.StringDtype()),
            "APISeqNum": pd.array(["00001"], dtype=pd.StringDtype()),
            "ReportYear": pd.array([2022], dtype=pd.Int32Dtype()),
            "ReportMonth": pd.array([3], dtype=pd.Int32Dtype()),
            "OilProduced": pd.array([100.0], dtype=pd.Float64Dtype()),
            "OilSold": pd.array([90.0], dtype=pd.Float64Dtype()),
            "GasProduced": pd.array([500.0], dtype=pd.Float64Dtype()),
            "GasSold": pd.array([490.0], dtype=pd.Float64Dtype()),
            "GasFlared": pd.array([0.0], dtype=pd.Float64Dtype()),
            "GasUsed": pd.array([10.0], dtype=pd.Float64Dtype()),
            "WaterProduced": pd.array([200.0], dtype=pd.Float64Dtype()),
            "DaysProduced": pd.array([28.0], dtype=pd.Float64Dtype()),
            "Well": pd.array(["W"], dtype=pd.StringDtype()),
            "OpName": pd.array(["Op"], dtype=pd.StringDtype()),
            "OpNum": pd.array(["1"], dtype=pd.StringDtype()),
            "DocNum": pd.array(["2"], dtype=pd.StringDtype()),
        }
    )
    interim_pq = interim_dir / "cogcc_production_interim.parquet"
    interim_pq.mkdir()
    df.to_parquet(interim_pq / "part.0.parquet", index=False)

    config = {
        "transform": {
            "interim_dir": str(interim_dir),
            "processed_dir": str(processed_dir),
            "cleaned_file": "cogcc_production_cleaned.parquet",
        }
    }
    result = run_transform(config, logger)
    assert isinstance(result, Path)
    assert result.name == "cogcc_production_cleaned.parquet"
