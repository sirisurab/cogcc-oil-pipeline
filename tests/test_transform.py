"""Tests for cogcc_pipeline.transform module."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from cogcc_pipeline.transform import (
    WELL_STATUS_CATEGORIES,
    _build_production_date,
    _build_well_id,
    _cast_well_status,
    _clean_production_volumes,
    _deduplicate,
    _transform_partition,
    check_data_integrity,
    check_well_completeness,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_df(**overrides: object) -> pd.DataFrame:
    """Return a minimal DataFrame with all required columns for transform tests."""
    base = {
        "ApiCountyCode": pd.array(["01"], dtype=pd.StringDtype()),
        "ApiSequenceNumber": pd.array(["12345"], dtype=pd.StringDtype()),
        "ApiSidetrack": pd.array(["00"], dtype=pd.StringDtype()),
        "Well": pd.array(["TestWell"], dtype=pd.StringDtype()),
        "ReportYear": pd.array([2021], dtype="int64"),
        "ReportMonth": pd.array([6], dtype="int64"),
        "AcceptedDate": pd.to_datetime(["2021-06-15"]),
        "DaysProduced": pd.array([28], dtype=pd.Int64Dtype()),
        "OilProduced": pd.array([100.0], dtype=pd.Float64Dtype()),
        "GasProduced": pd.array([500.0], dtype=pd.Float64Dtype()),
        "WaterProduced": pd.array([200.0], dtype=pd.Float64Dtype()),
        "WellStatus": pd.array(["AC"], dtype=pd.StringDtype()),
        "DocNum": pd.array([1], dtype=pd.Int64Dtype()),
        "Revised": pd.array([False], dtype=pd.BooleanDtype()),
        "OpName": pd.array(["Op"], dtype=pd.StringDtype()),
        "OpNumber": pd.array([100], dtype=pd.Int64Dtype()),
        "FacilityId": pd.array([200], dtype=pd.Int64Dtype()),
        "FormationCode": pd.array(["NIOBRARA"], dtype=pd.StringDtype()),
        "OilSales": pd.array([90.0], dtype=pd.Float64Dtype()),
        "OilAdjustment": pd.array([0.0], dtype=pd.Float64Dtype()),
        "OilGravity": pd.array([40.0], dtype=pd.Float64Dtype()),
        "GasSales": pd.array([450.0], dtype=pd.Float64Dtype()),
        "GasBtuSales": pd.array([1000.0], dtype=pd.Float64Dtype()),
        "GasUsedOnLease": pd.array([10.0], dtype=pd.Float64Dtype()),
        "GasShrinkage": pd.array([5.0], dtype=pd.Float64Dtype()),
        "GasPressureTubing": pd.array([200.0], dtype=pd.Float64Dtype()),
        "GasPressureCasing": pd.array([100.0], dtype=pd.Float64Dtype()),
        "WaterPressureTubing": pd.array([50.0], dtype=pd.Float64Dtype()),
        "WaterPressureCasing": pd.array([30.0], dtype=pd.Float64Dtype()),
        "FlaredVented": pd.array([2.0], dtype=pd.Float64Dtype()),
        "BomInvent": pd.array([10.0], dtype=pd.Float64Dtype()),
        "EomInvent": pd.array([5.0], dtype=pd.Float64Dtype()),
    }
    base.update(overrides)
    return pd.DataFrame(base)


# ---------------------------------------------------------------------------
# _build_well_id
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_well_id_basic() -> None:
    """well_id matches '05-{county}-{seq}-{sidetrack}'."""
    df = _minimal_df()
    result = _build_well_id(df)
    assert result.iloc[0] == "05-01-12345-00"


@pytest.mark.unit
def test_build_well_id_null_sidetrack() -> None:
    """Null ApiSidetrack → '00' sidetrack component."""
    df = _minimal_df(ApiSidetrack=pd.array([pd.NA], dtype=pd.StringDtype()))
    result = _build_well_id(df)
    assert result.iloc[0].endswith("-00")


@pytest.mark.unit
def test_build_well_id_vectorised(tmp_path: Path) -> None:
    """100-row DataFrame produces same result without a Python loop."""
    n = 100
    df = pd.DataFrame(
        {
            "ApiCountyCode": pd.array(["01"] * n, dtype=pd.StringDtype()),
            "ApiSequenceNumber": pd.array([f"{i:05d}" for i in range(n)], dtype=pd.StringDtype()),
            "ApiSidetrack": pd.array(["00"] * n, dtype=pd.StringDtype()),
        }
    )
    result = _build_well_id(df)
    assert len(result) == n
    for i, val in enumerate(result):
        assert val == f"05-01-{i:05d}-00"


@pytest.mark.unit
def test_build_well_id_dtype() -> None:
    """Return dtype is pd.StringDtype()."""
    df = _minimal_df()
    result = _build_well_id(df)
    assert isinstance(result.dtype, pd.StringDtype)


# ---------------------------------------------------------------------------
# _build_production_date
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_production_date_june() -> None:
    """ReportYear=2021, ReportMonth=6 → Timestamp('2021-06-01')."""
    df = _minimal_df(
        ReportYear=pd.array([2021], dtype="int64"),
        ReportMonth=pd.array([6], dtype="int64"),
    )
    result = _build_production_date(df)
    assert result.iloc[0] == pd.Timestamp("2021-06-01")


@pytest.mark.unit
def test_build_production_date_december() -> None:
    """ReportYear=2020, ReportMonth=12 → Timestamp('2020-12-01')."""
    df = _minimal_df(
        ReportYear=pd.array([2020], dtype="int64"),
        ReportMonth=pd.array([12], dtype="int64"),
    )
    result = _build_production_date(df)
    assert result.iloc[0] == pd.Timestamp("2020-12-01")


@pytest.mark.unit
def test_build_production_date_invalid_month() -> None:
    """Invalid month (13) → NaT (not an exception)."""
    df = _minimal_df(
        ReportYear=pd.array([2021], dtype="int64"),
        ReportMonth=pd.array([13], dtype="int64"),
    )
    result = _build_production_date(df)
    assert pd.isna(result.iloc[0])


@pytest.mark.unit
def test_build_production_date_dtype() -> None:
    """Return dtype is datetime64[ns]."""
    df = _minimal_df()
    result = _build_production_date(df)
    assert "datetime64" in str(result.dtype)


# ---------------------------------------------------------------------------
# _deduplicate
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_deduplicate_keeps_latest_accepted_date() -> None:
    """Two rows with same key → keep the one with the later AcceptedDate."""
    df = pd.DataFrame(
        {
            "ApiCountyCode": pd.array(["01", "01"], dtype=pd.StringDtype()),
            "ApiSequenceNumber": pd.array(["12345", "12345"], dtype=pd.StringDtype()),
            "ReportYear": [2021, 2021],
            "ReportMonth": [6, 6],
            "AcceptedDate": pd.to_datetime(["2021-06-01", "2021-06-15"]),
            "OilProduced": pd.array([100.0, 200.0], dtype=pd.Float64Dtype()),
        }
    )
    result = _deduplicate(df)
    assert len(result) == 1
    assert result["OilProduced"].iloc[0] == 200.0


@pytest.mark.unit
def test_deduplicate_no_duplicates_unchanged() -> None:
    """No duplicates → row count unchanged."""
    df = pd.DataFrame(
        {
            "ApiCountyCode": pd.array(["01", "02"], dtype=pd.StringDtype()),
            "ApiSequenceNumber": pd.array(["12345", "67890"], dtype=pd.StringDtype()),
            "ReportYear": [2021, 2021],
            "ReportMonth": [6, 7],
            "AcceptedDate": pd.to_datetime(["2021-06-01", "2021-07-01"]),
        }
    )
    result = _deduplicate(df)
    assert len(result) == 2


@pytest.mark.unit
def test_deduplicate_all_null_accepted_date() -> None:
    """All-null AcceptedDate for duplicates → one row retained per key."""
    df = pd.DataFrame(
        {
            "ApiCountyCode": pd.array(["01", "01"], dtype=pd.StringDtype()),
            "ApiSequenceNumber": pd.array(["12345", "12345"], dtype=pd.StringDtype()),
            "ReportYear": [2021, 2021],
            "ReportMonth": [6, 6],
            "AcceptedDate": pd.to_datetime(pd.Series([pd.NaT, pd.NaT])),
        }
    )
    result = _deduplicate(df)
    assert len(result) == 1


@pytest.mark.unit
def test_deduplicate_idempotent() -> None:
    """Calling _deduplicate twice equals calling it once (TR-15)."""
    df = pd.DataFrame(
        {
            "ApiCountyCode": pd.array(["01", "01", "02"], dtype=pd.StringDtype()),
            "ApiSequenceNumber": pd.array(["12345", "12345", "67890"], dtype=pd.StringDtype()),
            "ReportYear": [2021, 2021, 2021],
            "ReportMonth": [6, 6, 7],
            "AcceptedDate": pd.to_datetime(["2021-06-01", "2021-06-15", "2021-07-01"]),
        }
    )
    once = _deduplicate(df).reset_index(drop=True)
    twice = _deduplicate(_deduplicate(df)).reset_index(drop=True)
    pd.testing.assert_frame_equal(once, twice)


@pytest.mark.unit
def test_deduplicate_row_count_leq() -> None:
    """Row count after deduplication ≤ row count before (TR-15)."""
    df = _minimal_df()
    df = pd.concat([df, df]).reset_index(drop=True)
    result = _deduplicate(df)
    assert len(result) <= len(df)


# ---------------------------------------------------------------------------
# _clean_production_volumes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_clean_production_volumes_negative_oil(caplog: pytest.LogCaptureFixture) -> None:
    """Negative OilProduced → pd.NA (TR-01)."""
    df = _minimal_df(OilProduced=pd.array([-100.0], dtype=pd.Float64Dtype()))
    result = _clean_production_volumes(df)
    assert pd.isna(result["OilProduced"].iloc[0])


@pytest.mark.unit
def test_clean_production_volumes_zero_preserved() -> None:
    """Zero OilProduced → remains 0.0 (TR-05)."""
    df = _minimal_df(OilProduced=pd.array([0.0], dtype=pd.Float64Dtype()))
    result = _clean_production_volumes(df)
    assert result["OilProduced"].iloc[0] == 0.0


@pytest.mark.unit
def test_clean_production_volumes_outlier_oil(caplog: pytest.LogCaptureFixture) -> None:
    """OilProduced > 50,000 → pd.NA + WARNING (TR-02)."""
    df = _minimal_df(OilProduced=pd.array([75000.0], dtype=pd.Float64Dtype()))
    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.transform"):
        result = _clean_production_volumes(df)
    assert pd.isna(result["OilProduced"].iloc[0])
    assert any("outlier" in r.message.lower() or "75000" in r.message for r in caplog.records)


@pytest.mark.unit
def test_clean_production_volumes_negative_gas() -> None:
    """Negative GasProduced → pd.NA."""
    df = _minimal_df(GasProduced=pd.array([-1.0], dtype=pd.Float64Dtype()))
    result = _clean_production_volumes(df)
    assert pd.isna(result["GasProduced"].iloc[0])


@pytest.mark.unit
def test_clean_production_volumes_zero_water_zero_days() -> None:
    """WaterProduced=0.0 and DaysProduced=0 → no exception, values preserved."""
    df = _minimal_df(
        WaterProduced=pd.array([0.0], dtype=pd.Float64Dtype()),
        DaysProduced=pd.array([0], dtype=pd.Int64Dtype()),
    )
    result = _clean_production_volumes(df)
    assert result["WaterProduced"].iloc[0] == 0.0


@pytest.mark.unit
def test_clean_production_volumes_nonzero_oil_zero_days(caplog: pytest.LogCaptureFixture) -> None:
    """DaysProduced=0 but OilProduced=100 → WARNING, OilProduced unchanged."""
    df = _minimal_df(
        OilProduced=pd.array([100.0], dtype=pd.Float64Dtype()),
        DaysProduced=pd.array([0], dtype=pd.Int64Dtype()),
    )
    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.transform"):
        result = _clean_production_volumes(df)
    assert result["OilProduced"].iloc[0] == 100.0
    assert any("DaysProduced" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _cast_well_status
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cast_well_status_valid_values() -> None:
    """Valid WellStatus values → CategoricalDtype with declared categories."""
    df = _minimal_df()
    df = pd.concat([df, df, df]).reset_index(drop=True)
    df["WellStatus"] = pd.array(["AC", "SI", "PA"], dtype=pd.StringDtype())
    result = _cast_well_status(df)
    assert isinstance(result["WellStatus"].dtype, pd.CategoricalDtype)
    assert list(result["WellStatus"].cat.categories) == WELL_STATUS_CATEGORIES


@pytest.mark.unit
def test_cast_well_status_unknown_becomes_na() -> None:
    """Value 'XX' not in allowed list → pd.NA."""
    df = _minimal_df(WellStatus=pd.array(["XX"], dtype=pd.StringDtype()))
    result = _cast_well_status(df)
    assert pd.isna(result["WellStatus"].iloc[0])


@pytest.mark.unit
def test_cast_well_status_null_remains_null() -> None:
    """Null WellStatus → remains null after cast."""
    df = _minimal_df(WellStatus=pd.array([pd.NA], dtype=pd.StringDtype()))
    result = _cast_well_status(df)
    assert pd.isna(result["WellStatus"].iloc[0])


@pytest.mark.unit
def test_cast_well_status_unknown_count() -> None:
    """Non-null count after cast equals count of rows with recognised status codes."""
    df = pd.DataFrame(
        {
            "WellStatus": pd.array(["AC", "XX", "SI", pd.NA], dtype=pd.StringDtype()),
        }
    )
    result = _cast_well_status(df)
    recognised_count = 2  # AC and SI
    assert result["WellStatus"].notna().sum() == recognised_count


# ---------------------------------------------------------------------------
# _transform_partition
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_transform_partition_has_well_id_and_date() -> None:
    """Output contains well_id and production_date columns."""
    df = _minimal_df()
    result = _transform_partition(df)
    assert "well_id" in result.columns
    assert "production_date" in result.columns


@pytest.mark.unit
def test_transform_partition_drops_year_month() -> None:
    """Output does NOT contain ReportYear or ReportMonth."""
    df = _minimal_df()
    result = _transform_partition(df)
    assert "ReportYear" not in result.columns
    assert "ReportMonth" not in result.columns


@pytest.mark.unit
def test_transform_partition_well_id_dtype() -> None:
    """well_id dtype is pd.StringDtype()."""
    df = _minimal_df()
    result = _transform_partition(df)
    assert isinstance(result["well_id"].dtype, pd.StringDtype)


@pytest.mark.unit
def test_transform_partition_production_date_dtype() -> None:
    """production_date dtype is datetime64[ns]."""
    df = _minimal_df()
    result = _transform_partition(df)
    assert "datetime64" in str(result["production_date"].dtype)


@pytest.mark.unit
def test_transform_partition_well_status_categorical() -> None:
    """WellStatus dtype is CategoricalDtype."""
    df = _minimal_df()
    result = _transform_partition(df)
    assert isinstance(result["WellStatus"].dtype, pd.CategoricalDtype)


@pytest.mark.unit
def test_transform_partition_meta_consistency(tmp_path: Path, config_fixture: dict) -> None:
    """Meta from _transform_partition on empty DataFrame matches result columns."""
    from cogcc_pipeline.ingest import make_delayed_df
    from tests.conftest import _make_raw_csv_content

    csv_path = tmp_path / "test.csv"
    csv_path.write_text(_make_raw_csv_content(n_rows=3), encoding="utf-8")
    ddf = make_delayed_df(csv_path, config_fixture)

    meta = _transform_partition(ddf._meta.copy())
    transformed = ddf.map_partitions(_transform_partition, meta=meta)

    assert list(meta.columns) == list(transformed._meta.columns)


# ---------------------------------------------------------------------------
# check_well_completeness
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_check_well_completeness_no_gaps() -> None:
    """Well with 12 consecutive monthly records → gap_count == 0."""
    well_id = "05-01-12345-00"
    dates = pd.date_range("2021-01-01", periods=12, freq="MS")
    df = pd.DataFrame(
        {
            "well_id": pd.array([well_id] * 12, dtype=pd.StringDtype()),
            "production_date": dates,
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    summary = check_well_completeness(ddf)
    assert summary.loc[well_id, "gap_count"] == 0


@pytest.mark.unit
def test_check_well_completeness_one_gap(caplog: pytest.LogCaptureFixture) -> None:
    """Well missing March 2021 → gap_count == 1 and WARNING logged."""
    well_id = "05-01-12345-00"
    months = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12]  # missing month 3
    dates = [pd.Timestamp(f"2021-{m:02d}-01") for m in months]
    df = pd.DataFrame(
        {
            "well_id": pd.array([well_id] * len(dates), dtype=pd.StringDtype()),
            "production_date": dates,
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.transform"):
        summary = check_well_completeness(ddf)
    assert summary.loc[well_id, "gap_count"] == 1
    assert any("gap" in r.message.lower() for r in caplog.records)


@pytest.mark.unit
def test_check_well_completeness_two_wells() -> None:
    """Two wells → summary DataFrame has two rows."""
    df = pd.DataFrame(
        {
            "well_id": pd.array(["W1", "W1", "W2", "W2"], dtype=pd.StringDtype()),
            "production_date": pd.to_datetime(
                ["2021-01-01", "2021-02-01", "2021-01-01", "2021-02-01"]
            ),
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    summary = check_well_completeness(ddf)
    assert len(summary) == 2


# ---------------------------------------------------------------------------
# check_data_integrity
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_check_data_integrity_no_discrepancy(caplog: pytest.LogCaptureFixture) -> None:
    """Matching interim and processed → no WARNING logged."""
    df = pd.DataFrame(
        {
            "well_id": pd.array(["W1", "W1"], dtype=pd.StringDtype()),
            "OilProduced": pd.array([100.0, 200.0], dtype=pd.Float64Dtype()),
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.transform"):
        check_data_integrity(ddf, ddf)
    warn_messages = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "discrepancy" in r.message.lower()
    ]
    assert len(warn_messages) == 0


@pytest.mark.unit
def test_check_data_integrity_with_discrepancy(caplog: pytest.LogCaptureFixture) -> None:
    """Processed with different OilProduced sum → WARNING logged."""
    interim_df = pd.DataFrame(
        {
            "well_id": pd.array(["W1"], dtype=pd.StringDtype()),
            "OilProduced": pd.array([1000.0], dtype=pd.Float64Dtype()),
        }
    )
    processed_df = pd.DataFrame(
        {
            "well_id": pd.array(["W1"], dtype=pd.StringDtype()),
            "OilProduced": pd.array([1.0], dtype=pd.Float64Dtype()),
        }
    )
    interim_ddf = dd.from_pandas(interim_df, npartitions=1)
    processed_ddf = dd.from_pandas(processed_df, npartitions=1)

    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.transform"):
        check_data_integrity(interim_ddf, processed_ddf, sample_n=1)
    warn_messages = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "discrepancy" in r.message.lower()
    ]
    assert len(warn_messages) >= 1
