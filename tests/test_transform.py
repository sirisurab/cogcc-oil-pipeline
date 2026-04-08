"""Tests for cogcc_pipeline/transform.py."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from cogcc_pipeline.transform import (
    WELL_STATUS_CATEGORIES,
    _add_derived_keys,
    _cast_well_status,
    _drop_duplicates,
    _filter_date_range,
    _flag_unit_outliers,
    _handle_negative_production,
    _sort_partition_by_date,
    _standardize_strings,
    build_cleaning_report,
)


def make_partition(**kwargs) -> pd.DataFrame:  # type: ignore[no-untyped-def]
    """Helper to build a minimal partition for transform functions."""
    defaults = {
        "DocNum": pd.array([1], dtype="int64"),
        "ReportMonth": pd.array([3], dtype="int64"),
        "ReportYear": pd.array([2021], dtype="int64"),
        "DaysProduced": pd.array([31], dtype=pd.Int64Dtype()),
        "AcceptedDate": pd.to_datetime(["2021-04-01"]),
        "Revised": pd.array([False], dtype=pd.BooleanDtype()),
        "OpName": pd.array(["CHEVRON USA"], dtype=pd.StringDtype()),
        "OpNumber": pd.array([100], dtype=pd.Int64Dtype()),
        "FacilityId": pd.array([10], dtype=pd.Int64Dtype()),
        "ApiCountyCode": pd.array(["05"], dtype=pd.StringDtype()),
        "ApiSequenceNumber": pd.array(["00123"], dtype=pd.StringDtype()),
        "ApiSidetrack": pd.array(["00"], dtype=pd.StringDtype()),
        "Well": pd.array(["WELL A"], dtype=pd.StringDtype()),
        "WellStatus": pd.array(["PR"], dtype=pd.StringDtype()),
        "FormationCode": pd.array(["NIO"], dtype=pd.StringDtype()),
        "OilProduced": pd.array([100.0], dtype=pd.Float64Dtype()),
        "GasProduced": pd.array([500.0], dtype=pd.Float64Dtype()),
        "WaterProduced": pd.array([50.0], dtype=pd.Float64Dtype()),
        "OilSales": pd.array([95.0], dtype=pd.Float64Dtype()),
        "OilAdjustment": pd.array([0.0], dtype=pd.Float64Dtype()),
        "OilGravity": pd.array([35.0], dtype=pd.Float64Dtype()),
        "GasSales": pd.array([480.0], dtype=pd.Float64Dtype()),
        "GasBtuSales": pd.array([480000.0], dtype=pd.Float64Dtype()),
        "GasUsedOnLease": pd.array([20.0], dtype=pd.Float64Dtype()),
        "GasShrinkage": pd.array([0.0], dtype=pd.Float64Dtype()),
        "GasPressureTubing": pd.array([100.0], dtype=pd.Float64Dtype()),
        "GasPressureCasing": pd.array([95.0], dtype=pd.Float64Dtype()),
        "WaterPressureTubing": pd.array([50.0], dtype=pd.Float64Dtype()),
        "WaterPressureCasing": pd.array([45.0], dtype=pd.Float64Dtype()),
        "FlaredVented": pd.array([0.0], dtype=pd.Float64Dtype()),
        "BomInvent": pd.array([0.0], dtype=pd.Float64Dtype()),
        "EomInvent": pd.array([0.0], dtype=pd.Float64Dtype()),
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


# ---------------------------------------------------------------------------
# T-01: _add_derived_keys
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_add_derived_keys_production_date() -> None:
    pdf = make_partition(
        ReportYear=pd.array([2021], dtype="int64"), ReportMonth=pd.array([3], dtype="int64")
    )
    result = _add_derived_keys(pdf)
    assert result["production_date"].iloc[0] == pd.Timestamp("2021-03-01")


@pytest.mark.unit
def test_add_derived_keys_well_id_format() -> None:
    pdf = make_partition(
        ApiCountyCode=pd.array(["05"], dtype=pd.StringDtype()),
        ApiSequenceNumber=pd.array(["00123"], dtype=pd.StringDtype()),
        ApiSidetrack=pd.array(["00"], dtype=pd.StringDtype()),
    )
    result = _add_derived_keys(pdf)
    assert result["well_id"].iloc[0] == "05-00123-00"


@pytest.mark.unit
def test_add_derived_keys_production_date_dtype() -> None:
    pdf = make_partition()
    result = _add_derived_keys(pdf)
    assert result["production_date"].dtype in ("datetime64[ns]", "datetime64[us]")


@pytest.mark.unit
def test_add_derived_keys_well_id_dtype() -> None:
    pdf = make_partition()
    result = _add_derived_keys(pdf)
    assert result["well_id"].dtype == pd.StringDtype()


# ---------------------------------------------------------------------------
# T-02: _drop_duplicates
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_drop_duplicates_removes_one() -> None:
    pdf = make_partition()
    pdf_dup = pd.concat(
        [pdf, pdf, pdf.assign(DocNum=pd.array([99], dtype="int64"))], ignore_index=True
    )
    result = _drop_duplicates(pdf_dup)
    assert len(result) == 2


@pytest.mark.unit
def test_drop_duplicates_no_dups_unchanged() -> None:
    pdf = make_partition()
    result = _drop_duplicates(pdf)
    assert len(result) == len(pdf)


@pytest.mark.unit
def test_drop_duplicates_dtypes_unchanged() -> None:
    pdf = make_partition()
    result = _drop_duplicates(pdf)
    for col in pdf.columns:
        assert result[col].dtype == pdf[col].dtype


@pytest.mark.unit
def test_tr15_drop_duplicates_idempotent() -> None:
    pdf = make_partition()
    pdf_dup = pd.concat([pdf, pdf], ignore_index=True)
    once = _drop_duplicates(pdf_dup)
    twice = _drop_duplicates(once)
    assert len(once) == len(twice)


# ---------------------------------------------------------------------------
# T-03: _handle_negative_production (TR-01, TR-05)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr01_negative_oil_becomes_na() -> None:
    pdf = make_partition(OilProduced=pd.array([-5.0], dtype=pd.Float64Dtype()))
    result = _handle_negative_production(pdf)
    assert pd.isna(result["OilProduced"].iloc[0])
    assert result["OilProduced_negative"].iloc[0]


@pytest.mark.unit
def test_tr05_zero_oil_preserved() -> None:
    pdf = make_partition(OilProduced=pd.array([0.0], dtype=pd.Float64Dtype()))
    result = _handle_negative_production(pdf)
    assert result["OilProduced"].iloc[0] == 0.0
    assert not result["OilProduced_negative"].iloc[0]


@pytest.mark.unit
def test_oil_na_gives_na_flag() -> None:
    pdf = make_partition(OilProduced=pd.array([pd.NA], dtype=pd.Float64Dtype()))
    result = _handle_negative_production(pdf)
    assert pd.isna(result["OilProduced_negative"].iloc[0])


@pytest.mark.unit
def test_gas_positive_unchanged() -> None:
    pdf = make_partition(GasProduced=pd.array([100.0], dtype=pd.Float64Dtype()))
    result = _handle_negative_production(pdf)
    assert result["GasProduced"].iloc[0] == 100.0
    assert not result["GasProduced_negative"].iloc[0]


# ---------------------------------------------------------------------------
# T-04: _flag_unit_outliers (TR-02)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr02_oil_55000_flagged() -> None:
    pdf = make_partition(OilProduced=pd.array([55000.0], dtype=pd.Float64Dtype()))
    result = _flag_unit_outliers(pdf)
    assert result["OilProduced_unit_flag"].iloc[0]
    assert result["OilProduced"].iloc[0] == 55000.0


@pytest.mark.unit
def test_tr02_oil_49999_not_flagged() -> None:
    pdf = make_partition(OilProduced=pd.array([49999.0], dtype=pd.Float64Dtype()))
    result = _flag_unit_outliers(pdf)
    assert not result["OilProduced_unit_flag"].iloc[0]


@pytest.mark.unit
def test_oil_zero_not_flagged() -> None:
    pdf = make_partition(OilProduced=pd.array([0.0], dtype=pd.Float64Dtype()))
    result = _flag_unit_outliers(pdf)
    assert not result["OilProduced_unit_flag"].iloc[0]


@pytest.mark.unit
def test_oil_na_flag_is_na() -> None:
    pdf = make_partition(OilProduced=pd.array([pd.NA], dtype=pd.Float64Dtype()))
    result = _flag_unit_outliers(pdf)
    assert pd.isna(result["OilProduced_unit_flag"].iloc[0])


# ---------------------------------------------------------------------------
# T-05: _standardize_strings
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_standardize_strings_strips_and_uppercases() -> None:
    pdf = make_partition(OpName=pd.array(["  chevron  usa "], dtype=pd.StringDtype()))
    result = _standardize_strings(pdf)
    assert result["OpName"].iloc[0] == "CHEVRON USA"


@pytest.mark.unit
def test_standardize_strings_preserves_na() -> None:
    pdf = make_partition(OpName=pd.array([pd.NA], dtype=pd.StringDtype()))
    result = _standardize_strings(pdf)
    assert pd.isna(result["OpName"].iloc[0])


@pytest.mark.unit
def test_standardize_strings_double_space() -> None:
    pdf = make_partition(Well=pd.array(["well  name"], dtype=pd.StringDtype()))
    result = _standardize_strings(pdf)
    assert result["Well"].iloc[0] == "WELL NAME"


@pytest.mark.unit
def test_standardize_strings_formation_code() -> None:
    pdf = make_partition(FormationCode=pd.array(["niobrara"], dtype=pd.StringDtype()))
    result = _standardize_strings(pdf)
    assert result["FormationCode"].iloc[0] == "NIOBRARA"


# ---------------------------------------------------------------------------
# T-06: _cast_well_status
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cast_well_status_valid_value() -> None:
    pdf = make_partition(WellStatus=pd.array(["PR"], dtype=pd.StringDtype()))
    result = _cast_well_status(pdf)
    assert hasattr(result["WellStatus"].dtype, "categories")
    assert result["WellStatus"].iloc[0] == "PR"


@pytest.mark.unit
def test_cast_well_status_invalid_becomes_na() -> None:
    pdf = make_partition(WellStatus=pd.array(["UNKNOWN"], dtype=pd.StringDtype()))
    result = _cast_well_status(pdf)
    assert pd.isna(result["WellStatus"].iloc[0])


@pytest.mark.unit
def test_cast_well_status_na_stays_na() -> None:
    pdf = make_partition(WellStatus=pd.array([pd.NA], dtype=pd.StringDtype()))
    result = _cast_well_status(pdf)
    assert pd.isna(result["WellStatus"].iloc[0])


@pytest.mark.unit
def test_cast_well_status_categories() -> None:
    pdf = make_partition(WellStatus=pd.array(["PR"], dtype=pd.StringDtype()))
    result = _cast_well_status(pdf)
    cats = list(result["WellStatus"].cat.categories)
    assert cats == WELL_STATUS_CATEGORIES


# ---------------------------------------------------------------------------
# T-07: _filter_date_range
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_filter_date_range_keeps_correct_rows() -> None:
    pdf = make_partition()
    pdf = _add_derived_keys(pdf)
    extra_rows = []
    for y, m in [(2019, 12), (2020, 1), (2021, 6)]:
        row = make_partition(
            ReportYear=pd.array([y], dtype="int64"), ReportMonth=pd.array([m], dtype="int64")
        )
        extra_rows.append(_add_derived_keys(row))
    combined = pd.concat(extra_rows, ignore_index=True)
    result = _filter_date_range(combined, pd.Timestamp("2020-01-01"))
    assert all(result["production_date"] >= pd.Timestamp("2020-01-01"))
    assert len(result) == 2


@pytest.mark.unit
def test_filter_date_range_all_before_returns_empty() -> None:
    pdf = make_partition(
        ReportYear=pd.array([2019], dtype="int64"), ReportMonth=pd.array([6], dtype="int64")
    )
    pdf = _add_derived_keys(pdf)
    result = _filter_date_range(pdf, pd.Timestamp("2020-01-01"))
    assert len(result) == 0
    assert "production_date" in result.columns


# ---------------------------------------------------------------------------
# T-08: _sort_partition_by_date
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sort_partition_by_date_ascending() -> None:
    rows = []
    for y, m in [(2021, 3), (2021, 1), (2021, 2)]:
        row = make_partition(
            ReportYear=pd.array([y], dtype="int64"), ReportMonth=pd.array([m], dtype="int64")
        )
        rows.append(_add_derived_keys(row))
    pdf = pd.concat(rows, ignore_index=True)
    result = _sort_partition_by_date(pdf)
    dates = list(result["production_date"])
    assert dates == sorted(dates)


@pytest.mark.unit
def test_sort_partition_by_date_no_rows_lost() -> None:
    rows = []
    for m in [3, 1, 2]:
        row = make_partition(
            ReportYear=pd.array([2021], dtype="int64"), ReportMonth=pd.array([m], dtype="int64")
        )
        rows.append(_add_derived_keys(row))
    pdf = pd.concat(rows, ignore_index=True)
    result = _sort_partition_by_date(pdf)
    assert len(result) == len(pdf)


@pytest.mark.unit
def test_tr16_sort_stability() -> None:
    """TR-16: Sorted dates are ascending and no rows are lost."""
    rows = []
    for m in [6, 1, 3, 2]:
        row = make_partition(
            ReportYear=pd.array([2021], dtype="int64"), ReportMonth=pd.array([m], dtype="int64")
        )
        rows.append(_add_derived_keys(row))
    pdf = pd.concat(rows, ignore_index=True)
    result = _sort_partition_by_date(pdf)
    dates = list(result["production_date"])
    assert dates == sorted(dates)
    assert len(result) == len(pdf)


# ---------------------------------------------------------------------------
# T-09: build_cleaning_report
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_cleaning_report_creates_file(tmp_path: Path) -> None:
    stats = {
        "rows_before": 100,
        "rows_after_dedup": 95,
        "rows_after_date_filter": 90,
        "negative_oil_count": 2,
        "negative_gas_count": 1,
        "negative_water_count": 0,
        "unit_flag_oil_count": 3,
        "null_counts_per_column": {"OilProduced": 0},
    }
    output_path = tmp_path / "report.json"
    build_cleaning_report(stats, output_path)
    assert output_path.exists()


@pytest.mark.unit
def test_build_cleaning_report_valid_json(tmp_path: Path) -> None:
    stats = {
        "rows_before": 100,
        "rows_after_dedup": 95,
        "rows_after_date_filter": 90,
        "negative_oil_count": 0,
        "negative_gas_count": 0,
        "negative_water_count": 0,
        "unit_flag_oil_count": 0,
        "null_counts_per_column": {},
    }
    output_path = tmp_path / "report.json"
    build_cleaning_report(stats, output_path)
    data = json.loads(output_path.read_text())
    assert "rows_before" in data
    assert "null_counts_per_column" in data


# ---------------------------------------------------------------------------
# T-11 Domain tests (TR-01, TR-02, TR-04, TR-05, TR-11 through TR-16)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr01_all_negatives_become_na() -> None:
    """TR-01: Negative OilProduced, GasProduced, WaterProduced all become NA."""
    pdf = make_partition(
        OilProduced=pd.array([-10.0], dtype=pd.Float64Dtype()),
        GasProduced=pd.array([-20.0], dtype=pd.Float64Dtype()),
        WaterProduced=pd.array([-5.0], dtype=pd.Float64Dtype()),
    )
    result = _handle_negative_production(pdf)
    assert pd.isna(result["OilProduced"].iloc[0])
    assert pd.isna(result["GasProduced"].iloc[0])
    assert pd.isna(result["WaterProduced"].iloc[0])
    assert result["OilProduced_negative"].iloc[0] == True  # noqa: E712


@pytest.mark.unit
def test_tr04_well_12_records_preserved(sample_cleaned_df: pd.DataFrame) -> None:
    """TR-04: 12 monthly records for a well all preserved through cleaning."""
    rows = []
    for m in range(1, 13):
        row = make_partition(
            ReportYear=pd.array([2021], dtype="int64"),
            ReportMonth=pd.array([m], dtype="int64"),
        )
        rows.append(row)
    pdf = pd.concat(rows, ignore_index=True)
    result = _drop_duplicates(pdf)
    assert len(result) == 12


@pytest.mark.unit
def test_tr11_value_preserved_when_no_cleaning_triggered() -> None:
    """TR-11: OilProduced unchanged when no cleaning rules trigger."""
    pdf = make_partition(OilProduced=pd.array([123.45], dtype=pd.Float64Dtype()))
    result = _handle_negative_production(pdf)
    assert result["OilProduced"].iloc[0] == 123.45


@pytest.mark.unit
def test_tr14_schema_stability_across_partitions() -> None:
    """TR-14: Two partitions have identical columns and dtypes after map functions."""
    pdf1 = make_partition(OilProduced=pd.array([100.0], dtype=pd.Float64Dtype()))
    pdf2 = make_partition(OilProduced=pd.array([200.0], dtype=pd.Float64Dtype()))

    r1 = _handle_negative_production(_drop_duplicates(pdf1))
    r2 = _handle_negative_production(_drop_duplicates(pdf2))

    assert list(r1.columns) == list(r2.columns)
    for col in r1.columns:
        assert r1[col].dtype == r2[col].dtype


@pytest.mark.unit
def test_tr15_row_count_idempotency() -> None:
    """TR-15: Output rows <= input rows; dedup is idempotent."""
    pdf = make_partition()
    pdf_dup = pd.concat([pdf, pdf], ignore_index=True)
    r1 = _drop_duplicates(pdf_dup)
    r2 = _drop_duplicates(r1)
    assert len(r1) <= len(pdf_dup)
    assert len(r2) == len(r1)
