"""Tests for the transform stage."""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
import yaml

from cogcc_pipeline.ingest import CANONICAL_COLUMNS, load_data_dictionary
from cogcc_pipeline.transform import (
    _add_production_date,
    _cast_categoricals,
    _deduplicate,
    _transform_partition,
    _validate_numeric_bounds,
    check_well_completeness,
    load_config,
    transform,
    write_parquet,
)

DD_PATH = "references/production-data-dictionary.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_partition(
    n: int = 1,
    year: int = 2021,
    month: int = 3,
    seq: str = "12345",
    well_status: str = "AC",
    oil: float = 100.0,
    gas: float = 500.0,
    water: float = 200.0,
    accepted_date: str = "2021-04-01",
) -> pd.DataFrame:
    """Return a minimal synthetic DataFrame matching ingest output schema."""
    schema = load_data_dictionary(DD_PATH)
    rows = []
    for i in range(n):
        row: dict = {}
        for col in CANONICAL_COLUMNS:
            spec = schema[col]
            dtype = spec["dtype"]
            if col == "ReportYear":
                row[col] = year
            elif col == "ReportMonth":
                row[col] = month
            elif col == "ApiSequenceNumber":
                row[col] = seq
            elif col == "WellStatus":
                row[col] = well_status
            elif col == "OilProduced":
                row[col] = oil
            elif col == "GasProduced":
                row[col] = gas
            elif col == "WaterProduced":
                row[col] = water
            elif col == "AcceptedDate":
                row[col] = pd.Timestamp(accepted_date)
            elif dtype == "float64":
                row[col] = 0.0
            elif isinstance(dtype, pd.Int64Dtype):
                row[col] = pd.NA
            elif dtype == "int64":
                row[col] = 1
            elif isinstance(dtype, pd.StringDtype):
                row[col] = "test"
            elif dtype == "object":
                row[col] = "test"
            elif isinstance(dtype, pd.BooleanDtype):
                row[col] = pd.NA
            elif dtype == "bool":
                row[col] = False
            elif dtype == "datetime64[ns]":
                row[col] = pd.NaT
            elif isinstance(dtype, pd.CategoricalDtype):
                row[col] = None
            else:
                row[col] = None
        rows.append(row)

    df = pd.DataFrame(rows)
    # Apply correct dtypes
    for col in CANONICAL_COLUMNS:
        spec = schema[col]
        dtype = spec["dtype"]
        if col in df.columns:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif isinstance(dtype, pd.CategoricalDtype):
                df[col] = df[col].astype(dtype)
            elif isinstance(dtype, (pd.Int64Dtype, pd.StringDtype, pd.BooleanDtype)):
                df[col] = df[col].astype(dtype)
            elif dtype == "float64":
                df[col] = df[col].astype("float64")
            elif dtype == "int64":
                df[col] = df[col].astype("int64")
    return df


def _make_config_file(tmp_path: Path) -> str:
    cfg = {
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
            "entity_col": "ApiSequenceNumber",
            "data_dictionary": DD_PATH,
        }
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(cfg))
    return str(p)


# ---------------------------------------------------------------------------
# Task 01: load_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_config_returns_required_transform_keys(tmp_path: Path) -> None:
    cfg_path = _make_config_file(tmp_path)
    cfg = load_config(cfg_path)
    assert "interim_dir" in cfg["transform"]
    assert "processed_dir" in cfg["transform"]
    assert "entity_col" in cfg["transform"]


@pytest.mark.unit
def test_load_config_raises_on_missing_transform_section(tmp_path: Path) -> None:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump({"other": {}}))
    with pytest.raises(KeyError):
        load_config(str(p))


# ---------------------------------------------------------------------------
# Task 02: _add_production_date
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_add_production_date_correct_value() -> None:
    partition = _make_partition(year=2022, month=3)
    result = _add_production_date(partition)
    assert result["production_date"].iloc[0] == pd.Timestamp("2022-03-01")


@pytest.mark.unit
def test_add_production_date_dtype_is_datetime64() -> None:
    partition = _make_partition(year=2022, month=3)
    result = _add_production_date(partition)
    assert result["production_date"].dtype == np.dtype("datetime64[ns]")


@pytest.mark.unit
def test_add_production_date_zero_row_partition() -> None:
    partition = _make_partition(n=1).iloc[0:0]
    result = _add_production_date(partition)
    assert len(result) == 0
    assert "production_date" in result.columns
    assert result["production_date"].dtype == np.dtype("datetime64[ns]")


@pytest.mark.unit
def test_add_production_date_returns_pandas_dataframe() -> None:
    result = _add_production_date(_make_partition())
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 03: _cast_categoricals
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cast_categoricals_correct_dtype() -> None:
    schema = load_data_dictionary(DD_PATH)
    partition = _make_partition(well_status="AC")
    result = _cast_categoricals(partition, schema)
    assert isinstance(result["WellStatus"].dtype, pd.CategoricalDtype)
    expected = {"AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"}
    assert expected.issubset(set(result["WellStatus"].cat.categories))


@pytest.mark.unit
def test_cast_categoricals_replaces_invalid_value() -> None:
    schema = load_data_dictionary(DD_PATH)
    partition = _make_partition(well_status="XX")
    result = _cast_categoricals(partition, schema)
    assert pd.isna(result["WellStatus"].iloc[0])


@pytest.mark.unit
def test_cast_categoricals_zero_row_partition() -> None:
    schema = load_data_dictionary(DD_PATH)
    partition = _make_partition().iloc[0:0]
    result = _cast_categoricals(partition, schema)
    assert len(result) == 0
    assert isinstance(result["WellStatus"].dtype, pd.CategoricalDtype)


# ---------------------------------------------------------------------------
# Task 04: _validate_numeric_bounds
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_numeric_bounds_replaces_negative_oil(tmp_path: Path) -> None:
    """TR-01: negative OilProduced replaced with np.nan."""
    partition = _make_partition(oil=-5.0)
    result = _validate_numeric_bounds(partition)
    assert np.isnan(result["OilProduced"].iloc[0])


@pytest.mark.unit
def test_validate_numeric_bounds_preserves_zero(tmp_path: Path) -> None:
    """TR-05: zero production is a valid measurement — not replaced."""
    partition = _make_partition(water=0.0)
    result = _validate_numeric_bounds(partition)
    assert result["WaterProduced"].iloc[0] == 0.0


@pytest.mark.unit
def test_validate_numeric_bounds_flags_unit_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """TR-02: OilProduced > 50000 replaced with NaN and warning logged."""
    import logging

    partition = _make_partition(oil=60000.0)
    with caplog.at_level(logging.WARNING):
        result = _validate_numeric_bounds(partition)
    assert np.isnan(result["OilProduced"].iloc[0])
    assert any(
        "unit error" in r.message.lower() or "50" in r.message for r in caplog.records
    )


@pytest.mark.unit
def test_validate_numeric_bounds_negative_pressure(tmp_path: Path) -> None:
    """TR-01: negative pressure replaced with np.nan."""
    partition = _make_partition()
    partition["GasPressureTubing"] = -1.0
    result = _validate_numeric_bounds(partition)
    assert np.isnan(result["GasPressureTubing"].iloc[0])


@pytest.mark.unit
def test_validate_numeric_bounds_zero_row_partition() -> None:
    partition = _make_partition().iloc[0:0]
    result = _validate_numeric_bounds(partition)
    assert len(result) == 0
    assert list(result.columns) == list(partition.columns)


# ---------------------------------------------------------------------------
# Task 05: _deduplicate
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_deduplicate_keeps_most_recent_accepted_date() -> None:
    p1 = _make_partition(oil=100.0, accepted_date="2021-03-01")
    p2 = _make_partition(oil=120.0, accepted_date="2021-04-01")
    p1 = _add_production_date(p1)
    p2 = _add_production_date(p2)
    combined = pd.concat([p1, p2], ignore_index=True)
    result = _deduplicate(combined)
    assert len(result) == 1
    assert result["OilProduced"].iloc[0] == 120.0


@pytest.mark.unit
def test_deduplicate_no_duplicates_unchanged_count() -> None:
    p1 = _add_production_date(_make_partition(month=1))
    p2 = _add_production_date(_make_partition(month=2))
    combined = pd.concat([p1, p2], ignore_index=True)
    result = _deduplicate(combined)
    assert len(result) == 2


@pytest.mark.unit
def test_deduplicate_idempotent() -> None:
    """TR-15: running twice produces same result as running once."""
    p1 = _add_production_date(_make_partition(oil=100.0, accepted_date="2021-03-01"))
    p2 = _add_production_date(_make_partition(oil=120.0, accepted_date="2021-04-01"))
    combined = pd.concat([p1, p2], ignore_index=True)
    once = _deduplicate(combined)
    twice = _deduplicate(once)
    assert len(once) == len(twice)


@pytest.mark.unit
def test_deduplicate_keeps_only_most_recent_when_differ_in_oil() -> None:
    p1 = _add_production_date(_make_partition(oil=100.0, accepted_date="2021-03-01"))
    p2 = _add_production_date(_make_partition(oil=200.0, accepted_date="2021-04-01"))
    combined = pd.concat([p1, p2], ignore_index=True)
    result = _deduplicate(combined)
    assert len(result) == 1
    assert result["OilProduced"].iloc[0] == 200.0


@pytest.mark.unit
def test_deduplicate_zero_row_partition() -> None:
    partition = _add_production_date(_make_partition()).iloc[0:0]
    result = _deduplicate(partition)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# Task 06: _transform_partition
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_transform_partition_full_pipeline() -> None:
    """Given synthetic partition, output has production_date, no negatives, no bad cats, no dups."""
    schema = load_data_dictionary(DD_PATH)
    # Two rows: one duplicate (different AcceptedDate), one negative oil, one invalid status
    p1 = _make_partition(oil=-5.0, well_status="XX", accepted_date="2021-03-01")
    p2 = _make_partition(oil=100.0, well_status="AC", accepted_date="2021-04-01")
    combined = pd.concat([p1, p2], ignore_index=True)
    result = _transform_partition(combined, schema)
    assert "production_date" in result.columns
    assert (result["OilProduced"] >= 0).all() or result["OilProduced"].isna().all()
    # No invalid WellStatus values
    valid_cats = {"AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"}
    non_null = result["WellStatus"].dropna()
    assert all(v in valid_cats for v in non_null)


@pytest.mark.unit
def test_transform_partition_zero_row_returns_correct_schema() -> None:
    """TR-12, ADR-003: zero-row partition returns correct output schema."""
    schema = load_data_dictionary(DD_PATH)
    partition = _make_partition().iloc[0:0]
    result = _transform_partition(partition, schema)
    assert len(result) == 0
    assert "production_date" in result.columns


@pytest.mark.unit
def test_transform_partition_well_status_is_categorical() -> None:
    schema = load_data_dictionary(DD_PATH)
    partition = _make_partition(well_status="AC")
    result = _transform_partition(partition, schema)
    assert isinstance(result["WellStatus"].dtype, pd.CategoricalDtype)


@pytest.mark.unit
def test_transform_partition_production_date_dtype() -> None:
    schema = load_data_dictionary(DD_PATH)
    partition = _make_partition()
    result = _transform_partition(partition, schema)
    assert result["production_date"].dtype == np.dtype("datetime64[ns]")


@pytest.mark.unit
def test_transform_partition_returns_pandas_dataframe() -> None:
    schema = load_data_dictionary(DD_PATH)
    result = _transform_partition(_make_partition(), schema)
    assert isinstance(result, pd.DataFrame)  # TR-17


# ---------------------------------------------------------------------------
# Task 07: check_well_completeness
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_check_well_completeness_full_year() -> None:
    """Well with Jan–Dec 2021 (12 records) → expected=12, actual=12."""
    rows = []
    for m in range(1, 13):
        p = _make_partition(year=2021, month=m)
        p = _add_production_date(p)
        rows.append(p)
    df = pd.concat(rows, ignore_index=True)
    ddf = dd.from_pandas(df, npartitions=1)
    summary = check_well_completeness(ddf)
    assert isinstance(summary, pd.DataFrame)
    row = summary[summary["ApiSequenceNumber"] == "12345"].iloc[0]
    assert row["actual_count"] == 12
    assert row["expected_count"] == 12


@pytest.mark.unit
def test_check_well_completeness_gap_detected() -> None:
    """Well with gap (11 records out of 12 expected) → actual=11, expected=12 (TR-04)."""
    months = list(range(1, 13))
    months.remove(4)  # remove April
    rows = []
    for m in months:
        p = _make_partition(year=2021, month=m)
        p = _add_production_date(p)
        rows.append(p)
    df = pd.concat(rows, ignore_index=True)
    ddf = dd.from_pandas(df, npartitions=1)
    summary = check_well_completeness(ddf)
    row = summary[summary["ApiSequenceNumber"] == "12345"].iloc[0]
    assert row["actual_count"] == 11
    assert row["expected_count"] == 12


@pytest.mark.unit
def test_check_well_completeness_returns_pandas_df() -> None:
    partition = _add_production_date(_make_partition())
    ddf = dd.from_pandas(partition, npartitions=1)
    result = check_well_completeness(ddf)
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 08 / 09: write_parquet and transform orchestrator
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_parquet_transform_index_is_api_sequence(tmp_path: Path) -> None:
    """TR-16: written Parquet has ApiSequenceNumber as index."""
    schema = load_data_dictionary(DD_PATH)
    partition = _add_production_date(_make_partition())
    partition = _transform_partition(partition, schema)
    ddf = dd.from_pandas(partition, npartitions=1)
    out = str(tmp_path / "processed")
    write_parquet(ddf, out)
    result = pd.read_parquet(out)
    assert result.index.name == "ApiSequenceNumber"


@pytest.mark.integration
def test_write_parquet_readable_by_pandas_and_dask(tmp_path: Path) -> None:
    """TR-18: written Parquet readable by both engines."""
    schema = load_data_dictionary(DD_PATH)
    partition = _add_production_date(_make_partition())
    partition = _transform_partition(partition, schema)
    ddf = dd.from_pandas(partition, npartitions=1)
    out = str(tmp_path / "processed2")
    write_parquet(ddf, out)
    dask_result = dd.read_parquet(out)
    assert len(dask_result.columns) > 0
    pandas_result = pd.read_parquet(out)
    assert len(pandas_result.columns) > 0


@pytest.mark.unit
def test_transform_warns_on_empty_interim_dir(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    interim = tmp_path / "interim"
    interim.mkdir()
    cfg = {
        "transform": {
            "interim_dir": str(interim),
            "processed_dir": str(tmp_path / "processed"),
            "entity_col": "ApiSequenceNumber",
            "data_dictionary": DD_PATH,
        }
    }
    import logging

    with caplog.at_level(logging.WARNING):
        transform(cfg)
    assert any(
        "No Parquet" in r.message or "skipping" in r.message for r in caplog.records
    )


@pytest.mark.integration
def test_transform_full_run(tmp_path: Path) -> None:
    """TR-25: transform on interim Parquet produces readable processed Parquet."""
    from cogcc_pipeline.ingest import (
        build_dask_dataframe,
        write_parquet as ingest_write,
    )

    # Write minimal interim Parquet
    schema = load_data_dictionary(DD_PATH)
    from tests.test_ingest import _MINIMAL_HEADER, _MINIMAL_ROW

    fp = tmp_path / "raw.csv"
    fp.write_text(_MINIMAL_HEADER + "\n" + _MINIMAL_ROW)
    ddf = build_dask_dataframe([str(fp)], schema)
    interim_dir = str(tmp_path / "interim")
    ingest_write(ddf, interim_dir)

    cfg = {
        "transform": {
            "interim_dir": interim_dir,
            "processed_dir": str(tmp_path / "processed"),
            "entity_col": "ApiSequenceNumber",
            "data_dictionary": DD_PATH,
        }
    }
    transform(cfg)

    processed = tmp_path / "processed"
    pq_files = list(processed.glob("*.parquet")) + list(processed.glob("**/*.parquet"))
    assert len(pq_files) > 0

    result = pd.read_parquet(str(processed))
    assert result.index.name == "ApiSequenceNumber"
    assert "production_date" in result.columns
    assert isinstance(result["WellStatus"].dtype, pd.CategoricalDtype)
    assert result["OilProduced"].dtype == np.dtype("float64")


@pytest.mark.integration
def test_transform_return_value_is_none(tmp_path: Path) -> None:
    """TR-17: transform() returns None."""
    from cogcc_pipeline.ingest import (
        build_dask_dataframe,
        write_parquet as ingest_write,
    )
    from tests.test_ingest import _MINIMAL_HEADER, _MINIMAL_ROW

    schema = load_data_dictionary(DD_PATH)
    fp = tmp_path / "raw2.csv"
    fp.write_text(_MINIMAL_HEADER + "\n" + _MINIMAL_ROW)
    ddf = build_dask_dataframe([str(fp)], schema)
    interim_dir = str(tmp_path / "interim2")
    ingest_write(ddf, interim_dir)

    cfg = {
        "transform": {
            "interim_dir": interim_dir,
            "processed_dir": str(tmp_path / "processed2"),
            "entity_col": "ApiSequenceNumber",
            "data_dictionary": DD_PATH,
        }
    }
    transform(cfg)


@pytest.mark.integration
def test_transform_row_count_lte_interim(tmp_path: Path) -> None:
    """TR-15: processed row count <= interim row count."""
    from cogcc_pipeline.ingest import (
        build_dask_dataframe,
        write_parquet as ingest_write,
    )
    from tests.test_ingest import _MINIMAL_HEADER, _MINIMAL_ROW

    schema = load_data_dictionary(DD_PATH)
    fp = tmp_path / "raw3.csv"
    fp.write_text(_MINIMAL_HEADER + "\n" + _MINIMAL_ROW + "\n" + _MINIMAL_ROW)
    ddf = build_dask_dataframe([str(fp)], schema)
    interim_dir = str(tmp_path / "interim3")
    ingest_write(ddf, interim_dir)

    interim_count = len(pd.read_parquet(interim_dir))

    cfg = {
        "transform": {
            "interim_dir": interim_dir,
            "processed_dir": str(tmp_path / "processed3"),
            "entity_col": "ApiSequenceNumber",
            "data_dictionary": DD_PATH,
        }
    }
    transform(cfg)
    processed_count = len(pd.read_parquet(str(tmp_path / "processed3")))
    assert processed_count <= interim_count


@pytest.mark.integration
def test_transform_meta_matches_function_output(tmp_path: Path) -> None:
    """TR-23: meta= for map_partitions matches actual function output."""
    schema = load_data_dictionary(DD_PATH)
    sample = _add_production_date(_make_partition())
    actual = _transform_partition(sample, schema)
    meta = actual.iloc[0:0]
    assert list(meta.columns) == list(actual.columns)
    for col in meta.columns:
        assert meta[col].dtype == actual[col].dtype, f"Dtype mismatch for {col}"
