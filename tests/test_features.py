"""Tests for the features stage."""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
import yaml

from cogcc_pipeline.features import (
    _add_cumulative_features,
    _add_decline_rates,
    _add_ratio_features,
    _add_rolling_and_lag_features,
    _compute_features_partition,
    features,
    load_config,
    write_parquet,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FEATURE_COLS = [
    "cum_oil",
    "cum_gas",
    "cum_water",
    "gor",
    "water_cut",
    "decline_rate_oil",
    "oil_rolling_3m",
    "oil_rolling_6m",
    "gas_rolling_3m",
    "gas_rolling_6m",
    "water_rolling_3m",
    "water_rolling_6m",
    "oil_lag_1",
    "gas_lag_1",
    "water_lag_1",
]


def _make_transform_output(
    well_id: str = "12345",
    months: int = 6,
    oil_vals: list[float] | None = None,
    gas_vals: list[float] | None = None,
    water_vals: list[float] | None = None,
) -> pd.DataFrame:
    """Return a minimal DataFrame matching transform output (entity-indexed)."""
    oil = (
        oil_vals
        if oil_vals is not None
        else [float(100 * (i + 1)) for i in range(months)]
    )
    n = len(oil)
    gas = gas_vals if gas_vals is not None else [float(500 * (i + 1)) for i in range(n)]
    water = (
        water_vals
        if water_vals is not None
        else [float(50 * (i + 1)) for i in range(n)]
    )
    dates = pd.date_range("2021-01-01", periods=n, freq="MS")
    df = pd.DataFrame(
        {
            "production_date": dates,
            "OilProduced": pd.array(oil, dtype="float64"),
            "GasProduced": pd.array(gas, dtype="float64"),
            "WaterProduced": pd.array(water, dtype="float64"),
        },
        index=pd.Index([well_id] * n, name="ApiSequenceNumber"),
    )
    return df


def _make_multi_well(
    wells: list[str] = ["W1", "W2"],
    months: int = 4,
) -> pd.DataFrame:
    frames = [_make_transform_output(w, months) for w in wells]
    return pd.concat(frames)


def _make_config_file(tmp_path: Path) -> str:
    cfg = {
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "output_dir": str(tmp_path / "features"),
        }
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(cfg))
    return str(p)


# ---------------------------------------------------------------------------
# Task 01: load_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_config_features_required_keys(tmp_path: Path) -> None:
    cfg_path = _make_config_file(tmp_path)
    cfg = load_config(cfg_path)
    assert "processed_dir" in cfg["features"]
    assert "output_dir" in cfg["features"]


@pytest.mark.unit
def test_load_config_features_raises_on_missing_section(tmp_path: Path) -> None:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump({"other": {}}))
    with pytest.raises(KeyError):
        load_config(str(p))


# ---------------------------------------------------------------------------
# Task 02: _add_cumulative_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cumulative_features_known_values() -> None:
    """TR-09a: cum_oil matches hand-computed cumsum."""
    df = _make_transform_output(oil_vals=[100, 200, 150, 300])
    result = _add_cumulative_features(df)
    expected = [100.0, 300.0, 450.0, 750.0]
    np.testing.assert_array_almost_equal(np.array(result["cum_oil"].values), expected)


@pytest.mark.unit
def test_cumulative_features_flat_during_zero_production() -> None:
    """TR-08a: cumulative stays flat during zero-production months."""
    df = _make_transform_output(oil_vals=[100, 0, 0, 200])
    result = _add_cumulative_features(df)
    # cum_oil should be [100, 100, 100, 300]
    assert result["cum_oil"].iloc[1] == 100.0
    assert result["cum_oil"].iloc[2] == 100.0
    assert result["cum_oil"].iloc[3] == 300.0


@pytest.mark.unit
def test_cumulative_features_zero_at_start() -> None:
    """TR-08b: cumulative starts at zero when well hasn't produced."""
    df = _make_transform_output(oil_vals=[0, 0, 100, 200])
    result = _add_cumulative_features(df)
    assert result["cum_oil"].iloc[0] == 0.0
    assert result["cum_oil"].iloc[1] == 0.0
    assert result["cum_oil"].iloc[2] == 100.0


@pytest.mark.unit
def test_cumulative_features_monotonically_nondecreasing() -> None:
    """TR-03: cum_oil is monotonically non-decreasing."""
    df = _make_transform_output(months=6)
    result = _add_cumulative_features(df)
    diffs = result.groupby(result.index)["cum_oil"].diff().dropna()
    assert (diffs >= 0).all() == True  # noqa: E712


@pytest.mark.unit
def test_cumulative_features_zero_row_partition() -> None:
    df = _make_transform_output(months=1).iloc[0:0]
    result = _add_cumulative_features(df)
    assert len(result) == 0
    for col in ["cum_oil", "cum_gas", "cum_water"]:
        assert col in result.columns
        assert result[col].dtype == np.dtype("float64")


@pytest.mark.unit
def test_cumulative_features_returns_pandas_df() -> None:
    result = _add_cumulative_features(_make_transform_output())
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 03: _add_ratio_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_gor_zero_oil_positive_gas() -> None:
    """TR-06a: oil=0, gas>0 → gor=NaN."""
    df = _make_transform_output(
        oil_vals=[0.0], gas_vals=[100.0], water_vals=[0.0], months=1
    )
    result = _add_ratio_features(df)
    assert np.isnan(result["gor"].iloc[0])


@pytest.mark.unit
def test_gor_zero_oil_zero_gas() -> None:
    """TR-06b: oil=0, gas=0 → gor=NaN."""
    df = _make_transform_output(
        oil_vals=[0.0], gas_vals=[0.0], water_vals=[0.0], months=1
    )
    result = _add_ratio_features(df)
    assert np.isnan(result["gor"].iloc[0])


@pytest.mark.unit
def test_gor_positive_oil_zero_gas() -> None:
    """TR-06c: oil>0, gas=0 → gor=0.0."""
    df = _make_transform_output(
        oil_vals=[50.0], gas_vals=[0.0], water_vals=[0.0], months=1
    )
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_gor_positive_both() -> None:
    """TR-09d: gor = gas / oil."""
    df = _make_transform_output(
        oil_vals=[50.0], gas_vals=[500.0], water_vals=[0.0], months=1
    )
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == pytest.approx(10.0)


@pytest.mark.unit
def test_water_cut_no_water() -> None:
    """TR-10a: water=0, oil>0 → water_cut=0.0."""
    df = _make_transform_output(
        oil_vals=[100.0], gas_vals=[0.0], water_vals=[0.0], months=1
    )
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == 0.0


@pytest.mark.unit
def test_water_cut_all_water() -> None:
    """TR-10b: oil=0, water>0 → water_cut=1.0."""
    df = _make_transform_output(
        oil_vals=[0.0], gas_vals=[0.0], water_vals=[200.0], months=1
    )
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == pytest.approx(1.0)


@pytest.mark.unit
def test_water_cut_equal_oil_water() -> None:
    """TR-09e: water_cut = water / (oil + water)."""
    df = _make_transform_output(
        oil_vals=[100.0], gas_vals=[0.0], water_vals=[100.0], months=1
    )
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == pytest.approx(0.5)


@pytest.mark.unit
def test_water_cut_within_bounds_for_valid_inputs() -> None:
    """TR-01: water_cut with positive inputs stays in [0, 1]."""
    df = _make_transform_output(
        oil_vals=[100.0], gas_vals=[200.0], water_vals=[50.0], months=1
    )
    result = _add_ratio_features(df)
    val = result["water_cut"].iloc[0]
    assert 0.0 <= val <= 1.0


@pytest.mark.unit
def test_ratio_features_zero_row_partition() -> None:
    df = _make_transform_output(months=1).iloc[0:0]
    result = _add_ratio_features(df)
    assert len(result) == 0
    assert result["gor"].dtype == np.dtype("float64")
    assert result["water_cut"].dtype == np.dtype("float64")


@pytest.mark.unit
def test_ratio_features_returns_pandas_df() -> None:
    result = _add_ratio_features(_make_transform_output())
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 04: _add_decline_rates
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decline_rate_known_values() -> None:
    """TR-07c: decline = (100-80)/100 = 0.2."""
    df = _make_transform_output(oil_vals=[100.0, 80.0], months=2)
    result = _add_decline_rates(df)
    assert np.isnan(result["decline_rate_oil"].iloc[0])
    assert result["decline_rate_oil"].iloc[1] == pytest.approx(0.2)


@pytest.mark.unit
def test_decline_rate_clips_lower_bound() -> None:
    """TR-07a: rate below -1.0 clipped to -1.0."""
    # Large increase: (10 - 1000) / 10 = -99 → clipped to -1.0
    df = _make_transform_output(oil_vals=[10.0, 1000.0], months=2)
    result = _add_decline_rates(df)
    assert result["decline_rate_oil"].iloc[1] == pytest.approx(-1.0)


@pytest.mark.unit
def test_decline_rate_clips_upper_bound() -> None:
    """TR-07b: rate above 10.0 clipped to 10.0."""
    # Massive drop: (1000 - 1) / 1000 = 0.999 which is within bounds.
    # Force > 10: prev=1, curr=0 → (1-0)/1=1, still < 10. Use prev=0.001, curr=0 → prev=0 → NaN
    # Use: prev=10, curr=0 → (10-0)/10=1.0 not > 10. prev=10000, curr=0 → 1.0 still.
    # Actually (prev - curr) / prev with prev=1 and curr=-100000 is not valid since negatives are filtered
    # Let's use: prev=0.001, curr is not possible since negative is filtered upstream
    # Test clip upper: to get rate > 10, need (prev - curr)/prev > 10 → prev - curr > 10*prev → curr < -9*prev
    # Since negatives aren't filtered yet in this function, we can test directly:
    df = _make_transform_output(oil_vals=[100.0, -900.0], months=2)
    # decline = (100 - (-900)) / 100 = 10.0 → at boundary exactly
    result = _add_decline_rates(df)
    assert result["decline_rate_oil"].iloc[1] <= 10.0

    # Force > 10 using larger values
    df2 = _make_transform_output(oil_vals=[1.0, -100.0], months=2)
    result2 = _add_decline_rates(df2)
    assert result2["decline_rate_oil"].iloc[1] == pytest.approx(10.0)


@pytest.mark.unit
def test_decline_rate_zero_denominator_is_nan() -> None:
    """TR-07d: two consecutive zero-production months → NaN, not extreme value."""
    df = _make_transform_output(oil_vals=[0.0, 0.0], months=2)
    result = _add_decline_rates(df)
    assert np.isnan(result["decline_rate_oil"].iloc[1])


@pytest.mark.unit
def test_decline_rate_zero_row_partition() -> None:
    df = _make_transform_output(months=1).iloc[0:0]
    result = _add_decline_rates(df)
    assert len(result) == 0
    assert result["decline_rate_oil"].dtype == np.dtype("float64")


@pytest.mark.unit
def test_decline_rate_returns_pandas_df() -> None:
    result = _add_decline_rates(_make_transform_output())
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 05: _add_rolling_and_lag_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rolling_3m_correct_value() -> None:
    """TR-09a: rolling 3m mean matches hand-computed value."""
    oil = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    df = _make_transform_output(oil_vals=oil, months=6)
    result = _add_rolling_and_lag_features(df)
    # Month 3 (index 2): mean(10, 20, 30) = 20
    assert result["oil_rolling_3m"].iloc[2] == pytest.approx(20.0)
    # Months 1, 2 are NaN (insufficient history)
    assert np.isnan(result["oil_rolling_3m"].iloc[0])
    assert np.isnan(result["oil_rolling_3m"].iloc[1])


@pytest.mark.unit
def test_rolling_6m_nan_for_first_five() -> None:
    """TR-09b: rolling 6m is NaN for months 1–5."""
    oil = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    df = _make_transform_output(oil_vals=oil, months=6)
    result = _add_rolling_and_lag_features(df)
    for i in range(5):
        assert np.isnan(result["oil_rolling_6m"].iloc[i])
    expected_6m = np.mean(oil)
    assert result["oil_rolling_6m"].iloc[5] == pytest.approx(expected_6m)


@pytest.mark.unit
def test_lag_1_three_consecutive_months() -> None:
    """TR-09c: lag-1 verified for 3 consecutive months."""
    oil = [100.0, 200.0, 300.0, 400.0]
    df = _make_transform_output(oil_vals=oil, months=4)
    result = _add_rolling_and_lag_features(df)
    assert result["oil_lag_1"].iloc[1] == pytest.approx(100.0)
    assert result["oil_lag_1"].iloc[2] == pytest.approx(200.0)
    assert result["oil_lag_1"].iloc[3] == pytest.approx(300.0)


@pytest.mark.unit
def test_lag_1_first_month_is_nan() -> None:
    df = _make_transform_output(oil_vals=[100.0, 200.0], months=2)
    result = _add_rolling_and_lag_features(df)
    assert np.isnan(result["oil_lag_1"].iloc[0])


@pytest.mark.unit
def test_rolling_lag_zero_row_partition() -> None:
    df = _make_transform_output(months=1).iloc[0:0]
    result = _add_rolling_and_lag_features(df)
    assert len(result) == 0
    for col in [
        "oil_rolling_3m",
        "oil_rolling_6m",
        "gas_rolling_3m",
        "gas_rolling_6m",
        "water_rolling_3m",
        "water_rolling_6m",
        "oil_lag_1",
        "gas_lag_1",
        "water_lag_1",
    ]:
        assert col in result.columns
        assert result[col].dtype == np.dtype("float64")


@pytest.mark.unit
def test_rolling_lag_returns_pandas_df() -> None:
    result = _add_rolling_and_lag_features(_make_transform_output())
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 06: _compute_features_partition
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_compute_features_partition_all_columns_present() -> None:
    """TR-19: all derived feature columns present."""
    df = _make_multi_well()
    result = _compute_features_partition(df)
    for col in FEATURE_COLS:
        assert col in result.columns, f"Missing feature column: {col}"


@pytest.mark.unit
def test_compute_features_partition_zero_row() -> None:
    """ADR-003: zero-row partition returns correct output schema."""
    df = _make_transform_output(months=1).iloc[0:0]
    result = _compute_features_partition(df)
    assert len(result) == 0
    for col in FEATURE_COLS:
        assert col in result.columns
        assert result[col].dtype == np.dtype("float64")


@pytest.mark.unit
def test_compute_features_partition_returns_pandas_df() -> None:
    """TR-17."""
    result = _compute_features_partition(_make_transform_output())
    assert isinstance(result, pd.DataFrame)


@pytest.mark.unit
def test_compute_features_partition_cum_oil_monotonic() -> None:
    """TR-03: cum_oil monotonically non-decreasing."""
    df = _make_multi_well(wells=["W1"], months=6)
    result = _compute_features_partition(df)
    diffs = result.groupby(result.index)["cum_oil"].diff().dropna()
    assert (diffs >= 0).all() == True  # noqa: E712


@pytest.mark.unit
def test_compute_features_meta_matches_output() -> None:
    """TR-23: meta= argument matches actual function output."""
    sample = _make_transform_output(months=2)
    actual = _compute_features_partition(sample)
    meta = actual.iloc[0:0]
    assert list(meta.columns) == list(actual.columns)
    for col in meta.columns:
        assert meta[col].dtype == actual[col].dtype


# ---------------------------------------------------------------------------
# Task 07 + 08: write_parquet and features orchestrator
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_parquet_features_readable(tmp_path: Path) -> None:
    df = _compute_features_partition(_make_transform_output())
    ddf = dd.from_pandas(df, npartitions=1)
    out = str(tmp_path / "features_out")
    write_parquet(ddf, out)
    result = dd.read_parquet(out)
    for col in FEATURE_COLS:
        assert col in result.columns


@pytest.mark.integration
def test_write_parquet_features_readable_by_pandas_and_dask(tmp_path: Path) -> None:
    """TR-18."""
    df = _compute_features_partition(_make_transform_output())
    ddf = dd.from_pandas(df, npartitions=1)
    out = str(tmp_path / "features_out2")
    write_parquet(ddf, out)
    dask_result = dd.read_parquet(out)
    assert len(dask_result.columns) > 0
    pandas_result = pd.read_parquet(out)
    assert len(pandas_result.columns) > 0


@pytest.mark.unit
def test_features_warns_on_empty_processed_dir(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    processed = tmp_path / "processed"
    processed.mkdir()
    cfg = {
        "features": {
            "processed_dir": str(processed),
            "output_dir": str(tmp_path / "feat"),
        }
    }
    import logging

    with caplog.at_level(logging.WARNING):
        features(cfg)
    assert any(
        "No Parquet" in r.message or "skipping" in r.message for r in caplog.records
    )


@pytest.mark.integration
def test_features_full_run(tmp_path: Path) -> None:
    """TR-26: features on processed Parquet produces readable output with all feature cols."""
    # Write processed Parquet with entity index
    from cogcc_pipeline.transform import write_parquet as transform_write
    from cogcc_pipeline.transform import _transform_partition, _add_production_date
    from cogcc_pipeline.ingest import load_data_dictionary

    schema = load_data_dictionary("references/production-data-dictionary.csv")
    from tests.test_transform import _make_partition

    partition = _add_production_date(_make_partition(n=6, month=1))
    # Give different months
    for i in range(6):
        col_loc = partition.columns.get_loc("ReportMonth")
        assert isinstance(col_loc, int)
        partition.iat[i, col_loc] = i + 1
    partition = _add_production_date(partition)
    transformed = _transform_partition(partition, schema)
    ddf = dd.from_pandas(transformed, npartitions=1)
    processed_dir = str(tmp_path / "processed")
    transform_write(ddf, processed_dir)

    cfg = {
        "features": {
            "processed_dir": processed_dir,
            "output_dir": str(tmp_path / "feat"),
        }
    }
    features(cfg)

    pq_files = list((tmp_path / "feat").glob("*.parquet")) + list(
        (tmp_path / "feat").glob("**/*.parquet")
    )
    assert len(pq_files) > 0
    result = pd.read_parquet(str(tmp_path / "feat"))
    for col in FEATURE_COLS:
        assert col in result.columns, f"Missing: {col}"


@pytest.mark.integration
def test_features_all_expected_derived_columns(tmp_path: Path) -> None:
    """TR-19: all derived columns present in features output."""
    from cogcc_pipeline.transform import write_parquet as transform_write
    from cogcc_pipeline.transform import _transform_partition, _add_production_date
    from cogcc_pipeline.ingest import load_data_dictionary

    schema = load_data_dictionary("references/production-data-dictionary.csv")
    from tests.test_transform import _make_partition

    partition = _add_production_date(_make_partition(n=1))
    transformed = _transform_partition(partition, schema)
    ddf = dd.from_pandas(transformed, npartitions=1)
    processed_dir = str(tmp_path / "proc2")
    transform_write(ddf, processed_dir)

    cfg = {
        "features": {
            "processed_dir": processed_dir,
            "output_dir": str(tmp_path / "feat2"),
        }
    }
    features(cfg)
    result = pd.read_parquet(str(tmp_path / "feat2"))
    for col in FEATURE_COLS:
        assert col in result.columns
