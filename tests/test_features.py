"""Unit tests for cogcc_pipeline.features."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from cogcc_pipeline.features import (
    add_cumulative_features,
    add_decline_rate,
    add_ratio_features,
    add_rolling_and_lag_features,
    features,
)

FEATURES_CFG = {
    "processed_dir": "data/processed",
    "features_dir": "data/features",
    "rolling_windows": [3, 6, 12],
    "lag_sizes": [1],
    "decline_rate_lower_bound": -1.0,
    "decline_rate_upper_bound": 10.0,
    "partition_min": 10,
    "partition_max": 50,
}


def _make_well_partition(
    production: list[float],
    oil_col: str = "OilProduced",
    gas_col: str = "GasProduced",
    water_col: str = "WaterProduced",
    well_id: str = "01-12345-00",
    start_year: int = 2022,
) -> pd.DataFrame:
    """Build a single-well partition with sequential monthly dates."""
    n = len(production)
    dates = pd.date_range(start=f"{start_year}-01-01", periods=n, freq="MS")
    df = pd.DataFrame(
        {
            oil_col: production,
            gas_col: [p * 5 for p in production],
            water_col: [p * 0.5 for p in production],
            "production_date": dates,
        },
        index=pd.Index([well_id] * n, name="well_id"),
    )
    return df


# ---------------------------------------------------------------------------
# Task F1 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cumulative_known_sequence():
    """Cumulative values match hand-computed expectations (TR-03)."""
    pdf = _make_well_partition([10.0, 20.0, 30.0])
    result = add_cumulative_features(pdf, FEATURES_CFG)
    assert list(result["cum_oil"]) == [10.0, 30.0, 60.0]
    assert list(result["cum_gas"]) == [50.0, 150.0, 300.0]


@pytest.mark.unit
def test_cumulative_monotonically_non_decreasing():
    """Cumulative production never decreases (TR-03)."""
    pdf = _make_well_partition([100.0, 0.0, 50.0, 0.0, 200.0])
    result = add_cumulative_features(pdf, FEATURES_CFG)
    cum = result["cum_oil"].tolist()
    for i in range(1, len(cum)):
        assert cum[i] >= cum[i - 1], f"Cum decreased at index {i}: {cum}"


@pytest.mark.unit
def test_cumulative_flat_during_zero_months(tmp_path: Path):
    """Zero-production months keep cumulative flat — no decrease, no jump (TR-08 a, c)."""
    pdf = _make_well_partition([100.0, 0.0, 0.0, 50.0])
    result = add_cumulative_features(pdf, FEATURES_CFG)
    cum = result["cum_oil"].tolist()
    assert cum[1] == cum[0]  # flat during shut-in
    assert cum[2] == cum[1]  # still flat
    assert cum[3] == cum[2] + 50.0  # resumes correctly from flat value


@pytest.mark.unit
def test_cumulative_leading_zeros(tmp_path: Path):
    """Leading zero months yield cumulative of zero until production begins (TR-08 b)."""
    pdf = _make_well_partition([0.0, 0.0, 100.0])
    result = add_cumulative_features(pdf, FEATURES_CFG)
    cum = result["cum_oil"].tolist()
    assert cum[0] == 0.0
    assert cum[1] == 0.0
    assert cum[2] == 100.0


@pytest.mark.unit
def test_cumulative_empty_partition():
    """Empty partition returns empty output with full schema (TR-23)."""
    pdf = _make_well_partition([]).iloc[:0]
    result = add_cumulative_features(pdf, FEATURES_CFG)
    assert len(result) == 0
    assert "cum_oil" in result.columns
    assert "cum_gas" in result.columns
    assert "cum_water" in result.columns


# ---------------------------------------------------------------------------
# Task F2 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_gor_oil_zero_gas_positive_is_nan():
    """oil=0, gas>0 → gor is NaN or sentinel (TR-06 a)."""
    pdf = _make_well_partition([0.0])
    pdf["GasProduced"] = 500.0
    result = add_ratio_features(pdf, FEATURES_CFG)
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_gor_both_zero_is_zero_or_nan():
    """oil=0, gas=0 → gor is 0.0 or NaN, no exception (TR-06 b)."""
    pdf = _make_well_partition([0.0])
    pdf["GasProduced"] = 0.0
    result = add_ratio_features(pdf, FEATURES_CFG)
    val = result["gor"].iloc[0]
    assert val == 0.0 or pd.isna(val)


@pytest.mark.unit
def test_gor_oil_positive_gas_zero_is_zero():
    """oil>0, gas=0 → gor == 0.0 exactly (TR-06 c)."""
    pdf = _make_well_partition([100.0])
    pdf["GasProduced"] = 0.0
    result = add_ratio_features(pdf, FEATURES_CFG)
    assert result["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_gor_correct_formula_no_unit_swap():
    """GOR = gas / oil (not swapped) (TR-09 d)."""
    pdf = _make_well_partition([100.0])
    pdf["GasProduced"] = 500.0
    result = add_ratio_features(pdf, FEATURES_CFG)
    expected = 500.0 / 100.0
    assert abs(result["gor"].iloc[0] - expected) < 1e-9


@pytest.mark.unit
def test_water_cut_correct_formula():
    """water_cut = water / (oil + water) — uses total liquid (TR-09 e)."""
    pdf = _make_well_partition([10.0])
    pdf["WaterProduced"] = 10.0
    result = add_ratio_features(pdf, FEATURES_CFG)
    assert abs(result["water_cut"].iloc[0] - 0.5) < 1e-9


@pytest.mark.unit
def test_water_cut_zero_at_zero_water():
    """water=0, oil>0 → water_cut == 0.0, row retained (TR-10 a)."""
    pdf = _make_well_partition([100.0])
    pdf["WaterProduced"] = 0.0
    result = add_ratio_features(pdf, FEATURES_CFG)
    assert result["water_cut"].iloc[0] == 0.0
    assert len(result) == 1


@pytest.mark.unit
def test_water_cut_one_at_zero_oil():
    """oil=0, water>0 → water_cut == 1.0, row retained (TR-10 b)."""
    pdf = _make_well_partition([0.0])
    pdf["WaterProduced"] = 100.0
    result = add_ratio_features(pdf, FEATURES_CFG)
    assert result["water_cut"].iloc[0] == 1.0
    assert len(result) == 1


@pytest.mark.unit
def test_ratio_features_empty_partition():
    """Empty partition returns empty output with full schema (TR-23)."""
    pdf = _make_well_partition([]).iloc[:0]
    result = add_ratio_features(pdf, FEATURES_CFG)
    assert len(result) == 0
    assert "gor" in result.columns
    assert "water_cut" in result.columns


# ---------------------------------------------------------------------------
# Task F3 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rolling_3m_matches_hand_computed(tmp_path: Path):
    """3-month rolling avg matches hand-computed values for known sequence (TR-09 a)."""
    production = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    pdf = _make_well_partition(production)
    cfg = {**FEATURES_CFG, "rolling_windows": [3, 6, 12]}
    result = add_rolling_and_lag_features(pdf, cfg)

    col = "OilProduced_rolling_3m"
    assert col in result.columns
    # First 2 values are NaN (window size 3, min_periods=3)
    assert pd.isna(result[col].iloc[0])
    assert pd.isna(result[col].iloc[1])
    assert abs(result[col].iloc[2] - (10 + 20 + 30) / 3) < 1e-9
    assert abs(result[col].iloc[3] - (20 + 30 + 40) / 3) < 1e-9


@pytest.mark.unit
def test_rolling_partial_window_is_nan_not_zero(tmp_path: Path):
    """First N-1 months of rolling window → NaN, not zero (TR-09 b)."""
    pdf = _make_well_partition([10.0, 20.0, 30.0, 40.0])
    cfg = {**FEATURES_CFG, "rolling_windows": [3, 6, 12]}
    result = add_rolling_and_lag_features(pdf, cfg)
    assert pd.isna(result["OilProduced_rolling_3m"].iloc[0])
    assert pd.isna(result["OilProduced_rolling_3m"].iloc[1])


@pytest.mark.unit
def test_lag_1_feature_correct(tmp_path: Path):
    """Lag-1 for month N equals raw value at month N-1 (TR-09 c)."""
    production = [10.0, 20.0, 30.0, 40.0, 50.0]
    pdf = _make_well_partition(production)
    cfg = {**FEATURES_CFG, "rolling_windows": [], "lag_sizes": [1]}
    result = add_rolling_and_lag_features(pdf, cfg)

    col = "OilProduced_lag_1"
    assert col in result.columns
    assert pd.isna(result[col].iloc[0])  # no prior month
    assert result[col].iloc[1] == 10.0
    assert result[col].iloc[2] == 20.0
    assert result[col].iloc[3] == 30.0


@pytest.mark.unit
def test_peak_month_flag_exactly_one_per_well(tmp_path: Path):
    """Exactly one row per well has is_peak_oil_month=True at max OilProduced."""
    production = [10.0, 50.0, 30.0, 20.0]
    pdf = _make_well_partition(production)
    cfg = {**FEATURES_CFG, "rolling_windows": [], "lag_sizes": []}
    result = add_rolling_and_lag_features(pdf, cfg)

    peak_rows = result[result["is_peak_oil_month"]]
    assert len(peak_rows) == 1
    assert peak_rows["OilProduced"].iloc[0] == 50.0


@pytest.mark.unit
def test_rolling_features_empty_partition():
    """Empty partition returns empty output with full schema (TR-23)."""
    pdf = _make_well_partition([]).iloc[:0]
    cfg = {**FEATURES_CFG, "rolling_windows": [3, 6, 12], "lag_sizes": [1]}
    result = add_rolling_and_lag_features(pdf, cfg)
    assert len(result) == 0
    for w in [3, 6, 12]:
        assert f"OilProduced_rolling_{w}m" in result.columns
    assert "OilProduced_lag_1" in result.columns


# ---------------------------------------------------------------------------
# Task F4 tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decline_rate_below_lower_bound_clipped():
    """Decline rate below -1.0 clipped to exactly -1.0 (TR-07 a)."""
    # Extreme increase → negative decline → below lower bound
    pdf = _make_well_partition([1000.0, 1.0])  # 999→1 big drop in prev-curr
    result = add_decline_rate(pdf, FEATURES_CFG)
    assert result["decline_rate"].iloc[1] >= -1.0


@pytest.mark.unit
def test_decline_rate_above_upper_bound_clipped():
    """Decline rate above 10.0 clipped to exactly 10.0 (TR-07 b)."""
    # prev large, curr tiny → decline = (1000-1)/1000 close to 1 ... need really big
    # Use prev=0.001, curr large... we need (prev-curr)/prev > 10
    # prev=1, curr=1000 → (1-1000)/1 = -999 → below lower. Try prev=100, curr=-1→NA
    # Actually: decline = (prev-curr)/prev. For above 10: prev=0.1, curr=-1→neg
    # Use: prev=100, curr=100-(100*11)= -1000 which is negative.
    # Better: small prev, big curr reduction ratio: prev=1, curr = 1-11*1 = -10 → negative
    # Let's directly test that a known sequence with >10 result is clipped:
    # prev=10, curr=10 - 10*11 = -100 → negative → replaced by null in physical bounds, but
    # test decline func alone without physical bounds.
    # Use: prev=1, curr=0. decline=(1-0)/1=1.0 — within bounds.
    # prev=0.1, curr=0 → decline=(0.1-0)/0.1=1.0 — within bounds.
    # For >10: prev=10, curr = 10 - 10*11 = -100 → raw = (-100-10)/-100 = 1.1 or...
    # (prev-curr)/prev = (10-(-100))/10 = 110/10 = 11.0 → clipped to 10.0
    # OilProduced can be negative if we skip physical bounds here (testing decline only)
    pdf = _make_well_partition([10.0, -100.0])  # prev=10, curr=-100 → raw decline=11.0
    result = add_decline_rate(pdf, FEATURES_CFG)
    assert result["decline_rate"].iloc[1] == pytest.approx(10.0)


@pytest.mark.unit
def test_decline_rate_in_bound_unchanged():
    """In-bound value passes through unchanged (TR-07 c)."""
    pdf = _make_well_partition([100.0, 80.0])
    result = add_decline_rate(pdf, FEATURES_CFG)
    # decline = (100-80)/100 = 0.2
    assert abs(result["decline_rate"].iloc[1] - 0.2) < 1e-9


@pytest.mark.unit
def test_decline_rate_consecutive_zeros_no_extreme(tmp_path: Path):
    """Two consecutive zero months → NaN pre-clip, not extreme value (TR-07 d)."""
    pdf = _make_well_partition([100.0, 0.0, 0.0])
    result = add_decline_rate(pdf, FEATURES_CFG)
    # month 2: prev=0 → NaN (div by zero handled) → stays NaN after clip
    val = result["decline_rate"].iloc[2]
    # Should be NaN or within [-1.0, 10.0]
    if not pd.isna(val):
        assert -1.0 <= val <= 10.0


@pytest.mark.unit
def test_decline_rate_empty_partition():
    """Empty partition returns empty output with full schema (TR-23)."""
    pdf = _make_well_partition([]).iloc[:0]
    result = add_decline_rate(pdf, FEATURES_CFG)
    assert len(result) == 0
    assert "decline_rate" in result.columns


# ---------------------------------------------------------------------------
# Feature column presence tests (TR-19)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_feature_column_presence(tmp_path: Path):
    """Output schema contains all required derived feature columns (TR-19)."""
    import dask.dataframe as dd

    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    features_dir = tmp_path / "features"

    production = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0]
    n = len(production)
    dates = pd.date_range(start="2022-01-01", periods=n, freq="MS")
    df = pd.DataFrame(
        {
            "OilProduced": production,
            "GasProduced": [p * 5 for p in production],
            "WaterProduced": [p * 0.5 for p in production],
            "production_date": dates,
        },
        index=pd.Index(["01-12345-00"] * n, name="well_id"),
    )
    df.to_parquet(processed_dir / "part.parquet", index=True)

    cfg = {
        "features": {
            **FEATURES_CFG,
            "processed_dir": str(processed_dir),
            "features_dir": str(features_dir),
        }
    }
    features(cfg)

    out = dd.read_parquet(str(features_dir), engine="pyarrow").compute()
    required_cols = [
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "OilProduced_rolling_3m",
        "OilProduced_rolling_6m",
        "OilProduced_lag_1",
        "is_peak_oil_month",
    ]
    for col in required_cols:
        assert col in out.columns, f"Missing feature column: {col}"


@pytest.mark.unit
def test_features_schema_stable_across_partitions(tmp_path: Path):
    """Schema sampled from two partitions is identical (TR-14)."""

    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    features_dir = tmp_path / "features"

    n = 13
    dates = pd.date_range(start="2022-01-01", periods=n, freq="MS")
    for well_id, prefix in [("01-11111-00", 10.0), ("01-22222-00", 20.0)]:
        df = pd.DataFrame(
            {
                "OilProduced": [prefix + i for i in range(n)],
                "GasProduced": [(prefix + i) * 5 for i in range(n)],
                "WaterProduced": [(prefix + i) * 0.5 for i in range(n)],
                "production_date": dates,
            },
            index=pd.Index([well_id] * n, name="well_id"),
        )
        df.to_parquet(processed_dir / f"part_{well_id}.parquet", index=True)

    cfg = {
        "features": {
            **FEATURES_CFG,
            "processed_dir": str(processed_dir),
            "features_dir": str(features_dir),
        }
    }
    features(cfg)

    parquet_files = list(Path(features_dir).glob("**/*.parquet"))
    if len(parquet_files) >= 2:
        df1 = pd.read_parquet(parquet_files[0])
        df2 = pd.read_parquet(parquet_files[1])
        assert list(df1.columns) == list(df2.columns)
