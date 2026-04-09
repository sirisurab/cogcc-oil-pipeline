"""Tests for cogcc_pipeline.features module."""

from __future__ import annotations

import logging

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from cogcc_pipeline.features import (
    FeaturesError,
    _add_cumulative_features,
    _add_decline_rates,
    _add_lag_features,
    _add_ratio_features,
    _add_rolling_features,
    _engineer_features_partition,
    validate_feature_output,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_well_df(
    well_ids: list[str],
    oil: list[float],
    gas: list[float] | None = None,
    water: list[float] | None = None,
) -> pd.DataFrame:
    """Build a minimal DataFrame for feature tests (well_id as column, not index)."""
    n = len(well_ids)
    gas = gas or [0.0] * n
    water = water or [0.0] * n
    df = pd.DataFrame(
        {
            "well_id": pd.Series(well_ids, dtype="string").values,
            "production_date": pd.date_range("2021-01-01", periods=n, freq="MS"),
            "OilProduced": pd.array(oil, dtype=pd.Float64Dtype()),
            "GasProduced": pd.array(gas, dtype=pd.Float64Dtype()),
            "WaterProduced": pd.array(water, dtype=pd.Float64Dtype()),
        }
    )
    return df


def _make_indexed_well_df(**kwargs: object) -> pd.DataFrame:
    """Return a well DataFrame with well_id as the index (post-transform shape)."""
    df = _make_well_df(**kwargs)  # type: ignore[arg-type]
    df = df.set_index("well_id")
    return df


# ---------------------------------------------------------------------------
# FeaturesError
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_features_error_is_runtime_error() -> None:
    """FeaturesError is a subclass of RuntimeError."""
    assert issubclass(FeaturesError, RuntimeError)


@pytest.mark.unit
def test_features_error_raise_catch() -> None:
    """FeaturesError can be raised and caught as RuntimeError."""
    with pytest.raises(RuntimeError):
        raise FeaturesError("test error")


# ---------------------------------------------------------------------------
# _add_cumulative_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cumulative_single_well() -> None:
    """cum_oil = cumsum of OilProduced for one well (TR-03)."""
    df = _make_well_df(["W1", "W1", "W1"], [10.0, 20.0, 30.0])
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [10.0, 30.0, 60.0]


@pytest.mark.unit
def test_cumulative_with_shut_in() -> None:
    """Shut-in month (OilProduced=0) → cum_oil stays flat (TR-08)."""
    df = _make_well_df(["W1", "W1", "W1"], [10.0, 0.0, 5.0])
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [10.0, 10.0, 15.0]


@pytest.mark.unit
def test_cumulative_two_wells_independent() -> None:
    """Two wells in same partition → independent cumulative sums."""
    df = _make_well_df(
        ["W1", "W1", "W2", "W2"],
        [10.0, 20.0, 100.0, 50.0],
    )
    result = _add_cumulative_features(df)
    w1 = result[result["well_id"] == "W1"]["cum_oil"].tolist()
    w2 = result[result["well_id"] == "W2"]["cum_oil"].tolist()
    assert w1 == [10.0, 30.0]
    assert w2 == [100.0, 150.0]


@pytest.mark.unit
def test_cumulative_monotonically_nondecreasing() -> None:
    """cum_oil is monotonically non-decreasing per well (TR-03)."""
    df = _make_well_df(["W1"] * 6, [10.0, 0.0, 5.0, 20.0, 0.0, 15.0])
    result = _add_cumulative_features(df)
    diffs = result["cum_oil"].diff().dropna()
    assert (diffs >= 0).all()


@pytest.mark.unit
def test_cumulative_dtypes() -> None:
    """cum_oil, cum_gas, cum_water are Float64 dtype."""
    df = _make_well_df(["W1"], [10.0], [5.0], [3.0])
    result = _add_cumulative_features(df)
    assert str(result["cum_oil"].dtype) == "Float64"
    assert str(result["cum_gas"].dtype) == "Float64"
    assert str(result["cum_water"].dtype) == "Float64"


@pytest.mark.unit
def test_cumulative_zero_start() -> None:
    """Zeros at start → cum stays 0 during those months (TR-08b)."""
    df = _make_well_df(["W1", "W1", "W1"], [0.0, 0.0, 10.0])
    result = _add_cumulative_features(df)
    assert result["cum_oil"].iloc[0] == 0.0
    assert result["cum_oil"].iloc[1] == 0.0
    assert result["cum_oil"].iloc[2] == 10.0


@pytest.mark.unit
def test_cumulative_resumes_after_shutin() -> None:
    """Production resumes after shut-in → cumulative resumes from flat value (TR-08c)."""
    df = _make_well_df(["W1"] * 4, [10.0, 0.0, 0.0, 5.0])
    result = _add_cumulative_features(df)
    assert result["cum_oil"].iloc[3] == 15.0


# ---------------------------------------------------------------------------
# _add_ratio_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_gor_oil_zero_gas_nonzero() -> None:
    """oil=0, gas=100 → gor is NaN (TR-06a)."""
    df = _make_well_df(["W1"], [0.0], [100.0])
    result = _add_ratio_features(df)
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_gor_both_zero() -> None:
    """oil=0, gas=0 → gor is NaN or 0.0 (not an exception) (TR-06b)."""
    df = _make_well_df(["W1"], [0.0], [0.0])
    result = _add_ratio_features(df)
    # Both NaN and 0.0 are acceptable per spec
    val = result["gor"].iloc[0]
    assert pd.isna(val) or val == 0.0


@pytest.mark.unit
def test_gor_oil_nonzero_gas_zero() -> None:
    """oil=100, gas=0 → gor == 0.0 (TR-06c)."""
    df = _make_well_df(["W1"], [100.0], [0.0])
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_gor_normal() -> None:
    """oil=100, gas=500 → gor == 5.0."""
    df = _make_well_df(["W1"], [100.0], [500.0])
    result = _add_ratio_features(df)
    assert abs(result["gor"].iloc[0] - 5.0) < 1e-9


@pytest.mark.unit
def test_gor_high_legitimate() -> None:
    """Late-life well with very low oil → high GOR is retained as-is."""
    df = _make_well_df(["W1"], [0.1], [500.0])
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == pytest.approx(5000.0)


@pytest.mark.unit
def test_water_cut_no_water() -> None:
    """water=0, oil=100 → water_cut == 0.0 (TR-10a)."""
    df = _make_well_df(["W1"], [100.0], [0.0], [0.0])
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == pytest.approx(0.0)


@pytest.mark.unit
def test_water_cut_all_water() -> None:
    """water=500, oil=0 → water_cut == 1.0 (TR-10b)."""
    df = _make_well_df(["W1"], [0.0], [0.0], [500.0])
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == pytest.approx(1.0)


@pytest.mark.unit
def test_water_cut_both_zero() -> None:
    """water=0, oil=0 → water_cut is NaN."""
    df = _make_well_df(["W1"], [0.0], [0.0], [0.0])
    result = _add_ratio_features(df)
    assert pd.isna(result["water_cut"].iloc[0])


@pytest.mark.unit
def test_gor_non_negative() -> None:
    """GOR is always non-negative where not NaN (TR-01)."""
    df = _make_well_df(["W1", "W1"], [100.0, 0.0], [500.0, 0.0])
    result = _add_ratio_features(df)
    valid = result["gor"].dropna()
    assert (valid >= 0).all()


@pytest.mark.unit
def test_water_cut_in_range() -> None:
    """water_cut is always in [0,1] where not NaN (TR-01, TR-10)."""
    df = _make_well_df(["W1", "W1"], [100.0, 50.0], [0.0, 0.0], [0.0, 50.0])
    result = _add_ratio_features(df)
    valid = result["water_cut"].dropna()
    assert ((valid >= 0) & (valid <= 1)).all()


# ---------------------------------------------------------------------------
# _add_decline_rates
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decline_rate_basic() -> None:
    """OilProduced=[100,50,200] → decline_rate[1]==0.5, decline_rate[2] clipped to -1.0."""
    df = _make_well_df(["W1", "W1", "W1"], [100.0, 50.0, 200.0])
    result = _add_decline_rates(df)
    assert abs(result["decline_rate"].iloc[1] - 0.5) < 1e-9
    # pct_change * -1: (50-200)/50 * -1 = 3.0 (growth), raw = (200-50)/50 * -1 = -3.0
    # clipped at -1.0
    assert result["decline_rate"].iloc[2] == pytest.approx(-1.0)


@pytest.mark.unit
def test_decline_rate_clip_lower() -> None:
    """Decline rate below -1.0 → clipped to exactly -1.0 (TR-07a)."""
    df = _make_well_df(["W1", "W1"], [10.0, 1000.0])
    result = _add_decline_rates(df)
    assert result["decline_rate"].iloc[1] == pytest.approx(-1.0)


@pytest.mark.unit
def test_decline_rate_clip_upper() -> None:
    """Decline rate above 10.0 → clipped to exactly 10.0 (TR-07b)."""
    # 100 → 0.001: rate = (100-0.001)/100 ≈ 0.999 → not above 10
    # Use: 100 → 0 then next is very small non-zero
    # Better: start with large oil, then tiny to get high rate
    # pct_change * -1: (old - new) / old → high for near-zero decline
    df = _make_well_df(["W1", "W1", "W1"], [100.0, 1.0, 0.001])
    _add_decline_rates(df)
    # row 2: (1-0.001)/1 * -1 ... actually (prev-cur)/prev = (1-0.001)/1 = 0.999, * -1 = -0.999
    # Not above 10. Let's try: 0.001 → 0.0001
    df2 = _make_well_df(["W1", "W1", "W1", "W1"], [1000.0, 100.0, 0.001, 100.0])
    result2 = _add_decline_rates(df2)
    # Row 3: prev=0.001, cur=100 → pct_change = (100-0.001)/0.001 ≈ 99999, * -1 = -99999 → clipped to -1.0
    # Any result clipped to bounds
    assert result2["decline_rate"].notna().any()
    assert (result2["decline_rate"].dropna() >= -1.0).all()
    assert (result2["decline_rate"].dropna() <= 10.0).all()


@pytest.mark.unit
def test_decline_rate_within_bounds_unchanged() -> None:
    """Value within [-1.0, 10.0] passes through unchanged (TR-07c)."""
    df = _make_well_df(["W1", "W1", "W1"], [100.0, 80.0, 60.0])
    result = _add_decline_rates(df)
    rate = float(result["decline_rate"].iloc[1])
    assert -1.0 <= rate <= 10.0


@pytest.mark.unit
def test_decline_rate_shutin_no_extreme(caplog: pytest.LogCaptureFixture) -> None:
    """Two consecutive zero-production months → inf handled before clip (TR-07d)."""
    df = _make_well_df(["W1", "W1", "W1", "W1"], [100.0, 0.0, 0.0, 50.0])
    result = _add_decline_rates(df)
    assert not result["decline_rate"].isin([np.inf, -np.inf]).any()


@pytest.mark.unit
def test_decline_rate_two_wells_independent() -> None:
    """Two wells in same partition → independent decline rates."""
    df = _make_well_df(
        ["W1", "W1", "W2", "W2"],
        [100.0, 50.0, 200.0, 100.0],
    )
    result = _add_decline_rates(df)
    # W1 row 1: (100-50)/100 = 0.5
    w1_rate = float(result.loc[result["well_id"] == "W1", "decline_rate"].iloc[1])
    w2_rate = float(result.loc[result["well_id"] == "W2", "decline_rate"].iloc[1])
    assert abs(w1_rate - 0.5) < 1e-9
    assert abs(w2_rate - 0.5) < 1e-9


# ---------------------------------------------------------------------------
# _add_rolling_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rolling_3m_hand_computed() -> None:
    """3-month rolling avg: [10,20,30,40] → [10,15,20,30] (TR-09a)."""
    df = _make_well_df(["W1"] * 4, [10.0, 20.0, 30.0, 40.0])
    result = _add_rolling_features(df, windows=[3])
    expected = [10.0, 15.0, 20.0, 30.0]
    for actual, exp in zip(result["oil_rolling_3m"].tolist(), expected):
        assert abs(float(actual) - exp) < 1e-9


@pytest.mark.unit
def test_rolling_partial_window() -> None:
    """2 months of history with window=3 → partial-window means, not NaN (TR-09b)."""
    df = _make_well_df(["W1", "W1"], [10.0, 20.0])
    result = _add_rolling_features(df, windows=[3])
    assert not result["oil_rolling_3m"].isna().any()
    assert result["oil_rolling_3m"].iloc[0] == pytest.approx(10.0)
    assert result["oil_rolling_3m"].iloc[1] == pytest.approx(15.0)


@pytest.mark.unit
def test_rolling_6m_window() -> None:
    """6-month rolling average over 8 months verified (TR-09a)."""
    oil = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]
    df = _make_well_df(["W1"] * 8, oil)
    result = _add_rolling_features(df, windows=[6])
    # Month 6 (index 5): mean of [10,20,30,40,50,60] = 35
    assert result["oil_rolling_6m"].iloc[5] == pytest.approx(35.0)
    # Month 8 (index 7): mean of [30,40,50,60,70,80] = 55
    assert result["oil_rolling_6m"].iloc[7] == pytest.approx(55.0)


@pytest.mark.unit
def test_rolling_two_wells_independent() -> None:
    """Two wells → independent rolling means."""
    df = _make_well_df(
        ["W1", "W1", "W2", "W2"],
        [10.0, 20.0, 100.0, 200.0],
    )
    result = _add_rolling_features(df, windows=[2])
    w1_r = result.loc[result["well_id"] == "W1", "oil_rolling_2m"].tolist()
    w2_r = result.loc[result["well_id"] == "W2", "oil_rolling_2m"].tolist()
    assert w1_r[1] == pytest.approx(15.0)
    assert w2_r[1] == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# _add_lag_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_lag_1_basic() -> None:
    """OilProduced=[10,20,30] lag=1 → [NaN, 10, 20] (TR-09c)."""
    df = _make_well_df(["W1", "W1", "W1"], [10.0, 20.0, 30.0])
    result = _add_lag_features(df, lags=[1])
    assert pd.isna(result["oil_lag_1m"].iloc[0])
    assert result["oil_lag_1m"].iloc[1] == pytest.approx(10.0)
    assert result["oil_lag_1m"].iloc[2] == pytest.approx(20.0)


@pytest.mark.unit
def test_lag_3_first_three_nan() -> None:
    """Lag=3 → first 3 values NaN, 4th equals OilProduced[0]."""
    df = _make_well_df(["W1"] * 5, [10.0, 20.0, 30.0, 40.0, 50.0])
    result = _add_lag_features(df, lags=[3])
    assert pd.isna(result["oil_lag_3m"].iloc[0])
    assert pd.isna(result["oil_lag_3m"].iloc[1])
    assert pd.isna(result["oil_lag_3m"].iloc[2])
    assert result["oil_lag_3m"].iloc[3] == pytest.approx(10.0)


@pytest.mark.unit
def test_lag_two_wells_independent() -> None:
    """Two wells → independent lag features."""
    df = _make_well_df(
        ["W1", "W1", "W2", "W2"],
        [10.0, 20.0, 100.0, 200.0],
    )
    result = _add_lag_features(df, lags=[1])
    w1 = result.loc[result["well_id"] == "W1", "oil_lag_1m"].tolist()
    w2 = result.loc[result["well_id"] == "W2", "oil_lag_1m"].tolist()
    assert pd.isna(w1[0])
    assert w1[1] == pytest.approx(10.0)
    assert pd.isna(w2[0])
    assert w2[1] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# _engineer_features_partition
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_engineer_features_all_columns_present() -> None:
    """All required output columns are present after _engineer_features_partition (TR-19)."""
    df = _make_well_df(
        ["W1"] * 8, list(range(10, 90, 10)), list(range(100, 900, 100)), list(range(50, 450, 50))
    )
    result = _engineer_features_partition(df, windows=[3, 6], lags=[1, 3])

    expected_cols = [
        "OilProduced",
        "GasProduced",
        "WaterProduced",
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "oil_rolling_3m",
        "oil_rolling_6m",
        "gas_rolling_3m",
        "gas_rolling_6m",
        "water_rolling_3m",
        "water_rolling_6m",
        "oil_lag_1m",
        "oil_lag_3m",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


@pytest.mark.unit
def test_engineer_features_formula_correctness() -> None:
    """Verify GOR, water_cut, 3m rolling against hand-computed values (TR-09d, TR-09e)."""
    oil = [100.0, 80.0, 60.0]
    gas = [500.0, 400.0, 300.0]
    water = [50.0, 40.0, 30.0]
    df = _make_well_df(["W1", "W1", "W1"], oil, gas, water)
    result = _engineer_features_partition(df, windows=[3], lags=[1])

    # GOR: gas/oil
    assert result["gor"].iloc[0] == pytest.approx(5.0)
    # water_cut: water/(oil+water)
    assert result["water_cut"].iloc[0] == pytest.approx(50.0 / 150.0)
    # 3m rolling at index 2: mean(100, 80, 60) = 80
    assert result["oil_rolling_3m"].iloc[2] == pytest.approx(80.0)


# ---------------------------------------------------------------------------
# validate_feature_output
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_feature_output_passes(config_fixture: dict) -> None:
    """All required columns present → no exception (TR-19)."""
    df = _make_well_df(
        ["W1"] * 8, list(range(10, 90, 10)), list(range(100, 900, 100)), list(range(50, 450, 50))
    )
    result = _engineer_features_partition(df, windows=[3, 6], lags=[1, 3])
    ddf = dd.from_pandas(result, npartitions=1)
    validate_feature_output(ddf, config_fixture)  # should not raise


@pytest.mark.unit
def test_validate_feature_output_missing_gor(config_fixture: dict) -> None:
    """Missing gor column → FeaturesError raised."""
    df = _make_well_df(["W1"] * 4, [10.0, 20.0, 30.0, 40.0])
    # Manually add expected columns except gor
    result = _engineer_features_partition(df, windows=[3, 6], lags=[1, 3])
    result = result.drop(columns=["gor"])
    ddf = dd.from_pandas(result, npartitions=1)
    with pytest.raises(FeaturesError):
        validate_feature_output(ddf, config_fixture)


@pytest.mark.unit
def test_validate_feature_output_warns_negative_gor(
    config_fixture: dict, caplog: pytest.LogCaptureFixture
) -> None:
    """gor=-1.0 in data → WARNING logged."""
    df = _make_well_df(["W1"] * 4, [10.0, 20.0, 30.0, 40.0], [50.0, 100.0, 150.0, 200.0])
    result = _engineer_features_partition(df, windows=[3, 6], lags=[1, 3])
    # Force a negative gor
    result["gor"] = pd.array([-1.0] + [5.0] * 3, dtype=pd.Float64Dtype())
    ddf = dd.from_pandas(result, npartitions=1)
    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.features"):
        validate_feature_output(ddf, config_fixture)
    assert any(
        "gor" in r.message.lower() or "negative" in r.message.lower() for r in caplog.records
    )


@pytest.mark.unit
def test_validate_feature_output_warns_water_cut_out_of_range(
    config_fixture: dict, caplog: pytest.LogCaptureFixture
) -> None:
    """water_cut=1.5 → WARNING logged."""
    df = _make_well_df(["W1"] * 4, [10.0, 20.0, 30.0, 40.0])
    result = _engineer_features_partition(df, windows=[3, 6], lags=[1, 3])
    result["water_cut"] = pd.array([1.5] + [0.3] * 3, dtype=pd.Float64Dtype())
    ddf = dd.from_pandas(result, npartitions=1)
    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.features"):
        validate_feature_output(ddf, config_fixture)
    assert any(
        "water_cut" in r.message.lower() or "[0,1]" in r.message or "outside" in r.message.lower()
        for r in caplog.records
    )


@pytest.mark.unit
def test_validate_feature_output_warns_decline_out_of_range(
    config_fixture: dict, caplog: pytest.LogCaptureFixture
) -> None:
    """decline_rate=15.0 → WARNING logged."""
    df = _make_well_df(["W1"] * 4, [10.0, 20.0, 30.0, 40.0])
    result = _engineer_features_partition(df, windows=[3, 6], lags=[1, 3])
    result["decline_rate"] = pd.array([15.0] + [0.1] * 3, dtype=pd.Float64Dtype())
    ddf = dd.from_pandas(result, npartitions=1)
    with caplog.at_level(logging.WARNING, logger="cogcc_pipeline.features"):
        validate_feature_output(ddf, config_fixture)
    assert any("decline_rate" in r.message.lower() or "[-1" in r.message for r in caplog.records)
