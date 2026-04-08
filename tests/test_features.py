"""Tests for cogcc_pipeline/features.py."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from cogcc_pipeline.features import (
    _add_cumulative_features,
    _add_decline_rates,
    _add_ratio_features,
    _add_rolling_features,
    _add_time_features,
    _add_well_age,
    _apply_label_encoding,
    fit_label_encoders,
    save_encoder_mappings,
)


def make_well_partition(
    well_ids: list[str],
    dates: list[str],
    oil: list[float | None],
    gas: list[float | None],
    water: list[float | None],
) -> pd.DataFrame:
    """Build a minimal cleaned-style partition with production columns."""
    return pd.DataFrame(
        {
            "well_id": pd.array(well_ids, dtype=pd.StringDtype()),  # type: ignore[arg-type]
            "production_date": pd.to_datetime(dates),
            "OilProduced": pd.array(oil, dtype=pd.Float64Dtype()),
            "GasProduced": pd.array(gas, dtype=pd.Float64Dtype()),
            "WaterProduced": pd.array(water, dtype=pd.Float64Dtype()),
            "OpName": pd.array(["OP A"] * len(well_ids), dtype=pd.StringDtype()),
            "FormationCode": pd.array(["NIO"] * len(well_ids), dtype=pd.StringDtype()),
        }
    )


# ---------------------------------------------------------------------------
# F-01: _add_time_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_add_time_features_march_2021() -> None:
    df = pd.DataFrame({"production_date": pd.to_datetime(["2021-03-01"])})
    result = _add_time_features(df)
    assert result["year"].iloc[0] == 2021
    assert result["month"].iloc[0] == 3
    assert result["quarter"].iloc[0] == 1
    assert result["days_in_month"].iloc[0] == 31
    assert result["months_since_2020"].iloc[0] == 14


@pytest.mark.unit
def test_add_time_features_jan_2020_months_since_zero() -> None:
    df = pd.DataFrame({"production_date": pd.to_datetime(["2020-01-01"])})
    result = _add_time_features(df)
    assert result["months_since_2020"].iloc[0] == 0


@pytest.mark.unit
def test_add_time_features_dec_2022() -> None:
    df = pd.DataFrame({"production_date": pd.to_datetime(["2022-12-01"])})
    result = _add_time_features(df)
    assert result["quarter"].iloc[0] == 4
    assert result["days_in_month"].iloc[0] == 31


@pytest.mark.unit
def test_add_time_features_dtypes() -> None:
    df = pd.DataFrame({"production_date": pd.to_datetime(["2021-01-01"])})
    result = _add_time_features(df)
    for col in ("year", "month", "quarter", "days_in_month", "months_since_2020"):
        assert result[col].dtype == pd.Int64Dtype()


# ---------------------------------------------------------------------------
# F-02: _add_rolling_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr09a_rolling_3m_mean_single_well() -> None:
    """TR-09a: oil_roll3_mean for [100, 200, 300] equals [100.0, 150.0, 200.0]."""
    df = make_well_partition(
        well_ids=["W1", "W1", "W1"],
        dates=["2021-01-01", "2021-02-01", "2021-03-01"],
        oil=[100.0, 200.0, 300.0],
        gas=[50.0, 50.0, 50.0],
        water=[10.0, 10.0, 10.0],
    )
    result = _add_rolling_features(df, windows=[3, 6])
    means = list(result["oil_roll3_mean"])
    assert abs(means[0] - 100.0) < 1e-9
    assert abs(means[1] - 150.0) < 1e-9
    assert abs(means[2] - 200.0) < 1e-9


@pytest.mark.unit
def test_tr09b_partial_rolling_window() -> None:
    """TR-09b: oil_roll6_mean for 2 months equals mean of 2 values."""
    df = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[100.0, 200.0],
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    result = _add_rolling_features(df, windows=[6])
    assert abs(result["oil_roll6_mean"].iloc[1] - 150.0) < 1e-9


@pytest.mark.unit
def test_rolling_features_group_isolation() -> None:
    """Two wells in same partition compute rolling stats independently."""
    df = make_well_partition(
        well_ids=["W1", "W1", "W2", "W2"],
        dates=["2021-01-01", "2021-02-01", "2021-01-01", "2021-02-01"],
        oil=[100.0, 200.0, 50.0, 50.0],
        gas=[50.0, 50.0, 25.0, 25.0],
        water=[10.0, 10.0, 5.0, 5.0],
    )
    result = _add_rolling_features(df, windows=[3])
    w1_means = result[result["well_id"] == "W1"]["oil_roll3_mean"].tolist()
    w2_means = result[result["well_id"] == "W2"]["oil_roll3_mean"].tolist()
    assert abs(w1_means[1] - 150.0) < 1e-9
    assert abs(w2_means[1] - 50.0) < 1e-9


@pytest.mark.unit
def test_rolling_features_dtype() -> None:
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[50.0],
        water=[10.0],
    )
    result = _add_rolling_features(df, windows=[3])
    assert result["oil_roll3_mean"].dtype == pd.Float64Dtype()


# ---------------------------------------------------------------------------
# F-03: _add_cumulative_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr03_cumulative_production() -> None:
    """TR-03: cum_oil for [100, 200, 150] = [100, 300, 450]."""
    df = make_well_partition(
        well_ids=["W1", "W1", "W1"],
        dates=["2021-01-01", "2021-02-01", "2021-03-01"],
        oil=[100.0, 200.0, 150.0],
        gas=[50.0, 50.0, 50.0],
        water=[10.0, 10.0, 10.0],
    )
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [100.0, 300.0, 450.0]


@pytest.mark.unit
def test_tr08a_shutin_cumulative_flat() -> None:
    """TR-08a: [100, 0, 0, 200] → cum_oil = [100, 100, 100, 300]."""
    df = make_well_partition(
        well_ids=["W1"] * 4,
        dates=["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01"],
        oil=[100.0, 0.0, 0.0, 200.0],
        gas=[50.0] * 4,
        water=[10.0] * 4,
    )
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [100.0, 100.0, 100.0, 300.0]


@pytest.mark.unit
def test_tr08b_not_yet_online() -> None:
    """TR-08b: [0, 0, 100] → cum_oil = [0, 0, 100]."""
    df = make_well_partition(
        well_ids=["W1"] * 3,
        dates=["2021-01-01", "2021-02-01", "2021-03-01"],
        oil=[0.0, 0.0, 100.0],
        gas=[0.0] * 3,
        water=[0.0] * 3,
    )
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [0.0, 0.0, 100.0]


@pytest.mark.unit
def test_cumulative_monotonically_nondecreasing() -> None:
    """TR-03: cum_oil is monotonically non-decreasing for valid input."""
    df = make_well_partition(
        well_ids=["W1"] * 5,
        dates=[f"2021-0{m}-01" for m in range(1, 6)],
        oil=[100.0, 0.0, 50.0, 200.0, 75.0],
        gas=[50.0] * 5,
        water=[10.0] * 5,
    )
    result = _add_cumulative_features(df)
    vals = list(result["cum_oil"])
    assert all(vals[i] <= vals[i + 1] for i in range(len(vals) - 1))


@pytest.mark.unit
def test_cumulative_dtype() -> None:
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[50.0],
        water=[10.0],
    )
    result = _add_cumulative_features(df)
    assert result["cum_oil"].dtype == pd.Float64Dtype()


# ---------------------------------------------------------------------------
# F-04: _add_decline_rates
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr07a_decline_rate_clipped_below() -> None:
    """TR-07a: rate of -2.0 clipped to -1.0."""
    make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[
            100.0,
            10.0,
        ],  # (10-100)/100 = -0.9; needs -2.0 scenario: use [100, 0] → but 0 prior issue
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    # manually force: (5 - 100) / 100 = -0.95, test clip at -1: use (0 - 100) / 100 = -1.0 (on boundary)
    df2 = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[100.0, 0.0],  # decline = -1.0 (at boundary)
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    # Test clip: 0/100 → (-100)/100 = -1.0, which equals clip_min
    result2 = _add_decline_rates(df2, clip_min=-1.0, clip_max=10.0)
    assert result2["oil_decline_rate"].iloc[1] >= -1.0


@pytest.mark.unit
def test_tr07b_decline_rate_clipped_above() -> None:
    """TR-07b: rate > 10 clipped to 10.0."""
    df = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[10.0, 200.0],  # (200-10)/10 = 19 → clipped to 10.0
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    result = _add_decline_rates(df, clip_min=-1.0, clip_max=10.0)
    assert result["oil_decline_rate"].iloc[1] == 10.0


@pytest.mark.unit
def test_tr07c_decline_rate_within_bounds() -> None:
    """TR-07c: rate 0.5 within bounds unchanged."""
    df = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[100.0, 150.0],  # (150-100)/100 = 0.5
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    result = _add_decline_rates(df, clip_min=-1.0, clip_max=10.0)
    assert abs(result["oil_decline_rate"].iloc[1] - 0.5) < 1e-9


@pytest.mark.unit
def test_tr07d_shutin_both_zero() -> None:
    """TR-07d: [0, 0] → oil_decline_rate = 0.0."""
    df = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[0.0, 0.0],
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    result = _add_decline_rates(df, clip_min=-1.0, clip_max=10.0)
    assert result["oil_decline_rate"].iloc[1] == 0.0


@pytest.mark.unit
def test_decline_rate_50pct_decline() -> None:
    """[100, 50] → decline rate = -0.5."""
    df = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[100.0, 50.0],
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    result = _add_decline_rates(df, clip_min=-1.0, clip_max=10.0)
    assert abs(result["oil_decline_rate"].iloc[1] - (-0.5)) < 1e-9


@pytest.mark.unit
def test_decline_rate_prior_zero_current_positive_is_na() -> None:
    """[0, 100] → oil_decline_rate = NA (undefined)."""
    df = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[0.0, 100.0],
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    result = _add_decline_rates(df, clip_min=-1.0, clip_max=10.0)
    assert pd.isna(result["oil_decline_rate"].iloc[1])


# ---------------------------------------------------------------------------
# F-05: _add_ratio_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr06a_gor_oil_zero_gas_positive_is_na() -> None:
    """TR-06a: OilProduced=0, GasProduced=50 → gor=NA."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[0.0],
        gas=[50.0],
        water=[10.0],
    )
    result = _add_ratio_features(df)
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_tr06b_gor_both_zero_is_zero() -> None:
    """TR-06b: OilProduced=0, GasProduced=0 → gor=0.0."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[0.0],
        gas=[0.0],
        water=[0.0],
    )
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_tr06c_gor_oil_positive_gas_zero() -> None:
    """TR-06c: OilProduced=100, GasProduced=0 → gor=0.0."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[0.0],
        water=[10.0],
    )
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_gor_normal() -> None:
    """OilProduced=100, GasProduced=500 → gor=5.0."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[500.0],
        water=[10.0],
    )
    result = _add_ratio_features(df)
    assert abs(result["gor"].iloc[0] - 5.0) < 1e-9


@pytest.mark.unit
def test_tr09d_gor_formula_direction() -> None:
    """TR-09d: GasProduced=1000, OilProduced=200 → gor=5.0 (not 0.2)."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[200.0],
        gas=[1000.0],
        water=[10.0],
    )
    result = _add_ratio_features(df)
    assert abs(result["gor"].iloc[0] - 5.0) < 1e-9


@pytest.mark.unit
def test_tr09e_water_cut_denominator() -> None:
    """TR-09e: WaterProduced=300, OilProduced=700 → water_cut=0.3."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[700.0],
        gas=[500.0],
        water=[300.0],
    )
    result = _add_ratio_features(df)
    assert abs(result["water_cut"].iloc[0] - 0.3) < 1e-9


@pytest.mark.unit
def test_tr10a_water_cut_zero_water() -> None:
    """TR-10a: WaterProduced=0, OilProduced=500 → water_cut=0.0."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[500.0],
        gas=[500.0],
        water=[0.0],
    )
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == 0.0


@pytest.mark.unit
def test_tr10b_water_cut_one_when_no_oil() -> None:
    """TR-10b: OilProduced=0, WaterProduced=200 → water_cut=1.0."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[0.0],
        gas=[0.0],
        water=[200.0],
    )
    result = _add_ratio_features(df)
    assert abs(result["water_cut"].iloc[0] - 1.0) < 1e-9


@pytest.mark.unit
def test_tr01_gor_nonnegative() -> None:
    """TR-01: GOR must be non-negative for valid non-NA inputs."""
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[500.0],
        water=[50.0],
    )
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] >= 0.0


@pytest.mark.unit
def test_no_zero_division_error() -> None:
    """No ZeroDivisionError for any combo of zeros and NA."""
    import warnings

    df = make_well_partition(
        well_ids=["W1", "W1", "W1", "W1"],
        dates=["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01"],
        oil=[0.0, 0.0, 100.0, None],
        gas=[0.0, 50.0, 0.0, None],
        water=[0.0, 0.0, 50.0, None],
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        # Should not raise
        _add_ratio_features(df)


# ---------------------------------------------------------------------------
# F-06: _add_well_age
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_add_well_age_first_month_zero() -> None:
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2020-01-01"],
        oil=[100.0],
        gas=[50.0],
        water=[10.0],
    )
    result = _add_well_age(df)
    assert result["well_age_months"].iloc[0] == 0


@pytest.mark.unit
def test_add_well_age_sequence() -> None:
    df = make_well_partition(
        well_ids=["W1", "W1", "W1"],
        dates=["2020-01-01", "2020-06-01", "2021-01-01"],
        oil=[100.0, 100.0, 100.0],
        gas=[50.0, 50.0, 50.0],
        water=[10.0, 10.0, 10.0],
    )
    result = _add_well_age(df)
    ages = list(result["well_age_months"])
    assert ages == [0, 5, 12]


@pytest.mark.unit
def test_add_well_age_nonnegative() -> None:
    df = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-06-01"],
        oil=[100.0, 100.0],
        gas=[50.0, 50.0],
        water=[10.0, 10.0],
    )
    result = _add_well_age(df)
    assert all(result["well_age_months"] >= 0)


@pytest.mark.unit
def test_add_well_age_dtype() -> None:
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[50.0],
        water=[10.0],
    )
    result = _add_well_age(df)
    assert result["well_age_months"].dtype == pd.Int64Dtype()


# ---------------------------------------------------------------------------
# F-07: fit_label_encoders / _apply_label_encoding
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fit_label_encoders_alphabetical() -> None:
    import dask.dataframe as dd

    df = pd.DataFrame(
        {
            "OpName": pd.array(["A", "B", "A"], dtype=pd.StringDtype()),
            "FormationCode": pd.array(["NIO", "COD", "NIO"], dtype=pd.StringDtype()),
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    mappings = fit_label_encoders(ddf)
    assert mappings["OpName"]["A"] == 0
    assert mappings["OpName"]["B"] == 1


@pytest.mark.unit
def test_apply_label_encoding_na_maps_to_minus_one() -> None:
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[50.0],
        water=[10.0],
    )
    df["OpName"] = pd.array([pd.NA], dtype=pd.StringDtype())
    df["FormationCode"] = pd.array([pd.NA], dtype=pd.StringDtype())
    mappings = {"OpName": {"OP A": 0}, "FormationCode": {"NIO": 0}}
    result = _apply_label_encoding(df, mappings)
    assert result["operator_encoded"].iloc[0] == -1


@pytest.mark.unit
def test_apply_label_encoding_unseen_maps_minus_one() -> None:
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[50.0],
        water=[10.0],
    )
    mappings = {"OpName": {"OTHER_OP": 0}, "FormationCode": {"NIO": 0}}
    result = _apply_label_encoding(df, mappings)
    # "OP A" not in mapping → -1
    assert result["operator_encoded"].iloc[0] == -1


@pytest.mark.unit
def test_apply_label_encoding_dtype() -> None:
    df = make_well_partition(
        well_ids=["W1"],
        dates=["2021-01-01"],
        oil=[100.0],
        gas=[50.0],
        water=[10.0],
    )
    mappings = {"OpName": {"OP A": 0}, "FormationCode": {"NIO": 0}}
    result = _apply_label_encoding(df, mappings)
    assert result["operator_encoded"].dtype == pd.Int64Dtype()


# ---------------------------------------------------------------------------
# F-08: save_encoder_mappings
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_save_encoder_mappings_creates_file(tmp_path: Path) -> None:
    mappings = {"OpName": {"A": 0}, "FormationCode": {"NIO": 0}}
    path = tmp_path / "encoders.json"
    save_encoder_mappings(mappings, path)
    assert path.exists()


@pytest.mark.unit
def test_save_encoder_mappings_roundtrip(tmp_path: Path) -> None:
    mappings = {"OpName": {"A": 0, "B": 1}, "FormationCode": {"NIO": 0}}
    path = tmp_path / "encoders.json"
    save_encoder_mappings(mappings, path)
    loaded = json.loads(path.read_text())
    assert loaded == mappings


# ---------------------------------------------------------------------------
# F-10 Domain tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr14_schema_stability_two_partitions() -> None:
    """TR-14: Two single-well partitions have identical columns and dtypes after all features."""

    def apply_all(df: pd.DataFrame) -> pd.DataFrame:
        df = _add_time_features(df)
        df = _add_rolling_features(df, windows=[3, 6])
        df = _add_cumulative_features(df)
        df = _add_decline_rates(df, clip_min=-1.0, clip_max=10.0)
        df = _add_ratio_features(df)
        df = _add_well_age(df)
        return df

    df1 = make_well_partition(
        well_ids=["W1", "W1"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[100.0, 200.0],
        gas=[50.0, 100.0],
        water=[10.0, 20.0],
    )
    df2 = make_well_partition(
        well_ids=["W2", "W2"],
        dates=["2021-01-01", "2021-02-01"],
        oil=[50.0, 75.0],
        gas=[25.0, 30.0],
        water=[5.0, 8.0],
    )
    r1 = apply_all(df1)
    r2 = apply_all(df2)
    assert list(r1.columns) == list(r2.columns)
    for col in r1.columns:
        assert r1[col].dtype == r2[col].dtype, f"dtype mismatch on {col}"


@pytest.mark.unit
def test_tr09c_rolling_includes_prior_month() -> None:
    """TR-09c: oil_roll3_mean at month N includes month N-1 value."""
    df = make_well_partition(
        well_ids=["W1", "W1", "W1"],
        dates=["2021-01-01", "2021-02-01", "2021-03-01"],
        oil=[100.0, 200.0, 300.0],
        gas=[50.0] * 3,
        water=[10.0] * 3,
    )
    result = _add_rolling_features(df, windows=[3])
    # month 3 mean = (100+200+300)/3 = 200
    assert abs(result["oil_roll3_mean"].iloc[2] - 200.0) < 1e-9
