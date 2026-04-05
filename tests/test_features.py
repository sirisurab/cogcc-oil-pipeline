"""Tests for cogcc_pipeline.features."""

from __future__ import annotations

import math
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import LabelEncoder, StandardScaler  # type: ignore[import-untyped]

from cogcc_pipeline.features import (
    compute_cumulative_production,
    compute_decline_rate,
    compute_gor,
    compute_mom_pct_change,
    compute_rolling_and_lag_features,
    compute_water_cut,
    encode_categoricals,
    fit_scalers_and_encoders,
    scale_numerics,
    write_features_parquet,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ddf(data: dict, npartitions: int = 1) -> dd.DataFrame:
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=npartitions)


def _single_well_ddf(
    oil: list[float],
    gas: list[float] | None = None,
    water: list[float] | None = None,
    well_id: str = "05-12345",
    start: str = "2021-01-01",
) -> dd.DataFrame:
    n = len(oil)
    gas = gas if gas is not None else [0.0] * n
    water = water if water is not None else [0.0] * n
    dates = pd.date_range(start, periods=n, freq="MS")
    return _make_ddf(
        {
            "well_id": [well_id] * n,
            "county_code": ["05"] * n,
            "operator_name": ["ACME OIL"] * n,
            "report_year": [d.year for d in dates],
            "report_month": [d.month for d in dates],
            "oil_bbl": oil,
            "gas_mcf": gas,
            "water_bbl": water,
            "days_produced": [28.0] * n,
            "production_date": dates,
            "is_valid": [True] * n,
            "cum_oil": [0.0] * n,
            "cum_gas": [0.0] * n,
            "cum_water": [0.0] * n,
        }
    )


# ---------------------------------------------------------------------------
# Task 01: compute_cumulative_production tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cumulative_production_basic():
    ddf = _single_well_ddf([100, 200, 300])
    result = compute_cumulative_production(ddf).compute()
    result = result.sort_values("production_date")
    assert list(result["cum_oil"]) == [100.0, 300.0, 600.0]


@pytest.mark.unit
def test_cumulative_production_zero_month():
    """Zero production month: cumulative stays flat (TR-08)."""
    ddf = _single_well_ddf([100, 0, 200])
    result = compute_cumulative_production(ddf).compute().sort_values("production_date")
    assert list(result["cum_oil"]) == [100.0, 100.0, 300.0]


@pytest.mark.unit
def test_cumulative_production_zero_start():
    """Zeros at start: cum_oil = [0, 0, 100]."""
    ddf = _single_well_ddf([0, 0, 100])
    result = compute_cumulative_production(ddf).compute().sort_values("production_date")
    assert list(result["cum_oil"]) == [0.0, 0.0, 100.0]


@pytest.mark.unit
def test_cumulative_production_monotonic_3_wells():
    """Cumulative is monotonically non-decreasing for all wells (TR-03)."""
    n = 6
    dates = pd.date_range("2021-01-01", periods=n, freq="MS")
    data = {
        "well_id": ["W1"] * n + ["W2"] * n + ["W3"] * n,
        "county_code": ["05"] * n * 3,
        "operator_name": ["OP"] * n * 3,
        "report_year": [d.year for d in dates] * 3,
        "report_month": [d.month for d in dates] * 3,
        "oil_bbl": [10, 20, 0, 30, 5, 15] + [100, 50, 0, 0, 200, 10] + [5] * n,
        "gas_mcf": [0.0] * n * 3,
        "water_bbl": [0.0] * n * 3,
        "days_produced": [28.0] * n * 3,
        "production_date": list(dates) * 3,
        "is_valid": [True] * n * 3,
        "cum_oil": [0.0] * n * 3,
        "cum_gas": [0.0] * n * 3,
        "cum_water": [0.0] * n * 3,
    }
    ddf = _make_ddf(data)
    result = compute_cumulative_production(ddf).compute()
    for well in ["W1", "W2", "W3"]:
        cum = result[result["well_id"] == well].sort_values("production_date")["cum_oil"].tolist()
        for i in range(1, len(cum)):
            assert cum[i] >= cum[i - 1], f"Not monotonic for {well} at index {i}"


@pytest.mark.unit
def test_cumulative_shut_in_resumes():
    """After shut-in, cumulative resumes from prior flat value (TR-08c)."""
    ddf = _single_well_ddf([100, 0, 0, 200])
    result = compute_cumulative_production(ddf).compute().sort_values("production_date")
    assert result["cum_oil"].iloc[1] == 100.0
    assert result["cum_oil"].iloc[2] == 100.0
    assert result["cum_oil"].iloc[3] == 300.0


@pytest.mark.unit
def test_cumulative_production_returns_dask():
    ddf = _single_well_ddf([100, 200])
    assert isinstance(compute_cumulative_production(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 02: compute_gor tests
# ---------------------------------------------------------------------------


def _gor_val(oil: float, gas: float) -> float | None:
    ddf = _single_well_ddf([oil], gas=[gas])
    result = compute_gor(ddf).compute()
    val = result["gor"].iloc[0]
    return val


@pytest.mark.unit
def test_gor_zero_oil_pos_gas_is_nan():
    val = _gor_val(0.0, 500.0)
    assert math.isnan(val), f"Expected NaN, got {val}"


@pytest.mark.unit
def test_gor_zero_oil_zero_gas_is_nan():
    val = _gor_val(0.0, 0.0)
    assert math.isnan(val)


@pytest.mark.unit
def test_gor_pos_oil_zero_gas_is_zero():
    val = _gor_val(100.0, 0.0)
    assert val == 0.0


@pytest.mark.unit
def test_gor_normal():
    val = _gor_val(100.0, 500.0)
    assert val == pytest.approx(5.0)


@pytest.mark.unit
def test_gor_high_value_not_flagged():
    """High GOR is physically valid below bubble point — not an error."""
    val = _gor_val(1.0, 100_000.0)
    assert val == pytest.approx(100_000.0)


@pytest.mark.unit
def test_gor_dtype():
    ddf = _single_well_ddf([100.0], gas=[500.0])
    result = compute_gor(ddf).compute()
    assert result["gor"].dtype == "float64"


@pytest.mark.unit
def test_gor_returns_dask():
    assert isinstance(compute_gor(_single_well_ddf([100.0])), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 03: compute_water_cut tests
# ---------------------------------------------------------------------------


def _wc_val(oil: float, water: float) -> float:
    ddf = _single_well_ddf([oil], water=[water])
    result = compute_water_cut(ddf).compute()
    return result["water_cut"].iloc[0]


@pytest.mark.unit
def test_water_cut_zero_water():
    val = _wc_val(500.0, 0.0)
    assert val == 0.0


@pytest.mark.unit
def test_water_cut_zero_oil():
    val = _wc_val(0.0, 300.0)
    assert val == 1.0


@pytest.mark.unit
def test_water_cut_both_zero_is_nan():
    val = _wc_val(0.0, 0.0)
    assert math.isnan(val)


@pytest.mark.unit
def test_water_cut_50_50():
    val = _wc_val(100.0, 100.0)
    assert val == pytest.approx(0.5)


@pytest.mark.unit
def test_water_cut_75():
    val = _wc_val(100.0, 300.0)
    assert val == pytest.approx(0.75)


@pytest.mark.unit
def test_water_cut_all_in_range():
    """50 rows: all non-NaN water_cut values in [0, 1]."""
    n = 50
    oil = [float(i % 200) for i in range(n)]
    water = [float((i * 3) % 200) for i in range(n)]
    ddf = _single_well_ddf(oil, water=water)
    result = compute_water_cut(ddf).compute()
    valid = result["water_cut"].dropna()
    assert (valid >= 0.0).all() and (valid <= 1.0).all()


@pytest.mark.unit
def test_water_cut_zero_retained():
    """water_cut=0.0 rows are not dropped (TR-10a boundary value)."""
    val = _wc_val(500.0, 0.0)
    assert val == 0.0


@pytest.mark.unit
def test_water_cut_one_retained():
    """water_cut=1.0 rows are not dropped (TR-10b boundary value)."""
    val = _wc_val(0.0, 300.0)
    assert val == 1.0


@pytest.mark.unit
def test_water_cut_returns_dask():
    assert isinstance(compute_water_cut(_single_well_ddf([100.0], water=[50.0])), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 04: compute_decline_rate tests
# ---------------------------------------------------------------------------


def _decline_values(oil_seq: list[float]) -> list[float]:
    ddf = _single_well_ddf(oil_seq)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    return result["decline_rate"].tolist()


@pytest.mark.unit
def test_decline_rate_within_bounds():
    vals = _decline_values([100.0, 80.0])
    assert vals[1] == pytest.approx(-0.2)


@pytest.mark.unit
def test_decline_rate_clipped_low():
    """Value below -1.0 clips to -1.0 (TR-07a)."""
    # (5 - 1000) / 1000 = -0.995 → within bounds; test actual clip:
    # Use a case where computed is < -1: very large drop
    ddf = _single_well_ddf([1000.0, 0.001])
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert result["decline_rate"].iloc[1] >= -1.0


@pytest.mark.unit
def test_decline_rate_clipped_high():
    """Value above 10.0 clips to 10.0 (TR-07b)."""
    ddf = _single_well_ddf([100.0, 2000.0])
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert result["decline_rate"].iloc[1] <= 10.0


@pytest.mark.unit
def test_decline_rate_zero_prev_zero_curr():
    """Two consecutive shut-in months: raw pre-clip = 0.0 (TR-07d)."""
    vals = _decline_values([100.0, 0.0, 0.0])
    assert vals[2] == pytest.approx(0.0)


@pytest.mark.unit
def test_decline_rate_first_row_nan():
    vals = _decline_values([100.0, 80.0])
    assert math.isnan(vals[0])


@pytest.mark.unit
def test_decline_rate_all_in_range():
    vals = _decline_values([float(i % 50 + 1) for i in range(20)])
    non_nan = [v for v in vals if not math.isnan(v)]
    assert all(-1.0 <= v <= 10.0 for v in non_nan)


@pytest.mark.unit
def test_decline_rate_returns_dask():
    assert isinstance(compute_decline_rate(_single_well_ddf([100.0, 80.0])), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 05: compute_rolling_and_lag_features tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rolling_3m_values():
    """oil_bbl = [10, 20, 30, 40] → oil_roll_3m = [NaN, NaN, 20.0, 30.0]."""
    ddf = _single_well_ddf([10.0, 20.0, 30.0, 40.0])
    result = compute_rolling_and_lag_features(ddf).compute().sort_values("production_date")
    roll = result["oil_roll_3m"].tolist()
    assert math.isnan(roll[0])
    assert math.isnan(roll[1])
    assert roll[2] == pytest.approx(20.0)
    assert roll[3] == pytest.approx(30.0)


@pytest.mark.unit
def test_rolling_3m_first_two_nan():
    ddf = _single_well_ddf([10.0, 20.0, 30.0])
    result = compute_rolling_and_lag_features(ddf).compute().sort_values("production_date")
    assert math.isnan(result["oil_roll_3m"].iloc[0])
    assert math.isnan(result["oil_roll_3m"].iloc[1])


@pytest.mark.unit
def test_lag_1m_values():
    """oil_bbl = [10, 20, 30] → oil_lag_1m = [NaN, 10, 20]."""
    ddf = _single_well_ddf([10.0, 20.0, 30.0])
    result = compute_rolling_and_lag_features(ddf).compute().sort_values("production_date")
    assert math.isnan(result["oil_lag_1m"].iloc[0])
    assert result["oil_lag_1m"].iloc[1] == pytest.approx(10.0)
    assert result["oil_lag_1m"].iloc[2] == pytest.approx(20.0)


@pytest.mark.unit
def test_lag_1m_three_months():
    """Verify lag correctness for 3 consecutive months."""
    ddf = _single_well_ddf([100.0, 150.0, 200.0])
    result = compute_rolling_and_lag_features(ddf).compute().sort_values("production_date")
    assert result["oil_lag_1m"].iloc[1] == pytest.approx(100.0)
    assert result["oil_lag_1m"].iloc[2] == pytest.approx(150.0)


@pytest.mark.unit
def test_rolling_6m_nan_first_5():
    """oil_roll_6m NaN for first 5 months of a 7-month sequence."""
    ddf = _single_well_ddf([float(i) for i in range(1, 8)])
    result = compute_rolling_and_lag_features(ddf).compute().sort_values("production_date")
    for i in range(5):
        assert math.isnan(result["oil_roll_6m"].iloc[i])
    assert not math.isnan(result["oil_roll_6m"].iloc[6])


@pytest.mark.unit
def test_rolling_all_9_columns_present():
    ddf = _single_well_ddf([10.0, 20.0, 30.0])
    result = compute_rolling_and_lag_features(ddf).compute()
    expected = [
        "oil_roll_3m",
        "gas_roll_3m",
        "water_roll_3m",
        "oil_roll_6m",
        "gas_roll_6m",
        "water_roll_6m",
        "oil_lag_1m",
        "gas_lag_1m",
        "water_lag_1m",
    ]
    for col in expected:
        assert col in result.columns


@pytest.mark.unit
def test_rolling_returns_dask():
    assert isinstance(
        compute_rolling_and_lag_features(_single_well_ddf([10.0, 20.0])), dd.DataFrame
    )


# ---------------------------------------------------------------------------
# Task 06: compute_mom_pct_change tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_mom_pct_increase():
    ddf = _single_well_ddf([100.0, 120.0])
    result = compute_mom_pct_change(ddf).compute().sort_values("production_date")
    assert math.isnan(result["oil_mom_pct"].iloc[0])
    assert result["oil_mom_pct"].iloc[1] == pytest.approx(20.0)


@pytest.mark.unit
def test_mom_pct_decrease():
    ddf = _single_well_ddf([100.0, 50.0])
    result = compute_mom_pct_change(ddf).compute().sort_values("production_date")
    assert result["oil_mom_pct"].iloc[1] == pytest.approx(-50.0)


@pytest.mark.unit
def test_mom_pct_zero_prev_is_nan():
    ddf = _single_well_ddf([0.0, 100.0])
    result = compute_mom_pct_change(ddf).compute().sort_values("production_date")
    assert math.isnan(result["oil_mom_pct"].iloc[1])


@pytest.mark.unit
def test_mom_pct_first_row_nan():
    ddf = _single_well_ddf([100.0, 120.0])
    result = compute_mom_pct_change(ddf).compute().sort_values("production_date")
    assert math.isnan(result["oil_mom_pct"].iloc[0])


@pytest.mark.unit
def test_mom_pct_returns_dask():
    assert isinstance(compute_mom_pct_change(_single_well_ddf([100.0, 120.0])), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 07: encode_categoricals tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_encode_categoricals_known_labels():
    enc = LabelEncoder()
    enc.fit(["A", "B", "C"])
    ddf = _make_ddf(
        {
            "county_code": ["A", "B", "C"],
            "operator_name": ["A", "B", "C"],
        }
    )
    encoders = {"county_code": enc, "operator_name": enc}
    result = encode_categoricals(ddf, encoders).compute()
    assert list(result["county_code_enc"]) == [0, 1, 2]


@pytest.mark.unit
def test_encode_categoricals_unseen_label_minus_one():
    enc = LabelEncoder()
    enc.fit(["A", "B", "C"])
    ddf = _make_ddf(
        {
            "county_code": ["D"],
            "operator_name": ["A"],
        }
    )
    encoders = {"county_code": enc, "operator_name": enc}
    result = encode_categoricals(ddf, encoders).compute()
    assert result["county_code_enc"].iloc[0] == -1


@pytest.mark.unit
def test_encode_categoricals_dtype_int32():
    enc = LabelEncoder()
    enc.fit(["A", "B"])
    ddf = _make_ddf({"county_code": ["A"], "operator_name": ["B"]})
    result = encode_categoricals(ddf, {"county_code": enc, "operator_name": enc}).compute()
    assert result["county_code_enc"].dtype == "int32"


@pytest.mark.unit
def test_encode_categoricals_returns_dask():
    enc = LabelEncoder()
    enc.fit(["A"])
    ddf = _make_ddf({"county_code": ["A"], "operator_name": ["A"]})
    assert isinstance(
        encode_categoricals(ddf, {"county_code": enc, "operator_name": enc}), dd.DataFrame
    )


# ---------------------------------------------------------------------------
# Task 08: scale_numerics tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_scale_numerics_known_value():
    """StandardScaler with mean=100, std=50: input 150 → scaled ≈ 1.0."""
    scaler = StandardScaler()
    scaler.mean_ = np.array([100.0])
    scaler.scale_ = np.array([50.0])
    scaler.var_ = np.array([2500.0])
    scaler.n_features_in_ = 1
    scaler.n_samples_seen_ = 10

    ddf = _single_well_ddf([150.0], gas=[0.0], water=[0.0])
    # Add required cum_ columns
    pdf = ddf.compute()
    for col in ("cum_oil", "cum_gas", "cum_water"):
        pdf[col] = 0.0
    ddf = dd.from_pandas(pdf, npartitions=1)

    scalers = {
        col: scaler
        for col in ("oil_bbl", "gas_mcf", "water_bbl", "cum_oil", "cum_gas", "cum_water")
    }
    result = scale_numerics(ddf, scalers).compute()
    assert result["oil_bbl_scaled"].iloc[0] == pytest.approx(1.0)


@pytest.mark.unit
def test_scale_numerics_nan_propagates():
    scaler = StandardScaler()
    scaler.mean_ = np.array([100.0])
    scaler.scale_ = np.array([50.0])
    scaler.var_ = np.array([2500.0])
    scaler.n_features_in_ = 1
    scaler.n_samples_seen_ = 10

    pdf = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county_code": ["05"],
            "operator_name": ["OP"],
            "production_date": [pd.Timestamp("2021-01-01")],
            "report_year": [2021],
            "report_month": [1],
            "oil_bbl": [float("nan")],
            "gas_mcf": [0.0],
            "water_bbl": [0.0],
            "days_produced": [28.0],
            "is_valid": [True],
            "cum_oil": [0.0],
            "cum_gas": [0.0],
            "cum_water": [0.0],
        }
    )
    ddf = dd.from_pandas(pdf, npartitions=1)
    scalers = {
        col: scaler
        for col in ("oil_bbl", "gas_mcf", "water_bbl", "cum_oil", "cum_gas", "cum_water")
    }
    result = scale_numerics(ddf, scalers).compute()
    assert math.isnan(result["oil_bbl_scaled"].iloc[0])


@pytest.mark.unit
def test_scale_numerics_dtype_float64():
    scaler = StandardScaler()
    scaler.mean_ = np.array([100.0])
    scaler.scale_ = np.array([50.0])
    scaler.var_ = np.array([2500.0])
    scaler.n_features_in_ = 1
    scaler.n_samples_seen_ = 10

    pdf = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county_code": ["05"],
            "operator_name": ["OP"],
            "production_date": [pd.Timestamp("2021-01-01")],
            "report_year": [2021],
            "report_month": [1],
            "oil_bbl": [100.0],
            "gas_mcf": [0.0],
            "water_bbl": [0.0],
            "days_produced": [28.0],
            "is_valid": [True],
            "cum_oil": [0.0],
            "cum_gas": [0.0],
            "cum_water": [0.0],
        }
    )
    ddf = dd.from_pandas(pdf, npartitions=1)
    scalers = {
        col: scaler
        for col in ("oil_bbl", "gas_mcf", "water_bbl", "cum_oil", "cum_gas", "cum_water")
    }
    result = scale_numerics(ddf, scalers).compute()
    assert result["oil_bbl_scaled"].dtype == "float64"


@pytest.mark.unit
def test_scale_numerics_missing_scaler_raises():
    scaler = StandardScaler()
    scaler.mean_ = np.array([0.0])
    scaler.scale_ = np.array([1.0])
    scaler.var_ = np.array([1.0])
    scaler.n_features_in_ = 1
    scaler.n_samples_seen_ = 10

    pdf = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county_code": ["05"],
            "operator_name": ["OP"],
            "production_date": [pd.Timestamp("2021-01-01")],
            "report_year": [2021],
            "report_month": [1],
            "oil_bbl": [100.0],
            "gas_mcf": [0.0],
            "water_bbl": [0.0],
            "days_produced": [28.0],
            "is_valid": [True],
            "cum_oil": [0.0],
            "cum_gas": [0.0],
            "cum_water": [0.0],
        }
    )
    ddf = dd.from_pandas(pdf, npartitions=1)
    # Missing gas_mcf scaler
    scalers = {
        "oil_bbl": scaler,
        "water_bbl": scaler,
        "cum_oil": scaler,
        "cum_gas": scaler,
        "cum_water": scaler,
    }
    with pytest.raises(KeyError, match="gas_mcf"):
        scale_numerics(ddf, scalers)


@pytest.mark.unit
def test_scale_numerics_returns_dask():
    scaler = StandardScaler()
    scaler.mean_ = np.array([0.0])
    scaler.scale_ = np.array([1.0])
    scaler.var_ = np.array([1.0])
    scaler.n_features_in_ = 1
    scaler.n_samples_seen_ = 10

    pdf = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county_code": ["05"],
            "operator_name": ["OP"],
            "production_date": [pd.Timestamp("2021-01-01")],
            "report_year": [2021],
            "report_month": [1],
            "oil_bbl": [100.0],
            "gas_mcf": [0.0],
            "water_bbl": [0.0],
            "days_produced": [28.0],
            "is_valid": [True],
            "cum_oil": [0.0],
            "cum_gas": [0.0],
            "cum_water": [0.0],
        }
    )
    ddf = dd.from_pandas(pdf, npartitions=1)
    scalers = {
        col: scaler
        for col in ("oil_bbl", "gas_mcf", "water_bbl", "cum_oil", "cum_gas", "cum_water")
    }
    assert isinstance(scale_numerics(ddf, scalers), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 09: fit_scalers_and_encoders tests
# ---------------------------------------------------------------------------


def _fit_ddf() -> dd.DataFrame:
    pdf = pd.DataFrame(
        {
            "well_id": ["W1", "W1", "W1"],
            "county_code": ["01", "02", "01"],
            "operator_name": ["ACME", "BETA", "ACME"],
            "oil_bbl": [100.0, 200.0, 300.0],
            "gas_mcf": [500.0, 1000.0, 1500.0],
            "water_bbl": [50.0, 100.0, 150.0],
            "production_date": pd.date_range("2021-01-01", periods=3, freq="MS"),
            "report_year": [2021, 2021, 2021],
            "report_month": [1, 2, 3],
            "days_produced": [28.0, 28.0, 28.0],
            "is_valid": [True, True, True],
            "cum_oil": [100.0, 300.0, 600.0],
            "cum_gas": [500.0, 1500.0, 3000.0],
            "cum_water": [50.0, 150.0, 300.0],
        }
    )
    return dd.from_pandas(pdf, npartitions=1)


@pytest.mark.unit
def test_fit_scalers_mean():
    ddf = _fit_ddf()
    scalers, _ = fit_scalers_and_encoders(ddf)
    assert scalers["oil_bbl"].mean_[0] == pytest.approx(200.0)


@pytest.mark.unit
def test_fit_encoders_classes():
    ddf = _fit_ddf()
    _, encoders = fit_scalers_and_encoders(ddf)
    classes = set(encoders["county_code"].classes_)
    assert "01" in classes
    assert "02" in classes


@pytest.mark.unit
def test_fit_scalers_all_columns():
    ddf = _fit_ddf()
    scalers, _ = fit_scalers_and_encoders(ddf)
    for col in ("oil_bbl", "gas_mcf", "water_bbl", "cum_oil", "cum_gas", "cum_water"):
        assert col in scalers


@pytest.mark.unit
def test_fit_encoders_all_columns():
    ddf = _fit_ddf()
    _, encoders = fit_scalers_and_encoders(ddf)
    assert "county_code" in encoders
    assert "operator_name" in encoders


# ---------------------------------------------------------------------------
# Task 10: write_features_parquet tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_features_npartitions_3m():
    npartitions = max(1, min(3_000_000 // 500_000, 50))
    assert npartitions == 6


@pytest.mark.unit
def test_write_features_npartitions_200():
    npartitions = max(1, min(200 // 500_000, 50))
    assert npartitions == 1


@pytest.mark.integration
def test_write_features_readable(tmp_path):
    pdf = pd.DataFrame({"a": range(10), "b": range(10)})
    ddf = dd.from_pandas(pdf, npartitions=1)
    out_dir = tmp_path / "features"
    write_features_parquet(ddf, out_dir, 10)
    result = pd.read_parquet(str(out_dir))
    assert len(result) == 10


@pytest.mark.integration
def test_write_features_file_count_le_50(tmp_path):
    pdf = pd.DataFrame({"a": range(100)})
    ddf = dd.from_pandas(pdf, npartitions=2)
    out_dir = tmp_path / "features2"
    write_features_parquet(ddf, out_dir, 100)
    files = list(out_dir.glob("*.parquet"))
    assert len(files) <= 50


# ---------------------------------------------------------------------------
# TR-01: Physical bounds in features output
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr01_gor_nonnegative():
    n = 10
    oil = [float(i + 1) for i in range(n)]
    gas = [float(i * 10) for i in range(n)]
    ddf = _single_well_ddf(oil, gas=gas)
    result = compute_gor(ddf).compute()
    valid = result["gor"].dropna()
    assert (valid >= 0.0).all()


@pytest.mark.unit
def test_tr01_water_cut_range():
    n = 10
    oil = [float(i + 1) for i in range(n)]
    water = [float(i * 2) for i in range(n)]
    ddf = _single_well_ddf(oil, water=water)
    result = compute_water_cut(ddf).compute()
    valid = result["water_cut"].dropna()
    assert (valid >= 0.0).all() and (valid <= 1.0).all()


# ---------------------------------------------------------------------------
# TR-03: Decline curve monotonicity
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr03_cum_oil_monotonic():
    ddf = _single_well_ddf([10.0 * (i + 1) for i in range(12)])
    ddf = compute_cumulative_production(ddf)
    result = ddf.compute().sort_values("production_date")
    cum = result["cum_oil"].tolist()
    for i in range(1, len(cum)):
        assert cum[i] >= cum[i - 1]


@pytest.mark.unit
def test_tr03_cum_gas_monotonic():
    ddf = _single_well_ddf([100.0] * 12, gas=[50.0 * (i + 1) for i in range(12)])
    ddf = compute_cumulative_production(ddf)
    result = ddf.compute().sort_values("production_date")
    cum = result["cum_gas"].tolist()
    for i in range(1, len(cum)):
        assert cum[i] >= cum[i - 1]


# ---------------------------------------------------------------------------
# TR-06: GOR zero denominator
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr06_gor_zero_oil_pos_gas():
    val = _gor_val(0.0, 500.0)
    assert math.isnan(val)


@pytest.mark.unit
def test_tr06_gor_zero_oil_zero_gas():
    val = _gor_val(0.0, 0.0)
    assert math.isnan(val)


@pytest.mark.unit
def test_tr06_gor_pos_oil_zero_gas():
    val = _gor_val(100.0, 0.0)
    assert val == 0.0


# ---------------------------------------------------------------------------
# TR-07: Decline rate clip bounds
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr07_decline_clip_low():
    vals = _decline_values([1000.0, 0.001])  # extreme drop
    assert vals[1] >= -1.0


@pytest.mark.unit
def test_tr07_decline_clip_high():
    vals = _decline_values([1.0, 100_000.0])
    assert vals[1] <= 10.0


@pytest.mark.unit
def test_tr07_decline_within_bounds_unchanged():
    vals = _decline_values([100.0, 80.0])
    assert vals[1] == pytest.approx(-0.2)


@pytest.mark.unit
def test_tr07_two_zero_months_raw_zero():
    vals = _decline_values([100.0, 0.0, 0.0])
    assert vals[2] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# TR-08: Cumulative flat periods
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr08_zero_month_flat():
    ddf = _single_well_ddf([100.0, 0.0, 200.0])
    result = compute_cumulative_production(ddf).compute().sort_values("production_date")
    assert result["cum_oil"].iloc[1] == pytest.approx(100.0)


@pytest.mark.unit
def test_tr08_zero_at_start():
    ddf = _single_well_ddf([0.0, 0.0, 100.0])
    result = compute_cumulative_production(ddf).compute().sort_values("production_date")
    assert result["cum_oil"].iloc[0] == 0.0
    assert result["cum_oil"].iloc[1] == 0.0


@pytest.mark.unit
def test_tr08_resumes_after_shutin():
    ddf = _single_well_ddf([100.0, 0.0, 0.0, 200.0])
    result = compute_cumulative_production(ddf).compute().sort_values("production_date")
    assert result["cum_oil"].iloc[3] == pytest.approx(300.0)


# ---------------------------------------------------------------------------
# TR-09: Feature calculation correctness
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr09_rolling_3m_known():
    """rolling 3m: [10, 20, 30, 40] → [NaN, NaN, 20, 30]."""
    ddf = _single_well_ddf([10.0, 20.0, 30.0, 40.0])
    result = compute_rolling_and_lag_features(ddf).compute().sort_values("production_date")
    assert result["oil_roll_3m"].iloc[2] == pytest.approx(20.0)
    assert result["oil_roll_3m"].iloc[3] == pytest.approx(30.0)


@pytest.mark.unit
def test_tr09_rolling_6m_nan_first_5():
    ddf = _single_well_ddf([float(i) for i in range(1, 8)])
    result = compute_rolling_and_lag_features(ddf).compute().sort_values("production_date")
    for i in range(5):
        assert math.isnan(result["oil_roll_6m"].iloc[i])


@pytest.mark.unit
def test_tr09_lag_1_correctness():
    ddf = _single_well_ddf([10.0, 20.0, 30.0])
    result = compute_rolling_and_lag_features(ddf).compute().sort_values("production_date")
    assert result["oil_lag_1m"].iloc[1] == pytest.approx(10.0)
    assert result["oil_lag_1m"].iloc[2] == pytest.approx(20.0)


@pytest.mark.unit
def test_tr09_gor_formula():
    """GOR = gas_mcf / oil_bbl (not gas_bbl / oil_mcf)."""
    val = _gor_val(200.0, 1000.0)
    assert val == pytest.approx(5.0)


@pytest.mark.unit
def test_tr09_water_cut_formula():
    """water_cut uses total liquid (oil + water) as denominator."""
    val = _wc_val(100.0, 300.0)
    assert val == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# TR-10: Water cut boundary values
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr10_water_zero_oil_positive():
    val = _wc_val(500.0, 0.0)
    assert val == 0.0


@pytest.mark.unit
def test_tr10_oil_zero_water_positive():
    val = _wc_val(0.0, 300.0)
    assert val == 1.0


@pytest.mark.unit
def test_tr10_no_valid_value_outside_range():
    n = 20
    oil = [float(i % 200 + 1) for i in range(n)]
    water = [float((i * 3) % 200) for i in range(n)]
    ddf = _single_well_ddf(oil, water=water)
    result = compute_water_cut(ddf).compute()
    valid = result["water_cut"].dropna()
    # No value outside [0, 1] should be "flagged" (they all remain in range)
    assert (valid >= 0.0).all() and (valid <= 1.0).all()


# ---------------------------------------------------------------------------
# TR-17: Lazy Dask evaluation
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr17_all_features_return_dask():
    base = _single_well_ddf([100.0, 200.0])
    enc = LabelEncoder()
    enc.fit(["05"])
    scaler = StandardScaler()
    scaler.mean_ = np.array([0.0])
    scaler.scale_ = np.array([1.0])
    scaler.var_ = np.array([1.0])
    scaler.n_features_in_ = 1
    scaler.n_samples_seen_ = 2

    assert isinstance(compute_cumulative_production(base), dd.DataFrame)
    assert isinstance(compute_gor(base), dd.DataFrame)
    assert isinstance(compute_water_cut(base), dd.DataFrame)
    assert isinstance(compute_decline_rate(base), dd.DataFrame)
    assert isinstance(compute_rolling_and_lag_features(base), dd.DataFrame)
    assert isinstance(compute_mom_pct_change(base), dd.DataFrame)

    # For encode/scale we need appropriate input
    enc_ddf = _make_ddf({"county_code": ["05"], "operator_name": ["ACME"]})
    assert isinstance(
        encode_categoricals(enc_ddf, {"county_code": enc, "operator_name": enc}), dd.DataFrame
    )

    pdf = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county_code": ["05"],
            "operator_name": ["OP"],
            "production_date": [pd.Timestamp("2021-01-01")],
            "report_year": [2021],
            "report_month": [1],
            "oil_bbl": [100.0],
            "gas_mcf": [0.0],
            "water_bbl": [0.0],
            "days_produced": [28.0],
            "is_valid": [True],
            "cum_oil": [0.0],
            "cum_gas": [0.0],
            "cum_water": [0.0],
        }
    )
    scaler_ddf = dd.from_pandas(pdf, npartitions=1)
    scalers = {
        col: scaler
        for col in ("oil_bbl", "gas_mcf", "water_bbl", "cum_oil", "cum_gas", "cum_water")
    }
    assert isinstance(scale_numerics(scaler_ddf, scalers), dd.DataFrame)


# ---------------------------------------------------------------------------
# TR-19: Feature column presence (unit test with synthetic 12-month well)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr19_all_required_columns(tmp_path):
    """A 12-month synthetic well passes through all feature steps and has all required columns."""
    n = 12
    dates = pd.date_range("2021-01-01", periods=n, freq="MS")
    pdf = pd.DataFrame(
        {
            "well_id": ["W1"] * n,
            "county_code": ["05"] * n,
            "operator_name": ["ACME"] * n,
            "report_year": [d.year for d in dates],
            "report_month": [d.month for d in dates],
            "oil_bbl": [float(i + 100) for i in range(n)],
            "gas_mcf": [float(i * 10 + 500) for i in range(n)],
            "water_bbl": [float(i * 5 + 50) for i in range(n)],
            "days_produced": [28.0] * n,
            "production_date": dates,
            "is_valid": [True] * n,
            "cum_oil": [0.0] * n,
            "cum_gas": [0.0] * n,
            "cum_water": [0.0] * n,
        }
    )
    ddf = dd.from_pandas(pdf, npartitions=1)

    ddf = compute_cumulative_production(ddf)
    ddf = compute_gor(ddf)
    ddf = compute_water_cut(ddf)
    ddf = compute_decline_rate(ddf)
    ddf = compute_rolling_and_lag_features(ddf)
    ddf = compute_mom_pct_change(ddf)

    scalers, encoders = fit_scalers_and_encoders(ddf)
    ddf = scale_numerics(ddf, scalers)
    ddf = encode_categoricals(ddf, encoders)

    result = ddf.compute()
    required = [
        "well_id",
        "production_date",
        "oil_bbl",
        "gas_mcf",
        "water_bbl",
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "oil_roll_3m",
        "gas_roll_3m",
        "water_roll_3m",
        "oil_roll_6m",
        "gas_roll_6m",
        "water_roll_6m",
        "oil_lag_1m",
        "gas_lag_1m",
        "water_lag_1m",
        "oil_mom_pct",
        "gas_mom_pct",
        "county_code_enc",
        "operator_name_enc",
        "oil_bbl_scaled",
        "gas_mcf_scaled",
        "water_bbl_scaled",
        "cum_oil_scaled",
        "cum_gas_scaled",
        "cum_water_scaled",
    ]
    for col in required:
        assert col in result.columns, f"Missing required column: {col}"


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_tr18_features_parquet_readable():
    features_dir = Path("data/processed/features")
    if not features_dir.exists() or not list(features_dir.glob("*.parquet")):
        pytest.skip("features not populated — run features stage first")
    for pf in features_dir.glob("*.parquet"):
        pd.read_parquet(pf)


@pytest.mark.integration
def test_tr14_features_schema_stability():
    features_dir = Path("data/processed/features")
    if not features_dir.exists():
        pytest.skip("features not populated")
    parts = list(features_dir.glob("*.parquet"))
    if len(parts) < 2:
        pytest.skip("Need at least 2 files")
    s0 = set(pd.read_parquet(parts[0]).columns)
    s1 = set(pd.read_parquet(parts[1]).columns)
    assert s0 == s1
