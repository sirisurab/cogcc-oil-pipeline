"""Tests for cogcc_pipeline.features."""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from cogcc_pipeline.features import (
    apply_well_features_parallel,
    compute_cumulative_production,
    compute_decline_rate,
    compute_ratios,
    compute_rolling_and_lag,
    compute_well_age_and_normalised,
    encode_categoricals,
    engineer_well_features,
    run_features,
    scale_numeric_features,
    validate_features_schema,
    write_features_parquet,
)


@pytest.fixture()
def logger() -> logging.Logger:
    return logging.getLogger("test_features")


def _single_well_df(nmonths: int = 12, oil: list[float] | None = None) -> pd.DataFrame:
    """Build a synthetic single-well DataFrame with specified months."""
    if oil is None:
        oil = [float(100 + i * 10) for i in range(nmonths)]
    return pd.DataFrame(
        {
            "well_id": pd.array(["0500100001"] * nmonths, dtype=pd.StringDtype()),
            "production_date": pd.date_range("2022-01-01", periods=nmonths, freq="MS"),
            "oil_bbl": pd.array(oil, dtype="float64"),
            "gas_mcf": pd.array([float(500 + i * 5) for i in range(nmonths)], dtype="float64"),
            "water_bbl": pd.array([float(200 + i * 2) for i in range(nmonths)], dtype="float64"),
            "days_produced": pd.array([28.0] * nmonths, dtype="float64"),
            "OpName": pd.array(["Op X"] * nmonths, dtype=pd.StringDtype()),
            "Well": pd.array(["Well A"] * nmonths, dtype=pd.StringDtype()),
        }
    )


# ---------------------------------------------------------------------------
# compute_cumulative_production tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cumulative_is_monotonically_nondecreasing() -> None:
    oil = [float(x) for x in [100, 200, 150, 0, 0, 50, 75, 100, 80, 90, 110, 120]]
    df = _single_well_df(12, oil)
    result = compute_cumulative_production(df)
    cum = result["cum_oil"].tolist()
    for i in range(1, len(cum)):
        assert cum[i] >= cum[i - 1], f"Monotonicity violated at index {i}"


@pytest.mark.unit
def test_cumulative_flat_during_shutdown() -> None:
    oil = [float(x) for x in [100, 200, 0, 0, 150]]
    df = _single_well_df(5, oil)
    result = compute_cumulative_production(df)
    assert result["cum_oil"].iloc[2] == result["cum_oil"].iloc[3]


@pytest.mark.unit
def test_cumulative_zero_until_first_production() -> None:
    oil = [float(x) for x in [0, 0, 0, 100, 200]]
    df = _single_well_df(5, oil)
    result = compute_cumulative_production(df)
    assert result["cum_oil"].iloc[0] == 0.0
    assert result["cum_oil"].iloc[1] == 0.0
    assert result["cum_oil"].iloc[2] == 0.0
    assert result["cum_oil"].iloc[3] == 100.0


@pytest.mark.unit
def test_cumulative_resumes_after_shutin() -> None:
    oil = [float(x) for x in [100, 200, 0, 0, 150]]
    df = _single_well_df(5, oil)
    result = compute_cumulative_production(df)
    assert result["cum_oil"].iloc[4] == 100 + 200 + 0 + 0 + 150


@pytest.mark.unit
def test_cumulative_nan_treated_as_zero() -> None:
    oil_raw = [100.0, float("nan"), 200.0]
    df = pd.DataFrame(
        {
            "production_date": pd.date_range("2022-01-01", periods=3, freq="MS"),
            "oil_bbl": oil_raw,
            "gas_mcf": [500.0, 500.0, 500.0],
            "water_bbl": [200.0, 200.0, 200.0],
            "days_produced": [28.0, 28.0, 28.0],
        }
    )
    result = compute_cumulative_production(df)
    assert result["cum_oil"].iloc[2] == 300.0  # 100 + 0 (nan) + 200


# ---------------------------------------------------------------------------
# compute_ratios tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_gor_zero_oil_positive_gas_is_nan() -> None:
    df = pd.DataFrame({"oil_bbl": [0.0], "gas_mcf": [100.0], "water_bbl": [0.0]})
    result = compute_ratios(df)
    assert math.isnan(result["gor"].iloc[0])


@pytest.mark.unit
def test_gor_zero_oil_zero_gas_is_nan() -> None:
    df = pd.DataFrame({"oil_bbl": [0.0], "gas_mcf": [0.0], "water_bbl": [0.0]})
    result = compute_ratios(df)
    assert math.isnan(result["gor"].iloc[0])


@pytest.mark.unit
def test_gor_positive_oil_zero_gas_is_zero() -> None:
    df = pd.DataFrame({"oil_bbl": [100.0], "gas_mcf": [0.0], "water_bbl": [0.0]})
    result = compute_ratios(df)
    assert result["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_gor_correct_formula() -> None:
    df = pd.DataFrame({"oil_bbl": [200.0], "gas_mcf": [400.0], "water_bbl": [0.0]})
    result = compute_ratios(df)
    assert result["gor"].iloc[0] == 2.0


@pytest.mark.unit
def test_gor_formula_gas_over_oil() -> None:
    df = pd.DataFrame({"oil_bbl": [1.0], "gas_mcf": [5.0], "water_bbl": [0.0]})
    result = compute_ratios(df)
    assert result["gor"].iloc[0] == 5.0


@pytest.mark.unit
def test_water_cut_formula() -> None:
    df = pd.DataFrame({"oil_bbl": [70.0], "gas_mcf": [0.0], "water_bbl": [30.0]})
    result = compute_ratios(df)
    assert abs(result["water_cut"].iloc[0] - 0.3) < 1e-9


@pytest.mark.unit
def test_water_cut_zero_water_is_zero() -> None:
    df = pd.DataFrame({"oil_bbl": [500.0], "gas_mcf": [0.0], "water_bbl": [0.0]})
    result = compute_ratios(df)
    assert result["water_cut"].iloc[0] == 0.0


@pytest.mark.unit
def test_water_cut_all_water_is_one() -> None:
    df = pd.DataFrame({"oil_bbl": [0.0], "gas_mcf": [0.0], "water_bbl": [500.0]})
    result = compute_ratios(df)
    assert result["water_cut"].iloc[0] == 1.0


@pytest.mark.unit
def test_water_cut_zero_denominator_is_nan() -> None:
    df = pd.DataFrame({"oil_bbl": [0.0], "gas_mcf": [0.0], "water_bbl": [0.0]})
    result = compute_ratios(df)
    assert math.isnan(result["water_cut"].iloc[0])


# ---------------------------------------------------------------------------
# compute_decline_rate tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decline_rate_clipped_below() -> None:
    # prev=100, curr=350 → raw = (100-350)/100 = -2.5, clipped to -1.0
    oil = [100.0, 350.0]
    df = _single_well_df(2, oil)
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] == -1.0


@pytest.mark.unit
def test_decline_rate_clipped_above() -> None:
    # prev=10, curr=0 → raw=(10-0)/10=1.0, that's fine; for >10 need large prev drop
    # prev=1, curr=0 → (1-0)/1=1.0
    # To get >10: prev=100, curr=-900 ... use prev=1 and curr=0 → 1.0 (not >10)
    # better: prev=1, curr=0 is 1.0. To trigger clip >10: prev=100, curr=-900 impossible
    # prev=10, curr=0 → rate=1.0; prev=1, curr=0 → rate=1.0
    # Construct: prev=100, curr=0 → rate=1.0. Can't exceed 10 naturally.
    # Workaround: Mock the underlying series to produce a large intermediate value.
    # Per spec: pre-clip rate 15 → assert clipped to 10
    # rate = (prev-curr)/prev → 15 = (prev-curr)/prev → curr = prev*(1-15) = -14*prev
    # prev=1, curr=-14 → rate = (1-(-14))/1 = 15
    oil = [1.0, -14.0]  # negative is allowed pre-clip (will be clipped)
    df = _single_well_df(2, oil)
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] == 10.0


@pytest.mark.unit
def test_decline_rate_within_bounds_unchanged() -> None:
    # prev=200, curr=100 → rate=(200-100)/200=0.5
    oil = [200.0, 100.0]
    df = _single_well_df(2, oil)
    result = compute_decline_rate(df)
    assert abs(result["decline_rate"].iloc[1] - 0.5) < 1e-9


@pytest.mark.unit
def test_decline_rate_both_zero() -> None:
    oil = [0.0, 0.0]
    df = _single_well_df(2, oil)
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] == 0.0


@pytest.mark.unit
def test_decline_rate_prev_zero_curr_positive() -> None:
    oil = [0.0, 100.0]
    df = _single_well_df(2, oil)
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] == -1.0


@pytest.mark.unit
def test_decline_rate_first_record_is_nan() -> None:
    oil = [100.0, 200.0]
    df = _single_well_df(2, oil)
    result = compute_decline_rate(df)
    assert math.isnan(result["decline_rate"].iloc[0])


# ---------------------------------------------------------------------------
# compute_rolling_and_lag tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rolling_3m_value_at_index_2() -> None:
    oil = [100.0, 200.0, 150.0, 300.0, 250.0, 400.0]
    df = _single_well_df(6, oil)
    result = compute_rolling_and_lag(df, [3, 6, 12])
    expected = (100 + 200 + 150) / 3
    assert abs(result["oil_bbl_rolling_3m"].iloc[2] - expected) < 1e-6


@pytest.mark.unit
def test_rolling_3m_value_at_index_5() -> None:
    oil = [100.0, 200.0, 150.0, 300.0, 250.0, 400.0]
    df = _single_well_df(6, oil)
    result = compute_rolling_and_lag(df, [3, 6, 12])
    expected = (300 + 250 + 400) / 3
    assert abs(result["oil_bbl_rolling_3m"].iloc[5] - expected) < 0.01


@pytest.mark.unit
def test_rolling_partial_window_not_nan() -> None:
    oil = [100.0, 200.0]
    df = _single_well_df(2, oil)
    result = compute_rolling_and_lag(df, [3])
    # With min_periods=1, index 1 should use 2 values
    assert not math.isnan(result["oil_bbl_rolling_3m"].iloc[1])
    assert abs(result["oil_bbl_rolling_3m"].iloc[1] - 150.0) < 1e-6


@pytest.mark.unit
def test_lag1_values_correct() -> None:
    oil = [100.0, 200.0, 300.0]
    df = _single_well_df(3, oil)
    result = compute_rolling_and_lag(df, [3])
    assert math.isnan(result["oil_bbl_lag1"].iloc[0])
    assert result["oil_bbl_lag1"].iloc[1] == 100.0
    assert result["oil_bbl_lag1"].iloc[2] == 200.0


@pytest.mark.unit
def test_rolling_columns_present() -> None:
    df = _single_well_df(12)
    result = compute_rolling_and_lag(df, [3, 6, 12])
    for col in ("oil_bbl_rolling_3m", "oil_bbl_rolling_6m", "oil_bbl_rolling_12m"):
        assert col in result.columns


# ---------------------------------------------------------------------------
# compute_well_age_and_normalised tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_well_age_months_first_is_zero() -> None:
    df = _single_well_df(12)
    result = compute_well_age_and_normalised(df)
    assert result["well_age_months"].iloc[0] == 0
    assert result["well_age_months"].iloc[11] == 11


@pytest.mark.unit
def test_days_normalised_production() -> None:
    df = pd.DataFrame(
        {
            "production_date": pd.date_range("2022-01-01", periods=1, freq="MS"),
            "oil_bbl": [300.0],
            "gas_mcf": [500.0],
            "water_bbl": [200.0],
            "days_produced": [15.0],
        }
    )
    result = compute_well_age_and_normalised(df)
    assert abs(result["oil_bbl_30d"].iloc[0] - 600.0) < 1e-6


@pytest.mark.unit
def test_days_normalised_zero_days_is_nan() -> None:
    df = pd.DataFrame(
        {
            "production_date": pd.date_range("2022-01-01", periods=1, freq="MS"),
            "oil_bbl": [300.0],
            "gas_mcf": [500.0],
            "water_bbl": [200.0],
            "days_produced": [0.0],
        }
    )
    result = compute_well_age_and_normalised(df)
    assert math.isnan(result["oil_bbl_30d"].iloc[0])


@pytest.mark.unit
def test_days_normalised_null_days_is_nan() -> None:
    df = pd.DataFrame(
        {
            "production_date": pd.date_range("2022-01-01", periods=1, freq="MS"),
            "oil_bbl": [300.0],
            "gas_mcf": [500.0],
            "water_bbl": [200.0],
            "days_produced": [float("nan")],
        }
    )
    result = compute_well_age_and_normalised(df)
    assert math.isnan(result["oil_bbl_30d"].iloc[0])


# ---------------------------------------------------------------------------
# engineer_well_features tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_engineer_well_features_all_columns_present() -> None:
    df = _single_well_df(12)
    result = engineer_well_features(df, [3, 6, 12])
    for col in (
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "well_age_months",
        "oil_bbl_30d",
        "gas_mcf_30d",
        "water_bbl_30d",
        "oil_bbl_rolling_3m",
        "oil_bbl_rolling_6m",
        "oil_bbl_rolling_12m",
        "oil_bbl_lag1",
    ):
        assert col in result.columns, f"Missing column: {col}"


@pytest.mark.unit
def test_engineer_well_features_rolling_correct() -> None:
    oil = [100.0, 200.0, 150.0] + [0.0] * 9
    df = _single_well_df(12, oil)
    result = engineer_well_features(df, [3, 6, 12])
    expected_3m_idx2 = (100 + 200 + 150) / 3
    assert abs(result["oil_bbl_rolling_3m"].iloc[2] - expected_3m_idx2) < 1e-6


@pytest.mark.unit
def test_engineer_well_features_physical_constraints() -> None:
    df = _single_well_df(12)
    result = engineer_well_features(df, [3, 6, 12])
    # cum_oil >= 0
    assert (result["cum_oil"] >= 0).all()
    # water_cut in [0, 1] or NaN
    wc = result["water_cut"].dropna()
    assert ((wc >= 0) & (wc <= 1)).all()
    # decline_rate clipped [-1, 10] or NaN
    dr = result["decline_rate"].dropna()
    assert ((dr >= -1.0) & (dr <= 10.0)).all()


# ---------------------------------------------------------------------------
# apply_well_features_parallel tests
# ---------------------------------------------------------------------------


def _two_well_ddf() -> dd.DataFrame:
    rows = []
    for wid in ["0500100001", "0500100002"]:
        for i in range(6):
            rows.append(
                {
                    "well_id": wid,
                    "production_date": pd.Timestamp("2022-01-01") + pd.DateOffset(months=i),
                    "oil_bbl": 100.0 + i,
                    "gas_mcf": 500.0 + i,
                    "water_bbl": 200.0 + i,
                    "days_produced": 28.0,
                    "OpName": "Op X",
                }
            )
    df = pd.DataFrame(rows)
    return dd.from_pandas(df, npartitions=2)


@pytest.mark.unit
def test_apply_well_features_parallel_returns_dask(logger: logging.Logger) -> None:
    ddf = _two_well_ddf()
    result = apply_well_features_parallel(ddf, [3, 6, 12], logger)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_apply_well_features_parallel_cum_oil_monotonic(logger: logging.Logger) -> None:
    ddf = _two_well_ddf()
    result = apply_well_features_parallel(ddf, [3, 6, 12], logger).compute()
    for wid, grp in result.groupby("well_id"):
        grp = grp.sort_values("production_date")
        cum = grp["cum_oil"].tolist()
        for i in range(1, len(cum)):
            assert cum[i] >= cum[i - 1] - 1e-9


# ---------------------------------------------------------------------------
# encode_categoricals tests
# ---------------------------------------------------------------------------


def _encode_ddf() -> dd.DataFrame:
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001", "0500100002", "0500100001"], dtype=pd.StringDtype()),
            "production_date": pd.date_range("2022-01-01", periods=3, freq="MS"),
            "oil_bbl": [100.0, 200.0, 300.0],
            "OpName": pd.array(["Alpha", "Beta", "Gamma"], dtype=pd.StringDtype()),
        }
    )
    return dd.from_pandas(df, npartitions=1)


@pytest.mark.unit
def test_encode_categoricals_integer_values(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _encode_ddf()
    result = encode_categoricals(ddf, tmp_path / "encoders.json", logger)
    df = result.compute()
    assert "op_name_encoded" in df.columns
    assert df["op_name_encoded"].between(0, 2).all()


@pytest.mark.unit
def test_encode_categoricals_json_written(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _encode_ddf()
    enc_path = tmp_path / "encoders.json"
    encode_categoricals(ddf, enc_path, logger)
    assert enc_path.exists()
    data = json.loads(enc_path.read_text())
    assert "op_name" in data
    assert "county_code" in data


@pytest.mark.unit
def test_encode_categoricals_returns_dask(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _encode_ddf()
    result = encode_categoricals(ddf, tmp_path / "enc.json", logger)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# scale_numeric_features tests
# ---------------------------------------------------------------------------


def _scale_ddf() -> dd.DataFrame:
    n = 100
    df = pd.DataFrame(
        {
            "oil_bbl": np.random.randn(n) * 100 + 500,
            "gas_mcf": np.random.randn(n) * 200 + 1000,
            "water_bbl": np.random.randn(n) * 50 + 200,
            "cum_oil": np.linspace(0, 5000, n),
            "cum_gas": np.linspace(0, 10000, n),
            "cum_water": np.linspace(0, 2000, n),
            "gor": np.random.randn(n) + 5,
            "water_cut": np.random.uniform(0, 1, n),
            "decline_rate": np.random.uniform(-1, 1, n),
            "well_age_months": np.arange(n, dtype="int64"),
            "oil_bbl_rolling_3m": np.random.randn(n) * 100 + 500,
            "oil_bbl_rolling_6m": np.random.randn(n) * 100 + 500,
            "oil_bbl_rolling_12m": np.random.randn(n) * 100 + 500,
        }
    )
    return dd.from_pandas(df, npartitions=2)


@pytest.mark.unit
def test_scale_features_mean_approx_zero(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _scale_ddf()
    result = scale_numeric_features(ddf, tmp_path / "scaler.joblib", logger)
    df = result.compute()
    assert abs(df["oil_bbl"].mean()) < 0.1


@pytest.mark.unit
def test_scale_features_scaler_saved(tmp_path: Path, logger: logging.Logger) -> None:
    import joblib  # type: ignore[import-untyped]

    ddf = _scale_ddf()
    scaler_path = tmp_path / "scaler.joblib"
    scale_numeric_features(ddf, scaler_path, logger)
    assert scaler_path.exists()
    scaler = joblib.load(scaler_path)
    assert hasattr(scaler, "mean_")


@pytest.mark.unit
def test_scale_features_returns_dask(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _scale_ddf()
    result = scale_numeric_features(ddf, tmp_path / "sc.joblib", logger)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# write_features_parquet / validate_features_schema tests
# ---------------------------------------------------------------------------


def _features_ddf() -> dd.DataFrame:
    n = 5
    data: dict[str, object] = {
        "well_id": pd.array(["0500100001"] * n, dtype=pd.StringDtype()),
        "production_date": pd.date_range("2022-01-01", periods=n, freq="MS"),
        "oil_bbl": [100.0] * n,
        "gas_mcf": [500.0] * n,
        "water_bbl": [200.0] * n,
        "days_produced": [28.0] * n,
        "cum_oil": [float(i * 100) for i in range(n)],
        "cum_gas": [float(i * 500) for i in range(n)],
        "cum_water": [float(i * 200) for i in range(n)],
        "gor": [5.0] * n,
        "water_cut": [0.3] * n,
        "decline_rate": [0.1] * n,
        "well_age_months": list(range(n)),
        "oil_bbl_30d": [107.14] * n,
        "gas_mcf_30d": [535.7] * n,
        "water_bbl_30d": [214.3] * n,
        "oil_bbl_rolling_3m": [100.0] * n,
        "oil_bbl_rolling_6m": [100.0] * n,
        "oil_bbl_rolling_12m": [100.0] * n,
        "gas_mcf_rolling_3m": [500.0] * n,
        "gas_mcf_rolling_6m": [500.0] * n,
        "gas_mcf_rolling_12m": [500.0] * n,
        "water_bbl_rolling_3m": [200.0] * n,
        "water_bbl_rolling_6m": [200.0] * n,
        "water_bbl_rolling_12m": [200.0] * n,
        "oil_bbl_lag1": [None, 100.0, 100.0, 100.0, 100.0],
        "gas_mcf_lag1": [None, 500.0, 500.0, 500.0, 500.0],
        "water_bbl_lag1": [None, 200.0, 200.0, 200.0, 200.0],
        "op_name_encoded": pd.array([0] * n, dtype=pd.Int32Dtype()),
        "county_encoded": pd.array([0] * n, dtype=pd.Int32Dtype()),
    }
    return dd.from_pandas(pd.DataFrame(data), npartitions=1)


@pytest.mark.unit
def test_write_features_parquet_readable(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _features_ddf()
    out = write_features_parquet(ddf, tmp_path, "features.parquet", logger)
    df = pd.read_parquet(out)
    assert len(df) == 5


@pytest.mark.unit
def test_validate_features_schema_all_present(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _features_ddf()
    out = write_features_parquet(ddf, tmp_path, "features.parquet", logger)
    errors = validate_features_schema(out, logger)
    assert errors == []


@pytest.mark.unit
def test_validate_features_schema_missing_column(tmp_path: Path, logger: logging.Logger) -> None:
    ddf = _features_ddf()
    out = write_features_parquet(ddf, tmp_path, "features.parquet", logger)
    # Remove a column from one partition file to simulate missing column
    pq_files = sorted(out.glob("*.parquet"))
    df = pd.read_parquet(pq_files[0]).drop(columns=["decline_rate"])
    df.to_parquet(pq_files[0], index=False, engine="pyarrow")
    errors = validate_features_schema(out, logger)
    assert any("decline_rate" in e for e in errors)


@pytest.mark.unit
def test_write_features_parquet_file_count_at_most_50(
    tmp_path: Path, logger: logging.Logger
) -> None:
    ddf = _features_ddf()
    out = write_features_parquet(ddf, tmp_path, "features.parquet", logger)
    files = list(out.glob("*.parquet"))
    assert len(files) <= 50


# ---------------------------------------------------------------------------
# run_features unit test
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_features_logs_schema_error_without_raising(
    tmp_path: Path, logger: logging.Logger
) -> None:
    """run_features should log schema errors but not raise."""
    from unittest.mock import patch

    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    # Create a synthetic cleaned Parquet
    df = pd.DataFrame(
        {
            "well_id": pd.array(["0500100001"] * 3, dtype=pd.StringDtype()),
            "production_date": pd.date_range("2022-01-01", periods=3, freq="MS"),
            "oil_bbl": [100.0, 200.0, 150.0],
            "gas_mcf": [500.0, 600.0, 550.0],
            "water_bbl": [200.0, 210.0, 205.0],
            "days_produced": [28.0, 28.0, 28.0],
            "OpName": pd.array(["Op", "Op", "Op"], dtype=pd.StringDtype()),
        }
    )
    cleaned_pq = processed_dir / "cogcc_production_cleaned.parquet"
    cleaned_pq.mkdir()
    df.to_parquet(cleaned_pq / "part.0.parquet", index=False)

    config = {
        "features": {
            "processed_dir": str(processed_dir),
            "cleaned_file": "cogcc_production_cleaned.parquet",
            "features_file": "cogcc_production_features.parquet",
            "rolling_windows": [3, 6, 12],
        }
    }
    with patch(
        "cogcc_pipeline.features.validate_features_schema",
        return_value=["missing col"],
    ):
        result = run_features(config, logger)
    assert isinstance(result, Path)
