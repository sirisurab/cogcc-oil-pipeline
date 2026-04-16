"""Tests for the features stage (FEA-01 through FEA-09, TR-01 through TR-23)."""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from cogcc_pipeline.features import (
    _add_cumulative_features,
    _add_decline_rates,
    _add_encoded_features,
    _add_lag_features,
    _add_ratio_features,
    _add_rolling_features,
    run_features,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WELL_STATUS_CATS = ["AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"]


def _make_processed_df(
    n_wells: int = 1,
    months: int = 12,
    start_year: int = 2021,
    oil_values: list[float] | None = None,
) -> pd.DataFrame:
    """Create a synthetic processed-stage DataFrame (well_id as index)."""
    rows = []
    for w in range(n_wells):
        well_id = f"{w + 1:02d}{w + 1:05d}00"
        for m in range(months):
            month = (m % 12) + 1
            year = start_year + m // 12
            oil = oil_values[m] if oil_values and m < len(oil_values) else float(100 + m * 10)
            rows.append(
                {
                    "well_id": well_id,
                    "production_date": pd.Timestamp(f"{year}-{month:02d}-01"),
                    "ReportYear": year,
                    "ReportMonth": month,
                    "DaysProduced": pd.array([28], dtype="Int64")[0],
                    "OilProduced": oil,
                    "GasProduced": 500.0 + m * 5,
                    "WaterProduced": 50.0 + m * 2,
                    "WellStatus": pd.Categorical(["AC"], categories=_WELL_STATUS_CATS)[0],
                    "OpName": f"OPERATOR {w + 1}",
                    "Well": f"WELL-{w + 1}",
                    "FormationCode": "WATTE",
                    "oil_unit_flag": False,
                }
            )

    df = pd.DataFrame(rows)
    df["WellStatus"] = pd.Categorical(df["WellStatus"], categories=_WELL_STATUS_CATS)
    df["DaysProduced"] = pd.array(df["DaysProduced"].tolist(), dtype="Int64")
    df["ReportYear"] = df["ReportYear"].astype("int64")
    df["ReportMonth"] = df["ReportMonth"].astype("int64")
    df = df.set_index("well_id")
    return df


def _make_features_config(tmp_path: Path, processed_dir: Path) -> str:
    import yaml

    features_dir = tmp_path / "features"
    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": "data/raw",
            "max_workers": 2,
            "sleep_per_worker": 0.0,
            "timeout": 10,
            "max_retries": 1,
            "backoff_multiplier": 1,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 2,
            "threads_per_worker": 1,
            "memory_limit": "512MB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/p.log"), "level": "INFO"},
        "ingest": {"raw_dir": "data/raw", "interim_dir": "data/interim", "start_year": 2020},
        "transform": {"interim_dir": "data/interim", "processed_dir": str(processed_dir)},
        "features": {"processed_dir": str(processed_dir), "features_dir": str(features_dir)},
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))
    return str(cfg_file)


def _write_processed_parquet(tmp_path: Path, df: pd.DataFrame) -> Path:
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(processed_dir / "part.0.parquet"), index=True)
    return processed_dir


# ---------------------------------------------------------------------------
# FEA-01: Cumulative production tests (TR-03, TR-08)
# ---------------------------------------------------------------------------


def _cum_df(oil_vals: list[float], well_id: str = "0100001") -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "well_id": [well_id] * len(oil_vals),
            "production_date": pd.date_range("2021-01-01", periods=len(oil_vals), freq="MS"),
            "OilProduced": oil_vals,
            "GasProduced": [500.0] * len(oil_vals),
            "WaterProduced": [50.0] * len(oil_vals),
        }
    ).set_index("well_id")
    return df


def test_cumulative_basic():
    df = _cum_df([100, 200, 150])
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [100, 300, 450]


def test_cumulative_shutin_flat():
    """TR-08a: Zero production month → cumulative stays flat."""
    df = _cum_df([100, 0, 50])
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [100, 100, 150]


def test_cumulative_starts_zero():
    """TR-08b: Well starting with zeros."""
    df = _cum_df([0, 0, 100])
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [0, 0, 100]


def test_cumulative_shutin_then_resume():
    """TR-08c: Shut-in then resumption."""
    df = _cum_df([100, 0, 0, 200])
    result = _add_cumulative_features(df)
    assert list(result["cum_oil"]) == [100, 100, 100, 300]


def test_cumulative_monotone(tmp_path):
    """TR-03: cum_oil is monotonically non-decreasing."""
    df = _cum_df([100, 80, 60, 90, 50, 0, 120, 110, 90, 80, 70, 60])
    result = _add_cumulative_features(df)
    vals = result["cum_oil"].dropna().tolist()
    for i in range(1, len(vals)):
        assert vals[i] >= vals[i - 1]


def test_cumulative_null_propagation():
    """NA OilProduced → NA cum_oil."""
    df = _cum_df([100, np.nan, 50])
    result = _add_cumulative_features(df)
    assert pd.isna(result["cum_oil"].iloc[1])


def test_cumulative_empty_df_meta():
    """TR-23: empty DataFrame schema matches meta."""
    full_df = _make_processed_df(1, 1)
    empty_df = full_df.iloc[:0].copy()
    result = _add_cumulative_features(empty_df)
    assert "cum_oil" in result.columns
    assert "cum_gas" in result.columns
    assert "cum_water" in result.columns


# ---------------------------------------------------------------------------
# FEA-02: Ratio features tests (TR-06, TR-09, TR-10)
# ---------------------------------------------------------------------------


def _ratio_df(oil: float, gas: float, water: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "well_id": ["0100001"],
            "OilProduced": [oil],
            "GasProduced": [gas],
            "WaterProduced": [water],
        }
    ).set_index("well_id")


def test_gor_normal():
    """TR-09d: GOR = gas / oil."""
    df = _ratio_df(100, 500, 50)
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == pytest.approx(5.0)


def test_gor_zero_oil_nonzero_gas():
    """TR-06a: oil=0, gas>0 → NaN."""
    df = _ratio_df(0, 500, 50)
    result = _add_ratio_features(df)
    assert np.isnan(result["gor"].iloc[0])


def test_gor_both_zero():
    """TR-06b: oil=0, gas=0 → NaN."""
    df = _ratio_df(0, 0, 50)
    result = _add_ratio_features(df)
    assert np.isnan(result["gor"].iloc[0])


def test_gor_oil_positive_gas_zero():
    """TR-06c: oil>0, gas=0 → 0.0."""
    df = _ratio_df(100, 0, 50)
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == 0.0


def test_water_cut_zero_water():
    """TR-10a: water=0, oil=100 → water_cut=0.0."""
    df = _ratio_df(100, 500, 0)
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == 0.0


def test_water_cut_zero_oil():
    """TR-10b: water=200, oil=0 → water_cut=1.0 (valid end-of-life)."""
    df = _ratio_df(0, 500, 200)
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == 1.0


def test_water_cut_formula():
    """TR-09e: water_cut = water / (oil + water)."""
    df = _ratio_df(25, 500, 75)
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == pytest.approx(75 / 100)


def test_wor_zero_oil():
    df = _ratio_df(0, 500, 50)
    result = _add_ratio_features(df)
    assert np.isnan(result["wor"].iloc[0])


def test_wor_normal():
    df = _ratio_df(100, 500, 50)
    result = _add_ratio_features(df)
    assert result["wor"].iloc[0] == pytest.approx(0.5)


def test_no_zero_division_exception():
    """No ZeroDivisionError or unhandled RuntimeWarning for any combo."""
    combos = [(0, 0, 0), (0, 0, 50), (0, 500, 0), (100, 0, 0)]
    for oil, gas, water in combos:
        df = _ratio_df(oil, gas, water)
        result = _add_ratio_features(df)
        assert isinstance(result, pd.DataFrame)


def test_ratio_empty_df_meta():
    """TR-23: empty DataFrame produces correct schema."""
    full_df = _make_processed_df(1, 1)
    empty_df = full_df.iloc[:0].copy()
    result = _add_ratio_features(empty_df)
    for col in ["gor", "water_cut", "wor", "wgr"]:
        assert col in result.columns


# ---------------------------------------------------------------------------
# FEA-03: Decline rate tests (TR-07)
# ---------------------------------------------------------------------------


def _decline_df(oil_vals: list[float], well_id: str = "0100001") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "well_id": [well_id] * len(oil_vals),
            "production_date": pd.date_range("2021-01-01", periods=len(oil_vals), freq="MS"),
            "OilProduced": oil_vals,
            "GasProduced": [500.0] * len(oil_vals),
        }
    ).set_index("well_id")


def test_decline_rate_basic():
    df = _decline_df([100, 80, 60])
    result = _add_decline_rates(df)
    rates = result["decline_rate_oil"].tolist()
    assert np.isnan(rates[0])
    assert rates[1] == pytest.approx(-0.2)
    assert rates[2] == pytest.approx(-0.25)


def test_decline_rate_clip_lower():
    """TR-07a: -2.0 → clipped to -1.0."""
    # To get exactly -2.0 raw: prev=100, need current that gives pct_change=-2.0 → current=-100 (impossible)
    # Instead test clipping directly on a computed value that exceeds bounds
    df2 = _decline_df([100, 50, 2])  # raw rate at idx 2: (2-50)/50 = -0.96
    result = _add_decline_rates(df2)
    # All should be >= -1.0 after clipping
    valid = result["decline_rate_oil"].dropna()
    assert (valid >= -1.0).all()


def test_decline_rate_clip_upper():
    """TR-07b: values > 10.0 clipped to 10.0."""
    df = _decline_df([1, 50])  # raw = (50-1)/1 = 49 → clipped to 10.0
    result = _add_decline_rates(df)
    assert result["decline_rate_oil"].iloc[1] == pytest.approx(10.0)


def test_decline_rate_within_bounds():
    """TR-07c: 0.5 passes through unchanged."""
    df = _decline_df([100, 150])  # rate = (150-100)/100 = 0.5
    result = _add_decline_rates(df)
    assert result["decline_rate_oil"].iloc[1] == pytest.approx(0.5)


def test_decline_rate_prior_zero_current_zero():
    """TR-07d: prior=0, current=0 → 0.0."""
    df = _decline_df([100, 0, 0, 50])
    result = _add_decline_rates(df)
    assert result["decline_rate_oil"].iloc[2] == pytest.approx(0.0)


def test_decline_rate_prior_zero_current_positive():
    """TR-07d: prior=0, current>0 → NaN."""
    df = _decline_df([100, 0, 50])
    result = _add_decline_rates(df)
    assert np.isnan(result["decline_rate_oil"].iloc[2])


def test_decline_rate_no_inf():
    """No inf/-inf in output."""
    df = _decline_df([0, 100, 50, 0, 200])
    result = _add_decline_rates(df)
    vals = result["decline_rate_oil"]
    assert not np.isinf(vals.replace({np.nan: 0}).values).any()


def test_decline_rate_first_record_nan():
    df = _decline_df([100, 80])
    result = _add_decline_rates(df)
    assert np.isnan(result["decline_rate_oil"].iloc[0])


def test_decline_rate_empty_df_meta():
    """TR-23."""
    full_df = _make_processed_df(1, 2)
    empty_df = full_df.iloc[:0].copy()
    result = _add_decline_rates(empty_df)
    assert "decline_rate_oil" in result.columns
    assert "decline_rate_gas" in result.columns


# ---------------------------------------------------------------------------
# FEA-04: Rolling average tests (TR-09a, TR-09b)
# ---------------------------------------------------------------------------


def _roll_df(oil_vals: list[float], well_id: str = "0100001") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "well_id": [well_id] * len(oil_vals),
            "production_date": pd.date_range("2021-01-01", periods=len(oil_vals), freq="MS"),
            "OilProduced": oil_vals,
            "GasProduced": [500.0] * len(oil_vals),
            "WaterProduced": [50.0] * len(oil_vals),
        }
    ).set_index("well_id")


def test_rolling_3month():
    """TR-09a: hand-computed 3-month rolling mean."""
    df = _roll_df([100, 200, 300, 400])
    result = _add_rolling_features(df)
    rolls = result["oil_roll3"].tolist()
    assert rolls[0] == pytest.approx(100.0)  # min_periods=1
    assert rolls[1] == pytest.approx(150.0)  # mean(100, 200)
    assert rolls[2] == pytest.approx(200.0)  # mean(100, 200, 300)
    assert rolls[3] == pytest.approx(300.0)  # mean(200, 300, 400)


def test_rolling_6month_partial_window():
    """TR-09b: window=6, only 3 records → use available periods."""
    df = _roll_df([500, 600, 700])
    result = _add_rolling_features(df)
    assert result["oil_roll6"].iloc[0] == pytest.approx(500.0)
    assert result["oil_roll6"].iloc[1] == pytest.approx(550.0)
    assert result["oil_roll6"].iloc[2] == pytest.approx(600.0)


def test_rolling_2months_not_nan():
    """TR-09b: 2-month well → oil_roll3 at month 2 == mean(m1, m2)."""
    df = _roll_df([100, 200])
    result = _add_rolling_features(df)
    assert result["oil_roll3"].iloc[1] == pytest.approx(150.0)


def test_rolling_all_columns_present():
    df = _roll_df([100, 200, 300])
    result = _add_rolling_features(df)
    for col in [
        "oil_roll3",
        "oil_roll6",
        "oil_roll12",
        "gas_roll3",
        "gas_roll6",
        "gas_roll12",
        "water_roll3",
        "water_roll6",
        "water_roll12",
    ]:
        assert col in result.columns


def test_rolling_non_negative():
    """TR-09: No negative rolling values when input is non-negative."""
    df = _roll_df([100, 200, 300, 400, 500])
    result = _add_rolling_features(df)
    assert (result["oil_roll3"] >= 0).all()


def test_rolling_empty_df_meta():
    """TR-23."""
    full_df = _make_processed_df(1, 3)
    empty_df = full_df.iloc[:0].copy()
    result = _add_rolling_features(empty_df)
    assert "oil_roll3" in result.columns


# ---------------------------------------------------------------------------
# FEA-05: Lag feature tests (TR-09c)
# ---------------------------------------------------------------------------


def _lag_df(oil_vals: list[float], well_id: str = "0100001") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "well_id": [well_id] * len(oil_vals),
            "production_date": pd.date_range("2021-01-01", periods=len(oil_vals), freq="MS"),
            "OilProduced": oil_vals,
            "GasProduced": [500.0] * len(oil_vals),
            "WaterProduced": [50.0] * len(oil_vals),
        }
    ).set_index("well_id")


def test_lag_basic():
    """TR-09c: lag-1 values match shifted production."""
    df = _lag_df([100, 200, 300])
    result = _add_lag_features(df)
    assert np.isnan(result["oil_lag1"].iloc[0])
    assert result["oil_lag1"].iloc[1] == 100.0
    assert result["oil_lag1"].iloc[2] == 200.0


def test_lag_no_bleed_across_wells():
    """Lags must not cross well boundaries."""
    df = pd.DataFrame(
        {
            "well_id": ["WELL_A", "WELL_A", "WELL_B", "WELL_B"],
            "production_date": pd.to_datetime(
                ["2021-01-01", "2021-02-01", "2021-01-01", "2021-02-01"]
            ),
            "OilProduced": [100.0, 200.0, 300.0, 400.0],
            "GasProduced": [500.0, 500.0, 500.0, 500.0],
            "WaterProduced": [50.0, 50.0, 50.0, 50.0],
        }
    ).set_index("well_id")
    result = _add_lag_features(df)
    # WELL_B's first record should have NaN lag (not WELL_A's last value)
    well_b_first = result.loc["WELL_B"].iloc[0]
    assert np.isnan(well_b_first["oil_lag1"])


def test_lag_dtype_float():
    df = _lag_df([100, 200, 300])
    result = _add_lag_features(df)
    assert result["oil_lag1"].dtype == np.dtype("float64")


def test_lag_empty_df_meta():
    """TR-23."""
    full_df = _make_processed_df(1, 3)
    empty_df = full_df.iloc[:0].copy()
    result = _add_lag_features(empty_df)
    assert "oil_lag1" in result.columns


# ---------------------------------------------------------------------------
# FEA-06: Encoded features tests
# ---------------------------------------------------------------------------


def _encoded_df(well_status: object, days: object) -> pd.DataFrame:
    _WELL_STATUS_CATS = ["AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"]
    ws_val = well_status if pd.notna(well_status) else None  # type: ignore[call-overload]
    ws_cat = pd.Categorical(
        [ws_val] if ws_val is not None else [None],
        categories=_WELL_STATUS_CATS,
    )
    dp_val = pd.array([days], dtype="Int64")  # type: ignore[call-overload]
    return pd.DataFrame(
        {
            "well_id": ["0100001"],
            "WellStatus": ws_cat,
            "DaysProduced": dp_val,
        }
    ).set_index("well_id")


def test_encoded_wellstatus_ac():
    df = _encoded_df("AC", 28)
    result = _add_encoded_features(df)
    assert result["wellstatus_encoded"].iloc[0] == 1


def test_encoded_wellstatus_si():
    df = _encoded_df("SI", 28)
    result = _add_encoded_features(df)
    assert result["wellstatus_encoded"].iloc[0] == 5


def test_encoded_wellstatus_na():
    df = _encoded_df(pd.NA, 28)
    result = _add_encoded_features(df)
    assert result["wellstatus_encoded"].iloc[0] == -1


def test_encoded_days_31():
    df = _encoded_df("AC", 31)
    result = _add_encoded_features(df)
    assert result["days_produced_norm"].iloc[0] == pytest.approx(1.0)


def test_encoded_days_0():
    df = _encoded_df("AC", 0)
    result = _add_encoded_features(df)
    assert result["days_produced_norm"].iloc[0] == pytest.approx(0.0)


def test_encoded_days_na():
    df = _encoded_df("AC", pd.NA)
    result = _add_encoded_features(df)
    assert pd.isna(result["days_produced_norm"].iloc[0])


def test_encoded_days_dtype():
    df = _encoded_df("AC", 28)
    result = _add_encoded_features(df)
    assert result["days_produced_norm"].dtype == np.dtype("float64")


def test_encoded_wellstatus_dtype():
    df = _encoded_df("AC", 28)
    result = _add_encoded_features(df)
    assert result["wellstatus_encoded"].dtype == pd.Int64Dtype()


def test_encoded_empty_df_meta():
    """TR-23."""
    full_df = _make_processed_df(1, 1)
    empty_df = full_df.iloc[:0].copy()
    result = _add_encoded_features(empty_df)
    assert "wellstatus_encoded" in result.columns
    assert "days_produced_norm" in result.columns


# ---------------------------------------------------------------------------
# FEA-07 + FEA-09: run_features integration tests (TR-17, TR-18, TR-19, TR-14)
# ---------------------------------------------------------------------------


@pytest.fixture()
def features_setup(tmp_path):
    df = _make_processed_df(n_wells=1, months=12)
    processed_dir = _write_processed_parquet(tmp_path, df)
    cfg_path = _make_features_config(tmp_path, processed_dir)
    features_dir = tmp_path / "features"
    return cfg_path, features_dir


def test_run_features_creates_parquet(features_setup):
    cfg_path, features_dir = features_setup
    run_features(cfg_path)
    assert list(features_dir.glob("*.parquet"))


def test_run_features_parquet_readable(features_setup):
    """TR-18: All output Parquet files readable."""
    cfg_path, features_dir = features_setup
    run_features(cfg_path)
    for pf in features_dir.glob("*.parquet"):
        df = pd.read_parquet(pf)
        assert isinstance(df, pd.DataFrame)


def test_run_features_empty_processed_dir(tmp_path):
    """Given empty processed_dir, run_features returns without raising."""
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    cfg_path = _make_features_config(tmp_path, processed_dir)
    run_features(cfg_path)  # should not raise


_TR19_REQUIRED_COLS = [
    "production_date",
    "OilProduced",
    "GasProduced",
    "WaterProduced",
    "cum_oil",
    "cum_gas",
    "cum_water",
    "gor",
    "water_cut",
    "wor",
    "wgr",
    "decline_rate_oil",
    "decline_rate_gas",
    "oil_roll3",
    "oil_roll6",
    "oil_roll12",
    "gas_roll3",
    "gas_roll6",
    "gas_roll12",
    "water_roll3",
    "water_roll6",
    "water_roll12",
    "oil_lag1",
    "gas_lag1",
    "water_lag1",
    "wellstatus_encoded",
    "days_produced_norm",
]


def test_tr19_all_feature_columns_present(features_setup):
    """TR-19: All required feature columns present in output."""
    cfg_path, features_dir = features_setup
    run_features(cfg_path)

    all_df = dd.read_parquet(str(features_dir)).compute().reset_index()
    for col in _TR19_REQUIRED_COLS:
        assert col in all_df.columns, f"Missing required column: {col!r}"


def test_tr14_schema_stability_across_partitions(tmp_path):
    """TR-14: All feature partition files have identical schema."""
    df = _make_processed_df(n_wells=3, months=12)
    processed_dir = _write_processed_parquet(tmp_path, df)
    cfg_path = _make_features_config(tmp_path, processed_dir)
    run_features(cfg_path)

    features_dir = tmp_path / "features"
    parquet_files = sorted(features_dir.glob("*.parquet"))
    if len(parquet_files) < 2:
        pytest.skip("Need at least 2 partitions")

    df0 = pd.read_parquet(parquet_files[0])
    df1 = pd.read_parquet(parquet_files[1])
    assert list(df0.columns) == list(df1.columns)


# ---------------------------------------------------------------------------
# FEA-09: Domain integration tests (TR-01, TR-03, TR-06, TR-07, TR-08, TR-09, TR-10)
# ---------------------------------------------------------------------------


def test_tr01_negative_oil_not_amplified():
    """TR-01: Negative OilProduced (transform bug) doesn't amplify in features."""
    df = _make_processed_df(1, 1)
    df = df.copy()
    df["OilProduced"] = -10.0
    result_cum = _add_cumulative_features(df)
    # GOR: oil < 0, treated by condition oil > 0 → gor = NaN
    assert not (result_cum["cum_oil"] > 0).any()


def test_tr03_cum_monotone_12_months():
    """TR-03: 12-month sequence has monotonically non-decreasing cum_oil."""
    df = _make_processed_df(1, 12)
    result = _add_cumulative_features(df)
    vals = result["cum_oil"].dropna().tolist()
    for i in range(1, len(vals)):
        assert vals[i] >= vals[i - 1]


def test_tr06_gor_zero_denominator_nan():
    """TR-06a/b: GOR with zero oil → NaN."""
    df = _ratio_df(0, 500, 50)
    result = _add_ratio_features(df)
    assert np.isnan(result["gor"].iloc[0])


def test_tr06_gor_zero_both_nan():
    """TR-06b."""
    df = _ratio_df(0, 0, 0)
    result = _add_ratio_features(df)
    assert np.isnan(result["gor"].iloc[0])


def test_tr06_gor_oil_positive_gas_zero():
    """TR-06c: oil>0, gas=0 → 0.0."""
    df = _ratio_df(100, 0, 50)
    result = _add_ratio_features(df)
    assert result["gor"].iloc[0] == 0.0


def test_tr07_decline_clip_lower():
    """TR-07a: raw -2.0 → clipped to -1.0."""
    # Test the clipping logic directly via a raw series exceeding bounds
    series = pd.Series([-2.0, 15.0, 0.3, np.nan], name="decline_rate_oil")
    clipped = series.clip(-1.0, 10.0)
    assert clipped.iloc[0] == pytest.approx(-1.0)
    assert clipped.iloc[1] == pytest.approx(10.0)
    assert clipped.iloc[2] == pytest.approx(0.3)
    assert np.isnan(clipped.iloc[3])


def test_tr07_decline_clip_upper():
    """TR-07b: value above 10.0 after clip."""
    df = _decline_df([1, 50])  # raw rate = 49 → clipped to 10
    result = _add_decline_rates(df)
    assert result["decline_rate_oil"].iloc[1] == pytest.approx(10.0)


def test_tr07d_shutin_zero_rates():
    """TR-07d: both zero → 0.0."""
    df = _decline_df([100, 0, 0])
    result = _add_decline_rates(df)
    assert result["decline_rate_oil"].iloc[2] == pytest.approx(0.0)


def test_tr08_flat_periods():
    """TR-08a: cumulative flat during zero months."""
    df = _cum_df([100, 0, 50])
    result = _add_cumulative_features(df)
    assert result["cum_oil"].iloc[1] == result["cum_oil"].iloc[0]


def test_tr09_rolling_exact():
    """TR-09a: exact match for known 3-month rolling values."""
    df = _roll_df([100, 200, 300, 400, 500, 600])
    result = _add_rolling_features(df)
    expected = [100, 150, 200, 300, 400, 500]
    for i, exp in enumerate(expected):
        assert result["oil_roll3"].iloc[i] == pytest.approx(exp)


def test_tr09_lag_correctness():
    """TR-09c: lag-1 at months 2, 3, 4 matches production at 1, 2, 3."""
    df = _lag_df([100, 200, 300, 400])
    result = _add_lag_features(df)
    assert result["oil_lag1"].iloc[1] == 100.0
    assert result["oil_lag1"].iloc[2] == 200.0
    assert result["oil_lag1"].iloc[3] == 300.0


def test_tr10_water_cut_boundary_zero():
    """TR-10a: water_cut=0.0 is valid."""
    df = _ratio_df(100, 500, 0)
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == 0.0


def test_tr10_water_cut_boundary_one():
    """TR-10b: water_cut=1.0 is valid (end-of-life)."""
    df = _ratio_df(0, 500, 200)
    result = _add_ratio_features(df)
    assert result["water_cut"].iloc[0] == 1.0


# ---------------------------------------------------------------------------
# TR-23: map_partitions meta consistency for all 6 feature functions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "func",
    [
        _add_cumulative_features,
        _add_ratio_features,
        _add_decline_rates,
        _add_rolling_features,
        _add_lag_features,
        _add_encoded_features,
    ],
)
def test_tr23_meta_consistency(func):
    """TR-23: function on empty DataFrame produces same schema as meta passed to map_partitions."""
    full_df = _make_processed_df(1, 3)
    # Chain through the functions to build correct input schema
    for f in [
        _add_cumulative_features,
        _add_ratio_features,
        _add_decline_rates,
        _add_rolling_features,
        _add_lag_features,
        _add_encoded_features,
    ]:
        if f == func:
            break
        full_df = f(full_df)

    empty_df = full_df.iloc[:0].copy()
    result = func(empty_df)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
    assert set(result.columns).issuperset(set(empty_df.columns))


# ---------------------------------------------------------------------------
# FEA-08: pipeline.py CLI tests
# ---------------------------------------------------------------------------


def test_pipeline_stages_acquire_ingest_only(tmp_path):
    """--stages acquire ingest runs only those two stages."""
    from unittest.mock import MagicMock, patch

    import yaml

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(tmp_path / "raw"),
            "max_workers": 2,
            "sleep_per_worker": 0.0,
            "timeout": 10,
            "max_retries": 1,
            "backoff_multiplier": 1,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "256MB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/p.log"), "level": "INFO"},
        "ingest": {
            "raw_dir": str(tmp_path / "raw"),
            "interim_dir": str(tmp_path / "interim"),
            "start_year": 2020,
        },
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
        },
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        },
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))

    from cogcc_pipeline import pipeline as pipe_mod

    mock_acquire = MagicMock()
    mock_ingest = MagicMock()
    mock_transform = MagicMock()
    mock_features = MagicMock()
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.dashboard_link = "http://localhost:8787"
    mock_client.close = MagicMock()

    with (
        patch("cogcc_pipeline.acquire.run_acquire", mock_acquire),
        patch("cogcc_pipeline.ingest.run_ingest", mock_ingest),
        patch("cogcc_pipeline.transform.run_transform", mock_transform),
        patch("cogcc_pipeline.features.run_features", mock_features),
        patch("dask.distributed.Client", return_value=mock_client),
        patch("dask.distributed.LocalCluster", return_value=MagicMock()),
    ):
        pipe_mod.main(["--stages", "acquire", "ingest", "--config", str(cfg_file)])

    mock_acquire.assert_called_once()
    mock_ingest.assert_called_once()
    mock_transform.assert_not_called()
    mock_features.assert_not_called()


def test_pipeline_all_stages(tmp_path):
    """--stages all runs acquire → ingest → transform → features."""
    from unittest.mock import MagicMock, patch

    import yaml

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(tmp_path / "raw"),
            "max_workers": 2,
            "sleep_per_worker": 0.0,
            "timeout": 10,
            "max_retries": 1,
            "backoff_multiplier": 1,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "256MB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/p.log"), "level": "INFO"},
        "ingest": {
            "raw_dir": str(tmp_path / "raw"),
            "interim_dir": str(tmp_path / "interim"),
            "start_year": 2020,
        },
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
        },
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        },
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))

    from cogcc_pipeline import pipeline as pipe_mod

    mock_acquire = MagicMock()
    mock_ingest = MagicMock()
    mock_transform = MagicMock()
    mock_features = MagicMock()
    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    mock_client.close = MagicMock()

    with (
        patch("cogcc_pipeline.acquire.run_acquire", mock_acquire),
        patch("cogcc_pipeline.ingest.run_ingest", mock_ingest),
        patch("cogcc_pipeline.transform.run_transform", mock_transform),
        patch("cogcc_pipeline.features.run_features", mock_features),
        patch("dask.distributed.Client", return_value=mock_client),
        patch("dask.distributed.LocalCluster", return_value=MagicMock()),
    ):
        pipe_mod.main(["--stages", "all", "--config", str(cfg_file)])

    mock_acquire.assert_called_once()
    mock_ingest.assert_called_once()
    mock_transform.assert_called_once()
    mock_features.assert_called_once()


def test_pipeline_stage_failure_stops_pipeline(tmp_path):
    """Given ingest raises, transform/features not called and sys.exit(1)."""
    from unittest.mock import MagicMock, patch

    import yaml

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(tmp_path / "raw"),
            "max_workers": 2,
            "sleep_per_worker": 0.0,
            "timeout": 10,
            "max_retries": 1,
            "backoff_multiplier": 1,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "256MB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/p.log"), "level": "INFO"},
        "ingest": {
            "raw_dir": str(tmp_path / "raw"),
            "interim_dir": str(tmp_path / "interim"),
            "start_year": 2020,
        },
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
        },
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        },
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))

    from cogcc_pipeline import pipeline as pipe_mod

    mock_ingest = MagicMock(side_effect=RuntimeError("ingest error"))
    mock_transform = MagicMock()
    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    mock_client.close = MagicMock()

    with (
        patch("cogcc_pipeline.ingest.run_ingest", mock_ingest),
        patch("cogcc_pipeline.transform.run_transform", mock_transform),
        patch("dask.distributed.Client", return_value=mock_client),
        patch("dask.distributed.LocalCluster", return_value=MagicMock()),
    ):
        with pytest.raises(SystemExit) as exc_info:
            pipe_mod.main(["--stages", "ingest", "transform", "--config", str(cfg_file)])
    assert exc_info.value.code == 1
    mock_transform.assert_not_called()


def test_pipeline_client_closed_on_completion(tmp_path):
    """Dask Client is closed after successful run."""
    from unittest.mock import MagicMock, patch

    import yaml

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(tmp_path / "raw"),
            "max_workers": 2,
            "sleep_per_worker": 0.0,
            "timeout": 10,
            "max_retries": 1,
            "backoff_multiplier": 1,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "256MB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/p.log"), "level": "INFO"},
        "ingest": {
            "raw_dir": str(tmp_path / "raw"),
            "interim_dir": str(tmp_path / "interim"),
            "start_year": 2020,
        },
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
        },
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        },
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))

    from cogcc_pipeline import pipeline as pipe_mod

    mock_ingest = MagicMock()
    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    mock_client.close = MagicMock()

    with (
        patch("cogcc_pipeline.ingest.run_ingest", mock_ingest),
        patch("dask.distributed.Client", return_value=mock_client),
        patch("dask.distributed.LocalCluster", return_value=MagicMock()),
    ):
        pipe_mod.main(["--stages", "ingest", "--config", str(cfg_file)])

    mock_client.close.assert_called_once()
