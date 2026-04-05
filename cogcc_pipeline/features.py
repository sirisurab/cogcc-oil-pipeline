"""Features stage: engineer ML-ready features per well."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import joblib  # type: ignore[import-untyped]
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler  # type: ignore[import-untyped]

# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

FEATURE_COLUMNS: list[str] = [
    "well_id",
    "production_date",
    "oil_bbl",
    "gas_mcf",
    "water_bbl",
    "days_produced",
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
    "gas_mcf_rolling_3m",
    "gas_mcf_rolling_6m",
    "gas_mcf_rolling_12m",
    "water_bbl_rolling_3m",
    "water_bbl_rolling_6m",
    "water_bbl_rolling_12m",
    "oil_bbl_lag1",
    "gas_mcf_lag1",
    "water_bbl_lag1",
    "op_name_encoded",
    "county_encoded",
]

SCALE_COLS: list[str] = [
    "oil_bbl",
    "gas_mcf",
    "water_bbl",
    "cum_oil",
    "cum_gas",
    "cum_water",
    "gor",
    "water_cut",
    "decline_rate",
    "well_age_months",
    "oil_bbl_rolling_3m",
    "oil_bbl_rolling_6m",
    "oil_bbl_rolling_12m",
]


# ---------------------------------------------------------------------------
# Per-well feature functions (pandas)
# ---------------------------------------------------------------------------


def compute_cumulative_production(df: pd.DataFrame) -> pd.DataFrame:
    """Compute cumulative oil, gas, and water production per well.

    Null months are treated as zero for cumsum purposes so NaN does not
    propagate into the cumulative total.

    Args:
        df: Single-well DataFrame sorted ascending by production_date.

    Returns:
        DataFrame with ``cum_oil``, ``cum_gas``, ``cum_water`` columns added.
    """
    df = df.copy()
    df["cum_oil"] = df["oil_bbl"].fillna(0).cumsum()
    df["cum_gas"] = df["gas_mcf"].fillna(0).cumsum()
    df["cum_water"] = df["water_bbl"].fillna(0).cumsum()
    return df


def compute_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Compute gas-oil ratio (GOR) and water cut for each row.

    Uses ``numpy.where`` to avoid ZeroDivisionError and Inf values.

    Args:
        df: DataFrame with ``oil_bbl``, ``gas_mcf``, ``water_bbl`` columns.

    Returns:
        DataFrame with ``gor`` and ``water_cut`` columns added.
    """
    df = df.copy()
    oil = pd.to_numeric(df["oil_bbl"], errors="coerce")
    gas = pd.to_numeric(df["gas_mcf"], errors="coerce")
    water = pd.to_numeric(df["water_bbl"], errors="coerce")

    # GOR: gas / oil when oil > 0, else NaN
    gor = np.where(oil > 0, gas / oil, np.nan)
    df["gor"] = gor.astype(float)

    # Water cut: water / (oil + water) when denominator > 0
    denom = oil + water
    water_cut = np.where(denom > 0, water / denom, np.nan)
    df["water_cut"] = water_cut.astype(float)
    return df


def compute_decline_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Compute period-over-period oil production decline rate, clipped to [-1, 10].

    Special cases:
    - ``oil_bbl_prev == 0`` and ``oil_bbl_curr > 0``: decline_rate = -1.0
    - ``oil_bbl_prev == 0`` and ``oil_bbl_curr == 0``: decline_rate = 0.0
    - First record (no prior month): decline_rate = NaN

    Args:
        df: Single-well DataFrame sorted ascending by production_date.

    Returns:
        DataFrame with ``decline_rate`` column added.
    """
    df = df.copy()
    oil = pd.to_numeric(df["oil_bbl"], errors="coerce")
    prev = oil.shift(1)

    # Raw computation using numpy
    prev_arr = prev.to_numpy(dtype=float, na_value=np.nan)
    curr_arr = oil.to_numpy(dtype=float, na_value=np.nan)

    decline = np.full(len(df), np.nan)
    for i in range(len(df)):
        p = prev_arr[i]
        c = curr_arr[i]
        if np.isnan(p):
            decline[i] = np.nan
        elif p == 0 and c > 0:
            decline[i] = -1.0
        elif p == 0 and c == 0:
            decline[i] = 0.0
        elif np.isnan(c):
            decline[i] = np.nan
        else:
            decline[i] = (p - c) / p

    decline = np.clip(decline, -1.0, 10.0)
    df["decline_rate"] = decline
    return df


def compute_rolling_and_lag(df: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    """Compute rolling averages and lag-1 features for production columns.

    Rolling averages use ``min_periods=1`` so partial windows yield a value
    rather than NaN.

    Args:
        df: Single-well DataFrame sorted ascending by production_date.
        windows: List of rolling window sizes in months (e.g. [3, 6, 12]).

    Returns:
        DataFrame with rolling and lag columns added.
    """
    df = df.copy()
    prod_cols = ["oil_bbl", "gas_mcf", "water_bbl"]

    for col in prod_cols:
        series = pd.to_numeric(df[col], errors="coerce")
        for w in windows:
            df[f"{col}_rolling_{w}m"] = series.rolling(window=w, min_periods=1).mean()
        df[f"{col}_lag1"] = series.shift(1)

    return df


def compute_well_age_and_normalised(df: pd.DataFrame) -> pd.DataFrame:
    """Compute well age in months and 30-day-normalised production.

    ``well_age_months = 0`` for the first month.
    Days-normalised production is NaN when ``days_produced`` is 0 or null.

    Args:
        df: Single-well DataFrame sorted ascending by production_date.

    Returns:
        DataFrame with ``well_age_months``, ``oil_bbl_30d``, ``gas_mcf_30d``,
        ``water_bbl_30d`` columns added.
    """
    df = df.copy()
    dates = pd.to_datetime(df["production_date"])
    first_date = dates.min()

    df["well_age_months"] = (
        (dates.dt.year - first_date.year) * 12 + (dates.dt.month - first_date.month)
    ).astype("Int64")

    days = pd.to_numeric(df["days_produced"], errors="coerce")
    for col in ("oil_bbl", "gas_mcf", "water_bbl"):
        out_col = col.replace("bbl", "bbl_30d").replace("mcf", "mcf_30d")
        # Handle oil_bbl_30d vs oil_bbl_30d naming
        if col == "oil_bbl":
            out_col = "oil_bbl_30d"
        elif col == "gas_mcf":
            out_col = "gas_mcf_30d"
        else:
            out_col = "water_bbl_30d"
        vals = pd.to_numeric(df[col], errors="coerce")
        df[out_col] = np.where(days > 0, vals * 30.0 / days, np.nan)

    return df


def engineer_well_features(df: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    """Apply all per-well feature computations in order.

    Validates that the output contains exactly the expected columns.

    Args:
        df: Single-well pandas DataFrame (all rows for one well_id).
        windows: Rolling average window sizes.

    Returns:
        Enriched DataFrame with all feature columns.

    Raises:
        ValueError: If a required output column is missing.
    """
    df = df.sort_values("production_date").copy()

    # Rename canonical columns to feature names if needed
    rename_map = {
        "OilProduced": "oil_bbl",
        "GasProduced": "gas_mcf",
        "WaterProduced": "water_bbl",
        "DaysProduced": "days_produced",
        "OpName": "OpName",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Ensure required production columns exist
    for col in ("oil_bbl", "gas_mcf", "water_bbl", "days_produced"):
        if col not in df.columns:
            df[col] = pd.NA

    df = compute_cumulative_production(df)
    df = compute_ratios(df)
    df = compute_decline_rate(df)
    df = compute_rolling_and_lag(df, windows)
    df = compute_well_age_and_normalised(df)

    return df


# ---------------------------------------------------------------------------
# Dask parallel per-well dispatcher
# ---------------------------------------------------------------------------


def _build_features_meta(windows: list[int]) -> pd.DataFrame:
    """Build empty meta DataFrame for the features output schema."""
    # Column order must exactly match what engineer_well_features produces:
    # input cols first (well_id, production_date, oil_bbl, gas_mcf, water_bbl,
    # days_produced, OpName), then computed cols in order of addition.
    dtypes: dict[str, Any] = {
        "well_id": pd.StringDtype(),
        "production_date": pd.Series(dtype="datetime64[us]").dtype,
        "oil_bbl": pd.Float64Dtype(),
        "gas_mcf": pd.Float64Dtype(),
        "water_bbl": pd.Float64Dtype(),
        "days_produced": pd.Float64Dtype(),
        "OpName": pd.StringDtype(),
        "cum_oil": pd.Float64Dtype(),
        "cum_gas": pd.Float64Dtype(),
        "cum_water": pd.Float64Dtype(),
        "gor": pd.Float64Dtype(),
        "water_cut": pd.Float64Dtype(),
        "decline_rate": pd.Float64Dtype(),
    }
    for col in ("oil_bbl", "gas_mcf", "water_bbl"):
        for w in windows:
            dtypes[f"{col}_rolling_{w}m"] = pd.Float64Dtype()
        dtypes[f"{col}_lag1"] = pd.Float64Dtype()
    dtypes["well_age_months"] = pd.Float64Dtype()
    dtypes["oil_bbl_30d"] = pd.Float64Dtype()
    dtypes["gas_mcf_30d"] = pd.Float64Dtype()
    dtypes["water_bbl_30d"] = pd.Float64Dtype()

    meta = pd.DataFrame({k: pd.array([], dtype=v) for k, v in dtypes.items()})
    return meta


def _apply_per_well(df: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    """Apply engineer_well_features to each well within a partition group."""
    results = []
    for _, well_df in df.groupby("well_id", sort=False):
        try:
            enriched = engineer_well_features(well_df, windows)
            results.append(enriched)
        except Exception:
            pass
    if results:
        return pd.concat(results, ignore_index=True)
    return _build_features_meta(windows).iloc[:0]


def apply_well_features_parallel(
    ddf: dd.DataFrame,
    windows: list[int],
    logger: logging.Logger,
) -> dd.DataFrame:
    """Apply per-well feature engineering across all wells using Dask.

    Does not call ``.compute()`` internally.

    Args:
        ddf: Cleaned Dask DataFrame sorted by (well_id, production_date).
        windows: Rolling average window sizes.
        logger: Configured logger instance.

    Returns:
        Dask DataFrame with all feature columns.
    """
    logger.info("Applying per-well feature engineering (windows=%s)", windows)
    ddf = ddf.map_partitions(
        _apply_per_well,
        windows,
        meta=_build_features_meta(windows),
    )
    return ddf


# ---------------------------------------------------------------------------
# Categorical encoding
# ---------------------------------------------------------------------------


def encode_categoricals(
    ddf: dd.DataFrame,
    encoders_output_path: Path,
    logger: logging.Logger,
) -> dd.DataFrame:
    """Label-encode OpName and county_code; save mapping JSON.

    Calls ``.compute()`` once to build the encoding maps.

    Args:
        ddf: Dask DataFrame with ``OpName`` and ``well_id`` columns.
        encoders_output_path: Path for the JSON encoding mapping file.
        logger: Configured logger instance.

    Returns:
        Dask DataFrame with ``op_name_encoded`` and ``county_encoded`` columns added,
        ``OpName`` dropped.
    """
    logger.info("Encoding categorical columns")

    # Build county_code from well_id
    def _add_county(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["county_code"] = df["well_id"].str[2:5].astype(pd.StringDtype())
        return df

    meta_with_county = ddf._meta.copy()
    meta_with_county["county_code"] = pd.array([], dtype=pd.StringDtype())
    ddf = ddf.map_partitions(_add_county, meta=meta_with_county)

    # Compute unique values for encoding maps
    op_names = sorted(ddf["OpName"].dropna().compute().unique().tolist())  # type: ignore[union-attr]
    counties = sorted(ddf["county_code"].dropna().compute().unique().tolist())  # type: ignore[union-attr]

    op_map: dict[str, int] = {name: i for i, name in enumerate(op_names)}
    county_map: dict[str, int] = {code: i for i, code in enumerate(counties)}

    encoders_output_path.parent.mkdir(parents=True, exist_ok=True)
    with encoders_output_path.open("w", encoding="utf-8") as fh:
        json.dump({"op_name": op_map, "county_code": county_map}, fh, indent=2)
    logger.info("Label encoders saved to %s", encoders_output_path)

    def _encode_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["op_name_encoded"] = df["OpName"].map(op_map).fillna(-1).astype(pd.Int32Dtype())
        df["county_encoded"] = df["county_code"].map(county_map).fillna(-1).astype(pd.Int32Dtype())
        df = df.drop(columns=["OpName", "county_code"], errors="ignore")
        return df

    meta_encoded = ddf._meta.copy()
    meta_encoded["op_name_encoded"] = pd.array([], dtype=pd.Int32Dtype())
    meta_encoded["county_encoded"] = pd.array([], dtype=pd.Int32Dtype())
    meta_encoded = meta_encoded.drop(columns=["OpName", "county_code"], errors="ignore")

    return ddf.map_partitions(_encode_partition, meta=meta_encoded)


# ---------------------------------------------------------------------------
# Numeric scaling
# ---------------------------------------------------------------------------


def scale_numeric_features(
    ddf: dd.DataFrame,
    scaler_output_path: Path,
    logger: logging.Logger,
) -> dd.DataFrame:
    """Fit a StandardScaler and apply it to numeric feature columns.

    Calls ``.compute()`` to fit the scaler on the full dataset.

    Args:
        ddf: Dask DataFrame with numeric feature columns.
        scaler_output_path: Path where the fitted scaler will be saved.
        logger: Configured logger instance.

    Returns:
        Dask DataFrame with scaled numeric column values.
    """
    logger.info("Fitting StandardScaler on numeric features")
    scale_cols = [c for c in SCALE_COLS if c in ddf.columns]

    df_full = ddf[scale_cols].compute()
    fill_values = df_full.mean()
    df_filled = df_full.fillna(fill_values)

    scaler = StandardScaler()
    scaler.fit(df_filled)

    scaler_output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(scaler, scaler_output_path)
    logger.info("Scaler saved to %s", scaler_output_path)

    means = fill_values.to_dict()

    def _scale_partition(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        present = [c for c in scale_cols if c in df.columns]
        if present:
            sub = df[present].copy()
            for col in present:
                sub[col] = pd.to_numeric(sub[col], errors="coerce")
            sub = sub.fillna({c: means.get(c, 0.0) for c in present})
            scaled = scaler.transform(sub[scale_cols if scale_cols == present else present])
            for i, col in enumerate(present):
                df[col] = scaled[:, i]
        return df

    return ddf.map_partitions(_scale_partition, meta=ddf._meta)


# ---------------------------------------------------------------------------
# Feature Parquet writer and schema validator
# ---------------------------------------------------------------------------


def write_features_parquet(
    ddf: dd.DataFrame,
    processed_dir: Path,
    filename: str,
    logger: logging.Logger,
) -> Path:
    """Write the feature matrix to Parquet with 30 partitions.

    Args:
        ddf: Feature Dask DataFrame.
        processed_dir: Directory for output Parquet dataset.
        filename: Subdirectory name for the Parquet dataset.
        logger: Configured logger instance.

    Returns:
        Path to the written Parquet dataset directory.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    output_path = processed_dir / filename
    logger.info("Writing features Parquet to %s", output_path)
    ddf.repartition(npartitions=30).to_parquet(
        str(output_path),
        write_index=False,
        engine="pyarrow",
        overwrite=True,
    )
    logger.info("Features Parquet write complete: %s", output_path)
    return output_path


def validate_features_schema(features_parquet_path: Path, logger: logging.Logger) -> list[str]:
    """Validate that the features Parquet contains all expected columns.

    Args:
        features_parquet_path: Path to the Parquet dataset directory.
        logger: Configured logger instance.

    Returns:
        List of validation error strings; empty if all columns present.
    """
    parquet_files = sorted(features_parquet_path.glob("*.parquet"))
    if not parquet_files:
        return [f"No Parquet files found in {features_parquet_path}"]

    sample_file = parquet_files[0]
    df = pd.read_parquet(sample_file)
    errors: list[str] = []
    for col in FEATURE_COLUMNS:
        if col not in df.columns:
            msg = f"Missing column '{col}' in features Parquet"
            logger.warning(msg)
            errors.append(msg)
    return errors


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def run_features(config: dict[str, Any], logger: logging.Logger) -> Path:
    """Run the full features stage: engineer features, encode, scale, write.

    Args:
        config: Pipeline configuration dictionary (top-level).
        logger: Configured logger instance.

    Returns:
        Path to the features Parquet dataset directory.
    """
    fcfg = config["features"]
    processed_dir = Path(fcfg["processed_dir"])
    cleaned_file: str = fcfg["cleaned_file"]
    features_file: str = fcfg["features_file"]
    windows: list[int] = list(fcfg["rolling_windows"])

    encoders_path = processed_dir / "label_encoders.json"
    scaler_path = processed_dir / "scaler.joblib"

    cleaned_path = processed_dir / cleaned_file
    logger.info("Reading cleaned Parquet from %s", cleaned_path)
    ddf = dd.read_parquet(str(cleaned_path), engine="pyarrow")
    ddf = ddf.repartition(npartitions=min(ddf.npartitions, 50))

    # Sort by well_id, production_date for deterministic per-well processing
    ddf = ddf.map_partitions(
        lambda df: df.sort_values(["well_id", "production_date"]),
        meta=ddf._meta,
    )

    ddf = apply_well_features_parallel(ddf, windows, logger)
    ddf = encode_categoricals(ddf, encoders_path, logger)
    ddf = scale_numeric_features(ddf, scaler_path, logger)

    features_path = write_features_parquet(ddf, processed_dir, features_file, logger)

    errors = validate_features_schema(features_path, logger)
    for err in errors:
        logger.warning("Features schema validation: %s", err)

    return features_path
