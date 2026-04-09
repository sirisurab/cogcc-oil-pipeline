"""Features stage: engineer derived ML features from processed Parquet data."""

from __future__ import annotations

import logging

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_OUTPUT_COLUMNS = [
    "production_date",
    "OilProduced",
    "GasProduced",
    "WaterProduced",
    "cum_oil",
    "cum_gas",
    "cum_water",
    "gor",
    "water_cut",
    "decline_rate",
]


class FeaturesError(RuntimeError):
    """Raised on unrecoverable feature-engineering failures."""


def _add_cumulative_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add cum_oil, cum_gas, cum_water columns via per-well cumulative sum.

    Input DataFrame may have well_id as index — reset/restore around groupby.
    """
    df = df.copy()
    index_was_well_id = df.index.name == "well_id"
    if index_was_well_id:
        df = df.reset_index()

    for col, new_col in [
        ("OilProduced", "cum_oil"),
        ("GasProduced", "cum_gas"),
        ("WaterProduced", "cum_water"),
    ]:
        if col in df.columns:
            df[new_col] = df.groupby("well_id")[col].transform("cumsum").astype(pd.Float64Dtype())
        else:
            df[new_col] = pd.array([], dtype=pd.Float64Dtype()) if len(df) == 0 else pd.NA

    if index_was_well_id:
        df = df.set_index("well_id")
    return df


def _add_ratio_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add gor and water_cut columns using vectorised operations.

    GOR rules (TR-06):
    - oil > 0: gor = gas / oil
    - oil == 0 and gas > 0: gor = NaN
    - oil == 0 and gas == 0: gor = NaN
    - oil > 0 and gas == 0: gor = 0.0

    Water cut rules (TR-10):
    - denom > 0: water_cut = water / (oil + water)
    - denom == 0: water_cut = NaN
    - out of [0,1]: → pd.NA + WARNING
    """
    df = df.copy()
    index_was_well_id = df.index.name == "well_id"
    if index_was_well_id:
        df = df.reset_index()

    oil = df["OilProduced"].to_numpy(dtype=float, na_value=np.nan)
    gas = df["GasProduced"].to_numpy(dtype=float, na_value=np.nan)
    water = df["WaterProduced"].to_numpy(dtype=float, na_value=np.nan)

    # GOR
    with np.errstate(divide="ignore", invalid="ignore"):
        raw_gor = np.where(oil > 0, gas / oil, np.nan)
        # oil > 0 and gas == 0 → 0.0
        raw_gor = np.where((oil > 0) & (gas == 0), 0.0, raw_gor)

    df["gor"] = pd.array(raw_gor, dtype=pd.Float64Dtype())

    # Water cut
    denom = oil + water
    with np.errstate(divide="ignore", invalid="ignore"):
        raw_wc = np.where(denom > 0, water / denom, np.nan)

    # Values outside [0,1] → pd.NA + WARNING
    out_of_range = np.isfinite(raw_wc) & ((raw_wc < 0) | (raw_wc > 1))
    if out_of_range.any():
        logger.warning(
            "water_cut out of [0,1] for %d rows — replacing with NA", int(out_of_range.sum())
        )
        raw_wc = np.where(out_of_range, np.nan, raw_wc)

    df["water_cut"] = pd.array(raw_wc, dtype=pd.Float64Dtype())

    if index_was_well_id:
        df = df.set_index("well_id")
    return df


def _add_decline_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Add decline_rate column: period-over-period oil decline per well.

    Formula: pct_change * -1, inf → NaN, clipped to [-1.0, 10.0].
    """
    df = df.copy()
    index_was_well_id = df.index.name == "well_id"
    if index_was_well_id:
        df = df.reset_index()

    raw = df.groupby("well_id")["OilProduced"].transform(lambda s: s.pct_change() * -1)
    raw_np = raw.to_numpy(dtype=float, na_value=np.nan)
    raw_np = np.where(np.isinf(raw_np), np.nan, raw_np)
    clipped = np.clip(raw_np, -1.0, 10.0)

    df["decline_rate"] = pd.array(clipped, dtype=pd.Float64Dtype())

    if index_was_well_id:
        df = df.set_index("well_id")
    return df


def _add_rolling_features(df: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    """Add rolling average columns for OilProduced, GasProduced, WaterProduced per well.

    Columns named: oil_rolling_{w}m, gas_rolling_{w}m, water_rolling_{w}m.
    min_periods=1 ensures partial-window means for early history (TR-09b).
    """
    df = df.copy()
    index_was_well_id = df.index.name == "well_id"
    if index_was_well_id:
        df = df.reset_index()

    for w in windows:
        for col, prefix in [
            ("OilProduced", "oil"),
            ("GasProduced", "gas"),
            ("WaterProduced", "water"),
        ]:
            new_col = f"{prefix}_rolling_{w}m"
            if col in df.columns:
                result = df.groupby("well_id")[col].transform(
                    lambda s, window=w: s.rolling(window, min_periods=1).mean()
                )
                df[new_col] = result.astype(pd.Float64Dtype())
            else:
                df[new_col] = pd.array([], dtype=pd.Float64Dtype()) if len(df) == 0 else pd.NA

    if index_was_well_id:
        df = df.set_index("well_id")
    return df


def _add_lag_features(df: pd.DataFrame, lags: list[int]) -> pd.DataFrame:
    """Add oil lag feature columns: oil_lag_{l}m for each lag l.

    First l records per well will be NaN (no prior history). (TR-09c)
    """
    df = df.copy()
    index_was_well_id = df.index.name == "well_id"
    if index_was_well_id:
        df = df.reset_index()

    for lag in lags:
        new_col = f"oil_lag_{lag}m"
        result = df.groupby("well_id")["OilProduced"].transform(lambda s, lag_n=lag: s.shift(lag_n))
        df[new_col] = result.astype(pd.Float64Dtype())

    if index_was_well_id:
        df = df.set_index("well_id")
    return df


def _engineer_features_partition(
    df: pd.DataFrame, windows: list[int], lags: list[int]
) -> pd.DataFrame:
    """Apply all feature engineering steps to one partition.

    Order: cumulative → ratios → decline → rolling → lag.
    """
    df = _add_cumulative_features(df)
    df = _add_ratio_features(df)
    df = _add_decline_rates(df)
    df = _add_rolling_features(df, windows)
    df = _add_lag_features(df, lags)
    return df


def validate_feature_output(ddf: dd.DataFrame, config: dict) -> None:
    """Validate the feature-engineered Dask DataFrame before writing.

    Raises:
        FeaturesError: If required columns are missing or schema drifts.
    """
    windows: list[int] = config.get("features", {}).get("rolling_windows", [3, 6])
    lags: list[int] = config.get("features", {}).get("lag_periods", [1, 3])

    required = list(REQUIRED_OUTPUT_COLUMNS)
    for w in windows:
        for prefix in ("oil", "gas", "water"):
            required.append(f"{prefix}_rolling_{w}m")
    for lag in lags:
        required.append(f"oil_lag_{lag}m")

    actual = set(ddf.columns) | ({ddf.index.name} if ddf.index.name else set())
    missing = [c for c in required if c not in actual]
    if missing:
        raise FeaturesError(f"Missing required feature columns: {missing}")

    # Sample up to 3 partitions for validation
    n_sample = min(3, ddf.npartitions)
    sample_delayed = [ddf.get_partition(i) for i in range(n_sample)]
    sample_frames = dask.compute(*sample_delayed)

    # Check physical bounds via sampled frames
    for frame in sample_frames:
        if "gor" in frame.columns:
            bad_gor = frame["gor"].dropna()
            neg_gor = bad_gor[bad_gor < 0]
            if len(neg_gor) > 0:
                logger.warning("GOR has %d negative values", len(neg_gor))
        if "water_cut" in frame.columns:
            wc = frame["water_cut"].dropna()
            bad_wc = wc[(wc < 0) | (wc > 1)]
            if len(bad_wc) > 0:
                logger.warning("water_cut has %d values outside [0,1]", len(bad_wc))
        if "decline_rate" in frame.columns:
            dr = frame["decline_rate"].dropna()
            bad_dr = dr[(dr < -1.0) | (dr > 10.0)]
            if len(bad_dr) > 0:
                logger.warning("decline_rate has %d values outside [-1, 10]", len(bad_dr))

    # Schema consistency across sampled partitions
    if len(sample_frames) > 1:
        ref_cols = list(sample_frames[0].columns)
        ref_dtypes = dict(sample_frames[0].dtypes)
        for i, frame in enumerate(sample_frames[1:], start=1):
            if list(frame.columns) != ref_cols:
                raise FeaturesError(f"Schema drift detected between partition 0 and partition {i}")
            for col, dtype in ref_dtypes.items():
                if col in frame.dtypes and str(frame.dtypes[col]) != str(dtype):
                    raise FeaturesError(f"Dtype mismatch in partition {i} for column {col}")

    logger.info("Feature output validation passed: %d required columns present", len(required))


def run_features(config: dict) -> dd.DataFrame:
    """Orchestrate the full features stage.

    Returns:
        Dask DataFrame (not computed on full dataset).
    """
    try:
        from distributed import get_client

        client = get_client()
        logger.info("Reusing existing Dask client: %s", client)
    except ValueError:
        from distributed import Client, LocalCluster

        dask_cfg = config["dask"]
        cluster = LocalCluster(
            n_workers=dask_cfg.get("n_workers", 2),
            threads_per_worker=dask_cfg.get("threads_per_worker", 2),
            memory_limit=dask_cfg.get("memory_limit", "3GB"),
        )
        client = Client(cluster)
        logger.info("Initialised Dask LocalCluster: dashboard at %s", client.dashboard_link)

    processed_path = config["features"]["processed_dir"] + "/production.parquet"
    ddf = dd.read_parquet(processed_path, engine="pyarrow")

    windows: list[int] = config.get("features", {}).get("rolling_windows", [3, 6])
    lags: list[int] = config.get("features", {}).get("lag_periods", [1, 3])

    meta = _engineer_features_partition(ddf._meta.copy(), windows, lags)
    ddf = ddf.map_partitions(_engineer_features_partition, windows, lags, meta=meta)

    validate_feature_output(ddf, config)

    ddf = ddf.repartition(npartitions=max(10, min(ddf.npartitions, 50)))

    out_path = config["features"]["output_dir"] + "/production_features.parquet"
    ddf.to_parquet(out_path, engine="pyarrow", write_index=True, overwrite=True)
    logger.info("Wrote features Parquet to %s", out_path)

    return ddf
