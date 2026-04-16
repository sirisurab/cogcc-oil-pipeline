"""Features stage: compute all ML-ready derived features from processed Parquet."""

import logging
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd

from cogcc_pipeline.utils import get_partition_count, load_config

logger = logging.getLogger(__name__)

_WELL_STATUS_ORDER = ["AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"]
_WELL_STATUS_ENCODING = {code: i for i, code in enumerate(_WELL_STATUS_ORDER)}


# ---------------------------------------------------------------------------
# FEA-01: Cumulative production features
# ---------------------------------------------------------------------------


def _add_cumulative_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add cum_oil, cum_gas, cum_water per well. Pure and stateless.

    Uses cumsum with skipna=False so null periods propagate through cumulative.
    """
    df = df.copy()

    for src, dst in [
        ("OilProduced", "cum_oil"),
        ("GasProduced", "cum_gas"),
        ("WaterProduced", "cum_water"),
    ]:
        if src in df.columns:
            df[dst] = df.groupby(level="well_id", group_keys=False)[src].cumsum(skipna=False)
        else:
            df[dst] = np.nan

    return df


# ---------------------------------------------------------------------------
# FEA-02: Production ratio features (GOR, WOR, WGR, water cut)
# ---------------------------------------------------------------------------


def _add_ratio_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add gor, water_cut, wor, wgr ratio columns. Pure and stateless.

    All zero-denominator cases produce NaN (not exception, not inf).
    """
    df = df.copy()

    oil = df["OilProduced"] if "OilProduced" in df.columns else pd.Series(dtype=float)
    gas = df["GasProduced"] if "GasProduced" in df.columns else pd.Series(dtype=float)
    water = df["WaterProduced"] if "WaterProduced" in df.columns else pd.Series(dtype=float)

    # GOR: gas / oil; zero oil → NaN
    gor = np.where(
        oil.isna() | gas.isna(),
        np.nan,
        np.where(oil > 0, gas / oil, np.nan),
    )
    # When oil=0 and gas=0 → NaN (already handled by second np.where → NaN)
    # When oil>0 and gas=0 → 0.0
    gor = np.where(
        (~oil.isna()) & (~gas.isna()) & (oil > 0) & (gas == 0),
        0.0,
        gor,
    )
    df["gor"] = gor.astype(float)

    # Water cut: water / (oil + water); both zero → NaN
    total_liq = oil + water
    wc = np.where(
        oil.isna() | water.isna(),
        np.nan,
        np.where(total_liq > 0, water / total_liq, np.nan),
    )
    df["water_cut"] = wc.astype(float)

    # WOR: water / oil; zero oil → NaN
    wor = np.where(
        oil.isna() | water.isna(),
        np.nan,
        np.where(oil > 0, water / oil, np.nan),
    )
    df["wor"] = wor.astype(float)

    # WGR: water / gas; zero gas → NaN
    wgr = np.where(
        gas.isna() | water.isna(),
        np.nan,
        np.where(gas > 0, water / gas, np.nan),
    )
    df["wgr"] = wgr.astype(float)

    return df


# ---------------------------------------------------------------------------
# FEA-03: Decline rate feature
# ---------------------------------------------------------------------------


def _add_decline_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Add decline_rate_oil and decline_rate_gas per well. Pure and stateless.

    Rates clipped to [-1.0, 10.0]; inf/nan from zero-prior-period → NaN.
    Prior=0, current=0 → 0.0; Prior=0, current>0 → NaN (TR-07d).
    """
    df = df.copy()

    for src, dst in [("OilProduced", "decline_rate_oil"), ("GasProduced", "decline_rate_gas")]:
        if src not in df.columns:
            df[dst] = np.nan
            continue

        series = df.groupby(level="well_id", group_keys=False)[src]

        raw = series.pct_change()

        # Handle TR-07d: prior=0 and current=0 → 0.0; prior=0 and current>0 → NaN
        prev_vals = series.shift(1)
        curr_vals = df[src]

        both_zero = (prev_vals == 0) & (curr_vals == 0)
        prior_zero_curr_pos = (prev_vals == 0) & (curr_vals > 0)

        # Replace inf/-inf with NaN first
        raw = raw.replace([np.inf, -np.inf], np.nan)

        # Apply TR-07d corrections
        raw = raw.where(~both_zero, other=0.0)
        raw = raw.where(~prior_zero_curr_pos, other=np.nan)

        # Clip to [-1.0, 10.0] (NaN remains NaN — clip() preserves NaN)
        df[dst] = raw.clip(-1.0, 10.0)

    return df


# ---------------------------------------------------------------------------
# FEA-04: Rolling average features
# ---------------------------------------------------------------------------


def _add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add 3, 6, 12-month rolling means for oil, gas, water per well. Pure and stateless.

    Uses min_periods=1 so partial windows are computed from available data (not NaN).
    """
    df = df.copy()

    for src, prefix in [("OilProduced", "oil"), ("GasProduced", "gas"), ("WaterProduced", "water")]:
        if src not in df.columns:
            for w in [3, 6, 12]:
                df[f"{prefix}_roll{w}"] = np.nan
            continue

        grp = df.groupby(level="well_id", group_keys=False)[src]
        for window in [3, 6, 12]:
            col = f"{prefix}_roll{window}"
            df[col] = grp.rolling(window=window, min_periods=1).mean().values

    return df


# ---------------------------------------------------------------------------
# FEA-05: Lag features
# ---------------------------------------------------------------------------


def _add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add 1-period lag features for oil, gas, water per well. Pure and stateless."""
    df = df.copy()

    for src, dst in [
        ("OilProduced", "oil_lag1"),
        ("GasProduced", "gas_lag1"),
        ("WaterProduced", "water_lag1"),
    ]:
        if src in df.columns:
            df[dst] = df.groupby(level="well_id", group_keys=False)[src].shift(1)
        else:
            df[dst] = np.nan

    return df


# ---------------------------------------------------------------------------
# FEA-06: Categorical encoding and days normalization
# ---------------------------------------------------------------------------


def _add_encoded_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add wellstatus_encoded (Int64) and days_produced_norm (float64). Pure and stateless."""
    df = df.copy()

    # WellStatus ordinal encoding
    if "WellStatus" in df.columns:
        ws = df["WellStatus"]
        # Map categories to codes; NA → -1
        # Convert to str for mapping, handle NA
        ws_str = ws.astype(object)
        encoded = ws_str.map(lambda v: _WELL_STATUS_ENCODING.get(v, -1) if v is not None else -1)
        df["wellstatus_encoded"] = pd.array(encoded.tolist(), dtype="Int64")
    else:
        df["wellstatus_encoded"] = pd.array([-1] * len(df), dtype="Int64")

    # DaysProduced normalization
    if "DaysProduced" in df.columns:
        dp = df["DaysProduced"]
        # Float division, preserve NA
        norm = dp.astype(float) / 31.0
        df["days_produced_norm"] = norm
    else:
        df["days_produced_norm"] = np.nan

    return df


# ---------------------------------------------------------------------------
# FEA-07: Features pipeline assembler
# ---------------------------------------------------------------------------


def run_features(config_path: str = "config/config.yaml") -> None:
    """Top-level entry point for the features stage.

    Reads processed Parquet, computes all feature columns, writes ML-ready Parquet
    to data/features/.
    """
    config = load_config(config_path)
    fea_cfg = config["features"]

    processed_dir = Path(fea_cfg["processed_dir"])
    features_dir = Path(fea_cfg["features_dir"])

    parquet_files = list(processed_dir.glob("*.parquet"))
    if not parquet_files:
        logger.warning("No Parquet files found in %s — features skipped", processed_dir)
        return

    n = len(parquet_files)

    try:
        ddf = dd.read_parquet(str(processed_dir))

        # Chain all 6 map_partitions calls lazily
        meta1 = _add_cumulative_features(ddf._meta.copy())
        ddf = ddf.map_partitions(_add_cumulative_features, meta=meta1)

        meta2 = _add_ratio_features(ddf._meta.copy())
        ddf = ddf.map_partitions(_add_ratio_features, meta=meta2)

        meta3 = _add_decline_rates(ddf._meta.copy())
        ddf = ddf.map_partitions(_add_decline_rates, meta=meta3)

        meta4 = _add_rolling_features(ddf._meta.copy())
        ddf = ddf.map_partitions(_add_rolling_features, meta=meta4)

        meta5 = _add_lag_features(ddf._meta.copy())
        ddf = ddf.map_partitions(_add_lag_features, meta=meta5)

        meta6 = _add_encoded_features(ddf._meta.copy())
        ddf = ddf.map_partitions(_add_encoded_features, meta=meta6)

        # Repartition last (ADR-004)
        ddf = ddf.repartition(npartitions=get_partition_count(n))

        features_dir.mkdir(parents=True, exist_ok=True)
        ddf.to_parquet(str(features_dir), write_index=True, overwrite=True)

        logger.info(
            "Features complete: %d feature columns in schema, output → %s",
            len(ddf.columns),
            features_dir,
        )

    except Exception as exc:
        logger.error("Features stage failed in function: %s", exc)
        raise
