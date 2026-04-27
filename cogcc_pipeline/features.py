"""Features stage: compute derived ML-ready features from processed Parquet."""

import logging
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

REQUIRED_FEATURES_KEYS = ["processed_dir", "output_dir"]


def load_config(config_path: str) -> dict:
    """Read config.yaml and return the full config dict."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    features_cfg: dict = cfg.get("features", {})
    for key in REQUIRED_FEATURES_KEYS:
        if key not in features_cfg:
            raise KeyError(f"Missing required features config key: '{key}'")
    return cfg


def _add_cumulative_features(partition: pd.DataFrame) -> pd.DataFrame:
    """Add cum_oil, cum_gas, cum_water columns per entity (ADR-002 vectorized groupby).

    Temporal ordering is guaranteed by transform stage (boundary-transform-features.md).
    Nulls treated as zero for cumulative purposes (TR-08).
    """
    result = partition.copy()

    entity_col = result.index.name if result.index.name else "ApiSequenceNumber"
    if entity_col in result.columns:
        gb = result.groupby(str(entity_col), sort=False)
    else:
        # Index is entity — group by level 0 to avoid Hashable type mismatch
        gb = result.groupby(level=0, sort=False)  # type: ignore[assignment]

    result["cum_oil"] = (
        gb["OilProduced"].transform(lambda x: x.fillna(0.0).cumsum()).astype("float64")
    )
    result["cum_gas"] = (
        gb["GasProduced"].transform(lambda x: x.fillna(0.0).cumsum()).astype("float64")
    )
    result["cum_water"] = (
        gb["WaterProduced"]
        .transform(lambda x: x.fillna(0.0).cumsum())
        .astype("float64")
    )
    return result


def _add_ratio_features(partition: pd.DataFrame) -> pd.DataFrame:
    """Add gor and water_cut columns (TR-06, TR-09d, TR-09e, TR-10)."""
    result = partition.copy()

    oil = result["OilProduced"].astype("float64")
    gas = result["GasProduced"].astype("float64")
    water = result["WaterProduced"].astype("float64")

    # GOR: gas / oil, with zero-denominator handling (TR-06)
    gor = np.where(oil > 0, gas / oil, np.nan)
    result["gor"] = gor.astype("float64")

    # water_cut: water / (oil + water), zero-denominator → NaN (TR-09e, TR-10)
    total_liquid = oil + water
    water_cut = np.where(total_liquid > 0, water / total_liquid, np.nan)
    result["water_cut"] = water_cut.astype("float64")

    return result


def _add_decline_rates(partition: pd.DataFrame) -> pd.DataFrame:
    """Add decline_rate_oil column per entity group (TR-07).

    Formula: (prev - current) / prev. Zero prev → NaN before clipping.
    Clipped to [-1.0, 10.0].
    """
    result = partition.copy()

    entity_col = result.index.name if result.index.name else "ApiSequenceNumber"
    if entity_col in result.columns:
        gb = result.groupby(str(entity_col), sort=False)
    else:
        gb = result.groupby(level=0, sort=False)  # type: ignore[assignment]

    oil = result["OilProduced"].astype("float64")
    prev_oil = gb["OilProduced"].transform(lambda x: x.shift(1)).astype("float64")

    # Zero denominator: use NaN before clipping (TR-07d)
    raw_rate = np.where(prev_oil > 0, (prev_oil - oil) / prev_oil, np.nan)
    clipped = np.clip(raw_rate, -1.0, 10.0)
    result["decline_rate_oil"] = clipped.astype("float64")
    return result


def _add_rolling_and_lag_features(partition: pd.DataFrame) -> pd.DataFrame:
    """Add rolling averages (3m, 6m) and 1-month lag features per entity (TR-09a-c).

    Rolling uses min_periods=window so partial windows yield NaN (TR-09b).
    """
    result = partition.copy()

    entity_col = result.index.name if result.index.name else "ApiSequenceNumber"
    if entity_col in result.columns:
        gb = result.groupby(str(entity_col), sort=False)
    else:
        gb = result.groupby(level=0, sort=False)  # type: ignore[assignment]

    for col, prefix in [
        ("OilProduced", "oil"),
        ("GasProduced", "gas"),
        ("WaterProduced", "water"),
    ]:
        result[f"{prefix}_rolling_3m"] = (
            gb[col]
            .transform(lambda x: x.astype("float64").rolling(3, min_periods=3).mean())
            .astype("float64")
        )
        result[f"{prefix}_rolling_6m"] = (
            gb[col]
            .transform(lambda x: x.astype("float64").rolling(6, min_periods=6).mean())
            .astype("float64")
        )
        result[f"{prefix}_lag_1"] = (
            gb[col].transform(lambda x: x.astype("float64").shift(1)).astype("float64")
        )

    return result


def _compute_features_partition(partition: pd.DataFrame) -> pd.DataFrame:
    """Apply all feature computation steps in sequence to a single partition."""
    result = _add_cumulative_features(partition)
    result = _add_ratio_features(result)
    result = _add_decline_rates(result)
    result = _add_rolling_and_lag_features(result)
    return result


def write_parquet(ddf: dd.DataFrame, output_dir: str) -> None:
    """Repartition and write ML-ready Parquet (ADR-004)."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    n = ddf.npartitions
    n_out = max(10, min(n, 50))
    # Repartition is last operation before write (ADR-004)
    ddf.repartition(npartitions=n_out).to_parquet(str(out))


def features(config: dict) -> None:
    """Orchestrate the full features stage."""
    features_cfg: dict = config.get("features", config)
    processed_dir = str(features_cfg["processed_dir"])
    output_dir = str(features_cfg["output_dir"])

    processed_path = Path(processed_dir)
    parquet_files = list(processed_path.glob("*.parquet")) + list(
        processed_path.glob("**/*.parquet")
    )
    if not parquet_files:
        logger.warning(
            "No Parquet files found in %s — skipping features", processed_dir
        )
        return

    ddf = dd.read_parquet(processed_dir)

    # ADR-003: derive meta by calling actual function on minimal real input and
    # slicing to zero rows. Scan partitions to find a non-empty one so that
    # categorical columns retain their declared categories (not null) in the
    # Arrow schema inferred from meta.
    sample: pd.DataFrame = pd.DataFrame()
    for i in range(ddf.npartitions):
        candidate = ddf.get_partition(i).compute()
        if len(candidate) > 0:
            sample = candidate.iloc[:1]
            break
    if sample.empty:
        sample = ddf.get_partition(0).compute().iloc[:1]
    meta = _compute_features_partition(sample).iloc[0:0]

    result = ddf.map_partitions(_compute_features_partition, meta=meta)

    # Lazy until write (ADR-005)
    write_parquet(result, output_dir)
    logger.info("Features complete — wrote ML-ready Parquet to %s", output_dir)
