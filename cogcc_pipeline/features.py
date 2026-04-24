"""Features stage — compute derived ML features and write to data/features/."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
from distributed import Client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Task F1 — Cumulative production features
# ---------------------------------------------------------------------------


def add_cumulative_features(pdf: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Add per-well cumulative production columns to a partition.

    Columns added: cum_oil, cum_gas, cum_water (float).

    Input is sorted by production_date within each well group
    (boundary-transform-features downstream reliance).

    Args:
        pdf: Partition from processed Parquet (entity-indexed, sorted by date).
        config: The ``features`` section of config.yaml (unused here but
            kept for consistent signature across F1-F4).

    Returns:
        Partition with input columns plus cum_oil, cum_gas, cum_water.
    """
    if pdf.empty:
        return _add_empty_cols(pdf, ["cum_oil", "cum_gas", "cum_water"])

    df = pdf.copy()
    entity_col = df.index.name if df.index.name else "well_id"

    # Reset index temporarily so groupby can reference entity
    df_reset = df.reset_index()

    for vol_col, cum_col in [
        ("OilProduced", "cum_oil"),
        ("GasProduced", "cum_gas"),
        ("WaterProduced", "cum_water"),
    ]:
        if vol_col in df_reset.columns:
            # Use fillna(0) for cumsum so NA months don't propagate, but
            # distinguish NA (missing) from 0 (shut-in) — treat NA as 0
            # for cumulative purposes (produces flat segment, TR-08)
            vol = pd.to_numeric(df_reset[vol_col], errors="coerce").fillna(0.0)
            df_reset[cum_col] = df_reset.groupby(str(entity_col))[str(vol.name)].transform(
                lambda s: s.cumsum()  # noqa: B023
            )
            # Restore NA where original was NA (keep flat, not forward-cum)
            # Per TR-08: zero→flat is correct; NA months: flat by treating as 0
            # cumulative must be non-decreasing — fillna(0) achieves this
            df_reset[cum_col] = df_reset[cum_col].astype("float64")
        else:
            df_reset[cum_col] = pd.array([pd.NA] * len(df_reset), dtype="Float64")

    # Restore original index
    df_reset = df_reset.set_index(entity_col)
    return df_reset


def _add_empty_cols(pdf: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Add float NA columns to an empty partition (TR-23)."""
    df = pdf.copy()
    for col in cols:
        df[col] = pd.Series([], dtype="float64")
    return df


# ---------------------------------------------------------------------------
# Task F2 — Ratio features: GOR and water cut
# ---------------------------------------------------------------------------


def add_ratio_features(pdf: pd.DataFrame, config: dict) -> pd.DataFrame:  # noqa: ARG001
    """Add gor and water_cut columns to a partition.

    Formulas (TR-09 d, e):
    - gor = GasProduced / OilProduced
    - water_cut = WaterProduced / (OilProduced + WaterProduced)

    Zero-denominator handling (TR-06):
    - oil=0, gas>0 → gor = NaN
    - oil=0, gas=0 → gor = 0.0 (shut-in well convention)
    - oil>0, gas=0 → gor = 0.0

    Water cut boundary values 0.0 and 1.0 are valid (TR-10) and are retained.

    Args:
        pdf: Partition from processed Parquet.
        config: The ``features`` section of config.yaml.

    Returns:
        Partition with input columns plus gor and water_cut (float).
    """
    if pdf.empty:
        return _add_empty_cols(pdf, ["gor", "water_cut"])

    df = pdf.copy()

    oil = pd.to_numeric(df.get("OilProduced", pd.Series(0.0, index=df.index)), errors="coerce")
    gas = pd.to_numeric(df.get("GasProduced", pd.Series(0.0, index=df.index)), errors="coerce")
    water = pd.to_numeric(df.get("WaterProduced", pd.Series(0.0, index=df.index)), errors="coerce")

    # GOR computation (TR-06)
    # oil=0, gas>0 → NaN; oil=0, gas=0 → 0.0; oil>0, gas=0 → 0.0; general → gas/oil
    oil_zero = oil == 0.0
    gas_zero = gas == 0.0

    gor = gas / oil.replace(0.0, float("nan"))  # produces NaN where oil==0
    # oil=0, gas=0 → shut-in → gor = 0.0
    gor = gor.where(~(oil_zero & gas_zero), other=0.0)
    # oil>0, gas=0 → gor = 0.0 (already handled by 0/positive = 0 in numerator)
    df["gor"] = gor.astype("float64")

    # Water cut (TR-09 e)
    total_liquid = oil + water
    water_cut = water / total_liquid.replace(0.0, float("nan"))
    # both zero → 0.0 (dry shut-in)
    water_cut = water_cut.where(total_liquid != 0.0, other=0.0)
    df["water_cut"] = water_cut.clip(lower=0.0, upper=1.0).astype("float64")

    return df


# ---------------------------------------------------------------------------
# Task F3 — Rolling-window and lag features
# ---------------------------------------------------------------------------


def add_rolling_and_lag_features(pdf: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Add rolling averages, lag features, percent-change, and peak-month flag.

    Rolling windows and lag sizes come from config (TR-09). At minimum:
    - 3-month and 6-month rolling averages for OilProduced, GasProduced, WaterProduced
    - 12-month rolling average for oil and gas
    - Lag-1 for OilProduced
    - Month-over-month percent change for oil and gas
    - Per-well peak-production-month boolean flag

    Args:
        pdf: Partition from processed Parquet.
        config: The ``features`` section of config.yaml.

    Returns:
        Partition with rolling, lag, pct-change, and peak-month columns added.
    """
    rolling_windows: list[int] = list(config.get("rolling_windows", [3, 6, 12]))
    lag_sizes: list[int] = list(config.get("lag_sizes", [1]))

    if pdf.empty:
        extra_cols: list[str] = []
        for w in rolling_windows:
            for col in ["OilProduced", "GasProduced", "WaterProduced"]:
                extra_cols.append(f"{col}_rolling_{w}m")
        for lag in lag_sizes:
            extra_cols.append(f"OilProduced_lag_{lag}")
        extra_cols += [
            "OilProduced_pct_change",
            "GasProduced_pct_change",
            "is_peak_oil_month",
        ]
        return _add_empty_cols(pdf, extra_cols)

    df = pdf.copy()
    entity_col = df.index.name if df.index.name else "well_id"
    df_reset = df.reset_index()

    # Rolling averages per well (ADR-002: grouped vectorized transforms)
    for w in rolling_windows:
        for col in ["OilProduced", "GasProduced", "WaterProduced"]:
            out_col = f"{col}_rolling_{w}m"
            if col in df_reset.columns:
                df_reset[out_col] = df_reset.groupby(str(entity_col))[col].transform(
                    lambda s, window=w: (
                        pd.to_numeric(s, errors="coerce")  # type: ignore[union-attr, attr-defined]
                        .rolling(window=window, min_periods=window)
                        .mean()
                    )
                )  # type: ignore[union-attr, attr-defined]
            else:
                df_reset[out_col] = float("nan")
            df_reset[out_col] = df_reset[out_col].astype("float64")

    # Lag features (TR-09 c)
    for lag in lag_sizes:
        out_col = f"OilProduced_lag_{lag}"
        if "OilProduced" in df_reset.columns:
            df_reset[out_col] = df_reset.groupby(str(entity_col))["OilProduced"].transform(
                lambda s, n=lag: pd.to_numeric(s, errors="coerce").shift(n)  # type: ignore[union-attr, attr-defined]
            )
        else:
            df_reset[out_col] = float("nan")
        df_reset[out_col] = df_reset[out_col].astype("float64")

    # Month-over-month percent change
    for col in ["OilProduced", "GasProduced"]:
        out_col = f"{col}_pct_change"
        if col in df_reset.columns:
            df_reset[out_col] = df_reset.groupby(str(entity_col))[col].transform(
                lambda s: pd.to_numeric(s, errors="coerce").pct_change()  # type: ignore[union-attr, attr-defined]
            )
        else:
            df_reset[out_col] = float("nan")
        df_reset[out_col] = df_reset[out_col].astype("float64")

    # Per-well peak-production-month boolean flag
    if "OilProduced" in df_reset.columns:
        oil_numeric = pd.to_numeric(df_reset["OilProduced"], errors="coerce")
        df_reset["_oil_for_peak"] = oil_numeric
        group_max = df_reset.groupby(str(entity_col))["_oil_for_peak"].transform("max")
        df_reset["is_peak_oil_month"] = (oil_numeric == group_max) & oil_numeric.notna()
        df_reset = df_reset.drop(columns=["_oil_for_peak"])
    else:
        df_reset["is_peak_oil_month"] = False

    df_reset["is_peak_oil_month"] = df_reset["is_peak_oil_month"].astype("bool")

    df_reset = df_reset.set_index(entity_col)
    return df_reset


# ---------------------------------------------------------------------------
# Task F4 — Decline-rate feature with clip bounds
# ---------------------------------------------------------------------------


def add_decline_rate(pdf: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Add decline_rate column (period-over-period, clipped).

    The raw decline rate is computed as (prev - curr) / prev, then clipped to
    configured bounds. Division-by-zero and zero/zero are resolved to NaN
    before clipping (TR-07 d).

    Args:
        pdf: Partition from processed Parquet.
        config: The ``features`` section of config.yaml.

    Returns:
        Partition with decline_rate (float) added.
    """
    lower: float = float(config.get("decline_rate_lower_bound", -1.0))
    upper: float = float(config.get("decline_rate_upper_bound", 10.0))

    if pdf.empty:
        return _add_empty_cols(pdf, ["decline_rate"])

    df = pdf.copy()
    entity_col = df.index.name if df.index.name else "well_id"
    df_reset = df.reset_index()

    if "OilProduced" in df_reset.columns:
        oil = pd.to_numeric(df_reset["OilProduced"], errors="coerce")
        prev_oil = df_reset.groupby(str(entity_col))["OilProduced"].transform(
            lambda s: pd.to_numeric(s, errors="coerce").shift(1)  # type: ignore[union-attr, attr-defined]
        )
        prev_oil = pd.to_numeric(prev_oil, errors="coerce")

        # Decline rate: (prev - curr) / prev
        # Replace 0 denominator with NaN BEFORE division (TR-07 d)
        safe_prev = prev_oil.replace(0.0, float("nan"))
        raw_decline = (prev_oil - oil) / safe_prev

        # Clip to configured bounds
        df_reset["decline_rate"] = raw_decline.clip(lower=lower, upper=upper).astype("float64")
    else:
        df_reset["decline_rate"] = pd.array([pd.NA] * len(df_reset), dtype="Float64").astype(
            "float64"
        )

    df_reset = df_reset.set_index(entity_col)
    return df_reset


# ---------------------------------------------------------------------------
# Features stage orchestration
# ---------------------------------------------------------------------------


def _partition_count(n: int, cfg_min: int = 10, cfg_max: int = 50) -> int:
    return max(cfg_min, min(n, cfg_max))


def features(config: dict) -> None:
    """Stage entry point. Read processed Parquet, compute features, write output.

    Applies F1–F4 via map_partitions. Meta is derived by calling each function
    on a zero-row input (ADR-003). Lazy graph, single compute at write (ADR-005).

    Args:
        config: Full pipeline config dict (uses ``features`` sub-section).
    """
    features_cfg: dict = config.get("features", config)
    processed_dir = Path(features_cfg.get("processed_dir", "data/processed"))
    features_dir = Path(features_cfg.get("features_dir", "data/features"))
    p_min: int = int(features_cfg.get("partition_min", 10))
    p_max: int = int(features_cfg.get("partition_max", 50))

    features_dir.mkdir(parents=True, exist_ok=True)

    # Reuse existing distributed client (build-env-manifest)
    try:
        Client.current()
    except ValueError:
        pass

    ddf = dd.read_parquet(str(processed_dir), engine="pyarrow")
    logger.info("Loaded processed Parquet: %d partitions", ddf.npartitions)

    # Meta derivation per ADR-003: call actual function on zero-row real input
    meta0 = ddf._meta.copy()
    meta_cum = add_cumulative_features(meta0, features_cfg)
    meta_ratio = add_ratio_features(meta_cum, features_cfg)
    meta_roll = add_rolling_and_lag_features(meta_ratio, features_cfg)
    meta_decline = add_decline_rate(meta_roll, features_cfg)

    # Build lazy graph — no intermediate .compute() (ADR-005)
    ddf_cum = ddf.map_partitions(add_cumulative_features, features_cfg, meta=meta_cum)
    ddf_ratio = ddf_cum.map_partitions(add_ratio_features, features_cfg, meta=meta_ratio)
    ddf_roll = ddf_ratio.map_partitions(add_rolling_and_lag_features, features_cfg, meta=meta_roll)
    ddf_out = ddf_roll.map_partitions(add_decline_rate, features_cfg, meta=meta_decline)

    n_parts = _partition_count(ddf_out.npartitions, p_min, p_max)
    ddf_final = ddf_out.repartition(npartitions=n_parts)

    logger.info("Writing features Parquet to %s (%d partitions)", features_dir, n_parts)
    # Single compute at write (ADR-005)
    ddf_final.to_parquet(
        str(features_dir),
        engine="pyarrow",
        write_index=True,
        overwrite=True,
    )
    logger.info("Features complete — features Parquet written")
