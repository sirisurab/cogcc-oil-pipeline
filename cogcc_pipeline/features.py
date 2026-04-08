"""Features stage: engineer ML-ready features from cleaned production data."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client, LocalCluster

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared: Dask client management
# ---------------------------------------------------------------------------


def get_or_create_client(config: dict) -> tuple[Client, bool]:
    """Return existing Dask distributed Client or create a new one.

    Args:
        config: Full config dict.

    Returns:
        Tuple of (Client, created).
    """
    from dask.distributed import get_client

    try:
        client = get_client()
        logger.info("Reusing existing Dask client. Dashboard: %s", client.dashboard_link)
        return client, False
    except ValueError:
        dcfg = config["dask"]
        cluster = LocalCluster(
            n_workers=dcfg["n_workers"],
            threads_per_worker=dcfg["threads_per_worker"],
            memory_limit=dcfg["memory_limit"],
            dashboard_address=f":{dcfg['dashboard_port']}",
        )
        client = Client(cluster)
        logger.info("Created new Dask client. Dashboard: %s", client.dashboard_link)
        return client, True


# ---------------------------------------------------------------------------
# F-01: Time features
# ---------------------------------------------------------------------------


def _add_time_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add year, month, quarter, days_in_month, months_since_2020 columns.

    Args:
        pdf: Pandas partition with production_date column.

    Returns:
        DataFrame with five new time feature columns.
    """
    pdf = pdf.copy()
    dt = pdf["production_date"].dt
    year = dt.year.astype(pd.Int64Dtype())
    month = dt.month.astype(pd.Int64Dtype())
    pdf["year"] = year
    pdf["month"] = month
    pdf["quarter"] = dt.quarter.astype(pd.Int64Dtype())
    pdf["days_in_month"] = dt.days_in_month.astype(pd.Int64Dtype())
    pdf["months_since_2020"] = ((year - 2020) * 12 + (month - 1)).astype(pd.Int64Dtype())
    return pdf


# ---------------------------------------------------------------------------
# F-02: Rolling statistics per well
# ---------------------------------------------------------------------------


def _add_rolling_features(pdf: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    """Add per-well rolling mean and std for each production column and window.

    Uses groupby().transform() — never iterates over groups.

    Args:
        pdf: Pandas partition. well_id may be the index or a column.
        windows: List of rolling window sizes (e.g. [3, 6]).

    Returns:
        DataFrame with rolling feature columns added.
    """
    pdf = pdf.copy()
    prod_cols = ["OilProduced", "GasProduced", "WaterProduced"]
    prefix_map = {
        "OilProduced": "oil",
        "GasProduced": "gas",
        "WaterProduced": "water",
    }

    # Handle well_id as index or column
    if "well_id" not in pdf.columns and pdf.index.name == "well_id":
        grp = pdf.groupby(level=0)
    else:
        grp = pdf.groupby("well_id")  # type: ignore[assignment]

    for col in prod_cols:
        prefix = prefix_map[col]
        for w in windows:
            mean_col = f"{prefix}_roll{w}_mean"
            std_col = f"{prefix}_roll{w}_std"
            pdf[mean_col] = (
                grp[col]
                .transform(
                    lambda x, window=w: x.astype("float64").rolling(window, min_periods=1).mean()
                )
                .astype(pd.Float64Dtype())
            )
            pdf[std_col] = (
                grp[col]
                .transform(
                    lambda x, window=w: x.astype("float64").rolling(window, min_periods=1).std()
                )
                .astype(pd.Float64Dtype())
            )

    return pdf


# ---------------------------------------------------------------------------
# F-03: Cumulative production per well
# ---------------------------------------------------------------------------


def _add_cumulative_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add cum_oil, cum_gas, cum_water per well using groupby cumsum.

    NA values are treated as 0 for cumsum, but NAs do not propagate.

    Args:
        pdf: Pandas partition.

    Returns:
        DataFrame with cumulative production columns added.
    """
    pdf = pdf.copy()
    prod_map = {
        "OilProduced": "cum_oil",
        "GasProduced": "cum_gas",
        "WaterProduced": "cum_water",
    }

    for src_col, dst_col in prod_map.items():
        filled = pdf[src_col].fillna(0.0)
        if "well_id" not in pdf.columns and pdf.index.name == "well_id":
            cumsum = filled.groupby(level=0).transform("cumsum")
        else:
            cumsum = filled.groupby(pdf["well_id"]).transform("cumsum")
        pdf[dst_col] = cumsum.astype(pd.Float64Dtype())
    return pdf


# ---------------------------------------------------------------------------
# F-04: Decline rate calculation
# ---------------------------------------------------------------------------


def _add_decline_rates(
    pdf: pd.DataFrame,
    clip_min: float,
    clip_max: float,
) -> pd.DataFrame:
    """Add oil_decline_rate and gas_decline_rate (MoM % change, clipped).

    Args:
        pdf: Pandas partition.
        clip_min: Lower clip bound (e.g. -1.0).
        clip_max: Upper clip bound (e.g. 10.0).

    Returns:
        DataFrame with two new decline rate columns.
    """
    pdf = pdf.copy()

    if "well_id" not in pdf.columns and pdf.index.name == "well_id":

        def grp_fn(col: str) -> pd.Series:
            return pdf[col].groupby(level=0).transform(lambda x: x.shift(1))
    else:

        def grp_fn(col: str) -> pd.Series:
            return pdf[col].groupby(pdf["well_id"]).transform(lambda x: x.shift(1))

    for col, rate_col in [("OilProduced", "oil_decline_rate"), ("GasProduced", "gas_decline_rate")]:
        current = pdf[col].astype("float64")
        prior = grp_fn(col).astype("float64")

        # Compute raw rate: (current - prior) / abs(prior)
        raw = (current - prior) / prior.abs()

        # Edge cases (vectorized via np.where):
        both_zero = (prior == 0.0) & (current == 0.0)
        prior_zero_current_pos = (prior == 0.0) & (current > 0.0)
        either_na = current.isna() | prior.isna()

        rate = raw.copy()
        rate = rate.where(~both_zero, 0.0)
        rate = rate.where(~prior_zero_current_pos, np.nan)
        rate = rate.where(~either_na, np.nan)

        # Clip non-NA values
        rate = rate.clip(lower=clip_min, upper=clip_max)
        pdf[rate_col] = rate.astype(pd.Float64Dtype())

    return pdf


# ---------------------------------------------------------------------------
# F-05: Ratio features (GOR, water cut, WOR)
# ---------------------------------------------------------------------------


def _add_ratio_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add gor, water_cut, wor ratio features with zero-division handling.

    Args:
        pdf: Pandas partition with OilProduced, GasProduced, WaterProduced.

    Returns:
        DataFrame with gor, water_cut, wor columns added.
    """
    pdf = pdf.copy()
    oil = pdf["OilProduced"].astype("float64")
    gas = pdf["GasProduced"].astype("float64")
    water = pdf["WaterProduced"].astype("float64")

    oil_na = pdf["OilProduced"].isna()
    gas_na = pdf["GasProduced"].isna()
    water_na = pdf["WaterProduced"].isna()

    # GOR: GasProduced / OilProduced
    gor = np.where(
        oil_na | gas_na,
        np.nan,
        np.where(
            oil > 0,
            gas / oil,
            np.where(gas == 0, 0.0, np.nan),  # oil=0, gas=0 → 0.0; oil=0, gas>0 → NA
        ),
    )
    pdf["gor"] = pd.array(gor, dtype=pd.Float64Dtype())

    # Water cut: WaterProduced / (OilProduced + WaterProduced)
    denom = oil + water
    water_cut = np.where(
        oil_na | water_na,
        np.nan,
        np.where(denom > 0, water / denom, 0.0),
    )
    pdf["water_cut"] = pd.array(water_cut, dtype=pd.Float64Dtype())

    # WOR: WaterProduced / OilProduced
    wor = np.where(
        oil_na | water_na,
        np.nan,
        np.where(
            oil > 0,
            water / oil,
            np.where(water == 0, 0.0, np.nan),
        ),
    )
    pdf["wor"] = pd.array(wor, dtype=pd.Float64Dtype())

    return pdf


# ---------------------------------------------------------------------------
# F-06: Well age feature
# ---------------------------------------------------------------------------


def _add_well_age(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add well_age_months: months since first production date per well.

    Args:
        pdf: Pandas partition with production_date and well_id (column or index).

    Returns:
        DataFrame with well_age_months column added.
    """
    pdf = pdf.copy()

    if "well_id" not in pdf.columns and pdf.index.name == "well_id":
        first_date = pdf["production_date"].groupby(level=0).transform("min")
    else:
        first_date = pdf.groupby("well_id")["production_date"].transform("min")

    prod_date = pdf["production_date"]
    age = (
        (prod_date.dt.year - first_date.dt.year) * 12 + (prod_date.dt.month - first_date.dt.month)
    ).astype(pd.Int64Dtype())
    pdf["well_age_months"] = age
    return pdf


# ---------------------------------------------------------------------------
# F-07: Label encoder
# ---------------------------------------------------------------------------


def fit_label_encoders(ddf: dd.DataFrame) -> dict[str, dict[str, int]]:
    """Compute label encoder mappings for OpName and FormationCode.

    Sorts alphabetically for reproducibility. NA → -1.

    Args:
        ddf: Dask DataFrame with OpName and FormationCode columns.

    Returns:
        Dict of dicts: {"OpName": {name: int, ...}, "FormationCode": {code: int, ...}}.
    """
    op_unique, form_unique = dask.compute(
        ddf["OpName"].unique(),
        ddf["FormationCode"].unique(),
    )

    def _build_mapping(values: pd.arrays.ArrowExtensionArray | pd.Series | list) -> dict[str, int]:
        vals_clean = sorted(str(v) for v in values if not pd.isna(v))
        mapping: dict[str, int] = {v: i for i, v in enumerate(vals_clean)}
        return mapping

    return {
        "OpName": _build_mapping(op_unique),
        "FormationCode": _build_mapping(form_unique),
    }


def _apply_label_encoding(
    pdf: pd.DataFrame,
    mappings: dict[str, dict[str, int]],
) -> pd.DataFrame:
    """Add operator_encoded and formation_encoded columns using pre-fit mappings.

    Unknown or NA values map to -1.

    Args:
        pdf: Pandas partition.
        mappings: Dict from fit_label_encoders.

    Returns:
        DataFrame with two new encoded columns.
    """
    pdf = pdf.copy()

    def _encode(series: pd.Series, mapping: dict[str, int]) -> pd.Series:
        encoded = series.map(mapping)
        # fillna(-1) for unknowns and NAs
        encoded = encoded.fillna(-1).astype(pd.Int64Dtype())
        return encoded

    pdf["operator_encoded"] = _encode(pdf["OpName"], mappings["OpName"])
    pdf["formation_encoded"] = _encode(pdf["FormationCode"], mappings["FormationCode"])
    return pdf


# ---------------------------------------------------------------------------
# F-08: Encoder mappings writer
# ---------------------------------------------------------------------------


def save_encoder_mappings(mappings: dict[str, dict[str, int]], output_path: Path) -> None:
    """Write encoder mappings to a JSON file.

    Args:
        mappings: Dict from fit_label_encoders.
        output_path: Path to write JSON.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(mappings, f, indent=2)
    logger.info("Encoder mappings written to %s", output_path)


# ---------------------------------------------------------------------------
# F-09: Features stage runner
# ---------------------------------------------------------------------------


def run_features(config: dict) -> None:
    """Top-level features stage: engineer all features and write processed Parquet.

    Args:
        config: Full config dict.

    Raises:
        FileNotFoundError: If cleaned Parquet not found.
        RuntimeError: On computation errors.
    """
    fcfg = config["features"]
    interim_dir = Path(fcfg["interim_dir"])
    processed_dir = Path(fcfg["processed_dir"])
    input_path = interim_dir / "cogcc_cleaned.parquet"
    output_path = processed_dir / "cogcc_features.parquet"
    encoder_path = processed_dir / "encoder_mappings.json"
    windows: list[int] = list(fcfg["rolling_windows"])
    clip_min: float = float(fcfg["decline_rate_clip_min"])
    clip_max: float = float(fcfg["decline_rate_clip_max"])

    if not input_path.exists():
        raise FileNotFoundError(f"Cleaned Parquet not found: {input_path}")

    client, created = get_or_create_client(config)

    try:
        ddf = dd.read_parquet(str(input_path), engine="pyarrow")
        npartitions = min(ddf.npartitions, 50)
        ddf = ddf.repartition(npartitions=npartitions)

        # F-01: Time features meta
        meta = ddf._meta.copy()
        for col in ("year", "month", "quarter", "days_in_month", "months_since_2020"):
            meta[col] = pd.Series(dtype=pd.Int64Dtype())
        ddf = ddf.map_partitions(_add_time_features, meta=meta)

        # F-02: Rolling features meta
        meta = ddf._meta.copy()
        prod_prefix = [("OilProduced", "oil"), ("GasProduced", "gas"), ("WaterProduced", "water")]
        for w in windows:
            for _, prefix in prod_prefix:
                meta[f"{prefix}_roll{w}_mean"] = pd.Series(dtype=pd.Float64Dtype())
                meta[f"{prefix}_roll{w}_std"] = pd.Series(dtype=pd.Float64Dtype())
        ddf = ddf.map_partitions(_add_rolling_features, windows=windows, meta=meta)

        # F-03: Cumulative features meta
        meta = ddf._meta.copy()
        for col in ("cum_oil", "cum_gas", "cum_water"):
            meta[col] = pd.Series(dtype=pd.Float64Dtype())
        ddf = ddf.map_partitions(_add_cumulative_features, meta=meta)

        # F-04: Decline rates meta
        meta = ddf._meta.copy()
        meta["oil_decline_rate"] = pd.Series(dtype=pd.Float64Dtype())
        meta["gas_decline_rate"] = pd.Series(dtype=pd.Float64Dtype())
        ddf = ddf.map_partitions(
            _add_decline_rates, clip_min=clip_min, clip_max=clip_max, meta=meta
        )

        # F-05: Ratio features meta
        meta = ddf._meta.copy()
        for col in ("gor", "water_cut", "wor"):
            meta[col] = pd.Series(dtype=pd.Float64Dtype())
        ddf = ddf.map_partitions(_add_ratio_features, meta=meta)

        # F-06: Well age meta
        meta = ddf._meta.copy()
        meta["well_age_months"] = pd.Series(dtype=pd.Int64Dtype())
        ddf = ddf.map_partitions(_add_well_age, meta=meta)

        # F-07: Label encoding — fit on full dataset, then apply
        logger.info("Fitting label encoders…")
        mappings = fit_label_encoders(ddf)

        meta = ddf._meta.copy()
        meta["operator_encoded"] = pd.Series(dtype=pd.Int64Dtype())
        meta["formation_encoded"] = pd.Series(dtype=pd.Int64Dtype())
        ddf = ddf.map_partitions(_apply_label_encoding, mappings=mappings, meta=meta)

        # F-08: Save encoder mappings
        processed_dir.mkdir(parents=True, exist_ok=True)
        save_encoder_mappings(mappings, encoder_path)

        npartitions_out = max(1, ddf.npartitions // 10)

        logger.info("Writing feature Parquet to %s (%d partitions)", output_path, npartitions_out)
        try:
            ddf.repartition(npartitions=npartitions_out).to_parquet(
                str(output_path), write_index=True, engine="pyarrow"
            )
        except Exception as exc:
            logger.error("Feature Parquet write error: %s", exc)
            raise RuntimeError(f"Features write failed: {exc}") from exc

        logger.info("Features stage complete. Output: %s", output_path)

    finally:
        if created:
            client.close()
