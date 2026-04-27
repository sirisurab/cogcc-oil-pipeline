"""Transform stage: clean, deduplicate, cast, validate, index, and sort."""

import logging
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import yaml

from cogcc_pipeline.ingest import load_data_dictionary

logger = logging.getLogger(__name__)

REQUIRED_TRANSFORM_KEYS = ["interim_dir", "processed_dir", "entity_col"]

# Volume columns that must be >= 0
_VOLUME_COLS = [
    "OilProduced",
    "OilSales",
    "OilAdjustment",
    "GasProduced",
    "GasSales",
    "GasUsedOnLease",
    "GasShrinkage",
    "WaterProduced",
    "FlaredVented",
    "BomInvent",
    "EomInvent",
    "GasBtuSales",
]
_PRESSURE_COLS = [
    "GasPressureTubing",
    "GasPressureCasing",
    "WaterPressureTubing",
    "WaterPressureCasing",
]
_OIL_UNIT_THRESHOLD = 50_000.0


def load_config(config_path: str) -> dict:
    """Read config.yaml and return the full config dict."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    transform_cfg: dict = cfg.get("transform", {})
    for key in REQUIRED_TRANSFORM_KEYS:
        if key not in transform_cfg:
            raise KeyError(f"Missing required transform config key: '{key}'")
    return cfg


def _add_production_date(partition: pd.DataFrame) -> pd.DataFrame:
    """Add production_date column (first of reported month) to a partition."""
    result = partition.copy()
    year = pd.to_numeric(result["ReportYear"], errors="coerce")
    month = pd.to_numeric(result["ReportMonth"], errors="coerce")
    result["production_date"] = pd.to_datetime(
        dict(year=year, month=month, day=1), errors="coerce"
    ).astype("datetime64[ns]")
    return result


def _cast_categoricals(partition: pd.DataFrame, data_dict: dict) -> pd.DataFrame:
    """Cast categorical columns to declared CategoricalDtype, replacing unknown values."""
    result = partition.copy()
    for col, spec in data_dict.items():
        if col not in result.columns:
            continue
        dtype = spec["dtype"]
        if not isinstance(dtype, pd.CategoricalDtype):
            continue
        allowed = set(dtype.categories)
        # Replace values outside declared set with pd.NA before casting (ADR-003, H2)
        result[col] = result[col].mask(~result[col].isin(allowed))
        result[col] = result[col].astype(dtype)
    return result


def _validate_numeric_bounds(partition: pd.DataFrame) -> pd.DataFrame:
    """Enforce physical bounds on volume and pressure columns (TR-01, TR-02, TR-05)."""
    result = partition.copy()

    for col in _VOLUME_COLS:
        if col in result.columns:
            result[col] = result[col].where(result[col] >= 0, other=np.nan)

    for col in _PRESSURE_COLS:
        if col in result.columns:
            result[col] = result[col].where(result[col] >= 0, other=np.nan)

    # DaysProduced: must be >= 0 and <= 31
    if "DaysProduced" in result.columns:
        days = result["DaysProduced"]
        # Keep as Int64 with pd.NA for out-of-range (nullable int)
        invalid = (days < 0) | (days > 31)
        result["DaysProduced"] = days.mask(invalid)

    # TR-02: flag likely unit errors for OilProduced > 50,000 BBL/month
    if "OilProduced" in result.columns:
        flagged = result["OilProduced"] > _OIL_UNIT_THRESHOLD
        count = int(flagged.sum())
        if count > 0:
            logger.warning(
                "OilProduced > %s BBL/month (likely unit error) in %d row(s) — replacing with NaN",
                _OIL_UNIT_THRESHOLD,
                count,
            )
            result["OilProduced"] = result["OilProduced"].where(~flagged, other=np.nan)

    return result


def _deduplicate(partition: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate (ApiSequenceNumber, production_date) rows.

    Tie-break: sort by AcceptedDate descending, keep first (most recent revision).
    Keyed subset required — bare drop_duplicates() prohibited (H5).
    """
    if partition.empty:
        return partition

    sort_cols = ["AcceptedDate"]
    ascending = [False]

    if "AcceptedDate" in partition.columns:
        result = partition.sort_values(
            sort_cols, ascending=ascending, na_position="last"
        )
    else:
        result = partition

    result = result.drop_duplicates(
        subset=["ApiSequenceNumber", "production_date"], keep="first"
    )
    return result


def _transform_partition(partition: pd.DataFrame, data_dict: dict) -> pd.DataFrame:
    """Apply all cleaning and enrichment steps to a single partition."""
    result = _add_production_date(partition)
    result = _validate_numeric_bounds(result)
    result = _cast_categoricals(result, data_dict)
    result = _deduplicate(result)
    return result


def check_well_completeness(ddf: dd.DataFrame) -> pd.DataFrame:
    """Compute per-well completeness summary (TR-04). Returns a pandas DataFrame."""
    # ApiSequenceNumber may be the index (after set_index in write_parquet) or a column
    if ddf.index.name == "ApiSequenceNumber":
        group_col: str = "ApiSequenceNumber"
        # Reset index so we can groupby as a column
        work = ddf.reset_index()
    else:
        group_col = "ApiSequenceNumber"
        work = ddf

    summary = (
        work.groupby(group_col)["production_date"]
        .agg(["min", "max", "count"])
        .compute()
    )
    summary.columns = ["first_date", "last_date", "actual_count"]

    def expected_months(row: pd.Series) -> int:
        first = row["first_date"]
        last = row["last_date"]
        if pd.isnull(first) or pd.isnull(last):
            return 0
        first_ts = pd.Timestamp(first)
        last_ts = pd.Timestamp(last)
        return (
            (last_ts.year - first_ts.year) * 12 + (last_ts.month - first_ts.month) + 1
        )

    summary["expected_count"] = summary.apply(expected_months, axis=1)
    return summary.reset_index()


def write_parquet(ddf: dd.DataFrame, output_dir: str) -> None:
    """Set entity index, sort by production_date, repartition, write Parquet (ADR-004).

    Operation order (H1, H3, H4, ADR-004):
      1. set_index on entity column (last structural op before repartition/write)
      2. map_partitions sort by production_date (after shuffle destroys row order)
      3. repartition (last operation before write)
      4. to_parquet
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    n = ddf.npartitions
    n_out = max(10, min(n, 50))

    indexed = ddf.set_index("ApiSequenceNumber", drop=True, sort=False)
    # Sort by production_date within each partition after the shuffle (H1)
    sorted_ddf = indexed.map_partitions(
        lambda part: part.sort_values("production_date")
    )
    # Repartition is the last operation before write (ADR-004)
    sorted_ddf.repartition(npartitions=n_out).to_parquet(str(out))


def transform(config: dict) -> None:
    """Orchestrate the full transform stage."""
    transform_cfg: dict = config.get("transform", config)
    interim_dir = str(transform_cfg["interim_dir"])
    processed_dir = str(transform_cfg["processed_dir"])
    dd_path = str(transform_cfg["data_dictionary"])

    interim_path = Path(interim_dir)
    parquet_files = list(interim_path.glob("*.parquet")) + list(
        interim_path.glob("**/*.parquet")
    )
    if not parquet_files:
        logger.warning("No Parquet files found in %s — skipping transform", interim_dir)
        return

    data_dict = load_data_dictionary(dd_path)

    ddf = dd.read_parquet(interim_dir)

    # ADR-003: derive meta by calling actual function on minimal real input
    sample = ddf.get_partition(0).compute().iloc[:1]
    meta = _transform_partition(sample, data_dict).iloc[0:0]

    transformed = ddf.map_partitions(_transform_partition, data_dict, meta=meta)

    # Task graph is lazy until write (ADR-005)
    write_parquet(transformed, processed_dir)
    logger.info("Transform complete — wrote processed Parquet to %s", processed_dir)

    # Completeness diagnostic — separate compute after main write (ADR-005 note)
    try:
        processed_ddf = dd.read_parquet(processed_dir)
        completeness = check_well_completeness(processed_ddf)
        gaps = completeness[
            completeness["actual_count"] < completeness["expected_count"]
        ]
        logger.info(
            "Well completeness: %d wells checked, %d with gaps",
            len(completeness),
            len(gaps),
        )
    except Exception as exc:
        logger.warning("Completeness check failed: %s", exc)
