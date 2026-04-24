"""Transform stage — clean, sort, index, and write processed Parquet."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import numpy as np
import pandas as pd
from distributed import Client

logger = logging.getLogger(__name__)

_WELL_STATUS_CATEGORIES = ["AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"]

# Canonical column order for output (TR-14, TR-23: stable schema across partitions)
_CANONICAL_COL_ORDER = [
    "ApiCountyCode",
    "ApiSequenceNumber",
    "ApiSidetrack",
    "ReportYear",
    "ReportMonth",
    "OilProduced",
    "GasProduced",
    "WaterProduced",
    "OilSales",
    "GasSales",
    "WaterPressureTubing",
    "WaterPressureCasing",
    "GasPressureTubing",
    "GasPressureCasing",
    "WellStatus",
    "OpName",
    "Well",
    "DaysProduced",
    "DocNum",
    "OpNumber",
    "FacilityId",
    "AcceptedDate",
    "Revised",
    "OilAdjustment",
    "OilGravity",
    "GasBtuSales",
    "GasUsedOnLease",
    "GasShrinkage",
    "FlaredVented",
    "BomInvent",
    "EomInvent",
    "FormationCode",
]

# Output dtypes for canonical columns added when missing (TR-14, TR-23)
_CANONICAL_COL_DTYPES: dict[str, str] = {
    "ApiCountyCode": "string",
    "ApiSequenceNumber": "string",
    "ApiSidetrack": "string",
    "ReportYear": "float64",
    "ReportMonth": "float64",
    "OilProduced": "float64",
    "GasProduced": "float64",
    "WaterProduced": "float64",
    "OilSales": "float64",
    "GasSales": "float64",
    "WaterPressureTubing": "float64",
    "WaterPressureCasing": "float64",
    "GasPressureTubing": "float64",
    "GasPressureCasing": "float64",
    "WellStatus": "string",
    "OpName": "string",
    "Well": "string",
    "DaysProduced": "float64",
    "DocNum": "float64",
    "OpNumber": "float64",
    "FacilityId": "float64",
    "AcceptedDate": "datetime64[us]",
    "Revised": "bool",
    "OilAdjustment": "float64",
    "OilGravity": "float64",
    "GasBtuSales": "float64",
    "GasUsedOnLease": "float64",
    "GasShrinkage": "float64",
    "FlaredVented": "float64",
    "BomInvent": "float64",
    "EomInvent": "float64",
    "FormationCode": "string",
    "well_id": "string",
    "production_date": "datetime64[us]",
}


# ---------------------------------------------------------------------------
# Task T1 — Partition-level cleaning
# ---------------------------------------------------------------------------


def clean_partition(pdf: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Apply all partition-level cleaning rules.

    Input carries canonical schema and data-dictionary dtypes
    (boundary-ingest-transform downstream reliance). This function does not
    re-establish those.

    Returns a DataFrame that satisfies the boundary-transform-features contract:
    entity_col present, production_date present, categoricals clean.

    Args:
        pdf: Single partition from interim Parquet.
        config: The ``transform`` section of config.yaml.

    Returns:
        Cleaned partition.
    """
    if pdf.empty:
        return _empty_clean_partition(pdf, config)

    entity_col: str = config.get("entity_col", "well_id")
    date_col: str = config.get("date_col", "production_date")
    api_county: str = config.get("api_county_col", "ApiCountyCode")
    api_seq: str = config.get("api_sequence_col", "ApiSequenceNumber")
    api_sidetrack: str = config.get("api_sidetrack_col", "ApiSidetrack")
    min_year: int = int(config.get("min_report_year", 2020))
    current_year: int = datetime.now().year

    df = pdf.copy()

    # --- Build entity column (well identifier) from API components ---
    def _str_col(col: str) -> pd.Series:
        if col in df.columns:
            return df[col].astype(str).str.strip().replace("nan", pd.NA).replace("<NA>", pd.NA)
        return pd.Series([""] * len(df), dtype="string")

    county = _str_col(api_county)
    seq = _str_col(api_seq)
    sidetrack = _str_col(api_sidetrack)

    entity_valid = county.notna() & seq.notna() & sidetrack.notna()
    df[entity_col] = (county + "-" + seq + "-" + sidetrack).where(entity_valid, other=None)  # type: ignore[call-overload]

    # --- Build production_date from ReportYear + ReportMonth ---
    if "ReportYear" in df.columns and "ReportMonth" in df.columns:
        year_s = pd.to_numeric(df["ReportYear"], errors="coerce")
        month_s = pd.to_numeric(df["ReportMonth"], errors="coerce")
        valid_date = year_s.notna() & month_s.notna()
        df[date_col] = pd.NaT
        if valid_date.any():
            df.loc[valid_date, date_col] = pd.to_datetime(
                year_s[valid_date].astype(int).astype(str)
                + "-"
                + month_s[valid_date].astype(int).astype(str).str.zfill(2)
                + "-01",
                format="%Y-%m-%d",
                errors="coerce",
            )
    else:
        df[date_col] = pd.NaT

    # --- Drop rows missing entity or production_date ---
    before_null_drop = len(df)
    df = df[df[entity_col].notna() & df[date_col].notna()]
    logger.debug("Dropped %d rows with null entity or date", before_null_drop - len(df))

    if df.empty:
        return _empty_clean_partition(pdf, config)

    # --- Re-assert year filter (cheap, idempotent) ---
    if "ReportYear" in df.columns:
        year_s = pd.to_numeric(df["ReportYear"], errors="coerce")
        df = df[year_s.between(min_year, current_year, inclusive="both")]

    if df.empty:
        return _empty_clean_partition(pdf, config)

    # --- Deduplicate on (entity, production_date) — keep first (TR-15) ---
    before_dedup = len(df)
    df = df.drop_duplicates(subset=[entity_col, date_col], keep="first")
    logger.debug("Dropped %d duplicate rows", before_dedup - len(df))

    # --- Physical bound enforcement (TR-01, TR-05) ---
    # Clip negative volumes to null sentinel (sensor error).
    # Zeros are preserved (TR-05) — only strictly negative values are replaced.
    physical_bounds: dict = config.get("physical_bounds", {})
    volume_cols = [
        "OilProduced",
        "GasProduced",
        "WaterProduced",
        "OilSales",
        "GasSales",
        "WaterPressureTubing",
        "WaterPressureCasing",
        "GasPressureTubing",
        "GasPressureCasing",
    ]
    for col in volume_cols:
        if col in df.columns:
            bounds = physical_bounds.get(col, {"min": 0.0, "max": None})
            col_min = bounds.get("min")
            if col_min is not None:
                # Replace strictly-below-min with NA (preserves zeros when min=0)
                numeric_col = pd.to_numeric(df[col], errors="coerce")
                below_bound = numeric_col < col_min
                df[col] = numeric_col.where(~below_bound, other=None)  # type: ignore[call-overload]

    # --- Unit-consistency outlier detection (TR-02) ---
    unit_cfg: dict = config.get("unit_consistency", {})
    oil_max: float = float(unit_cfg.get("OilProduced_max_bbl_month", 50000.0))
    if "OilProduced" in df.columns:
        oil_series = pd.to_numeric(df["OilProduced"], errors="coerce")
        df["OilProduced"] = oil_series.where(oil_series <= oil_max, other=None)  # type: ignore[call-overload]

    # --- Normalize OpName (non-null → stripped uppercase) ---
    if "OpName" in df.columns:
        mask_notnull = df["OpName"].notna()
        df.loc[mask_notnull, "OpName"] = (
            df.loc[mask_notnull, "OpName"].astype(str).str.strip().str.upper()
        )

    # --- Cast WellStatus categorical (H2 — only declared values) ---
    if "WellStatus" in df.columns:
        mask_invalid = ~df["WellStatus"].isin(_WELL_STATUS_CATEGORIES) & df["WellStatus"].notna()
        df.loc[mask_invalid, "WellStatus"] = pd.NA
        df["WellStatus"] = df["WellStatus"].astype(
            pd.CategoricalDtype(categories=_WELL_STATUS_CATEGORIES, ordered=False)
        )

    # Ensure consistent column order across all partitions (TR-14, TR-23)
    # Add all canonical columns to dataframe, filling missing with NA
    for col in _CANONICAL_COL_ORDER:
        if col not in df.columns:
            target_dtype = _CANONICAL_COL_DTYPES.get(col, "object")
            na_val: Any
            if target_dtype.startswith("float"):
                na_val = float("nan")
            elif target_dtype.startswith("datetime"):
                na_val = pd.NaT
            elif target_dtype == "bool":
                na_val = False
            else:
                na_val = pd.NA
            df[col] = pd.Series([na_val] * len(df), dtype=target_dtype)
    # Use canonical order, then append any generated or extra columns
    final_cols = list(_CANONICAL_COL_ORDER)
    final_cols += [entity_col]
    final_cols += [date_col]
    # Remove duplicates while preserving order
    final_cols = list(dict.fromkeys(final_cols))
    result = df[final_cols].reset_index(drop=True)

    # Guarantee WellStatus categorical dtype on all return paths (TR-14, TR-23)
    if "WellStatus" in result.columns:
        result["WellStatus"] = result["WellStatus"].astype(
            pd.CategoricalDtype(categories=_WELL_STATUS_CATEGORIES, ordered=False)
        )

    # Normalize string dtypes to pd.StringDtype() for consistent meta (TR-23)
    for col in result.columns:
        if pd.api.types.is_string_dtype(result[col]) and result[col].dtype != pd.StringDtype():
            try:
                result[col] = result[col].astype(pd.StringDtype())
            except Exception:
                pass

    # Normalize nullable dtypes to numpy equivalents for consistent meta (TR-23)
    for col in result.columns:
        if hasattr(result[col].dtype, "numpy_dtype"):
            numpy_target = result[col].dtype.numpy_dtype  # type: ignore[union-attr]
            # Normalize integers to float64 to allow NaN representation
            if np.issubdtype(numpy_target, np.integer):
                numpy_target = np.dtype("float64")
            try:
                result[col] = result[col].astype(numpy_target)
            except Exception:
                pass

    return result


def _empty_clean_partition(pdf: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Return a zero-row output frame with the correct output schema."""
    entity_col: str = config.get("entity_col", "well_id")
    date_col: str = config.get("date_col", "production_date")

    df = pdf.iloc[:0].copy()
    if entity_col not in df.columns:
        df[entity_col] = pd.Series([], dtype="string")
    else:
        df[entity_col] = pd.Series([], dtype="string")
    if date_col not in df.columns:
        df[date_col] = pd.Series([], dtype="datetime64[ns]")
    else:
        df[date_col] = pd.Series([], dtype="datetime64[ns]")
    if "WellStatus" in df.columns:
        df["WellStatus"] = pd.Series(
            [], dtype=pd.CategoricalDtype(categories=_WELL_STATUS_CATEGORIES, ordered=False)
        )
    # Convert any remaining object dtype columns to string to avoid pyarrow serialization issues
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype("string")
    # Ensure consistent column order across all partitions (TR-14, TR-23)
    # Add all canonical columns to dataframe, filling missing with NA
    for col in _CANONICAL_COL_ORDER:
        if col not in df.columns:
            target_dtype = _CANONICAL_COL_DTYPES.get(col, "object")
            df[col] = pd.Series([], dtype=target_dtype)
    # Use canonical order, then append any generated or extra columns
    final_cols = list(_CANONICAL_COL_ORDER)
    final_cols += [entity_col]
    final_cols += [date_col]
    # Remove duplicates while preserving order
    final_cols = list(dict.fromkeys(final_cols))
    result = df[final_cols].reset_index(drop=True)

    # Normalize string dtypes to pd.StringDtype() for consistent meta (TR-23)
    for col in result.columns:
        if pd.api.types.is_string_dtype(result[col]) and result[col].dtype != pd.StringDtype():
            try:
                result[col] = result[col].astype(pd.StringDtype())
            except Exception:
                pass

    # Normalize nullable dtypes to numpy equivalents for consistent meta (TR-23)
    for col in result.columns:
        if hasattr(result[col].dtype, "numpy_dtype"):
            numpy_target = result[col].dtype.numpy_dtype  # type: ignore[union-attr]
            # Normalize integers to float64 to allow NaN representation
            if np.issubdtype(numpy_target, np.integer):
                numpy_target = np.dtype("float64")
            try:
                result[col] = result[col].astype(numpy_target)
            except Exception:
                pass

    return result


# ---------------------------------------------------------------------------
# Task T2 — Stage orchestration: clean, sort, index, partition, write
# ---------------------------------------------------------------------------


def _partition_count(n: int, cfg_min: int = 10, cfg_max: int = 50) -> int:
    return max(cfg_min, min(n, cfg_max))


def transform(config: dict) -> None:
    """Stage entry point. Read interim Parquet, clean, sort, index, write.

    Sort by production_date is applied per-partition AFTER set_index since
    set_index's shuffle destroys prior ordering (H1). set_index is the last
    structural operation before repartition and write (H4).

    Meta derivation: call clean_partition on zero-row real input (ADR-003).

    Args:
        config: Full pipeline config dict (uses ``transform`` sub-section).
    """
    transform_cfg: dict = config.get("transform", config)
    interim_dir = Path(transform_cfg.get("interim_dir", "data/interim"))
    processed_dir = Path(transform_cfg.get("processed_dir", "data/processed"))
    entity_col: str = transform_cfg.get("entity_col", "well_id")
    date_col: str = transform_cfg.get("date_col", "production_date")
    p_min: int = int(transform_cfg.get("partition_min", 10))
    p_max: int = int(transform_cfg.get("partition_max", 50))

    processed_dir.mkdir(parents=True, exist_ok=True)

    # Reuse existing distributed client (build-env-manifest)
    try:
        Client.current()
    except ValueError:
        pass  # no client; Dask uses default scheduler

    ddf = dd.read_parquet(str(interim_dir), engine="pyarrow")
    logger.info("Loaded interim Parquet: %d partitions", ddf.npartitions)

    # Meta derivation per ADR-003: call actual function on zero-row real input
    meta_sample = ddf._meta.copy()
    meta_df = clean_partition(meta_sample, transform_cfg)

    ddf_clean = ddf.map_partitions(clean_partition, transform_cfg, meta=meta_df)

    # Resolve H1 + H4:
    # - set_index on entity_col triggers distributed shuffle (destroys sort)
    # - After set_index, map_partitions to sort within each partition by date
    # - repartition is last op before write (ADR-004 H3)
    #
    # To satisfy boundary-transform-features (sorted by production_date within
    # each partition at stage exit), we sort AFTER set_index via map_partitions.

    # set_index on entity_col — last structural operation (H4)
    ddf_indexed = ddf_clean.set_index(entity_col, drop=True, sorted=False)

    # Sort within each partition by date AFTER set_index (resolves H1)
    def _sort_partition(part: pd.DataFrame) -> pd.DataFrame:
        if date_col in part.columns and not part.empty:
            return part.sort_values(date_col, kind="stable")
        return part

    sort_meta = ddf_indexed._meta.copy()
    ddf_sorted = ddf_indexed.map_partitions(_sort_partition, meta=sort_meta)

    n_parts = _partition_count(ddf_sorted.npartitions, p_min, p_max)
    ddf_out = ddf_sorted.repartition(npartitions=n_parts)

    # Convert WellStatus from categorical to string to ensure consistent dtypes across partitions (TR-14, TR-23)
    if "WellStatus" in ddf_out.columns:
        ddf_out = ddf_out.assign(WellStatus=ddf_out["WellStatus"].astype("string"))

    logger.info("Writing processed Parquet to %s (%d partitions)", processed_dir, n_parts)
    # Single compute at write (ADR-005)
    ddf_out.to_parquet(
        str(processed_dir),
        engine="pyarrow",
        write_index=True,
        overwrite=True,
    )
    logger.info("Transform complete — processed Parquet written")
