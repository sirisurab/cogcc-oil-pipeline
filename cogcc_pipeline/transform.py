"""Transform stage: clean, deduplicate, construct derived columns, write processed Parquet."""

from __future__ import annotations

import logging

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

WELL_STATUS_CATEGORIES = ["AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"]
WELL_STATUS_DTYPE = pd.CategoricalDtype(categories=WELL_STATUS_CATEGORIES, ordered=False)

OIL_OUTLIER_THRESHOLD = 50_000.0  # BBL — single-well monthly upper limit


class TransformError(RuntimeError):
    """Raised on unrecoverable transform failures."""


def _build_well_id(df: pd.DataFrame) -> pd.Series:
    """Build composite well_id: '05-{ApiCountyCode}-{ApiSequenceNumber}-{ApiSidetrack}'.

    ApiSidetrack is zero-padded to 2 digits; null sidetrack → '00'.

    Returns:
        pd.Series of dtype pd.StringDtype().
    """
    county = df["ApiCountyCode"].fillna("")
    seq = df["ApiSequenceNumber"].fillna("")
    sidetrack = df["ApiSidetrack"].str.zfill(2).where(df["ApiSidetrack"].notna(), other="00")
    result = "05-" + county + "-" + seq + "-" + sidetrack
    return result.astype(pd.StringDtype())


def _build_production_date(df: pd.DataFrame) -> pd.Series:
    """Build production_date (datetime64[ns]) from ReportYear and ReportMonth.

    Invalid year/month combinations produce NaT.
    """
    result = pd.to_datetime(
        {"year": df["ReportYear"], "month": df["ReportMonth"], "day": 1},
        errors="coerce",
    )
    return result


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows, keeping the one with the most recent AcceptedDate.

    Deduplication key: ApiCountyCode, ApiSequenceNumber, ReportYear, ReportMonth.
    Idempotent: calling twice produces the same result.
    """
    dup_cols = ["ApiCountyCode", "ApiSequenceNumber", "ReportYear", "ReportMonth"]
    if "AcceptedDate" in df.columns:
        df = df.sort_values("AcceptedDate", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=dup_cols, keep="first")
    return df


def _clean_production_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """Clean OilProduced, GasProduced, WaterProduced columns.

    Rules:
    - Negative → pd.NA (TR-01)
    - Zero preserved as 0.0 (TR-05)
    - OilProduced > 50,000 BBL → pd.NA + WARNING (TR-02)
    - DaysProduced=0 with non-zero volumes → WARNING only
    """
    df = df.copy()
    vol_cols = ["OilProduced", "GasProduced", "WaterProduced"]

    for col in vol_cols:
        if col not in df.columns:
            continue
        neg_mask = df[col].notna() & (df[col] < 0)
        if neg_mask.any():
            df.loc[neg_mask, col] = pd.NA

    # Oil outlier check
    if "OilProduced" in df.columns:
        outlier_mask = df["OilProduced"].notna() & (df["OilProduced"] > OIL_OUTLIER_THRESHOLD)
        if outlier_mask.any():
            n_outliers = int(outlier_mask.sum())
            bad_vals = df.loc[outlier_mask, "OilProduced"].tolist()
            logger.warning(
                "OilProduced outlier: %d rows exceed %s BBL (values: %s) — replacing with NA",
                n_outliers,
                OIL_OUTLIER_THRESHOLD,
                bad_vals[:5],
            )
            df.loc[outlier_mask, "OilProduced"] = pd.NA

    # Days consistency check (warning only)
    if "DaysProduced" in df.columns:
        days_zero = df["DaysProduced"].notna() & (df["DaysProduced"] == 0)
        for col in vol_cols:
            if col not in df.columns:
                continue
            nonzero_prod = df[col].notna() & (df[col] != 0)
            flag = days_zero & nonzero_prod
            if flag.any():
                logger.warning(
                    "DaysProduced=0 but %s is non-zero for %d rows (data quality flag)",
                    col,
                    flag.sum(),
                )

    return df


def _cast_well_status(df: pd.DataFrame) -> pd.DataFrame:
    """Cast WellStatus to CategoricalDtype; unknown values → pd.NA."""
    df = df.copy()
    if "WellStatus" not in df.columns:
        return df
    valid_mask = df["WellStatus"].isin(WELL_STATUS_CATEGORIES)
    df["WellStatus"] = df["WellStatus"].where(valid_mask, other=np.nan)
    df["WellStatus"] = df["WellStatus"].astype(WELL_STATUS_DTYPE)
    return df


def _transform_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Apply full per-partition transformation chain.

    Order: deduplicate → clean volumes → build well_id → build production_date →
    cast WellStatus → drop ReportYear/ReportMonth.
    """
    df = _deduplicate(df)
    df = _clean_production_volumes(df)
    df["well_id"] = _build_well_id(df)
    # Insert well_id as first column
    cols = ["well_id"] + [c for c in df.columns if c != "well_id"]
    df = df[cols]
    df["production_date"] = _build_production_date(df)
    # Insert production_date after well_id
    remaining = [c for c in df.columns if c not in ("well_id", "production_date")]
    df = df[["well_id", "production_date"] + remaining]
    df = _cast_well_status(df)
    df = df.drop(columns=["ReportYear", "ReportMonth"], errors="ignore")
    return df


def check_well_completeness(ddf: dd.DataFrame) -> pd.DataFrame:
    """Check for gaps in each well's monthly production record.

    Returns:
        Summary DataFrame with gap analysis per well.
    """
    summary = ddf.groupby("well_id")["production_date"].agg(["min", "max", "count"]).compute()
    summary.columns = ["first_date", "last_date", "actual_months"]

    def _expected(row: pd.Series) -> int:
        if pd.isna(row["first_date"]) or pd.isna(row["last_date"]):
            return 0
        first = row["first_date"].to_period("M")
        last = row["last_date"].to_period("M")
        return int((last - first).n) + 1

    summary["expected_months"] = summary.apply(_expected, axis=1)
    summary["gap_count"] = summary["expected_months"] - summary["actual_months"]

    wells_with_gaps = summary[summary["gap_count"] > 0]
    for well_id, row in wells_with_gaps.iterrows():
        logger.warning(
            "Well %s has %d production gap(s) between %s and %s",
            well_id,
            row["gap_count"],
            row["first_date"],
            row["last_date"],
        )
    logger.info(
        "Well completeness: %d wells checked, %d with gaps", len(summary), len(wells_with_gaps)
    )
    return summary


def check_data_integrity(
    interim_ddf: dd.DataFrame,
    processed_ddf: dd.DataFrame,
    sample_n: int = 20,
) -> None:
    """Spot-check production volumes from interim to processed for a random sample of wells.

    Batches both compute calls via dask.compute().
    """
    well_ids_series = (
        processed_ddf["well_id"]
        if "well_id" in processed_ddf.columns
        else processed_ddf.index.to_series()
    )
    (well_ids_arr,) = dask.compute(well_ids_series.unique())
    well_ids = list(well_ids_arr)

    import random

    sample = random.sample(well_ids, min(sample_n, len(well_ids)))

    discrepancies = 0
    for well_id in sample:
        if "well_id" in interim_ddf.columns:
            interim_mask = interim_ddf["well_id"] == well_id
        else:
            interim_mask = interim_ddf.index == well_id

        if "well_id" in processed_ddf.columns:
            processed_mask = processed_ddf["well_id"] == well_id
        else:
            processed_mask = processed_ddf.index == well_id

        interim_sum_delayed = interim_ddf[interim_mask]["OilProduced"].sum()
        processed_sum_delayed = processed_ddf[processed_mask]["OilProduced"].sum()

        interim_sum, processed_sum = dask.compute(interim_sum_delayed, processed_sum_delayed)

        diff = abs(float(interim_sum or 0) - float(processed_sum or 0))
        if diff > 1e-3:
            discrepancies += 1
            logger.warning(
                "OilProduced discrepancy for well %s: interim=%.4f processed=%.4f diff=%.6f",
                well_id,
                interim_sum,
                processed_sum,
                diff,
            )

    logger.info("Integrity check: %d wells checked, %d discrepancies", len(sample), discrepancies)


def run_transform(config: dict) -> dd.DataFrame:
    """Orchestrate the full transform stage.

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

    interim_path = config["transform"]["interim_dir"] + "/production.parquet"
    ddf = dd.read_parquet(interim_path, engine="pyarrow")

    # Keep reference to interim for integrity check
    interim_ddf = ddf

    # Derive meta by calling the transform on an empty copy of the schema
    meta = _transform_partition(ddf._meta.copy())
    ddf = ddf.map_partitions(_transform_partition, meta=meta)

    # Filter rows where well_id is null or empty using map_partitions
    def _filter_empty_well_id(df: pd.DataFrame) -> pd.DataFrame:
        return df[df["well_id"].notna() & (df["well_id"] != "")]

    filter_meta = meta[meta["well_id"].notna() & (meta["well_id"] != "")]
    ddf = ddf.map_partitions(_filter_empty_well_id, meta=filter_meta)

    # Diagnostic completeness check (does not block pipeline)
    try:
        check_well_completeness(ddf)
    except Exception as exc:
        logger.warning("Well completeness check failed (non-blocking): %s", exc)

    # Final sequence: set_index → repartition → sort within partitions
    ddf = ddf.set_index("well_id")
    ddf = ddf.repartition(npartitions=max(10, min(ddf.npartitions, 50)))

    sort_meta = ddf._meta.copy().sort_values("production_date")
    ddf = ddf.map_partitions(lambda df: df.sort_values("production_date"), meta=sort_meta)

    processed_path = config["transform"]["processed_dir"] + "/production.parquet"
    ddf.to_parquet(processed_path, engine="pyarrow", write_index=True, overwrite=True)
    logger.info("Wrote processed Parquet to %s", processed_path)

    # Integrity check (audit step — read back processed)
    try:
        processed_ddf = dd.read_parquet(processed_path, engine="pyarrow")
        check_data_integrity(interim_ddf, processed_ddf)
    except Exception as exc:
        logger.warning("Data integrity check failed (non-blocking): %s", exc)

    return ddf
