"""Transform stage: clean, validate, derive columns, and write processed Parquet."""

import logging
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from cogcc_pipeline.utils import get_partition_count, load_config

logger = logging.getLogger(__name__)

_VOLUME_COLS = [
    "OilProduced",
    "OilSales",
    "GasProduced",
    "GasSales",
    "GasUsedOnLease",
    "FlaredVented",
    "WaterProduced",
]
_PRESSURE_COLS = [
    "GasPressureTubing",
    "GasPressureCasing",
    "WaterPressureTubing",
    "WaterPressureCasing",
]
_LEGAL_SUFFIXES = [", LLC", ", INC.", ", INC", ", LP", ", LTD", ", CORP", ", CO.", ", CO"]


# ---------------------------------------------------------------------------
# TRN-01: Derive entity key and production date
# ---------------------------------------------------------------------------


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add well_id (canonical entity key) and production_date columns.

    Pure and stateless — called via map_partitions.
    """
    county = df["ApiCountyCode"].fillna("").str.zfill(2)
    seq = df["ApiSequenceNumber"].fillna("").str.zfill(5)
    sidetrack = df["ApiSidetrack"].fillna("00").str.zfill(2)

    df = df.copy()
    df["well_id"] = county + seq + sidetrack
    df["production_date"] = pd.to_datetime(
        {"year": df["ReportYear"], "month": df["ReportMonth"], "day": 1}
    )
    return df


# ---------------------------------------------------------------------------
# TRN-02: Production volume validation and cleaning
# ---------------------------------------------------------------------------


def _clean_production_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce physical and domain validity rules on production volume columns.

    Pure and stateless — called via map_partitions.
    """
    df = df.copy()

    # Rule 1: Replace negative values with NA (TR-01, TR-05)
    for col in _VOLUME_COLS:
        if col in df.columns:
            df[col] = df[col].where(df[col].isna() | (df[col] >= 0), other=pd.NA)  # type: ignore[call-overload]

    # Rule 2: Oil unit flag (TR-02)
    if "OilProduced" in df.columns:
        df["oil_unit_flag"] = df["OilProduced"].fillna(0) > 50_000

    # Rule 3: Pressure column negative replacement (TR-01)
    for col in _PRESSURE_COLS:
        if col in df.columns:
            df[col] = df[col].where(df[col].isna() | (df[col] >= 0), other=pd.NA)  # type: ignore[call-overload]

    # Rule 4: DaysProduced range validation
    if "DaysProduced" in df.columns:
        dp = df["DaysProduced"]
        df["DaysProduced"] = dp.where(dp.isna() | ((dp >= 0) & (dp <= 31)), other=pd.NA)  # type: ignore[call-overload]

    return df


# ---------------------------------------------------------------------------
# TRN-03: Operator name normalization
# ---------------------------------------------------------------------------


def _normalize_operator_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize OpName values for grouping consistency.

    Pure and stateless — called via map_partitions.
    """
    df = df.copy()

    if "OpName" not in df.columns:
        return df

    s = df["OpName"]

    # Step 1: Strip leading/trailing whitespace
    s = s.str.strip()
    # Step 2: Collapse internal multiple spaces
    s = s.str.replace(r"\s+", " ", regex=True)
    # Step 3: Uppercase
    s = s.str.upper()
    # Step 4: Remove legal suffixes (order matters — longer first to avoid partial matches)
    for suffix in _LEGAL_SUFFIXES:
        s = s.str.replace(suffix.upper() + "$", "", regex=True)
        # Strip trailing comma/space that may remain after suffix removal
        s = s.str.replace(r"[, ]+$", "", regex=True)

    # Step 5: Empty or NA → NA
    s = s.str.strip()
    s = s.where(s.notna() & (s != ""), other=pd.NA)  # type: ignore[call-overload]

    df["OpName"] = s
    return df


# ---------------------------------------------------------------------------
# TRN-04: Deduplication
# ---------------------------------------------------------------------------


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicates and apply well-month revision deduplication.

    Pure and stateless — called via map_partitions.
    Output row count is always <= input row count (TR-15).
    """
    df = df.copy()

    # Rule 1: Exact duplicate removal
    df = df.drop_duplicates()

    # Rule 2: Well-month revision deduplication
    key_cols = ["well_id", "ReportYear", "ReportMonth"]
    # Only apply if all key columns are present
    if all(c in df.columns for c in key_cols):
        # Sort: Revised=True rows last, then by AcceptedDate ascending (NA last)
        has_revised = "Revised" in df.columns
        has_accepted = "AcceptedDate" in df.columns

        sort_cols: list[str] = []
        ascending: list[bool] = []

        if has_revised:
            # Convert Revised to bool-like for sorting (False < True → True rows are kept)
            df["_revised_sort"] = df["Revised"].fillna(False).astype(bool)
            sort_cols.append("_revised_sort")
            ascending.append(True)  # True values come last → kept by drop_duplicates keep="last"

        if has_accepted:
            sort_cols.append("AcceptedDate")
            ascending.append(True)

        if sort_cols:
            df = df.sort_values(sort_cols, ascending=ascending, na_position="first")

        df = df.drop_duplicates(subset=key_cols, keep="last")

        if "_revised_sort" in df.columns:
            df = df.drop(columns=["_revised_sort"])

    return df


# ---------------------------------------------------------------------------
# TRN-05: Well completeness gap detection
# ---------------------------------------------------------------------------


def check_well_completeness(ddf: dd.DataFrame) -> pd.DataFrame:
    """Compute a summary of wells with unexpected gaps in monthly production records.

    Returns a pandas DataFrame — calls .compute() once on aggregated result.
    """

    def _compute_gaps(df: pd.DataFrame) -> pd.DataFrame:
        """Group-level gap computation applied after groupby."""
        result_rows = []
        for well_id, group in df.groupby("well_id"):
            min_date = group["production_date"].min()
            max_date = group["production_date"].max()
            min_yr, min_mo = min_date.year, min_date.month
            max_yr, max_mo = max_date.year, max_date.month
            expected = (max_yr * 12 + max_mo) - (min_yr * 12 + min_mo) + 1
            actual = len(group)
            if actual < expected:
                result_rows.append(
                    {
                        "well_id": well_id,
                        "min_date": min_date,
                        "max_date": max_date,
                        "expected_months": expected,
                        "actual_months": actual,
                        "gap_count": expected - actual,
                    }
                )
        return pd.DataFrame(result_rows)

    # Reset index so well_id is a column
    ddf_reset = ddf.reset_index()

    # Compute the aggregation by partition, then combine
    # Use a simple approach: aggregate min/max/count per well via Dask groupby
    agg = (
        ddf_reset.groupby("well_id")["production_date"]
        .agg(["min", "max", "count"])
        .compute()
        .reset_index()
    )
    agg.columns = ["well_id", "min_date", "max_date", "actual_months"]

    def _expected(row: pd.Series) -> int:
        return (
            (row["max_date"].year * 12 + row["max_date"].month)
            - (row["min_date"].year * 12 + row["min_date"].month)
            + 1
        )

    agg["expected_months"] = agg.apply(_expected, axis=1)
    agg["gap_count"] = agg["expected_months"] - agg["actual_months"]

    gaps = agg[agg["gap_count"] > 0].sort_values("gap_count", ascending=False)
    return gaps[
        ["well_id", "min_date", "max_date", "expected_months", "actual_months", "gap_count"]
    ]


# ---------------------------------------------------------------------------
# TRN-06: Transform pipeline assembler
# ---------------------------------------------------------------------------


def run_transform(config_path: str = "config/config.yaml") -> None:
    """Top-level entry point for the transform stage.

    Reads interim Parquet, applies all cleaning/validation/derivation steps,
    and writes processed Parquet to data/processed/.
    """
    config = load_config(config_path)
    trn_cfg = config["transform"]

    interim_dir = Path(trn_cfg["interim_dir"])
    processed_dir = Path(trn_cfg["processed_dir"])

    parquet_files = list(interim_dir.glob("*.parquet"))
    if not parquet_files:
        logger.warning("No Parquet files found in %s — transform skipped", interim_dir)
        return

    n = len(parquet_files)

    try:
        ddf = dd.read_parquet(str(interim_dir))

        # Step 3: Add derived columns
        meta_derived = _add_derived_columns(ddf._meta.copy())
        ddf = ddf.map_partitions(_add_derived_columns, meta=meta_derived)

        # Step 4: Clean production volumes
        meta_cleaned = _clean_production_volumes(ddf._meta.copy())
        ddf = ddf.map_partitions(_clean_production_volumes, meta=meta_cleaned)

        # Step 5: Normalize operator names
        meta_normalized = _normalize_operator_names(ddf._meta.copy())
        ddf = ddf.map_partitions(_normalize_operator_names, meta=meta_normalized)

        # Step 6: Deduplicate
        meta_deduped = _deduplicate(ddf._meta.copy())
        ddf = ddf.map_partitions(_deduplicate, meta=meta_deduped)

        # Step 7: Set index on well_id
        ddf = ddf.set_index("well_id", sorted=False, drop=True)

        # Step 8: Sort by production_date within partitions
        ddf = ddf.map_partitions(lambda df: df.sort_values("production_date"))

        # Step 11: Well completeness diagnostic (before repartition)
        try:
            gaps_df = check_well_completeness(ddf)
            if gaps_df.empty:
                logger.info("Well completeness check: no gaps found")
            else:
                top10 = gaps_df.head(10)
                logger.warning("Well completeness gaps found:\n%s", top10.to_string())
        except Exception as exc:
            logger.warning("Well completeness check failed (non-blocking): %s", exc)

        # Step 9: Repartition
        ddf = ddf.repartition(npartitions=get_partition_count(n))

        # Step 10: Write Parquet
        processed_dir.mkdir(parents=True, exist_ok=True)
        ddf.to_parquet(str(processed_dir), write_index=True, overwrite=True)

        logger.info("Transform complete: %d input partitions → %s", n, processed_dir)

    except Exception as exc:
        logger.error("Transform stage failed: %s", exc)
        raise
