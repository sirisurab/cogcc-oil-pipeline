"""Transform stage: clean and validate the interim Parquet dataset."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import numpy as np
import pandas as pd

SENTINEL_VALUES: list[float | int] = [-9999, -999, 9999, 9999.0, -9999.0]

NUMERIC_COLS: list[str] = [
    "OilProduced",
    "OilSold",
    "GasProduced",
    "GasSold",
    "GasFlared",
    "GasUsed",
    "WaterProduced",
    "DaysProduced",
    "ReportYear",
    "ReportMonth",
]


def _replace_sentinels_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Replace sentinel values in numeric columns with pd.NA (partition-level)."""
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].where(~df[col].isin(SENTINEL_VALUES), other=np.nan)
    return df


def replace_sentinels(ddf: dd.DataFrame, logger: logging.Logger) -> dd.DataFrame:
    """Replace known ECMC sentinel values in numeric columns with pd.NA.

    Preserves zero values. Only affects numeric columns; string columns unchanged.

    Args:
        ddf: Input Dask DataFrame with canonical schema.
        logger: Configured logger instance.

    Returns:
        Dask DataFrame with sentinel values replaced by pd.NA.
    """
    logger.info("Replacing sentinel values: %s", SENTINEL_VALUES)
    return ddf.map_partitions(_replace_sentinels_partition, meta=ddf._meta)


# ---------------------------------------------------------------------------
# Well ID construction
# ---------------------------------------------------------------------------

_WELL_ID_RE = re.compile(r"^05\d{8}$")


def _build_well_id_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Construct and validate well_id for a single partition."""

    def _make_well_id(row: pd.Series) -> str | Any:
        county = row["APICountyCode"]
        seq = row["APISeqNum"]
        if pd.isna(county) or pd.isna(seq):
            return pd.NA  # type: ignore[return-value]
        try:
            county_str = str(county).strip().zfill(3)
            seq_str = str(seq).strip().zfill(5)
        except Exception:
            return pd.NA  # type: ignore[return-value]

        well_id = f"05{county_str}{seq_str}"
        if not _WELL_ID_RE.match(well_id):
            return pd.NA  # type: ignore[return-value]
        county_int = int(county_str)
        seq_int = int(seq_str)
        if not (1 <= county_int <= 125):
            return pd.NA  # type: ignore[return-value]
        if not (1 <= seq_int <= 99999):
            return pd.NA  # type: ignore[return-value]
        return well_id

    df = df.copy()
    df.insert(0, "well_id", df.apply(_make_well_id, axis=1))
    df["well_id"] = df["well_id"].astype(pd.StringDtype())
    return df


def build_well_id(ddf: dd.DataFrame, logger: logging.Logger) -> dd.DataFrame:
    """Construct a 10-digit ``well_id`` column and validate it.

    Invalid rows receive ``pd.NA`` for ``well_id``.

    Args:
        ddf: Input Dask DataFrame with canonical schema.
        logger: Configured logger instance.

    Returns:
        Dask DataFrame with ``well_id`` prepended as the first column.
    """
    logger.info("Building well_id column")
    meta_df = ddf._meta.copy()
    meta_df.insert(0, "well_id", pd.array([], dtype=pd.StringDtype()))
    result = ddf.map_partitions(_build_well_id_partition, meta=meta_df)
    return result


# ---------------------------------------------------------------------------
# Production date construction
# ---------------------------------------------------------------------------

_CURRENT_YEAR = datetime.now().year


def _build_production_date_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Construct production_date from ReportYear/ReportMonth for one partition."""
    df = df.copy()

    year = pd.to_numeric(df["ReportYear"], errors="coerce")
    month = pd.to_numeric(df["ReportMonth"], errors="coerce")

    valid_year = year.between(2020, _CURRENT_YEAR)
    valid_month = month.between(1, 12)
    valid = valid_year & valid_month

    prod_dates: pd.Series = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    if valid.any():
        tmp = pd.to_datetime(
            {
                "year": year.where(valid),
                "month": month.where(valid),
                "day": pd.Series(1, index=df.index),
            },
            errors="coerce",
        )
        prod_dates = tmp

    df["production_date"] = prod_dates
    df = df.drop(columns=["ReportYear", "ReportMonth"], errors="ignore")
    return df


def build_production_date(ddf: dd.DataFrame, logger: logging.Logger) -> dd.DataFrame:
    """Construct a ``production_date`` datetime column from year/month fields.

    Drops ``ReportYear`` and ``ReportMonth`` from the output.

    Args:
        ddf: Input Dask DataFrame.
        logger: Configured logger instance.

    Returns:
        Dask DataFrame with ``production_date`` added and year/month columns dropped.
    """
    logger.info("Building production_date column")
    meta_df = ddf._meta.copy()
    meta_df["production_date"] = pd.Series(dtype="datetime64[ns]")
    meta_df = meta_df.drop(columns=["ReportYear", "ReportMonth"], errors="ignore")
    return ddf.map_partitions(_build_production_date_partition, meta=meta_df)


# ---------------------------------------------------------------------------
# Physical bounds validation
# ---------------------------------------------------------------------------


def _validate_bounds_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Apply physical bound rules and add outlier_flag column."""
    df = df.copy()
    df["outlier_flag"] = pd.array([False] * len(df), dtype=pd.BooleanDtype())

    # Negative → null
    neg_cols = [
        "OilProduced",
        "OilSold",
        "GasProduced",
        "GasSold",
        "GasFlared",
        "GasUsed",
        "WaterProduced",
    ]
    for col in neg_cols:
        if col in df.columns:
            mask_neg = pd.to_numeric(df[col], errors="coerce") < 0
            df.loc[mask_neg, col] = pd.NA

    # Oil/OilSold upper bound → outlier flag
    for col in ("OilProduced", "OilSold"):
        if col in df.columns:
            mask_high = pd.to_numeric(df[col], errors="coerce") > 50_000
            df.loc[mask_high, "outlier_flag"] = True

    # DaysProduced [0, 31]
    if "DaysProduced" in df.columns:
        days = pd.to_numeric(df["DaysProduced"], errors="coerce")
        df.loc[(days < 0) | (days > 31), "DaysProduced"] = pd.NA

    return df


def validate_physical_bounds(ddf: dd.DataFrame, logger: logging.Logger) -> dd.DataFrame:
    """Apply physical bounds rules and add boolean ``outlier_flag`` column.

    Negative production values are nulled; values exceeding the upper bound are
    flagged but retained.

    Args:
        ddf: Input Dask DataFrame.
        logger: Configured logger instance.

    Returns:
        Dask DataFrame with ``outlier_flag`` column added.
    """
    logger.info("Validating physical bounds")
    meta_df = ddf._meta.copy()
    meta_df["outlier_flag"] = pd.array([], dtype=pd.BooleanDtype())
    return ddf.map_partitions(_validate_bounds_partition, meta=meta_df)


# ---------------------------------------------------------------------------
# Duplicate removal
# ---------------------------------------------------------------------------


def remove_duplicates(ddf: dd.DataFrame, logger: logging.Logger) -> dd.DataFrame:
    """Remove duplicate rows keyed on (well_id, production_date).

    Rows with null keys are kept (not considered duplicates of each other).

    Args:
        ddf: Input Dask DataFrame.
        logger: Configured logger instance.

    Returns:
        Dask DataFrame with duplicate rows removed.
    """
    logger.info("Removing duplicates on (well_id, production_date)")
    return ddf.drop_duplicates(subset=["well_id", "production_date"])


# ---------------------------------------------------------------------------
# Data quality report
# ---------------------------------------------------------------------------


def generate_quality_report(
    ddf: dd.DataFrame,
    output_path: Path,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Compute and serialise a JSON data quality report.

    This function calls ``.compute()`` to materialise summary statistics.

    Args:
        ddf: Cleaned Dask DataFrame.
        output_path: Path where the JSON report will be written.
        logger: Configured logger instance.

    Returns:
        Dictionary containing data quality metrics.
    """
    logger.info("Generating data quality report")
    df = ddf.compute()

    null_counts = df.isnull().sum().to_dict()
    total_rows = len(df)
    outlier_count = int(df["outlier_flag"].sum()) if "outlier_flag" in df.columns else 0
    invalid_well_count = int(df["well_id"].isnull().sum()) if "well_id" in df.columns else 0
    invalid_date_count = (
        int(df["production_date"].isnull().sum()) if "production_date" in df.columns else 0
    )
    wells_count = int(df["well_id"].dropna().nunique()) if "well_id" in df.columns else 0

    date_min: str | None = None
    date_max: str | None = None
    if "production_date" in df.columns:
        valid_dates = df["production_date"].dropna()
        if not valid_dates.empty:
            date_min = valid_dates.min().isoformat()
            date_max = valid_dates.max().isoformat()

    report: dict[str, Any] = {
        "total_rows": total_rows,
        "null_counts": {k: int(v) for k, v in null_counts.items()},
        "outlier_flag_count": outlier_count,
        "invalid_well_id_count": invalid_well_count,
        "invalid_production_date_count": invalid_date_count,
        "wells_count": wells_count,
        "date_range_min": date_min,
        "date_range_max": date_max,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    logger.info("Quality report written to %s", output_path)
    return report


# ---------------------------------------------------------------------------
# Well completeness checker
# ---------------------------------------------------------------------------


def check_well_completeness(ddf: dd.DataFrame, logger: logging.Logger) -> dict[str, list[str]]:
    """Identify wells with gaps in their monthly production records.

    Calls ``.compute()`` internally.

    Args:
        ddf: Cleaned Dask DataFrame with ``well_id`` and ``production_date``.
        logger: Configured logger instance.

    Returns:
        Mapping of ``well_id`` → list of ISO date strings for missing months.
    """
    logger.info("Checking well completeness")
    df = ddf[["well_id", "production_date"]].dropna().compute()

    gaps: dict[str, list[str]] = {}
    grouped = df.groupby("well_id")["production_date"]

    total = 0
    with_gaps = 0
    for well_id, dates in grouped:
        total += 1
        d_min: pd.Timestamp = dates.min()
        d_max: pd.Timestamp = dates.max()
        expected_months = (d_max.year - d_min.year) * 12 + (d_max.month - d_min.month) + 1
        actual = len(dates)
        if actual < expected_months:
            with_gaps += 1
            full_range = pd.date_range(d_min, d_max, freq="MS")
            actual_set = set(dates.dt.to_period("M"))
            missing = [
                d.strftime("%Y-%m-%d") for d in full_range if d.to_period("M") not in actual_set
            ]
            gaps[str(well_id)] = missing
        else:
            gaps[str(well_id)] = []

    logger.info(
        "Well completeness: %d total, %d with gaps, %d without gaps",
        total,
        with_gaps,
        total - with_gaps,
    )
    return gaps


# ---------------------------------------------------------------------------
# Cleaned Parquet writer
# ---------------------------------------------------------------------------


def write_cleaned_parquet(
    ddf: dd.DataFrame,
    processed_dir: Path,
    filename: str,
    logger: logging.Logger,
) -> Path:
    """Sort, repartition, and write the cleaned Dask DataFrame to Parquet.

    Args:
        ddf: Cleaned Dask DataFrame.
        processed_dir: Directory for the Parquet dataset.
        filename: Subdirectory name for the Parquet dataset.
        logger: Configured logger instance.

    Returns:
        Path to the written Parquet dataset directory.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    output_path = processed_dir / filename
    logger.info("Writing cleaned Parquet to %s (30 partitions)", output_path)
    ddf.repartition(npartitions=30).to_parquet(
        str(output_path),
        write_index=False,
        engine="pyarrow",
        overwrite=True,
    )
    logger.info("Cleaned Parquet write complete: %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def run_transform(config: dict[str, Any], logger: logging.Logger) -> Path:
    """Run the full transform stage: clean, validate, and write processed data.

    Steps (all lazy until step 9):
    1. Read interim Parquet.
    2. Repartition.
    3. Replace sentinels.
    4. Build well_id.
    5. Build production_date.
    6. Validate physical bounds.
    7. Remove duplicates.
    8. Write cleaned Parquet.
    9. Generate quality report (triggers .compute()).
    10. Check well completeness (triggers .compute()).

    Args:
        config: Pipeline configuration dictionary (top-level).
        logger: Configured logger instance.

    Returns:
        Path to the cleaned Parquet dataset directory.
    """
    tcfg = config["transform"]
    interim_dir = Path(tcfg["interim_dir"])
    processed_dir = Path(tcfg["processed_dir"])
    cleaned_file: str = tcfg["cleaned_file"]

    interim_path = interim_dir / "cogcc_production_interim.parquet"
    logger.info("Reading interim Parquet from %s", interim_path)
    ddf = dd.read_parquet(str(interim_path), engine="pyarrow")
    ddf = ddf.repartition(npartitions=min(ddf.npartitions, 50))

    ddf = replace_sentinels(ddf, logger)
    ddf = build_well_id(ddf, logger)
    ddf = build_production_date(ddf, logger)
    ddf = validate_physical_bounds(ddf, logger)
    ddf = remove_duplicates(ddf, logger)

    cleaned_path = write_cleaned_parquet(ddf, processed_dir, cleaned_file, logger)

    quality_report_path = processed_dir / "data_quality_report.json"
    cleaned_ddf = dd.read_parquet(str(cleaned_path), engine="pyarrow")
    generate_quality_report(cleaned_ddf, quality_report_path, logger)
    check_well_completeness(cleaned_ddf, logger)

    return cleaned_path
