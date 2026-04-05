"""Ingest stage: read raw CSVs, normalise columns, write interim Parquet."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import pandas as pd
from dask import delayed

# ---------------------------------------------------------------------------
# Canonical schema
# ---------------------------------------------------------------------------

CANONICAL_COLUMNS: list[str] = [
    "APICountyCode",
    "APISeqNum",
    "ReportYear",
    "ReportMonth",
    "OilProduced",
    "OilSold",
    "GasProduced",
    "GasSold",
    "GasFlared",
    "GasUsed",
    "WaterProduced",
    "DaysProduced",
    "Well",
    "OpName",
    "OpNum",
    "DocNum",
]

# Dtypes for canonical columns
CANONICAL_DTYPES: dict[str, Any] = {
    "APICountyCode": pd.StringDtype(),
    "APISeqNum": pd.StringDtype(),
    "ReportYear": pd.Int32Dtype(),
    "ReportMonth": pd.Int32Dtype(),
    "OilProduced": pd.Float64Dtype(),
    "OilSold": pd.Float64Dtype(),
    "GasProduced": pd.Float64Dtype(),
    "GasSold": pd.Float64Dtype(),
    "GasFlared": pd.Float64Dtype(),
    "GasUsed": pd.Float64Dtype(),
    "WaterProduced": pd.Float64Dtype(),
    "DaysProduced": pd.Float64Dtype(),
    "Well": pd.StringDtype(),
    "OpName": pd.StringDtype(),
    "OpNum": pd.StringDtype(),
    "DocNum": pd.StringDtype(),
}

# ---------------------------------------------------------------------------
# Column name variants mapping
# ---------------------------------------------------------------------------

COLUMN_MAP: dict[str, str] = {
    # APICountyCode
    "API_County_Code": "APICountyCode",
    "Api_County_Code": "APICountyCode",
    "apicountycode": "APICountyCode",
    "api_county_code": "APICountyCode",
    "APICOUNTYCODE": "APICountyCode",
    # APISeqNum
    "API_Seq_Num": "APISeqNum",
    "Api_Seq_Num": "APISeqNum",
    "apiseqnum": "APISeqNum",
    "api_seq_num": "APISeqNum",
    "APISEQNUM": "APISeqNum",
    # ReportYear
    "Report_Year": "ReportYear",
    "report_year": "ReportYear",
    "REPORTYEAR": "ReportYear",
    "reportyear": "ReportYear",
    # ReportMonth
    "Report_Month": "ReportMonth",
    "report_month": "ReportMonth",
    "REPORTMONTH": "ReportMonth",
    "reportmonth": "ReportMonth",
    # OilProduced
    "Oil_Produced": "OilProduced",
    "oil_produced": "OilProduced",
    "OILPRODUCED": "OilProduced",
    "oilproduced": "OilProduced",
    # OilSold
    "Oil_Sold": "OilSold",
    "oil_sold": "OilSold",
    "OILSOLD": "OilSold",
    "oilsold": "OilSold",
    # GasProduced
    "Gas_Produced": "GasProduced",
    "gas_produced": "GasProduced",
    "GASPRODUCED": "GasProduced",
    "gasproduced": "GasProduced",
    # GasSold
    "Gas_Sold": "GasSold",
    "gas_sold": "GasSold",
    "GASSOLD": "GasSold",
    "gassold": "GasSold",
    # GasFlared
    "Gas_Flared": "GasFlared",
    "gas_flared": "GasFlared",
    "GASFLARED": "GasFlared",
    "gasflared": "GasFlared",
    # GasUsed
    "Gas_Used": "GasUsed",
    "gas_used": "GasUsed",
    "GASUSED": "GasUsed",
    "gasused": "GasUsed",
    # WaterProduced
    "Water_Produced": "WaterProduced",
    "water_produced": "WaterProduced",
    "WATERPRODUCED": "WaterProduced",
    "waterproduced": "WaterProduced",
    # DaysProduced
    "Days_Produced": "DaysProduced",
    "days_produced": "DaysProduced",
    "DAYSPRODUCED": "DaysProduced",
    "daysproduced": "DaysProduced",
    # Well — no common variants; keep as-is
    "well": "Well",
    "WELL": "Well",
    # OpName
    "Op_Name": "OpName",
    "op_name": "OpName",
    "OPNAME": "OpName",
    "opname": "OpName",
    # OpNum
    "Op_Num": "OpNum",
    "op_num": "OpNum",
    "OPNUM": "OpNum",
    "opnum": "OpNum",
    # DocNum
    "Doc_Num": "DocNum",
    "doc_num": "DocNum",
    "DOCNUM": "DocNum",
    "docnum": "DocNum",
}


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename raw ECMC column variants to canonical names and enforce dtypes.

    Unknown columns are dropped. Missing canonical columns are added as nulls.

    Args:
        df: Raw pandas DataFrame with any column naming convention.

    Returns:
        DataFrame with exactly the 16 canonical columns in canonical order.
    """
    # Strip whitespace from column names
    df = df.rename(columns=lambda c: c.strip())
    # Apply variant mapping
    df = df.rename(columns=COLUMN_MAP)
    # Drop non-canonical columns
    extra = [c for c in df.columns if c not in CANONICAL_COLUMNS]
    df = df.drop(columns=extra)
    # Add missing canonical columns as null with correct dtype
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.array([pd.NA] * len(df), dtype=CANONICAL_DTYPES[col])
    # Coerce dtypes
    for col in CANONICAL_COLUMNS:
        try:
            df[col] = df[col].astype(CANONICAL_DTYPES[col])
        except (ValueError, TypeError):
            df[col] = pd.array([pd.NA] * len(df), dtype=CANONICAL_DTYPES[col])
    # Return in canonical column order
    return df[CANONICAL_COLUMNS]


def read_raw_csv(path: Path, logger: logging.Logger) -> pd.DataFrame:
    """Read a single ECMC production CSV into a normalised pandas DataFrame.

    Tries UTF-8 encoding first, falls back to Latin-1 on decode error. Applies
    :func:`normalise_columns` and filters to ``ReportYear >= 2020``.

    Args:
        path: Path to the raw CSV file.
        logger: Configured logger instance.

    Returns:
        Normalised and filtered pandas DataFrame.

    Raises:
        ValueError: If the file cannot be parsed as a CSV.
    """
    try:
        try:
            df = pd.read_csv(path, encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            logger.warning("UTF-8 decode failed for %s, retrying with latin-1", path)
            df = pd.read_csv(path, encoding="latin-1", low_memory=False)
    except Exception as exc:
        raise ValueError(f"Cannot parse {path} as CSV: {exc}") from exc

    raw_count = len(df)

    # Strip whitespace from string values
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    df = normalise_columns(df)

    # Filter to ReportYear >= 2020
    year_col = df["ReportYear"]
    # Convert to numeric for comparison (handles NA)
    mask = pd.to_numeric(year_col, errors="coerce").ge(2020).fillna(False)
    df = df.loc[mask].reset_index(drop=True)

    logger.info("Read %s: %d raw rows → %d rows after 2020 filter", path.name, raw_count, len(df))
    return df


def _build_meta() -> pd.DataFrame:
    """Build an empty meta DataFrame for Dask with canonical schema."""
    return pd.DataFrame(
        {col: pd.array([], dtype=CANONICAL_DTYPES[col]) for col in CANONICAL_COLUMNS}
    )


def ingest_raw_files(raw_dir: Path, logger: logging.Logger) -> dd.DataFrame:
    """Discover raw CSVs and return a lazy Dask DataFrame.

    Wraps :func:`read_raw_csv` with ``dask.delayed`` for parallel execution.
    Does **not** call ``.compute()``.

    Args:
        raw_dir: Directory containing raw ECMC CSV files.
        logger: Configured logger instance.

    Returns:
        Lazy :class:`dask.dataframe.DataFrame` with canonical schema.

    Raises:
        FileNotFoundError: If no matching CSV files are found in raw_dir.
    """
    patterns = ["*_prod_reports.csv", "monthly_prod.csv"]
    csv_paths: list[Path] = []
    for pat in patterns:
        csv_paths.extend(sorted(raw_dir.glob(pat)))

    if not csv_paths:
        raise FileNotFoundError(f"No CSV files found in {raw_dir}")

    logger.info("Found %d CSV file(s) in %s", len(csv_paths), raw_dir)
    meta = _build_meta()

    delayed_dfs = [delayed(read_raw_csv)(p, logger) for p in csv_paths]
    ddf = dd.from_delayed(delayed_dfs, meta=meta)
    return ddf


def write_interim_parquet(
    ddf: dd.DataFrame,
    interim_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Repartition and write the Dask DataFrame to Parquet.

    Args:
        ddf: Dask DataFrame to write.
        interim_dir: Directory for the Parquet dataset.
        logger: Configured logger instance.

    Returns:
        Path to the written Parquet dataset directory.
    """
    interim_dir.mkdir(parents=True, exist_ok=True)
    output_path = interim_dir / "cogcc_production_interim.parquet"

    # Estimate partition count
    n_parts = max(1, ddf.npartitions)
    target_partitions = n_parts

    logger.info(
        "Writing interim Parquet to %s with %d partition(s)", output_path, target_partitions
    )
    ddf.repartition(npartitions=target_partitions).to_parquet(
        str(output_path),
        write_index=False,
        engine="pyarrow",
        overwrite=True,
    )
    logger.info("Interim Parquet write complete: %s", output_path)
    return output_path


def validate_interim_schema(
    interim_parquet_path: Path,
    logger: logging.Logger,
) -> list[str]:
    """Check that sampled Parquet partitions contain all 16 canonical columns.

    Args:
        interim_parquet_path: Path to the Parquet dataset directory.
        logger: Configured logger instance.

    Returns:
        List of validation error strings; empty if all sampled partitions pass.
    """
    parquet_files = sorted(interim_parquet_path.glob("*.parquet"))
    sample = parquet_files[:3] if len(parquet_files) >= 3 else parquet_files

    errors: list[str] = []
    for pf in sample:
        df = pd.read_parquet(pf)
        for col in CANONICAL_COLUMNS:
            if col not in df.columns:
                msg = f"{pf.name}: missing column '{col}'"
                logger.warning(msg)
                errors.append(msg)
    return errors


def run_ingest(config: dict[str, Any], logger: logging.Logger) -> Path:
    """Top-level entry point for the ingest stage.

    Reads all raw CSVs, writes interim Parquet, and validates the output schema.

    Args:
        config: Pipeline configuration dictionary (top-level).
        logger: Configured logger instance.

    Returns:
        Path to the interim Parquet dataset directory.
    """
    icfg = config["ingest"]
    raw_dir = Path(icfg["raw_dir"])
    interim_dir = Path(icfg["interim_dir"])

    ddf = ingest_raw_files(raw_dir, logger)
    interim_path = write_interim_parquet(ddf, interim_dir, logger)
    errors = validate_interim_schema(interim_path, logger)
    for err in errors:
        logger.warning("Schema validation: %s", err)
    return interim_path
