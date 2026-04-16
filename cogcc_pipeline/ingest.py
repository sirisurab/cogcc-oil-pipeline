"""Ingest stage: read raw CSVs, validate schema, write interim Parquet."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

import dask
import dask.dataframe as dd
import pandas as pd

logger = logging.getLogger(__name__)

# Canonical schema: column → pandas dtype
DTYPE_MAP: dict[str, object] = {
    "DocNum": pd.Int64Dtype(),
    "ReportMonth": "int64",
    "ReportYear": "int64",
    "DaysProduced": pd.Int64Dtype(),
    "Revised": pd.BooleanDtype(),
    "OpName": pd.StringDtype(),
    "OpNumber": pd.Int64Dtype(),
    "FacilityId": pd.Int64Dtype(),
    "ApiCountyCode": pd.StringDtype(),
    "ApiSequenceNumber": pd.StringDtype(),
    "ApiSidetrack": pd.StringDtype(),
    "Well": pd.StringDtype(),
    "WellStatus": pd.StringDtype(),
    "FormationCode": pd.StringDtype(),
    "OilProduced": pd.Float64Dtype(),
    "OilSales": pd.Float64Dtype(),
    "OilAdjustment": pd.Float64Dtype(),
    "OilGravity": pd.Float64Dtype(),
    "GasProduced": pd.Float64Dtype(),
    "GasSales": pd.Float64Dtype(),
    "GasBtuSales": pd.Float64Dtype(),
    "GasUsedOnLease": pd.Float64Dtype(),
    "GasShrinkage": pd.Float64Dtype(),
    "GasPressureTubing": pd.Float64Dtype(),
    "GasPressureCasing": pd.Float64Dtype(),
    "WaterProduced": pd.Float64Dtype(),
    "WaterPressureTubing": pd.Float64Dtype(),
    "WaterPressureCasing": pd.Float64Dtype(),
    "FlaredVented": pd.Float64Dtype(),
    "BomInvent": pd.Float64Dtype(),
    "EomInvent": pd.Float64Dtype(),
}

# Non-nullable columns per the data dictionary
NON_NULLABLE = {"ReportMonth", "ReportYear", "ApiCountyCode", "ApiSequenceNumber", "Well"}

REQUIRED_COLUMNS = set(DTYPE_MAP.keys()) | {"AcceptedDate"}


class IngestError(RuntimeError):
    """Raised on unrecoverable ingest failures."""


def log_file_metadata(path: Path) -> dict:
    """Compute and log file-level metadata for a raw CSV.

    Returns:
        Dict with file_path, file_size_bytes, sha256, row_count_raw.
    """
    file_size = path.stat().st_size
    content = path.read_bytes()
    sha256 = hashlib.sha256(content).hexdigest()
    row_count_raw = max(0, content.decode("utf-8", errors="replace").count("\n") - 1)

    meta = {
        "file_path": str(path.absolute()),
        "file_size_bytes": file_size,
        "sha256": sha256,
        "row_count_raw": row_count_raw,
    }
    logger.info(
        "File metadata: path=%s size=%d sha256=%s rows=%d",
        meta["file_path"],
        file_size,
        sha256,
        row_count_raw,
    )
    return meta


def read_raw_file(path: Path, config: dict) -> pd.DataFrame:
    """Read a single raw CSV with schema enforcement and year filtering.

    Raises:
        IngestError: If required columns are missing or encoding cannot be handled.
    """
    start_year: int = config["ingest"]["start_year"]

    # Build dtype dict excluding AcceptedDate (handled via parse_dates)
    dtype_map: dict[str, Any] = {k: v for k, v in DTYPE_MAP.items()}

    def _read(enc: str) -> pd.DataFrame:
        return pd.read_csv(
            path,
            dtype=dtype_map,  # type: ignore[arg-type]
            parse_dates=["AcceptedDate"],
            encoding=enc,
        )

    try:
        df = _read("utf-8-sig")
    except UnicodeDecodeError:
        try:
            df = _read("latin-1")
        except Exception as exc:
            raise IngestError(f"Cannot decode {path}: {exc}") from exc

    # Normalize column names to canonical schema via case-insensitive matching
    canonical_lower = {c.lower(): c for c in DTYPE_MAP}
    rename_map = {}
    drop_cols = []
    for col in df.columns:
        canonical = canonical_lower.get(col.lower())
        if canonical and canonical != col:
            rename_map[col] = canonical
        elif not canonical and col != "AcceptedDate":
            drop_cols.append(col)
    if rename_map:
        logger.warning("Renaming non-canonical columns in %s: %s", path, rename_map)
        df = df.rename(columns=rename_map)
    if drop_cols:
        logger.warning("Dropping non-canonical columns in %s: %s", path, drop_cols)
        df = df.drop(columns=drop_cols)

    # Raise only if non-nullable columns are missing
    missing_required = [c for c in NON_NULLABLE if c not in df.columns]
    if missing_required:
        raise IngestError(f"Missing required columns in {path}: {missing_required}")

    # Fill missing nullable columns with NA at the correct dtype
    for col, dtype in DTYPE_MAP.items():
        if col not in df.columns and col not in NON_NULLABLE:
            logger.warning("Missing nullable column '%s' in %s — filling with NA", col, path)
            df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)

    # Filter to configured start year
    df = df[df["ReportYear"] >= start_year].copy()

    # Drop rows where both ApiCountyCode and ApiSequenceNumber are null
    both_null = df["ApiCountyCode"].isna() & df["ApiSequenceNumber"].isna()
    if both_null.any():
        logger.warning(
            "Dropping %d rows with null ApiCountyCode and ApiSequenceNumber", both_null.sum()
        )
        df = df[~both_null].copy()

    # Warn on nulls in non-nullable columns
    for col in NON_NULLABLE:
        if col in df.columns and df[col].isna().any():
            logger.warning("Non-nullable column %s contains nulls in %s", col, path)

    return df


def make_delayed_df(path: Path, config: dict) -> dd.DataFrame:
    """Wrap read_raw_file in a dask.delayed call and return a Dask DataFrame.

    The meta is built by reading the first 5 data rows of the file.
    """

    # Build meta from a minimal real read (head of file only)
    try:
        meta_df = read_raw_file(path, config)
        meta: pd.DataFrame = meta_df.iloc[0:0].copy()
    except Exception:
        # Fallback: construct empty DataFrame from schema
        meta_cols = {**DTYPE_MAP}
        meta = pd.DataFrame({c: pd.Series([], dtype=t) for c, t in meta_cols.items()})  # type: ignore[call-overload]
        meta["AcceptedDate"] = pd.Series(dtype="datetime64[ns]")

    delayed_read = dask.delayed(read_raw_file)(path, config)
    ddf = dd.from_delayed([delayed_read], meta=meta)
    return ddf


def validate_schema(ddf: dd.DataFrame) -> None:
    """Validate that a Dask DataFrame matches the canonical schema.

    Raises:
        IngestError: If required columns are missing.
    """
    actual_cols = set(ddf.columns)
    expected_cols = set(DTYPE_MAP.keys()) | {"AcceptedDate"}
    missing = expected_cols - actual_cols
    if missing:
        raise IngestError(f"Schema validation failed — missing columns: {sorted(missing)}")

    # Warn on dtype mismatches (do not raise)
    for col, expected_dtype in DTYPE_MAP.items():
        if col not in ddf.columns:
            continue
        actual_dtype = ddf.dtypes[col]
        if str(actual_dtype) != str(expected_dtype):
            logger.warning(
                "Dtype mismatch for %s: expected %s, got %s", col, expected_dtype, actual_dtype
            )

    n_partitions = ddf.npartitions
    logger.info(
        "Schema validated: %d columns, %d partitions (estimated)",
        len(ddf.columns),
        n_partitions,
    )


def write_interim(ddf: dd.DataFrame, config: dict) -> None:
    """Repartition and write Dask DataFrame to interim Parquet dataset."""
    target_n = max(10, min(ddf.npartitions, 50))
    ddf = ddf.repartition(npartitions=target_n)

    out_dir = config["ingest"]["interim_dir"] + "/production.parquet"
    ddf.to_parquet(out_dir, engine="pyarrow", write_index=False, overwrite=True)
    logger.info("Wrote interim Parquet: %d partitions → %s", target_n, out_dir)


def run_ingest(config: dict) -> dd.DataFrame:
    """Orchestrate the full ingest stage.

    Returns:
        Dask DataFrame (not computed).

    Raises:
        IngestError: If no CSV files are found or on schema failure.
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

    raw_dir = Path(config["ingest"]["raw_dir"])
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise IngestError(f"No CSV files found in {raw_dir}")

    ddf_list: list[dd.DataFrame] = []
    for csv_path in csv_files:
        log_file_metadata(csv_path)
        ddf_list.append(make_delayed_df(csv_path, config))

    ddf = dd.concat(ddf_list)
    validate_schema(ddf)
    write_interim(ddf, config)
    return ddf
