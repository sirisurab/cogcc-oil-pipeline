"""Ingest stage: read raw CSVs, validate schema, write interim Parquet."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import pandas as pd
from dask import delayed
from dask.distributed import Client, LocalCluster

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# I-01: dtype mapping and meta construction
# ---------------------------------------------------------------------------


def build_dtype_map() -> dict[str, Any]:
    """Return a column→dtype dict for all raw CSV columns (excluding AcceptedDate).

    Returns:
        Dict mapping column names to pandas dtypes for use in pd.read_csv dtype=.
    """
    return {
        "DocNum": "int64",
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


def build_meta() -> pd.DataFrame:
    """Return an empty DataFrame with all columns and dtypes per the data dictionary.

    Returns:
        Empty pandas DataFrame matching the full schema.
    """
    dtype_map = build_dtype_map()  # type: ignore[call-overload]
    meta = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in dtype_map.items()})
    meta["AcceptedDate"] = pd.Series(dtype="datetime64[us]")
    # Reorder to logical column order
    ordered_cols = [
        "DocNum",
        "ReportMonth",
        "ReportYear",
        "DaysProduced",
        "AcceptedDate",
        "Revised",
        "OpName",
        "OpNumber",
        "FacilityId",
        "ApiCountyCode",
        "ApiSequenceNumber",
        "ApiSidetrack",
        "Well",
        "WellStatus",
        "FormationCode",
        "OilProduced",
        "OilSales",
        "OilAdjustment",
        "OilGravity",
        "GasProduced",
        "GasSales",
        "GasBtuSales",
        "GasUsedOnLease",
        "GasShrinkage",
        "GasPressureTubing",
        "GasPressureCasing",
        "WaterProduced",
        "WaterPressureTubing",
        "WaterPressureCasing",
        "FlaredVented",
        "BomInvent",
        "EomInvent",
    ]
    return meta[ordered_cols]


# ---------------------------------------------------------------------------
# I-02: Single raw CSV reader
# ---------------------------------------------------------------------------


def read_raw_file(file_path: Path) -> pd.DataFrame:
    """Read a single raw production CSV with enforced dtypes and schema validation.

    Missing columns are added as all-NA with the correct dtype.

    Args:
        file_path: Path to a raw CSV file.

    Returns:
        pandas DataFrame matching the schema defined by build_meta().

    Raises:
        FileNotFoundError: If file does not exist.
        ValueError: On CSV parse error.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"CSV not found: {file_path}")

    dtype_map = build_dtype_map()
    try:
        df = pd.read_csv(
            file_path,
            dtype=dtype_map,  # type: ignore[arg-type]
            parse_dates=["AcceptedDate"],
            low_memory=False,
            na_values=["N/A", "NA", "n/a", ""],
            keep_default_na=True,
        )
    except Exception as exc:
        logger.error("CSV parse error in %s: %s", file_path, exc)
        raise ValueError(f"CSV parse error in {file_path}: {exc}") from exc

    meta = build_meta()
    for col, dtype in meta.dtypes.items():
        if col not in df.columns:
            logger.warning("Missing column '%s' in %s — filling with NA", col, file_path.name)
            df[col] = pd.array([pd.NA] * len(df), dtype=dtype)  # type: ignore[call-overload]

    # Cast datetime columns to match meta dtype (datetime64[us])
    for col in df.select_dtypes(include=["datetime64"]).columns:
        if col in meta.columns:
            df[col] = df[col].astype(meta[col].dtype)

    # Ensure column order matches meta
    df = df[list(meta.columns)]
    return df


# ---------------------------------------------------------------------------
# I-03: Year filter
# ---------------------------------------------------------------------------


def _filter_year(pdf: pd.DataFrame, year_start: int) -> pd.DataFrame:
    """Filter a pandas partition to rows where ReportYear >= year_start.

    Args:
        pdf: A pandas partition.
        year_start: Minimum year (inclusive).

    Returns:
        Filtered pandas DataFrame.
    """
    return pdf[pdf["ReportYear"] >= year_start]


# ---------------------------------------------------------------------------
# I-04: Schema validation
# ---------------------------------------------------------------------------


def validate_schema(ddf: dd.DataFrame, expected_columns: list[str]) -> list[str]:
    """Return list of column names missing from ddf (without calling .compute()).

    Args:
        ddf: A Dask DataFrame.
        expected_columns: List of expected column names.

    Returns:
        List of missing column names (empty if all present).
    """
    present = set(ddf.columns)
    missing = [c for c in expected_columns if c not in present]
    for col in missing:
        logger.warning("Schema validation: missing column '%s'", col)
    return missing


# ---------------------------------------------------------------------------
# I-05: Dask distributed client management
# ---------------------------------------------------------------------------


def get_or_create_client(config: dict) -> tuple[Client, bool]:
    """Return existing Dask distributed Client or create a new one.

    Args:
        config: Full config dict.

    Returns:
        Tuple of (Client, created) where created=True if a new client was made.
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
# I-06: Ingest stage runner
# ---------------------------------------------------------------------------


def run_ingest(config: dict) -> None:
    """Top-level ingest stage: read raw CSVs, filter, and write interim Parquet.

    Args:
        config: Full config dict.

    Raises:
        FileNotFoundError: If no CSV files are found in raw_dir.
        RuntimeError: On Dask computation errors.
    """
    raw_dir = Path(config["ingest"]["raw_dir"])
    interim_dir = Path(config["ingest"]["interim_dir"])
    year_start: int = config["ingest"]["year_start"]

    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {raw_dir}")

    logger.info("Found %d CSV file(s) in %s", len(csv_files), raw_dir)

    client, created = get_or_create_client(config)

    try:
        delayed_list = [delayed(read_raw_file)(fp) for fp in csv_files]
        meta = build_meta()
        ddf = dd.from_delayed(delayed_list, meta=meta)

        missing_cols = validate_schema(ddf, list(meta.columns))
        if missing_cols:
            logger.warning("Missing columns detected: %s", missing_cols)

        ddf = ddf.map_partitions(_filter_year, year_start=year_start, meta=ddf._meta)

        npartitions = min(ddf.npartitions, 50)
        ddf = ddf.repartition(npartitions=npartitions)

        npartitions_out = max(1, ddf.npartitions // 10)

        interim_dir.mkdir(parents=True, exist_ok=True)
        output_path = interim_dir / "cogcc_raw.parquet"

        logger.info("Writing interim Parquet to %s (%d partitions)", output_path, npartitions_out)
        try:
            ddf.repartition(npartitions=npartitions_out).to_parquet(
                str(output_path), write_index=False, engine="pyarrow"
            )
        except Exception as exc:
            logger.error("Dask computation error during ingest: %s", exc)
            raise RuntimeError(f"Ingest computation failed: {exc}") from exc

        logger.info("Ingest complete. Output: %s", output_path)
    finally:
        if created:
            client.close()
