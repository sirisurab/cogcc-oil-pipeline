"""Ingest stage — load raw CSVs into interim Parquet via Dask."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from cogcc_pipeline.acquire import get_logger, load_config

_log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Required schema columns
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS: list[str] = [
    "ApiCountyCode",
    "ApiSequenceNumber",
    "ReportYear",
    "ReportMonth",
    "OilProduced",
    "OilSales",
    "GasProduced",
    "GasSales",
    "FlaredVented",
    "GasUsedOnLease",
    "WaterProduced",
    "DaysProduced",
    "Well",
    "OpName",
    "OpNum",
    "DocNum",
]

# Dtype map for standardise_dtypes
_DTYPE_MAP: dict[str, object] = {
    "ReportYear": "int32",
    "ReportMonth": "int32",
    "OilProduced": "float64",
    "OilSales": "float64",
    "GasProduced": "float64",
    "GasSales": "float64",
    "FlaredVented": "float64",
    "GasUsedOnLease": "float64",
    "WaterProduced": "float64",
    "DaysProduced": "float64",
}

_STRING_COLS = {"ApiCountyCode", "ApiSequenceNumber", "Well", "OpName", "OpNum", "DocNum"}


# ---------------------------------------------------------------------------
# Task 01 — single-file reader
# ---------------------------------------------------------------------------


def read_csv_file(csv_path: Path, cfg: dict) -> dd.DataFrame:
    """Read a single raw production CSV into a Dask DataFrame.

    Raises FileNotFoundError for missing files; ValueError for empty files.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    if csv_path.stat().st_size == 0:
        raise ValueError(f"Empty file: {csv_path}")

    ddf = dd.read_csv(str(csv_path), blocksize="64MB", assume_missing=True)
    before = ddf.npartitions
    ddf = ddf.repartition(npartitions=min(ddf.npartitions, 50))
    _log.info("read_csv_file: %s | partitions %d→%d", csv_path, before, ddf.npartitions)
    return ddf


# ---------------------------------------------------------------------------
# Task 02 — year-range filter
# ---------------------------------------------------------------------------


def filter_by_year(ddf: dd.DataFrame, start_year: int) -> dd.DataFrame:
    """Filter rows where ReportYear >= start_year using map_partitions."""
    if "ReportYear" not in ddf.columns:
        raise KeyError("ReportYear column not found in DataFrame")

    _log.info("filter_by_year: %d partitions, start_year=%d", ddf.npartitions, start_year)

    def _filter(pdf: pd.DataFrame) -> pd.DataFrame:
        return pdf[pdf["ReportYear"].astype(int) >= start_year]

    return ddf.map_partitions(_filter, meta=ddf._meta)


# ---------------------------------------------------------------------------
# Task 03 — schema validator
# ---------------------------------------------------------------------------


def validate_schema(ddf: dd.DataFrame) -> list[str]:
    """Return list of required columns absent from *ddf*. Empty list = OK."""
    missing = [col for col in REQUIRED_COLUMNS if col not in ddf.columns]
    for col in missing:
        _log.warning("Schema validation: missing column '%s'", col)
    return missing


# ---------------------------------------------------------------------------
# Task 04 — multi-file loader
# ---------------------------------------------------------------------------


def load_all_raw_files(raw_dir: Path, cfg: dict) -> dd.DataFrame:
    """Load all CSVs from *raw_dir*, concatenate, filter, and validate."""
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise RuntimeError("No raw CSV files could be loaded")

    loaded: list[dd.DataFrame] = []
    for f in csv_files:
        try:
            loaded.append(read_csv_file(f, cfg))
        except (FileNotFoundError, ValueError, Exception) as exc:
            _log.error("Failed to load %s: %s", f, exc)

    if not loaded:
        raise RuntimeError("No raw CSV files could be loaded")

    combined = dd.concat(loaded, axis=0)
    combined = combined.repartition(npartitions=min(combined.npartitions, 50))

    start_year = int(cfg.get("target_start_year", 2020))
    combined = filter_by_year(combined, start_year)

    missing = validate_schema(combined)
    if missing:
        _log.warning("Combined DataFrame missing columns: %s", missing)

    return combined


# ---------------------------------------------------------------------------
# Task 05 — dtype standardiser
# ---------------------------------------------------------------------------


def standardise_dtypes(ddf: dd.DataFrame) -> dd.DataFrame:
    """Apply stable dtype assignments to required columns via map_partitions."""

    # Build meta with correct dtypes
    meta_dict: dict[str, object] = {}
    for col in ddf.columns:
        if col in _DTYPE_MAP:
            meta_dict[col] = _DTYPE_MAP[col]
        elif col in _STRING_COLS:
            meta_dict[col] = pd.StringDtype()
        else:
            meta_dict[col] = ddf._meta[col].dtype

    meta = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in meta_dict.items()})  # type: ignore[call-overload]

    def _cast(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        for col, target_dtype in _DTYPE_MAP.items():
            if col in out.columns:
                try:
                    out[col] = pd.to_numeric(out[col], errors="coerce").astype(target_dtype)  # type: ignore[call-overload]
                except Exception as exc:  # noqa: BLE001
                    _log.warning("Cannot cast %s to %s: %s", col, target_dtype, exc)
        for col in _STRING_COLS:
            if col in out.columns:
                try:
                    out[col] = out[col].astype(pd.StringDtype())
                except Exception as exc:  # noqa: BLE001
                    _log.warning("Cannot cast %s to StringDtype: %s", col, exc)
        return out

    return ddf.map_partitions(_cast, meta=meta)


# ---------------------------------------------------------------------------
# Task 06 — interim Parquet writer
# ---------------------------------------------------------------------------


def write_interim_parquet(
    ddf: dd.DataFrame,
    interim_dir: Path,
    total_rows_estimate: int,
) -> None:
    """Write *ddf* to *interim_dir* as consolidated Parquet files."""
    npartitions = max(1, min(total_rows_estimate // 500_000, 50))
    interim_dir.mkdir(parents=True, exist_ok=True)
    _log.info("write_interim_parquet: npartitions=%d → %s", npartitions, interim_dir)
    ddf.repartition(npartitions=npartitions).to_parquet(
        str(interim_dir), write_index=False, overwrite=True
    )
    _log.info("Interim Parquet written to %s", interim_dir)


# ---------------------------------------------------------------------------
# Task 07 — ingest orchestrator
# ---------------------------------------------------------------------------


def run_ingest(config_path: str = "config.yaml") -> dd.DataFrame:
    """Run the full ingest stage: CSV → interim Parquet."""
    t0 = datetime.now()
    _log.info("Ingest stage started")

    cfg = load_config(config_path)
    raw_dir = Path(cfg["ingest"]["raw_dir"])
    interim_dir = Path(cfg["ingest"]["interim_dir"])

    ddf = load_all_raw_files(raw_dir, cfg["ingest"])
    ddf = standardise_dtypes(ddf)

    total_rows_estimate: int = int(ddf.shape[0].compute())
    _log.info("Total rows estimate: %d", total_rows_estimate)

    write_interim_parquet(ddf, interim_dir, total_rows_estimate)

    elapsed = (datetime.now() - t0).total_seconds()
    _log.info("Ingest stage complete in %.1fs", elapsed)
    return ddf


if __name__ == "__main__":
    run_ingest()
