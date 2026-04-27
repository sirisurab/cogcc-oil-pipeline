"""Ingest stage: read raw CSVs, enforce schema, write interim Parquet."""

import logging
from pathlib import Path
from typing import cast

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Canonical column order from data dictionary
CANONICAL_COLUMNS: list[str] = [
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

# dtype mapping: (dd_dtype_str, nullable) -> pandas dtype
# "Int64" (capital I) is the data dictionary spelling for nullable integer.
_DTYPE_MAP: dict[tuple[str, bool], object] = {
    ("int", False): "int64",
    ("int", True): pd.Int64Dtype(),
    ("Int64", False): pd.Int64Dtype(),
    ("Int64", True): pd.Int64Dtype(),
    ("float", False): "float64",
    ("float", True): "float64",
    ("string", False): pd.StringDtype(),
    ("string", True): pd.StringDtype(),
    ("bool", False): "bool",
    ("bool", True): pd.BooleanDtype(),
    ("datetime", False): "datetime64[ns]",
    ("datetime", True): "datetime64[ns]",
}

# String values that map to True/False for boolean columns in COGCC source data
_BOOL_TRUE_VALS = {"True", "true", "TRUE", "Y", "y", "1", "yes", "Yes", "YES"}
_BOOL_FALSE_VALS = {"False", "false", "FALSE", "N", "n", "0", "no", "No", "NO"}


def load_data_dictionary(dd_path: str) -> dict:
    """Read production-data-dictionary.csv and return schema dict keyed by column name."""
    import csv

    schema: dict = {}
    with open(dd_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            col = row["column"]
            dtype_str = row["dtype"].strip()
            nullable = row["nullable"].strip().lower() == "yes"
            cats_raw = row.get("categories", "").strip()

            if dtype_str == "categorical":
                cats = [c.strip() for c in cats_raw.split("|") if c.strip()]
                pd_dtype: object = pd.CategoricalDtype(categories=cats, ordered=False)
            else:
                pd_dtype = _DTYPE_MAP.get((dtype_str, nullable))
                if pd_dtype is None:
                    pd_dtype = _DTYPE_MAP.get((dtype_str, False), "object")
                cats = []

            schema[col] = {
                "dtype": pd_dtype,
                "nullable": nullable,
                "categories": cats,
            }
    return schema


def _cast_column(series: pd.Series, dtype: object, nullable: bool) -> pd.Series:
    """Cast a single column to the target dtype with appropriate null handling."""
    if isinstance(dtype, pd.CategoricalDtype):
        cat_dtype = cast(pd.CategoricalDtype, dtype)
        allowed = set(cat_dtype.categories)
        # Replace values outside allowed categories with NA before casting (ADR-003)
        series = series.mask(~series.isin(allowed))
        return series.astype(cat_dtype)

    if dtype == "datetime64[ns]":
        return pd.to_datetime(series, errors="coerce").astype("datetime64[ns]")

    if isinstance(dtype, pd.BooleanDtype):
        # COGCC source stores booleans as strings — map to Python bool first
        mapped = series.map(
            lambda v: (
                True
                if str(v) in _BOOL_TRUE_VALS
                else (False if str(v) in _BOOL_FALSE_VALS else pd.NA)
            )
        )
        return pd.array(mapped.tolist(), dtype=pd.BooleanDtype())  # type: ignore[return-value]

    if isinstance(dtype, pd.StringDtype):
        str_dtype = cast(pd.StringDtype, dtype)
        return series.astype(str_dtype)

    if isinstance(dtype, pd.Int64Dtype):
        int_dtype = cast(pd.Int64Dtype, dtype)
        return pd.to_numeric(series, errors="coerce").astype(int_dtype)

    if dtype == "float64":
        return pd.to_numeric(series, errors="coerce").astype("float64")

    if dtype == "int64":
        return pd.to_numeric(series, errors="coerce").astype("int64")

    if dtype == "bool":
        return series.astype("bool")

    return series.astype("object")


def read_raw_file(file_path: str, data_dict: dict) -> pd.DataFrame:
    """Read a single raw CSV and return a schema-conformant pandas DataFrame."""
    raw_df = pd.read_csv(file_path, dtype=object, low_memory=False)

    present_cols = set(raw_df.columns)

    for col in present_cols - set(data_dict):
        logger.warning("Column '%s' in CSV is not in data dictionary — ignoring", col)

    columns: dict[str, pd.Series] = {}
    for col in CANONICAL_COLUMNS:
        spec = data_dict[col]
        dtype = spec["dtype"]
        nullable = spec["nullable"]

        if col in present_cols:
            columns[col] = _cast_column(raw_df[col], dtype, nullable)
        elif nullable:
            # nullable=yes absent column: add all-NA column at correct dtype
            n = len(raw_df)
            if isinstance(dtype, pd.CategoricalDtype):
                arr = pd.Categorical([None] * n, categories=dtype.categories)
                columns[col] = pd.Series(arr, dtype=dtype)
            elif isinstance(dtype, pd.StringDtype):
                columns[col] = pd.Series(pd.array([pd.NA] * n, dtype=dtype))
            elif isinstance(dtype, pd.BooleanDtype):
                columns[col] = pd.Series(pd.array([pd.NA] * n, dtype=dtype))
            elif isinstance(dtype, pd.Int64Dtype):
                columns[col] = pd.Series(pd.array([pd.NA] * n, dtype=dtype))
            elif dtype == "float64":
                columns[col] = pd.Series([np.nan] * n, dtype="float64")
            elif dtype == "datetime64[ns]":
                columns[col] = pd.Series([pd.NaT] * n, dtype="datetime64[ns]")
            else:
                columns[col] = pd.Series([None] * n, dtype=dtype)  # type: ignore[arg-type]
        else:
            raise ValueError(
                f"Required column '{col}' (nullable=no) is absent from '{file_path}'"
            )

    result = pd.DataFrame(columns)[CANONICAL_COLUMNS]
    return result


def build_dask_dataframe(file_paths: list[str], data_dict: dict) -> dd.DataFrame:
    """Build a lazy Dask DataFrame from delayed per-file reads.

    Meta is derived by calling read_raw_file on the first available file and
    slicing to zero rows (ADR-003).
    """
    delayed_frames = [dask.delayed(read_raw_file)(fp, data_dict) for fp in file_paths]
    # ADR-003: derive meta by calling actual function on minimal real input
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
    return dd.from_delayed(delayed_frames, meta=meta)


def filter_year(ddf: dd.DataFrame, min_year: int) -> dd.DataFrame:
    """Return lazy Dask DataFrame filtered to ReportYear >= min_year."""
    return ddf[ddf["ReportYear"] >= min_year]


def write_parquet(ddf: dd.DataFrame, output_dir: str) -> None:
    """Repartition and write Dask DataFrame to partitioned Parquet (ADR-004)."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    n = ddf.npartitions
    n_out = max(10, min(n, 50))
    # Repartition is the last operation before write (ADR-004)
    ddf.repartition(npartitions=n_out).to_parquet(str(out), write_index=False)


def ingest(config: dict) -> None:
    """Orchestrate the full ingest stage."""
    ingest_cfg: dict = config.get("ingest", config)
    raw_dir = Path(ingest_cfg["raw_dir"])
    interim_dir = str(ingest_cfg["interim_dir"])
    dd_path = str(ingest_cfg["data_dictionary"])
    min_year: int = int(ingest_cfg["min_year"])

    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory does not exist: {raw_dir}")

    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files found in %s — skipping ingest", raw_dir)
        return

    file_paths = [str(p) for p in csv_files]
    data_dict = load_data_dictionary(dd_path)

    # Build lazy graph (ADR-005)
    ddf = build_dask_dataframe(file_paths, data_dict)
    ddf = filter_year(ddf, min_year)

    # Single compute trigger: Parquet write
    write_parquet(ddf, interim_dir)
    logger.info("Ingest complete — wrote interim Parquet to %s", interim_dir)
