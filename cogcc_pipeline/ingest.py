"""Ingest stage: read raw COGCC CSV files, enforce schema, write Parquet to data/interim/."""

import logging
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd

from cogcc_pipeline.utils import get_partition_count, load_config

logger = logging.getLogger(__name__)

_REQUIRED_DICT_COLUMNS = {"column", "description", "dtype", "nullable", "categories"}
_DICT_PATH = "references/production-data-dictionary.csv"


# ---------------------------------------------------------------------------
# ING-01: Data dictionary loader
# ---------------------------------------------------------------------------


def load_data_dictionary(dict_path: str) -> dict[str, dict]:
    """Read the production data dictionary CSV and return a column-keyed lookup dict.

    Returns:
        Dict keyed by column name, each value containing:
          - dtype: str token
          - nullable: bool
          - categories: list[str]

    Raises:
        FileNotFoundError: if dict_path does not exist.
        ValueError: if the CSV is missing required header columns.
    """
    path = Path(dict_path)
    if not path.exists():
        raise FileNotFoundError(f"Data dictionary not found: {dict_path}")

    df = pd.read_csv(path)
    missing = _REQUIRED_DICT_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Data dictionary CSV missing required columns: {missing}")

    result: dict[str, dict] = {}
    for _, row in df.iterrows():
        cats_raw = str(row["categories"]) if pd.notna(row["categories"]) else ""
        categories = [c.strip() for c in cats_raw.split("|") if c.strip()] if cats_raw else []
        result[str(row["column"])] = {
            "dtype": str(row["dtype"]),
            "nullable": str(row["nullable"]).strip().lower() == "yes",
            "categories": categories,
        }
    return result


# ---------------------------------------------------------------------------
# ING-02: dtype mapper
# ---------------------------------------------------------------------------


def map_dtype(dtype_token: str, nullable: bool, categories: list[str]) -> object:
    """Map a data-dictionary dtype token to the correct pandas dtype object.

    Raises:
        ValueError: if dtype_token is unknown.
    """
    if dtype_token in ("int", "Int64"):
        if nullable or dtype_token == "Int64":
            return pd.Int64Dtype()
        return np.dtype("int64")
    if dtype_token == "float":
        return np.dtype("float64")
    if dtype_token == "string":
        return pd.StringDtype()
    if dtype_token == "datetime":
        # Loaded as string, converted post-load
        return "datetime64[ns]"
    if dtype_token == "bool":
        return pd.BooleanDtype()
    if dtype_token == "categorical":
        return pd.CategoricalDtype(categories=categories, ordered=False)
    raise ValueError(f"Unknown dtype token: {dtype_token!r}")


# ---------------------------------------------------------------------------
# ING-03: Schema enforcer (partition-level)
# ---------------------------------------------------------------------------


def enforce_schema(df: pd.DataFrame, data_dict: dict[str, dict]) -> pd.DataFrame:
    """Enforce the canonical schema on a raw partition DataFrame.

    Called via ddf.map_partitions(enforce_schema, data_dict). Must be pure and stateless.

    Raises:
        KeyError: if a non-nullable column is absent from df.
    """
    # Step 1: Absent column handling
    for col, meta in data_dict.items():
        if col not in df.columns:
            if not meta["nullable"]:
                raise KeyError(f"Non-nullable column '{col}' is absent from the source DataFrame")
            # Add nullable column as all-NA
            pd_dtype = map_dtype(meta["dtype"], meta["nullable"], meta["categories"])
            if pd_dtype == "datetime64[ns]":
                df[col] = pd.NaT
            elif isinstance(pd_dtype, pd.CategoricalDtype):
                df[col] = pd.Categorical([None] * len(df), categories=meta["categories"])
            else:
                df[col] = np.nan

    # Step 2: Datetime columns
    if "AcceptedDate" in df.columns:
        df["AcceptedDate"] = pd.to_datetime(df["AcceptedDate"], errors="coerce")

    # Step 3: Categorical columns
    if "WellStatus" in df.columns:
        cat_meta = data_dict["WellStatus"]
        valid_cats = set(cat_meta["categories"])
        ws_series = df["WellStatus"].astype(object)  # ensure object type for isin
        ws = ws_series.where(ws_series.isin(valid_cats), other=None)  # type: ignore[call-overload]
        df["WellStatus"] = pd.Categorical(ws, categories=cat_meta["categories"])

    # Step 4: Cast non-datetime, non-categorical columns
    for col, meta in data_dict.items():
        if col in ("AcceptedDate", "WellStatus"):
            continue
        if col not in df.columns:
            continue
        pd_dtype = map_dtype(meta["dtype"], meta["nullable"], meta["categories"])
        if pd_dtype == "datetime64[ns]":
            continue
        try:
            if isinstance(pd_dtype, pd.Int64Dtype):
                numeric = pd.to_numeric(df[col], errors="coerce")
                df[col] = pd.array(numeric, dtype=pd_dtype)  # type: ignore[call-overload]
            elif isinstance(pd_dtype, pd.BooleanDtype):
                # Map common string values to bool
                s = df[col].astype(str).str.strip().str.lower()
                mapped = s.map(
                    {
                        "true": True,
                        "false": False,
                        "1": True,
                        "0": False,
                        "yes": True,
                        "no": False,
                        "nan": pd.NA,
                        "none": pd.NA,
                    }
                )
                df[col] = pd.array(mapped, dtype=pd_dtype)  # type: ignore[call-overload]
            elif isinstance(pd_dtype, pd.StringDtype):
                df[col] = df[col].astype(pd_dtype)
            else:
                # Standard numpy dtypes (int64, float64) — coerce numeric first
                numeric = pd.to_numeric(df[col], errors="coerce")
                df[col] = numeric.astype(pd_dtype)  # type: ignore[call-overload]
        except Exception:
            if meta["nullable"]:
                try:
                    df[col] = pd.array(  # type: ignore[call-overload]
                        pd.to_numeric(df[col], errors="coerce"),
                        dtype=pd_dtype,
                    )
                except Exception:
                    df[col] = pd.array([pd.NA] * len(df), dtype=pd_dtype)  # type: ignore[call-overload]
            else:
                raise

    # Step 5: Select and order columns per data dictionary
    ordered_cols = [c for c in data_dict if c in df.columns]
    return df[ordered_cols]


# ---------------------------------------------------------------------------
# ING-04: Raw CSV reader
# ---------------------------------------------------------------------------


def read_raw_csv(file_path: Path, data_dict: dict[str, dict]) -> dd.DataFrame:
    """Read a single raw CSV file into a Dask DataFrame with schema enforcement.

    Returns a lazy Dask DataFrame — does not call .compute().

    Raises:
        FileNotFoundError: if file_path does not exist.
        KeyError: from enforce_schema for missing non-nullable columns.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Raw CSV not found: {file_path}")

    ddf = dd.read_csv(str(file_path), dtype=str, assume_missing=True)

    # Derive meta by running enforce_schema on the empty meta DataFrame
    meta = enforce_schema(ddf._meta.copy(), data_dict)

    ddf = ddf.map_partitions(enforce_schema, data_dict, meta=meta)

    # Apply year filter lazily
    ddf = ddf[ddf["ReportYear"] >= 2020]

    return ddf


# ---------------------------------------------------------------------------
# ING-05: Multi-file ingest runner
# ---------------------------------------------------------------------------


def run_ingest(config_path: str = "config/config.yaml") -> None:
    """Top-level entry point for the ingest stage.

    Reads all CSV files from data/raw/, concatenates into a Dask DataFrame,
    enforces schema, filters to ReportYear >= 2020, and writes Parquet to data/interim/.
    """
    config = load_config(config_path)
    ing_cfg = config["ingest"]

    raw_dir = Path(ing_cfg["raw_dir"])
    interim_dir = Path(ing_cfg["interim_dir"])

    data_dict = load_data_dictionary(_DICT_PATH)

    csv_files = list(raw_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files found in %s — ingest skipped", raw_dir)
        return

    dfs: list[dd.DataFrame] = []
    for csv_file in csv_files:
        try:
            ddf = read_raw_csv(csv_file, data_dict)
            dfs.append(ddf)
        except KeyError as exc:
            logger.error("Schema error in %s: %s", csv_file.name, exc)
            raise

    combined = dd.concat(dfs, ignore_unknown_divisions=True)

    n = len(csv_files)
    n_partitions = get_partition_count(n)
    combined = combined.repartition(npartitions=n_partitions)

    interim_dir.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(str(interim_dir), write_index=False, overwrite=True, schema="infer")

    logger.info(
        "Ingest complete: %d CSV files processed → %d partitions in %s",
        n,
        n_partitions,
        interim_dir,
    )
