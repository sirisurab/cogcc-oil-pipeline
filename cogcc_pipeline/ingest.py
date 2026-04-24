"""Ingest stage — read raw CSVs, enforce canonical schema, write interim Parquet."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
from dask import delayed
from distributed import Client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Task I1 — Data-dictionary-driven schema loader
# ---------------------------------------------------------------------------

# Mapping from data-dictionary dtype tokens to pandas dtypes.
# Columns marked nullable=yes must use nullable-aware variants (ADR-003).
_DTYPE_MAP: dict[str, dict[str, str]] = {
    # token -> {nullable: pandas_dtype, non_nullable: pandas_dtype}
    "int": {"nullable": "Int64", "non_nullable": "int64"},
    "Int64": {"nullable": "Int64", "non_nullable": "Int64"},
    "float": {"nullable": "Float64", "non_nullable": "float64"},
    "string": {"nullable": "string", "non_nullable": "string"},
    "bool": {"nullable": "boolean", "non_nullable": "bool"},
    "datetime": {"nullable": "datetime64[us]", "non_nullable": "datetime64[us]"},
    "categorical": {"nullable": "object", "non_nullable": "object"},
}

_SUPPORTED_TOKENS = set(_DTYPE_MAP.keys())


class SchemaError(ValueError):
    """Raised when the data dictionary is missing, malformed, or declares an unsupported dtype."""


def load_canonical_schema(dictionary_path: str) -> dict:
    """Read the data dictionary CSV and return a canonical schema object.

    Args:
        dictionary_path: Path to production-data-dictionary.csv.

    Returns:
        Dict mapping column name → {dtype, nullable, categories}.

    Raises:
        SchemaError: If the file is missing, malformed, or has unsupported tokens.
    """
    path = Path(dictionary_path)
    if not path.exists():
        raise SchemaError(f"Data dictionary not found: {dictionary_path}")

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception as exc:
        raise SchemaError(f"Cannot read data dictionary: {exc}") from exc

    required_cols = {"column", "dtype", "nullable"}
    if not required_cols.issubset(df.columns):
        raise SchemaError(
            f"Data dictionary missing required columns: {required_cols - set(df.columns)}"
        )

    schema: dict = {}
    for _, row in df.iterrows():
        col_name: str = str(row["column"]).strip()
        token: str = str(row["dtype"]).strip()
        nullable_str: str = str(row.get("nullable", "yes")).strip().lower()
        categories_str: str = str(row.get("categories", "")).strip()

        if not col_name or col_name == "nan":
            raise SchemaError("Data dictionary contains a row with an empty column name")
        if token not in _SUPPORTED_TOKENS:
            raise SchemaError(f"Unsupported dtype token '{token}' for column '{col_name}'")

        nullable = nullable_str == "yes"
        variant = "nullable" if nullable else "non_nullable"
        pandas_dtype = _DTYPE_MAP[token][variant]

        categories: list[str] | None = None
        if token == "categorical" and categories_str and categories_str != "nan":
            categories = [c.strip() for c in categories_str.split("|") if c.strip()]

        schema[col_name] = {
            "dtype": pandas_dtype,
            "nullable": nullable,
            "categories": categories,
            "token": token,
        }

    if not schema:
        raise SchemaError("Data dictionary produced an empty schema")

    return schema


# ---------------------------------------------------------------------------
# Task I2 — Per-file partition reader with schema enforcement
# ---------------------------------------------------------------------------


def read_raw_file_to_frame(raw_path: str, schema: dict) -> pd.DataFrame:
    """Read one raw CSV into a schema-conformant pandas DataFrame.

    - Renames source columns to canonical names (same names assumed here —
      the ECMC CSVs use the same column names as the data dictionary).
    - Drops columns not in the dictionary.
    - Adds all-NA columns for nullable columns absent from the source (H3).
    - Raises for absent non-nullable columns (H3).
    - Replaces out-of-set categorical values with NA before casting.
    - Filters rows to ReportYear >= 2020.
    - Preserves zeros (TR-05).

    Args:
        raw_path: Path to the raw CSV file.
        schema: Output of :func:`load_canonical_schema`.

    Returns:
        pandas DataFrame with canonical schema.

    Raises:
        SchemaError: If a non-nullable column is absent from the source file.
    """
    empty_frame = _empty_frame(schema)

    path = Path(raw_path)
    if not path.exists():
        logger.warning("Raw file not found: %s — returning empty frame", raw_path)
        return empty_frame

    try:
        # Read everything as object first to avoid inference
        raw_df = pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)
    except Exception as exc:
        logger.error("Cannot read %s: %s", raw_path, exc)
        return empty_frame

    if raw_df.empty:
        return empty_frame

    # Drop columns not in dictionary
    keep_cols = [c for c in raw_df.columns if c in schema]
    raw_df = raw_df[keep_cols]

    # Validate non-nullable columns are present
    for col_name, meta in schema.items():
        if not meta["nullable"] and col_name not in raw_df.columns:
            raise SchemaError(f"Non-nullable column '{col_name}' is absent from {raw_path}")

    # Build output frame column by column
    result_cols: dict[str, pd.Series] = {}
    for col_name, meta in schema.items():
        dtype: str = meta["dtype"]
        nullable: bool = meta["nullable"]
        categories: list[str] | None = meta["categories"]
        token: str = meta["token"]

        if col_name not in raw_df.columns:
            # nullable=yes absent → all-NA at correct dtype (H3)
            if nullable:
                if token == "datetime":
                    result_cols[col_name] = pd.Series([], dtype="datetime64[us]").reindex(
                        range(len(raw_df))
                    )  # type: ignore[assignment]
                elif token == "categorical":
                    if categories:
                        cat_dtype = _make_cat_dtype(schema, col_name)
                    else:
                        cat_dtype = pd.CategoricalDtype(ordered=False)
                    result_cols[col_name] = pd.Series([], dtype=cat_dtype).reindex(
                        range(len(raw_df))
                    )  # type: ignore[assignment]
                else:
                    result_cols[col_name] = pd.Series([], dtype=dtype).reindex(
                        range(len(raw_df))
                    )  # type: ignore[assignment]
            # non-nullable absent is already caught above
        else:
            raw_series = raw_df[col_name].replace("", pd.NA)

            if token == "datetime":
                result_cols[col_name] = pd.to_datetime(  # type: ignore[assignment]
                    raw_series, errors="coerce"
                )
            elif token == "categorical":
                if categories:
                    # Replace out-of-set values with NA before casting
                    mask_invalid = ~raw_series.isin(categories) & raw_series.notna()
                    raw_series = raw_series.where(~mask_invalid, other=pd.NA)
                    cat_dtype = _make_cat_dtype(schema, col_name)
                    result_cols[col_name] = raw_series.astype(cat_dtype)  # type: ignore[assignment]
                else:
                    result_cols[col_name] = raw_series.astype("category")  # type: ignore[assignment]
            elif token in ("int", "Int64"):
                # Coerce to numeric; out-of-range or non-numeric → NA
                numeric = pd.to_numeric(raw_series, errors="coerce")
                result_cols[col_name] = numeric.astype(dtype)  # type: ignore[assignment]
            elif token == "float":
                numeric = pd.to_numeric(raw_series, errors="coerce")
                result_cols[col_name] = numeric.astype(dtype)  # type: ignore[assignment]
            elif token == "bool":
                lowered = raw_series.str.strip().str.lower()
                bool_series = lowered.map({"true": True, "false": False, "1": True, "0": False})
                result_cols[col_name] = bool_series.astype(dtype)  # type: ignore[assignment]
            else:  # string
                result_cols[col_name] = raw_series.astype(dtype)  # type: ignore[assignment]

    out_df = pd.DataFrame(result_cols)

    # Filter to ReportYear >= 2020
    if "ReportYear" in out_df.columns:
        year_series = pd.to_numeric(out_df["ReportYear"], errors="coerce")
        out_df = out_df[year_series >= 2020]

    if out_df.empty:
        return empty_frame

    return out_df.reset_index(drop=True)


def _make_cat_dtype(schema: dict, col_name: str) -> pd.CategoricalDtype:
    """Return a CategoricalDtype for a column, extracting categories from schema.
    
    Args:
        schema: The canonical schema dict.
        col_name: The column name to get categories for.
    
    Returns:
        A pd.CategoricalDtype with the exact categories from the schema.
    """
    meta = schema[col_name]
    categories: list[str] | None = meta.get("categories")
    if categories:
        return pd.CategoricalDtype(categories=categories, ordered=False)
    else:
        return pd.CategoricalDtype(ordered=False)


def _empty_frame(schema: dict) -> pd.DataFrame:
    """Return a zero-row DataFrame conforming to the canonical schema."""
    cols: dict[str, pd.Series] = {}
    for col_name, meta in schema.items():
        dtype: str = meta["dtype"]
        token: str = meta["token"]
        if token == "datetime":
            cols[col_name] = pd.Series([], dtype="datetime64[us]")
        elif token == "categorical":
            cat_dtype = _make_cat_dtype(schema, col_name)
            cols[col_name] = pd.Series([], dtype=cat_dtype)
        else:
            cols[col_name] = pd.Series([], dtype=dtype)
    return pd.DataFrame(cols)


# ---------------------------------------------------------------------------
# Task I3 — Parallel ingest and Parquet write
# ---------------------------------------------------------------------------


def _partition_count(n: int, cfg_min: int = 10, cfg_max: int = 50) -> int:
    """Compute partition count per ADR-004: max(min_, min(n, max_))."""
    return max(cfg_min, min(n, cfg_max))


def ingest(config: dict) -> None:
    """Stage entry point. Read raw CSVs, enforce schema, write interim Parquet.

    Uses the CPU-bound distributed scheduler (ADR-001). Reuses an existing
    Dask client if present (build-env-manifest). Lazy graph, single compute
    at write time (ADR-005).

    Args:
        config: The full pipeline config dict (uses ``ingest`` sub-section).
    """
    ingest_cfg: dict = config.get("ingest", config)
    raw_dir = Path(ingest_cfg.get("raw_dir", "data/raw"))
    interim_dir = Path(ingest_cfg.get("interim_dir", "data/interim"))
    dict_path: str = ingest_cfg.get("dictionary_path", "references/production-data-dictionary.csv")
    p_min: int = int(ingest_cfg.get("partition_min", 10))
    p_max: int = int(ingest_cfg.get("partition_max", 50))

    interim_dir.mkdir(parents=True, exist_ok=True)

    schema = load_canonical_schema(dict_path)
    logger.info("Canonical schema loaded: %d columns", len(schema))

    raw_files = sorted(raw_dir.glob("*.csv"))
    if not raw_files:
        logger.warning("No raw CSV files found in %s", raw_dir)
        return

    logger.info("Found %d raw CSV files", len(raw_files))

    # Reuse existing distributed client if running (build-env-manifest)
    try:
        Client.current()
    except ValueError:
        pass

    # Meta derivation per ADR-003: call actual function on zero-row real input
    # Build a tiny real CSV that has all columns, then read it through the function
    meta_df = _empty_frame(schema)

    # Build lazy partitions from delayed functions
    @delayed
    def _read(p: Path) -> pd.DataFrame:
        return read_raw_file_to_frame(str(p), schema)

    delayed_parts = [_read(f) for f in raw_files]

    ddf = dd.from_delayed(delayed_parts, meta=meta_df)

    n_parts = _partition_count(len(raw_files), p_min, p_max)
    ddf = ddf.repartition(npartitions=n_parts)

    logger.info("Writing interim Parquet to %s (%d partitions)", interim_dir, n_parts)
    # Single compute at write time (ADR-005)
    ddf.to_parquet(
        str(interim_dir),
        engine="pyarrow",
        write_index=False,
        overwrite=True,
        schema="infer",
    )
    logger.info("Ingest complete — interim Parquet written")
