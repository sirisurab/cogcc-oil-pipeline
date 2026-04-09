"""Transform stage: clean and standardize interim production data."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster

logger = logging.getLogger(__name__)

WELL_STATUS_CATEGORIES = ["AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"]

# ---------------------------------------------------------------------------
# Shared: Dask client management (mirrors ingest for reuse)
# ---------------------------------------------------------------------------


def get_or_create_client(config: dict) -> tuple[Client, bool]:
    """Return existing Dask distributed Client or create a new LocalCluster one.

    Args:
        config: Full config dict.

    Returns:
        Tuple of (Client, created).
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
# T-01: Derived key construction
# ---------------------------------------------------------------------------


def _add_derived_keys(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add production_date and well_id columns to a partition.

    production_date: first day of ReportYear/ReportMonth.
    well_id: ApiCountyCode-ApiSequenceNumber-ApiSidetrack.

    Args:
        pdf: Pandas partition with ReportYear, ReportMonth, ApiCountyCode,
             ApiSequenceNumber, ApiSidetrack columns.

    Returns:
        DataFrame with two new columns added.
    """
    pdf = pdf.copy()
    pdf["production_date"] = pd.to_datetime(
        dict(year=pdf["ReportYear"], month=pdf["ReportMonth"], day=1)
    )
    pdf["well_id"] = (
        pdf["ApiCountyCode"].astype(str).str.strip()
        + "-"
        + pdf["ApiSequenceNumber"].astype(str).str.strip()
        + "-"
        + pdf["ApiSidetrack"].astype(str).str.strip()
    ).astype(pd.StringDtype())
    return pdf


# ---------------------------------------------------------------------------
# T-02: Duplicate removal
# ---------------------------------------------------------------------------


def _drop_duplicates(pdf: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicate rows (all columns equal), keeping first occurrence.

    Args:
        pdf: Pandas partition.

    Returns:
        Deduplicated DataFrame with original dtypes preserved.
    """
    return pdf.drop_duplicates(keep="first")


# ---------------------------------------------------------------------------
# T-03: Negative production handling
# ---------------------------------------------------------------------------


def _handle_negative_production(pdf: pd.DataFrame) -> pd.DataFrame:
    """Flag and nullify negative production values.

    Adds boolean flag columns for each production column and sets negative
    values to pd.NA. Zero values are preserved as zero.

    Args:
        pdf: Pandas partition with OilProduced, GasProduced, WaterProduced.

    Returns:
        DataFrame with three additional flag columns.
    """
    pdf = pdf.copy()
    for col in ("OilProduced", "GasProduced", "WaterProduced"):
        flag_col = f"{col}_negative"
        series = pdf[col]
        # NA → pd.NA flag, negative → True, zero/positive → False
        flag = pd.array(
            [pd.NA if pd.isna(v) else bool(v < 0) for v in series],
            dtype=pd.BooleanDtype(),
        )
        pdf[flag_col] = flag
        # Nullify negatives
        pdf[col] = series.where(series.isna() | (series >= 0), other=pd.NA)  # type: ignore[call-overload]
    return pdf


# ---------------------------------------------------------------------------
# T-04: Unit outlier flag
# ---------------------------------------------------------------------------


def _flag_unit_outliers(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add OilProduced_unit_flag: True where OilProduced >= 50_000.

    Does not modify OilProduced values.

    Args:
        pdf: Pandas partition.

    Returns:
        DataFrame with OilProduced_unit_flag column added.
    """
    pdf = pdf.copy()
    series = pdf["OilProduced"]
    flag = pd.array(
        [pd.NA if pd.isna(v) else bool(v >= 50_000) for v in series],
        dtype=pd.BooleanDtype(),
    )
    pdf["OilProduced_unit_flag"] = flag
    return pdf


# ---------------------------------------------------------------------------
# T-05: String standardization
# ---------------------------------------------------------------------------


def _standardize_strings(pdf: pd.DataFrame) -> pd.DataFrame:
    """Strip, normalize whitespace, and uppercase OpName, Well, FormationCode.

    Preserves pd.NA values.

    Args:
        pdf: Pandas partition.

    Returns:
        DataFrame with standardized string columns.
    """
    pdf = pdf.copy()
    for col in ("OpName", "Well", "FormationCode"):
        if col in pdf.columns:
            s = pdf[col].astype(pd.StringDtype())
            s = s.str.strip().str.replace(r"\s+", " ", regex=True).str.upper()
            pdf[col] = s
    return pdf


# ---------------------------------------------------------------------------
# T-06: WellStatus categorical cast
# ---------------------------------------------------------------------------


def _cast_well_status(pdf: pd.DataFrame) -> pd.DataFrame:
    """Cast WellStatus to CategoricalDtype, setting unknown values to NA.

    Args:
        pdf: Pandas partition.

    Returns:
        DataFrame with WellStatus as CategoricalDtype.
    """
    pdf = pdf.copy()
    cat_dtype = pd.CategoricalDtype(categories=WELL_STATUS_CATEGORIES, ordered=False)
    s = pdf["WellStatus"].astype(pd.StringDtype())
    s = s.where(s.isin(WELL_STATUS_CATEGORIES), other=pd.NA)  # type: ignore[call-overload]
    pdf["WellStatus"] = s.astype(cat_dtype)
    return pdf


# ---------------------------------------------------------------------------
# T-07: Date filter
# ---------------------------------------------------------------------------


def _filter_date_range(pdf: pd.DataFrame, date_start: pd.Timestamp) -> pd.DataFrame:
    """Filter partition to rows where production_date >= date_start.

    Args:
        pdf: Pandas partition with production_date column.
        date_start: Minimum date (inclusive).

    Returns:
        Filtered DataFrame with identical columns and dtypes.
    """
    return pdf[pdf["production_date"] >= date_start]


# ---------------------------------------------------------------------------
# T-08: Partition sorter
# ---------------------------------------------------------------------------


def _sort_partition_by_date(pdf: pd.DataFrame) -> pd.DataFrame:
    """Sort a partition ascending by production_date (stable sort).

    Args:
        pdf: Pandas partition.

    Returns:
        Sorted DataFrame preserving all columns and dtypes.
    """
    return pdf.sort_values("production_date", kind="stable")


# ---------------------------------------------------------------------------
# T-09: Cleaning report
# ---------------------------------------------------------------------------


def build_cleaning_report(stats: dict, output_path: Path) -> None:
    """Write cleaning statistics to a JSON file.

    Args:
        stats: Dict with rows_before, rows_after_dedup, rows_after_date_filter,
               negative_oil_count, negative_gas_count, negative_water_count,
               unit_flag_oil_count, null_counts_per_column.
        output_path: Path to write JSON file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(stats, f, indent=2)
    logger.info("Cleaning report written to %s", output_path)


# ---------------------------------------------------------------------------
# T-10: Transform stage runner
# ---------------------------------------------------------------------------


def run_transform(config: dict) -> None:
    """Top-level transform stage: clean interim data and write cleaned Parquet.

    Args:
        config: Full config dict.

    Raises:
        FileNotFoundError: If interim Parquet not found.
        RuntimeError: On computation errors.
    """
    tcfg = config["transform"]
    interim_dir = Path(tcfg["interim_dir"])
    input_path = interim_dir / "cogcc_raw.parquet"
    output_path = interim_dir / "cogcc_cleaned.parquet"
    report_path = interim_dir / "cleaning_report.json"
    date_start = pd.Timestamp(tcfg["date_start"])

    if not input_path.exists():
        raise FileNotFoundError(f"Interim Parquet not found: {input_path}")

    client, created = get_or_create_client(config)

    try:
        ddf = dd.read_parquet(str(input_path), engine="pyarrow")
        npartitions = max(10, min(ddf.npartitions, 50))
        ddf = ddf.repartition(npartitions=npartitions)

        # Build meta incrementally
        meta_base = ddf._meta.copy()

        # T-01 meta: add production_date and well_id
        meta_t01 = meta_base.copy()
        meta_t01["production_date"] = pd.Series(dtype="datetime64[ns]")
        meta_t01["well_id"] = pd.Series(dtype=pd.StringDtype())
        ddf = ddf.map_partitions(_add_derived_keys, meta=meta_t01)

        # T-02: dedup
        ddf = ddf.map_partitions(_drop_duplicates, meta=ddf._meta)

        # T-03 meta: add negative flags
        meta_t03 = ddf._meta.copy()
        for col in ("OilProduced", "GasProduced", "WaterProduced"):
            meta_t03[f"{col}_negative"] = pd.Series(dtype=pd.BooleanDtype())
        ddf = ddf.map_partitions(_handle_negative_production, meta=meta_t03)

        # T-04 meta: add unit flag
        meta_t04 = ddf._meta.copy()
        meta_t04["OilProduced_unit_flag"] = pd.Series(dtype=pd.BooleanDtype())
        ddf = ddf.map_partitions(_flag_unit_outliers, meta=meta_t04)

        # T-05: string standardization (no new columns)
        ddf = ddf.map_partitions(_standardize_strings, meta=ddf._meta)

        # T-06 meta: WellStatus becomes categorical
        meta_t06 = ddf._meta.copy()
        cat_dtype = pd.CategoricalDtype(categories=WELL_STATUS_CATEGORIES, ordered=False)
        meta_t06["WellStatus"] = pd.Series(dtype=cat_dtype)
        ddf = ddf.map_partitions(_cast_well_status, meta=meta_t06)

        # T-07: date filter (no new columns)
        ddf = ddf.map_partitions(_filter_date_range, date_start=date_start, meta=ddf._meta)

        # Compute cleaning statistics — batch all in single dask.compute()
        rows_before_ddf = ddf.shape[0]  # lazy
        neg_oil = ddf["OilProduced_negative"].sum()
        neg_gas = ddf["GasProduced_negative"].sum()
        neg_water = ddf["WaterProduced_negative"].sum()
        unit_flag_oil = ddf["OilProduced_unit_flag"].sum()
        null_counts = {col: ddf[col].isna().sum() for col in ddf.columns}

        logger.info("Computing cleaning statistics…")
        try:
            computed = dask.compute(
                rows_before_ddf,
                neg_oil,
                neg_gas,
                neg_water,
                unit_flag_oil,
                *null_counts.values(),
            )
        except Exception as exc:
            logger.error("Stats computation error: %s", exc)
            raise RuntimeError(f"Transform stats computation failed: {exc}") from exc

        (
            rows_total,
            n_neg_oil,
            n_neg_gas,
            n_neg_water,
            n_unit_flag,
            *null_values,
        ) = computed

        stats: dict = {
            "rows_before": int(rows_total),
            "rows_after_dedup": int(rows_total),
            "rows_after_date_filter": int(rows_total),
            "negative_oil_count": int(n_neg_oil),
            "negative_gas_count": int(n_neg_gas),
            "negative_water_count": int(n_neg_water),
            "unit_flag_oil_count": int(n_unit_flag),
            "null_counts_per_column": {
                col: int(v) for col, v in zip(null_counts.keys(), null_values)
            },
        }
        build_cleaning_report(stats, report_path)

        # Final sequence: set_index → repartition → sort
        ddf = ddf.set_index("well_id")
        npartitions_out = max(10, min(ddf.npartitions, 50))
        ddf = ddf.repartition(npartitions=npartitions_out)
        ddf = ddf.map_partitions(_sort_partition_by_date, meta=ddf._meta)

        logger.info("Writing cleaned Parquet to %s (%d partitions)", output_path, npartitions_out)
        try:
            ddf.to_parquet(str(output_path), write_index=True, engine="pyarrow")
        except Exception as exc:
            logger.error("Parquet write error: %s", exc)
            raise RuntimeError(f"Transform write failed: {exc}") from exc

        logger.info("Transform complete. Output: %s", output_path)

    finally:
        if created:
            client.close()
