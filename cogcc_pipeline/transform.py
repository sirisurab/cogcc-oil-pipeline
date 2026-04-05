"""Transform stage — clean, enrich, and validate interim Parquet data."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from cogcc_pipeline.acquire import get_logger, load_config

_log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Column rename map (raw → snake_case)
# ---------------------------------------------------------------------------

RENAME_MAP: dict[str, str] = {
    "ApiCountyCode": "county_code",
    "ApiSequenceNumber": "api_seq",
    "ReportYear": "report_year",
    "ReportMonth": "report_month",
    "OilProduced": "oil_bbl",
    "OilSales": "oil_sales_bbl",
    "GasProduced": "gas_mcf",
    "GasSales": "gas_sales_mcf",
    "FlaredVented": "gas_flared_mcf",
    "GasUsedOnLease": "gas_lease_mcf",
    "WaterProduced": "water_bbl",
    "DaysProduced": "days_produced",
    "Well": "well_name",
    "OpName": "operator_name",
    "OpNum": "operator_num",
    "DocNum": "doc_num",
}

# Columns that must exist after rename before downstream tasks can run
_VALIDITY_REQUIRED = {
    "oil_bbl",
    "gas_mcf",
    "water_bbl",
    "days_produced",
    "production_date",
    "well_id",
}


# ---------------------------------------------------------------------------
# Task 01 — column renamer
# ---------------------------------------------------------------------------


def rename_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """Rename raw column names to snake_case per RENAME_MAP."""
    present = {k: v for k, v in RENAME_MAP.items() if k in ddf.columns}
    _log.debug("rename_columns: renaming %d columns", len(present))
    return ddf.rename(columns=present)


# ---------------------------------------------------------------------------
# Task 02 — well identifier builder
# ---------------------------------------------------------------------------


def build_well_id(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add composite well_id column: county_code.zfill(2) + '-' + api_seq.zfill(5)."""
    # Build meta including well_id
    meta = ddf._meta.copy()
    meta["well_id"] = pd.Series(dtype=pd.StringDtype())

    def _add_well_id(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        county = out["county_code"].astype(str).str.zfill(2)
        api = out["api_seq"].astype(str).str.zfill(5)
        out["well_id"] = (county + "-" + api).where(  # type: ignore[call-overload]
            out["county_code"].notna() & out["api_seq"].notna(), other=None
        )
        out["well_id"] = out["well_id"].astype(pd.StringDtype())
        null_frac = out["well_id"].isna().mean()
        if null_frac > 0.01:
            _log.warning("build_well_id: %.1f%% null well_id values", null_frac * 100)
        return out

    return ddf.map_partitions(_add_well_id, meta=meta)


# ---------------------------------------------------------------------------
# Task 03 — production date constructor
# ---------------------------------------------------------------------------


def build_production_date(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add production_date (datetime64[ns]) from report_year + report_month."""
    meta = ddf._meta.copy()
    meta["production_date"] = pd.Series(dtype="datetime64[ns]")

    def _add_date(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        date_str = (
            out["report_year"].astype(str)
            + "-"
            + out["report_month"].astype(str).str.zfill(2)
            + "-01"
        )
        out["production_date"] = pd.to_datetime(date_str, format="%Y-%m-%d", errors="coerce")
        nat_count = out["production_date"].isna().sum()
        if nat_count > 0:
            _log.warning("build_production_date: %d NaT values produced", nat_count)
        return out

    return ddf.map_partitions(_add_date, meta=meta)


# ---------------------------------------------------------------------------
# Task 04 — deduplication
# ---------------------------------------------------------------------------


def deduplicate(ddf: dd.DataFrame) -> dd.DataFrame:
    """Remove duplicate rows per (well_id, report_year, report_month), keep highest oil_bbl."""
    if "well_id" not in ddf.columns:
        raise KeyError("well_id column required for deduplication — run build_well_id first")

    def _dedup(pdf: pd.DataFrame) -> pd.DataFrame:
        return pdf.sort_values("oil_bbl", ascending=False, na_position="last").drop_duplicates(
            subset=["well_id", "report_year", "report_month"], keep="first"
        )

    return ddf.map_partitions(_dedup, meta=ddf._meta)


# ---------------------------------------------------------------------------
# Task 05 — validity flags
# ---------------------------------------------------------------------------


def apply_validity_flags(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add is_valid (bool) and flag_reason (StringDtype) columns."""
    missing = _VALIDITY_REQUIRED - set(ddf.columns)
    if missing:
        raise KeyError(f"apply_validity_flags: missing columns: {missing}")

    meta = ddf._meta.copy()
    meta["is_valid"] = pd.Series(dtype=bool)
    meta["flag_reason"] = pd.Series(dtype=pd.StringDtype())

    def _flag(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        n = len(out)
        is_valid = pd.array([True] * n, dtype=bool)
        flag_reason = pd.array([""] * n, dtype=pd.StringDtype())

        rules = [
            (out["oil_bbl"] < 0, "negative_oil"),
            (out["gas_mcf"] < 0, "negative_gas"),
            (out["water_bbl"] < 0, "negative_water"),
            ((out["days_produced"] < 0) | (out["days_produced"] > 31), "invalid_days"),
            (out["production_date"].isna(), "invalid_date"),
            (out["oil_bbl"] > 50_000, "oil_volume_outlier"),
            (out["gas_mcf"] > 500_000, "gas_volume_outlier"),
            (out["well_id"].isna(), "missing_well_id"),
        ]

        for mask, reason in rules:
            # Only apply rule to rows not yet flagged
            apply_mask = mask & is_valid
            is_valid[apply_mask.to_numpy()] = False
            flag_reason[apply_mask.to_numpy()] = reason

        out["is_valid"] = is_valid
        out["flag_reason"] = flag_reason
        return out

    return ddf.map_partitions(_flag, meta=meta)


# ---------------------------------------------------------------------------
# Task 06 — zero vs null guard
# ---------------------------------------------------------------------------


def preserve_zero_production(ddf: dd.DataFrame) -> dd.DataFrame:
    """Guard: assert that 0.0 values in production columns have not been replaced by NaN."""
    prod_cols = ["oil_bbl", "gas_mcf", "water_bbl"]

    def _check(pdf: pd.DataFrame) -> pd.DataFrame:
        for col in prod_cols:
            if col not in pdf.columns:
                continue
            zeros_mask = pdf[col] == 0.0
            if zeros_mask.any() and pdf.loc[zeros_mask, col].isna().any():
                raise RuntimeError("Zero production values were incorrectly nulled")
        return pdf

    return ddf.map_partitions(_check, meta=ddf._meta)


# ---------------------------------------------------------------------------
# Task 07 — monthly gap filler
# ---------------------------------------------------------------------------


def fill_monthly_gaps(ddf: dd.DataFrame) -> dd.DataFrame:
    """Insert missing monthly rows per well so each well has a contiguous series."""
    if "well_id" not in ddf.columns:
        raise KeyError("well_id column required for fill_monthly_gaps")

    id_cols = [
        "well_id",
        "county_code",
        "api_seq",
        "report_year",
        "report_month",
        "operator_name",
        "operator_num",
    ]

    def _fill(pdf: pd.DataFrame) -> pd.DataFrame:
        if "production_date" not in pdf.columns or pdf.empty:
            return pdf

        groups = []
        for well_id, grp in pdf.groupby("well_id", sort=False):
            # Skip wells with NaT production dates
            if grp["production_date"].isna().any():
                _log.warning("fill_monthly_gaps: NaT dates for well %s — skipping", well_id)
                groups.append(grp)
                continue

            grp = grp.sort_values("production_date").set_index("production_date")
            full_range = pd.date_range(grp.index.min(), grp.index.max(), freq="MS")
            grp = grp.reindex(full_range)

            # Forward-fill identifier columns
            for col in id_cols:
                if col in grp.columns:
                    grp[col] = grp[col].ffill()

            # Fill report_year/report_month from index if they were empty
            if "report_year" in grp.columns:
                grp["report_year"] = (
                    grp["report_year"]
                    .fillna(pd.Series(pd.DatetimeIndex(grp.index).year, index=grp.index))
                    .astype("int32")
                )
            if "report_month" in grp.columns:
                grp["report_month"] = (
                    grp["report_month"]
                    .fillna(pd.Series(pd.DatetimeIndex(grp.index).month, index=grp.index))
                    .astype("int32")
                )

            # Mark newly inserted gap rows
            gap_rows = grp.index.isin(full_range) & grp["oil_bbl"].isna()
            if "is_valid" in grp.columns:
                grp.loc[gap_rows, "is_valid"] = True
            if "flag_reason" in grp.columns:
                grp.loc[gap_rows, "flag_reason"] = "gap_filled"

            grp = grp.reset_index().rename(columns={"index": "production_date"})
            groups.append(grp)

        if not groups:
            return pdf
        result = pd.concat(groups, ignore_index=True)
        return result

    meta = ddf._meta.copy()
    # Reorder meta to match actual output: production_date should be first
    col_order = ["production_date"] + [c for c in ddf._meta.columns if c != "production_date"]
    meta = meta[col_order]
    return ddf.map_partitions(_fill, meta=meta)


# ---------------------------------------------------------------------------
# Task 08 — unit consistency check
# ---------------------------------------------------------------------------


def check_unit_consistency(ddf: dd.DataFrame) -> dict:
    """Audit production volume ranges. Returns summary dict. Calls .compute()."""
    required = {"oil_bbl", "gas_mcf", "water_bbl", "is_valid"}
    missing = required - set(ddf.columns)
    if missing:
        raise KeyError(f"check_unit_consistency: missing columns: {missing}")

    valid = ddf[ddf["is_valid"]].compute()

    oil_max = float(valid["oil_bbl"].max()) if not valid.empty else 0.0
    gas_max = float(valid["gas_mcf"].max()) if not valid.empty else 0.0
    water_max = float(valid["water_bbl"].max()) if not valid.empty else 0.0

    n_valid = len(valid)
    oil_outlier_fraction = float((valid["oil_bbl"] > 50_000).sum()) / n_valid if n_valid else 0.0
    gas_outlier_fraction = float((valid["gas_mcf"] > 500_000).sum()) / n_valid if n_valid else 0.0

    summary = {
        "oil_max": oil_max,
        "gas_max": gas_max,
        "water_max": water_max,
        "oil_outlier_fraction": oil_outlier_fraction,
        "gas_outlier_fraction": gas_outlier_fraction,
    }
    _log.info("Unit consistency: %s", summary)

    if oil_outlier_fraction > 0.01:
        _log.warning("oil_outlier_fraction %.4f > 1%% threshold", oil_outlier_fraction)

    return summary


# ---------------------------------------------------------------------------
# Task 09 — processed Parquet writer
# ---------------------------------------------------------------------------


def write_processed_parquet(
    ddf: dd.DataFrame,
    processed_dir: Path,
    total_rows_estimate: int,
) -> None:
    """Write cleaned DataFrame to *processed_dir* as consolidated Parquet."""
    npartitions = max(1, min(total_rows_estimate // 500_000, 50))
    processed_dir.mkdir(parents=True, exist_ok=True)
    _log.info("write_processed_parquet: npartitions=%d → %s", npartitions, processed_dir)
    ddf.repartition(npartitions=npartitions).to_parquet(
        str(processed_dir), write_index=False, overwrite=True
    )
    _log.info("Processed Parquet written to %s", processed_dir)


# ---------------------------------------------------------------------------
# Task 10 — transform orchestrator
# ---------------------------------------------------------------------------


def run_transform(config_path: str = "config.yaml") -> dd.DataFrame:
    """Run the full transform stage: interim Parquet → processed Parquet."""
    import traceback

    t0 = datetime.now()
    _log.info("Transform stage started")

    cfg = load_config(config_path)
    interim_dir = Path(cfg["transform"]["interim_dir"])
    processed_dir = Path(cfg["transform"]["processed_dir"])

    try:
        ddf = dd.read_parquet(str(interim_dir))
        ddf = ddf.repartition(npartitions=min(ddf.npartitions, 50))

        ddf = rename_columns(ddf)
        ddf = build_well_id(ddf)
        ddf = build_production_date(ddf)
        ddf = deduplicate(ddf)
        ddf = apply_validity_flags(ddf)
        ddf = fill_monthly_gaps(ddf)
        ddf = preserve_zero_production(ddf)

        total_rows_estimate: int = int(ddf.shape[0].compute())
        _log.info("Transform rows estimate: %d", total_rows_estimate)

        check_unit_consistency(ddf)
        write_processed_parquet(ddf, processed_dir, total_rows_estimate)

    except Exception:
        _log.error("Transform stage failed:\n%s", traceback.format_exc())
        raise

    elapsed = (datetime.now() - t0).total_seconds()
    _log.info("Transform stage complete in %.1fs", elapsed)
    return ddf


if __name__ == "__main__":
    run_transform()
