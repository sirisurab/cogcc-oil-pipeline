"""Features stage — engineer ML-ready features from processed Parquet data."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler  # type: ignore[import-untyped]

from cogcc_pipeline.acquire import get_logger, load_config

_log = get_logger(__name__)

_NUMERIC_SCALE_COLS = ["oil_bbl", "gas_mcf", "water_bbl", "cum_oil", "cum_gas", "cum_water"]
_CAT_ENCODE_COLS = ["county_code", "operator_name"]


# ---------------------------------------------------------------------------
# Task 01 — cumulative production
# ---------------------------------------------------------------------------


def compute_cumulative_production(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add cum_oil, cum_gas, cum_water per well (monotonically non-decreasing)."""
    if "well_id" not in ddf.columns or "production_date" not in ddf.columns:
        raise KeyError("compute_cumulative_production requires well_id and production_date")

    meta = ddf._meta.copy()
    for col in ("cum_oil", "cum_gas", "cum_water"):
        meta[col] = pd.Series(dtype="float64")

    def _cumulative(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        cum_results: dict[str, list] = {"cum_oil": [], "cum_gas": [], "cum_water": []}
        for well_id, grp in out.groupby("well_id", sort=False):
            grp = grp.sort_values("production_date")
            cum_results["cum_oil"].append(grp["oil_bbl"].fillna(0).cumsum())
            cum_results["cum_gas"].append(grp["gas_mcf"].fillna(0).cumsum())
            cum_results["cum_water"].append(grp["water_bbl"].fillna(0).cumsum())

        for col, parts in cum_results.items():
            if parts:
                combined = pd.concat(parts).reindex(out.index)
                out[col] = combined
            else:
                out[col] = np.nan

        return out

    return ddf.map_partitions(_cumulative, meta=meta)


# ---------------------------------------------------------------------------
# Task 02 — GOR calculator
# ---------------------------------------------------------------------------


def compute_gor(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add gor = gas_mcf / oil_bbl with zero/null handling per TR-06."""
    meta = ddf._meta.copy()
    meta["gor"] = pd.Series(dtype="float64")

    def _gor(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        oil = out["oil_bbl"]
        gas = out["gas_mcf"]
        gor = pd.Series(np.nan, index=out.index, dtype="float64")
        # oil > 0: normal division
        mask_pos = oil > 0
        gor[mask_pos] = gas[mask_pos] / oil[mask_pos]
        # oil == 0 and gas == 0: NaN (shut-in)  — already NaN
        # oil == 0 and gas > 0: NaN — already NaN
        # oil > 0 and gas == 0 or gas is NaN → 0.0 (fillna with 0 for gas when oil > 0)
        gor[mask_pos] = (gas[mask_pos].fillna(0)) / oil[mask_pos]
        out["gor"] = gor

        null_frac = gor[oil > 0].isna().mean() if (oil > 0).any() else 0.0
        if null_frac > 0.10:
            _log.debug("compute_gor: %.1f%% null gor for oil>0 rows", null_frac * 100)
        return out

    return ddf.map_partitions(_gor, meta=meta)


# ---------------------------------------------------------------------------
# Task 03 — water cut calculator
# ---------------------------------------------------------------------------


def compute_water_cut(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add water_cut = water_bbl / (oil_bbl + water_bbl) per TR-10."""
    meta = ddf._meta.copy()
    meta["water_cut"] = pd.Series(dtype="float64")

    def _water_cut(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        oil = out["oil_bbl"].fillna(0.0)
        water = out["water_bbl"].fillna(0.0)
        total = oil + water

        wc = pd.Series(np.nan, index=out.index, dtype="float64")
        mask_pos = total > 0
        wc[mask_pos] = water[mask_pos] / total[mask_pos]
        # shut-in (both 0): NaN — already NaN

        out["water_cut"] = wc

        # Integrity check
        invalid = wc[(~wc.isna()) & ((wc < 0.0) | (wc > 1.0))]
        if not invalid.empty:
            _log.error("compute_water_cut: %d values outside [0,1]", len(invalid))

        return out

    return ddf.map_partitions(_water_cut, meta=meta)


# ---------------------------------------------------------------------------
# Task 04 — decline rate calculator
# ---------------------------------------------------------------------------


def compute_decline_rate(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add decline_rate per well per TR-07. Clipped to [-1.0, 10.0]."""
    meta = ddf._meta.copy()
    meta["decline_rate"] = pd.Series(dtype="float64")

    def _decline(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        dr_series: list[pd.Series] = []

        for _, grp in out.groupby("well_id", sort=False):
            grp = grp.sort_values("production_date")
            oil = grp["oil_bbl"].fillna(0.0)
            prev = oil.shift(1)

            raw = pd.Series(np.nan, index=grp.index, dtype="float64")
            has_prev = prev.notna()

            # prev == 0, current == 0 → 0.0
            mask_zero_zero = has_prev & (prev == 0) & (oil == 0)
            raw[mask_zero_zero] = 0.0
            # prev == 0, current > 0 → 10.0 (cap, handled by clip)
            mask_zero_pos = has_prev & (prev == 0) & (oil > 0)
            raw[mask_zero_pos] = 10.0
            # prev > 0 → normal
            mask_normal = has_prev & (prev > 0)
            raw[mask_normal] = (oil[mask_normal] - prev[mask_normal]) / prev[mask_normal]

            raw = raw.clip(lower=-1.0, upper=10.0)
            dr_series.append(raw)

        if dr_series:
            out["decline_rate"] = pd.concat(dr_series).reindex(out.index)
        else:
            out["decline_rate"] = np.nan
        return out

    return ddf.map_partitions(_decline, meta=meta)


# ---------------------------------------------------------------------------
# Task 05 — rolling averages and lag features
# ---------------------------------------------------------------------------


def compute_rolling_and_lag_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add 3m/6m rolling means and 1m lag features per well (TR-09)."""
    if "well_id" not in ddf.columns or "production_date" not in ddf.columns:
        raise KeyError("compute_rolling_and_lag_features requires well_id and production_date")

    new_cols = [
        "oil_roll_3m",
        "gas_roll_3m",
        "water_roll_3m",
        "oil_roll_6m",
        "gas_roll_6m",
        "water_roll_6m",
        "oil_lag_1m",
        "gas_lag_1m",
        "water_lag_1m",
    ]
    meta = ddf._meta.copy()
    for col in new_cols:
        meta[col] = pd.Series(dtype="float64")

    def _rolling_lag(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        accum: dict[str, list[pd.Series]] = {c: [] for c in new_cols}

        for _, grp in out.groupby("well_id", sort=False):
            grp = grp.sort_values("production_date")
            for prod, w3, w6, lag in [
                ("oil_bbl", "oil_roll_3m", "oil_roll_6m", "oil_lag_1m"),
                ("gas_mcf", "gas_roll_3m", "gas_roll_6m", "gas_lag_1m"),
                ("water_bbl", "water_roll_3m", "water_roll_6m", "water_lag_1m"),
            ]:
                s = grp[prod]
                accum[w3].append(s.rolling(window=3, min_periods=3).mean())
                accum[w6].append(s.rolling(window=6, min_periods=6).mean())
                accum[lag].append(s.shift(1))

        for col, parts in accum.items():
            if parts:
                out[col] = pd.concat(parts).reindex(out.index)
            else:
                out[col] = np.nan
        return out

    return ddf.map_partitions(_rolling_lag, meta=meta)


# ---------------------------------------------------------------------------
# Task 06 — month-over-month % change
# ---------------------------------------------------------------------------


def compute_mom_pct_change(ddf: dd.DataFrame) -> dd.DataFrame:
    """Add oil_mom_pct and gas_mom_pct per well. Clipped to [-100, 1000]."""
    meta = ddf._meta.copy()
    meta["oil_mom_pct"] = pd.Series(dtype="float64")
    meta["gas_mom_pct"] = pd.Series(dtype="float64")

    def _mom(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        oil_acc: list[pd.Series] = []
        gas_acc: list[pd.Series] = []

        for _, grp in out.groupby("well_id", sort=False):
            grp = grp.sort_values("production_date")
            for s, acc in [(grp["oil_bbl"], oil_acc), (grp["gas_mcf"], gas_acc)]:
                prev = s.shift(1)
                pct = pd.Series(np.nan, index=grp.index, dtype="float64")
                mask = prev.notna() & (prev != 0)
                pct[mask] = ((s[mask] - prev[mask]) / prev[mask]) * 100
                pct = pct.clip(lower=-100.0, upper=1000.0)
                acc.append(pct)

        out["oil_mom_pct"] = pd.concat(oil_acc).reindex(out.index) if oil_acc else np.nan
        out["gas_mom_pct"] = pd.concat(gas_acc).reindex(out.index) if gas_acc else np.nan
        return out

    return ddf.map_partitions(_mom, meta=meta)


# ---------------------------------------------------------------------------
# Task 07 — categorical encoder
# ---------------------------------------------------------------------------


def encode_categoricals(ddf: dd.DataFrame, encoders: dict[str, Any]) -> dd.DataFrame:
    """Apply pre-fitted LabelEncoders to categorical columns (adds _enc columns)."""
    enc_cols = {"county_code": "county_code_enc", "operator_name": "operator_name_enc"}
    meta = ddf._meta.copy()
    for _, enc_col in enc_cols.items():
        meta[enc_col] = pd.Series(dtype="int32")

    def _encode(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        for src_col, enc_col in enc_cols.items():
            enc = encoders[src_col]
            classes = set(enc.classes_)
            vals = out[src_col].fillna("UNKNOWN").astype(str)
            encoded = np.full(len(vals), -1, dtype="int32")
            known_mask = vals.isin(classes)
            if (~known_mask).any():
                _log.warning("encode_categoricals: unseen labels in %s", src_col)
            if known_mask.any():
                encoded[known_mask.to_numpy()] = enc.transform(vals[known_mask]).astype("int32")
            out[enc_col] = encoded
        return out

    return ddf.map_partitions(_encode, meta=meta)


# ---------------------------------------------------------------------------
# Task 08 — numerical scaler
# ---------------------------------------------------------------------------


def scale_numerics(ddf: dd.DataFrame, scalers: dict[str, Any]) -> dd.DataFrame:
    """Apply pre-fitted StandardScalers to numeric columns (adds _scaled columns)."""
    scale_map = {col: f"{col}_scaled" for col in _NUMERIC_SCALE_COLS}

    # Validate all scalers present
    for col in _NUMERIC_SCALE_COLS:
        if col not in scalers:
            raise KeyError(f"No scaler provided for column: {col}")

    meta = ddf._meta.copy()
    for _, scaled_col in scale_map.items():
        meta[scaled_col] = pd.Series(dtype="float64")

    def _scale(pdf: pd.DataFrame) -> pd.DataFrame:
        out = pdf.copy()
        for col, scaled_col in scale_map.items():
            scaler = scalers[col]
            vals = out[col].values.astype("float64")
            result = np.full(len(vals), np.nan, dtype="float64")
            mask = ~np.isnan(vals)
            if mask.any():
                result[mask] = scaler.transform(np.asarray(vals[mask]).reshape(-1, 1)).flatten()
            out[scaled_col] = result
        return out

    return ddf.map_partitions(_scale, meta=meta)


# ---------------------------------------------------------------------------
# Task 09 — scaler and encoder fitter
# ---------------------------------------------------------------------------


def fit_scalers_and_encoders(
    ddf: dd.DataFrame,
) -> tuple[dict[str, StandardScaler], dict[str, LabelEncoder]]:
    """Fit StandardScaler per numeric column and LabelEncoder per categorical column."""
    required_num = set(_NUMERIC_SCALE_COLS)
    required_cat = set(_CAT_ENCODE_COLS)
    missing = (required_num | required_cat) - set(ddf.columns)
    if missing:
        raise KeyError(f"fit_scalers_and_encoders: missing columns: {missing}")

    # Fit scalers
    num_df = ddf[_NUMERIC_SCALE_COLS].compute()
    scalers: dict[str, StandardScaler] = {}
    for col in _NUMERIC_SCALE_COLS:
        valid = num_df[col].dropna()
        scaler = StandardScaler()
        if len(valid) == 0:
            _log.warning("fit_scalers_and_encoders: no non-null values for %s", col)
            scaler.mean_ = np.array([0.0])
            scaler.scale_ = np.array([1.0])
            scaler.var_ = np.array([1.0])
            scaler.n_features_in_ = 1
            scaler.n_samples_seen_ = 0
        else:
            scaler.fit(valid.values.reshape(-1, 1))
        _log.info(
            "Scaler %s: mean=%.4f std=%.4f", col, float(scaler.mean_[0]), float(scaler.scale_[0])
        )
        scalers[col] = scaler

    # Fit encoders
    cat_df = ddf[_CAT_ENCODE_COLS].compute()
    encoders: dict[str, LabelEncoder] = {}
    for col in _CAT_ENCODE_COLS:
        enc = LabelEncoder()
        vals = cat_df[col].fillna("UNKNOWN").astype(str)
        enc.fit(vals)
        _log.info("Encoder %s: %d unique classes", col, len(enc.classes_))
        encoders[col] = enc

    return scalers, encoders


# ---------------------------------------------------------------------------
# Task 10 — features Parquet writer
# ---------------------------------------------------------------------------


def write_features_parquet(
    ddf: dd.DataFrame,
    output_dir: Path,
    total_rows_estimate: int,
) -> None:
    """Write ML-ready features DataFrame to *output_dir* as Parquet."""
    npartitions = max(1, min(total_rows_estimate // 500_000, 50))
    output_dir.mkdir(parents=True, exist_ok=True)
    _log.info("write_features_parquet: npartitions=%d → %s", npartitions, output_dir)
    ddf.repartition(npartitions=npartitions).to_parquet(
        str(output_dir), write_index=False, overwrite=True
    )
    _log.info("Features Parquet written to %s", output_dir)


# ---------------------------------------------------------------------------
# Task 11 — features orchestrator
# ---------------------------------------------------------------------------


def run_features(config_path: str = "config.yaml") -> dd.DataFrame:
    """Run the full features stage: processed Parquet → ML-ready features Parquet."""
    import traceback

    t0 = datetime.now()
    _log.info("Features stage started")

    cfg = load_config(config_path)
    processed_dir = Path(cfg["features"]["processed_dir"])
    output_dir = Path(cfg["features"]["output_dir"])

    try:
        ddf = dd.read_parquet(str(processed_dir))
        ddf = ddf.repartition(npartitions=min(ddf.npartitions, 50))

        # Filter to valid rows only for ML
        if "is_valid" in ddf.columns:
            ddf = ddf[ddf["is_valid"]]

        ddf = compute_cumulative_production(ddf)
        ddf = compute_gor(ddf)
        ddf = compute_water_cut(ddf)
        ddf = compute_decline_rate(ddf)
        ddf = compute_rolling_and_lag_features(ddf)
        ddf = compute_mom_pct_change(ddf)

        scalers, encoders = fit_scalers_and_encoders(ddf)
        ddf = scale_numerics(ddf, scalers)
        ddf = encode_categoricals(ddf, encoders)

        total_rows_estimate: int = int(ddf.shape[0].compute())
        _log.info("Features rows estimate: %d", total_rows_estimate)

        write_features_parquet(ddf, output_dir, total_rows_estimate)

    except Exception:
        _log.error("Features stage failed:\n%s", traceback.format_exc())
        raise

    elapsed = (datetime.now() - t0).total_seconds()
    _log.info("Features stage complete in %.1fs, columns: %d", elapsed, len(ddf.columns))
    return ddf


if __name__ == "__main__":
    run_features()
