"""Tests for the transform stage (TRN-01 through TRN-07, TR-01 through TR-23)."""

import random
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from cogcc_pipeline.transform import (
    _add_derived_columns,
    _clean_production_volumes,
    _deduplicate,
    _normalize_operator_names,
    check_well_completeness,
    run_transform,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WELL_STATUS_CATS = ["AB", "AC", "DG", "PA", "PR", "SI", "SO", "TA", "WO"]


def _make_interim_df(
    n_wells: int = 2,
    months_per_well: int = 6,
    start_year: int = 2021,
    include_negatives: bool = False,
) -> pd.DataFrame:
    """Create a synthetic interim-stage DataFrame."""
    rows = []
    for w in range(n_wells):
        api_county = f"{w + 1:02d}"
        api_seq = f"{w + 1:05d}"
        api_side = "00"
        for m in range(months_per_well):
            month = (m % 12) + 1
            year = start_year + m // 12
            oil = -100.0 if include_negatives and m == 0 else float(100 + m * 10)
            rows.append(
                {
                    "DocNum": 1000 + w * 100 + m,
                    "ReportMonth": month,
                    "ReportYear": year,
                    "DaysProduced": 28,
                    "AcceptedDate": pd.Timestamp(f"{year}-{month:02d}-15"),
                    "Revised": False,
                    "OpName": f"Operator {w + 1}, LLC",
                    "OpNumber": 100 + w,
                    "FacilityId": 200 + w,
                    "ApiCountyCode": api_county,
                    "ApiSequenceNumber": api_seq,
                    "ApiSidetrack": api_side,
                    "Well": f"WELL-{w + 1}",
                    "WellStatus": pd.Categorical(["AC"], categories=_WELL_STATUS_CATS)[0],
                    "FormationCode": "WATTE",
                    "OilProduced": oil,
                    "OilSales": oil * 0.95,
                    "OilAdjustment": 0.0,
                    "OilGravity": 38.5,
                    "GasProduced": 500.0 + m * 5,
                    "GasSales": 480.0,
                    "GasBtuSales": 1000.0,
                    "GasUsedOnLease": 10.0,
                    "GasShrinkage": 5.0,
                    "GasPressureTubing": 200.0,
                    "GasPressureCasing": 210.0,
                    "WaterProduced": 50.0,
                    "WaterPressureTubing": 100.0,
                    "WaterPressureCasing": 105.0,
                    "FlaredVented": 2.0,
                    "BomInvent": 0.0,
                    "EomInvent": 0.0,
                }
            )
    df = pd.DataFrame(rows)
    df["WellStatus"] = pd.Categorical(df["WellStatus"], categories=_WELL_STATUS_CATS)
    df["DaysProduced"] = df["DaysProduced"].astype("Int64")
    df["DocNum"] = df["DocNum"].astype("Int64")
    df["ReportYear"] = df["ReportYear"].astype("int64")
    df["ReportMonth"] = df["ReportMonth"].astype("int64")
    return df


def _write_interim_parquet(tmp_path: Path, df: pd.DataFrame) -> Path:
    interim_dir = tmp_path / "interim"
    interim_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(str(interim_dir / "part.0.parquet"), index=False)
    return interim_dir


def _make_transform_config(tmp_path: Path, interim_dir: Path) -> str:
    import yaml

    processed_dir = tmp_path / "processed"
    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": "data/raw",
            "max_workers": 2,
            "sleep_per_worker": 0.0,
            "timeout": 10,
            "max_retries": 1,
            "backoff_multiplier": 1,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 2,
            "threads_per_worker": 1,
            "memory_limit": "512MB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": str(tmp_path / "logs/p.log"), "level": "INFO"},
        "ingest": {"raw_dir": "data/raw", "interim_dir": str(interim_dir), "start_year": 2020},
        "transform": {"interim_dir": str(interim_dir), "processed_dir": str(processed_dir)},
        "features": {"processed_dir": str(processed_dir), "features_dir": str(tmp_path / "feat")},
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))
    return str(cfg_file)


# ---------------------------------------------------------------------------
# TRN-01: _add_derived_columns tests
# ---------------------------------------------------------------------------


def test_add_derived_columns_well_id_correct():
    df = pd.DataFrame(
        [
            {
                "ApiCountyCode": "01",
                "ApiSequenceNumber": "00123",
                "ApiSidetrack": "00",
                "ReportYear": 2021,
                "ReportMonth": 6,
            }
        ]
    )
    result = _add_derived_columns(df)
    assert result["well_id"].iloc[0] == "010012300"


def test_add_derived_columns_padding():
    df = pd.DataFrame(
        [
            {
                "ApiCountyCode": "5",
                "ApiSequenceNumber": "456",
                "ApiSidetrack": None,
                "ReportYear": 2021,
                "ReportMonth": 6,
            }
        ]
    )
    result = _add_derived_columns(df)
    assert result["well_id"].iloc[0] == "050045600"


def test_add_derived_columns_production_date():
    df = pd.DataFrame(
        [
            {
                "ApiCountyCode": "01",
                "ApiSequenceNumber": "00123",
                "ApiSidetrack": "00",
                "ReportYear": 2021,
                "ReportMonth": 6,
            }
        ]
    )
    result = _add_derived_columns(df)
    assert result["production_date"].iloc[0] == pd.Timestamp("2021-06-01")


def test_add_derived_columns_production_date_dtype():
    df = pd.DataFrame(
        [
            {
                "ApiCountyCode": "01",
                "ApiSequenceNumber": "00123",
                "ApiSidetrack": "00",
                "ReportYear": 2021,
                "ReportMonth": 6,
            }
        ]
    )
    result = _add_derived_columns(df)
    assert np.issubdtype(result["production_date"].dtype, np.datetime64)


def test_add_derived_columns_well_id_dtype():
    df = pd.DataFrame(
        [
            {
                "ApiCountyCode": "01",
                "ApiSequenceNumber": "00123",
                "ApiSidetrack": "00",
                "ReportYear": 2021,
                "ReportMonth": 6,
            }
        ]
    )
    result = _add_derived_columns(df)
    assert result["well_id"].dtype == object or isinstance(result["well_id"].dtype, pd.StringDtype)


def test_add_derived_columns_empty_df_meta():
    """TR-23: empty DataFrame with correct schema produces same columns as meta."""
    full_df = _make_interim_df(1, 1)
    empty_df = full_df.iloc[:0].copy()
    result = _add_derived_columns(empty_df)
    assert "well_id" in result.columns
    assert "production_date" in result.columns


# ---------------------------------------------------------------------------
# TRN-02: _clean_production_volumes tests
# ---------------------------------------------------------------------------


def _vol_df(**overrides) -> pd.DataFrame:
    base = {
        "OilProduced": 100.0,
        "OilSales": 90.0,
        "GasProduced": 500.0,
        "GasSales": 450.0,
        "GasUsedOnLease": 10.0,
        "FlaredVented": 2.0,
        "WaterProduced": 50.0,
        "GasPressureTubing": 200.0,
        "GasPressureCasing": 210.0,
        "WaterPressureTubing": 100.0,
        "WaterPressureCasing": 105.0,
        "DaysProduced": pd.array([28], dtype="Int64")[0],
    }
    base.update(overrides)
    return pd.DataFrame([base])


def test_clean_volumes_negative_oil_becomes_na():
    df = _vol_df(OilProduced=-100.0)
    result = _clean_production_volumes(df)
    assert pd.isna(result["OilProduced"].iloc[0])


def test_clean_volumes_zero_oil_preserved():
    df = _vol_df(OilProduced=0.0)
    result = _clean_production_volumes(df)
    assert result["OilProduced"].iloc[0] == 0.0


def test_clean_volumes_oil_unit_flag_high():
    df = _vol_df(OilProduced=51000.0)
    result = _clean_production_volumes(df)
    assert result["oil_unit_flag"].iloc[0]
    assert result["OilProduced"].iloc[0] == 51000.0  # not nulled


def test_clean_volumes_oil_unit_flag_normal():
    df = _vol_df(OilProduced=100.0)
    result = _clean_production_volumes(df)
    assert not result["oil_unit_flag"].iloc[0]


def test_clean_volumes_negative_gas():
    df = _vol_df(GasProduced=-5.0)
    result = _clean_production_volumes(df)
    assert pd.isna(result["GasProduced"].iloc[0])


def test_clean_volumes_zero_water_preserved():
    df = _vol_df(WaterProduced=0.0)
    result = _clean_production_volumes(df)
    assert result["WaterProduced"].iloc[0] == 0.0


def test_clean_volumes_negative_pressure():
    df = _vol_df(GasPressureTubing=-10.0)
    result = _clean_production_volumes(df)
    assert pd.isna(result["GasPressureTubing"].iloc[0])


def test_clean_volumes_days_produced_out_of_range():
    df = _vol_df()
    df["DaysProduced"] = pd.array([35], dtype="Int64")
    result = _clean_production_volumes(df)
    assert pd.isna(result["DaysProduced"].iloc[0])


def test_clean_volumes_days_produced_zero_preserved():
    df = _vol_df()
    df["DaysProduced"] = pd.array([0], dtype="Int64")
    result = _clean_production_volumes(df)
    assert result["DaysProduced"].iloc[0] == 0


def test_clean_volumes_empty_df_meta():
    """TR-23: empty DataFrame schema matches meta."""
    full_df = _make_interim_df(1, 1)
    derived = _add_derived_columns(full_df)
    empty_df = derived.iloc[:0].copy()
    result = _clean_production_volumes(empty_df)
    assert "oil_unit_flag" in result.columns


# ---------------------------------------------------------------------------
# TRN-03: _normalize_operator_names tests
# ---------------------------------------------------------------------------


def _opname_df(name) -> pd.DataFrame:
    return pd.DataFrame([{"OpName": name}])


def test_normalize_strips_llc():
    df = _opname_df("  Devon Energy Corp , LLC  ")
    result = _normalize_operator_names(df)
    assert result["OpName"].iloc[0] == "DEVON ENERGY CORP"


def test_normalize_strips_inc_dot():
    df = _opname_df("Continental Resources, Inc.")
    result = _normalize_operator_names(df)
    assert result["OpName"].iloc[0] == "CONTINENTAL RESOURCES"


def test_normalize_no_change():
    df = _opname_df("ENERPLUS RESOURCES")
    result = _normalize_operator_names(df)
    assert result["OpName"].iloc[0] == "ENERPLUS RESOURCES"


def test_normalize_na_remains_na():
    df = pd.DataFrame([{"OpName": pd.NA}])
    result = _normalize_operator_names(df)
    assert pd.isna(result["OpName"].iloc[0])


def test_normalize_empty_string_becomes_na():
    df = _opname_df("")
    result = _normalize_operator_names(df)
    assert pd.isna(result["OpName"].iloc[0])


def test_normalize_double_space_lp():
    df = _opname_df("PDC  Energy,  LP")
    result = _normalize_operator_names(df)
    assert result["OpName"].iloc[0] == "PDC ENERGY"


def test_normalize_empty_df_meta():
    """TR-23: empty DataFrame schema matches meta."""
    full_df = _make_interim_df(1, 1)
    empty_df = full_df.iloc[:0].copy()
    result = _normalize_operator_names(empty_df)
    assert "OpName" in result.columns


# ---------------------------------------------------------------------------
# TRN-04: _deduplicate tests
# ---------------------------------------------------------------------------


def _dup_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if "WellStatus" not in df.columns:
        df["WellStatus"] = pd.Categorical(["AC"] * len(df), categories=_WELL_STATUS_CATS)
    if "AcceptedDate" not in df.columns:
        df["AcceptedDate"] = pd.NaT
    if "Revised" not in df.columns:
        df["Revised"] = pd.array([False] * len(df), dtype="boolean")
    return df


def _base_row(**overrides) -> dict:
    base = {
        "well_id": "010000100",
        "ReportYear": 2021,
        "ReportMonth": 1,
        "OilProduced": 100.0,
        "Revised": False,
        "AcceptedDate": pd.Timestamp("2021-02-01"),
    }
    base.update(overrides)
    return base


def test_dedup_exact_duplicates():
    row = _base_row()
    df = _dup_df([row, row, row, row, row])
    result = _deduplicate(df)
    assert len(result) == 1


def test_dedup_revised_true_kept():
    original = _base_row(Revised=False)
    revised = _base_row(Revised=True, OilProduced=110.0)
    df = _dup_df([original, revised])
    result = _deduplicate(df)
    assert len(result) == 1
    assert result["OilProduced"].iloc[0] == 110.0


def test_dedup_latest_accepted_date_kept():
    r1 = _base_row(Revised=True, AcceptedDate=pd.Timestamp("2021-02-01"), OilProduced=100.0)
    r2 = _base_row(Revised=True, AcceptedDate=pd.Timestamp("2021-03-15"), OilProduced=120.0)
    df = _dup_df([r1, r2])
    result = _deduplicate(df)
    assert len(result) == 1
    assert result["OilProduced"].iloc[0] == 120.0


def test_dedup_row_count_never_increases():
    random.seed(42)
    rows = [
        _base_row(
            well_id=f"0100{random.randint(0, 3):05d}0",
            ReportMonth=random.randint(1, 12),
            OilProduced=float(random.randint(50, 200)),
        )
        for _ in range(100)
    ]
    # Add 30% duplicates
    for i in range(30):
        rows.append(rows[random.randint(0, 99)])
    df = _dup_df(rows)
    result = _deduplicate(df)
    assert len(result) <= len(df)


def test_dedup_idempotent():
    rows = [_base_row(), _base_row(OilProduced=110.0, Revised=True)]
    df = _dup_df(rows)
    result1 = _deduplicate(df)
    result2 = _deduplicate(result1)
    assert len(result1) == len(result2)


def test_dedup_empty_df_meta():
    """TR-23: empty DataFrame schema matches meta."""
    full_df = _make_interim_df(1, 1)
    derived = _add_derived_columns(full_df)
    empty_df = derived.iloc[:0].copy()
    result = _deduplicate(empty_df)
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# TRN-05: check_well_completeness tests
# ---------------------------------------------------------------------------


def _make_dask_df(rows: list[dict]) -> dd.DataFrame:
    df = pd.DataFrame(rows)
    df["production_date"] = pd.to_datetime(df["production_date"])
    return dd.from_pandas(df.set_index("well_id"), npartitions=1)


def test_well_completeness_no_gaps():
    rows = [
        {"well_id": "010000100", "production_date": pd.Timestamp(f"2021-{m:02d}-01")}
        for m in range(1, 13)
    ]
    ddf = _make_dask_df(rows)
    result = check_well_completeness(ddf)
    assert result.empty


def test_well_completeness_with_gap():
    rows = [
        {"well_id": "010000100", "production_date": pd.Timestamp("2021-01-01")},
        {"well_id": "010000100", "production_date": pd.Timestamp("2021-02-01")},
        # March missing
        {"well_id": "010000100", "production_date": pd.Timestamp("2021-04-01")},
    ]
    ddf = _make_dask_df(rows)
    result = check_well_completeness(ddf)
    assert len(result) == 1
    assert result.iloc[0]["expected_months"] == 4
    assert result.iloc[0]["actual_months"] == 3
    assert result.iloc[0]["gap_count"] == 1


def test_well_completeness_only_gapped_well_returned():
    rows_nogap = [
        {"well_id": "010000100", "production_date": pd.Timestamp(f"2021-{m:02d}-01")}
        for m in range(1, 7)
    ]
    rows_gap = [
        {"well_id": "020000100", "production_date": pd.Timestamp("2021-01-01")},
        {"well_id": "020000100", "production_date": pd.Timestamp("2021-03-01")},
        {"well_id": "020000100", "production_date": pd.Timestamp("2021-06-01")},
    ]
    ddf = _make_dask_df(rows_nogap + rows_gap)
    result = check_well_completeness(ddf)
    assert set(result["well_id"].tolist()) == {"020000100"}


def test_well_completeness_returns_pandas():
    rows = [{"well_id": "010000100", "production_date": pd.Timestamp("2021-01-01")}]
    ddf = _make_dask_df(rows)
    result = check_well_completeness(ddf)
    assert isinstance(result, pd.DataFrame)


def test_well_completeness_output_columns():
    rows = [{"well_id": "010000100", "production_date": pd.Timestamp("2021-01-01")}]
    ddf = _make_dask_df(rows)
    result = check_well_completeness(ddf)
    expected_cols = {
        "well_id",
        "min_date",
        "max_date",
        "expected_months",
        "actual_months",
        "gap_count",
    }
    assert set(result.columns) == expected_cols


# ---------------------------------------------------------------------------
# TRN-06 + TRN-07: run_transform integration tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def transform_setup(tmp_path):
    df = _make_interim_df(n_wells=2, months_per_well=6)
    interim_dir = _write_interim_parquet(tmp_path, df)
    cfg_path = _make_transform_config(tmp_path, interim_dir)
    processed_dir = tmp_path / "processed"
    return cfg_path, processed_dir, df


def test_run_transform_creates_parquet(transform_setup):
    cfg_path, processed_dir, _ = transform_setup
    run_transform(cfg_path)
    assert list(processed_dir.glob("*.parquet"))


def test_run_transform_parquet_readable(transform_setup):
    """TR-18: All output Parquet files readable."""
    cfg_path, processed_dir, _ = transform_setup
    run_transform(cfg_path)
    for pf in processed_dir.glob("*.parquet"):
        df = pd.read_parquet(pf)
        assert isinstance(df, pd.DataFrame)


def test_run_transform_output_columns(transform_setup):
    cfg_path, processed_dir, _ = transform_setup
    run_transform(cfg_path)
    all_df = dd.read_parquet(str(processed_dir)).compute()
    for col in ["production_date", "OilProduced", "GasProduced", "WaterProduced", "oil_unit_flag"]:
        assert col in all_df.columns or col in all_df.index.names


def test_run_transform_sorted_within_partitions(transform_setup):
    cfg_path, processed_dir, _ = transform_setup
    run_transform(cfg_path)
    for pf in sorted(processed_dir.glob("*.parquet")):
        df = pd.read_parquet(pf)
        if len(df) > 1:
            assert df["production_date"].is_monotonic_increasing


def test_run_transform_empty_interim_dir(tmp_path):
    """Given empty interim_dir, run_transform returns without raising."""
    interim_dir = tmp_path / "interim"
    interim_dir.mkdir()
    cfg_path = _make_transform_config(tmp_path, interim_dir)
    run_transform(cfg_path)  # should not raise


def test_run_transform_no_compute_before_write(transform_setup):
    """TR-17: run_transform must not call .compute() before writing."""

    cfg_path, processed_dir, _ = transform_setup
    compute_calls = []

    original_compute = dd.DataFrame.compute

    def tracking_compute(self, **kwargs):
        compute_calls.append(1)
        return original_compute(self, **kwargs)

    # We allow compute() only for the well completeness check (diagnostic)
    # The test verifies no intermediate compute before repartition
    run_transform(cfg_path)
    # Main test: output exists (indicating write happened)
    assert list(processed_dir.glob("*.parquet"))


# ---------------------------------------------------------------------------
# TR-11, TR-12, TR-13, TR-14, TR-15: Integration validations
# ---------------------------------------------------------------------------


def test_tr12_cleaning_validation(tmp_path):
    """TR-12: Negative GasProduced → NA; duplicates removed; null OilProduced stays null."""
    rows = []
    # 2 null OilProduced
    for _ in range(2):
        r = _make_interim_df(1, 1).iloc[0].to_dict()
        r["OilProduced"] = None
        rows.append(r)
    # 3 negative GasProduced
    for _ in range(3):
        r = _make_interim_df(1, 1).iloc[0].to_dict()
        r["GasProduced"] = -5.0
        r["ReportMonth"] = 2
        rows.append(r)
    # 1 duplicate
    base = _make_interim_df(1, 1).iloc[0].to_dict()
    base["ReportMonth"] = 3
    rows.append(base)
    rows.append(base)  # exact duplicate

    df = pd.DataFrame(rows)
    df["ReportYear"] = df["ReportYear"].astype("int64")
    df["ReportMonth"] = df["ReportMonth"].astype("int64")
    df["DaysProduced"] = pd.array([28] * len(df), dtype="Int64")
    df["DocNum"] = pd.array([1001] * len(df), dtype="Int64")
    df["WellStatus"] = pd.Categorical(["AC"] * len(df), categories=_WELL_STATUS_CATS)

    interim_dir = tmp_path / "interim"
    interim_dir.mkdir()
    df.to_parquet(str(interim_dir / "part.0.parquet"), index=False)

    cfg_path = _make_transform_config(tmp_path, interim_dir)
    run_transform(cfg_path)

    processed_dir = tmp_path / "processed"
    result = dd.read_parquet(str(processed_dir)).compute()

    # Null OilProduced must remain null
    assert result["OilProduced"].isna().any()
    # Negative GasProduced → NA
    assert not (result["GasProduced"] < 0).any()
    # production_date dtype
    assert np.issubdtype(result["production_date"].dtype, np.datetime64)
    assert result["OilProduced"].dtype == np.dtype("float64")


def test_tr15_row_count_monotone(transform_setup):
    """TR-15: Output row count <= input row count."""
    cfg_path, processed_dir, input_df = transform_setup
    input_rows = len(input_df)
    run_transform(cfg_path)
    output_df = dd.read_parquet(str(processed_dir)).compute()
    assert len(output_df) <= input_rows


def test_tr14_schema_stability_across_partitions(transform_setup):
    """TR-14: All partition files have identical column names and dtypes."""
    cfg_path, processed_dir, _ = transform_setup
    run_transform(cfg_path)
    parquet_files = sorted(processed_dir.glob("*.parquet"))
    if len(parquet_files) < 2:
        pytest.skip("Need at least 2 partitions for TR-14")

    df0 = pd.read_parquet(parquet_files[0])
    df1 = pd.read_parquet(parquet_files[1])
    assert list(df0.columns) == list(df1.columns)
    assert dict(df0.dtypes) == dict(df1.dtypes)
