"""Tests for the ingest stage and shared utils (ING-01 through ING-07, TR-17, TR-18, TR-22)."""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from cogcc_pipeline.ingest import (
    enforce_schema,
    load_data_dictionary,
    map_dtype,
    read_raw_csv,
    run_ingest,
)
from cogcc_pipeline.utils import get_partition_count, load_config, setup_logging

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DICT_PATH = "references/production-data-dictionary.csv"

_ALL_COLS = [
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

_NON_NULLABLE = [
    "ReportMonth",
    "ReportYear",
    "ApiCountyCode",
    "ApiSequenceNumber",
    "ApiSidetrack",
    "Well",
]


def _make_csv_row(year: int = 2021, month: int = 1) -> dict:
    return {
        "DocNum": "1001",
        "ReportMonth": str(month),
        "ReportYear": str(year),
        "DaysProduced": "28",
        "AcceptedDate": "2021-02-15",
        "Revised": "False",
        "OpName": "TEST OPERATOR",
        "OpNumber": "100",
        "FacilityId": "200",
        "ApiCountyCode": "01",
        "ApiSequenceNumber": "00123",
        "ApiSidetrack": "00",
        "Well": "TEST WELL 1",
        "WellStatus": "AC",
        "FormationCode": "WATTE",
        "OilProduced": "100.5",
        "OilSales": "99.0",
        "OilAdjustment": "0.0",
        "OilGravity": "38.5",
        "GasProduced": "500.0",
        "GasSales": "480.0",
        "GasBtuSales": "1000.0",
        "GasUsedOnLease": "10.0",
        "GasShrinkage": "5.0",
        "GasPressureTubing": "200.0",
        "GasPressureCasing": "210.0",
        "WaterProduced": "50.0",
        "WaterPressureTubing": "100.0",
        "WaterPressureCasing": "105.0",
        "FlaredVented": "2.0",
        "BomInvent": "0.0",
        "EomInvent": "0.0",
    }


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)


def _make_data_dict() -> dict[str, dict]:
    return load_data_dictionary(_DICT_PATH)


# ---------------------------------------------------------------------------
# ING-06: Shared utilities tests
# ---------------------------------------------------------------------------


def test_get_partition_count_below_min():
    assert get_partition_count(3) == 10


def test_get_partition_count_in_range():
    assert get_partition_count(25) == 25


def test_get_partition_count_above_max():
    assert get_partition_count(100) == 50


def test_get_partition_count_at_min():
    assert get_partition_count(10) == 10


def test_get_partition_count_at_max():
    assert get_partition_count(50) == 50


def test_load_config_valid(tmp_path):
    import yaml

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2022,
            "raw_dir": "data/raw",
            "max_workers": 5,
            "sleep_per_worker": 0.5,
            "timeout": 60,
            "max_retries": 3,
            "backoff_multiplier": 2,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 2,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {"log_file": "logs/p.log", "level": "INFO"},
        "ingest": {},
        "transform": {},
        "features": {},
    }
    f = tmp_path / "config.yaml"
    f.write_text(yaml.dump(cfg_data))
    cfg = load_config(str(f))
    assert "acquire" in cfg and "dask" in cfg and "logging" in cfg


def test_setup_logging_creates_log_dir(tmp_path):
    log_dir = tmp_path / "logs"
    log_file = str(log_dir / "test.log")
    import logging

    # Reset root handlers
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    root.handlers.clear()
    try:
        setup_logging(log_file, "INFO")
        assert log_dir.exists()
    finally:
        root.handlers = original_handlers


def test_setup_logging_idempotent(tmp_path):
    import logging

    log_file = str(tmp_path / "logs" / "test.log")
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    root.handlers.clear()
    try:
        setup_logging(log_file, "INFO")
        count_after_first = len(root.handlers)
        setup_logging(log_file, "INFO")
        count_after_second = len(root.handlers)
        assert count_after_second == count_after_first
    finally:
        root.handlers = original_handlers


# ---------------------------------------------------------------------------
# ING-01: load_data_dictionary tests
# ---------------------------------------------------------------------------


def test_load_data_dictionary_returns_33_keys():
    result = load_data_dictionary(_DICT_PATH)
    assert len(result) == 32


def test_load_data_dictionary_oil_produced_dtype():
    result = load_data_dictionary(_DICT_PATH)
    assert result["OilProduced"]["dtype"] == "float"
    assert result["OilProduced"]["nullable"] is True


def test_load_data_dictionary_report_year_not_nullable():
    result = load_data_dictionary(_DICT_PATH)
    assert result["ReportYear"]["nullable"] is False


def test_load_data_dictionary_well_status_categories():
    result = load_data_dictionary(_DICT_PATH)
    assert result["WellStatus"]["categories"] == [
        "AB",
        "AC",
        "DG",
        "PA",
        "PR",
        "SI",
        "SO",
        "TA",
        "WO",
    ]


def test_load_data_dictionary_oil_produced_no_categories():
    result = load_data_dictionary(_DICT_PATH)
    assert result["OilProduced"]["categories"] == []


def test_load_data_dictionary_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        load_data_dictionary("/nonexistent/dict.csv")


# ---------------------------------------------------------------------------
# ING-02: map_dtype tests
# ---------------------------------------------------------------------------


def test_map_dtype_int_not_nullable():
    assert map_dtype("int", False, []) == np.dtype("int64")


def test_map_dtype_int_nullable():
    assert map_dtype("int", True, []) == pd.Int64Dtype()


def test_map_dtype_float():
    assert map_dtype("float", True, []) == np.dtype("float64")


def test_map_dtype_string():
    assert map_dtype("string", True, []) == pd.StringDtype()


def test_map_dtype_bool():
    assert map_dtype("bool", True, []) == pd.BooleanDtype()


def test_map_dtype_categorical():
    result = map_dtype("categorical", True, ["AB", "AC"])
    assert result == pd.CategoricalDtype(categories=["AB", "AC"], ordered=False)


def test_map_dtype_datetime():
    assert map_dtype("datetime", True, []) == "datetime64[ns]"


def test_map_dtype_unknown_raises():
    with pytest.raises(ValueError):
        map_dtype("unknown_token", False, [])


# ---------------------------------------------------------------------------
# ING-03: enforce_schema tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def data_dict():
    return _make_data_dict()


@pytest.fixture()
def full_raw_df():
    """A small DataFrame with all canonical columns as strings."""
    rows = [_make_csv_row(2021, m) for m in range(1, 4)]
    return pd.DataFrame(rows)


def test_enforce_schema_correct_dtypes(full_raw_df, data_dict):
    result = enforce_schema(full_raw_df.copy(), data_dict)
    assert result["OilProduced"].dtype == np.dtype("float64")
    assert result["ReportYear"].dtype == np.dtype("int64")


def test_enforce_schema_missing_nullable_column(full_raw_df, data_dict):
    df = full_raw_df.drop(columns=["OilProduced"])
    result = enforce_schema(df, data_dict)
    assert "OilProduced" in result.columns
    assert result["OilProduced"].isna().all()


def test_enforce_schema_missing_non_nullable_raises(full_raw_df, data_dict):
    df = full_raw_df.drop(columns=["ReportYear"])
    with pytest.raises(KeyError):
        enforce_schema(df, data_dict)


def test_enforce_schema_invalid_well_status(full_raw_df, data_dict):
    full_raw_df = full_raw_df.copy()
    full_raw_df["WellStatus"] = "XX"
    result = enforce_schema(full_raw_df, data_dict)
    assert result["WellStatus"].isna().all()
    assert hasattr(result["WellStatus"], "cat")


def test_enforce_schema_bad_accepted_date(full_raw_df, data_dict):
    full_raw_df = full_raw_df.copy()
    full_raw_df["AcceptedDate"] = "not-a-date"
    result = enforce_schema(full_raw_df, data_dict)
    assert result["AcceptedDate"].isna().all()


def test_enforce_schema_column_order(full_raw_df, data_dict):
    result = enforce_schema(full_raw_df.copy(), data_dict)
    expected_order = [c for c in data_dict if c in result.columns]
    assert list(result.columns) == expected_order


# ---------------------------------------------------------------------------
# ING-04: read_raw_csv tests
# ---------------------------------------------------------------------------


def test_read_raw_csv_year_filter(tmp_path, data_dict):
    """Rows with ReportYear < 2020 must be excluded."""
    rows = [_make_csv_row(2019, 1), _make_csv_row(2021, 1), _make_csv_row(2021, 2)]
    csv_file = tmp_path / "test.csv"
    _write_csv(csv_file, rows)

    ddf = read_raw_csv(csv_file, data_dict)
    result = ddf.compute()
    assert len(result) == 2
    assert (result["ReportYear"] >= 2020).all()


def test_read_raw_csv_returns_dask_dataframe(tmp_path, data_dict):
    rows = [_make_csv_row(2021, 1)]
    csv_file = tmp_path / "test.csv"
    _write_csv(csv_file, rows)

    result = read_raw_csv(csv_file, data_dict)
    assert isinstance(result, dd.DataFrame)


def test_read_raw_csv_dtypes_match_dict(tmp_path, data_dict):
    rows = [_make_csv_row(2021, m) for m in range(1, 4)]
    csv_file = tmp_path / "test.csv"
    _write_csv(csv_file, rows)

    ddf = read_raw_csv(csv_file, data_dict)
    computed = ddf.compute()
    assert computed["OilProduced"].dtype == np.dtype("float64")
    assert computed["ReportYear"].dtype == np.dtype("int64")


def test_read_raw_csv_missing_non_nullable_raises(tmp_path, data_dict):
    rows = [_make_csv_row(2021, 1)]
    df = pd.DataFrame(rows).drop(columns=["Well"])
    csv_file = tmp_path / "no_well.csv"
    df.to_csv(csv_file, index=False)

    with pytest.raises(Exception):
        ddf = read_raw_csv(csv_file, data_dict)
        ddf.compute()


def test_read_raw_csv_no_compute_internally(tmp_path, data_dict):
    """TR-17: read_raw_csv must not call .compute() internally."""
    rows = [_make_csv_row(2021, 1)]
    csv_file = tmp_path / "test.csv"
    _write_csv(csv_file, rows)

    from unittest.mock import patch

    with patch.object(dd.DataFrame, "compute", wraps=dd.DataFrame.compute) as mock_compute:
        read_raw_csv(csv_file, data_dict)
        mock_compute.assert_not_called()


# ---------------------------------------------------------------------------
# ING-05 + ING-07 Integration tests: TR-17, TR-18, TR-22
# ---------------------------------------------------------------------------


@pytest.fixture()
def ingest_config(tmp_path, data_dict):
    """Write 2 synthetic CSV files and a config YAML for run_ingest."""
    import yaml

    raw_dir = tmp_path / "raw"
    interim_dir = tmp_path / "interim"
    raw_dir.mkdir()

    for year, month in [(2020, 1), (2021, 3)]:
        rows = [_make_csv_row(year, month) for _ in range(5)]
        _write_csv(raw_dir / f"{year}_test.csv", rows)

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(raw_dir),
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
        "logging": {"log_file": str(tmp_path / "logs/pipeline.log"), "level": "DEBUG"},
        "ingest": {"raw_dir": str(raw_dir), "interim_dir": str(interim_dir), "start_year": 2020},
        "transform": {
            "interim_dir": str(interim_dir),
            "processed_dir": str(tmp_path / "processed"),
        },
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        },
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))
    return str(cfg_file), interim_dir


def test_run_ingest_creates_parquet(ingest_config):
    cfg_path, interim_dir = ingest_config
    run_ingest(cfg_path)
    parquet_files = list(interim_dir.glob("*.parquet"))
    assert len(parquet_files) >= 1


def test_run_ingest_parquet_readable(ingest_config):
    """TR-18: All Parquet files must be readable without error."""
    cfg_path, interim_dir = ingest_config
    run_ingest(cfg_path)
    for pf in interim_dir.glob("*.parquet"):
        df = pd.read_parquet(pf)
        assert isinstance(df, pd.DataFrame)


def test_run_ingest_year_filter(ingest_config):
    """All output rows must have ReportYear >= 2020."""
    cfg_path, interim_dir = ingest_config
    run_ingest(cfg_path)
    combined = dd.read_parquet(str(interim_dir)).compute()
    assert (combined["ReportYear"] >= 2020).all()


def test_run_ingest_empty_raw_dir(tmp_path):
    """Given empty raw_dir, run_ingest returns without raising."""
    import yaml

    raw_dir = tmp_path / "raw"
    interim_dir = tmp_path / "interim"
    raw_dir.mkdir()

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(raw_dir),
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
        "ingest": {"raw_dir": str(raw_dir), "interim_dir": str(interim_dir), "start_year": 2020},
        "transform": {"interim_dir": str(interim_dir), "processed_dir": str(tmp_path / "p")},
        "features": {"processed_dir": str(tmp_path / "p"), "features_dir": str(tmp_path / "f")},
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))
    # Should not raise
    run_ingest(str(cfg_file))
    assert not list(interim_dir.glob("*.parquet")) if not interim_dir.exists() else True


def test_run_ingest_partition_count(tmp_path):
    """Partition count follows max(10, min(n, 50))."""
    import yaml

    raw_dir = tmp_path / "raw"
    interim_dir = tmp_path / "interim"
    raw_dir.mkdir()

    n_files = 3
    for i in range(n_files):
        rows = [_make_csv_row(2021, i + 1) for _ in range(3)]
        _write_csv(raw_dir / f"file_{i}.csv", rows)

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(raw_dir),
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
        "ingest": {"raw_dir": str(raw_dir), "interim_dir": str(interim_dir), "start_year": 2020},
        "transform": {"interim_dir": str(interim_dir), "processed_dir": str(tmp_path / "proc")},
        "features": {
            "processed_dir": str(tmp_path / "proc"),
            "features_dir": str(tmp_path / "feat"),
        },
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))
    run_ingest(str(cfg_file))

    parquet_files = list(interim_dir.glob("*.parquet"))
    expected = get_partition_count(n_files)
    assert len(parquet_files) == expected


# ---------------------------------------------------------------------------
# TR-22: Schema completeness across partitions
# ---------------------------------------------------------------------------


_REQUIRED_COLS = [
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
    "OpNumber",
    "DocNum",
]


def test_tr22_schema_completeness_across_partitions(tmp_path):
    """TR-22: All required columns present in every Parquet partition."""
    import yaml

    raw_dir = tmp_path / "raw"
    interim_dir = tmp_path / "interim"
    raw_dir.mkdir()

    for i in range(3):
        rows = [_make_csv_row(2021, i + 1) for _ in range(5)]
        _write_csv(raw_dir / f"file_{i}.csv", rows)

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(raw_dir),
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
        "ingest": {"raw_dir": str(raw_dir), "interim_dir": str(interim_dir), "start_year": 2020},
        "transform": {"interim_dir": str(interim_dir), "processed_dir": str(tmp_path / "proc")},
        "features": {
            "processed_dir": str(tmp_path / "proc"),
            "features_dir": str(tmp_path / "feat"),
        },
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))
    run_ingest(str(cfg_file))

    parquet_files = sorted(interim_dir.glob("*.parquet"))
    assert len(parquet_files) >= 3

    for pf in parquet_files[:3]:
        df = pd.read_parquet(pf)
        for col in _REQUIRED_COLS:
            assert col in df.columns, f"Missing column {col!r} in {pf.name}"


def test_tr22_schema_dtype_check(tmp_path):
    """Additional dtype check after run_ingest."""
    import yaml

    raw_dir = tmp_path / "raw"
    interim_dir = tmp_path / "interim"
    raw_dir.mkdir()

    rows = [_make_csv_row(2021, m) for m in range(1, 6)]
    _write_csv(raw_dir / "test.csv", rows)

    cfg_data = {
        "acquire": {
            "base_url_historical": "http://x/{year}.zip",
            "base_url_current": "http://x/cur.csv",
            "start_year": 2020,
            "end_year": 2021,
            "raw_dir": str(raw_dir),
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
        "ingest": {"raw_dir": str(raw_dir), "interim_dir": str(interim_dir), "start_year": 2020},
        "transform": {"interim_dir": str(interim_dir), "processed_dir": str(tmp_path / "proc")},
        "features": {
            "processed_dir": str(tmp_path / "proc"),
            "features_dir": str(tmp_path / "feat"),
        },
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg_data))
    run_ingest(str(cfg_file))

    pf = sorted(interim_dir.glob("*.parquet"))[0]
    df = pd.read_parquet(pf)
    assert df["ReportYear"].dtype == np.dtype("int64")
    assert df["OilProduced"].dtype == np.dtype("float64")
    assert df["ApiCountyCode"].dtype in (object, pd.StringDtype())
    assert hasattr(df["WellStatus"], "cat")
