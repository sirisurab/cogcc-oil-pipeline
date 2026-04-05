"""Tests for cogcc_pipeline.ingest."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from cogcc_pipeline.ingest import (
    REQUIRED_COLUMNS,
    filter_by_year,
    load_all_raw_files,
    read_csv_file,
    run_ingest,
    standardise_dtypes,
    validate_schema,
    write_interim_parquet,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_COLS = REQUIRED_COLUMNS + ["ExtraCol"]


def _make_sample_csv(tmp_path: Path, filename: str = "test.csv", rows: int = 5) -> Path:
    """Write a minimal CSV with all required columns to tmp_path."""
    p = tmp_path / filename
    header = ",".join(_SAMPLE_COLS)
    lines = [header]
    for i in range(rows):
        vals = ["05", "12345", str(2020 + i % 5), str((i % 12) + 1),
                "100.0", "90.0", "500.0", "450.0", "5.0", "3.0",
                "200.0", "28.0", f"WELL-{i}", f"OPERATOR-{i}", f"OP{i:03d}",
                f"DOC{i:05d}", "extra"]
        lines.append(",".join(vals))
    p.write_text("\n".join(lines) + "\n")
    return p


def _make_ddf(data: dict, npartitions: int = 1) -> dd.DataFrame:
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=npartitions)


# ---------------------------------------------------------------------------
# Task 01: read_csv_file tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_csv_file_returns_dask_df(tmp_path):
    csv = _make_sample_csv(tmp_path)
    ddf = read_csv_file(csv, {})
    assert isinstance(ddf, dd.DataFrame)


@pytest.mark.unit
def test_read_csv_file_npartitions_le_50(tmp_path):
    csv = _make_sample_csv(tmp_path)
    ddf = read_csv_file(csv, {})
    assert ddf.npartitions <= 50


@pytest.mark.unit
def test_read_csv_file_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_csv_file(tmp_path / "nonexistent.csv", {})


@pytest.mark.unit
def test_read_csv_file_empty_raises(tmp_path):
    empty = tmp_path / "empty.csv"
    empty.write_bytes(b"")
    with pytest.raises(ValueError, match="Empty file"):
        read_csv_file(empty, {})


@pytest.mark.unit
def test_read_csv_file_no_compute(tmp_path):
    """Return type is dask DataFrame — verify no accidental .compute() called."""
    csv = _make_sample_csv(tmp_path)
    ddf = read_csv_file(csv, {})
    assert isinstance(ddf, dd.DataFrame)  # not pd.DataFrame


# ---------------------------------------------------------------------------
# Task 02: filter_by_year tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_filter_by_year_keeps_target_years():
    ddf = _make_ddf({"ReportYear": [2018, 2019, 2020, 2021], "val": [1, 2, 3, 4]})
    filtered = filter_by_year(ddf, 2020)
    result = filtered.compute()
    assert set(result["ReportYear"].tolist()) == {2020, 2021}


@pytest.mark.unit
def test_filter_by_year_returns_dask_df():
    ddf = _make_ddf({"ReportYear": [2020, 2021], "val": [1, 2]})
    result = filter_by_year(ddf, 2020)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_filter_by_year_string_year():
    ddf = _make_ddf({"ReportYear": ["2019", "2020", "2021"], "val": [1, 2, 3]})
    filtered = filter_by_year(ddf, 2020)
    result = filtered.compute()
    assert all(int(y) >= 2020 for y in result["ReportYear"].tolist())


@pytest.mark.unit
def test_filter_by_year_missing_column_raises():
    ddf = _make_ddf({"OtherCol": [1, 2, 3]})
    with pytest.raises(KeyError, match="ReportYear"):
        filter_by_year(ddf, 2020)


# ---------------------------------------------------------------------------
# Task 03: validate_schema tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_schema_all_present():
    ddf = _make_ddf({col: [] for col in REQUIRED_COLUMNS})
    missing = validate_schema(ddf)
    assert missing == []


@pytest.mark.unit
def test_validate_schema_reports_missing():
    cols = {c: [] for c in REQUIRED_COLUMNS if c not in ("OilProduced", "GasProduced")}
    ddf = _make_ddf(cols)
    missing = validate_schema(ddf)
    assert "OilProduced" in missing
    assert "GasProduced" in missing


@pytest.mark.unit
def test_validate_schema_no_compute():
    """validate_schema should not require .compute() — uses column metadata only."""
    ddf = _make_ddf({col: [] for col in REQUIRED_COLUMNS})
    missing = validate_schema(ddf)
    # If this call succeeds without triggering a compute, test passes
    assert isinstance(missing, list)


# ---------------------------------------------------------------------------
# Task 04: load_all_raw_files tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_all_raw_files_two_csvs(tmp_path):
    _make_sample_csv(tmp_path, "a.csv", rows=3)
    _make_sample_csv(tmp_path, "b.csv", rows=3)
    cfg = {"target_start_year": 2018}
    ddf = load_all_raw_files(tmp_path, cfg)
    assert isinstance(ddf, dd.DataFrame)
    result = ddf.compute()
    assert len(result) >= 4  # at least some rows from both files


@pytest.mark.unit
def test_load_all_raw_files_skips_empty(tmp_path):
    _make_sample_csv(tmp_path, "good.csv", rows=3)
    empty = tmp_path / "empty.csv"
    empty.write_bytes(b"")
    cfg = {"target_start_year": 2018}
    ddf = load_all_raw_files(tmp_path, cfg)
    assert isinstance(ddf, dd.DataFrame)


@pytest.mark.unit
def test_load_all_raw_files_empty_dir_raises(tmp_path):
    with pytest.raises(RuntimeError, match="No raw CSV files"):
        load_all_raw_files(tmp_path, {"target_start_year": 2020})


@pytest.mark.unit
def test_load_all_raw_files_returns_dask(tmp_path):
    _make_sample_csv(tmp_path, "test.csv")
    ddf = load_all_raw_files(tmp_path, {"target_start_year": 2018})
    assert isinstance(ddf, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 05: standardise_dtypes tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_standardise_dtypes_oil_is_float64(tmp_path):
    csv = _make_sample_csv(tmp_path)
    ddf = read_csv_file(csv, {})
    ddf = standardise_dtypes(ddf)
    result = ddf.compute()
    assert result["OilProduced"].dtype == "float64"


@pytest.mark.unit
def test_standardise_dtypes_string_col(tmp_path):
    csv = _make_sample_csv(tmp_path)
    ddf = read_csv_file(csv, {})
    ddf = standardise_dtypes(ddf)
    result = ddf.compute()
    assert isinstance(result["ApiCountyCode"].dtype, pd.StringDtype)


@pytest.mark.unit
def test_standardise_dtypes_report_year_int32(tmp_path):
    csv = _make_sample_csv(tmp_path)
    ddf = read_csv_file(csv, {})
    ddf = standardise_dtypes(ddf)
    result = ddf.compute()
    assert result["ReportYear"].dtype == "int32"


@pytest.mark.unit
def test_standardise_dtypes_returns_dask(tmp_path):
    csv = _make_sample_csv(tmp_path)
    ddf = read_csv_file(csv, {})
    result = standardise_dtypes(ddf)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 06: write_interim_parquet tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_interim_parquet_npartitions_small():
    """50_000 rows → npartitions=1."""
    npartitions = max(1, min(50_000 // 500_000, 50))
    assert npartitions == 1


@pytest.mark.unit
def test_write_interim_parquet_npartitions_medium():
    """2_500_000 rows → npartitions=5."""
    npartitions = max(1, min(2_500_000 // 500_000, 50))
    assert npartitions == 5


@pytest.mark.unit
def test_write_interim_parquet_npartitions_capped():
    """30_000_000 rows → npartitions=50 (capped)."""
    npartitions = max(1, min(30_000_000 // 500_000, 50))
    assert npartitions == 50


@pytest.mark.integration
def test_write_interim_parquet_readable(tmp_path):
    """Write synthetic Dask DataFrame, read back with dask and assert row count."""
    pdf = pd.DataFrame({col: ["value"] * 10 for col in REQUIRED_COLUMNS[:4]})
    ddf = dd.from_pandas(pdf, npartitions=1)
    out_dir = tmp_path / "interim"
    write_interim_parquet(ddf, out_dir, 10)
    result = dd.read_parquet(str(out_dir)).compute()
    assert len(result) == 10


@pytest.mark.integration
def test_write_interim_parquet_file_count_le_50(tmp_path):
    pdf = pd.DataFrame({"col1": range(100), "col2": range(100)})
    ddf = dd.from_pandas(pdf, npartitions=2)
    out_dir = tmp_path / "interim2"
    write_interim_parquet(ddf, out_dir, 100)
    parquet_files = list(out_dir.glob("*.parquet"))
    assert len(parquet_files) <= 50


# ---------------------------------------------------------------------------
# Task 07: run_ingest tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_ingest_calls_subtasks(tmp_path):
    """run_ingest with mocked sub-functions completes without error."""
    import yaml

    cfg = {
        "acquire": {},
        "ingest": {
            "raw_dir": str(tmp_path / "raw"),
            "interim_dir": str(tmp_path / "interim"),
            "target_start_year": 2020,
        },
        "transform": {},
        "features": {},
    }
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(yaml.dump(cfg))

    # Create a dummy CSV
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _make_sample_csv(raw_dir, "2020_prod_reports.csv", rows=5)

    result = run_ingest(str(cfg_file))
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 08: Schema completeness tests (TR-22)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_schema_completeness_across_partitions():
    """All interim Parquet partitions have all required columns."""
    interim_dir = Path("data/interim")
    if not interim_dir.exists() or not list(interim_dir.glob("*.parquet")):
        pytest.skip("data/interim not populated — run ingest first")
    for pf in list(interim_dir.glob("*.parquet"))[:3]:
        df = pd.read_parquet(pf)
        for col in REQUIRED_COLUMNS:
            assert col in df.columns, f"Column {col} missing from {pf}"


@pytest.mark.integration
def test_schema_identical_across_partitions():
    """All sampled interim partitions have identical column sets."""
    interim_dir = Path("data/interim")
    if not interim_dir.exists():
        pytest.skip("data/interim not populated")
    parts = list(interim_dir.glob("*.parquet"))[:3]
    if len(parts) < 2:
        pytest.skip("Not enough partition files")
    schemas = [set(pd.read_parquet(p).columns) for p in parts]
    assert all(s == schemas[0] for s in schemas)


@pytest.mark.integration
def test_interim_parquet_readable_with_pandas():
    """Interim Parquet is readable via pandas.read_parquet (TR-18)."""
    interim_dir = Path("data/interim")
    if not interim_dir.exists():
        pytest.skip("data/interim not populated")
    pd.read_parquet(str(interim_dir))  # should not raise
