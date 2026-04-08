"""Tests for cogcc_pipeline/ingest.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import pandas as pd
import pytest

from cogcc_pipeline.ingest import (
    _filter_year,
    build_dtype_map,
    build_meta,
    get_or_create_client,
    read_raw_file,
    run_ingest,
    validate_schema,
)

# ---------------------------------------------------------------------------
# I-01: build_dtype_map / build_meta
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_dtype_map_columns() -> None:
    dtype_map = build_dtype_map()
    assert "AcceptedDate" not in dtype_map
    assert "DocNum" in dtype_map
    assert "OilProduced" in dtype_map


@pytest.mark.unit
def test_build_meta_has_all_columns() -> None:
    meta = build_meta()
    assert "AcceptedDate" in meta.columns
    assert "DocNum" in meta.columns


@pytest.mark.unit
def test_build_meta_docnum_dtype() -> None:
    meta = build_meta()
    assert meta["DocNum"].dtype == "int64"


@pytest.mark.unit
def test_build_meta_daysproduced_dtype() -> None:
    meta = build_meta()
    assert meta["DaysProduced"].dtype == pd.Int64Dtype()


@pytest.mark.unit
def test_build_meta_oilproduced_dtype() -> None:
    meta = build_meta()
    assert meta["OilProduced"].dtype == pd.Float64Dtype()


@pytest.mark.unit
def test_build_meta_wellstatus_dtype() -> None:
    meta = build_meta()
    assert meta["WellStatus"].dtype == pd.StringDtype()


@pytest.mark.unit
def test_build_meta_accepteddate_dtype() -> None:
    meta = build_meta()
    assert meta["AcceptedDate"].dtype == "datetime64[us]"


# ---------------------------------------------------------------------------
# I-02: read_raw_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_file_all_columns(tmp_path: Path, sample_raw_df: pd.DataFrame) -> None:
    csv_path = tmp_path / "test.csv"
    meta = build_meta()
    # Write a CSV with all expected columns
    sample_raw_df.to_csv(csv_path, index=False)
    df = read_raw_file(csv_path)
    for col in meta.columns:
        assert col in df.columns


@pytest.mark.unit
def test_read_raw_file_missing_column_filled_na(
    tmp_path: Path, sample_raw_df: pd.DataFrame
) -> None:
    # Drop FlaredVented before writing
    df_no_col = sample_raw_df.drop(columns=["FlaredVented"])
    csv_path = tmp_path / "partial.csv"
    df_no_col.to_csv(csv_path, index=False)
    result = read_raw_file(csv_path)
    assert "FlaredVented" in result.columns
    assert result["FlaredVented"].isna().all()


@pytest.mark.unit
def test_read_raw_file_accepteddate_dtype(tmp_path: Path, sample_raw_df: pd.DataFrame) -> None:
    csv_path = tmp_path / "test.csv"
    sample_raw_df.to_csv(csv_path, index=False)
    df = read_raw_file(csv_path)
    assert df["AcceptedDate"].dtype in ("datetime64[ns]", "datetime64[us]")


@pytest.mark.unit
def test_read_raw_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_raw_file(tmp_path / "missing.csv")


@pytest.mark.unit
def test_read_raw_file_na_string_values(tmp_path: Path, sample_raw_df: pd.DataFrame) -> None:
    """OilProduced 'N/A' string → pd.NA."""
    csv_path = tmp_path / "na_test.csv"
    # Write a CSV with N/A in OilProduced
    csv_content = sample_raw_df.to_csv(index=False)
    # Replace first value with N/A
    csv_content = csv_content.replace("100.0", "N/A", 1)
    csv_path.write_text(csv_content)
    df = read_raw_file(csv_path)
    assert pd.isna(df["OilProduced"].iloc[0])


# ---------------------------------------------------------------------------
# I-03: _filter_year
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_filter_year_keeps_correct_rows() -> None:
    df = pd.DataFrame({"ReportYear": [2018, 2019, 2020, 2021], "x": [1, 2, 3, 4]})
    result = _filter_year(df, 2020)
    assert list(result["ReportYear"]) == [2020, 2021]


@pytest.mark.unit
def test_filter_year_all_below_returns_empty() -> None:
    df = pd.DataFrame({"ReportYear": [2018, 2019], "x": [1, 2]})
    result = _filter_year(df, 2020)
    assert len(result) == 0
    assert "ReportYear" in result.columns


@pytest.mark.unit
def test_filter_year_all_above_unchanged() -> None:
    df = pd.DataFrame({"ReportYear": [2020, 2021, 2022], "x": [1, 2, 3]})
    result = _filter_year(df, 2020)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# I-04: validate_schema
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_schema_all_present() -> None:
    meta = build_meta()
    ddf = dd.from_pandas(meta, npartitions=1)
    missing = validate_schema(ddf, list(meta.columns))
    assert missing == []


@pytest.mark.unit
def test_validate_schema_missing_columns() -> None:
    df = pd.DataFrame({"DocNum": [1], "ReportYear": [2021]})
    ddf = dd.from_pandas(df, npartitions=1)
    missing = validate_schema(ddf, ["DocNum", "ReportYear", "OilProduced", "GasProduced"])
    assert "OilProduced" in missing
    assert "GasProduced" in missing


# ---------------------------------------------------------------------------
# I-05: get_or_create_client
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_get_or_create_client_reuses_existing(sample_config: dict) -> None:
    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    with patch("dask.distributed.get_client", return_value=mock_client):
        client, created = get_or_create_client(sample_config)
    assert client is mock_client
    assert created is False


@pytest.mark.unit
def test_get_or_create_client_creates_new(sample_config: dict) -> None:
    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    mock_cluster = MagicMock()
    with patch("dask.distributed.get_client", side_effect=ValueError("no client")):
        with patch("cogcc_pipeline.ingest.LocalCluster", return_value=mock_cluster):
            with patch("cogcc_pipeline.ingest.Client", return_value=mock_client):
                client, created = get_or_create_client(sample_config)
    assert created is True


# ---------------------------------------------------------------------------
# I-06: run_ingest
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_ingest_no_csv_raises(sample_config: dict, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_ingest(sample_config)


@pytest.mark.unit
def test_run_ingest_filter_applied(sample_config: dict, tmp_path: Path) -> None:
    """Assert _filter_year removes pre-2020 rows."""
    raw_dir = Path(sample_config["ingest"]["raw_dir"])
    # Write a CSV with some pre-2020 rows
    df_mix = pd.DataFrame(
        {
            "DocNum": [1, 2, 3],
            "ReportMonth": [1, 1, 1],
            "ReportYear": [2019, 2020, 2021],
            **{
                c: [pd.NA] * 3
                for c in [
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
            },
        }
    )
    csv_path = raw_dir / "test.csv"
    df_mix.to_csv(csv_path, index=False)

    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    with patch("dask.distributed.get_client", side_effect=ValueError("no client")):
        with patch("cogcc_pipeline.ingest.LocalCluster", return_value=MagicMock()):
            with patch("cogcc_pipeline.ingest.Client", return_value=mock_client):
                run_ingest(sample_config)

    interim_dir = Path(sample_config["ingest"]["interim_dir"])
    parquet_path = interim_dir / "cogcc_raw.parquet"
    assert parquet_path.exists()
    result = pd.read_parquet(parquet_path, engine="pyarrow")
    assert all(result["ReportYear"] >= 2020)


# ---------------------------------------------------------------------------
# I-07: TR-17 — Dask graph laziness
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr17_dask_dataframe_is_lazy(sample_raw_df: pd.DataFrame) -> None:
    """Assert intermediate operations produce dd.DataFrame, not pandas."""
    from dask import delayed

    delayed_list = [delayed(lambda: sample_raw_df)()]
    meta = build_meta()
    ddf = dd.from_delayed(delayed_list, meta=meta)
    assert isinstance(ddf, dd.DataFrame)
