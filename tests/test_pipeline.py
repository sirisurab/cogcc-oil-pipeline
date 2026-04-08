"""Tests for cogcc_pipeline/pipeline.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cogcc_pipeline.pipeline import (
    bootstrap_directories,
    init_dask_client,
    main,
    run_pipeline,
    write_run_report,
)

# ---------------------------------------------------------------------------
# O-01: write_run_report
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_run_report_creates_file(tmp_path: Path) -> None:
    report = {
        "pipeline_start": "2024-01-01T00:00:00+00:00",
        "pipeline_end": "2024-01-01T01:00:00+00:00",
        "stages": [
            {"name": "acquire", "status": "success", "duration_seconds": 1.0, "error": None}
        ],
        "overall_status": "success",
    }
    output_path = tmp_path / "report.json"
    write_run_report(report, output_path)
    assert output_path.exists()


@pytest.mark.unit
def test_write_run_report_valid_json(tmp_path: Path) -> None:
    report: dict = {
        "pipeline_start": "2024-01-01T00:00:00+00:00",
        "pipeline_end": None,
        "stages": [],
        "overall_status": "failed",
    }
    output_path = tmp_path / "subdir" / "report.json"
    write_run_report(report, output_path)
    data = json.loads(output_path.read_text())
    assert "pipeline_start" in data
    assert "overall_status" in data
    assert "stages" in data


# ---------------------------------------------------------------------------
# O-02: init_dask_client
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_init_dask_client_local(sample_config: dict) -> None:
    mock_cluster = MagicMock()
    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    with patch("cogcc_pipeline.pipeline.LocalCluster", return_value=mock_cluster) as mock_lc:
        with patch("cogcc_pipeline.pipeline.Client", return_value=mock_client) as mock_c:
            client = init_dask_client(sample_config)
    mock_lc.assert_called_once()
    mock_c.assert_called_once_with(mock_cluster)
    assert client is mock_client


@pytest.mark.unit
def test_init_dask_client_tcp(sample_config: dict) -> None:
    cfg = dict(sample_config)
    cfg["dask"] = dict(sample_config["dask"])
    cfg["dask"]["scheduler"] = "tcp://localhost:8786"
    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    with patch("cogcc_pipeline.pipeline.Client", return_value=mock_client) as mock_c:
        client = init_dask_client(cfg)
    mock_c.assert_called_once_with("tcp://localhost:8786")
    assert client is mock_client


# ---------------------------------------------------------------------------
# O-03: bootstrap_directories
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_bootstrap_directories_creates_all(sample_config: dict, tmp_path: Path) -> None:
    bootstrap_directories(sample_config)
    assert Path(sample_config["acquire"]["raw_dir"]).exists()
    assert Path(sample_config["ingest"]["interim_dir"]).exists()
    assert Path(sample_config["features"]["processed_dir"]).exists()


@pytest.mark.unit
def test_bootstrap_directories_idempotent(sample_config: dict) -> None:
    bootstrap_directories(sample_config)
    bootstrap_directories(sample_config)  # Should not raise


# ---------------------------------------------------------------------------
# O-04: run_pipeline
# ---------------------------------------------------------------------------


def _make_pipeline_mocks(config_path: Path, sample_config: dict):
    """Helper that patches all external pipeline dependencies."""
    return [
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_acquire"),
        patch("cogcc_pipeline.pipeline.run_ingest"),
        patch("cogcc_pipeline.pipeline.run_transform"),
        patch("cogcc_pipeline.pipeline.run_features"),
        patch("cogcc_pipeline.pipeline.init_dask_client"),
        patch("cogcc_pipeline.pipeline.write_run_report"),
    ]


@pytest.mark.unit
def test_run_pipeline_all_stages_called_in_order(sample_config: dict, tmp_path: Path) -> None:
    call_order = []
    mock_client = MagicMock()

    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch(
            "cogcc_pipeline.pipeline.run_acquire",
            side_effect=lambda c: call_order.append("acquire"),
        ),
        patch(
            "cogcc_pipeline.pipeline.run_ingest", side_effect=lambda c: call_order.append("ingest")
        ),
        patch(
            "cogcc_pipeline.pipeline.run_transform",
            side_effect=lambda c: call_order.append("transform"),
        ),
        patch(
            "cogcc_pipeline.pipeline.run_features",
            side_effect=lambda c: call_order.append("features"),
        ),
        patch("cogcc_pipeline.pipeline.init_dask_client", return_value=mock_client),
        patch("cogcc_pipeline.pipeline.write_run_report"),
    ):
        run_pipeline(stages=None, config_path=Path("config.yaml"))

    assert call_order == ["acquire", "ingest", "transform", "features"]


@pytest.mark.unit
def test_run_pipeline_acquire_failure_stops_pipeline(sample_config: dict) -> None:
    mock_client = MagicMock()
    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_acquire", side_effect=RuntimeError("acquire down")),
        patch("cogcc_pipeline.pipeline.run_ingest") as mock_ingest,
        patch("cogcc_pipeline.pipeline.init_dask_client", return_value=mock_client),
        patch("cogcc_pipeline.pipeline.write_run_report") as mock_report,
    ):
        with pytest.raises(RuntimeError, match="acquire"):
            run_pipeline(stages=None, config_path=Path("config.yaml"))

    mock_ingest.assert_not_called()
    mock_report.assert_called_once()
    args = mock_report.call_args[0]
    stages = args[0]["stages"]
    acquire_rec = next(s for s in stages if s["name"] == "acquire")
    assert acquire_rec["status"] == "failed"


@pytest.mark.unit
def test_run_pipeline_stage_selection(sample_config: dict) -> None:
    mock_client = MagicMock()
    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_acquire") as mock_acq,
        patch("cogcc_pipeline.pipeline.run_ingest") as mock_ing,
        patch("cogcc_pipeline.pipeline.run_transform") as mock_tr,
        patch("cogcc_pipeline.pipeline.run_features") as mock_ft,
        patch("cogcc_pipeline.pipeline.init_dask_client", return_value=mock_client),
        patch("cogcc_pipeline.pipeline.write_run_report"),
    ):
        run_pipeline(stages=["ingest", "transform"], config_path=Path("config.yaml"))

    mock_acq.assert_not_called()
    mock_ing.assert_called_once()
    mock_tr.assert_called_once()
    mock_ft.assert_not_called()


@pytest.mark.unit
def test_run_pipeline_no_dask_client_for_acquire_only(sample_config: dict) -> None:
    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_acquire"),
        patch("cogcc_pipeline.pipeline.init_dask_client") as mock_dask,
        patch("cogcc_pipeline.pipeline.write_run_report"),
    ):
        run_pipeline(stages=["acquire"], config_path=Path("config.yaml"))

    mock_dask.assert_not_called()


@pytest.mark.unit
def test_run_pipeline_dask_client_initialized_for_ingest(sample_config: dict) -> None:
    mock_client = MagicMock()
    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_ingest"),
        patch("cogcc_pipeline.pipeline.init_dask_client", return_value=mock_client) as mock_dask,
        patch("cogcc_pipeline.pipeline.write_run_report"),
    ):
        run_pipeline(stages=["ingest"], config_path=Path("config.yaml"))

    mock_dask.assert_called_once()


@pytest.mark.unit
def test_run_pipeline_report_written_on_failure(sample_config: dict) -> None:
    mock_client = MagicMock()
    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_acquire"),
        patch("cogcc_pipeline.pipeline.run_ingest", side_effect=RuntimeError("fail")),
        patch("cogcc_pipeline.pipeline.init_dask_client", return_value=mock_client),
        patch("cogcc_pipeline.pipeline.write_run_report") as mock_report,
    ):
        with pytest.raises(RuntimeError):
            run_pipeline(stages=None, config_path=Path("config.yaml"))

    mock_report.assert_called()


@pytest.mark.unit
def test_run_pipeline_raises_on_stage_failure(sample_config: dict) -> None:
    mock_client = MagicMock()
    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_acquire"),
        patch("cogcc_pipeline.pipeline.run_ingest", side_effect=RuntimeError("boom")),
        patch("cogcc_pipeline.pipeline.init_dask_client", return_value=mock_client),
        patch("cogcc_pipeline.pipeline.write_run_report"),
    ):
        with pytest.raises(RuntimeError):
            run_pipeline(stages=None, config_path=Path("config.yaml"))


# ---------------------------------------------------------------------------
# O-05 + O-07: CLI tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_main_stages_passed_to_run_pipeline(sample_config: dict) -> None:
    with patch("sys.argv", ["cogcc-pipeline", "--stages", "acquire", "ingest"]):
        with patch("cogcc_pipeline.pipeline.run_pipeline") as mock_rp:
            mock_rp.return_value = None
            with patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config):
                main()
    mock_rp.assert_called_once()
    call_stages = mock_rp.call_args[1].get("stages") or mock_rp.call_args[0][0]
    assert set(call_stages) == {"acquire", "ingest"}


@pytest.mark.unit
def test_main_no_stages_calls_run_pipeline_with_none(sample_config: dict) -> None:
    with patch("sys.argv", ["cogcc-pipeline"]):
        with patch("cogcc_pipeline.pipeline.run_pipeline") as mock_rp:
            main()
    mock_rp.assert_called_once()


@pytest.mark.unit
def test_main_invalid_stage_exits_1() -> None:
    with patch("sys.argv", ["cogcc-pipeline", "--stages", "invalid_stage"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
    assert exc_info.value.code == 2 or exc_info.value.code == 1


@pytest.mark.unit
def test_main_config_path_passed(sample_config: dict) -> None:
    with patch("sys.argv", ["cogcc-pipeline", "--config", "path/to/config.yaml"]):
        with patch("cogcc_pipeline.pipeline.run_pipeline") as mock_rp:
            main()
    mock_rp.assert_called_once()
    kwargs = mock_rp.call_args[1]
    assert kwargs.get("config_path") == Path("path/to/config.yaml")


# ---------------------------------------------------------------------------
# O-07: Stage sequencing / client lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_stage_skipping(sample_config: dict) -> None:
    """transform and features skipped when stages=["acquire", "ingest"]."""
    mock_client = MagicMock()
    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_acquire"),
        patch("cogcc_pipeline.pipeline.run_ingest"),
        patch("cogcc_pipeline.pipeline.run_transform") as mock_tr,
        patch("cogcc_pipeline.pipeline.run_features") as mock_ft,
        patch("cogcc_pipeline.pipeline.init_dask_client", return_value=mock_client),
        patch("cogcc_pipeline.pipeline.write_run_report"),
    ):
        run_pipeline(stages=["acquire", "ingest"], config_path=Path("config.yaml"))

    mock_tr.assert_not_called()
    mock_ft.assert_not_called()


@pytest.mark.unit
def test_client_lifecycle(sample_config: dict) -> None:
    """init_dask_client called once; client.close() called after features."""
    mock_client = MagicMock()
    mock_client.dashboard_link = "http://localhost:8787"
    with (
        patch("cogcc_pipeline.pipeline.load_config", return_value=sample_config),
        patch("cogcc_pipeline.pipeline.setup_logging"),
        patch("cogcc_pipeline.pipeline.bootstrap_directories"),
        patch("cogcc_pipeline.pipeline.run_acquire"),
        patch("cogcc_pipeline.pipeline.run_ingest"),
        patch("cogcc_pipeline.pipeline.run_transform"),
        patch("cogcc_pipeline.pipeline.run_features"),
        patch("cogcc_pipeline.pipeline.init_dask_client", return_value=mock_client) as mock_init,
        patch("cogcc_pipeline.pipeline.write_run_report"),
    ):
        run_pipeline(stages=None, config_path=Path("config.yaml"))

    mock_init.assert_called_once()
    mock_client.close.assert_called_once()
