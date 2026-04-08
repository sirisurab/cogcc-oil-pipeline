"""Pipeline orchestrator: CLI entry point and stage sequencing."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dask.distributed import Client, LocalCluster

from cogcc_pipeline.acquire import load_config, run_acquire, setup_logging
from cogcc_pipeline.features import run_features
from cogcc_pipeline.ingest import run_ingest
from cogcc_pipeline.transform import run_transform

logger = logging.getLogger(__name__)

VALID_STAGES = ["acquire", "ingest", "transform", "features"]


# ---------------------------------------------------------------------------
# O-01: Pipeline run report writer
# ---------------------------------------------------------------------------

def write_run_report(report: dict, output_path: Path) -> None:
    """Write pipeline run report as indented JSON.

    Args:
        report: Dict with pipeline_start, pipeline_end, stages, overall_status.
        output_path: Path to write JSON file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Run report written to %s", output_path)


# ---------------------------------------------------------------------------
# O-02: Dask distributed client initialiser
# ---------------------------------------------------------------------------

def init_dask_client(config: dict) -> Client:
    """Initialize a Dask distributed Client from config.

    Creates a LocalCluster if scheduler is "local"; connects to remote URL otherwise.

    Args:
        config: Full config dict.

    Returns:
        Initialized distributed.Client.
    """
    dcfg = config["dask"]
    scheduler = dcfg["scheduler"]

    if scheduler == "local":
        cluster = LocalCluster(
            n_workers=dcfg["n_workers"],
            threads_per_worker=dcfg["threads_per_worker"],
            memory_limit=dcfg["memory_limit"],
            dashboard_address=f":{dcfg['dashboard_port']}",
        )
        client = Client(cluster)
    elif scheduler.startswith("tcp://"):
        client = Client(scheduler)
    else:
        client = Client(scheduler)

    logger.info("Dask dashboard: %s", client.dashboard_link)
    return client


# ---------------------------------------------------------------------------
# O-03: Directory bootstrapper
# ---------------------------------------------------------------------------

def bootstrap_directories(config: dict) -> None:
    """Create all required data and log directories if absent.

    Args:
        config: Full config dict.
    """
    dirs = [
        Path(config["acquire"]["raw_dir"]),
        Path(config["ingest"]["interim_dir"]),
        Path(config["features"]["processed_dir"]),
        Path(config["logging"]["log_file"]).parent,
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        logger.debug("Directory ready: %s", d)


# ---------------------------------------------------------------------------
# O-04: Pipeline stage runner
# ---------------------------------------------------------------------------

def run_pipeline(
    stages: list[str] | None = None,
    config_path: Path = Path("config.yaml"),
) -> None:
    """Orchestrate pipeline stages in canonical order.

    Args:
        stages: List of stage names to run; None runs all stages.
        config_path: Path to config.yaml.

    Raises:
        RuntimeError: If any stage fails; downstream stages are not executed.
    """
    config = load_config(config_path)
    setup_logging(config)
    bootstrap_directories(config)

    # Canonical order, filter to requested stages
    if stages is None:
        stages_to_run = VALID_STAGES[:]
    else:
        stages_to_run = [s for s in VALID_STAGES if s in stages]

    pipeline_start = datetime.now(tz=timezone.utc).isoformat()
    processed_dir = Path(config["features"]["processed_dir"])
    report_path = processed_dir / "pipeline_run_report.json"

    stage_records: list[dict] = []
    overall_status = "success"
    client: Client | None = None

    report: dict = {
        "pipeline_start": pipeline_start,
        "pipeline_end": None,
        "stages": stage_records,
        "overall_status": overall_status,
    }

    def _record(name: str, status: str, duration: float | None, error: str | None) -> None:
        stage_records.append({
            "name": name,
            "status": status,
            "duration_seconds": duration,
            "error": error,
        })

    try:
        # Acquire (uses Dask threaded scheduler, no distributed client)
        if "acquire" in stages_to_run:
            t0 = time.perf_counter()
            try:
                run_acquire(config)
                _record("acquire", "success", time.perf_counter() - t0, None)
                logger.info("Stage acquire completed in %.2fs", time.perf_counter() - t0)
            except Exception as exc:
                _record("acquire", "failed", time.perf_counter() - t0, str(exc))
                report["overall_status"] = "failed"
                report["pipeline_end"] = datetime.now(tz=timezone.utc).isoformat()
                write_run_report(report, report_path)
                raise RuntimeError(f"Stage 'acquire' failed: {exc}") from exc

        # Initialize Dask distributed client for ingest/transform/features
        needs_client = any(s in stages_to_run for s in ("ingest", "transform", "features"))
        if needs_client:
            client = init_dask_client(config)

        # Ingest
        if "ingest" in stages_to_run:
            t0 = time.perf_counter()
            try:
                run_ingest(config)
                _record("ingest", "success", time.perf_counter() - t0, None)
            except Exception as exc:
                _record("ingest", "failed", time.perf_counter() - t0, str(exc))
                report["overall_status"] = "failed"
                report["pipeline_end"] = datetime.now(tz=timezone.utc).isoformat()
                if client:
                    client.close()
                write_run_report(report, report_path)
                raise RuntimeError(f"Stage 'ingest' failed: {exc}") from exc

        # Transform
        if "transform" in stages_to_run:
            t0 = time.perf_counter()
            try:
                run_transform(config)
                _record("transform", "success", time.perf_counter() - t0, None)
            except Exception as exc:
                _record("transform", "failed", time.perf_counter() - t0, str(exc))
                report["overall_status"] = "failed"
                report["pipeline_end"] = datetime.now(tz=timezone.utc).isoformat()
                if client:
                    client.close()
                write_run_report(report, report_path)
                raise RuntimeError(f"Stage 'transform' failed: {exc}") from exc

        # Features
        if "features" in stages_to_run:
            t0 = time.perf_counter()
            try:
                run_features(config)
                _record("features", "success", time.perf_counter() - t0, None)
            except Exception as exc:
                _record("features", "failed", time.perf_counter() - t0, str(exc))
                report["overall_status"] = "failed"
                report["pipeline_end"] = datetime.now(tz=timezone.utc).isoformat()
                if client:
                    client.close()
                write_run_report(report, report_path)
                raise RuntimeError(f"Stage 'features' failed: {exc}") from exc

    finally:
        if client is not None:
            client.close()

    report["pipeline_end"] = datetime.now(tz=timezone.utc).isoformat()
    report["overall_status"] = "success"
    write_run_report(report, report_path)
    logger.info("Pipeline complete. Status: success")


# ---------------------------------------------------------------------------
# O-05: CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point: cogcc-pipeline [--stages STAGE ...] [--config PATH]."""
    parser = argparse.ArgumentParser(
        description="COGCC oil and gas production data pipeline",
        prog="cogcc-pipeline",
    )
    parser.add_argument(
        "--stages",
        nargs="*",
        choices=VALID_STAGES,
        default=None,
        metavar="STAGE",
        help=f"Stages to run: {VALID_STAGES}. Defaults to all.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config YAML file (default: config.yaml).",
    )

    args = parser.parse_args()

    if args.stages is not None:
        invalid = [s for s in args.stages if s not in VALID_STAGES]
        if invalid:
            print(f"Error: invalid stage(s): {invalid}. Valid: {VALID_STAGES}", file=sys.stderr)
            sys.exit(1)

    stages = args.stages if args.stages else None
    run_pipeline(stages=stages, config_path=Path(args.config))
