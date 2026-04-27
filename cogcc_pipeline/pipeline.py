"""Pipeline entry point: orchestrate all stages with logging and Dask scheduler."""

import argparse
import logging
import sys
import time
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

ALL_STAGES = ["acquire", "ingest", "transform", "features"]
DISTRIBUTED_STAGES = {"ingest", "transform", "features"}


def _setup_logging(log_file: str, level: str) -> None:
    """Set up dual-channel logging: console + file (ADR-006)."""
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Remove existing handlers to avoid duplicate output on re-invocation
    root.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)


def _init_dask_client(dask_cfg: dict) -> object:
    """Initialize Dask distributed client (ADR-001, build-env-manifest)."""
    from dask.distributed import Client, LocalCluster

    scheduler: str = str(dask_cfg.get("scheduler", "local"))

    if scheduler == "local":
        cluster = LocalCluster(
            n_workers=int(dask_cfg.get("n_workers", 4)),
            threads_per_worker=int(dask_cfg.get("threads_per_worker", 2)),
            memory_limit=str(dask_cfg.get("memory_limit", "3GB")),
            dashboard_address=f":{int(dask_cfg.get('dashboard_port', 8787))}",
        )
        client = Client(cluster)
    else:
        client = Client(scheduler)

    if isinstance(client.dashboard_link, str):
        logger.info("Dask dashboard: %s", client.dashboard_link)
    return client


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for the COGCC pipeline."""
    parser = argparse.ArgumentParser(description="COGCC production data pipeline")
    parser.add_argument(
        "stages",
        nargs="*",
        choices=ALL_STAGES,
        default=ALL_STAGES,
        help="Stages to run (default: all four)",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    args = parser.parse_args(argv)
    stages: list[str] = args.stages if args.stages else ALL_STAGES

    # Load configuration
    with open(args.config) as f:
        config: dict = yaml.safe_load(f)

    # Set up logging before any stage runs (ADR-006)
    log_cfg = config.get("logging", {})
    _setup_logging(
        log_file=str(log_cfg.get("log_file", "logs/pipeline.log")),
        level=str(log_cfg.get("level", "INFO")),
    )
    logger.info("Starting pipeline — stages: %s", stages)

    # Initialize distributed scheduler once if any distributed stage is requested
    # (build-env-manifest, ADR-001)
    client = None
    needs_distributed = any(s in DISTRIBUTED_STAGES for s in stages)
    if needs_distributed:
        dask_cfg = config.get("dask", {})
        try:
            client = _init_dask_client(dask_cfg)
            logger.info("Dask distributed scheduler initialized")
        except Exception as exc:
            logger.warning("Could not initialize Dask distributed client: %s", exc)

    # Import stage functions
    from cogcc_pipeline.acquire import acquire
    from cogcc_pipeline.features import features
    from cogcc_pipeline.ingest import ingest
    from cogcc_pipeline.transform import transform

    stage_fn = {
        "acquire": acquire,
        "ingest": ingest,
        "transform": transform,
        "features": features,
    }

    for stage in stages:
        fn = stage_fn[stage]
        logger.info("Starting stage: %s", stage)
        t0 = time.time()
        try:
            fn(config)
            elapsed = time.time() - t0
            logger.info("Stage '%s' completed in %.1fs", stage, elapsed)
        except Exception as exc:
            elapsed = time.time() - t0
            logger.error(
                "Stage '%s' failed after %.1fs: %s", stage, elapsed, exc, exc_info=True
            )
            sys.exit(1)

    logger.info("Pipeline finished successfully")

    if client is not None:
        try:
            if hasattr(client, "close"):
                client.close()  # type: ignore[union-attr]
        except Exception:
            pass
