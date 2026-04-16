"""CLI entry point for the COGCC production data pipeline."""

import argparse
import logging
import sys
import time

from cogcc_pipeline.utils import load_config, setup_logging

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="COGCC production data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=["acquire", "ingest", "transform", "features", "all"],
        default=["all"],
        help="Stages to run (default: all)",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config YAML (default: config/config.yaml)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Override the log level from config",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point registered as cogcc-pipeline in pyproject.toml."""
    args = _parse_args(argv)

    # Determine which stages to run
    if "all" in args.stages:
        stages_to_run = ["acquire", "ingest", "transform", "features"]
    else:
        stages_to_run = args.stages

    # Load config
    config = load_config(args.config)

    # Configure logging before any stage runs (ADR-006)
    log_cfg = config["logging"]
    log_level = args.log_level or log_cfg.get("level", "INFO")
    setup_logging(log_file=log_cfg["log_file"], level=log_level)

    logger.info("Starting COGCC pipeline — stages: %s", stages_to_run)

    client = None

    try:
        # Run acquire (I/O-bound, uses Dask threaded scheduler — no distributed Client needed)
        if "acquire" in stages_to_run:
            from cogcc_pipeline.acquire import run_acquire

            t0 = time.time()
            try:
                run_acquire(args.config)
            except Exception as exc:
                logger.error("Stage 'acquire' failed: %s", exc)
                sys.exit(1)
            logger.info("Stage 'acquire' completed in %.1f s", time.time() - t0)

        # Initialize Dask distributed Client for CPU-bound stages
        if any(s in stages_to_run for s in ("ingest", "transform", "features")):
            from dask.distributed import Client, LocalCluster

            dask_cfg = config["dask"]
            scheduler = dask_cfg.get("scheduler", "local")

            if scheduler == "local":
                cluster = LocalCluster(
                    n_workers=dask_cfg.get("n_workers", 4),
                    threads_per_worker=dask_cfg.get("threads_per_worker", 1),
                    memory_limit=dask_cfg.get("memory_limit", "3GB"),
                    dashboard_address=f":{dask_cfg.get('dashboard_port', 8787)}",
                )
                client = Client(cluster)
            else:
                client = Client(address=scheduler)

            logger.info("Dask dashboard: %s", client.dashboard_link)

        if "ingest" in stages_to_run:
            from cogcc_pipeline.ingest import run_ingest

            t0 = time.time()
            try:
                run_ingest(args.config)
            except Exception as exc:
                logger.error("Stage 'ingest' failed: %s", exc)
                sys.exit(1)
            logger.info("Stage 'ingest' completed in %.1f s", time.time() - t0)

        if "transform" in stages_to_run:
            from cogcc_pipeline.transform import run_transform

            t0 = time.time()
            try:
                run_transform(args.config)
            except Exception as exc:
                logger.error("Stage 'transform' failed: %s", exc)
                sys.exit(1)
            logger.info("Stage 'transform' completed in %.1f s", time.time() - t0)

        if "features" in stages_to_run:
            from cogcc_pipeline.features import run_features

            t0 = time.time()
            try:
                run_features(args.config)
            except Exception as exc:
                logger.error("Stage 'features' failed: %s", exc)
                sys.exit(1)
            logger.info("Stage 'features' completed in %.1f s", time.time() - t0)

        logger.info("Pipeline complete.")

    finally:
        if client is not None:
            client.close()
            logger.info("Dask client closed.")


if __name__ == "__main__":
    main()
