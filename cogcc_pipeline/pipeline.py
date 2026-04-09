"""Pipeline orchestrator: chains acquire → ingest → transform → features."""

from __future__ import annotations

import argparse
import logging
import time

logger = logging.getLogger(__name__)

ALL_STAGES = ["acquire", "ingest", "transform", "features"]


def run_pipeline(stages: list[str] | None = None) -> None:
    """Run the full pipeline or a subset of stages.

    Args:
        stages: List of stage names to run, or None to run all stages in order.

    Raises:
        RuntimeError: If any stage fails (prevents downstream stages from running).
    """
    from cogcc_pipeline.acquire import load_config, run_acquire, setup_logging

    config = load_config("config.yaml")
    setup_logging(config)

    if stages is None:
        stages_to_run = list(ALL_STAGES)
    else:
        stages_to_run = [s for s in ALL_STAGES if s in stages]

    logger.info("Pipeline starting with stages: %s", stages_to_run)

    client = None
    cluster = None

    try:
        # Run acquire with Dask threaded scheduler (no distributed Client)
        if "acquire" in stages_to_run:
            t0 = time.time()
            logger.info("Stage: acquire")
            try:
                from cogcc_pipeline.acquire import run_acquire
                run_acquire(config)
            except Exception as exc:
                logger.error("Stage acquire failed: %s", exc)
                raise RuntimeError(f"Stage acquire failed: {exc}") from exc
            logger.info("Stage acquire completed in %.1fs", time.time() - t0)

        # Initialise Dask distributed Client before ingest
        if any(s in stages_to_run for s in ["ingest", "transform", "features"]):
            dask_cfg = config["dask"]
            scheduler = dask_cfg.get("scheduler", "local")

            if scheduler == "local":
                from distributed import Client, LocalCluster
                cluster = LocalCluster(
                    n_workers=dask_cfg.get("n_workers", 2),
                    threads_per_worker=dask_cfg.get("threads_per_worker", 2),
                    memory_limit=dask_cfg.get("memory_limit", "3GB"),
                )
                client = Client(cluster)
            else:
                from distributed import Client
                client = Client(scheduler)

            logger.info("Dask Client initialised: dashboard at %s", client.dashboard_link)

        if "ingest" in stages_to_run:
            t0 = time.time()
            logger.info("Stage: ingest")
            try:
                from cogcc_pipeline.ingest import run_ingest
                run_ingest(config)
            except Exception as exc:
                logger.error("Stage ingest failed: %s", exc)
                raise RuntimeError(f"Stage ingest failed: {exc}") from exc
            logger.info("Stage ingest completed in %.1fs", time.time() - t0)

        if "transform" in stages_to_run:
            t0 = time.time()
            logger.info("Stage: transform")
            try:
                from cogcc_pipeline.transform import run_transform
                run_transform(config)
            except Exception as exc:
                logger.error("Stage transform failed: %s", exc)
                raise RuntimeError(f"Stage transform failed: {exc}") from exc
            logger.info("Stage transform completed in %.1fs", time.time() - t0)

        if "features" in stages_to_run:
            t0 = time.time()
            logger.info("Stage: features")
            try:
                from cogcc_pipeline.features import run_features
                run_features(config)
            except Exception as exc:
                logger.error("Stage features failed: %s", exc)
                raise RuntimeError(f"Stage features failed: {exc}") from exc
            logger.info("Stage features completed in %.1fs", time.time() - t0)

        logger.info("Pipeline completed successfully.")

    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        if cluster is not None:
            try:
                cluster.close()
            except Exception:
                pass


def main() -> None:
    """CLI entry point for cogcc-pipeline."""
    parser = argparse.ArgumentParser(
        prog="cogcc-pipeline",
        description="COGCC oil and gas production data pipeline",
    )
    parser.add_argument(
        "--stages",
        type=str,
        default=None,
        help="Comma-separated list of stages to run (e.g. acquire,ingest). Default: all stages.",
    )
    args = parser.parse_args()

    stages: list[str] | None = None
    if args.stages:
        stages = [s.strip() for s in args.stages.split(",")]

    run_pipeline(stages=stages)


if __name__ == "__main__":
    main()
