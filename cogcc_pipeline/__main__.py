"""Pipeline entry point — runs all stages in order or a subset."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import yaml
from distributed import Client, LocalCluster

logger = logging.getLogger(__name__)

ALL_STAGES = ["acquire", "ingest", "transform", "features"]


def setup_logging(cfg: dict) -> None:
    """Configure dual-channel logging (file + console) from config."""
    log_cfg = cfg.get("logging", {})
    level_name: str = log_cfg.get("level", "INFO")
    log_file: str = log_cfg.get("log_file", "logs/pipeline.log")

    level = getattr(logging, level_name.upper(), logging.INFO)
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s")

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    root.addHandler(fh)


def _init_dask_client(cfg: dict) -> Client:
    """Initialize or reuse a Dask distributed client."""
    try:
        client = Client.current()
        logger.info("Reusing existing Dask client: %s", client.dashboard_link)
        return client
    except ValueError:
        pass

    dask_cfg = cfg.get("dask", {})
    scheduler: str = dask_cfg.get("scheduler", "local")

    if scheduler == "local":
        cluster = LocalCluster(
            n_workers=dask_cfg.get("n_workers", 4),
            threads_per_worker=dask_cfg.get("threads_per_worker", 1),
            memory_limit=dask_cfg.get("memory_limit", "3GB"),
            dashboard_address=f":{dask_cfg.get('dashboard_port', 8787)}",
        )
        client = Client(cluster)
    else:
        client = Client(scheduler)

    logger.info("Dask dashboard: %s", client.dashboard_link)
    return client


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="COGCC production data pipeline")
    parser.add_argument(
        "stages",
        nargs="*",
        choices=ALL_STAGES + [[]],  # type: ignore[list-item]
        default=ALL_STAGES,
        help="Stages to run (default: all)",
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = parser.parse_args(argv)

    with open(args.config, "r", encoding="utf-8") as fh:
        cfg: dict = yaml.safe_load(fh)

    setup_logging(cfg)
    stages: list[str] = args.stages if args.stages else ALL_STAGES

    logger.info("Pipeline starting — stages: %s", stages)

    client: Client | None = None

    for stage in stages:
        t0 = time.perf_counter()
        logger.info("Stage [%s] starting", stage)
        try:
            if stage == "acquire":
                from cogcc_pipeline.acquire import acquire

                acquire(cfg.get("acquire", {}))

                # Init distributed scheduler after acquire, before CPU-bound stages
                if any(s in stages for s in ("ingest", "transform", "features")):
                    client = _init_dask_client(cfg)

            elif stage == "ingest":
                if client is None:
                    client = _init_dask_client(cfg)
                from cogcc_pipeline.ingest import ingest

                ingest(cfg)

            elif stage == "transform":
                if client is None:
                    client = _init_dask_client(cfg)
                from cogcc_pipeline.transform import transform

                transform(cfg)

            elif stage == "features":
                if client is None:
                    client = _init_dask_client(cfg)
                from cogcc_pipeline.features import features

                features(cfg)

        except Exception:
            logger.exception("Stage [%s] FAILED — stopping pipeline", stage)
            sys.exit(1)

        elapsed = time.perf_counter() - t0
        logger.info("Stage [%s] completed in %.1f s", stage, elapsed)

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
