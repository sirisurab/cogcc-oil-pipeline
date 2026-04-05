"""Pipeline orchestrator: runs all stages in sequence."""

from __future__ import annotations

import sys
import time
import traceback
from pathlib import Path

from cogcc_pipeline.acquire import run_acquire, validate_raw_files
from cogcc_pipeline.features import run_features
from cogcc_pipeline.ingest import run_ingest
from cogcc_pipeline.transform import run_transform
from cogcc_pipeline.utils import get_logger, load_config


def orchestrate(config_path: str | Path = "config/pipeline_config.yaml") -> None:
    """Run the full COGCC pipeline: acquire → ingest → transform → features.

    Each stage is wrapped in a try/except block. If a stage raises, the
    exception is logged with its traceback and re-raised so the process exits
    with a non-zero code.

    Args:
        config_path: Path to the YAML pipeline configuration file.
    """
    config = load_config(config_path)
    log_cfg = config["logging"]
    logger = get_logger(
        "cogcc_pipeline.orchestrator",
        log_cfg["log_dir"],
        log_cfg["log_file"],
        log_cfg["level"],
    )

    start_time = time.time()
    logger.info("=== COGCC Pipeline Starting ===")

    # Stage 1: Acquire
    try:
        logger.info("--- Stage: Acquire ---")
        acquired_paths = run_acquire(config, logger)
        raw_dir = Path(config["acquire"]["raw_dir"])
        file_errors = validate_raw_files(raw_dir, logger)
        for err in file_errors:
            logger.error("File validation error: %s", err)
        logger.info("Acquire complete: %d file(s) acquired", len(acquired_paths))
    except Exception:
        logger.error("Acquire stage failed:\n%s", traceback.format_exc())
        raise

    # Stage 2: Ingest
    try:
        logger.info("--- Stage: Ingest ---")
        interim_path = run_ingest(config, logger)
        logger.info("Ingest complete: %s", interim_path)
    except Exception:
        logger.error("Ingest stage failed:\n%s", traceback.format_exc())
        raise

    # Stage 3: Transform
    try:
        logger.info("--- Stage: Transform ---")
        cleaned_path = run_transform(config, logger)
        logger.info("Transform complete: %s", cleaned_path)
    except Exception:
        logger.error("Transform stage failed:\n%s", traceback.format_exc())
        raise

    # Stage 4: Features
    try:
        logger.info("--- Stage: Features ---")
        features_path = run_features(config, logger)
        logger.info("Features complete: %s", features_path)
    except Exception:
        logger.error("Features stage failed:\n%s", traceback.format_exc())
        raise

    elapsed = time.time() - start_time
    logger.info(
        "=== COGCC Pipeline Complete === (elapsed: %.1fs) | files acquired: %d | output: %s",
        elapsed,
        len(acquired_paths),
        features_path,
    )


def main() -> None:
    """CLI entry point for the pipeline orchestrator.

    Reads config path from ``sys.argv[1]`` if provided; defaults to
    ``config/pipeline_config.yaml``.
    """
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/pipeline_config.yaml"
    try:
        orchestrate(config_path)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
