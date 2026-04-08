# Orchestrator (Pipeline) Stage — Task Specifications

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/pipeline.py`
**Test file:** `tests/test_pipeline.py`

## Overview

The pipeline orchestrator chains all four stages (acquire → ingest → transform → features) in order. It provides a CLI entry point registered in `pyproject.toml`, initializes the Dask distributed cluster after acquire completes, passes the shared `Client` to downstream stages, logs per-stage timing, and emits a final pipeline run report. All configuration is read from `config.yaml`.

## Design Decisions & Constraints

- The CLI entry point is `main()`, registered as `cogcc-pipeline = cogcc_pipeline.pipeline:main` in `pyproject.toml`.
- The pipeline is runnable as `cogcc-pipeline [--stages STAGE [STAGE ...]] [--config CONFIG]`.
- The `make pipeline` Makefile target must invoke this CLI entry point.
- Stage ordering is always acquire → ingest → transform → features. If `--stages` is specified, only the named stages run, but they still execute in the canonical order.
- The Dask distributed `Client` is initialized **after** acquire completes and **before** ingest begins. It is passed implicitly (ingest/transform/features all call `get_or_create_client` which reuses the existing one).
- If `dask.scheduler == "local"` in config, create a `LocalCluster` with `n_workers`, `threads_per_worker`, `memory_limit` from config. If it is a URL (e.g. `"tcp://host:port"`), connect to the remote scheduler. Log the dashboard URL.
- The acquire stage must NOT receive or use the distributed Client.
- Each stage is timed with `time.perf_counter`. Per-stage timing and status are recorded in the run report.
- On stage failure: log the error, raise `RuntimeError` to prevent downstream stages from running. Downstream stages must not execute if an upstream stage fails.
- A final pipeline run report is written to `data/processed/pipeline_run_report.json` after all stages complete (or after the first failure).
- Logging is set up at pipeline startup (both stdout and `logs/pipeline.log`). The `logs/` directory is created if absent.
- `data/` subdirectories (`raw/`, `interim/`, `processed/`) are created at pipeline startup if absent.
- Use `pathlib.Path` throughout. All functions have type hints and docstrings.

## `config.yaml` — complete structure reminder

All pipeline configuration is in `config.yaml`. The orchestrator loads it with `load_config(Path("config.yaml"))` and passes the full config dict to each stage's `run_*` function.

```
acquire:
  year_start: 2020
  max_workers: 5
  retry_max_attempts: 3
  retry_backoff_factor: 1.0
  zip_url_template: "https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip"
  monthly_url: "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv"
  raw_dir: "data/raw"

ingest:
  raw_dir: "data/raw"
  interim_dir: "data/interim"
  year_start: 2020

transform:
  interim_dir: "data/interim"
  processed_dir: "data/processed"
  date_start: "2020-01-01"

features:
  interim_dir: "data/interim"
  processed_dir: "data/processed"
  rolling_windows: [3, 6]
  decline_rate_clip_min: -1.0
  decline_rate_clip_max: 10.0

dask:
  scheduler: "local"
  n_workers: 2
  threads_per_worker: 2
  memory_limit: "3GB"
  dashboard_port: 8787

logging:
  log_file: "logs/pipeline.log"
  level: "INFO"
```

---

## Task O-01: Pipeline run report writer

**Module:** `cogcc_pipeline/pipeline.py`
**Function:** `write_run_report(report: dict, output_path: Path) -> None`

**Description:**
Write the pipeline run report to `output_path` as a JSON file (indented, human-readable). Create parent directories if they do not exist.

The `report` dict must contain:
- `pipeline_start`: ISO-8601 UTC timestamp string
- `pipeline_end`: ISO-8601 UTC timestamp string (or `null` if interrupted)
- `stages`: list of stage dicts, each with:
  - `name`: stage name string (`"acquire"`, `"ingest"`, `"transform"`, `"features"`)
  - `status`: `"success"`, `"failed"`, or `"skipped"`
  - `duration_seconds`: float or `null`
  - `error`: error message string or `null`
- `overall_status`: `"success"` or `"failed"`

**Test cases:**
- `@pytest.mark.unit` Assert a report dict is written as valid JSON to the specified path.
- `@pytest.mark.unit` Assert the JSON can be re-read and all expected keys are present.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task O-02: Dask distributed client initializer

**Module:** `cogcc_pipeline/pipeline.py`
**Function:** `init_dask_client(config: dict) -> distributed.Client`

**Description:**
Initialize the Dask distributed `Client` for use by ingest, transform, and features stages:
- If `config["dask"]["scheduler"] == "local"`: create a `LocalCluster` with `n_workers`, `threads_per_worker`, `memory_limit`, `dashboard_address=f":{config['dask']['dashboard_port']}"`, then create a `Client` from it.
- If `config["dask"]["scheduler"]` is a URL (starts with `"tcp://"`): connect to the remote scheduler via `distributed.Client(config["dask"]["scheduler"])`.
- Log the dashboard URL (`client.dashboard_link`).
- Return the `Client`.

**Parameters:**
- `config`: full config dict

**Returns:** `distributed.Client`

**Test cases:**
- `@pytest.mark.unit` Mock `LocalCluster` and `distributed.Client`; assert they are called with correct args when scheduler is `"local"`.
- `@pytest.mark.unit` When scheduler is `"tcp://localhost:8786"`, assert `distributed.Client` is called with that URL directly (no LocalCluster).

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task O-03: Directory bootstrapper

**Module:** `cogcc_pipeline/pipeline.py`
**Function:** `bootstrap_directories(config: dict) -> None`

**Description:**
Create all required data directories at pipeline startup if they do not exist:
- `data/raw/` (from `config["acquire"]["raw_dir"]`)
- `data/interim/` (from `config["ingest"]["interim_dir"]`)
- `data/processed/` (from `config["features"]["processed_dir"]`)
- `logs/` (from `config["logging"]["log_file"]` parent directory)

Use `Path.mkdir(parents=True, exist_ok=True)` for all directories.

**Test cases:**
- `@pytest.mark.unit` Given a config pointing to temp directories, assert all directories are created.
- `@pytest.mark.unit` Assert the function is idempotent — calling it twice raises no error.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task O-04: Pipeline stage runner

**Module:** `cogcc_pipeline/pipeline.py`
**Function:** `run_pipeline(stages: list[str] | None = None, config_path: Path = Path("config.yaml")) -> None`

**Description:**
Main pipeline orchestration function. Executes the following:

1. Load config via `load_config(config_path)`.
2. Set up logging via `setup_logging(config)`.
3. Bootstrap directories via `bootstrap_directories(config)`.
4. Determine which stages to run: if `stages` is `None`, run all four in order; otherwise run only those specified (still in canonical order: acquire → ingest → transform → features).
5. Initialize report dict with `pipeline_start`.
6. **If acquire is in stages:** Run `run_acquire(config)`. Time it. Record status and duration. On failure: record error, write partial report, raise `RuntimeError`.
7. **After acquire (if ingest, transform, or features are in stages):** Call `init_dask_client(config)` to initialize the distributed client. Log dashboard URL. Skip if only acquire is running.
8. **If ingest is in stages:** Run `run_ingest(config)`. Time it. On failure: close Client, write partial report, raise `RuntimeError`.
9. **If transform is in stages:** Run `run_transform(config)`. Time it. On failure: close Client, write partial report, raise `RuntimeError`.
10. **If features is in stages:** Run `run_features(config)`. Time it. On failure: close Client, write partial report, raise `RuntimeError`.
11. Close Dask Client if it was initialized in step 7.
12. Set `pipeline_end` and `overall_status="success"`.
13. Write final report via `write_run_report`.
14. Log total pipeline duration.

**Parameters:**
- `stages`: list of stage names to run; `None` means all stages
- `config_path`: path to config.yaml

**Returns:** `None`

**Error handling:**
- Any stage failure raises `RuntimeError` and aborts remaining stages.
- Final report is always written, even on failure.

**Test cases:**
- `@pytest.mark.unit` Mock all four `run_*` functions; call `run_pipeline(stages=None)`; assert all four were called in order.
- `@pytest.mark.unit` Mock `run_acquire` to raise `RuntimeError`; assert `run_ingest` is never called and the run report records `acquire` as `"failed"`.
- `@pytest.mark.unit` Call `run_pipeline(stages=["ingest", "transform"])`; assert only `run_ingest` and `run_transform` are called (not acquire or features).
- `@pytest.mark.unit` Assert `init_dask_client` is NOT called when `stages=["acquire"]`.
- `@pytest.mark.unit` Assert `init_dask_client` IS called when `stages=["ingest"]` (even without acquire).
- `@pytest.mark.unit` Assert `write_run_report` is called even when a stage fails.
- `@pytest.mark.unit` Assert `run_pipeline` raises `RuntimeError` when a stage fails.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task O-05: CLI entry point

**Module:** `cogcc_pipeline/pipeline.py`
**Function:** `main() -> None`

**Description:**
CLI entry point registered in `pyproject.toml` as `cogcc-pipeline = cogcc_pipeline.pipeline:main`.

Parse command-line arguments using `argparse`:
- `--stages`: zero or more stage names from `["acquire", "ingest", "transform", "features"]`. Optional; defaults to all stages.
- `--config`: path to config YAML file. Optional; defaults to `"config.yaml"`.

Validate that any specified stage names are from the allowed set. Print an error message and exit with code 1 if invalid stage names are provided.

Call `run_pipeline(stages=parsed_stages, config_path=Path(parsed_config))`.

**Test cases:**
- `@pytest.mark.unit` Mock `run_pipeline`; invoke `main()` with `sys.argv = ["cogcc-pipeline", "--stages", "acquire", "ingest"]`; assert `run_pipeline` is called with `stages=["acquire", "ingest"]`.
- `@pytest.mark.unit` Invoke `main()` with no `--stages` argument; assert `run_pipeline` is called with `stages=None`.
- `@pytest.mark.unit` Invoke `main()` with `--stages invalid_stage`; assert exit code is 1 (use `pytest.raises(SystemExit)`).
- `@pytest.mark.unit` Invoke `main()` with `--config path/to/config.yaml`; assert `run_pipeline` receives `config_path=Path("path/to/config.yaml")`.

**Definition of done:** Function implemented, tests pass, entry point registered in `pyproject.toml`, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task O-06: Makefile stage targets and pipeline target

**Module:** `Makefile`

**Description:**
Extend the Makefile (created in Task A-01) to ensure all individual stage targets and the pipeline target are correctly defined:

```
acquire:
	cogcc-pipeline --stages acquire

ingest:
	cogcc-pipeline --stages ingest

transform:
	cogcc-pipeline --stages transform

features:
	cogcc-pipeline --stages features

pipeline: acquire ingest transform features
	cogcc-pipeline
```

Each target must be runnable independently (e.g. `make ingest` without running `make acquire` first — useful for re-running a single stage after fixing a bug).

The `make pipeline` target must depend on `acquire ingest transform features` in sequence and invoke the full CLI. The Dask Client initialization (which only applies to ingest+) will be handled by the pipeline orchestrator.

**Test cases:**
- `@pytest.mark.unit` Assert `cogcc-pipeline --stages acquire` is the command in the `acquire` Makefile target.
- `@pytest.mark.unit` Assert the `pipeline` target lists `acquire ingest transform features` as dependencies.

(These are documentation/review tests; the coder must manually verify the Makefile is correct and consistent with Task A-01.)

**Definition of done:** Makefile updated with all targets, `make install` succeeds in a fresh venv, `ruff` and `mypy` report no errors on `cogcc_pipeline/pipeline.py`, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task O-07: End-to-end pipeline integration test

**Test file:** `tests/test_pipeline.py`

**Description:**
Integration tests for the full pipeline orchestrator.

**Test cases:**
- `@pytest.mark.unit` **Stage sequencing**: mock all four `run_*` functions; call `run_pipeline()`; assert calls were made in order: acquire → ingest → transform → features.
- `@pytest.mark.unit` **Stage skipping**: call `run_pipeline(stages=["transform", "features"])`; assert `run_acquire` and `run_ingest` are not called.
- `@pytest.mark.unit` **Failure propagation**: mock `run_ingest` to raise; assert `run_transform` is never called and the run report records ingest as `"failed"`.
- `@pytest.mark.unit` **Report always written**: mock `run_transform` to raise; assert `write_run_report` is still called.
- `@pytest.mark.unit` **Client lifecycle**: assert `init_dask_client` is called once before ingest when the full pipeline runs, and `client.close()` is called once after features completes.
- `@pytest.mark.unit` **Config loading**: mock `load_config` to return a synthetic config; assert `run_acquire` receives the same config dict.
- `@pytest.mark.integration` **Full pipeline run**: run `cogcc-pipeline --stages acquire` (current year only); assert `data/raw/monthly_prod.csv` exists.
- `@pytest.mark.integration` **Run report exists**: after a full pipeline run, assert `data/processed/pipeline_run_report.json` exists, is valid JSON, and contains `"overall_status": "success"`.

**Definition of done:** All test cases implemented, integration tests marked with `@pytest.mark.integration`, unit tests passing, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.
