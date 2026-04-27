# Pipeline Orchestration, Logging, and Entry Point Tasks

**Modules:** `cogcc_pipeline/pipeline.py`, `pyproject.toml`, `Makefile`, `config.yaml`,
`.gitignore`
**Test file:** `tests/test_pipeline.py`

---

## Governing documents

Build backend, environment setup, Makefile targets, entry point: build-env-manifest.md.
Scheduler initialization and reuse: build-env-manifest.md (Dask scheduler initialization
section).
Dual-channel logging: ADR-006.
Configuration structure: build-env-manifest.md (Configuration structure section).
Technology stack: ADR-007.
Test marking: ADR-008.
End-to-end integration test: TR-27 in test-requirements.xml.

---

## Scope constraints (apply to every task in this file)

- bs4/BeautifulSoup is present for the acquire stage only — do not use it in pipeline,
  build, or configuration tasks.
- The Makefile `venv` target must invoke `python3` — do not derive a versioned binary
  name (e.g. `python3.11`, `python3.12`, or any other versioned binary name) from the
  `requires-python` field in `pyproject.toml`. The wrong answer looks like `python3.11`,
  `python3.12`, or any other versioned binary name.
- No configurable values may be hardcoded in pipeline code — all settings come from
  `config.yaml` (build-env-manifest.md).
- The distributed Dask scheduler must be initialized once before any distributed-stage
  run and reused across all stages in a single invocation — individual stage functions
  must not initialize their own cluster (build-env-manifest.md).
- The acquire stage uses the Dask threaded scheduler — do not initialize a distributed
  cluster when acquire is the only stage (build-env-manifest.md, ADR-001).

---

## Task 01: Write pyproject.toml

**Artefact:** `pyproject.toml`

**Description:** Define the package build configuration for `cogcc_pipeline`. Must
declare:
- The build backend as specified by build-env-manifest.md — the primary public entry
  point of the package installer, not an internal alias, submodule, or path containing
  `backends` or `legacy`.
- `requires-python = ">=3.11"` (ADR-007).
- Package name `cogcc-pipeline`.
- All runtime dependencies including: `dask[distributed]`, `pandas`, `numpy`, `pyarrow`,
  `requests`, `pyyaml`, `bokeh` (Dask dashboard runtime dependency — build-env-manifest.md).
- Development dependencies including: `pytest`, `ruff`, `mypy`, `pandas-stubs`,
  `types-requests` (type stubs for pandas and the HTTP request library — build-env-manifest.md).
- A `[project.scripts]` entry point that registers the CLI command `cogcc-pipeline`
  pointing to the pipeline entry function (Task 03).

**Not required:** optional dependency groups beyond `[dev]`, version pinning beyond
lower bounds.

**Test cases:** None — this is a configuration artefact, not executable code. Verified
implicitly by a successful `pip install -e .[dev]`.

**Definition of done:** `pyproject.toml` is written, `pip install -e .[dev]` succeeds
in a clean venv, ruff and mypy report no errors on the package after install,
requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Write Makefile

**Artefact:** `Makefile`

**Description:** Define the following targets (build-env-manifest.md):

- `venv`: create a clean virtual environment using `python3` (unversioned — see scope
  constraints). Bootstrap the package installer before installing project dependencies.
- `install`: install all dependencies including development tools from `pyproject.toml`.
- `acquire`: run the acquire stage via the pipeline entry point.
- `ingest`: run the ingest stage via the pipeline entry point.
- `transform`: run the transform stage via the pipeline entry point.
- `features`: run the features stage via the pipeline entry point.
- `pipeline`: run the full pipeline (all four stages in order: acquire → ingest →
  transform → features). Must use exactly one invocation approach — either chain stage
  targets as dependencies with no recipe body, or invoke the entry point once. Must not
  combine both (build-env-manifest.md — a pipeline target that both chains stage targets
  as dependencies AND invokes the entry point in the recipe body will run every stage
  twice).
- `clean`: remove generated data files and build artefacts.

**Not required:** parallel make execution, CI/CD integration targets.

**Test cases:** None — this is a build artefact. Verified implicitly by running
`make install` and `make pipeline` in a clean environment.

**Definition of done:** Makefile is written, all targets execute without error in a
clean venv, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Write pipeline entry point and logging setup

**Module:** `cogcc_pipeline/pipeline.py`
**Function (entry point):** `main() -> None`

**Description:** Implement the pipeline CLI entry point registered in `pyproject.toml`.
`main()` must (build-env-manifest.md):
- Accept an optional list of stage names from the command line (defaulting to all four:
  `acquire`, `ingest`, `transform`, `features` when none are specified).
- Read all settings from `config.yaml` (path resolved relative to the project root or
  from a `--config` flag).
- Set up dual-channel logging (ADR-006) before any stage runs: one handler writing to
  the console, one handler writing to the log file at the path from `config.yaml`.
  The log output directory must be created at startup if absent.
- Initialize the Dask distributed scheduler once before any distributed stage
  (ingest, transform, features) runs, conditioned on at least one distributed stage
  being present in the current invocation (build-env-manifest.md). Do not initialize the
  distributed cluster if only `acquire` is requested — acquire uses the threaded
  scheduler (ADR-001).
- If `dask.scheduler` in config is `"local"`, initialize a local distributed cluster
  with settings from the `dask` config section. If it is a URL, connect to the remote
  scheduler at that address. Log the dashboard URL after initialization
  (build-env-manifest.md).
- Run each requested stage in order, timing each stage and logging elapsed time.
- Stop execution on stage failure — downstream stages must not run if an upstream stage
  fails (build-env-manifest.md). Log the exception and stage name before exiting.

**Not required:** signal handling, graceful cluster teardown on SIGINT beyond Python
defaults, configuration schema validation.

**Dependencies:** dask.distributed, logging (stdlib), argparse (stdlib), time (stdlib),
pathlib (stdlib), yaml

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a config with `dask.scheduler = "local"`, assert `main(["acquire"])` does not
  initialize a distributed cluster (mock `dask.distributed.Client` to assert it is not
  called).
- Given a config with `dask.scheduler = "local"`, assert `main(["ingest"])` initializes
  a local distributed cluster before calling `ingest()`.
- Given a stage that raises an exception, assert subsequent stages are not called and the
  exception is logged.
- Assert that log handlers are set up before any stage function is called.
- `@pytest.mark.integration` (TR-27): Run `main(["ingest", "transform", "features"])` on
  2–3 real raw source files using a config dict with all output paths pointing to
  `tmp_path`. Assert: (a) ingest output satisfies all guarantees in
  boundary-ingest-transform.md; (b) transform output satisfies all guarantees in
  boundary-transform-features.md; (c) features output satisfies TR-26; (d) no unhandled
  exceptions are raised across the full run.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Write config.yaml

**Artefact:** `config.yaml`

**Description:** Write the project configuration file with all top-level sections required
by build-env-manifest.md:

```
acquire:
  zip_url_template:  # zip-file-path template from task-writer-cogcc.md
  monthly_url:       # monthly-file-path from task-writer-cogcc.md
  start_year: 2020
  raw_dir: data/raw
  max_workers: 5
  sleep_seconds: 0.5

ingest:
  raw_dir: data/raw
  interim_dir: data/interim
  data_dictionary: references/production-data-dictionary.csv
  min_year: 2020

transform:
  interim_dir: data/interim
  processed_dir: data/processed
  entity_col: ApiSequenceNumber
  data_dictionary: references/production-data-dictionary.csv

features:
  processed_dir: data/processed
  output_dir: data/features

dask:
  scheduler: local
  n_workers: 4
  threads_per_worker: 2
  memory_limit: "3GB"
  dashboard_port: 8787

logging:
  log_file: logs/pipeline.log
  level: INFO
```

All URL values must use the exact templates from `task-writer-cogcc.md`. No configurable
values may be hardcoded in pipeline code — they must be present here.

**Not required:** environment-specific config overrides, secrets management.

**Test cases:** None — verified implicitly by `load_config` tasks in each stage.

**Definition of done:** `config.yaml` is written with all required sections, all stage
`load_config` functions can read it without raising, requirements.txt updated with all
third-party packages imported in this task.

---

## Task 05: Write .gitignore

**Artefact:** `.gitignore`

**Description:** Write the project `.gitignore` to exclude runtime and generated
artefacts from version control. Must include at minimum (build-env-manifest.md):

- `data/` — raw, interim, and processed data files
- `logs/` — runtime log files
- `large_tool_results/`
- `conversation_history/`
- `.venv/` — virtual environment directory
- `__pycache__/`, `*.py[cod]`, `*.egg-info/` — Python build artefacts
- `.mypy_cache/`, `.ruff_cache/`, `.pytest_cache/` — tool caches

**Test cases:** None — this is a version control configuration artefact.

**Definition of done:** `.gitignore` is written with all required entries, requirements.txt
updated with all third-party packages imported in this task.
