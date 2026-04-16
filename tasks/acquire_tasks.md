# Acquire Stage — Task Specifications

## Stage Overview

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`
**Output directory:** `data/raw/`
**Scheduler:** Dask threaded scheduler (I/O-bound stage — ADR-001)

The acquire stage downloads COGCC/ECMC production data files from external URLs for
all configured years (2020 through current year) and places raw files in `data/raw/`.
It must not use a distributed scheduler — the threaded scheduler is correct for I/O-bound
downloads. Browser automation tools are prohibited; use `requests` and `beautifulsoup4`
only (build-env-manifest.md).

### URL patterns
- Historical years (2020 through previous year):
  `https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip`
- Current year (monthly rolling file):
  `https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv`

### Output filenames
- Historical years: `data/raw/{year}_prod_reports.csv` (CSV extracted from zip)
- Current year: `data/raw/monthly_prod.csv`

### Parallelism constraint
Rate-limit parallel downloads to a maximum of 5 concurrent workers via Dask threaded
scheduler. Each download worker must sleep 0.5 seconds before initiating its HTTP
request to avoid overloading the server.

### Design constraints
- A failed or partial download must not leave a file on disk that appears valid.
  Write to a `.tmp` file first; rename to final filename only on verified success.
- File existence alone does not prove validity — a zero-byte or non-UTF-8 file is
  a silent failure (H2, stage manifest acquire).
- The stage is idempotent: re-running it on the same `data/raw/` directory skips
  already-present valid files without error (TR-20).

---

## Task ACQ-01: Project scaffolding and configuration

**Module:** `cogcc_pipeline/acquire.py`, `config/config.yaml`, `pyproject.toml`,
`Makefile`, `.gitignore`, `requirements.txt`

**Description:**
Set up the project package and configuration infrastructure required before any
pipeline code can run.

### pyproject.toml
- Declare `cogcc_pipeline` as the installable package.
- Build backend must be explicitly declared (hatchling or setuptools — not implicit).
- Register the CLI entry point: `cogcc-pipeline = cogcc_pipeline.pipeline:main`.
- Declare runtime and development dependencies consistent with the requirements.txt
  approach (separate runtime deps from dev deps via optional dependency groups).

### config/config.yaml
Create `config/config.yaml` with the following top-level sections exactly as listed:

```
acquire:
  base_url_historical: "https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip"
  base_url_current: "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv"
  start_year: 2020
  end_year: null          # null means use current calendar year at runtime
  raw_dir: "data/raw"
  max_workers: 5
  sleep_per_worker: 0.5   # seconds, applied before each download
  timeout: 60             # HTTP request timeout in seconds
  max_retries: 3
  backoff_multiplier: 2   # exponential backoff base multiplier

ingest:
  raw_dir: "data/raw"
  interim_dir: "data/interim"
  start_year: 2020

transform:
  interim_dir: "data/interim"
  processed_dir: "data/processed"

features:
  processed_dir: "data/processed"
  features_dir: "data/features"

dask:
  scheduler: "local"
  n_workers: 4
  threads_per_worker: 1
  memory_limit: "3GB"
  dashboard_port: 8787

logging:
  log_file: "logs/pipeline.log"
  level: "INFO"
```

### Makefile
Provide the following targets:
- `venv` — create `.venv` with `python -m venv .venv`
- `install` — bootstrap pip then `pip install -e ".[dev]"`
- `acquire` — run `cogcc-pipeline --stages acquire`
- `ingest` — run `cogcc-pipeline --stages ingest`
- `transform` — run `cogcc-pipeline --stages transform`
- `features` — run `cogcc-pipeline --stages features`
- `pipeline` — run `cogcc-pipeline --stages acquire ingest transform features`;
  must depend on the individual stage targets in sequence
- `test` — run `pytest tests/`
- `lint` — run `ruff check cogcc_pipeline/`
- `typecheck` — run `mypy cogcc_pipeline/`

### .gitignore
Add exclusions for: `data/`, `logs/`, `large_tool_results/`, `conversation_history/`,
`.venv/`, `__pycache__/`, `*.pyc`, `.DS_Store`, `dist/`, `*.egg-info/`

### requirements.txt
Seed the file with all runtime dependencies used by the acquire stage:
`requests`, `beautifulsoup4`, `dask[complete]`, `distributed`, `bokeh`, `pyyaml`,
`tenacity`, `tqdm`, `pandas`, `pyarrow`

**Definition of done:** `pyproject.toml` is valid and `pip install -e ".[dev]"` succeeds.
`config/config.yaml` exists with all required sections. `Makefile` exposes all listed
targets. `.gitignore` excludes all listed paths. `requirements.txt` updated with all
third-party packages imported in this task.

---

## Task ACQ-02: Configuration loader

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `load_config(config_path: str) -> dict`

**Description:**
Read `config/config.yaml` and return the full configuration dictionary. Called at the
top of `run_acquire()` before any download logic executes.

- Uses `pyyaml` to load the YAML file.
- If `acquire.end_year` is `null` in config, resolve it to `datetime.date.today().year`
  at load time so downstream code always receives an integer.
- Raises `FileNotFoundError` if `config_path` does not exist.
- Raises `KeyError` if any required top-level section (`acquire`, `dask`, `logging`)
  is absent from the config.
- Does not mutate the YAML file.

**Error handling:**
- Missing config file → `FileNotFoundError` with a descriptive message including the path.
- Missing required key → `KeyError` naming the missing section.

**Dependencies:** `pyyaml`, `datetime`

**Test cases:**
- Given a valid config YAML, assert the returned dict contains keys `acquire`, `dask`,
  `logging` and that `acquire.end_year` is an integer (not `None`).
- Given a config where `end_year` is `null`, assert `load_config` resolves it to
  `datetime.date.today().year`.
- Given a path to a non-existent file, assert `FileNotFoundError` is raised.
- Given a YAML file missing the `acquire` key, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ACQ-03: URL builder

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `build_download_targets(config: dict) -> list[dict]`

**Description:**
Given the acquire configuration dict, return an ordered list of download target
descriptors. Each descriptor is a dict with the following keys:
- `year` — integer year
- `url` — the full download URL string
- `output_path` — the full resolved output path as a `pathlib.Path`
- `is_current_year` — bool; `True` only for the current calendar year

Logic:
- For each year from `config["acquire"]["start_year"]` through
  `config["acquire"]["end_year"]` inclusive:
  - If the year equals `end_year` (current year): use `base_url_current` URL and
    output filename `monthly_prod.csv` under `raw_dir`.
  - Otherwise: format `base_url_historical` with the year and set output filename to
    `{year}_prod_reports.zip` under `raw_dir`.
- Return targets in ascending year order.
- `raw_dir` is resolved to an absolute path relative to the caller's working directory.

**Error handling:**
- If `start_year` > `end_year`, raise `ValueError` with a message stating the invalid range.

**Dependencies:** `pathlib`, `datetime`

**Test cases:**
- Given `start_year=2020`, `end_year=2022`, assert the returned list has 3 entries.
- Assert that the entry for year 2022 (end_year) has `is_current_year=True` and URL
  equals `base_url_current`.
- Assert that entries for years 2020 and 2021 have `is_current_year=False` and URLs
  match the `base_url_historical` pattern with the correct year substituted.
- Given `start_year=2023`, `end_year=2020`, assert `ValueError` is raised.
- Assert that all `output_path` values are `pathlib.Path` instances.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ACQ-04: File validity checker

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `is_valid_download(file_path: Path) -> bool`

**Description:**
Return `True` if the file at `file_path` exists, has size greater than 0 bytes, and is
readable as text without a `UnicodeDecodeError`. Return `False` otherwise.

This function is used in two places:
1. Before a download — to decide whether to skip an already-present file.
2. After a download — to verify the downloaded content is not corrupt or empty.

Logic:
- If `file_path` does not exist → return `False`.
- If `file_path.stat().st_size == 0` → return `False`.
- Attempt to open and read the first 1024 bytes as UTF-8 text.
  If a `UnicodeDecodeError` is raised → return `False`.
- Return `True` if all checks pass.

**Dependencies:** `pathlib`

**Test cases:**
- Given a file that does not exist, assert `is_valid_download` returns `False`.
- Given a file with 0 bytes, assert returns `False`.
- Given a file with valid UTF-8 content, assert returns `True`.
- Given a file with binary (non-UTF-8) content, assert returns `False`.
- Given a file with exactly 1 byte of valid UTF-8, assert returns `True`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ACQ-05: Single-file downloader with retry

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `download_file(target: dict, timeout: int, max_retries: int, backoff_multiplier: int) -> Path | None`

**Description:**
Download the file described by `target` (a descriptor dict from `build_download_targets`)
to disk. Returns the `Path` of the written file on success; returns `None` if the file
was skipped because a valid copy already existed.

### Behaviour
1. Check `is_valid_download(target["output_path"])`.
   If `True`, log an INFO message that the file is already present and return `None`.
2. Ensure the parent directory of `target["output_path"]` exists (create if absent).
3. Sleep `sleep_per_worker` seconds (read from module-level config) before initiating
   the HTTP request to respect the server rate-limit constraint.
4. Write downloaded bytes to a `.tmp` sibling of `target["output_path"]` first.
   On a network error or non-200 response, delete the `.tmp` file and raise
   (allowing tenacity to retry).
5. For zip targets (`is_current_year=False`): after the `.tmp` file is complete, open
   it as a zip archive, extract the first `.csv` file found inside, write the CSV to
   `target["output_path"]` (without the `.zip` extension — the output path for zips
   already has `.csv` extension as set by `build_download_targets`), then delete the
   `.tmp` zip file.
6. For CSV targets (`is_current_year=True`): rename `.tmp` directly to
   `target["output_path"]`.
7. Verify the final file with `is_valid_download`. If it fails, raise `RuntimeError`.
8. Log an INFO message with the filename and byte size on success.

### Retry logic
Use `tenacity` with:
- `stop=stop_after_attempt(max_retries)`
- `wait=wait_exponential(multiplier=backoff_multiplier, min=1, max=60)`
- Retry only on `requests.exceptions.RequestException` and `OSError`.
- On final failure, log a WARNING with the URL and exception message; return `None`.

### Error handling
- HTTP response status != 200 → raise `requests.exceptions.HTTPError`.
- Empty zip (no CSV file found inside) → raise `ValueError` with a message naming
  the zip URL.
- `UnicodeDecodeError` on the written file → raise `RuntimeError`; delete the corrupt file.

**Dependencies:** `requests`, `tenacity`, `zipfile`, `pathlib`, `time`, `logging`

**Test cases (all external HTTP calls must be mocked with `pytest-mock`):**
- Given a mocked 200 response for a CSV target, assert the file is written to
  `output_path`, `is_valid_download` returns `True` for it, and the function returns
  the `Path`.
- Given a mocked 200 response for a zip target, assert the extracted CSV is written
  to `output_path` (not the zip path) and the function returns the `Path`.
- Given a pre-existing valid file at `output_path`, assert `download_file` returns
  `None` and no HTTP request is made (mock verifies 0 calls).
- Given a mocked 404 response on all retries, assert the function logs a WARNING and
  returns `None` after `max_retries` attempts.
- Given a `requests.exceptions.ConnectionError` on the first attempt and a 200 on the
  second, assert the file is eventually written (retry succeeded).
- Given a mocked 200 response that writes 0 bytes, assert `RuntimeError` is raised
  and no corrupt file is left in `output_path`.
- Given a mocked zip response with no CSV inside, assert `ValueError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ACQ-06: Parallel acquire runner

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `run_acquire(config_path: str = "config/config.yaml") -> list[Path]`

**Description:**
Top-level entry point for the acquire stage. Orchestrates downloading all configured
year files in parallel using the Dask threaded scheduler.

### Behaviour
1. Call `load_config(config_path)` to get settings.
2. Ensure `raw_dir` exists (create recursively if absent).
3. Call `build_download_targets(config["acquire"])` to get the list of targets.
4. Use a `dask.bag.from_sequence(targets, npartitions=min(len(targets), max_workers))`
   to map `download_file` over all targets in parallel using the Dask threaded
   scheduler (not the distributed scheduler — I/O-bound stage per ADR-001).
5. Call `.compute(scheduler="threads")` to execute and collect results.
6. Return a list of `Path` objects for files that were downloaded (non-`None` results).
7. Log a summary: total targets, files downloaded, files skipped.

### Parallelism constraint
The Dask bag must be limited to `max_workers` (default: 5) concurrent workers.
Each individual `download_file` call sleeps `sleep_per_worker` seconds (default: 0.5)
before initiating its HTTP request.

**Dependencies:** `dask`, `dask.bag`, `pathlib`, `logging`

**Test cases (all HTTP calls mocked):**
- Given 3 targets (2 historical, 1 current year) all returning 200, assert 3 files
  are returned in the result list and all exist on disk.
- Given 1 target that already exists as a valid file, assert `run_acquire` returns an
  empty list (0 new downloads) and no HTTP calls are made.
- Given a mix where 2 of 3 targets are already present, assert exactly 1 download
  is performed.
- Assert the Dask scheduler used is `"threads"` (not `"synchronous"` or `"distributed"`).
  Verify by patching `dask.bag.Bag.compute` and inspecting the `scheduler` kwarg.
- TR-20: Run `run_acquire` twice on the same directory with all files mocked.
  Assert the second run produces 0 new downloads and the file count in `raw_dir`
  is identical after both runs.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ACQ-07: Acquire stage integration tests

**Module:** `tests/test_acquire.py`
**Scope:** TR-20, TR-21 (from test-requirements.xml)

**Description:**
Write integration-level tests that exercise the full acquire stage output contract.
These tests use a temporary directory and fully mocked HTTP responses — no real
network calls are made.

### TR-20 — Idempotency
- Set up a temporary `raw_dir` with pre-written valid CSV files for 2020 and 2021.
- Call `run_acquire` pointing at that directory.
- Assert the file count in `raw_dir` is unchanged.
- Assert no HTTP requests were made (mock assert_not_called).
- Assert the content of pre-existing files is byte-for-byte identical after the run.

### TR-21 — File integrity
- Mock HTTP responses to return small but valid CSV text for all targets.
- Call `run_acquire` into an empty temp directory.
- After the run, for every file in `raw_dir`:
  - Assert file size > 0 bytes.
  - Assert the file is readable as UTF-8 text without `UnicodeDecodeError`.
  - Assert the file contains at least 2 lines (header + at least 1 data row).
- Additionally test: if the HTTP response returns 0 bytes, assert the file is NOT
  written to `output_path` (only `.tmp` may exist, and it must be cleaned up).

**Definition of done:** All TR-20 and TR-21 test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
