# Acquire Stage — Task Specifications

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`

## Overview

The acquire stage downloads COGCC annual production ZIP files (2020 through the prior year) and the current-year monthly CSV directly from the ECMC server. Files are saved to `data/raw/`. A JSON download manifest is written to `data/raw/download_manifest.json` recording metadata for every acquired file. All configuration is read from `config.yaml`. Parallel downloads use the Dask threaded scheduler (not `dask.distributed`).

## Data Sources

| Year | URL pattern | Local filename |
|------|-------------|----------------|
| 2020 … prior year | `https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip` | `data/raw/{year}_prod_reports.csv` (after unzip) |
| Current year | `https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv` | `data/raw/monthly_prod.csv` |

## Design Decisions & Constraints

- Use `requests` with retry logic (exponential backoff, configurable max retries) for all HTTP operations. Do **not** use Playwright or any browser automation.
- Parallel downloads use `dask.compute()` with `scheduler="threads"` and `num_workers` from `config["acquire"]["max_workers"]` (maximum 5 concurrent workers). Do **not** initialize a `dask.distributed` Client in this stage.
- Each download worker sleeps 0.5 seconds after completing its download to avoid overloading the server.
- Skip already-downloaded files (idempotent): if the local CSV already exists, do not re-download.
- ZIP files are downloaded to a temp path inside `data/raw/`, extracted, then the ZIP is deleted — only the extracted CSV is kept.
- A download manifest (`data/raw/download_manifest.json`) records, for each file: `url`, `filename`, `size_bytes`, `download_timestamp` (ISO-8601 UTC), `md5_checksum`.
- All configurable values (URLs, directory paths, worker counts, retry settings, year range) come from `config.yaml` — never hardcoded.
- Use `pathlib.Path` throughout (no `os.path`).
- All functions must have type hints and docstrings.
- Logging via Python `logging` module (not `print`). Logs go to both stdout and `logs/pipeline.log`.

## `config.yaml` — acquire section

```
acquire:
  year_start: 2020
  max_workers: 5
  retry_max_attempts: 3
  retry_backoff_factor: 1.0
  zip_url_template: "https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip"
  monthly_url: "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv"
  raw_dir: "data/raw"
```

---

## Task A-01: Project scaffolding and configuration

**Module:** `cogcc_pipeline/__init__.py`, `cogcc_pipeline/acquire.py`, `config.yaml`, `pyproject.toml`, `Makefile`, `.gitignore`, `requirements.txt`

**Description:**
Create the full project scaffold. This is done **once** and shared across all pipeline stages.

### Files to create

1. **`pyproject.toml`**
   - `build-backend = "setuptools.build_meta"` (never `legacy:build`)
   - Package name: `cogcc_pipeline`
   - Python requires: `>=3.11`
   - Runtime dependencies: `requests`, `pandas>=2.0`, `pyarrow`, `dask[distributed]`, `bokeh`, `pyyaml`, `scikit-learn`, `numpy`
   - Dev dependencies: `pytest`, `pytest-mock`, `mypy`, `ruff`, `pandas-stubs`, `types-requests`
   - Entry point: `cogcc-pipeline = cogcc_pipeline.pipeline:main`

2. **`config.yaml`**
   - Top-level sections: `acquire`, `ingest`, `transform`, `features`, `dask`, `logging`
   - `acquire` section: per design decisions above
   - `ingest` section: `raw_dir: "data/raw"`, `interim_dir: "data/interim"`, `year_start: 2020`
   - `transform` section: `interim_dir: "data/interim"`, `processed_dir: "data/processed"`, `date_start: "2020-01-01"`
   - `features` section: `interim_dir: "data/interim"`, `processed_dir: "data/processed"`, `rolling_windows: [3, 6]`, `decline_rate_clip_min: -1.0`, `decline_rate_clip_max: 10.0`
   - `dask` section: `scheduler: "local"`, `n_workers: 2`, `threads_per_worker: 2`, `memory_limit: "3GB"`, `dashboard_port: 8787`
   - `logging` section: `log_file: "logs/pipeline.log"`, `level: "INFO"`

3. **`Makefile`**
   - `make env`: creates `.venv` using `python3 -m venv .venv`
   - `make install`: bootstraps pip/setuptools/wheel then runs `pip install -e ".[dev]"`. Never use `pip install -r requirements.txt` as the sole install step.
   - `make acquire`: runs the acquire stage via the CLI entry point
   - `make ingest`: runs the ingest stage
   - `make transform`: runs the transform stage
   - `make features`: runs the features stage
   - `make pipeline`: depends on `acquire ingest transform features` in sequence. Invokes the CLI entry point.
   - `make test`: runs `pytest tests/`
   - `make lint`: runs `ruff check .`
   - `make typecheck`: runs `mypy cogcc_pipeline/`

4. **`.gitignore`**
   - Must include: `data/`, `logs/`, `large_tool_results/`, `conversation_history/`, `.venv/`, `__pycache__/`, `*.egg-info/`, `.DS_Store`

5. **`cogcc_pipeline/__init__.py`** — empty or package-level docstring only.

6. **`requirements.txt`** — list all third-party packages used across the pipeline (not pinned versions; this file supplements `pyproject.toml` for reference).

**Definition of done:** All scaffold files exist. `pip install -e ".[dev]"` succeeds in a fresh venv. `ruff` and `mypy` report no errors on the empty package skeleton. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-02: Logging setup utility

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `setup_logging(config: dict) -> logging.Logger`

**Description:**
Create a module-level logging setup function. It must configure a logger that writes to both stdout (`StreamHandler`) and `logs/pipeline.log` (`FileHandler`). The `logs/` directory must be created if it does not exist. Log level and log file path are read from `config["logging"]`.

**Parameters:**
- `config`: full config dict loaded from `config.yaml`

**Returns:** configured `logging.Logger` instance

**Error handling:**
- If `logs/` cannot be created due to permission error, raise `OSError` with descriptive message.

**Dependencies:** `logging`, `pathlib`

**Definition of done:** Function implemented, logs written to both stdout and file simultaneously, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-03: Configuration loader

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `load_config(config_path: Path) -> dict`

**Description:**
Load `config.yaml` and return as a Python dict. Validate that all required top-level sections (`acquire`, `ingest`, `transform`, `features`, `dask`, `logging`) are present. Raise `KeyError` with a descriptive message if any section is missing.

**Parameters:**
- `config_path`: absolute or relative `Path` to `config.yaml`

**Returns:** `dict` with full configuration

**Error handling:**
- File not found → raise `FileNotFoundError`
- YAML parse error → raise `ValueError` wrapping the original exception
- Missing section → raise `KeyError`

**Dependencies:** `pyyaml`, `pathlib`

**Definition of done:** Function implemented, all error paths tested, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-04: MD5 checksum computation

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `compute_md5(file_path: Path) -> str`

**Description:**
Compute the MD5 hexdigest of a file, reading in 8 KB chunks to support large files without loading the entire file into memory.

**Parameters:**
- `file_path`: path to the file

**Returns:** lowercase hex MD5 string

**Error handling:**
- File not found → raise `FileNotFoundError`

**Dependencies:** `hashlib`, `pathlib`

**Test cases:**
- `@pytest.mark.unit` Given a known small file with known content, assert the returned MD5 matches the expected hexdigest.
- `@pytest.mark.unit` Given a non-existent path, assert `FileNotFoundError` is raised.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-05: HTTP file downloader with retry logic

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `download_file(url: str, dest_path: Path, retry_max_attempts: int, retry_backoff_factor: float) -> Path`

**Description:**
Download a file from `url` and write it to `dest_path`. Implement exponential backoff retry: on HTTP errors or connection errors, wait `retry_backoff_factor * (2 ** attempt)` seconds before retrying. Stream the response body in chunks (8 KB) to support large files without loading entirely into memory. Sleep 0.5 seconds after a successful download.

**Parameters:**
- `url`: HTTP URL to download
- `dest_path`: local path where file will be written
- `retry_max_attempts`: maximum number of attempts (from config)
- `retry_backoff_factor`: base multiplier for backoff (from config)

**Returns:** `dest_path` (the path that was written)

**Error handling:**
- All attempts failed → raise `RuntimeError` with the URL and last exception message.
- HTTP status ≥ 400 on final attempt → raise `RuntimeError`.
- Parent directory of `dest_path` is created if it does not exist.

**Dependencies:** `requests`, `time`, `pathlib`

**Test cases:**
- `@pytest.mark.unit` Mock `requests.get` to return a 200 response with binary content; assert file is written to `dest_path` and the function returns `dest_path`.
- `@pytest.mark.unit` Mock `requests.get` to raise `requests.ConnectionError` on first two attempts and succeed on the third; assert the function returns successfully and the retry count was 3.
- `@pytest.mark.unit` Mock `requests.get` to always raise `requests.ConnectionError`; assert `RuntimeError` is raised after `retry_max_attempts` attempts.
- `@pytest.mark.unit` Mock `requests.get` to return HTTP 404; assert `RuntimeError` is raised.

**Definition of done:** Function implemented, all test cases pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-06: ZIP extractor

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `extract_csv_from_zip(zip_path: Path, dest_dir: Path) -> Path`

**Description:**
Open the ZIP file at `zip_path`, find the first `.csv` file inside it, extract it to `dest_dir`, and delete the ZIP file. Return the path to the extracted CSV.

**Parameters:**
- `zip_path`: path to the downloaded ZIP file
- `dest_dir`: directory where CSV should be placed

**Returns:** `Path` to extracted CSV file

**Error handling:**
- ZIP contains no CSV file → raise `ValueError` describing the ZIP filename.
- ZIP file is corrupted → let `zipfile.BadZipFile` propagate naturally.

**Dependencies:** `zipfile`, `pathlib`

**Test cases:**
- `@pytest.mark.unit` Create an in-memory ZIP with a single CSV file; assert `extract_csv_from_zip` returns a path pointing to a `.csv` file in `dest_dir`.
- `@pytest.mark.unit` Create an in-memory ZIP with no CSV files; assert `ValueError` is raised.
- `@pytest.mark.unit` After extraction, assert the source ZIP file no longer exists on disk.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-07: Download manifest writer

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `update_manifest(manifest_path: Path, entry: dict) -> None`

**Description:**
Read the existing JSON manifest from `manifest_path` (if it exists), add or update the entry keyed by `entry["filename"]`, and write the manifest back. Each manifest entry must contain: `url`, `filename`, `size_bytes`, `download_timestamp` (ISO-8601 UTC string), `md5_checksum`.

**Parameters:**
- `manifest_path`: path to the JSON manifest file
- `entry`: dict with the fields listed above

**Error handling:**
- If manifest file does not exist, create it from scratch.
- If manifest file contains invalid JSON, log a warning and overwrite it.

**Dependencies:** `json`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` Given a non-existent manifest path, assert a new manifest file is created containing the entry.
- `@pytest.mark.unit` Given an existing manifest with one entry, add a second entry and assert both entries are present in the result.
- `@pytest.mark.unit` Given a manifest file with corrupted JSON, assert a warning is logged and the manifest is overwritten with the new entry.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-08: Per-year acquire worker

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `acquire_year(year: int, config: dict) -> dict`

**Description:**
Acquire production data for a single year. Determines whether the year is the current year or a past year, selects the appropriate URL, skips download if the target CSV already exists on disk (idempotent), downloads, extracts (for ZIP years), computes MD5, and returns a manifest entry dict.

**Parameters:**
- `year`: the year to acquire (integer)
- `config`: full config dict

**Returns:** manifest entry dict (see Task A-07 for fields)

**Behavior:**
- If `year == current_year`: URL = `config["acquire"]["monthly_url"]`, dest = `data/raw/monthly_prod.csv`
- If `year < current_year`: URL = zip template with year, dest = `data/raw/{year}_prod_reports.csv`; downloads ZIP to a temp path then calls `extract_csv_from_zip`
- If dest CSV already exists: log a message ("Skipping {filename}, already exists") and return a manifest entry with `skipped: true`
- After download (not skipped): call `compute_md5`, record `size_bytes`, `download_timestamp` (UTC now), write to manifest via `update_manifest`
- Sleep 0.5 seconds after any successful download

**Error handling:**
- If download or extraction fails, log the error and re-raise so the Dask graph can surface the failure.

**Dependencies:** `datetime`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` When the target CSV already exists, assert the function returns a dict with `skipped: True` and `download_file` is never called.
- `@pytest.mark.unit` For a past year (mocked download and extraction), assert the returned dict contains all required manifest fields.
- `@pytest.mark.unit` For the current year (mocked download), assert the dest filename is `monthly_prod.csv`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-09: Acquire stage runner

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `run_acquire(config: dict) -> list[dict]`

**Description:**
Top-level acquire stage entry point. Builds a list of years from `config["acquire"]["year_start"]` through the current year (inclusive), creates a Dask `delayed` task per year calling `acquire_year`, then runs all tasks using `dask.compute(..., scheduler="threads", num_workers=config["acquire"]["max_workers"])`. Returns a list of manifest entry dicts. Logs a summary: total files attempted, skipped, downloaded, failed.

**Parameters:**
- `config`: full config dict

**Returns:** `list[dict]` — one manifest entry per year

**Error handling:**
- If any year's task raises, log the error and continue (do not abort remaining years). Collect failed years and log a summary at the end.
- If `data/raw/` directory does not exist, create it before building the task graph.

**Dependencies:** `dask`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` Mock `acquire_year` to return a fixed dict; assert `run_acquire` returns one entry per year in `year_start`..`current_year`.
- `@pytest.mark.unit` Mock `acquire_year` to raise for one year; assert the other years' results are still returned and the failure is logged.
- `@pytest.mark.integration` (requires network) Call `run_acquire` with `year_start` = current year only; assert `data/raw/monthly_prod.csv` exists and is non-empty after the call.

**Definition of done:** Function implemented, unit tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task A-10: Acquire idempotency and file integrity tests (TR-20, TR-21)

**Test file:** `tests/test_acquire.py`

**Description:**
Implement the test cases required by TR-20 and TR-21 from `test-requirements.xml`.

**Test cases:**

- `@pytest.mark.unit` **TR-20a** Mock the filesystem so that the target CSV already exists; call `run_acquire` a second time; assert file count in `data/raw/` is unchanged and no download function is called.
- `@pytest.mark.unit` **TR-20b** Mock a first acquire run that writes a file; mock a second run; assert the file content is identical after both runs.
- `@pytest.mark.unit` **TR-20c** Assert no exception is raised when `acquire_year` is called and the target CSV already exists.
- `@pytest.mark.integration` **TR-21a** After a real acquire run (current year only), assert every file in `data/raw/` has `os.path.getsize > 0`.
- `@pytest.mark.integration` **TR-21b** After a real acquire run, assert every file in `data/raw/` is readable as text (UTF-8) without `UnicodeDecodeError`.
- `@pytest.mark.integration` **TR-21c** After a real acquire run, assert every CSV file contains at least one row beyond the header line.

**Definition of done:** All test cases implemented and passing (unit tests pass without network; integration tests marked appropriately), `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.
