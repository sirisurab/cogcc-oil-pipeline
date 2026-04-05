# Acquire Stage — Task Specifications

## Overview

The acquire stage is responsible for downloading COGCC/ECMC oil and gas production data
files from the public ECMC data portal for years 2020 through the current year. Each year
except the current year is available as a ZIP archive containing a CSV; the current year
is available as a direct CSV download. The stage uses Dask (via `dask.bag` or
`concurrent.futures` coordinated by Dask delayed) for parallel downloads, rate-limited to
a maximum of 5 concurrent workers with a 0.5-second sleep per worker.

The pipeline package is named `cogcc_pipeline`. All source modules live under
`cogcc_pipeline/`. All test files live under `tests/`.

---

## Component Architecture

### Modules and files produced by this stage

| Artefact | Purpose |
|---|---|
| `cogcc_pipeline/acquire.py` | All acquisition logic |
| `config/pipeline_config.yaml` | YAML configuration for URLs, years, paths, worker counts |
| `references/production-data-dictionary.csv` | Data dictionary parsed from ECMC HTML page |
| `data/raw/{year}_prod_reports.csv` | Annual production CSVs (2020 … current-year-minus-1) |
| `data/raw/monthly_prod.csv` | Current-year live monthly CSV |
| `tests/test_acquire.py` | Pytest unit and integration tests |
| `pyproject.toml` | Project build configuration (build-backend = setuptools.build_meta) |
| `Makefile` | `make env` and `make install` targets |
| `.gitignore` | Ignores data/, large_tool_results/, conversation_history/ |
| `requirements.txt` | Third-party dependencies |

---

## Design Decisions and Constraints

- **Build backend**: `pyproject.toml` must use `build-backend = "setuptools.build_meta"`.
  Never use `"setuptools.backends.legacy:build"`.
- **Makefile**: must include a `make env` target that creates `.venv` via
  `python3 -m venv .venv`. The `make install` target must bootstrap pip, setuptools, and
  wheel (`pip install --upgrade pip setuptools wheel`) before running
  `pip install -e ".[dev]"`. Never rely solely on `pip install -r requirements.txt`.
- **HTTP only**: Use `requests` and `BeautifulSoup` for HTTP downloads and HTML parsing.
  Do not use Playwright or any browser automation.
- **Idempotency**: If a target file already exists in `data/raw/`, skip the download.
  Never overwrite or corrupt an existing file.
- **Rate limiting**: Maximum 5 concurrent download workers. Each worker sleeps 0.5 seconds
  before issuing its HTTP request.
- **Parallelism**: Coordinate parallel downloads using Dask delayed or
  `concurrent.futures.ThreadPoolExecutor` with `max_workers=5`.
- **Configuration**: All URLs, year range, output paths, and worker counts must be read
  from `config/pipeline_config.yaml`. Hard-coding is not permitted.
- **Logging**: Use Python `logging` (structured). Log download start, skip (file exists),
  success (bytes written), and failure (exception type + message) for every file.
- **`.gitignore`**: The file must list `data/`, `large_tool_results/`, and
  `conversation_history/` so large data files are not committed.
- **pyproject.toml dev extras** must include `pandas-stubs` and `types-requests` so mypy
  type checks pass.
- **Type hints**: All public functions and classes must have full type annotations.
- **Docstrings**: All public functions and classes must have docstrings.

---

## Task 01: Create project scaffolding

**Artefacts:**
- `pyproject.toml`
- `Makefile`
- `.gitignore`
- `requirements.txt`
- `cogcc_pipeline/__init__.py`
- `config/pipeline_config.yaml`

**Description:**

Create the full project scaffolding so that the package is installable and the development
environment is reproducible.

### pyproject.toml

Define the package name `cogcc_pipeline`, version `0.1.0`, Python requires `>=3.11`.
Set `build-backend = "setuptools.build_meta"` (not the legacy variant).

Runtime dependencies (install_requires):
`requests`, `beautifulsoup4`, `lxml`, `pandas`, `numpy`, `pyarrow`, `dask[dataframe]`,
`pyyaml`, `tqdm`, `scikit-learn`.

Dev extras (dev):
`pytest`, `pytest-mock`, `responses`, `mypy`, `ruff`, `pandas-stubs`, `types-requests`,
`types-PyYAML`.

The `[tool.ruff]` section must target Python 3.11 and enable `E`, `F`, `I` rule sets.
The `[tool.mypy]` section must set `strict = true` and `ignore_missing_imports = true`.

### Makefile

Include the following targets:

- `env`: creates `.venv` with `python3 -m venv .venv`
- `install`: activates `.venv`, runs `pip install --upgrade pip setuptools wheel`,
  then `pip install -e ".[dev]"`
- `lint`: runs `ruff check cogcc_pipeline/ tests/`
- `type-check`: runs `mypy cogcc_pipeline/ tests/`
- `test`: runs `pytest tests/ -v`
- `test-unit`: runs `pytest tests/ -v -m unit`
- `test-integration`: runs `pytest tests/ -v -m integration`
- `pipeline`: runs the full pipeline via `python -m cogcc_pipeline`

### .gitignore

Must include: `data/`, `large_tool_results/`, `conversation_history/`, `.venv/`,
`__pycache__/`, `*.pyc`, `*.egg-info/`, `.mypy_cache/`, `.ruff_cache/`, `logs/`,
`dist/`, `build/`.

### config/pipeline_config.yaml

Define the following configuration keys:

```
acquire:
  base_url_zip: "https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip"
  base_url_monthly: "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv"
  data_dictionary_url: "https://ecmc.state.co.us/documents/data/downloads/production/production_record_data_dictionary.htm"
  start_year: 2020
  raw_dir: "data/raw"
  references_dir: "references"
  max_workers: 5
  sleep_seconds: 0.5

ingest:
  raw_dir: "data/raw"
  interim_dir: "data/interim"
  start_year: 2020

transform:
  interim_dir: "data/interim"
  processed_dir: "data/processed"
  cleaned_file: "cogcc_production_cleaned.parquet"

features:
  processed_dir: "data/processed"
  cleaned_file: "cogcc_production_cleaned.parquet"
  features_file: "cogcc_production_features.parquet"
  quality_report: "data_quality_report.json"
  rolling_windows: [3, 6, 12]

logging:
  log_dir: "logs"
  log_file: "pipeline_run.log"
  level: "INFO"
```

### cogcc_pipeline/__init__.py

Empty or minimal; exposes package version string `__version__ = "0.1.0"`.

### requirements.txt

List all third-party runtime packages (without version pins initially) that the project
imports. This file is generated from the pyproject.toml dependencies for reference but
`pip install -e ".[dev]"` is the authoritative install mechanism.

**Definition of done:** `pyproject.toml`, `Makefile`, `.gitignore`, `requirements.txt`,
`cogcc_pipeline/__init__.py`, and `config/pipeline_config.yaml` are created; `pip install
-e ".[dev]"` completes without error; `ruff check` and `mypy` report no errors on an
empty module; `requirements.txt` updated with all third-party packages imported in this
task.

---

## Task 02: Implement configuration loader utility

**Module:** `cogcc_pipeline/utils.py`
**Function:** `load_config(config_path: str | Path) -> dict`

**Description:**

Implement a utility function that reads `config/pipeline_config.yaml` using `PyYAML` and
returns the parsed dictionary. The function must resolve the path relative to the project
root if a relative path is passed.

Also implement:

**Function:** `get_logger(name: str, log_dir: str, log_file: str, level: str) -> logging.Logger`

Sets up a Python `logging.Logger` that writes simultaneously to stdout (StreamHandler)
and to a rotating file at `{log_dir}/{log_file}` (RotatingFileHandler, max 10 MB,
3 backups). Log format: `%(asctime)s | %(name)s | %(levelname)s | %(message)s`.
Creates `log_dir` if it does not exist. Returns the configured logger.

Both functions must have full type hints and docstrings.

**Dependencies:** `pyyaml`, `pathlib`, `logging`

**Test cases (`tests/test_acquire.py` — unit section):**

- `@pytest.mark.unit` — Given a valid YAML file, assert `load_config` returns a dict with
  top-level keys `acquire`, `ingest`, `transform`, `features`, `logging`.
- `@pytest.mark.unit` — Given a non-existent path, assert `load_config` raises
  `FileNotFoundError`.
- `@pytest.mark.unit` — Given valid config params, assert `get_logger` returns a
  `logging.Logger` instance and that it has at least two handlers (stream + file).

**Definition of done:** `cogcc_pipeline/utils.py` implemented; all unit tests pass;
`ruff` and `mypy` report no errors; `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 03: Implement data dictionary downloader and parser

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `fetch_data_dictionary(url: str, output_path: Path, logger: logging.Logger) -> Path`

**Description:**

Use `requests.get` to fetch the HTML page at the ECMC data dictionary URL. Parse the
first HTML table on the page using `BeautifulSoup` with the `lxml` parser. Convert the
table into a `pandas.DataFrame` with columns inferred from the `<th>` header row. Write
the DataFrame to `output_path` as a CSV file using UTF-8 encoding. Return the output path.

If the file already exists at `output_path`, log a skip message and return the path
without fetching.

If the HTTP response status code is not 200, raise `requests.HTTPError` with a message
that includes the URL and status code.

If no table is found on the page, raise `ValueError("No table found on data dictionary page")`.

**Dependencies:** `requests`, `beautifulsoup4`, `lxml`, `pandas`, `pathlib`, `logging`

**Test cases (`tests/test_acquire.py`):**

- `@pytest.mark.unit` — Mock `requests.get` to return a minimal HTML page with one table.
  Assert the output CSV is written to the expected path and contains the correct columns
  and at least one row.
- `@pytest.mark.unit` — Mock `requests.get` to return HTTP 404. Assert `HTTPError` is raised.
- `@pytest.mark.unit` — Mock `requests.get` to return HTML with no table. Assert
  `ValueError` is raised.
- `@pytest.mark.unit` — Given the output file already exists, assert `requests.get` is
  never called (skip behaviour verified via mock call count = 0).
- `@pytest.mark.integration` — Call `fetch_data_dictionary` against the real ECMC URL.
  Assert the returned path exists, the file is non-empty, and the CSV contains at least
  one column named `Field` or similar. Requires network access.

**Definition of done:** `fetch_data_dictionary` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 04: Implement per-year download functions

**Module:** `cogcc_pipeline/acquire.py`

**Function:** `download_annual_zip(year: int, url_template: str, raw_dir: Path, sleep_seconds: float, logger: logging.Logger) -> Path | None`

**Description:**

Downloads the ZIP archive for the given year from the URL formed by substituting `{year}`
into `url_template`. Extracts the first `.csv` file found in the ZIP to
`raw_dir/{year}_prod_reports.csv`. Returns the path to the extracted CSV.

If `raw_dir/{year}_prod_reports.csv` already exists, log a skip message and return the
path without downloading.

Sleep `sleep_seconds` before issuing the HTTP request.

If the HTTP response status code is not 200, log a warning with the URL and status code
and return `None`.

If the ZIP contains no CSV file, log a warning and return `None`.

Raise `OSError` if `raw_dir` cannot be created.

**Function:** `download_monthly_csv(url: str, raw_dir: Path, sleep_seconds: float, logger: logging.Logger) -> Path | None`

**Description:**

Downloads the current-year live CSV directly from `url` to
`raw_dir/monthly_prod.csv`. Returns the path.

If `raw_dir/monthly_prod.csv` already exists, log a skip message and return the path
without downloading (idempotency). Note: unlike annual files, the monthly file may be
updated daily; the skip logic should allow an optional `force_refresh: bool = False`
parameter that, when `True`, re-downloads even if the file exists.

Sleep `sleep_seconds` before issuing the HTTP request.

If the HTTP response status code is not 200, log a warning and return `None`.

**Dependencies:** `requests`, `zipfile`, `pathlib`, `time`, `logging`

**Test cases (`tests/test_acquire.py`):**

- `@pytest.mark.unit` — Mock `requests.get` to return a valid ZIP binary containing one
  CSV file. Assert the extracted CSV is written to the correct path and the function
  returns that path.
- `@pytest.mark.unit` — When the target file already exists, assert `requests.get` is
  not called (skip behaviour) and the existing file path is returned unchanged.
- `@pytest.mark.unit` — Mock `requests.get` to return HTTP 503. Assert the function
  returns `None` and logs a warning.
- `@pytest.mark.unit` — Mock `requests.get` to return a ZIP containing no CSV files.
  Assert the function returns `None` and logs a warning.
- `@pytest.mark.unit` — Mock `requests.get` for `download_monthly_csv` to return CSV
  bytes. Assert the file is written to `monthly_prod.csv`.
- `@pytest.mark.unit` — `download_monthly_csv` with `force_refresh=True` on an existing
  file: assert `requests.get` IS called and the file is overwritten.
- `@pytest.mark.unit` (TR-20) — Simulate two sequential calls to `download_annual_zip`
  for the same year using mocked HTTP responses. Assert the file count in `raw_dir` after
  the second call equals the count after the first call — no duplication. Assert the
  content of the file is identical to after the first run.
- `@pytest.mark.integration` (TR-20) — Run `download_annual_zip` twice for year 2020
  against the real ECMC server. Assert no error, same file count, file unchanged.
  Requires network access.

**Definition of done:** Both functions implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 05: Implement parallel download orchestration

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `run_acquire(config: dict, logger: logging.Logger) -> list[Path]`

**Description:**

This is the top-level entry point for the acquire stage. It performs the following steps:

1. Read `acquire` section from `config`.
2. Compute the list of years to download: integers from `start_year` through
   `current_year - 1` (inclusive) for ZIP downloads, plus the current year for the
   monthly CSV.
3. Build a list of download tasks — one `download_annual_zip` call per historical year
   plus one `download_monthly_csv` call for the current year.
4. Execute all tasks in parallel using `concurrent.futures.ThreadPoolExecutor` with
   `max_workers = config["acquire"]["max_workers"]` (capped at 5).
5. Collect results. Any task that returned `None` is logged as a failed download but does
   not abort the run.
6. Call `fetch_data_dictionary` (single-threaded, not parallelised) after all downloads
   complete.
7. Return the list of non-None Paths from all download tasks.

**Dependencies:** `concurrent.futures`, `datetime`, `pathlib`, `logging`

**Test cases (`tests/test_acquire.py`):**

- `@pytest.mark.unit` — Mock `download_annual_zip` and `download_monthly_csv` to return
  known paths. Assert `run_acquire` returns a list of those paths and that worker count
  does not exceed 5.
- `@pytest.mark.unit` — When two of the mocked download functions return `None`, assert
  those are excluded from the returned list and a warning is logged for each.
- `@pytest.mark.unit` (TR-21) — Mock all download functions. After the mock run, assert
  every path in the returned list has `stat().st_size > 0`. Use `tmp_path` fixture to
  write synthetic CSV content into each mocked path before returning it.
- `@pytest.mark.integration` — Run `run_acquire` against the real ECMC server with
  years 2020–2021 only (override config). Assert at least two files are written to
  `data/raw/` and each file size > 0 bytes. Requires network access.

**Definition of done:** `run_acquire` implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 06: Implement acquired file integrity validation

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `validate_raw_files(raw_dir: Path, logger: logging.Logger) -> list[str]`

**Description:**

After downloading, validate every `.csv` file in `raw_dir`. For each file, check:

1. File size > 0 bytes.
2. File is readable as UTF-8 text (attempt `open(path, encoding="utf-8").read(4096)`).
   If that fails, try `latin-1` encoding. If both fail, record a validation error.
3. File contains at least one data row beyond a header line (line count >= 2).

Return a list of validation error strings (empty list if all files pass). Log one warning
per failed file.

**Test cases (`tests/test_acquire.py`):**

- `@pytest.mark.unit` (TR-21a) — Write a valid multi-line CSV to `tmp_path`. Assert
  `validate_raw_files` returns an empty error list.
- `@pytest.mark.unit` (TR-21b) — Write a 0-byte file to `tmp_path`. Assert the error
  list contains one entry mentioning that file.
- `@pytest.mark.unit` (TR-21c) — Write a file with only a header line and no data rows.
  Assert the error list contains one entry for that file.
- `@pytest.mark.unit` — Write a file with non-UTF-8 bytes that are valid Latin-1. Assert
  `validate_raw_files` returns an empty error list (fallback encoding succeeded).

**Definition of done:** `validate_raw_files` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 07: Wire acquire stage as CLI entry point

**Module:** `cogcc_pipeline/acquire.py`
**Block:** `if __name__ == "__main__":` guard and `cogcc_pipeline/__main__.py`

**Description:**

Add an `if __name__ == "__main__":` block in `acquire.py` that:
1. Calls `load_config("config/pipeline_config.yaml")`.
2. Calls `get_logger(...)` using logging config.
3. Calls `run_acquire(config, logger)`.
4. Calls `validate_raw_files(raw_dir, logger)` and logs any errors.
5. Exits with code 1 if any validation errors were returned; exits with code 0 otherwise.

Create `cogcc_pipeline/__main__.py` that imports and calls an `orchestrate()` stub (to be
completed in a later stage) or directly calls `run_acquire` as a fallback for now.
The full orchestrator will replace this stub in a later task.

**Test cases (`tests/test_acquire.py`):**

- `@pytest.mark.unit` — Mock `run_acquire` and `validate_raw_files`. Invoke the
  `__main__` guard via `subprocess.run` or by importing and calling the entry-point
  function. Assert exit code is 0 when no validation errors.
- `@pytest.mark.unit` — Mock `validate_raw_files` to return one error string. Assert
  exit code is 1.

**Definition of done:** Entry point implemented; all tests pass; `ruff` and `mypy` report
no errors; `requirements.txt` updated with all third-party packages imported in this task.
