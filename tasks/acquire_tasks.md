# Acquire Stage — Task Specifications

## Overview

The acquire stage downloads COGCC (Colorado Energy & Carbon Management Commission, ECMC) production
data files for years 2020 through the current year and places the resulting CSV files in `data/raw/`.

- For years 2020 through (current year − 1): download a zip archive, extract the CSV, and save it.
- For the current year: download the live monthly CSV directly (no zip).

All downloads must be rate-limited to a maximum of 5 concurrent workers and must be idempotent
(skip already-downloaded files). A 0.5-second sleep is applied per download worker after each
request to avoid overloading the server.

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`

---

## Design Decisions & Constraints

- Use `requests` for all HTTP operations. Do not use Playwright, Selenium, or any browser automation.
- Use `concurrent.futures.ThreadPoolExecutor` (I/O-bound) with `max_workers=5` for parallel downloads.
- All configurable values (base URL, output directory, years, worker count, sleep duration, retry
  parameters) must be read from `config.yaml` at runtime, never hardcoded in function bodies.
- Retry logic: up to 3 attempts per file with exponential back-off (1 s, 2 s, 4 s). Log a warning
  after each failed attempt; log an error and skip the file after 3 failures.
- A download failure for one year must not abort downloads for other years.
- File-already-exists check: if the target CSV file is present on disk and has size > 0 bytes, skip
  the download entirely (idempotency — TR-20).
- Zip extraction: use `zipfile.ZipFile`. Extract only the CSV file from the archive (ignore any other
  members). If the zip contains multiple CSV files, select the one whose name matches
  `*prod_reports*` (case-insensitive).
- URL patterns:
    - Annual zip: `https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip`
    - Current-year monthly CSV: `https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv`
- Output file naming convention:
    - Annual files: `data/raw/{year}_prod_reports.csv`
    - Current-year file: `data/raw/monthly_prod.csv`
- The data dictionary HTML page must be fetched once and saved to
  `references/production-data-dictionary.md` during the acquire stage.
- Data dictionary URL: `https://ecmc.state.co.us/documents/data/downloads/production/production_record_data_dictionary.htm`
- All logging must use Python's `logging` module with structured messages (include year/filename in
  every log entry). Do not use `print()`.
- `pyproject.toml` build-backend must be `setuptools.build_meta`. The `[project.optional-dependencies]`
  `dev` group must include `pandas-stubs` and `types-requests`.
- The `make env` target must create `.venv` using `python3 -m venv .venv` and bootstrap pip before
  installing: `pip install --upgrade pip setuptools wheel && pip install -e ".[dev]"`.

---

## Task 01: Create project scaffolding

**Module:** `cogcc_pipeline/__init__.py`, `pyproject.toml`, `Makefile`, `config.yaml`, `.gitignore`

**Description:**
Set up the project package and configuration infrastructure that all pipeline stages depend on.

Create the following artefacts:

1. **`cogcc_pipeline/__init__.py`** — empty init file marking the package.

2. **`pyproject.toml`** — project metadata and dependency specification:
   - `build-backend = "setuptools.build_meta"` (never `setuptools.backends.legacy:build`)
   - `[project]` name: `cogcc-pipeline`, version: `0.1.0`, requires-python: `>=3.11`
   - Runtime dependencies: `pandas`, `pyarrow`, `dask[dataframe]`, `requests`, `tqdm`,
     `scikit-learn`, `pyyaml`, `beautifulsoup4`, `lxml`
   - `[project.optional-dependencies]` dev group: `pytest`, `pytest-mock`, `ruff`, `mypy`,
     `pandas-stubs`, `types-requests`
   - `[tool.setuptools.packages.find]` where: `["."]`

3. **`Makefile`** — with the following targets:
   - `env`: creates `.venv` using `python3 -m venv .venv`
   - `install`: activates `.venv`, runs `pip install --upgrade pip setuptools wheel`,
     then `pip install -e ".[dev]"`
   - `test`: runs `pytest tests/ -v`
   - `lint`: runs `ruff check cogcc_pipeline/ tests/`
   - `typecheck`: runs `mypy cogcc_pipeline/`
   - `acquire`, `ingest`, `transform`, `features`: each runs the corresponding pipeline module
     as `python -m cogcc_pipeline.<stage>`

4. **`config.yaml`** — all pipeline configuration in one file:
   ```
   acquire:
     base_url: "https://ecmc.state.co.us/documents/data/downloads/production"
     data_dict_url: "https://ecmc.state.co.us/documents/data/downloads/production/production_record_data_dictionary.htm"
     monthly_url: "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv"
     start_year: 2020
     raw_dir: "data/raw"
     references_dir: "references"
     max_workers: 5
     sleep_seconds: 0.5
     retry_attempts: 3
     retry_backoff_base: 1
   ingest:
     raw_dir: "data/raw"
     interim_dir: "data/interim"
     target_start_year: 2020
     chunk_size: 100000
   transform:
     interim_dir: "data/interim"
     processed_dir: "data/processed"
   features:
     processed_dir: "data/processed"
     output_dir: "data/processed/features"
   ```

5. **`.gitignore`** — must include: `data/`, `large_tool_results/`, `conversation_history/`,
   `.venv/`, `__pycache__/`, `*.pyc`, `*.egg-info/`, `.pytest_cache/`, `.ruff_cache/`, `.mypy_cache/`

**Error handling:** `pyproject.toml` must be parseable by pip. Config must be loadable by
`yaml.safe_load`.

**Dependencies:** pyyaml, setuptools

**Test cases:**
- `@pytest.mark.unit` — Assert that `config.yaml` is loadable with `yaml.safe_load` and contains all
  required top-level keys: `acquire`, `ingest`, `transform`, `features`.
- `@pytest.mark.unit` — Assert that `pyproject.toml` specifies `build-backend = "setuptools.build_meta"`.
- `@pytest.mark.unit` — Assert that the `dev` optional-dependencies in `pyproject.toml` include
  `pandas-stubs` and `types-requests`.

**Definition of done:** All artefacts created, unit tests pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement configuration loader

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `load_config(config_path: str = "config.yaml") -> dict`

**Description:**
Read `config.yaml` from disk and return the full configuration dictionary. This function is used
by all stages to access their respective configuration sub-trees.

- Accept an optional `config_path` argument defaulting to `"config.yaml"`.
- Use `yaml.safe_load` to parse the file.
- Raise a descriptive `FileNotFoundError` if the config file does not exist at the specified path.
- Raise a `KeyError` with a helpful message if a required top-level key is missing.
- Return the full config dict (not a sub-tree) so callers can access any section.

**Error handling:** `FileNotFoundError` for missing config; `ValueError` for YAML parse errors.

**Dependencies:** pyyaml

**Test cases:**
- `@pytest.mark.unit` — Given a valid `config.yaml` with all required keys, assert the returned dict
  contains keys `acquire`, `ingest`, `transform`, `features`.
- `@pytest.mark.unit` — Given a path to a non-existent file, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given a YAML file with invalid syntax, assert a `ValueError` is raised.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement structured logger

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `get_logger(name: str) -> logging.Logger`

**Description:**
Create and return a configured `logging.Logger` for use throughout the pipeline. This is a
shared utility function placed in `acquire.py` and re-used by all other modules.

- Configure a `StreamHandler` writing to stdout with the format:
  `%(asctime)s | %(name)s | %(levelname)s | %(message)s`
- Set default log level to `INFO`.
- Do not add duplicate handlers if the logger already exists (check `logger.handlers`).
- Return the configured logger.

**Error handling:** No exceptions expected; this function must not raise.

**Dependencies:** logging (stdlib)

**Test cases:**
- `@pytest.mark.unit` — Assert that `get_logger("test")` returns a `logging.Logger` instance.
- `@pytest.mark.unit` — Assert that calling `get_logger("test")` twice returns a logger with
  exactly one handler (no duplicate handlers appended).
- `@pytest.mark.unit` — Assert the logger level is `logging.INFO`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement retry-enabled HTTP downloader

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `download_with_retry(url: str, dest_path: Path, cfg: dict) -> bool`

**Description:**
Download a file from `url` and write it to `dest_path`. Implements retry logic with exponential
back-off. Returns `True` on success, `False` after all retries are exhausted.

- If `dest_path` already exists on disk with size > 0 bytes, log an info message and return `True`
  immediately without making any network request (idempotency).
- Make up to `cfg["retry_attempts"]` HTTP GET attempts (default 3).
- Use `stream=True` to download in chunks (chunk size 8192 bytes) and write incrementally.
- After each successful chunk write, do not sleep (sleep is applied at the worker level).
- On `requests.RequestException`: log a warning with the attempt number and URL, wait
  `cfg["retry_backoff_base"] * (2 ** attempt)` seconds before the next attempt.
- After all attempts fail: log an error stating the file could not be downloaded and return `False`.
- Ensure partial files are deleted if a download fails mid-stream (use a try/finally or a temp-file
  strategy).
- Do not raise exceptions to the caller — always return `True` or `False`.

**Error handling:** Catches `requests.RequestException`. Cleans up partial files. Logs all
attempt failures.

**Dependencies:** requests, pathlib, time, logging

**Test cases:**
- `@pytest.mark.unit` — Given a mock `requests.get` that returns a 200 response with bytes content,
  assert the function returns `True` and the file is written to `dest_path`.
- `@pytest.mark.unit` — Given a `dest_path` that already exists with size > 0, assert the function
  returns `True` immediately and `requests.get` is never called (mock and assert `call_count == 0`).
- `@pytest.mark.unit` — Given a mock `requests.get` that raises `requests.ConnectionError` on all
  attempts, assert the function returns `False` and no partial file remains on disk.
- `@pytest.mark.unit` — Given a mock `requests.get` that fails on the first two attempts then
  succeeds, assert the function returns `True` (retry recovery works).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement data dictionary fetcher and saver

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `fetch_data_dictionary(cfg: dict) -> Path`

**Description:**
Fetch the ECMC production data dictionary HTML page, parse the column table with
`BeautifulSoup`, and write a Markdown-formatted reference file to
`references/production-data-dictionary.md`. Return the path to the written file.

- Fetch the URL at `cfg["data_dict_url"]` using `requests.get`.
- Parse with `BeautifulSoup(html, "lxml")`.
- Find the HTML table containing column names and descriptions.
- Write the output to `references/production-data-dictionary.md` as a Markdown table with columns:
  `| Column Name | Description |`.
- If the file already exists, overwrite it unconditionally (data dictionary may be updated).
- If the fetch fails (network error or non-200 status), log an error and raise
  `RuntimeError("Failed to fetch data dictionary")`.
- Create the `references/` directory if it does not exist.

**Error handling:** `RuntimeError` on fetch failure. `OSError` logged and re-raised on write failure.

**Dependencies:** requests, beautifulsoup4, lxml, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` — Given a mock HTTP response containing a minimal HTML table with two rows,
  assert the function writes a file to `references/production-data-dictionary.md` that contains
  a Markdown table header `| Column Name | Description |`.
- `@pytest.mark.unit` — Given a mock HTTP response returning status 404, assert `RuntimeError` is
  raised.
- `@pytest.mark.integration` — Run against the live URL; assert the file is written and contains
  at least 10 rows (i.e., at least 10 known column names from the production schema).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`references/production-data-dictionary.md` is written to disk, `requirements.txt` updated with
all third-party packages imported in this task.

---

## Task 06: Implement annual zip downloader and extractor

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `download_annual_zip(year: int, cfg: dict) -> Path | None`

**Description:**
Download the annual production zip archive for a given year, extract the production CSV file,
and return the path to the extracted CSV. Used for all years from 2020 through (current year − 1).

- Construct the zip URL as: `{cfg["base_url"]}/{year}_prod_reports.zip`
- Call `download_with_retry` to download the zip to a temporary path `data/raw/{year}_prod_reports.zip`.
- If download returns `False`, log an error and return `None`.
- After a 0.5-second sleep (`cfg["sleep_seconds"]`), open the zip with `zipfile.ZipFile`.
- Find the CSV member matching `*prod_reports*` (case-insensitive). If none matches, raise
  `ValueError(f"No CSV found in zip for {year}")`.
- Extract that CSV to `data/raw/{year}_prod_reports.csv`.
- Delete the zip file after successful extraction.
- If the target CSV already exists and has size > 0, skip download and extraction entirely
  (idempotency), log info, and return the existing path.
- Return the `Path` to the extracted CSV.

**Error handling:** Returns `None` on download failure. Raises `ValueError` if zip contains no
matching CSV. Cleans up zip file in a `finally` block.

**Dependencies:** requests, zipfile, pathlib, time, logging

**Test cases:**
- `@pytest.mark.unit` — Given a mock `download_with_retry` returning `True` and a mock zip archive
  containing a file named `2022_prod_reports.csv`, assert the function returns a path to
  `data/raw/2022_prod_reports.csv` and the zip file is deleted.
- `@pytest.mark.unit` — Given a mock `download_with_retry` returning `False`, assert the function
  returns `None`.
- `@pytest.mark.unit` — Given a zip with no file matching `*prod_reports*`, assert `ValueError`
  is raised.
- `@pytest.mark.unit` — Given that `data/raw/2022_prod_reports.csv` already exists with size > 0,
  assert no download is attempted and the existing path is returned.
- `@pytest.mark.integration` — Download the 2022 zip from the live ECMC URL, assert the extracted
  CSV exists, has size > 0, and contains a recognisable header row.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Implement current-year monthly CSV downloader

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `download_monthly_csv(cfg: dict) -> Path | None`

**Description:**
Download the live monthly production CSV for the current calendar year from
`cfg["monthly_url"]`. This file is updated continuously throughout the year by ECMC. Return the
path to the saved file, or `None` on failure.

- Always re-download this file unconditionally (do NOT skip if it exists) because it is updated
  daily by the source.
- Target path: `data/raw/monthly_prod.csv`
- Apply the 0.5-second sleep after the download completes.
- Use `download_with_retry` with `skip_if_exists=False` override (pass a modified cfg that sets
  `retry_attempts` from config).
- If download fails, log an error and return `None`.

**Error handling:** Returns `None` on failure. Does not raise.

**Dependencies:** requests, pathlib, time, logging

**Test cases:**
- `@pytest.mark.unit` — Given a mock `download_with_retry` that writes a non-empty file, assert
  the function returns a `Path` pointing to `data/raw/monthly_prod.csv`.
- `@pytest.mark.unit` — Given a mock `download_with_retry` returning `False`, assert the function
  returns `None`.
- `@pytest.mark.integration` — Download the live monthly CSV; assert the file exists, size > 0,
  and the first line is a recognisable CSV header matching expected column names.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Implement parallel acquire orchestrator

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `run_acquire(config_path: str = "config.yaml") -> list[Path]`

**Description:**
Top-level entry point for the acquire stage. Orchestrates all downloads for years 2020 through
the current year using a `ThreadPoolExecutor` with at most `cfg["max_workers"]` workers (default 5).
Returns a list of `Path` objects for all successfully downloaded/extracted CSV files.

- Load config via `load_config(config_path)`.
- Compute `years = range(cfg["acquire"]["start_year"], current_year)` for annual zips.
- Submit `download_annual_zip(year, cfg["acquire"])` for each historical year to the thread pool.
- Submit `download_monthly_csv(cfg["acquire"])` as a separate task for the current year.
- Collect `Future` results; log a summary of how many files were acquired successfully vs. failed.
- Return only the paths that are not `None`.
- Also call `fetch_data_dictionary(cfg["acquire"])` once before the parallel downloads (sequential,
  outside the pool).
- Create `data/raw/` directory if it does not exist.

**Error handling:** Individual file failures are caught and logged. The function always returns
(never raises) — a partial list is a valid result.

**Dependencies:** concurrent.futures, pathlib, logging, datetime

**Test cases:**
- `@pytest.mark.unit` — Given mocked `download_annual_zip` returning valid paths for years 2020–2022
  and mocked `download_monthly_csv` returning a valid path, assert `run_acquire` returns a list
  of 4 `Path` objects (3 annual + 1 monthly).
- `@pytest.mark.unit` — Given mocked `download_annual_zip` returning `None` for one year, assert
  that year is absent from the returned list and no exception is raised.
- `@pytest.mark.unit` — Assert that the thread pool respects `max_workers=2` when configured
  with a patch (mock `ThreadPoolExecutor` and verify it is called with `max_workers=2`).
- `@pytest.mark.integration` — Run `run_acquire` end-to-end (real network); assert each returned
  path exists on disk, has size > 0, and is readable as text (TR-21).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 09: Implement acquire idempotency verification (TR-20)

**Module:** `tests/test_acquire.py`

**Description:**
Dedicated test suite verifying acquire idempotency per TR-20. These tests must be separate from
the unit tests in Task 08 to make the TR-20 coverage explicit and auditable.

**Test cases:**
- `@pytest.mark.unit` — Simulate a first acquire run writing a file to a tmp directory. Then call
  the relevant download function again. Assert file count in the directory is the same as after the
  first run (not doubled).
- `@pytest.mark.unit` — Simulate a first acquire run writing known content to a file. Then call the
  download function again with the same dest_path. Assert the file content is unchanged.
- `@pytest.mark.unit` — Assert no exception is raised when `download_with_retry` is called with
  a `dest_path` that already exists.

**Definition of done:** Tests implemented and passing, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 10: Implement acquire file integrity verification (TR-21)

**Module:** `tests/test_acquire.py`

**Description:**
Dedicated test suite verifying acquired file integrity per TR-21.

**Test cases:**
- `@pytest.mark.integration` — After running `run_acquire` against real or mocked download
  output, assert every file in `data/raw/` has size > 0 bytes.
- `@pytest.mark.integration` — Assert every file in `data/raw/` is readable as UTF-8 text without
  a `UnicodeDecodeError`.
- `@pytest.mark.integration` — Assert every file in `data/raw/` contains at least 2 lines
  (a header plus at least one data row).

**Definition of done:** Tests implemented and passing, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Module-level entry point

**Module:** `cogcc_pipeline/acquire.py`

Add a `if __name__ == "__main__":` block that calls `run_acquire()` so the stage can be executed
via `python -m cogcc_pipeline.acquire`.
