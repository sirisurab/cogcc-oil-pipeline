# Acquire Component Tasks

**Pipeline package:** `cogcc_pipeline`
**Module file:** `cogcc_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`

## Overview

The acquire stage downloads COGCC/ECMC production data files from the Colorado Energy
and Carbon Management Commission (ECMC) website. For historical years (2020 through the
year prior to the current year), it downloads annual ZIP archives and extracts the
contained CSV. For the current year it downloads a single flat monthly CSV directly.
All resulting CSV files are placed in `data/raw/`. Downloads are parallelised using the
Dask threaded scheduler (not the distributed scheduler). A 0.5-second sleep per worker
is applied to avoid server overload. The stage is fully idempotent — re-running it must
skip files that already exist and are valid.

---

## Design Decisions and Constraints

- URL patterns are read from `config.yaml` (`acquire` section). No URL is hardcoded in
  `acquire.py`.
- Historical years: `https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip`
- Current year: `https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv`
- Output filenames:
  - Historical: `data/raw/{year}_prod_reports.csv`
  - Current year: `data/raw/monthly_prod.csv`
- Maximum 5 concurrent download workers (from `config.yaml acquire.max_workers`).
- Use `requests` + `BeautifulSoup` only. Do NOT use Playwright or any browser automation.
- Use Dask's threaded scheduler: pass `scheduler="threads"` and
  `num_workers=config["acquire"]["max_workers"]` directly to `dask.compute()`. Do NOT
  initialise a distributed Client in this stage.
- Skip download if the target file already exists on disk AND has size > 0 bytes AND is
  readable as UTF-8 text with at least one data row beyond the header. Log a
  `DEBUG` message for skipped files.
- Log a `WARNING` if a downloaded file is 0 bytes; do not raise.
- All other HTTP or IO errors must be caught, logged at `ERROR` level, and re-raised as
  `AcquireError`.
- The `data/raw/` directory is created automatically if it does not exist.
- `logs/` directory must be created at startup if absent; log to both stdout and
  `logs/pipeline.log`.

---

## Task 01: Scaffold project structure and pyproject.toml

**Module:** `pyproject.toml`, `Makefile`, `config.yaml`, `.gitignore`, `cogcc_pipeline/__init__.py`

**Description:**
Create the project scaffold required before any pipeline code can be written.

**Artefacts to create:**

### `pyproject.toml`
- `build-backend` must be `"setuptools.build_meta"` (never `"setuptools.backends.legacy:build"`).
- Package name: `cogcc-pipeline`; `packages = [{find = {}}]`; `python_requires = ">=3.11"`
- Runtime dependencies: `pandas`, `numpy`, `requests`, `pyarrow`, `dask[distributed]`,
  `bokeh`, `scikit-learn`, `pydantic`, `tqdm`, `pyyaml`, `beautifulsoup4`, `lxml`
- Dev dependencies: `pytest`, `pytest-mock`, `ruff`, `mypy`, `pandas-stubs`,
  `types-requests`, `types-PyYAML`
- CLI entry point: `cogcc-pipeline = "cogcc_pipeline.pipeline:main"`

### `Makefile`
- `env` target: creates `.venv` using `python3 -m venv .venv`
- `install` target: activates venv, runs `pip install --upgrade pip setuptools wheel`,
  then `pip install -e ".[dev]"`
- Individual stage targets (`acquire`, `ingest`, `transform`, `features`) each invoke the
  CLI with the appropriate stage argument, e.g.:
  `python -m cogcc_pipeline.pipeline --stages acquire`
- `pipeline` target: depends on `acquire ingest transform features` in order and invokes
  `cogcc-pipeline` (the registered CLI entry point) with no arguments (runs all stages).
- `test` target: `pytest tests/ -v`
- `lint` target: `ruff check cogcc_pipeline/ tests/`
- `typecheck` target: `mypy cogcc_pipeline/`

### `config.yaml`
Top-level sections: `acquire`, `ingest`, `transform`, `features`, `dask`, `logging`.

```
acquire:
  start_year: 2020
  raw_dir: "data/raw"
  max_workers: 5
  zip_url_template: "https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip"
  monthly_url: "https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv"
  sleep_seconds: 0.5

ingest:
  raw_dir: "data/raw"
  interim_dir: "data/interim"
  start_year: 2020

transform:
  interim_dir: "data/interim"
  processed_dir: "data/processed"

features:
  processed_dir: "data/processed"
  output_dir: "data/features"
  rolling_windows: [3, 6]
  lag_periods: [1, 3]

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

### `.gitignore`
Must include: `.venv/`, `__pycache__/`, `*.pyc`, `*.egg-info/`, `.pytest_cache/`,
`dist/`, `build/`, `logs/`, `data/`, `large_tool_results/`, `conversation_history/`

### `cogcc_pipeline/__init__.py`
Empty init; version string `__version__ = "0.1.0"`.

**Definition of done:** All scaffold files exist. `make env && make install` completes
without errors. `cogcc-pipeline --help` prints usage. `pytest tests/` runs (even with 0
tests). `ruff check cogcc_pipeline/` and `mypy cogcc_pipeline/` report no errors.
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement custom exception and config loader

**Module:** `cogcc_pipeline/acquire.py`
**Classes/Functions:**
- `class AcquireError(RuntimeError)` — raised on unrecoverable download or extraction errors
- `load_config(config_path: str = "config.yaml") -> dict` — reads and returns the parsed
  YAML config dict

**Description:**
`AcquireError` is a thin subclass of `RuntimeError` with no additional attributes.
`load_config` uses `pyyaml` to parse `config.yaml`. It must raise `FileNotFoundError` if
the config file does not exist, and `ValueError` if any required top-level key
(`acquire`, `ingest`, `transform`, `features`, `dask`, `logging`) is absent.

**Error handling:**
- `FileNotFoundError` when config path is missing
- `ValueError` when required sections are absent

**Dependencies:** `pyyaml`

**Test cases:**

- `@pytest.mark.unit` — Given a valid config dict written to a temp file, assert
  `load_config` returns a dict with all six required top-level keys.
- `@pytest.mark.unit` — Given a config file missing the `dask` key, assert `load_config`
  raises `ValueError`.
- `@pytest.mark.unit` — Given a non-existent path, assert `load_config` raises
  `FileNotFoundError`.

**Definition of done:** `AcquireError` and `load_config` implemented, all test cases
pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 03: Implement file validity checker

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `is_valid_file(path: Path) -> bool`

**Description:**
Returns `True` if and only if:
1. `path` exists on disk
2. `path.stat().st_size > 0`
3. The file is readable as UTF-8 text without a `UnicodeDecodeError`
4. The file contains at least one non-header data line (i.e., line count >= 2)

Returns `False` for any other condition. Never raises.

**Dependencies:** `pathlib`

**Test cases:**

- `@pytest.mark.unit` — Given a temp file with a header and one data row, assert returns
  `True`.
- `@pytest.mark.unit` — Given an empty temp file (0 bytes), assert returns `False`.
- `@pytest.mark.unit` — Given a temp file with only a header line and no data rows, assert
  returns `False`.
- `@pytest.mark.unit` — Given a path that does not exist, assert returns `False`.
- `@pytest.mark.unit` — Given a temp file containing invalid UTF-8 bytes, assert returns
  `False`.

**Definition of done:** `is_valid_file` implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 04: Implement ZIP downloader and extractor for historical years

**Module:** `cogcc_pipeline/acquire.py`
**Function:**
`download_year_zip(year: int, config: dict) -> Path`

**Description:**
Downloads the annual production ZIP file for a given historical year and extracts the
first CSV found inside the archive. Steps:
1. Construct the ZIP URL from `config["acquire"]["zip_url_template"].format(year=year)`.
2. Construct the expected output path: `Path(config["acquire"]["raw_dir"]) / f"{year}_prod_reports.csv"`.
3. If `is_valid_file(output_path)` returns `True`, log at `DEBUG` level and return
   `output_path` immediately (idempotent skip).
4. Otherwise, `GET` the ZIP URL using `requests`, stream the response, write to a temp
   file, then extract the first `.csv` entry from the ZIP archive to `output_path`.
5. Sleep `config["acquire"]["sleep_seconds"]` seconds after the download completes.
6. If the resulting file fails `is_valid_file`, log a `WARNING`.
7. On any `requests.RequestException` or `zipfile.BadZipFile`, catch, log `ERROR`, raise
   `AcquireError`.

**Returns:** `Path` to the extracted CSV file.

**Error handling:**
- `AcquireError` on network or ZIP extraction failures
- Logs `DEBUG` for skipped (already valid) files
- Logs `WARNING` for 0-byte or unreadable results
- Logs `ERROR` before re-raising as `AcquireError`

**Dependencies:** `requests`, `zipfile`, `pathlib`, `time`

**Test cases:**

- `@pytest.mark.unit` — Mock `requests.get` to return a valid ZIP bytes payload; assert
  the function returns a `Path` pointing to a `.csv` file in the configured raw dir.
- `@pytest.mark.unit` — If the output CSV already exists and passes `is_valid_file`,
  assert the function returns immediately without calling `requests.get` (mock is never
  called).
- `@pytest.mark.unit` — Mock `requests.get` to raise `requests.ConnectionError`; assert
  `AcquireError` is raised.
- `@pytest.mark.unit` — Mock `requests.get` to return a non-ZIP binary payload; assert
  `AcquireError` is raised (bad zip file).
- `@pytest.mark.integration` — Given network access, call `download_year_zip(2020, config)`;
  assert the returned path exists in `data/raw/` and passes `is_valid_file`.

**Definition of done:** `download_year_zip` implemented, all test cases pass, ruff and
mypy report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 05: Implement current-year monthly CSV downloader

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `download_monthly_csv(config: dict) -> Path`

**Description:**
Downloads the current-year monthly production CSV directly (no ZIP). Steps:
1. URL: `config["acquire"]["monthly_url"]`.
2. Output path: `Path(config["acquire"]["raw_dir"]) / "monthly_prod.csv"`.
3. If `is_valid_file(output_path)` returns `True`, log `DEBUG` and return immediately.
4. Otherwise stream-download and write to `output_path`.
5. Sleep `config["acquire"]["sleep_seconds"]` after download.
6. Warn if result fails `is_valid_file`.
7. On `requests.RequestException`, catch, log `ERROR`, raise `AcquireError`.

**Returns:** `Path` to the downloaded CSV file.

**Dependencies:** `requests`, `pathlib`, `time`

**Test cases:**

- `@pytest.mark.unit` — Mock `requests.get` to return CSV text; assert the function
  returns a `Path` with filename `monthly_prod.csv`.
- `@pytest.mark.unit` — If `monthly_prod.csv` already exists and is valid, assert
  `requests.get` is never called.
- `@pytest.mark.unit` — Mock `requests.get` to raise `requests.Timeout`; assert
  `AcquireError` is raised.
- `@pytest.mark.integration` — With network access, call `download_monthly_csv(config)`;
  assert the file exists and passes `is_valid_file`.

**Definition of done:** `download_monthly_csv` implemented, all test cases pass, ruff and
mypy report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 06: Implement parallel acquire orchestrator

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `run_acquire(config: dict) -> list[Path]`

**Description:**
Orchestrates parallel downloads for all target years using the Dask threaded scheduler.
Steps:
1. Compute `target_years`: `range(config["acquire"]["start_year"], current_year)` for
   historical years (exclusive of current year).
2. Build a list of `dask.delayed` calls: one `delayed(download_year_zip)(year, config)`
   per historical year, plus one `delayed(download_monthly_csv)(config)` for the current
   year.
3. Call `dask.compute(*delayed_list, scheduler="threads",
   num_workers=config["acquire"]["max_workers"])`.
4. Return the flat list of `Path` objects from the compute result.
5. Log the total number of files downloaded and their paths at `INFO` level.

**Important:** Do NOT initialise a `distributed.Client` or `LocalCluster` anywhere in the
acquire stage. Use only `scheduler="threads"` passed to `dask.compute()`.

**Dependencies:** `dask`, `pathlib`, `datetime`

**Test cases:**

- `@pytest.mark.unit` — Mock `download_year_zip` and `download_monthly_csv`; assert
  `run_acquire` returns a list of `Path` objects of length = (current_year - start_year + 1).
- `@pytest.mark.unit` — Assert that `dask.compute` is called with `scheduler="threads"`
  (use `pytest-mock` to spy on `dask.compute`).
- `@pytest.mark.unit` — If one `download_year_zip` raises `AcquireError`, assert the
  error propagates out of `run_acquire`.
- `@pytest.mark.integration` — Run `run_acquire(config)` with network; assert one CSV
  per target year exists in `data/raw/` and all pass `is_valid_file` (TR-20, TR-21).

**Acquire idempotency test (TR-20):**

- `@pytest.mark.unit` — Mock all downloaders; call `run_acquire` twice; assert the
  mock downloader is called the same number of times in both runs (files already valid
  on the second run trigger the skip path and do not re-download).

**Acquired file integrity test (TR-21):**

- `@pytest.mark.integration` — After `run_acquire`, iterate every file in `data/raw/`;
  assert each file has `stat().st_size > 0`, is readable as UTF-8, and has at least one
  data row beyond the header.

**Definition of done:** `run_acquire` implemented, all test cases pass (including TR-20
and TR-21), ruff and mypy report no errors, `requirements.txt` updated with all
third-party packages imported in this task.

---

## Task 07: Implement logging setup helper

**Module:** `cogcc_pipeline/acquire.py` (also used by all other pipeline modules via import)
**Function:** `setup_logging(config: dict) -> logging.Logger`

**Description:**
Configures the root logger with two handlers:
1. A `StreamHandler` writing to stdout at the configured level.
2. A `FileHandler` writing to `config["logging"]["log_file"]`.

Creates the `logs/` directory if it does not exist. Returns the configured logger.
This function is called once at pipeline startup in `pipeline.py` and must be safe to
call multiple times without duplicating handlers (check `logger.handlers` before adding).

**Dependencies:** `logging`, `pathlib`

**Test cases:**

- `@pytest.mark.unit` — Call `setup_logging` twice with the same config; assert the
  root logger has exactly two handlers (not four).
- `@pytest.mark.unit` — Assert `logs/` directory is created when it does not exist.
- `@pytest.mark.unit` — Assert the returned logger level matches `config["logging"]["level"]`.

**Definition of done:** `setup_logging` implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in
this task.
