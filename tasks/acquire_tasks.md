# Acquire Stage Tasks

**Module:** `cogcc_pipeline/acquire.py`
**Test file:** `tests/test_acquire.py`

---

## Governing documents

All scheduler decisions: ADR-001.
All design decisions in this stage: stage-manifest-acquire.md.
Build environment and HTTP library constraints: build-env-manifest.md.
Test marking (unit vs integration): ADR-008.

---

## Scope constraints (apply to every task in this file)

- URL construction uses the `zip-file-path` and `monthly-file-path` templates from
  `task-writer-cogcc.md` directly — do not fetch or scrape an index page to discover URLs.
- No retry logic. No caching layer. No URL discovery. No HTML link scraping.
- bs4/BeautifulSoup is listed in the build environment for acquire HTML parsing tasks only.
  There are no HTML parsing tasks in this stage — the URLs are fully specified. Do not use
  bs4/BeautifulSoup anywhere in this stage.
- The acquire stage uses the Dask threaded scheduler (see ADR-001). Do not use the Dask
  distributed scheduler or initialize a distributed cluster in this stage.
- Scheduler choice is governed by ADR-001 — cite it, do not restate it.

---

## Task 01: Load acquire configuration

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `load_config(config_path: str) -> dict`

**Description:** Read `config.yaml` from `config_path` and return the `acquire` section as
a dict. The returned dict must contain all acquire-stage settings: the zip URL template,
the monthly CSV URL, the list of target years, the maximum number of concurrent workers,
the per-worker sleep duration, and the raw output directory path. No default values are
assumed — all required keys must be present in the config file.

**Error handling:** Raise `KeyError` with a descriptive message if any required acquire
key is missing from the loaded config.

**Dependencies:** pyyaml

**Not required:** environment variable fallback, config merging, schema validation beyond
key-presence checks.

**Test cases (all marked `@pytest.mark.unit`):**
- Given a valid config dict with all required acquire keys present, assert the function
  returns a dict containing all expected keys.
- Given a config file missing a required acquire key, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Build target file list

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `build_download_targets(config: dict) -> list[dict]`

**Description:** Given the acquire config dict (from Task 01), return a list of download
target descriptors — one per year in the configured year range. Each descriptor must
contain: the download URL, the destination file path under `data/raw/`, and the year.
For all years except the current calendar year, the URL is the zip-file-path template
from `task-writer-cogcc.md` with `{year}` substituted; the destination filename follows
the pattern `data/raw/{year}_prod_reports.csv`. For the current calendar year, the URL is
the monthly-file-path; the destination filename is `data/raw/monthly_prod.csv`. URL
construction uses the templates directly — no index page is fetched.

**Not required:** file existence checks, URL validation, network access.

**Dependencies:** datetime (stdlib), pathlib (stdlib)

**Test cases (all marked `@pytest.mark.unit`):**
- Given a config specifying years 2020–2022 where the current year is beyond 2022, assert
  the function returns exactly 3 descriptors, each with the zip URL template resolved for
  its year, and each destination path under `data/raw/`.
- Given a config where the current year is included in the target year list, assert that
  descriptor uses the monthly-file-path URL and the `monthly_prod.csv` destination.
- Assert that no descriptor URL contains an unresolved `{year}` placeholder.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Download a single file

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `download_file(target: dict, sleep_seconds: float) -> Path | None`

**Description:** Download the file described by `target` (a descriptor from Task 02) to
its configured destination path. If the destination file already exists on disk, skip the
download and return the existing path without making any network request — this satisfies
TR-20 (idempotency). For zip targets, extract the CSV from the downloaded zip in-memory
and write only the CSV to the destination path; do not persist the zip file to disk. For
monthly CSV targets, write the response body directly to the destination path. Sleep for
`sleep_seconds` after the download completes (or after the skip, to preserve consistent
pacing). Return the destination `Path` on success, or `None` if the download fails for
any reason. A failed download must not produce a file at the destination path — see
stage-manifest-acquire.md H2.

**Error handling:** On any exception during download or extraction, log a warning with
the year and URL, ensure no partial file remains at the destination path, and return
`None`.

**Dependencies:** requests, zipfile (stdlib), pathlib (stdlib), time (stdlib), logging
(stdlib)

**Not required:** retry logic, redirect following beyond requests defaults, checksum
verification, content-type validation.

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a target whose destination file already exists on disk, assert the function
  returns the existing path and makes no network request (mock `requests.get` to assert
  it is not called).
- Given a mocked successful zip download, assert the function writes a CSV file (not a
  zip) to the destination path and returns a `Path`.
- Given a mocked successful monthly CSV download, assert the function writes the CSV
  content to the destination path and returns a `Path`.
- Given a mocked network error (`requests.exceptions.RequestException`), assert the
  function returns `None` and leaves no file at the destination path.
- Given a mocked download that returns an empty response body (0 bytes), assert the
  function returns `None` and leaves no file at the destination path — satisfying
  TR-21(a).
- `@pytest.mark.integration`: After a real download to `tmp_path`, assert the output
  file size is greater than 0 bytes, the file is readable as UTF-8 text, and contains
  at least two lines (header + one data row) — satisfying TR-21(a)(b)(c).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Run parallel acquire stage

**Module:** `cogcc_pipeline/acquire.py`
**Function:** `acquire(config: dict) -> list[Path | None]`

**Description:** Orchestrate the full acquire stage. Build the list of download targets
(Task 02), then execute all downloads in parallel using the Dask threaded scheduler
(ADR-001) with a maximum of `config["acquire"]["max_workers"]` concurrent workers. Return
a list of results — one per target — where each element is the `Path` written or `None`
for a failed download. The output directory `data/raw/` must be created if it does not
exist before any download is attempted.

Parallelization mechanism: governed by ADR-001. The acquire stage is I/O-bound; see
ADR-001 for scheduler choice.

**Not required:** progress bar, retry orchestration, partial-result recovery beyond
returning `None` per failed target.

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a config with 3 targets where `download_file` is mocked to return a `Path` for
  each, assert `acquire` returns a list of 3 `Path` values.
- Given a config where one target's `download_file` mock returns `None`, assert `acquire`
  returns a list containing `None` at the corresponding position without raising.
- `@pytest.mark.integration` (TR-20): Run `acquire` twice on the same `tmp_path` output
  directory. Assert the file count after the second run equals the file count after the
  first run, no file content has changed, and no exception is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
