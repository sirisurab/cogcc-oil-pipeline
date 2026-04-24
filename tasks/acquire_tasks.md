# Acquire Stage — Task Specifications

Module: `cogcc_pipeline/acquire.py`
Tests: `tests/test_acquire.py`

Authoritative references (read before implementing; cite — do not restate):
- `/agent_docs/ADRs.md` — ADR-001 (scheduler choice), ADR-006 (logging), ADR-007
  (technology stack), ADR-008 (test strategy)
- `/agent_docs/stage-manifest-acquire.md` — purpose, input/output state, hazards H1, H2
- `/agent_docs/build-env-manifest.md` — configuration structure, HTTP library
  constraint, Dask scheduler initialization
- `/task-writer-cogcc.md` — dataset layout (`{year}_prod_reports.zip` per year,
  `monthly_prod.csv` for current year), download rules, rate-limiting constraint
  (max 5 concurrent workers, 0.5 s sleep per worker)
- `/test-requirements.xml` — TR-20, TR-21

All configurable values (URLs, years range, output directory, worker count, sleep
between downloads, retry count) are read from the `acquire` section of `config.yaml`
per build-env-manifest "Configuration structure". No configurable values may be
hardcoded.

---

## Task A1 — Build the list of download targets

Module: `cogcc_pipeline/acquire.py`
Function: `build_download_targets(config: dict) -> list[dict]`

Description: Produce the list of download targets the stage must fetch. For each
year in the configured range (start year through current year), emit one target.
Per `/task-writer-cogcc.md` dataset instructions, the current year uses the
monthly CSV URL; each prior year uses the annual ZIP URL. The returned list is
the single source of truth consumed by A2 and A3 — no other function re-derives
it.

Each target is a dict carrying at minimum: year, source URL, target file path
under `data/raw/`, and download kind (`zip` or `csv`) so the downloader knows
whether post-download extraction is required.

Design decisions the coder must make and the governing doc for each:
- Year range boundaries — from `acquire` section of `config.yaml` (build-env-manifest
  "Configuration structure").
- Current-year vs. prior-year URL selection — governed by
  `/task-writer-cogcc.md` dataset instructions.
- Target filename convention — governed by `/task-writer-cogcc.md` context section
  (`data/raw/{year}_prod_reports.csv`, `data/raw/monthly_prod.csv`).

Error handling: raise a clearly named exception if the configured year range is
empty, inverted, or missing.

Test cases (unit, do not hit the network):
- Given a config with `start_year=2020` and a fixed `current_year=2024`, assert
  the returned list has one entry per year 2020..2024, all prior years are `zip`
  kind, 2024 is `csv` kind.
- Given a config with `start_year > current_year`, assert the function raises.
- Assert target file paths resolve under the configured raw directory.

Definition of done: Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A2 — Download a single target with retries and validation

Module: `cogcc_pipeline/acquire.py`
Function: `download_target(target: dict, config: dict) -> dict`

Description: Download exactly one target produced by A1, write the downloaded
bytes to the target path under `data/raw/`, and return a per-target result record
containing at minimum: target year, source URL, final CSV path on disk, size in
bytes, download timestamp, and status (`downloaded`, `skipped`, `failed`). The
function is the unit of work submitted to the parallel executor in A3 and must be
safe to call concurrently with itself on different targets.

Behavior requirements:
- Idempotent: if the target CSV already exists on disk and is valid (non-zero
  bytes, UTF-8 decodable, more than one line), return a `skipped` record without
  re-downloading. Governs TR-20.
- Integrity: a file is only considered successfully downloaded if it is non-empty,
  UTF-8 decodable, and contains at least one data row beyond the header. Governs
  TR-21 and stage-manifest-acquire H2.
- Retries: transient HTTP/network errors retry up to the configured retry count
  with backoff. Exhausted retries produce a `failed` status — not an exception
  that aborts the whole stage.
- ZIP targets: after a successful download of a ZIP, extract the inner production
  CSV into `data/raw/` under the configured filename and remove the ZIP
  artifact. The returned `final_path` points to the CSV, not the ZIP.
- Per-worker rate limit: honour the sleep interval specified in the project
  task-writer file (`/task-writer-cogcc.md` dataset instructions).
- Library choice: HTTP request library and HTML parsing library only — browser
  automation is prohibited per ADR-007 and build-env-manifest "HTTP library for
  acquire".

Error handling: a failed download must not leave a partially written file that
would pass an existence check at ingest time (stage-manifest-acquire H2). Any
partially written artifact is removed before the function returns `failed`.

Test cases (unit, no real network — mock the HTTP client):
- Given a target whose mocked response returns 200 and a valid multi-line body,
  assert the file is written and the returned record status is `downloaded`.
- Given a target whose file already exists on disk with valid content, assert
  no HTTP call is made and the returned record status is `skipped` (TR-20).
- Given a target whose mocked response returns an empty body, assert no file is
  left on disk and the returned record status is `failed` (TR-21, H2).
- Given a target that fails with a connection error on every attempt up to the
  retry limit, assert the function returns `failed` and does not raise.
- Given a ZIP target whose mocked response returns a valid archive containing a
  CSV, assert the CSV is extracted to the configured path and the ZIP artifact
  is removed.

Definition of done: Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task A3 — Parallel acquire orchestration

Module: `cogcc_pipeline/acquire.py`
Function: `acquire(config: dict) -> list[dict]`

Description: Entry point for the acquire stage. Produces the target list via A1,
downloads all targets in parallel, and returns the list of per-target result
records produced by A2.

Design decisions the coder must make and the governing doc for each:
- Parallelization mechanism — ADR-001 specifies the threaded scheduler for the
  I/O-bound acquire stage. Concurrency is bounded by the worker count from the
  `acquire` section of `config.yaml`, capped at the limit in
  `/task-writer-cogcc.md` dataset instructions.
- Output directory creation — `data/raw/` is created if absent before any
  download runs.
- Failure handling — a failed target does not abort the stage; `failed` records
  are returned alongside `downloaded` and `skipped` records so downstream stages
  can reason about what is present.
- Logging — per ADR-006, this stage uses the logger configured at pipeline
  startup; it does not configure its own handlers.

Return contract: the returned list contains one record per target in the input
list. Every return path satisfies the same record shape and keys — regardless of
whether a target was downloaded, skipped, or failed.

Test cases (unit):
- Given a mocked A2 that returns `downloaded` for all targets, assert the
  aggregate result length equals the target count and every record has status
  `downloaded`.
- Given a mocked A2 that returns `skipped` for a target that already exists and
  `downloaded` for others, assert the result reflects both statuses without
  duplicating or re-ordering targets.
- Given a mocked A2 that returns `failed` for one target, assert the stage still
  returns and the result records for the remaining targets are present.
- Running `acquire` twice against a raw directory pre-populated with valid files
  produces the same file count after both runs and leaves existing file content
  unchanged (TR-20 a, b, c).
- Every file in the raw directory after acquire has size > 0 bytes, is UTF-8
  readable, and contains at least one row beyond the header (TR-21 a, b, c).

Definition of done: Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
