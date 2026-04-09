# Ingest Component Tasks

**Pipeline package:** `cogcc_pipeline`
**Module file:** `cogcc_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`

## Overview

The ingest stage reads all raw CSV files from `data/raw/`, validates and enforces the
canonical schema defined in `references/production-data-dictionary.csv`, filters rows to
`ReportYear >= 2020`, logs file-level metadata (row counts, file sizes, SHA-256
checksums), and writes consolidated interim Parquet files to `data/interim/`. The stage
uses the Dask distributed scheduler; it checks for an existing distributed Client and
reuses it if present (as set up by `pipeline.py`), or initialises its own `LocalCluster`
and `Client` if called independently.

---

## Canonical Schema

The following column→dtype mapping is derived from
`references/production-data-dictionary.csv`. These dtypes must be passed to
`pd.read_csv(dtype=...)` at read time. Datetime columns are handled via `parse_dates=`.
`WellStatus` is read as `pd.StringDtype()` at ingest (cast to `CategoricalDtype` in the
transform stage).

| Column                | pandas dtype              | nullable |
|-----------------------|---------------------------|----------|
| DocNum                | `pd.Int64Dtype()`         | yes      |
| ReportMonth           | `int64`                   | no       |
| ReportYear            | `int64`                   | no       |
| DaysProduced          | `pd.Int64Dtype()`         | yes      |
| AcceptedDate          | `datetime64[ns]`          | yes      |
| Revised               | `pd.BooleanDtype()`       | yes      |
| OpName                | `pd.StringDtype()`        | yes      |
| OpNumber              | `pd.Int64Dtype()`         | yes      |
| FacilityId            | `pd.Int64Dtype()`         | yes      |
| ApiCountyCode         | `pd.StringDtype()`        | no       |
| ApiSequenceNumber     | `pd.StringDtype()`        | no       |
| ApiSidetrack          | `pd.StringDtype()`        | no       |
| Well                  | `pd.StringDtype()`        | no       |
| WellStatus            | `pd.StringDtype()`        | yes      |
| FormationCode         | `pd.StringDtype()`        | yes      |
| OilProduced           | `pd.Float64Dtype()`       | yes      |
| OilSales              | `pd.Float64Dtype()`       | yes      |
| OilAdjustment         | `pd.Float64Dtype()`       | yes      |
| OilGravity            | `pd.Float64Dtype()`       | yes      |
| GasProduced           | `pd.Float64Dtype()`       | yes      |
| GasSales              | `pd.Float64Dtype()`       | yes      |
| GasBtuSales           | `pd.Float64Dtype()`       | yes      |
| GasUsedOnLease        | `pd.Float64Dtype()`       | yes      |
| GasShrinkage          | `pd.Float64Dtype()`       | yes      |
| GasPressureTubing     | `pd.Float64Dtype()`       | yes      |
| GasPressureCasing     | `pd.Float64Dtype()`       | yes      |
| WaterProduced         | `pd.Float64Dtype()`       | yes      |
| WaterPressureTubing   | `pd.Float64Dtype()`       | yes      |
| WaterPressureCasing   | `pd.Float64Dtype()`       | yes      |
| FlaredVented          | `pd.Float64Dtype()`       | yes      |
| BomInvent             | `pd.Float64Dtype()`       | yes      |
| EomInvent             | `pd.Float64Dtype()`       | yes      |

**Non-negotiable dtype rule:** Never use `"object"` dtype for strings anywhere.
Always use `pd.StringDtype()`. Never rely on pandas inference — pass explicit `dtype=`
and `parse_dates=` arguments to `pd.read_csv`.

---

## Design Decisions and Constraints

- Raw files may contain records from years prior to 2020. Filter to `ReportYear >= 2020`
  immediately after reading, inside `read_raw_file`, before any downstream processing.
- One call to `read_raw_file` per source CSV file. Each call returns a
  `dask.dataframe.DataFrame` (via `dd.from_delayed`).
- The consolidated interim output must NOT be one file per source file — apply the
  repartition formula `max(10, min(ddf.npartitions, 50))` before writing Parquet.
- Parquet output directory: `data/interim/production.parquet/` (a Dask multi-file
  Parquet dataset, not a single file).
- Log file-level metadata: original row count, post-filter row count, file size in bytes,
  and SHA-256 checksum for each raw CSV, at `INFO` level.
- The ingest stage must check for an existing distributed Client
  (`distributed.get_client()`, catching `ValueError`); reuse it if present, otherwise
  create its own `LocalCluster` + `Client` from `config["dask"]` settings.

---

## Task 01: Implement raw file metadata logger

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `log_file_metadata(path: Path) -> dict`

**Description:**
Computes and logs file-level metadata for a raw CSV file:
- `file_path`: the absolute path as a string
- `file_size_bytes`: `path.stat().st_size`
- `sha256`: hex-digest SHA-256 checksum of file contents
- `row_count_raw`: number of lines in the file minus 1 (header)

Returns a `dict` with the four keys above. Logs all four values at `INFO` level.

**Dependencies:** `hashlib`, `pathlib`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Given a small temp CSV file with a known SHA-256 digest, assert
  the returned dict contains the correct `sha256` hex string.
- `@pytest.mark.unit` — Given a temp file with a header + 5 data rows, assert
  `row_count_raw == 5`.
- `@pytest.mark.unit` — Given a temp file, assert `file_size_bytes` equals
  `os.path.getsize(path)`.

**Definition of done:** `log_file_metadata` implemented, all test cases pass, ruff and
mypy report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 02: Implement raw CSV file reader with schema enforcement

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `read_raw_file(path: Path, config: dict) -> pd.DataFrame`

**Description:**
Reads a single raw CSV file into a pandas DataFrame with the following steps:
1. Build the `dtype` dict from the canonical schema table (all columns except
   `AcceptedDate`). Pass to `pd.read_csv(dtype=dtype_map, parse_dates=["AcceptedDate"],
   encoding="utf-8-sig")`. The `encoding="utf-8-sig"` handles BOM characters common in
   ECMC downloads.
2. Immediately after reading, filter rows to `ReportYear >= config["ingest"]["start_year"]`
   using a boolean mask (vectorised — no `apply` or loop).
3. Drop rows where both `ApiCountyCode` and `ApiSequenceNumber` are null (cannot
   identify a well without at least one API component).
4. Log a `WARNING` if any non-nullable column contains nulls after reading.
5. Return the filtered `pd.DataFrame`.

**Important dtype rules:**
- Pass `dtype=` explicitly. Never rely on pandas inference.
- `WellStatus` must be read as `pd.StringDtype()` (cast to `CategoricalDtype` in transform).
- `AcceptedDate` is handled via `parse_dates=["AcceptedDate"]` not the dtype map.

**Error handling:**
- Raise `IngestError` (see Task 03) if the file is missing required columns after read.
- Catch `UnicodeDecodeError`; retry with `encoding="latin-1"` once before raising
  `IngestError`.

**Dependencies:** `pandas`, `pathlib`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Given a temp CSV with all canonical columns, assert the returned
  DataFrame has correct dtypes for `DocNum` (`Int64`), `OilProduced` (`Float64`),
  `Revised` (`boolean`), `ApiCountyCode` (`StringDtype`), and `AcceptedDate`
  (`datetime64[ns]`).
- `@pytest.mark.unit` — Given a CSV containing rows with `ReportYear=2018` and
  `ReportYear=2021`, assert only the 2021 rows appear in the output (TR-04 prerequisite).
- `@pytest.mark.unit` — Given a CSV missing `ApiCountyCode` column entirely, assert
  `IngestError` is raised.
- `@pytest.mark.unit` — Given a CSV where both `ApiCountyCode` and `ApiSequenceNumber`
  are null for some rows, assert those rows are dropped.
- `@pytest.mark.unit` — Given a CSV with a BOM (`\ufeff`) at the start, assert the
  function reads it without error and column names are not prefixed with `\ufeff`.

**Definition of done:** `read_raw_file` implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 03: Implement IngestError and Dask delayed reader

**Module:** `cogcc_pipeline/ingest.py`
**Classes/Functions:**
- `class IngestError(RuntimeError)` — raised on unrecoverable ingest failures
- `make_delayed_df(path: Path, config: dict) -> dask.dataframe.DataFrame`

**Description:**
`IngestError` is a thin subclass of `RuntimeError` with no additional attributes.

`make_delayed_df` wraps `read_raw_file` in a `dask.delayed` call and constructs a
`dask.dataframe.DataFrame` via `dd.from_delayed`:
1. Build `meta` by calling `read_raw_file` on a small representative sample (first 5
   rows of the file, or an empty DataFrame constructed from the canonical schema). The
   meta must match the function output exactly in column names, column order, and dtypes.
   **Do not manually reorder columns** — derive meta by calling the reader on a minimal
   real input.
2. Call `dd.from_delayed([delayed(read_raw_file)(path, config)], meta=meta)`.
3. Return the resulting Dask DataFrame.

**Non-negotiable:** The `meta=` argument must match `read_raw_file`'s output exactly.
Build meta from the function on an empty or minimal real input, not a manually constructed
dict. Any column-order mismatch causes a Dask metadata mismatch error at compute time.

**Dependencies:** `dask`, `dask.dataframe`, `pathlib`

**Test cases:**

- `@pytest.mark.unit` — Given a valid temp CSV, assert `make_delayed_df` returns a
  `dask.dataframe.DataFrame` (not a pandas DataFrame), verifying TR-17.
- `@pytest.mark.unit` — Assert the returned Dask DataFrame's `dtypes` dict matches the
  canonical schema dtype map for all columns present in the test CSV.
- `@pytest.mark.unit` — Assert that calling `.compute()` on the result returns a pandas
  DataFrame with the expected row count.

**Definition of done:** `IngestError` and `make_delayed_df` implemented, all test cases
pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 04: Implement schema validator

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `validate_schema(ddf: dask.dataframe.DataFrame) -> None`

**Description:**
Validates that a Dask DataFrame matches the canonical schema. Checks:
1. All expected columns are present. Raises `IngestError` listing missing columns if not.
2. Each column's dtype matches the expected dtype from the canonical schema. Logs a
   `WARNING` (does not raise) for dtype mismatches — mismatches are logged and flagged but
   do not abort the pipeline, since Dask's `from_delayed` may slightly vary nullable
   integer representation.
3. Logs a summary at `INFO` level: column count, row count estimate (via
   `ddf.npartitions` — never call `ddf.shape[0].compute()`), and any warnings.

**Note:** Use `max(1, ddf.npartitions // 10)` for any partition-based estimation. Never
call `ddf.shape[0].compute()`.

**Dependencies:** `dask.dataframe`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Given a Dask DataFrame with all canonical columns and correct
  dtypes, assert `validate_schema` returns without raising.
- `@pytest.mark.unit` — Given a Dask DataFrame missing `OilProduced`, assert
  `IngestError` is raised and the error message mentions `OilProduced`.
- `@pytest.mark.unit` — Given a Dask DataFrame with `DocNum` as `int64` instead of
  `Int64`, assert a `WARNING` is logged (not an exception).

**Definition of done:** `validate_schema` implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 05: Implement interim Parquet writer

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `write_interim(ddf: dask.dataframe.DataFrame, config: dict) -> None`

**Description:**
Writes a Dask DataFrame to the interim Parquet dataset:
1. Compute target partition count: `max(10, min(ddf.npartitions, 50))`.
2. Repartition: `ddf = ddf.repartition(npartitions=target_n)`.
3. Write: `ddf.to_parquet(config["ingest"]["interim_dir"] + "/production.parquet",
   engine="pyarrow", write_index=False, overwrite=True)`.
4. Log the number of partitions written and the output path at `INFO` level.

**Important:** Never write one file per source entity. Never write more than 50 Parquet
files. The repartition step is mandatory before `to_parquet`.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Given a Dask DataFrame with 3 partitions, assert
  `write_interim` writes at least 10 Parquet files (repartition up to min 10).
- `@pytest.mark.unit` — Given a Dask DataFrame with 200 partitions, assert
  `write_interim` writes at most 50 Parquet files.
- `@pytest.mark.integration` — After `write_interim`, assert the output directory
  contains `.parquet` files; read them back with `pd.read_parquet` and assert no error
  (TR-18). Assert all expected columns are present (TR-22).

**Definition of done:** `write_interim` implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 06: Implement ingest stage orchestrator

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `run_ingest(config: dict) -> dask.dataframe.DataFrame`

**Description:**
Orchestrates the full ingest stage:
1. Detect or initialise Dask distributed Client:
   - Call `distributed.get_client()`; if `ValueError` is raised (no running client),
     create a `LocalCluster` and `Client` using `config["dask"]` settings
     (`n_workers`, `threads_per_worker`, `memory_limit`). Log the dashboard URL.
2. Discover raw CSV files: glob `config["ingest"]["raw_dir"]` for `*.csv` files.
   Raise `IngestError` if no files are found.
3. For each discovered file, call `log_file_metadata(path)` and
   `make_delayed_df(path, config)`.
4. Concatenate all Dask DataFrames: `ddf = dd.concat(ddf_list)`.
5. Call `validate_schema(ddf)`.
6. Call `write_interim(ddf, config)`.
7. Return the Dask DataFrame (before `.compute()`).

**Non-negotiable:** Do NOT call `.compute()` inside `run_ingest`. Return the Dask
DataFrame. Verify via TR-17.

**Dependencies:** `dask`, `dask.dataframe`, `distributed`, `pathlib`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Mock `log_file_metadata`, `make_delayed_df`, `validate_schema`,
  `write_interim`; assert `run_ingest` returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given no CSV files in raw dir, assert `IngestError` is raised.
- `@pytest.mark.unit` — Assert `run_ingest` does not call `.compute()` on any
  intermediate result (mock Dask DataFrame and assert `.compute()` is never invoked).
- `@pytest.mark.integration` — Run `run_ingest(config)` against `data/raw/`; assert
  `data/interim/production.parquet/` exists and is readable (TR-18). Sample 3 partitions
  and assert all expected columns are present (TR-22).

**Schema completeness test (TR-22):**

- `@pytest.mark.integration` — After `run_ingest`, read at least 3 partitions from
  `data/interim/production.parquet/` using `pd.read_parquet`. Assert that each partition
  contains all of the following columns: `ApiCountyCode`, `ApiSequenceNumber`,
  `ReportYear`, `ReportMonth`, `OilProduced`, `OilSales`, `GasProduced`, `GasSales`,
  `FlaredVented`, `GasUsedOnLease`, `WaterProduced`, `DaysProduced`, `Well`, `OpName`,
  `OpNumber`, `DocNum`. Assert no expected column is missing or renamed in any sampled
  partition.

**Parquet readability test (TR-18):**

- `@pytest.mark.integration` — After `run_ingest`, assert every `.parquet` file in
  `data/interim/production.parquet/` is readable by `pd.read_parquet` without raising an
  exception.

**Definition of done:** `run_ingest` implemented, all test cases pass (including TR-17,
TR-18, TR-22), ruff and mypy report no errors, `requirements.txt` updated with all
third-party packages imported in this task.
