# Ingest Stage — Task Specifications

## Overview

The ingest stage reads all raw CSV files produced by the acquire stage from `data/raw/`, applies
minimal structural normalisation (correct dtypes, filter to target year range), consolidates the
data into a small number of Parquet files in `data/interim/`, and makes the data available as a
Dask DataFrame for downstream stages.

The ingest stage must NOT perform substantive data cleaning (that is the transform stage's
responsibility). It must only: read, validate column presence, set dtypes, filter by year, and
write consolidated Parquet files.

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`

---

## Raw Schema (Authoritative Column Names)

The following column names come from the ECMC production data dictionary and the raw CSV header.
These are the exact names as they appear in the raw files and must be used verbatim throughout
this stage. Do not rename, alias, or snake_case these column names during ingest — renaming is
handled by the transform stage.

| Column Name        | Description                                      |
|--------------------|--------------------------------------------------|
| ApiCountyCode      | API county code (2-digit numeric string)         |
| ApiSequenceNumber  | API sequence number (5-digit numeric string)     |
| ReportYear         | 4-digit year of the production report            |
| ReportMonth        | 2-digit month of the production report           |
| OilProduced        | Oil produced (BBL)                               |
| OilSales           | Oil sold (BBL)                                   |
| GasProduced        | Gas produced (MCF)                               |
| GasSales           | Gas sold (MCF)                                   |
| FlaredVented       | Gas flared or vented (MCF)                       |
| GasUsedOnLease     | Gas used on lease (MCF)                          |
| WaterProduced      | Water produced (BBL)                             |
| DaysProduced       | Number of days well produced in the month        |
| Well               | Well name / identifier string                    |
| OpName             | Operator name                                    |
| OpNum              | Operator number                                  |
| DocNum             | Document number (unique report identifier)       |

Additional columns may be present in some files. They must be retained as-is and not dropped
during ingest (downstream stages will decide what to use).

---

## Design Decisions & Constraints

- Use `dask.dataframe` for all reading and writing operations.
- Read CSVs with Dask using `dask.dataframe.read_csv` with `blocksize="64MB"` and
  `assume_missing=True` (source files may have inconsistent nullability).
- After reading, immediately repartition to `min(current_npartitions, 50)` before any operations.
- Filter rows to `ReportYear >= cfg["target_start_year"]` (default 2020) using `map_partitions`
  (not direct `.str` or series accessors — see non-negotiable Dask constraints).
- Target output: `npartitions = max(1, total_rows // 500_000)` consolidated Parquet files under
  `data/interim/`. Never write one file per source entity. Always repartition before writing.
- Write with `ddf.repartition(npartitions=N).to_parquet("data/interim/", write_index=False)`.
- Use `pd.StringDtype()` for all string-typed columns in any `meta=` argument passed to
  `map_partitions`. Never use `"object"` as a dtype value in a `meta=` argument.
- All functions that return Dask DataFrames must NOT call `.compute()` internally (TR-17). They
  must return `dask.dataframe.DataFrame`.
- Logging must use Python's `logging` module. No `print()` statements.
- Parallel CSV reading: use Dask's built-in parallelism — do not manually spawn additional processes.
- Error handling: if a single CSV file cannot be read (corrupt, empty, wrong schema), log an error
  and continue with remaining files. Do not abort the full ingest.
- All configurable values must be read from `config.yaml` (passed in as a dict `cfg`).

---

## Task 01: Implement single-file CSV reader

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `read_csv_file(csv_path: Path, cfg: dict) -> dask.dataframe.DataFrame`

**Description:**
Read a single raw production CSV file into a Dask DataFrame, applying only minimal structural
operations: correct blocksize, `assume_missing=True`, and a repartition to at most 50 partitions.

- Use `dask.dataframe.read_csv(str(csv_path), blocksize="64MB", assume_missing=True)`.
- After reading, repartition to `min(ddf.npartitions, 50)`.
- Log the file path, number of partitions before and after, at INFO level.
- If the file does not exist at `csv_path`, raise `FileNotFoundError` with a descriptive message.
- If the file is empty (0 bytes), raise `ValueError(f"Empty file: {csv_path}")`.
- Return the Dask DataFrame without calling `.compute()`.

**Error handling:** `FileNotFoundError` for missing file; `ValueError` for empty file;
`dask.dataframe.errors` wrapped and logged for CSV parse errors.

**Dependencies:** dask[dataframe], pathlib, logging

**Test cases:**
- `@pytest.mark.unit` — Given a small synthetic CSV written to a tmp directory with the correct
  column names, assert the function returns a `dask.dataframe.DataFrame` (not `pandas.DataFrame`).
- `@pytest.mark.unit` — Assert the returned Dask DataFrame has `npartitions <= 50`.
- `@pytest.mark.unit` — Given a path to a non-existent file, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given a path to an empty (0-byte) file, assert `ValueError` is raised.
- `@pytest.mark.unit` — Assert the function does not call `.compute()` internally — verify the
  return type is `dask.dataframe.DataFrame` (TR-17).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement year-range filter

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `filter_by_year(ddf: dask.dataframe.DataFrame, start_year: int) -> dask.dataframe.DataFrame`

**Description:**
Filter a Dask DataFrame to rows where `ReportYear >= start_year`. This handles the fact that
ECMC raw files may contain historical production data going back decades, regardless of which
year's zip was downloaded.

- Use `map_partitions` with an inner Pandas function (not a direct Dask `.str` accessor or series
  comparison that may fail after repartition/astype operations):
  ```
  def _filter(pdf):
      pdf = pdf[pdf["ReportYear"].astype(int) >= start_year]
      return pdf
  ddf = ddf.map_partitions(_filter, meta=ddf._meta)
  ```
- The `meta=` argument must be `ddf._meta` (which is an empty Pandas DataFrame with the correct
  schema).
- Log at INFO level: how many partitions are being filtered and the target start year.
- Do not call `.compute()`. Return the filtered Dask DataFrame.
- The `ReportYear` column may be read as a string or integer from CSV — handle both: cast to int
  inside the inner `_filter` function before comparison.

**Error handling:** If `ReportYear` column is missing from the DataFrame, raise
`KeyError("ReportYear column not found in DataFrame")`.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a synthetic Dask DataFrame with `ReportYear` values [2018, 2019,
  2020, 2021], assert that after `filter_by_year(ddf, 2020)`, calling `.compute()` yields only
  rows with `ReportYear` in [2020, 2021].
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame where `ReportYear` is stored as a string column
  ("2020", "2019"), assert the filter still correctly excludes pre-2020 rows.
- `@pytest.mark.unit` — Given a DataFrame missing the `ReportYear` column, assert `KeyError`
  is raised.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement schema validator

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `validate_schema(ddf: dask.dataframe.DataFrame) -> list[str]`

**Description:**
Check that all required columns are present in the Dask DataFrame. Returns a list of missing
column names (empty list if all are present). This is a lightweight structural check — it does not
inspect data values.

Required columns (must all be present):
`ApiCountyCode`, `ApiSequenceNumber`, `ReportYear`, `ReportMonth`, `OilProduced`, `OilSales`,
`GasProduced`, `GasSales`, `FlaredVented`, `GasUsedOnLease`, `WaterProduced`, `DaysProduced`,
`Well`, `OpName`, `OpNum`, `DocNum`

- Check `ddf.columns` (no `.compute()` needed — column names are available from the metadata).
- Return the list of column names that are absent.
- Log a WARNING for each missing column.
- Do not raise — return the list so the caller can decide how to respond.

**Error handling:** No exceptions raised. Missing columns are returned in the list.

**Dependencies:** dask[dataframe], logging

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with all 16 required columns present, assert
  `validate_schema` returns an empty list.
- `@pytest.mark.unit` — Given a Dask DataFrame missing `OilProduced` and `GasProduced`, assert
  the returned list contains exactly those two names.
- `@pytest.mark.unit` — Assert the function does not call `.compute()` (return type inspection
  only).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement multi-file ingestion loader

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `load_all_raw_files(raw_dir: Path, cfg: dict) -> dask.dataframe.DataFrame`

**Description:**
Load all CSV files found in `raw_dir`, concatenate them into a single Dask DataFrame, apply the
year filter, and validate schema. Returns the combined Dask DataFrame without writing to disk.

- Find all CSV files in `raw_dir` matching `*.csv` using `glob` (or `Path.glob`).
- For each file: call `read_csv_file`. If it raises, log the error and continue to the next file
  (graceful per-file failure).
- If no files are successfully loaded, raise `RuntimeError("No raw CSV files could be loaded")`.
- Concatenate all successfully loaded Dask DataFrames using `dask.dataframe.concat(ddfs, axis=0)`.
- Repartition to `min(combined.npartitions, 50)` after concatenation.
- Call `filter_by_year(combined_ddf, cfg["target_start_year"])`.
- Call `validate_schema(combined_ddf)`. If any required column is missing, log a WARNING listing
  the missing columns (do not abort).
- Return the final Dask DataFrame without calling `.compute()`.

**Error handling:** Per-file errors are caught, logged, and skipped. `RuntimeError` if no files
load successfully.

**Dependencies:** dask[dataframe], pathlib, logging

**Test cases:**
- `@pytest.mark.unit` — Given a tmp directory with two synthetic CSV files, assert the function
  returns a `dask.dataframe.DataFrame` with rows from both files.
- `@pytest.mark.unit` — Given a tmp directory where one CSV is empty and one is valid, assert
  the function returns a Dask DataFrame from the valid file and logs an error for the empty one.
- `@pytest.mark.unit` — Given an empty directory, assert `RuntimeError` is raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame` (TR-17).
- `@pytest.mark.integration` — Given the real `data/raw/` directory after an acquire run, assert
  the function returns a non-empty Dask DataFrame with all required columns present (TR-22).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement dtype standardiser

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `standardise_dtypes(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Apply correct dtype assignments to the Dask DataFrame columns before writing to Parquet. This
ensures Parquet files have a stable, queryable schema.

Apply the following dtype mappings:
- `ApiCountyCode`: `pd.StringDtype()`
- `ApiSequenceNumber`: `pd.StringDtype()`
- `ReportYear`: `int32`
- `ReportMonth`: `int32`
- `OilProduced`: `float64`
- `OilSales`: `float64`
- `GasProduced`: `float64`
- `GasSales`: `float64`
- `FlaredVented`: `float64`
- `GasUsedOnLease`: `float64`
- `WaterProduced`: `float64`
- `DaysProduced`: `float64`
- `Well`: `pd.StringDtype()`
- `OpName`: `pd.StringDtype()`
- `OpNum`: `pd.StringDtype()`
- `DocNum`: `pd.StringDtype()`

- Apply dtype assignments using `map_partitions` with an explicit `meta=` argument constructed
  as an empty `pd.DataFrame` with the above types. Do not use `ddf.astype({...})` directly on
  string columns (use `map_partitions` for string columns to avoid the `object` dtype issue).
- For any column not in the list above that is present in the DataFrame, leave it unchanged.
- Do not call `.compute()`. Return the typed Dask DataFrame.

**Error handling:** If a column cannot be cast to its target dtype (e.g., non-numeric string in a
float column), log a WARNING and leave the column as-is rather than raising.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a synthetic Dask DataFrame with all required columns as object/str
  dtype, assert that after `standardise_dtypes`, the `OilProduced` column dtype is `float64`.
- `@pytest.mark.unit` — Assert that `ApiCountyCode` dtype is `pd.StringDtype()` (not `object`).
- `@pytest.mark.unit` — Assert `ReportYear` dtype is `int32`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert `meta=` arguments to `map_partitions` use `pd.StringDtype()` for
  string columns and never use `"object"` as a dtype value.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Implement interim Parquet writer

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `write_interim_parquet(ddf: dask.dataframe.DataFrame, interim_dir: Path, total_rows_estimate: int) -> None`

**Description:**
Write the consolidated Dask DataFrame to `data/interim/` as a set of Parquet files. The number
of output files must be proportional to data volume — never write one file per source entity,
and never write more than 200 files total.

- Compute `npartitions = max(1, total_rows_estimate // 500_000)`.
- Cap at 50 partitions: `npartitions = min(npartitions, 50)`.
- Repartition: `ddf = ddf.repartition(npartitions=npartitions)`.
- Write: `ddf.to_parquet(str(interim_dir), write_index=False, overwrite=True)`.
- Create `interim_dir` if it does not exist.
- Log the target `npartitions` and `interim_dir` at INFO level before writing.
- After writing, log a confirmation message at INFO level.
- Do NOT use `partition_on=` with high-cardinality columns.

**Error handling:** Propagate `OSError` or `pyarrow` write errors after logging.

**Dependencies:** dask[dataframe], pyarrow, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` — Given a synthetic 5-partition Dask DataFrame and `total_rows_estimate=50_000`,
  assert `npartitions` is set to 1 (50_000 // 500_000 = 0, max(1, 0) = 1).
- `@pytest.mark.unit` — Given `total_rows_estimate=2_500_000`, assert `npartitions = 5`.
- `@pytest.mark.unit` — Given `total_rows_estimate=30_000_000`, assert `npartitions = 50`
  (capped at 50).
- `@pytest.mark.integration` — Write a synthetic Dask DataFrame to a tmp directory, then read
  back with `dask.dataframe.read_parquet` and assert the row count matches (TR-18).
- `@pytest.mark.integration` — Assert the number of Parquet files written is <= 50.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Implement ingest orchestrator

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `run_ingest(config_path: str = "config.yaml") -> dask.dataframe.DataFrame`

**Description:**
Top-level entry point for the ingest stage. Loads all raw CSV files, standardises dtypes, writes
interim Parquet files, and returns the Dask DataFrame for optional chaining.

- Load config via `load_config(config_path)`.
- Resolve `raw_dir = Path(cfg["ingest"]["raw_dir"])` and `interim_dir = Path(cfg["ingest"]["interim_dir"])`.
- Call `load_all_raw_files(raw_dir, cfg["ingest"])`.
- Call `standardise_dtypes(ddf)`.
- Estimate total rows: call `.shape[0].compute()` once (this single `.compute()` call is permitted
  in the orchestrator to size the output partitions). Store as `total_rows_estimate`.
- Call `write_interim_parquet(ddf, interim_dir, total_rows_estimate)`.
- Return the Dask DataFrame (post-repartition, pre-compute) for optional downstream use.
- Log a start and end message with elapsed time.

**Error handling:** Propagate `RuntimeError` from `load_all_raw_files` if no files load. All other
errors are logged and re-raised.

**Dependencies:** dask[dataframe], pathlib, logging, datetime

**Test cases:**
- `@pytest.mark.unit` — Given mocked `load_all_raw_files` returning a synthetic Dask DataFrame
  and mocked `write_interim_parquet`, assert `run_ingest` completes without error and returns a
  `dask.dataframe.DataFrame`.
- `@pytest.mark.integration` — Run `run_ingest` against real `data/raw/` (post-acquire); assert
  Parquet files exist in `data/interim/`, are readable, and have all required columns (TR-22).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Implement schema completeness test (TR-22)

**Module:** `tests/test_ingest.py`

**Description:**
Dedicated test suite verifying schema completeness across interim Parquet partitions per TR-22.

**Test cases:**
- `@pytest.mark.integration` — After running `run_ingest`, read at least 3 interim Parquet
  partition files from `data/interim/`. For each, assert that all of the following columns are
  present: `ApiCountyCode`, `ApiSequenceNumber`, `ReportYear`, `ReportMonth`, `OilProduced`,
  `OilSales`, `GasProduced`, `GasSales`, `FlaredVented`, `GasUsedOnLease`, `WaterProduced`,
  `DaysProduced`, `Well`, `OpName`, `OpNum`, `DocNum`. Assert no column is missing or renamed
  in any sampled partition.
- `@pytest.mark.integration` — Assert that all sampled partitions have the same set of column
  names (schema is identical across partitions).
- `@pytest.mark.integration` — Read the interim Parquet dataset with `pandas.read_parquet` and
  assert it does not raise (TR-18 — parquet readability).

**Definition of done:** Tests implemented and passing, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Module-level entry point

**Module:** `cogcc_pipeline/ingest.py`

Add a `if __name__ == "__main__":` block that calls `run_ingest()` so the stage can be executed
via `python -m cogcc_pipeline.ingest`.
