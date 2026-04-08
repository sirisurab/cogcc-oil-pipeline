# Ingest Stage — Task Specifications

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`

## Overview

The ingest stage reads all raw CSV files from `data/raw/`, validates their schema against the data dictionary, applies correct dtypes, filters to records with `ReportYear >= 2020`, and writes a consolidated interim Parquet dataset to `data/interim/cogcc_raw.parquet`. All configuration is read from `config.yaml`. This stage uses the Dask distributed scheduler; if a distributed `Client` is already running (e.g. started by the pipeline orchestrator) it is reused; otherwise a `LocalCluster` and `Client` are created from config.

## Data Dictionary — Column Names and dtypes

The following is the authoritative column list from `references/production-data-dictionary.csv`. All columns and their types must be used exactly as specified below. Column names are **case-sensitive** as they appear in the raw CSV.

| column | dtype | nullable | pandas type |
|--------|-------|----------|-------------|
| DocNum | int | no | `int64` |
| ReportMonth | int | no | `int64` |
| ReportYear | int | no | `int64` |
| DaysProduced | int | yes | `pd.Int64Dtype()` |
| AcceptedDate | datetime | yes | `datetime64[ns]` |
| Revised | bool | yes | `pd.BooleanDtype()` |
| OpName | string | yes | `pd.StringDtype()` |
| OpNumber | int | yes | `pd.Int64Dtype()` |
| FacilityId | int | yes | `pd.Int64Dtype()` |
| ApiCountyCode | string | no | `pd.StringDtype()` |
| ApiSequenceNumber | string | no | `pd.StringDtype()` |
| ApiSidetrack | string | no | `pd.StringDtype()` |
| Well | string | no | `pd.StringDtype()` |
| WellStatus | string | yes | `pd.StringDtype()` (cast to CategoricalDtype in transform) |
| FormationCode | string | yes | `pd.StringDtype()` |
| OilProduced | float | yes | `pd.Float64Dtype()` |
| OilSales | float | yes | `pd.Float64Dtype()` |
| OilAdjustment | float | yes | `pd.Float64Dtype()` |
| OilGravity | float | yes | `pd.Float64Dtype()` |
| GasProduced | float | yes | `pd.Float64Dtype()` |
| GasSales | float | yes | `pd.Float64Dtype()` |
| GasBtuSales | float | yes | `pd.Float64Dtype()` |
| GasUsedOnLease | float | yes | `pd.Float64Dtype()` |
| GasShrinkage | float | yes | `pd.Float64Dtype()` |
| GasPressureTubing | float | yes | `pd.Float64Dtype()` |
| GasPressureCasing | float | yes | `pd.Float64Dtype()` |
| WaterProduced | float | yes | `pd.Float64Dtype()` |
| WaterPressureTubing | float | yes | `pd.Float64Dtype()` |
| WaterPressureCasing | float | yes | `pd.Float64Dtype()` |
| FlaredVented | float | yes | `pd.Float64Dtype()` |
| BomInvent | float | yes | `pd.Float64Dtype()` |
| EomInvent | float | yes | `pd.Float64Dtype()` |

> **Note on WellStatus:** Read as `pd.StringDtype()` during ingest. The transform stage will cast it to `pd.CategoricalDtype` after cleaning. Never cast to categorical before cleaning.

## Design Decisions & Constraints

- Read raw CSVs using `dd.read_csv` with an explicit `dtype=` dict derived from the data dictionary above. Pass `parse_dates=["AcceptedDate"]`. Do not rely on pandas inference.
- Filter to `ReportYear >= 2020` using `map_partitions` with a `_filter` function — do not filter directly on a Dask Series.
- After reading, immediately repartition to `min(ddf.npartitions, 50)` before any transformations.
- Target output: `max(1, total_estimated_rows // 500_000)` partitions — use `max(1, ddf.npartitions // 10)` for estimation; never call `ddf.shape[0].compute()`.
- Write to `data/interim/cogcc_raw.parquet` using `ddf.repartition(npartitions=N).to_parquet(...)`.
- Never write more than 200 Parquet files. Target 20–50 output files.
- The `meta=` argument in `dd.from_delayed` or `map_partitions` must declare the exact same dtypes as the reader function output.
- Log ingestion statistics: rows per file (approximate from partition info), total rows written (from parquet metadata), missing columns detected.
- Reuse an existing distributed `Client` if present (check with `distributed.get_client()`, catch `ValueError` if none).
- Use `pathlib.Path` throughout. All functions must have type hints and docstrings.
- Logging to both stdout and `logs/pipeline.log`.

## `config.yaml` — ingest section

```
ingest:
  raw_dir: "data/raw"
  interim_dir: "data/interim"
  year_start: 2020
```

---

## Task I-01: dtype mapping and meta construction

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `build_dtype_map() -> dict[str, any]`
**Function:** `build_meta() -> pd.DataFrame`

**Description:**
`build_dtype_map()` returns a dict mapping each raw CSV column name (as it appears in the CSV header) to its pandas dtype, derived from the data dictionary table above. Date columns (`AcceptedDate`) are excluded from this dict and handled via `parse_dates=` instead. Integer columns with `nullable=yes` must use `pd.Int64Dtype()`. Float columns use `pd.Float64Dtype()`. String columns use `pd.StringDtype()`. The bool column (`Revised`) uses `pd.BooleanDtype()`.

`build_meta()` returns an empty pandas DataFrame with all columns and dtypes as defined by the data dictionary, to be used as the `meta=` argument when constructing Dask DataFrames.

**Dependencies:** `pandas`

**Test cases:**
- `@pytest.mark.unit` Assert `build_dtype_map()` contains exactly the columns in the data dictionary (no extra, no missing), excluding `AcceptedDate`.
- `@pytest.mark.unit` Assert `build_meta()` has all columns including `AcceptedDate`, with dtype `datetime64[ns]` for that column.
- `@pytest.mark.unit` Assert `build_meta()["DocNum"].dtype == "int64"` (non-nullable int).
- `@pytest.mark.unit` Assert `build_meta()["DaysProduced"].dtype == pd.Int64Dtype()` (nullable int).
- `@pytest.mark.unit` Assert `build_meta()["OilProduced"].dtype == pd.Float64Dtype()`.
- `@pytest.mark.unit` Assert `build_meta()["WellStatus"].dtype == pd.StringDtype()`.

**Definition of done:** Both functions implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task I-02: Single raw CSV reader

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `read_raw_file(file_path: Path) -> pd.DataFrame`

**Description:**
Read a single raw production CSV using `pd.read_csv` with the following:
- `dtype=build_dtype_map()` — apply all dtypes at read time, immediately after `read_csv`
- `parse_dates=["AcceptedDate"]`
- `low_memory=False`
- After reading, validate that all expected columns from the data dictionary are present. Log a warning for any missing column (do not raise — some columns may be absent in older files). Missing columns must be added as all-`pd.NA` columns with the correct dtype.
- Return a pandas DataFrame with all columns and dtypes exactly matching `build_meta()`.

**Parameters:**
- `file_path`: path to a raw CSV file

**Returns:** `pd.DataFrame` matching the schema defined by `build_meta()`

**Error handling:**
- File not found → raise `FileNotFoundError`
- CSV parse error → log an error and re-raise `ValueError`

**Dependencies:** `pandas`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` Create a synthetic CSV with all expected columns; assert `read_raw_file` returns a DataFrame whose dtypes match `build_meta()` exactly.
- `@pytest.mark.unit` Create a synthetic CSV missing one column (e.g. `FlaredVented`); assert the returned DataFrame contains the column filled with `pd.NA` and the correct dtype.
- `@pytest.mark.unit` Assert `AcceptedDate` column dtype is `datetime64[ns]` after reading.
- `@pytest.mark.unit` Assert `OilProduced` values that were strings `"N/A"` in the CSV are read as `pd.NA` (not as string).
- `@pytest.mark.integration` Read an actual file from `data/raw/` and assert all columns are present and dtypes match the data dictionary.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task I-03: Year filter

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `_filter_year(pdf: pd.DataFrame, year_start: int) -> pd.DataFrame`

**Description:**
Filter a pandas partition to only rows where `ReportYear >= year_start`. This function is called via `map_partitions`. Returns the filtered DataFrame unchanged (same dtypes, same columns).

**Parameters:**
- `pdf`: a pandas partition
- `year_start`: minimum year (inclusive), from `config["ingest"]["year_start"]`

**Returns:** filtered `pd.DataFrame`

**Constraints:** Must use vectorized boolean indexing, not a Python loop over rows.

**Test cases:**
- `@pytest.mark.unit` Given a DataFrame with rows for 2018, 2019, 2020, 2021, and `year_start=2020`, assert only 2020 and 2021 rows are returned.
- `@pytest.mark.unit` Given a DataFrame where all rows have `ReportYear < 2020`, assert an empty DataFrame is returned (with all columns intact).
- `@pytest.mark.unit` Given a DataFrame where all rows have `ReportYear >= 2020`, assert all rows are returned unchanged.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task I-04: Schema validation

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `validate_schema(ddf: dd.DataFrame, expected_columns: list[str]) -> list[str]`

**Description:**
Compare the columns present in `ddf` against `expected_columns`. Return a list of any missing column names. Log a warning for each missing column. This function must **not** call `.compute()`.

**Parameters:**
- `ddf`: a Dask DataFrame
- `expected_columns`: list of column names from the data dictionary

**Returns:** list of missing column names (empty list if all present)

**Test cases:**
- `@pytest.mark.unit` Given a Dask DataFrame with all expected columns, assert the function returns an empty list.
- `@pytest.mark.unit` Given a Dask DataFrame missing two columns, assert those two column names are returned.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task I-05: Dask distributed client management

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `get_or_create_client(config: dict) -> tuple[distributed.Client, bool]`

**Description:**
Check for an existing Dask distributed `Client` using `distributed.get_client()`. If found, reuse it. If a `ValueError` is raised (no running client), create a new `LocalCluster` and `Client` using `config["dask"]` settings: `n_workers`, `threads_per_worker`, `memory_limit`. Return a tuple of `(client, created)` where `created=True` if a new client was created (caller is responsible for closing it). Log the dashboard URL.

**Parameters:**
- `config`: full config dict

**Returns:** `tuple[distributed.Client, bool]`

**Test cases:**
- `@pytest.mark.unit` Mock `distributed.get_client()` to return a mock Client; assert the function returns the existing client and `created=False`.
- `@pytest.mark.unit` Mock `distributed.get_client()` to raise `ValueError`; assert a new Client is created and `created=True`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task I-06: Ingest stage runner

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `run_ingest(config: dict) -> None`

**Description:**
Top-level ingest stage entry point. Performs the following steps in order:

1. Resolve `data/raw/` from `config["ingest"]["raw_dir"]`. Enumerate all `.csv` files in the directory.
2. Call `get_or_create_client(config)` to get/create the Dask distributed Client.
3. For each CSV file, create a `dask.delayed(read_raw_file)(file_path)` call.
4. Assemble a Dask DataFrame using `dd.from_delayed(delayed_list, meta=build_meta())`.
5. Call `validate_schema(ddf, expected_columns)` and log any missing columns.
6. Apply year filter via `ddf.map_partitions(_filter_year, year_start=config["ingest"]["year_start"], meta=ddf._meta)`.
7. Repartition to `min(ddf.npartitions, 50)` immediately after assembly.
8. Compute target output partitions: `npartitions_out = max(1, ddf.npartitions // 10)`.
9. Create `data/interim/` directory if it does not exist.
10. Write `ddf.repartition(npartitions=npartitions_out).to_parquet(interim_dir / "cogcc_raw.parquet", write_index=False)`.
11. Log total output partitions written.
12. If a new Client was created (not reused), close it.

**Parameters:**
- `config`: full config dict

**Returns:** `None`

**Error handling:**
- No CSV files found in `data/raw/` → raise `FileNotFoundError` with a descriptive message.
- Any Dask computation error → log and re-raise as `RuntimeError`.

**Dependencies:** `dask`, `dask.distributed`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` Mock `read_raw_file` to return a synthetic DataFrame; assert `to_parquet` is called with the correct output path.
- `@pytest.mark.unit` If no CSV files are present in `raw_dir`, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` Assert that `_filter_year` is applied via `map_partitions` — verify the Dask graph includes the filter step by checking that a DataFrame with only pre-2020 rows produces an empty result.
- `@pytest.mark.integration` Run `run_ingest` against real files in `data/raw/`; assert `data/interim/cogcc_raw.parquet/` directory exists and contains `.parquet` files.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task I-07: Parquet readability and schema completeness tests (TR-18, TR-22)

**Test file:** `tests/test_ingest.py`

**Description:**
Implement the test cases required by TR-18 and TR-22 from `test-requirements.xml`.

**Test cases:**

- `@pytest.mark.integration` **TR-18** After running `run_ingest`, open each Parquet file in `data/interim/cogcc_raw.parquet/` using `pd.read_parquet`. Assert no exception is raised for any file.
- `@pytest.mark.integration` **TR-22** Read at least 3 Parquet partition files from `data/interim/cogcc_raw.parquet/`. For each, assert that all of the following columns are present: `ApiCountyCode`, `ApiSequenceNumber`, `ReportYear`, `ReportMonth`, `OilProduced`, `OilSales`, `GasProduced`, `GasSales`, `FlaredVented`, `GasUsedOnLease`, `WaterProduced`, `DaysProduced`, `Well`, `OpName`, `OpNumber`, `DocNum`. Assert no expected column is missing or renamed in any sampled partition.
- `@pytest.mark.unit` **TR-17** Assert that `run_ingest`'s internal Dask operations return `dask.dataframe.DataFrame` objects (not pandas) before the final `.to_parquet()` call. Verify by checking that intermediate results from the Dask graph are lazy (i.e., type is `dd.DataFrame`).

**Definition of done:** All test cases implemented, integration tests marked with `@pytest.mark.integration`, unit tests passing, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.
