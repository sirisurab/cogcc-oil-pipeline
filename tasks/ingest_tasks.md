# Ingest Stage — Task Specifications

## Stage Overview

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`
**Input directory:** `data/raw/`
**Output directory:** `data/interim/` (Parquet files)
**Scheduler:** Dask distributed scheduler (CPU-bound stage — ADR-001)

The ingest stage reads raw COGCC CSV files from `data/raw/`, enforces the canonical
schema defined in `references/production-data-dictionary.csv`, filters rows to
`ReportYear >= 2020`, and writes consolidated partitioned Parquet files to
`data/interim/`.

### Canonical column schema
The following columns and their types are authoritative per
`references/production-data-dictionary.csv`. All dtype mappings follow ADR-003:
nullable integer columns use `Int64` (pandas nullable integer); non-nullable integers
use `int64`; floats use `float64`; strings use `pd.StringDtype()` (`"string"`);
datetime columns use `datetime64[ns]`; bools use `pd.BooleanDtype()` (`"boolean"`);
categoricals use `pd.CategoricalDtype(categories=[...], ordered=False)`.

| Column | dtype | nullable |
|---|---|---|
| DocNum | Int64 | yes |
| ReportMonth | int64 | no |
| ReportYear | int64 | no |
| DaysProduced | Int64 | yes |
| AcceptedDate | datetime64[ns] | yes |
| Revised | boolean | yes |
| OpName | string | yes |
| OpNumber | Int64 | yes |
| FacilityId | Int64 | yes |
| ApiCountyCode | string | no |
| ApiSequenceNumber | string | no |
| ApiSidetrack | string | no |
| Well | string | no |
| WellStatus | CategoricalDtype(["AB","AC","DG","PA","PR","SI","SO","TA","WO"]) | yes |
| FormationCode | string | yes |
| OilProduced | float64 | yes |
| OilSales | float64 | yes |
| OilAdjustment | float64 | yes |
| OilGravity | float64 | yes |
| GasProduced | float64 | yes |
| GasSales | float64 | yes |
| GasBtuSales | float64 | yes |
| GasUsedOnLease | float64 | yes |
| GasShrinkage | float64 | yes |
| GasPressureTubing | float64 | yes |
| GasPressureCasing | float64 | yes |
| WaterProduced | float64 | yes |
| WaterPressureTubing | float64 | yes |
| WaterPressureCasing | float64 | yes |
| FlaredVented | float64 | yes |
| BomInvent | float64 | yes |
| EomInvent | float64 | yes |

### Boundary contract delivered by ingest (→ transform)
- All columns carry data-dictionary dtypes (no inference).
- Column names match the data dictionary exactly.
- Nullable absent columns filled with NA (not raised).
- Non-nullable absent columns raise immediately.
- Partitioned to `max(10, min(n, 50))` where `n` = number of raw CSV files processed.
- Sort: unsorted — transform must not assume any sort order.

### Data filtering constraint
After reading each raw file, filter to rows where `ReportYear >= 2020` before any
further processing. Raw ECMC files may contain historical rows from earlier years.

---

## Task ING-01: Data dictionary loader

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `load_data_dictionary(dict_path: str) -> dict[str, dict]`

**Description:**
Read `references/production-data-dictionary.csv` and return a lookup dict keyed by
column name. Each value is a dict with keys: `dtype` (string token from the CSV),
`nullable` (bool), `categories` (list of strings or empty list).

The data dictionary CSV columns are: `column, description, dtype, nullable, categories`.
- `nullable` is `"yes"` in the CSV → map to Python `True`; `"no"` → `False`.
- `categories` is a pipe-separated string (e.g. `"AB|AC|DG"`) or empty → map to list
  of strings or empty list.

This function is called once at stage startup and the result is passed to all
schema-enforcement functions. It must not be called per-partition.

**Error handling:**
- If `dict_path` does not exist → raise `FileNotFoundError`.
- If the CSV is missing any of the required header columns → raise `ValueError`.

**Dependencies:** `pandas`, `pathlib`

**Test cases:**
- Given the real `references/production-data-dictionary.csv`, assert the returned dict
  has 33 keys (one per column in the data dictionary).
- Assert that `result["OilProduced"]["dtype"] == "float"` and
  `result["OilProduced"]["nullable"] == True`.
- Assert that `result["ReportYear"]["nullable"] == False`.
- Assert that `result["WellStatus"]["categories"] == ["AB","AC","DG","PA","PR","SI","SO","TA","WO"]`.
- Assert that `result["OilProduced"]["categories"] == []`.
- Given a path to a non-existent file, assert `FileNotFoundError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ING-02: dtype mapper

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `map_dtype(dtype_token: str, nullable: bool, categories: list[str]) -> object`

**Description:**
Map a data-dictionary dtype token string to the correct pandas dtype object, respecting
nullability per ADR-003 rules. Returns a pandas dtype object suitable for use in
`pd.read_csv(dtype=...)` and for Dask schema enforcement.

### Mapping rules
| dtype_token | nullable | pandas dtype |
|---|---|---|
| `"int"` | `False` | `np.dtype("int64")` |
| `"int"` | `True` | `pd.Int64Dtype()` |
| `"Int64"` | `True` | `pd.Int64Dtype()` |
| `"float"` | any | `np.dtype("float64")` |
| `"string"` | any | `pd.StringDtype()` |
| `"datetime"` | any | `"datetime64[ns]"` (string token, applied post-load via `pd.to_datetime`) |
| `"bool"` | any | `pd.BooleanDtype()` |
| `"categorical"` | any | `pd.CategoricalDtype(categories=categories, ordered=False)` |

- `datetime` columns must NOT be passed to `pd.read_csv(dtype=...)` — they must be
  loaded as string first and converted via `pd.to_datetime(..., errors="coerce")` after
  the initial CSV read.
- Categorical columns must load as `string` first, then be cast to
  `pd.CategoricalDtype` after values outside the declared set are replaced with `NA`.

**Error handling:**
- Unknown `dtype_token` → raise `ValueError` naming the unknown token.

**Dependencies:** `pandas`, `numpy`

**Test cases:**
- Assert `map_dtype("int", False, [])` returns `np.dtype("int64")`.
- Assert `map_dtype("int", True, [])` returns `pd.Int64Dtype()`.
- Assert `map_dtype("float", True, [])` returns `np.dtype("float64")`.
- Assert `map_dtype("string", True, [])` returns `pd.StringDtype()`.
- Assert `map_dtype("bool", True, [])` returns `pd.BooleanDtype()`.
- Assert `map_dtype("categorical", True, ["AB","AC"])` returns
  `pd.CategoricalDtype(categories=["AB","AC"], ordered=False)`.
- Assert `map_dtype("datetime", True, [])` returns the string token `"datetime64[ns]"`.
- Assert `map_dtype("unknown_token", False, [])` raises `ValueError`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ING-03: Schema enforcer (partition-level)

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `enforce_schema(df: pd.DataFrame, data_dict: dict[str, dict]) -> pd.DataFrame`

**Description:**
Accept a raw pandas DataFrame (as produced by reading one CSV partition) and return a
DataFrame with the canonical schema enforced. This function is called inside a
`ddf.map_partitions(enforce_schema, data_dict)` call — it must be pure and stateless.

### Processing steps (applied in this order):

1. **Absent column handling:**
   - For each column in `data_dict`:
     - If the column is absent from `df.columns`:
       - If `nullable=False` → raise `KeyError` naming the column and the file.
       - If `nullable=True` → add the column as an all-`NA` Series at the correct dtype.

2. **Datetime columns:** Convert `AcceptedDate` using
   `pd.to_datetime(df["AcceptedDate"], errors="coerce")` — invalid strings become `NaT`.

3. **Categorical columns:** For `WellStatus`:
   - Replace any values not in the declared category set with `pd.NA`.
   - Cast to `pd.CategoricalDtype(categories=[...], ordered=False)`.

4. **Non-datetime, non-categorical columns:** Cast to the pandas dtype returned by
   `map_dtype`. Use `pd.array(..., dtype=...).astype(dtype)` pattern to preserve
   nullable semantics.

5. **Column selection and ordering:** Return only the columns declared in `data_dict`,
   in the order they appear in the data dictionary.

**Error handling:**
- Non-nullable column absent from source DataFrame → `KeyError` with the column name.
- Cast failure for a non-nullable column → propagate the original exception; do not
  silently swallow casting errors.
- Cast failure for a nullable column → coerce invalid values to `NA` rather than
  raising.

**Dependencies:** `pandas`, `numpy`

**Test cases:**
- Given a DataFrame with all expected columns and correct values, assert the returned
  DataFrame has identical dtypes per the data dictionary mapping.
- Given a DataFrame missing a nullable column (`OilProduced`), assert the returned
  DataFrame contains `OilProduced` as all-NA float64 column.
- Given a DataFrame missing a non-nullable column (`ReportYear`), assert `KeyError`
  is raised.
- Given a DataFrame with `WellStatus` containing an invalid value `"XX"`, assert it
  is replaced with `pd.NA` and the column dtype is categorical.
- Given a DataFrame with `AcceptedDate` as `"not-a-date"`, assert the value becomes
  `NaT` and no exception is raised.
- Assert that the output DataFrame has columns in the exact order declared in the
  data dictionary.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ING-04: Raw CSV reader

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `read_raw_csv(file_path: Path, data_dict: dict[str, dict]) -> dd.DataFrame`

**Description:**
Read a single raw CSV file from `data/raw/` into a Dask DataFrame and apply
`enforce_schema` via `map_partitions`. Returns a Dask DataFrame — does NOT call
`.compute()`.

### Processing steps:
1. Use `dd.read_csv(file_path, dtype=str, assume_missing=True)` to load all columns
   as strings initially — this avoids pandas inference problems on mixed-type columns.
2. Derive the `meta` argument for `map_partitions` by calling `enforce_schema` on
   `ddf._meta.copy()` (an empty DataFrame with the inferred column names as string type).
   Never construct meta manually.
3. Apply `ddf.map_partitions(enforce_schema, data_dict, meta=meta)`.
4. Return the resulting Dask DataFrame without calling `.compute()`.

### Year filter:
After `map_partitions`, apply the year filter:
`ddf = ddf[ddf["ReportYear"] >= 2020]`
The filter must be applied as a lazy Dask operation — do not compute before filtering.

**Error handling:**
- If `file_path` does not exist → raise `FileNotFoundError`.
- Propagate any `KeyError` raised by `enforce_schema` for missing non-nullable columns.

**Dependencies:** `dask.dataframe`, `pandas`, `pathlib`

**Test cases:**
- Given a temporary CSV file with all canonical columns and 10 rows (mix of years 2019
  and 2021), assert the returned Dask DataFrame has `ReportYear >= 2020` only (i.e.,
  rows with year 2019 are excluded) — verify by computing row count.
- Assert the return type is `dask.dataframe.DataFrame` (not `pd.DataFrame`).
- Assert all returned column dtypes match the data dictionary after `.compute()`.
- Given a CSV missing a non-nullable column (`Well`), assert `KeyError` is propagated.
- TR-17: Assert the function returns a Dask DataFrame without calling `.compute()`
  internally (verify by patching `dask.dataframe.DataFrame.compute` and asserting
  it is never called during `read_raw_csv`).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ING-05: Multi-file ingest runner

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `run_ingest(config_path: str = "config/config.yaml") -> None`

**Description:**
Top-level entry point for the ingest stage. Reads all CSV files from `data/raw/`,
concatenates them into a single Dask DataFrame, applies schema enforcement and year
filtering, repartitions, and writes to `data/interim/` as Parquet.

### Processing steps:
1. Call `load_config(config_path)` (reuse from `cogcc_pipeline/acquire.py` or place
   a shared config loader in a utility module — see Task ING-06).
2. Load the data dictionary via `load_data_dictionary`.
3. Discover all `*.csv` files in `raw_dir` using `pathlib.Path.glob("*.csv")`.
   If no files are found, log a WARNING and return — do not raise.
4. For each CSV file, call `read_raw_csv(file_path, data_dict)` to get a lazy Dask
   DataFrame.
5. Concatenate all lazy Dask DataFrames with `dd.concat(dfs, ignore_unknown_divisions=True)`.
6. Compute partition count: `n = len(csv_files)`;
   `n_partitions = max(10, min(n, 50))`.
7. Call `ddf.repartition(npartitions=n_partitions)` — this must be the last operation
   before writing (ADR-004).
8. Write to `interim_dir` using `ddf.to_parquet(interim_dir, write_index=False,
   overwrite=True, schema="infer")`.
9. Log total rows written (from partition metadata, not by calling `.compute()`).

### Scheduler:
Use the Dask distributed scheduler. If a `Client` is already running (initialized by
the pipeline entry point), reuse it. Do not initialize a new cluster inside this
function.

**Error handling:**
- No CSV files found in `raw_dir` → log WARNING and return (not an error).
- Any `KeyError` from `enforce_schema` → log ERROR with the offending filename and
  re-raise to stop the stage.

**Dependencies:** `dask.dataframe`, `dask.distributed`, `pandas`, `pathlib`, `logging`

**Test cases:**
- Given 2 synthetic CSV files in a temp `raw_dir` (one for 2020, one for 2021, each
  with 5 rows), assert that `run_ingest` creates at least one Parquet file in
  `interim_dir`.
- Assert the Parquet output is readable by `dd.read_parquet` without error (TR-18).
- Assert the union of all Parquet rows has `ReportYear >= 2020` only.
- Given an empty `raw_dir`, assert `run_ingest` returns without raising and no Parquet
  files are written.
- Assert the number of Parquet partition files equals `max(10, min(n, 50))` where
  `n` is the number of input CSV files.
- TR-17: Assert `run_ingest` does not call `.compute()` before the final write —
  patch `dask.dataframe.DataFrame.compute` and assert 0 calls.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ING-06: Shared utilities module

**Module:** `cogcc_pipeline/utils.py`
**Functions:**
- `load_config(config_path: str) -> dict`
- `setup_logging(log_file: str, level: str) -> None`
- `get_partition_count(n: int) -> int`

**Description:**
Extract shared utility functions used across pipeline stages into a single module.
This avoids circular imports and duplication between `acquire.py` and `ingest.py`.

### `load_config(config_path: str) -> dict`
Identical to the function specified in ACQ-02 but located in `utils.py`.
The acquire module must import it from `utils.py`. Update `acquire.py` to remove its
own copy of this function and use `from cogcc_pipeline.utils import load_config`.

### `setup_logging(log_file: str, level: str) -> None`
Configure a root logger with two handlers (ADR-006):
- `StreamHandler` → console (stdout)
- `FileHandler` → `log_file` path
Both handlers must use the same format: `"%(asctime)s %(levelname)s %(name)s — %(message)s"`.
The log output directory must be created if it does not exist.
This function is idempotent — calling it twice must not add duplicate handlers.

### `get_partition_count(n: int) -> int`
Return `max(10, min(n, 50))`. This formula is used by every stage that writes
Parquet output (ADR-004).

**Error handling:**
- `load_config`: `FileNotFoundError` on missing file; `KeyError` on missing required sections.
- `setup_logging`: `OSError` on unwritable log directory → propagate.

**Dependencies:** `pyyaml`, `logging`, `pathlib`, `datetime`

**Test cases:**
- Assert `get_partition_count(3)` returns `10`.
- Assert `get_partition_count(25)` returns `25`.
- Assert `get_partition_count(100)` returns `50`.
- Assert `get_partition_count(10)` returns `10`.
- Assert `get_partition_count(50)` returns `50`.
- Given a valid config YAML, assert `load_config` returns a dict with required keys.
- Assert `setup_logging` creates the log directory if it does not exist.
- Assert `setup_logging` called twice on the same logger does not add duplicate handlers.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task ING-07: Ingest stage integration tests

**Module:** `tests/test_ingest.py`
**Scope:** TR-17, TR-18, TR-22 (from test-requirements.xml)

**Description:**
Write integration-level tests covering the full ingest output contract.
All tests use synthetic CSV data in a temp directory — no real data files required.

### TR-22 — Schema completeness across partitions
- Create a synthetic temp directory with 3 CSV files, each containing all canonical
  columns.
- Run `run_ingest` to produce Parquet output in a temp `interim_dir`.
- Read at least 3 Parquet partition files individually using `pd.read_parquet`.
- For each partition, assert that ALL of the following columns are present:
  `ApiCountyCode`, `ApiSequenceNumber`, `ReportYear`, `ReportMonth`, `OilProduced`,
  `OilSales`, `GasProduced`, `GasSales`, `FlaredVented`, `GasUsedOnLease`,
  `WaterProduced`, `DaysProduced`, `Well`, `OpName`, `OpNumber`, `DocNum`.
- Assert no expected column is missing or renamed in any sampled partition.

### TR-18 — Parquet readability
- After `run_ingest` completes, read every Parquet file in `interim_dir` using
  `pd.read_parquet` in a fresh read (simulate a new process by using a separate
  `pd.read_parquet` call with no caching).
- Assert no `pyarrow.ArrowInvalid` or `OSError` is raised for any file.

### Additional schema dtype test
- After `run_ingest`, read one partition and assert column dtypes match expected:
  - `ReportYear` is `int64`
  - `OilProduced` is `float64`
  - `ApiCountyCode` dtype is `object` or `string`
  - `WellStatus` is categorical

**Definition of done:** All TR-17, TR-18, TR-22 test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
