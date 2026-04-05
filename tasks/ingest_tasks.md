# Ingest Stage — Task Specifications

## Overview

The ingest stage reads all raw CSV files produced by the acquire stage from `data/raw/`,
applies minimal structural normalization (column name standardisation, data-type coercion,
date range filtering), and writes the result as consolidated Parquet files to
`data/interim/`. Dask is used for parallel reading and writing. No cleaning, outlier
removal, or feature engineering happens here — those are transform and features stage
concerns.

The pipeline package is named `cogcc_pipeline`. All source modules live under
`cogcc_pipeline/`. All test files live under `tests/`.

---

## Raw Data Description (ECMC Production CSV Schema)

The ECMC production CSV files contain the following columns (as documented in
`references/production-data-dictionary.csv`):

| Column | Type | Description |
|---|---|---|
| `APICountyCode` | str/int | 5-digit FIPS county code component of the well API number |
| `APISeqNum` | str/int | 5-digit sequence number component of the well API number |
| `ReportYear` | int | Year of the production report |
| `ReportMonth` | int | Month of the production report (1–12) |
| `OilProduced` | float | Oil produced in BBL |
| `OilSold` | float | Oil sold in BBL |
| `GasProduced` | float | Gas produced in MCF |
| `GasSold` | float | Gas sold in MCF |
| `GasFlared` | float | Gas flared in MCF |
| `GasUsed` | float | Gas used on-site in MCF |
| `WaterProduced` | float | Water produced in BBL |
| `DaysProduced` | float | Number of days the well produced in the month |
| `Well` | str | Well name/identifier string |
| `OpName` | str | Operator name |
| `OpNum` | str/int | Operator number |
| `DocNum` | str/int | Document number |

Note: Some historical files may contain additional or differently-named columns. Column
name normalisation maps all encountered variants to the canonical names above.

---

## Component Architecture

### Modules and files produced by this stage

| Artefact | Purpose |
|---|---|
| `cogcc_pipeline/ingest.py` | All ingestion logic |
| `data/interim/cogcc_production_interim.parquet/` | Parquet dataset directory (Dask output) |
| `tests/test_ingest.py` | Pytest unit and integration tests |

---

## Design Decisions and Constraints

- **Dask for parallelism**: Use `dask.dataframe` to read and process files in parallel.
  Do not call `.compute()` inside any pipeline function — return `dask.dataframe.DataFrame`
  objects. (TR-17)
- **Parquet output file count**: Target `npartitions = max(1, total_rows // 500_000)`.
  Always call `ddf.repartition(npartitions=N).to_parquet(...)` before writing.
  Never write one file per source CSV or per well.
- **Post-read repartition**: After reading any Parquet dataset in downstream stages,
  immediately repartition to `min(npartitions, 50)` before any transformations.
- **String dtype in meta**: All `map_partitions` meta DataFrames must use
  `pd.StringDtype()` for string columns. Never use `"object"` dtype in any `meta=`
  argument.
- **Date range filter**: After reading each raw file, filter rows to `ReportYear >= 2020`
  to exclude pre-2020 history that may appear in annual files.
- **Column name normalisation**: A canonical mapping dict must handle at least the
  following known ECMC variant spellings and apply them uniformly across all files.
- **Encoding**: Try UTF-8 first; fall back to Latin-1 if `UnicodeDecodeError` is raised.
- **Schema completeness**: Every interim Parquet partition must contain all 16 canonical
  columns listed above. Missing columns in a source file must be added as null columns of
  the correct dtype.
- **Logging**: Log file name, row count read, row count after filtering, and any encoding
  fallback for each file processed.
- **Type hints and docstrings**: All public functions and classes.

---

## Task 08: Implement column name normalisation

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `normalise_columns(df: pd.DataFrame) -> pd.DataFrame`

**Description:**

Define a `COLUMN_MAP` dictionary at module level that maps known ECMC column name variants
to the canonical column names defined in the schema table above. Examples of variants to
handle:

- `"API_County_Code"`, `"Api_County_Code"`, `"apicountycode"` → `"APICountyCode"`
- `"API_Seq_Num"`, `"Api_Seq_Num"`, `"apiseqnum"` → `"APISeqNum"`
- `"Report_Year"`, `"report_year"`, `"REPORTYEAR"` → `"ReportYear"`
- `"Report_Month"`, `"report_month"`, `"REPORTMONTH"` → `"ReportMonth"`
- `"Oil_Produced"`, `"oil_produced"`, `"OILPRODUCED"` → `"OilProduced"`
- `"Oil_Sold"`, `"oil_sold"` → `"OilSold"`
- `"Gas_Produced"`, `"gas_produced"` → `"GasProduced"`
- `"Gas_Sold"`, `"gas_sold"` → `"GasSold"`
- `"Gas_Flared"`, `"gas_flared"` → `"GasFlared"`
- `"Gas_Used"`, `"gas_used"` → `"GasUsed"`
- `"Water_Produced"`, `"water_produced"`, `"WATERPRODUCED"` → `"WaterProduced"`
- `"Days_Produced"`, `"days_produced"`, `"DAYSPRODUCED"` → `"DaysProduced"`
- `"Op_Name"`, `"op_name"`, `"OPNAME"` → `"OpName"`
- `"Op_Num"`, `"op_num"` → `"OpNum"`
- `"Doc_Num"`, `"doc_num"` → `"DocNum"`

The function strips leading/trailing whitespace from all column names before applying the
mapping. Unmapped columns are kept as-is (they will be dropped later).

After renaming, add any canonical column that is missing as a null column with the correct
dtype:
- `APICountyCode`, `APISeqNum`, `Well`, `OpName`, `OpNum`, `DocNum` → `pd.StringDtype()`
- `ReportYear`, `ReportMonth` → `Int32` (nullable integer)
- `OilProduced`, `OilSold`, `GasProduced`, `GasSold`, `GasFlared`, `GasUsed`,
  `WaterProduced`, `DaysProduced` → `Float64` (nullable float)

Return only the 16 canonical columns in the fixed order listed in the schema table.

**Dependencies:** `pandas`

**Test cases (`tests/test_ingest.py`):**

- `@pytest.mark.unit` — Given a DataFrame with canonical column names already correct,
  assert the returned DataFrame has the same 16 columns in canonical order and no data
  loss.
- `@pytest.mark.unit` — Given a DataFrame using underscore-separated variant names
  (e.g. `Oil_Produced`), assert all columns are correctly renamed.
- `@pytest.mark.unit` — Given a DataFrame missing `GasFlared` and `GasUsed`, assert those
  columns are added as null `Float64` columns in the result.
- `@pytest.mark.unit` — Given a DataFrame with extra non-canonical columns, assert they
  are dropped from the result.
- `@pytest.mark.unit` — Given column names with leading/trailing whitespace, assert they
  are still mapped correctly.

**Definition of done:** `normalise_columns` implemented with `COLUMN_MAP`; all unit tests
pass; `ruff` and `mypy` report no errors; `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 09: Implement single-file CSV reader

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `read_raw_csv(path: Path, logger: logging.Logger) -> pd.DataFrame`

**Description:**

Read a single ECMC production CSV file into a pandas DataFrame. Apply the following steps:

1. Attempt to read with `encoding="utf-8"`. On `UnicodeDecodeError`, re-read with
   `encoding="latin-1"` and log a warning identifying the file and fallback encoding.
2. Strip leading/trailing whitespace from all string column values.
3. Call `normalise_columns(df)` to standardise column names and dtypes.
4. Filter rows to `ReportYear >= 2020`. Use `.loc` with an integer comparison — do not
   use string operations here.
5. Log: file path, raw row count, row count after date filtering.
6. Return the filtered DataFrame.

If the file cannot be parsed as CSV (e.g. it is binary or malformed), raise `ValueError`
with a descriptive message including the file path.

**Dependencies:** `pandas`, `pathlib`, `logging`

**Test cases (`tests/test_ingest.py`):**

- `@pytest.mark.unit` — Given a valid CSV `tmp_path` file with rows for years 2019, 2020,
  2021, assert the returned DataFrame contains only the 2020 and 2021 rows.
- `@pytest.mark.unit` — Given a CSV with UTF-8 encoding, assert it is read without
  warning.
- `@pytest.mark.unit` — Given a CSV with Latin-1 encoded special characters (e.g.
  accented operator names), assert the fallback encoding is used and a warning is logged.
- `@pytest.mark.unit` — Given an empty CSV (header only), assert the returned DataFrame
  has 0 rows and the correct 16 columns.
- `@pytest.mark.unit` — Given a binary file that cannot be parsed as CSV, assert
  `ValueError` is raised.

**Definition of done:** `read_raw_csv` implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 10: Implement parallel multi-file ingestion with Dask

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `ingest_raw_files(raw_dir: Path, logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Discover all CSV files in `raw_dir` matching the patterns `*_prod_reports.csv` and
`monthly_prod.csv`. For each file, use `dask.delayed` to wrap `read_raw_csv` so that
reading happens lazily and in parallel when `.compute()` is eventually called by the
caller. Construct a `dask.dataframe.DataFrame` from the list of delayed pandas DataFrames
using `dask.dataframe.from_delayed`.

The meta for `from_delayed` must be constructed using the canonical 16-column schema with
correct dtypes (`pd.StringDtype()` for string columns, `pd.Int32Dtype()` for integer
columns, `pd.Float64Dtype()` for float columns). Never use `"object"` dtype in meta.

Do not call `.compute()` inside this function.

Return the Dask DataFrame. The caller is responsible for triggering computation and
writing output.

If no CSV files are found in `raw_dir`, raise `FileNotFoundError` with a message
including the directory path.

**Dependencies:** `dask`, `dask.dataframe`, `dask.delayed`, `pandas`, `pathlib`, `logging`

**Test cases (`tests/test_ingest.py`):**

- `@pytest.mark.unit` (TR-17) — Assert the return type of `ingest_raw_files` is
  `dask.dataframe.DataFrame`, not `pandas.DataFrame`.
- `@pytest.mark.unit` — Given `tmp_path` with two synthetic CSV files, assert the
  returned Dask DataFrame has the correct meta schema (16 canonical columns, correct
  dtypes). Do not call `.compute()` in this assertion — check `ddf._meta` instead.
- `@pytest.mark.unit` — Given an empty `raw_dir`, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` (TR-18) — Given the real `data/raw/` directory (populated
  by acquire), call `ingest_raw_files`, then `.compute()`, then write to a tmp Parquet
  path and re-read with `pd.read_parquet`. Assert the re-read DataFrame is non-empty
  and contains all 16 canonical columns.

**Definition of done:** `ingest_raw_files` implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 11: Implement interim Parquet writer

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `write_interim_parquet(ddf: dask.dataframe.DataFrame, interim_dir: Path, logger: logging.Logger) -> Path`

**Description:**

Compute target partition count: `max(1, estimated_rows // 500_000)` where
`estimated_rows` is approximated from the Dask DataFrame's known divisions or a quick
`len()` call after a `.persist()`. If divisions are not known, default to
`npartitions = max(1, ddf.npartitions)`.

Call `ddf.repartition(npartitions=N).to_parquet(str(interim_dir / "cogcc_production_interim.parquet"), write_index=False, engine="pyarrow", overwrite=True)`.

Log the output directory, target partition count, and completion status.

Return `interim_dir / "cogcc_production_interim.parquet"` as a `Path`.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases (`tests/test_ingest.py`):**

- `@pytest.mark.unit` — Given a small synthetic Dask DataFrame (< 500,000 rows), assert
  the function writes to the expected directory, returns the correct Path, and the
  partition count is at least 1.
- `@pytest.mark.unit` (TR-18) — After `write_interim_parquet` completes on a synthetic
  DataFrame, read the written Parquet with `pd.read_parquet` and assert it is non-empty
  and contains all 16 canonical columns.
- `@pytest.mark.unit` — Assert the function does not raise when `interim_dir` does not
  yet exist (it must be created).

**Definition of done:** `write_interim_parquet` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 12: Implement schema completeness validator for interim output

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `validate_interim_schema(interim_parquet_path: Path, logger: logging.Logger) -> list[str]`

**Description:**

Read a sample of at least 3 Parquet partition files from the interim output directory
(use `glob` to list `*.parquet` files). For each sampled partition, check that all 16
canonical columns are present. Collect and return a list of validation error strings
describing any missing columns per file. Log one warning per error. Return empty list
if all sampled partitions pass.

**Test cases (`tests/test_ingest.py`):**

- `@pytest.mark.unit` (TR-22) — Write 3 synthetic Parquet files to `tmp_path`, each
  containing all 16 canonical columns. Assert `validate_interim_schema` returns an empty
  error list.
- `@pytest.mark.unit` (TR-22) — Write 3 synthetic Parquet files where one is missing
  `GasFlared`. Assert the error list contains one entry naming that file and column.
- `@pytest.mark.unit` (TR-22) — Write only 1 Parquet file. Assert the function does not
  raise (graceful handling when fewer than 3 partitions exist).

**Definition of done:** `validate_interim_schema` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 13: Implement top-level ingest entry point

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `run_ingest(config: dict, logger: logging.Logger) -> Path`

**Description:**

Top-level entry point for the ingest stage. Performs the following steps in order:

1. Read `ingest` section from `config`.
2. Call `ingest_raw_files(raw_dir, logger)` to obtain a Dask DataFrame.
3. Call `write_interim_parquet(ddf, interim_dir, logger)` to write the output.
4. Call `validate_interim_schema(interim_parquet_path, logger)` to validate output.
5. If validation errors exist, log each error as a warning (do not raise — a partial
   schema is recoverable in transform).
6. Return the interim Parquet path.

**Test cases (`tests/test_ingest.py`):**

- `@pytest.mark.unit` (TR-17) — Mock `ingest_raw_files` to return a synthetic Dask
  DataFrame. Assert `run_ingest` returns a Path and does not call `.compute()` internally
  (verify by checking the mock return type is still a Dask DataFrame before write).
- `@pytest.mark.unit` — Mock `validate_interim_schema` to return two error strings.
  Assert `run_ingest` still returns the interim path (does not raise) and logs two
  warnings.
- `@pytest.mark.integration` — Run `run_ingest` against real `data/raw/` files. Assert
  the returned Path exists, is a directory containing `.parquet` files, and at least one
  file is readable by `pd.read_parquet`. Requires data files on disk.

**Definition of done:** `run_ingest` implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.
