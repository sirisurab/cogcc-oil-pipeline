# Transform Stage — Task Specifications

## Overview

The transform stage reads the interim Parquet dataset produced by the ingest stage,
applies comprehensive data cleaning and validation, constructs a synthetic API well number
(`well_id`) from component fields, resolves date columns into a proper `production_date`,
removes duplicates, validates physical bounds, flags outliers, and writes a cleaned,
consolidated Parquet dataset to `data/processed/cogcc_production_cleaned.parquet/`.

All transformation logic runs on Dask DataFrames. No `.compute()` calls are made inside
pipeline functions — the caller (orchestrator) triggers materialisation.

The pipeline package is named `cogcc_pipeline`. All source modules live under
`cogcc_pipeline/`. All test files live under `tests/`.

---

## Domain Context (from oil-and-gas-domain-expert)

### API Well Number
Colorado wells are identified by a 10-digit API number structured as:
`05` (state code) + `{3-digit county FIPS}` + `{5-digit sequence number}`.
The ECMC data provides `APICountyCode` (county component) and `APISeqNum` (sequence
number). The pipeline constructs a `well_id` string of the form `"05{county:03d}{seq:05d}"`.
Valid county codes for Colorado are in the range 001–125 (odd numbers only for most FIPS,
but for validation purposes accept 1–125). Sequence numbers range from 00001–99999.

### Production Volumes — Physical Bounds
- `OilProduced` and `OilSold`: must be ≥ 0 BBL. A single-well monthly value above
  50,000 BBL/month is a likely unit error (TR-02).
- `GasProduced` and `GasSold`: must be ≥ 0 MCF.
- `GasFlared` and `GasUsed`: must be ≥ 0 MCF.
- `WaterProduced`: must be ≥ 0 BBL.
- `DaysProduced`: must be in range [0, 31].

### Zero vs Null Distinction (TR-05)
A zero production value in a month means the well was shut-in or produced nothing but
is still active. This is a valid measurement. A null/NaN means the data was not reported.
The transform stage must preserve zeros and not coerce them to null. Only truly missing
values (NaN, blank strings, known sentinel values like `-9999`, `-999`, `9999`) should be
set to null.

### Sentinel Values
Common ECMC sentinel values for missing data: `-9999`, `-999`, `9999`, `9999.0`,
`-9999.0`. These must be replaced with `pd.NA` for numeric columns.

### Duplicate Records
Duplicate rows are defined as having identical `(well_id, ReportYear, ReportMonth)`.
Keep the first occurrence; drop subsequent duplicates.

### Well Completeness (TR-04)
After cleaning, each well should have a record for every month from its first to last
production date. Gaps (missing months in an otherwise active well's record) are flagged
but not filled at this stage — gap-filling or forward-fill is a features-stage decision.

---

## Component Architecture

### Modules and files produced by this stage

| Artefact | Purpose |
|---|---|
| `cogcc_pipeline/transform.py` | All cleaning and transformation logic |
| `data/processed/cogcc_production_cleaned.parquet/` | Cleaned consolidated Parquet dataset |
| `data/processed/data_quality_report.json` | Data quality metrics and flag summary |
| `tests/test_transform.py` | Pytest unit and integration tests |

---

## Design Decisions and Constraints

- **Dask throughout**: All functions accept and return `dask.dataframe.DataFrame`. No
  `.compute()` inside pipeline functions (TR-17).
- **meta dtypes**: All `map_partitions` calls must use a meta DataFrame with
  `pd.StringDtype()` for string columns. Never use `"object"` in `meta=`.
- **Repartition after read**: After reading the interim Parquet dataset, immediately
  repartition to `min(npartitions, 50)`.
- **Row filtering**: Use `map_partitions` with an inner pandas function for any string
  filtering operation. Do not use Dask's `.str` accessor directly on repartitioned Series.
- **Output file count**: Target 20–50 output Parquet files. Repartition to 30 partitions
  before writing (do not partition_on well_id — it has too high cardinality).
- **Parquet write**: `ddf.repartition(npartitions=30).to_parquet(output_path, write_index=False, engine="pyarrow", overwrite=True)`.
- **Idempotency**: Running transform twice on the same interim data must produce the same
  output (TR-15).
- **Schema**: The cleaned output schema must include all 16 canonical columns plus
  `well_id` (string) and `production_date` (datetime64[ns]).
- **Type hints and docstrings**: All public functions and classes.

---

## Task 14: Implement sentinel value replacement

**Module:** `cogcc_pipeline/transform.py`
**Function:** `replace_sentinels(ddf: dask.dataframe.DataFrame, logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Define a module-level constant `SENTINEL_VALUES: list[float | int]` containing:
`[-9999, -999, 9999, 9999.0, -9999.0]`.

For each numeric column in the canonical schema (`OilProduced`, `OilSold`, `GasProduced`,
`GasSold`, `GasFlared`, `GasUsed`, `WaterProduced`, `DaysProduced`, `ReportYear`,
`ReportMonth`), replace any value in `SENTINEL_VALUES` with `pd.NA` using
`map_partitions`. Do not alter string columns.

Preserve zero values — only sentinel values are replaced, not zeros.

Return the modified Dask DataFrame with identical schema.

**Dependencies:** `dask.dataframe`, `pandas`

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` (TR-05) — Given a DataFrame with `OilProduced = 0.0` for some
  rows, assert those rows still have `OilProduced = 0.0` after `replace_sentinels` (zeros
  are not treated as sentinels).
- `@pytest.mark.unit` — Given a DataFrame with `OilProduced = -9999.0`, assert it
  becomes `pd.NA` after the function.
- `@pytest.mark.unit` — Given a DataFrame with `OilProduced = 9999.0`, assert it
  becomes `pd.NA`.
- `@pytest.mark.unit` — Given a DataFrame with string column `OpName = "-9999"`, assert
  it is unchanged (string columns are not affected).
- `@pytest.mark.unit` (TR-17) — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** `replace_sentinels` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 15: Implement API well number construction and validation

**Module:** `cogcc_pipeline/transform.py`
**Function:** `build_well_id(ddf: dask.dataframe.DataFrame, logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Construct a `well_id` column by zero-padding `APICountyCode` to 3 digits and `APISeqNum`
to 5 digits and prepending Colorado state code `"05"`:
`well_id = "05" + APICountyCode.zfill(3) + APISeqNum.zfill(5)`

This produces a 10-character string in the form `"05{ccc}{sssss}"`.

Validate `well_id` values:
- Length must be exactly 10 characters.
- Must match the regex pattern `^05\d{8}$`.
- County digits (characters 2–4) must form an integer in range [001, 125].
- Sequence digits (characters 5–9) must form an integer in range [00001, 99999].

Rows that fail any validation rule must have their `well_id` set to `pd.NA` and be
counted. Log a warning with the count of invalid well IDs constructed.

Use `map_partitions` for the construction and validation logic. The meta DataFrame for
`map_partitions` must include `well_id` as `pd.StringDtype()`.

Return the Dask DataFrame with `well_id` added as the first column.

**Dependencies:** `dask.dataframe`, `pandas`, `re`

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` — Given `APICountyCode = "1"` and `APISeqNum = "23"`, assert
  `well_id = "0500100023"`.
- `@pytest.mark.unit` — Given `APICountyCode = "123"` and `APISeqNum = "45678"`, assert
  `well_id = "0512345678"`.
- `@pytest.mark.unit` — Given `APICountyCode = "200"` (invalid county > 125), assert
  `well_id` is `pd.NA`.
- `@pytest.mark.unit` — Given `APICountyCode = "0"` and `APISeqNum = "0"` (sequence = 0,
  invalid), assert `well_id` is `pd.NA`.
- `@pytest.mark.unit` — Given null `APICountyCode`, assert `well_id` is `pd.NA` and no
  exception is raised.
- `@pytest.mark.unit` (TR-17) — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** `build_well_id` implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 16: Implement production date construction and validation

**Module:** `cogcc_pipeline/transform.py`
**Function:** `build_production_date(ddf: dask.dataframe.DataFrame, logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Construct a `production_date` column of dtype `datetime64[ns]` by combining `ReportYear`
and `ReportMonth` into the first day of that month:
`production_date = pd.to_datetime({"year": df.ReportYear, "month": df.ReportMonth, "day": 1})`.

Validate:
- `ReportYear` must be in range [2020, current_year]. Rows outside this range get
  `production_date = pd.NaT` and are logged.
- `ReportMonth` must be in range [1, 12]. Rows outside this range get
  `production_date = pd.NaT` and are logged.

Use `map_partitions`. The meta for the output must include `production_date` as
`datetime64[ns]`.

Drop `ReportYear` and `ReportMonth` from the DataFrame after constructing
`production_date` to avoid duplication. The cleaned schema carries `production_date`
instead.

Return the modified Dask DataFrame.

**Dependencies:** `dask.dataframe`, `pandas`, `datetime`

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` — Given `ReportYear=2022`, `ReportMonth=3`, assert
  `production_date = datetime(2022, 3, 1)`.
- `@pytest.mark.unit` — Given `ReportMonth=13` (invalid), assert `production_date` is
  `pd.NaT`.
- `@pytest.mark.unit` — Given `ReportYear=2019` (below 2020), assert `production_date`
  is `pd.NaT`.
- `@pytest.mark.unit` — Given null `ReportYear`, assert no exception raised and
  `production_date` is `pd.NaT`.
- `@pytest.mark.unit` (TR-17) — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** `build_production_date` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 17: Implement physical bounds validation and outlier flagging

**Module:** `cogcc_pipeline/transform.py`
**Function:** `validate_physical_bounds(ddf: dask.dataframe.DataFrame, logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Apply physical bound rules to numeric production columns. Rules:

| Column | Min | Max | Action on violation |
|---|---|---|---|
| `OilProduced` | 0.0 | 50,000 BBL/month | Values < 0 → `pd.NA`; values > 50,000 → add `outlier_flag = True` |
| `OilSold` | 0.0 | 50,000 | Values < 0 → `pd.NA`; values > 50,000 → `outlier_flag = True` |
| `GasProduced` | 0.0 | no upper | Values < 0 → `pd.NA` |
| `GasSold` | 0.0 | no upper | Values < 0 → `pd.NA` |
| `GasFlared` | 0.0 | no upper | Values < 0 → `pd.NA` |
| `GasUsed` | 0.0 | no upper | Values < 0 → `pd.NA` |
| `WaterProduced` | 0.0 | no upper | Values < 0 → `pd.NA` |
| `DaysProduced` | 0.0 | 31.0 | Values outside [0, 31] → `pd.NA` |

Add a boolean column `outlier_flag` (default `False`) to the DataFrame. Set it to `True`
for rows where any production volume exceeds its upper bound.

Log a count of nulled negative values and a count of outlier-flagged rows.

Use `map_partitions`. The meta must include `outlier_flag` as `pd.BooleanDtype()`.

Do not drop outlier-flagged rows — the downstream ML stage decides whether to exclude them.

**Dependencies:** `dask.dataframe`, `pandas`

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` (TR-01) — Given `OilProduced = -100.0`, assert it is replaced with
  `pd.NA` and `outlier_flag = False`.
- `@pytest.mark.unit` (TR-01) — Given `WaterProduced = -0.001`, assert it is replaced
  with `pd.NA`.
- `@pytest.mark.unit` (TR-02) — Given `OilProduced = 75_000.0`, assert `outlier_flag`
  is `True` and the value is retained.
- `@pytest.mark.unit` (TR-02) — Given `OilProduced = 49_999.0`, assert `outlier_flag`
  is `False`.
- `@pytest.mark.unit` — Given `DaysProduced = 32`, assert it becomes `pd.NA`.
- `@pytest.mark.unit` (TR-05) — Given `OilProduced = 0.0`, assert it remains `0.0`
  (not nulled).
- `@pytest.mark.unit` (TR-17) — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** `validate_physical_bounds` implemented; all tests pass; `ruff`
and `mypy` report no errors; `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 18: Implement duplicate removal

**Module:** `cogcc_pipeline/transform.py`
**Function:** `remove_duplicates(ddf: dask.dataframe.DataFrame, logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Remove duplicate rows where `(well_id, production_date)` is identical across rows.
Keep the first occurrence. Use `ddf.drop_duplicates(subset=["well_id", "production_date"])`.

Rows with null `well_id` or null `production_date` should not be considered duplicates
of each other — they represent missing-key records and should all be retained (even if
there are multiple nulls).

Log the count of removed duplicates.

**Dependencies:** `dask.dataframe`, `pandas`

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` (TR-15) — Given a DataFrame with 3 rows where 2 share the same
  `(well_id, production_date)`, assert the result has 2 rows.
- `@pytest.mark.unit` (TR-15) — Run `remove_duplicates` twice on the same input. Assert
  the output after the second call equals the output after the first call (idempotency).
- `@pytest.mark.unit` — Given a DataFrame with two rows having null `well_id`, assert
  both are retained.
- `@pytest.mark.unit` (TR-17) — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** `remove_duplicates` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 19: Implement data quality report generation

**Module:** `cogcc_pipeline/transform.py`
**Function:** `generate_quality_report(ddf: dask.dataframe.DataFrame, output_path: Path, logger: logging.Logger) -> dict`

**Description:**

Compute and serialise a JSON data quality report. The report must include:

- `total_rows`: total row count.
- `null_counts`: per-column null count (as a dict).
- `outlier_flag_count`: count of rows where `outlier_flag == True`.
- `invalid_well_id_count`: count of rows where `well_id` is null.
- `invalid_production_date_count`: count of rows where `production_date` is null.
- `duplicate_rows_removed`: tracked from `remove_duplicates` (pass as parameter or log).
- `wells_count`: count of distinct non-null `well_id` values.
- `date_range_min`: earliest `production_date` as ISO string.
- `date_range_max`: latest `production_date` as ISO string.

This function IS allowed to call `.compute()` because it materialises summary statistics
for serialisation.

Write the report dict as JSON to `output_path`. Return the report dict.

**Dependencies:** `dask.dataframe`, `pandas`, `json`, `pathlib`, `logging`

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` (TR-12) — Given a small synthetic Dask DataFrame with known null
  counts, assert the returned dict has the correct `null_counts` values.
- `@pytest.mark.unit` — Assert the JSON file is written to `output_path` and is
  parseable by `json.loads`.
- `@pytest.mark.unit` — Given a DataFrame with 2 outlier-flagged rows, assert
  `outlier_flag_count == 2`.

**Definition of done:** `generate_quality_report` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 20: Implement well completeness checker

**Module:** `cogcc_pipeline/transform.py`
**Function:** `check_well_completeness(ddf: dask.dataframe.DataFrame, logger: logging.Logger) -> dict[str, list[str]]`

**Description:**

For each distinct `well_id`, compute the expected number of months from its earliest to
its latest `production_date` using the formula:
`expected = (year_max - year_min) * 12 + (month_max - month_min) + 1`.

Compare to actual row count for that well. If actual < expected, the well has gaps.

Return a dict mapping `well_id` → list of ISO date strings for the missing months.

Log a summary: total wells checked, count with gaps, count without gaps.

This function calls `.compute()` to materialise the grouped statistics.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` (TR-04) — Given a synthetic well with 12 monthly records from
  Jan 2022 to Dec 2022 (no gaps), assert the returned dict entry for that well is an
  empty list.
- `@pytest.mark.unit` (TR-04) — Given a synthetic well with records for Jan–Mar 2022 and
  May–Dec 2022 (missing April), assert the returned dict entry for that well contains the
  ISO string `"2022-04-01"`.
- `@pytest.mark.unit` — Given a well with a single record, assert no gap is reported.

**Definition of done:** `check_well_completeness` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 21: Implement cleaned Parquet writer

**Module:** `cogcc_pipeline/transform.py`
**Function:** `write_cleaned_parquet(ddf: dask.dataframe.DataFrame, processed_dir: Path, filename: str, logger: logging.Logger) -> Path`

**Description:**

Sort the Dask DataFrame by `["well_id", "production_date"]` (use `ddf.sort_values`
or per-partition sort via `map_partitions` with a final `repartition`).

Repartition to 30 partitions before writing. Do NOT use `partition_on="well_id"` — the
high cardinality of well IDs would produce tens of thousands of files.

Write to `processed_dir / filename` using:
`ddf.repartition(npartitions=30).to_parquet(..., write_index=False, engine="pyarrow", overwrite=True)`.

Return the output path.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` (TR-18) — After writing a synthetic Dask DataFrame, read the output
  with `pd.read_parquet` and assert it is non-empty and has the correct schema.
- `@pytest.mark.unit` (TR-16) — After writing, read two sequential Parquet partition
  files for the same well and assert the last `production_date` of the first file is
  earlier than or equal to the first `production_date` of the second file (sort
  stability).
- `@pytest.mark.unit` — Assert the returned Path is `processed_dir / filename`.
- `@pytest.mark.unit` — Assert the output directory contains at most 50 `.parquet` files
  (file count constraint).

**Definition of done:** `write_cleaned_parquet` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 22: Implement top-level transform entry point

**Module:** `cogcc_pipeline/transform.py`
**Function:** `run_transform(config: dict, logger: logging.Logger) -> Path`

**Description:**

Top-level entry point for the transform stage. Executes the following pipeline in order:

1. Read `transform` config section.
2. Read interim Parquet dataset using `dask.dataframe.read_parquet(interim_dir / "cogcc_production_interim.parquet")`.
3. Immediately repartition to `min(ddf.npartitions, 50)`.
4. Call `replace_sentinels`.
5. Call `build_well_id`.
6. Call `build_production_date`.
7. Call `validate_physical_bounds`.
8. Call `remove_duplicates`.
9. Call `write_cleaned_parquet`.
10. Call `generate_quality_report` (this triggers `.compute()` for the statistics).
11. Call `check_well_completeness` (triggers `.compute()` for completeness check).
12. Return the cleaned Parquet path.

Do not call `.compute()` before step 9. The pipeline must remain lazy through steps 2–8.

**Test cases (`tests/test_transform.py`):**

- `@pytest.mark.unit` (TR-17) — Mock steps 2–8 to pass through a synthetic Dask
  DataFrame. Assert that between steps 2 and 8, `.compute()` is never called (mock
  `dask.dataframe.DataFrame.compute` and assert call count is 0).
- `@pytest.mark.unit` (TR-15) — Mock `remove_duplicates` to return a known DataFrame.
  Run `run_transform` twice with the same input. Assert the output Parquet content is
  identical both times (idempotency).
- `@pytest.mark.unit` (TR-11) — Assert the returned path is the expected cleaned Parquet
  path.
- `@pytest.mark.integration` — Run `run_transform` against real `data/interim/` data.
  Assert the output Parquet directory exists, is readable by `pd.read_parquet`, has all
  expected columns, and the data quality report JSON exists and is valid. Requires data
  files on disk at `data/interim/`.

**Additional integration test cases:**

- `@pytest.mark.integration` (TR-12) — Read the cleaned Parquet output. Assert:
  - `production_date` column dtype is `datetime64[ns]`.
  - All float columns are `Float64` or `float64`.
  - `well_id` is `string` or `object` dtype.
  - No column is unexpectedly `object` when it should be numeric.
- `@pytest.mark.integration` (TR-11) — For 10 randomly sampled `(well_id, production_date)`
  rows in the cleaned output, read the matching raw row from `data/raw/` and assert the
  `OilProduced` value matches (spot-check data integrity).
- `@pytest.mark.integration` (TR-14) — Read two different Parquet partition files from
  the cleaned output. Assert their column names and dtypes are identical.

**Definition of done:** `run_transform` implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.
