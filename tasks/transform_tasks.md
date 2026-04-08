# Transform Stage — Task Specifications

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/transform.py`
**Test file:** `tests/test_transform.py`

## Overview

The transform stage reads the interim Parquet dataset (`data/interim/cogcc_raw.parquet`), applies all cleaning and standardization operations, and writes a cleaned, indexed Parquet dataset to `data/interim/cogcc_cleaned.parquet`. It also emits a cleaning report JSON to `data/interim/cleaning_report.json`.

This stage uses the Dask distributed scheduler — it checks for an existing distributed `Client` (reuse if present, otherwise create from config). All configuration is read from `config.yaml`.

## Design Decisions & Constraints

- All transformation logic inside `map_partitions` functions must use vectorized pandas/numpy operations. No `iterrows()`, `itertuples()`, or Python loops over rows.
- After reading the interim Parquet, immediately repartition to `min(npartitions, 50)`.
- A synthetic `production_date` column (type `datetime64[ns]`) is constructed from `ReportYear` and `ReportMonth` (first day of the month). This becomes the primary time index for all downstream processing.
- A composite well identifier `well_id` is constructed by concatenating `ApiCountyCode` + `ApiSequenceNumber` + `ApiSidetrack`. This is the entity identifier used for groupby and set_index operations.
- `WellStatus` must be cast from `pd.StringDtype()` to `pd.CategoricalDtype(categories=["AB","AC","DG","PA","PR","SI","SO","TA","WO"], ordered=False)` **after** cleaning. Any value not in the allowed category list must be replaced with `pd.NA` before casting.
- The **final steps** before writing output must follow this exact sequence:
  1. `set_index("well_id")` — triggers a distributed shuffle; call exactly once.
  2. `repartition(npartitions=N)` — target `max(1, ddf.npartitions // 10)`, never more than 50 output files.
  3. `map_partitions(_sort_partition_by_date, meta=ddf._meta)` — sort within each partition by `production_date`.
- Never write more than 200 Parquet files. Target 20–50 output files.
- Row filtering using string operations must be done inside a `map_partitions` function, not directly on a Dask Series.
- Zero production values are **valid measurements** (not missing data) and must be preserved as zeros. Only `pd.NA` / `NaN` indicates missing data.
- Negative production volumes are physical violations: set to `pd.NA` (not zero) and add a boolean flag column (`OilProduced_negative`, `GasProduced_negative`, `WaterProduced_negative`).
- Oil volumes ≥ 50,000 BBL/month for a single well are flagged as potential unit errors (add `OilProduced_unit_flag` boolean column; do **not** remove the value).
- Duplicate rows are defined as exact duplicates on all columns. Remove them (keep first occurrence).
- Date filter: retain rows where `production_date >= config["transform"]["date_start"]` (i.e., `2020-01-01`).
- Reuse `get_or_create_client` pattern from ingest (or import from a shared `cogcc_pipeline/utils.py` module — the coder may choose to refactor this into a utility function).
- A cleaning report JSON is written to `data/interim/cleaning_report.json` containing: `rows_before`, `rows_after_dedup`, `rows_after_date_filter`, `negative_oil_count`, `negative_gas_count`, `negative_water_count`, `unit_flag_oil_count`, `null_counts_per_column`.
- Use `pathlib.Path` throughout. All functions have type hints and docstrings. Logging to stdout and `logs/pipeline.log`.

## `config.yaml` — transform section

```
transform:
  interim_dir: "data/interim"
  processed_dir: "data/processed"
  date_start: "2020-01-01"
```

---

## Task T-01: Construct production_date and well_id columns

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_add_derived_keys(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Add two derived columns to a pandas partition:
- `production_date`: `datetime64[ns]` constructed as the first day of the month using `pd.to_datetime` on `ReportYear` and `ReportMonth`. Formula: `pd.to_datetime(dict(year=pdf["ReportYear"], month=pdf["ReportMonth"], day=1))`.
- `well_id`: `pd.StringDtype()` constructed by concatenating `ApiCountyCode` + `"-"` + `ApiSequenceNumber` + `"-"` + `ApiSidetrack` (zero-padded strings, preserving leading zeros). Use vectorized string concatenation.

This function is called via `map_partitions`. The updated `meta` for this transformation must include the two new columns with their correct dtypes.

**Test cases:**
- `@pytest.mark.unit` Given a partition with `ReportYear=2021`, `ReportMonth=3`, assert `production_date == 2021-03-01`.
- `@pytest.mark.unit` Given `ApiCountyCode="05"`, `ApiSequenceNumber="00123"`, `ApiSidetrack="00"`, assert `well_id == "05-00123-00"`.
- `@pytest.mark.unit` Assert `production_date` dtype is `datetime64[ns]`.
- `@pytest.mark.unit` Assert `well_id` dtype is `pd.StringDtype()`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-02: Duplicate removal

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_drop_duplicates(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Remove exact duplicate rows (all columns equal). Use `pdf.drop_duplicates(keep="first")`. Return the deduplicated DataFrame with all original columns and dtypes preserved.

This function is called via `map_partitions`.

**Test cases:**
- `@pytest.mark.unit` Given a partition with 3 rows where 2 are identical, assert the result has 2 rows (one duplicate removed).
- `@pytest.mark.unit` Given a partition with no duplicates, assert the result has the same number of rows as the input.
- `@pytest.mark.unit` Assert dtypes are unchanged after deduplication.
- **TR-15** `@pytest.mark.unit` Apply `_drop_duplicates` twice on the same data; assert the result after the second application is identical to the result after the first (idempotency).

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-03: Negative production value handling

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_handle_negative_production(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
For each of `OilProduced`, `GasProduced`, `WaterProduced`:
- Add a boolean flag column (`OilProduced_negative`, `GasProduced_negative`, `WaterProduced_negative`) using `pd.BooleanDtype()`: `True` where the value is < 0, `False` otherwise, `pd.NA` where the original value is `pd.NA`.
- Set negative values to `pd.NA` using `np.where` or `pd.Series.where`.
- Preserve zero values as zero (do not modify them).

This function is called via `map_partitions`. The `meta` for this transformation must include the three new flag columns.

**Test cases:**
- `@pytest.mark.unit` **TR-01** Given `OilProduced=-5.0`, assert the result is `pd.NA` and `OilProduced_negative=True`.
- `@pytest.mark.unit` **TR-01** Given `OilProduced=0.0`, assert the result is `0.0` (not `pd.NA`) and `OilProduced_negative=False`.
- `@pytest.mark.unit` **TR-05** Given `OilProduced=0.0`, assert zero is preserved as `0.0` in the output (not converted to `pd.NA`).
- `@pytest.mark.unit` Given `OilProduced=pd.NA`, assert `OilProduced_negative=pd.NA` (not `True` or `False`).
- `@pytest.mark.unit` Given `GasProduced=100.0`, assert value is unchanged and `GasProduced_negative=False`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-04: Production unit flag

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_flag_unit_outliers(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Add a boolean flag column `OilProduced_unit_flag` (`pd.BooleanDtype()`): `True` where `OilProduced >= 50_000` (potential unit error per TR-02), `False` otherwise, `pd.NA` where `OilProduced` is `pd.NA`. Do **not** modify the `OilProduced` value itself.

This function is called via `map_partitions`.

**Test cases:**
- `@pytest.mark.unit` **TR-02** Given `OilProduced=55000.0`, assert `OilProduced_unit_flag=True` and value is unchanged.
- `@pytest.mark.unit` **TR-02** Given `OilProduced=49999.0`, assert `OilProduced_unit_flag=False`.
- `@pytest.mark.unit` Given `OilProduced=0.0`, assert `OilProduced_unit_flag=False`.
- `@pytest.mark.unit` Given `OilProduced=pd.NA`, assert `OilProduced_unit_flag=pd.NA`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-05: String standardization

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_standardize_strings(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
For columns `OpName`, `Well`, `FormationCode`:
- Strip leading/trailing whitespace.
- Normalize internal whitespace (replace multiple consecutive spaces with a single space).
- Convert to uppercase.
- Use only vectorized string operations (`pd.Series.str.strip()`, `str.upper()`, `str.replace()` with regex).
- Preserve `pd.NA` values (do not convert to the string `"NAN"` or `"NA"`).

This function is called via `map_partitions`.

**Test cases:**
- `@pytest.mark.unit` Given `OpName="  chevron  usa "`, assert result is `"CHEVRON USA"`.
- `@pytest.mark.unit` Given `OpName=pd.NA`, assert result remains `pd.NA`.
- `@pytest.mark.unit` Given `Well="well  name"` (double space), assert result is `"WELL NAME"` (single space).
- `@pytest.mark.unit` Given `FormationCode="niobrara"`, assert result is `"NIOBRARA"`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-06: WellStatus categorical cast

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_cast_well_status(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Cast `WellStatus` from `pd.StringDtype()` to `pd.CategoricalDtype(categories=["AB","AC","DG","PA","PR","SI","SO","TA","WO"], ordered=False)`.
- Before casting, replace any value **not** in the allowed list with `pd.NA`. Use vectorized `pd.Series.where(series.isin(allowed_list), other=pd.NA)`.
- After the replacement, cast using `.astype(pd.CategoricalDtype(...))`.
- Allowed categories must be read from the data dictionary constant (do not hardcode elsewhere).

This function is called via `map_partitions`.

**Test cases:**
- `@pytest.mark.unit` Given `WellStatus="PR"`, assert dtype is `CategoricalDtype` and value is `"PR"`.
- `@pytest.mark.unit` Given `WellStatus="UNKNOWN"` (not in allowed list), assert value is `pd.NA` after casting.
- `@pytest.mark.unit` Given `WellStatus=pd.NA`, assert value remains `pd.NA` after casting.
- `@pytest.mark.unit` Assert the categories of the result are exactly `["AB","AC","DG","PA","PR","SI","SO","TA","WO"]` in that order.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-07: Date filter

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_filter_date_range(pdf: pd.DataFrame, date_start: pd.Timestamp) -> pd.DataFrame`

**Description:**
Filter a pandas partition to only rows where `production_date >= date_start`. Returns the filtered DataFrame with identical columns and dtypes.

**Parameters:**
- `pdf`: pandas partition (must already have `production_date` column from Task T-01)
- `date_start`: minimum date (inclusive), from `pd.Timestamp(config["transform"]["date_start"])`

**Test cases:**
- `@pytest.mark.unit` Given rows with `production_date` in 2019, 2020, 2021 and `date_start=2020-01-01`, assert only 2020 and 2021 rows are returned.
- `@pytest.mark.unit` Given all rows before `date_start`, assert empty DataFrame is returned with all columns.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-08: Partition sorter

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_sort_partition_by_date(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Sort a pandas partition by `production_date` in ascending order. This function is called via `map_partitions` **after** `set_index("well_id")` and `repartition`. It preserves all columns and dtypes.

**Test cases:**
- `@pytest.mark.unit` Given a partition with dates out of order, assert the result is sorted ascending by `production_date`.
- `@pytest.mark.unit` Assert sort stability: rows with the same `production_date` maintain their relative order.
- **TR-16** `@pytest.mark.unit` Given two simulated partitions for the same well, assert the last date in partition 0 is earlier than the first date in partition 1.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-09: Cleaning report generator

**Module:** `cogcc_pipeline/transform.py`
**Function:** `build_cleaning_report(stats: dict, output_path: Path) -> None`

**Description:**
Write a JSON cleaning report to `output_path`. The `stats` dict must contain:
- `rows_before`: total rows before deduplication (integer)
- `rows_after_dedup`: total rows after deduplication (integer)
- `rows_after_date_filter`: total rows after date filter (integer)
- `negative_oil_count`: count of rows where `OilProduced_negative=True` (integer)
- `negative_gas_count`: count of rows where `GasProduced_negative=True` (integer)
- `negative_water_count`: count of rows where `WaterProduced_negative=True` (integer)
- `unit_flag_oil_count`: count of rows where `OilProduced_unit_flag=True` (integer)
- `null_counts_per_column`: dict mapping each column name to its null count (integer)

**Parameters:**
- `stats`: dict with all fields above
- `output_path`: path to write JSON file

**Test cases:**
- `@pytest.mark.unit` Given a valid stats dict, assert the JSON file is created and contains all expected keys.
- `@pytest.mark.unit` Assert the written JSON is valid and can be re-read with `json.loads`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-10: Transform stage runner

**Module:** `cogcc_pipeline/transform.py`
**Function:** `run_transform(config: dict) -> None`

**Description:**
Top-level transform stage entry point. Executes all cleaning steps in the following order:

1. Get/create Dask distributed Client via `get_or_create_client(config)`.
2. Read `data/interim/cogcc_raw.parquet` using `dd.read_parquet`.
3. Repartition to `min(ddf.npartitions, 50)` immediately after read.
4. Apply `_add_derived_keys` via `map_partitions`.
5. Apply `_drop_duplicates` via `map_partitions`.
6. Apply `_handle_negative_production` via `map_partitions`.
7. Apply `_flag_unit_outliers` via `map_partitions`.
8. Apply `_standardize_strings` via `map_partitions`.
9. Apply `_cast_well_status` via `map_partitions`.
10. Apply `_filter_date_range` via `map_partitions`.
11. Collect cleaning statistics using `dask.compute()` — batch all stat computations in a single `dask.compute()` call (do not call `.compute()` sequentially on independent results).
12. Write cleaning report via `build_cleaning_report`.
13. Final sequence (must follow this exact order):
    a. `ddf.set_index("well_id")` — triggers distributed shuffle.
    b. `ddf.repartition(npartitions=max(1, ddf.npartitions // 10))`.
    c. `ddf.map_partitions(_sort_partition_by_date, meta=ddf._meta)`.
14. Write to `data/interim/cogcc_cleaned.parquet` using `.to_parquet(write_index=True)`.
15. Log completion summary: rows written, partitions written.
16. Close Client if created by this stage.

**Parameters:**
- `config`: full config dict

**Returns:** `None`

**Error handling:**
- Input Parquet not found → raise `FileNotFoundError`.
- Computation errors → log and re-raise as `RuntimeError`.

**Dependencies:** `dask`, `dask.distributed`, `pandas`, `numpy`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` Mock all `map_partitions` steps; assert `to_parquet` is called with the correct output path.
- `@pytest.mark.unit` **TR-17** Assert that the Dask graph is lazy — the function returns without calling `.compute()` except in the stats batch and the final `.to_parquet()`.
- `@pytest.mark.unit` **TR-15** Run `run_transform` on the same synthetic input twice (using a temp dir); assert the output row count after the second run equals the row count after the first run.
- `@pytest.mark.integration` Run `run_transform` on real data; assert `data/interim/cogcc_cleaned.parquet` exists and the number of output Parquet files is ≤ 50.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task T-11: Domain correctness and technical integrity tests (TR-01 through TR-16)

**Test file:** `tests/test_transform.py`

**Description:**
Implement all domain and technical test cases from `test-requirements.xml` that apply to the transform stage.

**Test cases:**

- `@pytest.mark.unit` **TR-01** Physical bound validation: create a synthetic DataFrame with negative `OilProduced`, `GasProduced`, `WaterProduced`; assert all are set to `pd.NA` after transform and flag columns are `True`.
- `@pytest.mark.unit` **TR-02** Unit consistency: create a row with `OilProduced=55000.0`; assert `OilProduced_unit_flag=True` and the value is still `55000.0`.
- `@pytest.mark.unit` **TR-04** Well completeness: given a synthetic well with 12 monthly records from Jan–Dec 2021, assert the output contains exactly 12 records for that well.
- `@pytest.mark.unit` **TR-05** Zero production preservation: given `OilProduced=0.0`, assert the value remains `0.0` (not `pd.NA`) in the cleaned output.
- `@pytest.mark.unit` **TR-11** Data integrity: apply all transform steps to a synthetic partition; for a randomly selected well/month, assert the `OilProduced` value in the output matches the input (when no cleaning rules triggered).
- `@pytest.mark.unit` **TR-12** Data cleaning validation: apply `_handle_negative_production` and `_cast_well_status` to a synthetic partition; assert null counts and dtypes are correct.
- `@pytest.mark.unit` **TR-13** Partition correctness: for a synthetic Dask DataFrame with `well_id` as the index, assert that after `set_index("well_id")` and `repartition`, a sample of partitions each contain rows for a limited set of `well_id` values (verifying the shuffle grouped data correctly).
- `@pytest.mark.unit` **TR-14** Schema stability: create two synthetic partitions with different wells; assert both have identical columns and dtypes after `map_partitions`.
- `@pytest.mark.unit` **TR-15** Row count reconciliation: assert output rows ≤ input rows. Apply dedup step twice, assert idempotency.
- `@pytest.mark.unit` **TR-16** Sort stability: given a synthetic partition with out-of-order dates, assert after `_sort_partition_by_date` dates are ascending and no rows are lost.
- `@pytest.mark.integration` **TR-11** Spot-check 5 randomly chosen well/month combinations from `data/raw/` CSV; assert `OilProduced` in `data/interim/cogcc_cleaned.parquet` matches the source (accounting for negative→NA cleaning).
- `@pytest.mark.integration` **TR-12** Read `data/interim/cogcc_cleaned.parquet`; assert `production_date` dtype is `datetime64[ns]`, `OilProduced` dtype is `Float64`, `well_id` dtype is string.
- `@pytest.mark.integration` **TR-13** Read all Parquet partitions in `data/interim/cogcc_cleaned.parquet`; assert that `well_id` index values within any single partition are consistent (no partition spans unexpectedly many wells).
- `@pytest.mark.integration` **TR-14** Read schema from 3 different partition files in `data/interim/cogcc_cleaned.parquet`; assert all have identical column names and dtypes.

**Definition of done:** All test cases implemented and passing, integration tests marked with `@pytest.mark.integration`, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.
