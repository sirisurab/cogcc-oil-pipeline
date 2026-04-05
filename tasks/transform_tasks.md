# Transform Stage — Task Specifications

## Overview

The transform stage reads the interim Parquet files produced by ingest, applies comprehensive
data cleaning, structural normalisation, and enrichment, and writes the output to `data/processed/`
as clean Parquet files partitioned by `report_year` and `county_code`.

This stage covers:
1. Renaming columns to `snake_case`
2. Constructing a composite well identifier (`well_id`)
3. Constructing a `production_date` column from `report_year` + `report_month`
4. Deduplication
5. Handling missing values and outliers with domain-aware rules
6. Validating and flagging invalid production records
7. Ensuring zero-production records are preserved (not confused with nulls)
8. Filling gaps in a well's monthly time series
9. Writing consolidated cleaned Parquet files

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/transform.py`
**Test file:** `tests/test_transform.py`

---

## Column Renaming Map (snake_case)

The following renames must be applied exactly. The left column is the raw name from the ingest
stage; the right column is the cleaned name used in all downstream stages.

| Raw Column Name    | Cleaned Column Name |
|--------------------|---------------------|
| ApiCountyCode      | county_code         |
| ApiSequenceNumber  | api_seq             |
| ReportYear         | report_year         |
| ReportMonth        | report_month        |
| OilProduced        | oil_bbl             |
| OilSales           | oil_sales_bbl       |
| GasProduced        | gas_mcf             |
| GasSales           | gas_sales_mcf       |
| FlaredVented       | gas_flared_mcf      |
| GasUsedOnLease     | gas_lease_mcf       |
| WaterProduced      | water_bbl           |
| DaysProduced       | days_produced       |
| Well               | well_name           |
| OpName             | operator_name       |
| OpNum              | operator_num        |
| DocNum             | doc_num             |

Any columns present in the interim data that are NOT in the above map must be retained as-is
(not dropped, not renamed).

---

## Domain Constraints (informed by oil-and-gas domain expert)

- **Unit convention:** `oil_bbl` is in barrels (BBL), `gas_mcf` is in thousand cubic feet (MCF),
  `water_bbl` is in barrels (BBL). These units must not be changed; they are the standard ECMC
  reporting units.
- **Physical bounds (TR-01):**
  - `oil_bbl`, `gas_mcf`, `water_bbl` cannot be negative. Any negative value is a data error.
  - `days_produced` must be between 0 and 31 (inclusive).
  - These physical bound violations must be flagged: rows violating them get `is_valid = False`.
- **Unit error detection (TR-02):**
  - A single-well monthly `oil_bbl` value > 50,000 BBL is almost certainly a unit error
    (e.g., value reported in gallons or was entered as MBBLs). Flag these rows `is_valid = False`
    with a `flag_reason` label of `"oil_volume_outlier"`.
  - A single-well monthly `gas_mcf` value > 500,000 MCF per month is similarly suspect. Flag
    with `"gas_volume_outlier"`.
- **Zero vs. null distinction (TR-05):**
  - A zero in `oil_bbl`, `gas_mcf`, or `water_bbl` is a valid measurement (shut-in well or dry
    period). Zeros must remain as `0.0` in processed output. They must never be replaced with NaN.
  - NaN/null values in production columns indicate missing data. They must remain as NaN (not
    filled with 0) unless the gap-filling task explicitly decides otherwise.
- **Well time-series completeness (TR-04):**
  - After cleaning, each well should have a contiguous monthly record from its first to last
    production date. Any missing months must be inserted as rows with production columns set to
    NaN (indicating missing data, not zero production). This ensures downstream ML time-series
    operations have a regular grid.

---

## Design Decisions & Constraints

- Use `dask.dataframe` for all DataFrame operations.
- All `map_partitions` calls must use an explicit `meta=` argument with `pd.StringDtype()` for
  string columns — never `"object"` dtype.
- After reading the interim Parquet dataset, immediately repartition to `min(npartitions, 50)`.
- Never call `.compute()` inside any transform function (TR-17). The only permitted `.compute()`
  call is in the orchestrator `run_transform()` for the row-count estimate used to size output
  partitions.
- Output: write to `data/processed/` using `ddf.repartition(npartitions=N).to_parquet(...)` where
  `N = max(1, total_rows // 500_000)`, capped at 50. Do not use `partition_on=` with
  high-cardinality columns (e.g., `well_id`).
- All logging must use Python's `logging` module.

---

## Task 01: Implement column renamer

**Module:** `cogcc_pipeline/transform.py`
**Function:** `rename_columns(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Apply the snake_case rename map to the Dask DataFrame. Any columns not in the map are retained
unchanged. Return the renamed Dask DataFrame without calling `.compute()`.

- Build the rename dict from the Column Renaming Map table above.
- Call `ddf.rename(columns=rename_dict)`.
- Log the number of columns renamed at DEBUG level.
- Return the Dask DataFrame.

**Error handling:** No exceptions raised. If a raw column name is missing from the DataFrame
(e.g., optional column), it is silently skipped in the rename.

**Dependencies:** dask[dataframe], logging

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with raw column names, assert that after
  `rename_columns`, the column `OilProduced` becomes `oil_bbl` and `WaterProduced` becomes
  `water_bbl`.
- `@pytest.mark.unit` — Assert that a column not in the rename map (e.g., `ExtraColumn`) is
  still present in the output with its original name.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement well identifier builder

**Module:** `cogcc_pipeline/transform.py`
**Function:** `build_well_id(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Construct a composite well identifier column `well_id` by combining `county_code` and `api_seq`.
The resulting `well_id` uniquely identifies a well across the dataset and is used as the primary
grouping key in all downstream stages.

- Format: `well_id = county_code.str.zfill(2) + "-" + api_seq.str.zfill(5)`
  (e.g., county_code="5", api_seq="1234" → `well_id="05-01234"`)
- Implement using `map_partitions`. The inner Pandas function must construct the new column and
  return the full DataFrame with `well_id` added.
- `meta=` must include the `well_id` column typed as `pd.StringDtype()`.
- Do not call `.compute()`.

**Error handling:** If `county_code` or `api_seq` is null for a row, `well_id` for that row will
be NaN. Log a warning if more than 1% of rows produce a null `well_id`.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame with `county_code="5"` and `api_seq="1234"`, assert
  `well_id="05-01234"`.
- `@pytest.mark.unit` — Given `county_code="05"` and `api_seq="00123"`, assert `well_id="05-00123"`.
- `@pytest.mark.unit` — Assert `well_id` column dtype is `pd.StringDtype()`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a row with null `county_code`, assert `well_id` is NaN/null for
  that row (not an exception).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement production date constructor

**Module:** `cogcc_pipeline/transform.py`
**Function:** `build_production_date(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Construct a `production_date` column (dtype `datetime64[ns]`) from the `report_year` and
`report_month` integer columns. The date represents the first day of the reporting month.

- Use `map_partitions`. Inside the inner function:
  - Construct a string `YYYY-MM-01` from `report_year` and `report_month`.
  - Parse with `pd.to_datetime(..., format="%Y-%m-%d", errors="coerce")`.
  - Rows where parsing fails (invalid year/month values) will produce NaT — log a warning
    if any NaT values are produced.
- Add the `production_date` column to the DataFrame and return.
- `meta=` must include `production_date` as `datetime64[ns]`.
- Do not call `.compute()`.

**Error handling:** `errors="coerce"` ensures invalid dates produce NaT rather than exceptions.
Log a WARNING if the resulting column contains any NaT values.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given `report_year=2021`, `report_month=3`, assert `production_date`
  equals `pd.Timestamp("2021-03-01")`.
- `@pytest.mark.unit` — Given `report_month=13` (invalid), assert the resulting `production_date`
  is `NaT` and no exception is raised.
- `@pytest.mark.unit` — Assert `production_date` column dtype is `datetime64[ns]`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement deduplication

**Module:** `cogcc_pipeline/transform.py`
**Function:** `deduplicate(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Remove duplicate rows from the Dask DataFrame. A duplicate is defined as a row with identical
values in `well_id`, `report_year`, and `report_month` — the natural primary key of a production
record. When duplicates exist, retain the row with the highest `oil_bbl` (or first occurrence if
oil_bbl is null).

- Use `map_partitions` with an inner function that:
  - Sorts by `oil_bbl` descending (with NaN last).
  - Calls `drop_duplicates(subset=["well_id", "report_year", "report_month"], keep="first")`.
- Log the number of duplicate rows removed (requires calling `.compute()` on the deduplicated
  count vs. original count — this single `.compute()` call is permitted in the orchestrator
  only; inside the function do NOT call `.compute()`).
- Return the deduplicated Dask DataFrame.
- `meta=` must equal `ddf._meta`.

**Error handling:** If `well_id` column is missing, raise `KeyError("well_id column required for
deduplication — run build_well_id first")`.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame with two rows for the same well/year/month (different
  `oil_bbl` values of 100.0 and 200.0), assert the output retains only the row with `oil_bbl=200.0`
  (TR-15 deduplication correctness).
- `@pytest.mark.unit` — Assert row count after deduplication is <= row count before deduplication
  (TR-15).
- `@pytest.mark.unit` — Assert running `deduplicate` twice on the same DataFrame produces the
  same row count as running it once (TR-15 idempotency).
- `@pytest.mark.unit` — Given a DataFrame missing `well_id`, assert `KeyError` is raised.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement physical bounds validator and is_valid flag (TR-01, TR-02)

**Module:** `cogcc_pipeline/transform.py`
**Function:** `apply_validity_flags(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Add two columns to every row: `is_valid` (bool) and `flag_reason` (string). A row is valid unless
it violates a physical constraint or a domain-specific outlier rule.

Invalidity rules (applied in order; a row is flagged with the FIRST matching rule):

| Rule ID | Condition                              | flag_reason               |
|---------|----------------------------------------|---------------------------|
| R01     | `oil_bbl < 0`                          | `"negative_oil"`          |
| R02     | `gas_mcf < 0`                          | `"negative_gas"`          |
| R03     | `water_bbl < 0`                        | `"negative_water"`        |
| R04     | `days_produced < 0` or `> 31`          | `"invalid_days"`          |
| R05     | `production_date` is NaT               | `"invalid_date"`          |
| R06     | `oil_bbl > 50000`                      | `"oil_volume_outlier"`    |
| R07     | `gas_mcf > 500000`                     | `"gas_volume_outlier"`    |
| R08     | `well_id` is null                      | `"missing_well_id"`       |

For rows not matching any rule: `is_valid = True`, `flag_reason = ""` (empty string, not NaN).

- Implement using `map_partitions`. Inside the inner function, build a boolean mask for each rule
  and assign `is_valid` and `flag_reason` accordingly.
- `is_valid` dtype: `bool`
- `flag_reason` dtype: `pd.StringDtype()`
- `meta=` must include `is_valid` as `bool` and `flag_reason` as `pd.StringDtype()`.
- Do not drop invalid rows — flag them and retain. Downstream consumers decide whether to filter.
- Log at INFO level: the total count of invalid rows per flag_reason category (requires a
  `.compute()` in the orchestrator only).

**Error handling:** If required columns (`oil_bbl`, `gas_mcf`, `water_bbl`, `days_produced`,
`production_date`, `well_id`) are missing, raise `KeyError` listing the missing columns.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a row with `oil_bbl = -5.0`, assert `is_valid = False` and
  `flag_reason = "negative_oil"` (TR-01).
- `@pytest.mark.unit` — Given a row with `oil_bbl = 75000.0`, assert `is_valid = False` and
  `flag_reason = "oil_volume_outlier"` (TR-02).
- `@pytest.mark.unit` — Given a row with `oil_bbl = 100.0`, `gas_mcf = 500.0`, `water_bbl = 50.0`,
  `days_produced = 28`, valid `production_date`, and non-null `well_id`, assert `is_valid = True`
  and `flag_reason = ""`.
- `@pytest.mark.unit` — Given a row with `days_produced = 35`, assert `is_valid = False` and
  `flag_reason = "invalid_days"` (TR-01).
- `@pytest.mark.unit` — Assert rows with `oil_bbl = 0.0` (shut-in well) are NOT flagged as
  invalid (TR-05: zero is a valid measurement, not an error).
- `@pytest.mark.unit` — Assert `flag_reason` column dtype is `pd.StringDtype()`.
- `@pytest.mark.unit` — Assert `is_valid` column dtype is `bool`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Implement zero vs. null preservation check (TR-05)

**Module:** `cogcc_pipeline/transform.py`
**Function:** `preserve_zero_production(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Ensure that zero values in production columns (`oil_bbl`, `gas_mcf`, `water_bbl`) are not
inadvertently replaced with NaN during any cleaning step.

This function is a guard/assertion layer, not a transformation. It should:
- Use `map_partitions` to verify that `0.0` values in production columns remain `0.0` (not NaN).
- If any production column that had a `0.0` value in the incoming data has been replaced with NaN,
  raise `RuntimeError("Zero production values were incorrectly nulled")`.
- To check this: compute a mask where the original value was `0.0` and assert those positions
  are still `0.0`.
- Since this is a guard function and not a transformation, it returns the DataFrame unchanged.
- In practice, this function should be inserted as a validation step in the orchestrator after all
  cleaning operations, NOT called inside other map_partitions.

**Note for the coder:** This function exists to make TR-05 machine-verifiable. The check is done
using `map_partitions` with the original data passed as a captured variable — or by re-reading
the pre-cleaning snapshot.

**Error handling:** `RuntimeError` if zero values have been replaced with NaN.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame with `oil_bbl = [0.0, 100.0, NaN]`, after passing through
  the cleaning pipeline and calling `preserve_zero_production`, assert the `0.0` value is still
  `0.0` (not NaN) (TR-05).
- `@pytest.mark.unit` — Given a DataFrame where `oil_bbl` contains a `0.0` that has been replaced
  with NaN (simulating a bug), assert `RuntimeError` is raised.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Implement monthly time-series gap filler (TR-04)

**Module:** `cogcc_pipeline/transform.py`
**Function:** `fill_monthly_gaps(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Ensure each well has a contiguous monthly record from its first to last `production_date`. Insert
rows for any missing months, with production columns (`oil_bbl`, `gas_mcf`, `water_bbl`,
`oil_sales_bbl`, `gas_sales_mcf`, `gas_flared_mcf`, `gas_lease_mcf`, `water_bbl`) set to NaN
to distinguish missing data from zero production (TR-05).

This is a per-well operation that must be applied using `map_partitions` where the partition
boundary aligns with `well_id`. Because Dask does not guarantee per-well partitioning at this
stage, use `groupby` within each partition and apply a Pandas `reindex` per group.

- Inner Pandas function logic:
  - For each `well_id` group within the partition:
    - Determine the min and max `production_date`.
    - Generate a complete monthly date range using `pd.date_range(start, end, freq="MS")`.
    - Reindex the group on `production_date` to include all months in the range.
    - For newly inserted rows, fill `well_id`, `county_code`, `api_seq`, `report_year`,
      `report_month`, `operator_name`, `operator_num` by forward-filling from the adjacent row.
    - Leave production columns as NaN (do NOT fill with 0).
    - Set `is_valid = True` for newly inserted gap rows (they are structurally valid, just empty).
    - Set `flag_reason = "gap_filled"` for newly inserted rows.
  - Return the re-indexed group.
- `meta=` must match the full schema of `ddf._meta` (same columns and dtypes, plus any added by
  previous steps).
- Do not call `.compute()`.

**Error handling:** If `production_date` contains NaT values, skip gap-filling for that well and
log a warning. If `well_id` is missing, raise `KeyError`.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a well with records for Jan 2021, Mar 2021 (Feb missing), assert
  the output contains a Feb 2021 row with `oil_bbl = NaN` and `flag_reason = "gap_filled"` (TR-04).
- `@pytest.mark.unit` — Given a well with complete monthly records (no gaps), assert the output
  has the same row count as the input.
- `@pytest.mark.unit` — Assert that an existing `oil_bbl = 0.0` row (valid zero production) is
  not changed to NaN during gap filling (TR-05).
- `@pytest.mark.unit` — Given a well with records for Jan 2021 and Dec 2021 (10 missing months),
  assert the output has 12 rows for that well.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Implement unit consistency check (TR-02)

**Module:** `cogcc_pipeline/transform.py`
**Function:** `check_unit_consistency(ddf: dask.dataframe.DataFrame) -> dict`

**Description:**
Validate that production volumes fall within physically realistic ranges after cleaning. Returns
a summary dictionary of any anomalous statistics found. This function is called from the
orchestrator and is intended to produce audit output, not to mutate the DataFrame.

- Compute (via `.compute()` — this is an audit/reporting function, not a transformation):
  - Fraction of non-flagged (`is_valid=True`) wells where monthly `oil_bbl > 50_000`
  - Fraction of non-flagged wells where monthly `gas_mcf > 500_000`
  - Min and max `oil_bbl`, `gas_mcf`, `water_bbl` for valid records
- Return a dict with keys: `oil_max`, `gas_max`, `water_max`, `oil_outlier_fraction`,
  `gas_outlier_fraction`.
- Log the summary at INFO level.
- If `oil_outlier_fraction > 0.01` (more than 1% of valid records are oil outliers), log a WARNING.

**Error handling:** If required columns are missing, raise `KeyError`.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame with 100 valid rows where 2 have `oil_bbl > 50_000`,
  assert `oil_outlier_fraction == 0.02`.
- `@pytest.mark.unit` — Given a DataFrame with all `oil_bbl <= 50_000`, assert
  `oil_outlier_fraction == 0.0`.
- `@pytest.mark.unit` — Assert the returned dict contains all 5 expected keys.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 09: Implement processed Parquet writer

**Module:** `cogcc_pipeline/transform.py`
**Function:** `write_processed_parquet(ddf: dask.dataframe.DataFrame, processed_dir: Path, total_rows_estimate: int) -> None`

**Description:**
Write the cleaned Dask DataFrame to `data/processed/` as consolidated Parquet files.

- `npartitions = max(1, min(total_rows_estimate // 500_000, 50))`.
- Repartition before writing: `ddf.repartition(npartitions=npartitions).to_parquet(str(processed_dir), write_index=False, overwrite=True)`.
- Create `processed_dir` if it does not exist.
- Log the target `npartitions` and path at INFO level.
- Do NOT use `partition_on=` with high-cardinality columns.

**Error handling:** Propagate `OSError` and `pyarrow` write errors after logging.

**Dependencies:** dask[dataframe], pyarrow, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` — Given a synthetic Dask DataFrame and `total_rows_estimate=1_000_000`,
  assert `npartitions = 2`.
- `@pytest.mark.unit` — Given `total_rows_estimate=100`, assert `npartitions = 1`.
- `@pytest.mark.integration` — Write a synthetic Dask DataFrame to a tmp directory; read back
  with `pandas.read_parquet` and assert no error (TR-18).
- `@pytest.mark.integration` — Assert the number of written Parquet files is <= 50.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 10: Implement transform orchestrator

**Module:** `cogcc_pipeline/transform.py`
**Function:** `run_transform(config_path: str = "config.yaml") -> dask.dataframe.DataFrame`

**Description:**
Top-level entry point for the transform stage. Reads interim Parquet, applies all cleaning and
enrichment steps in sequence, writes processed Parquet, and returns the final Dask DataFrame.

Execution order:
1. Load config via `load_config(config_path)`.
2. Read `data/interim/` with `dask.dataframe.read_parquet`.
3. Repartition to `min(npartitions, 50)`.
4. Call `rename_columns`.
5. Call `build_well_id`.
6. Call `build_production_date`.
7. Call `deduplicate`.
8. Call `apply_validity_flags`.
9. Call `fill_monthly_gaps`.
10. Call `preserve_zero_production` (guard check).
11. Call `.shape[0].compute()` once to estimate total rows.
12. Call `check_unit_consistency` (audit, uses `.compute()` internally).
13. Call `write_processed_parquet`.
14. Return the Dask DataFrame.

- Log elapsed time for the full stage at INFO level.
- Log a summary: total rows in, total rows out (post-dedup), invalid row count.

**Error handling:** Propagate errors from all sub-functions. Log the full traceback before
re-raising.

**Dependencies:** dask[dataframe], pathlib, logging, datetime

**Test cases:**
- `@pytest.mark.unit` — Given mocked sub-functions, assert `run_transform` calls them in the
  correct order (use `unittest.mock.call` or patch-and-assert-called).
- `@pytest.mark.integration` — Run `run_transform` against real `data/interim/` (post-ingest);
  assert Parquet files exist in `data/processed/`, are readable, and contain all cleaned columns
  including `well_id`, `production_date`, `is_valid`, `flag_reason`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 11: Domain and technical correctness tests (TR-01 through TR-16)

**Module:** `tests/test_transform.py`

**Description:**
Comprehensive test suite covering all transform-stage test requirements from the authoritative
test requirements specification. All tests use synthetic DataFrames unless marked `integration`.

**Test cases (TR-01 — Physical bound validation):**
- `@pytest.mark.unit` — Given a row with `oil_bbl = -10.0`, assert `is_valid = False`.
- `@pytest.mark.unit` — Given a row with `gas_mcf = -1.0`, assert `is_valid = False`.
- `@pytest.mark.unit` — Given a row with `water_bbl = -0.01`, assert `is_valid = False`.

**Test cases (TR-02 — Unit consistency):**
- `@pytest.mark.unit` — Given a row with `oil_bbl = 60_000.0`, assert `is_valid = False` and
  `flag_reason = "oil_volume_outlier"`.
- `@pytest.mark.unit` — Given a row with `oil_bbl = 49_999.0`, assert `is_valid = True`
  (below threshold is accepted).

**Test cases (TR-04 — Well completeness):**
- `@pytest.mark.unit` — Given a well with records for months 1, 2, 4, 5 of 2021 (month 3
  missing), after `fill_monthly_gaps` assert the well has exactly 5 records for 2021.
- `@pytest.mark.unit` — Assert that for a well with a continuous Jan–Dec 2021 record, the
  count of records is exactly 12.

**Test cases (TR-05 — Zero production handling):**
- `@pytest.mark.unit` — Given a DataFrame containing a row with `oil_bbl = 0.0`, after running
  the full transform pipeline, assert that row still has `oil_bbl = 0.0` (not NaN).

**Test cases (TR-11 — Data integrity):**
- `@pytest.mark.integration` — Read a sample of 50 rows from `data/interim/`, run through the
  transform pipeline, and assert that the `well_id` values match the expected format
  `"{county_code:02d}-{api_seq:05d}"` for all sampled rows.

**Test cases (TR-12 — Data cleaning validation):**
- `@pytest.mark.unit` — Given a DataFrame with null `oil_bbl` and zero `oil_bbl` values, assert
  that nulls remain null and zeros remain zero after `apply_validity_flags`.
- `@pytest.mark.unit` — Assert `production_date` column dtype is `datetime64[ns]` after
  `build_production_date`.
- `@pytest.mark.unit` — Assert `oil_bbl`, `gas_mcf`, `water_bbl` are `float64` after
  `standardise_dtypes` (called in ingest, but schema carries through).

**Test cases (TR-13 — Partition correctness):**
- `@pytest.mark.integration` — Read a sampled processed Parquet file; assert it does not contain
  rows from more than one `well_id` if the partition strategy targets per-well output.
  (Note: the default strategy in this pipeline does NOT use per-well partition_on; this test
  verifies the partition file can be read and contains a coherent subset of the data.)

**Test cases (TR-14 — Schema stability):**
- `@pytest.mark.integration` — Read two different processed Parquet partition files; assert
  their column names and dtypes are identical.

**Test cases (TR-15 — Row count reconciliation):**
- `@pytest.mark.unit` — Assert that deduplication produces row count <= input row count.
- `@pytest.mark.unit` — Assert running `deduplicate` twice produces the same row count as once
  (idempotency).

**Test cases (TR-16 — Sort stability):**
- `@pytest.mark.unit` — Given a single well's records written across two synthetic partitions,
  assert the last `production_date` in the first partition is earlier than the first
  `production_date` in the second partition.

**Test cases (TR-17 — Lazy Dask evaluation):**
- `@pytest.mark.unit` — Assert that `rename_columns`, `build_well_id`, `build_production_date`,
  `deduplicate`, `apply_validity_flags`, `fill_monthly_gaps`, `preserve_zero_production` each
  return a `dask.dataframe.DataFrame` (not `pandas.DataFrame`).

**Test cases (TR-18 — Parquet readability):**
- `@pytest.mark.integration` — After `run_transform`, read every file in `data/processed/`
  using `pandas.read_parquet`; assert no file raises an exception.

**Definition of done:** All tests implemented and passing, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Module-level entry point

**Module:** `cogcc_pipeline/transform.py`

Add a `if __name__ == "__main__":` block that calls `run_transform()` so the stage can be executed
via `python -m cogcc_pipeline.transform`.
