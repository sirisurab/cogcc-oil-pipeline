# Transform Component Tasks

**Pipeline package:** `cogcc_pipeline`
**Module file:** `cogcc_pipeline/transform.py`
**Test file:** `tests/test_transform.py`

## Overview

The transform stage reads the consolidated interim Parquet dataset, performs thorough
data cleaning (deduplication, null handling, outlier removal, type coercion, unit
validation), constructs a composite `well_id` column, casts `WellStatus` to its
categorical dtype, sets the index to `well_id` via a distributed shuffle, repartitions,
sorts within each partition by production date, and writes the processed Parquet dataset
to `data/processed/`. All transformation logic uses vectorised pandas/numpy operations
inside `map_partitions` — never `iterrows`, `itertuples`, or Python for-loops over rows.
The stage uses the Dask distributed scheduler (reuse existing Client or initialise its
own).

---

## Derived Column: `well_id`

`well_id` is the composite API well identifier constructed as:
`"05-" + ApiCountyCode + "-" + ApiSequenceNumber + "-" + ApiSidetrack`

Where `ApiSidetrack` is zero-padded to 2 digits when present; use `"00"` when null.
`well_id` dtype: `pd.StringDtype()`.

---

## Derived Column: `production_date`

Construct a `production_date` column of `datetime64[ns]` from `ReportYear` and
`ReportMonth` using `pd.to_datetime({"year": df["ReportYear"], "month": df["ReportMonth"], "day": 1})`.

---

## Categorical Column: `WellStatus`

`WellStatus` must be cast to `pd.CategoricalDtype(categories=["AB","AC","DG","PA","PR","SI","SO","TA","WO"], ordered=False)`
in the transform stage, after cleaning. Any value not in this list must be replaced with
`pd.NA` before casting. Never cast to categorical before cleaning — raw data may contain
values outside the known set.

---

## Design Decisions and Constraints

- All transformation logic inside `map_partitions` callbacks must use vectorised
  pandas/numpy operations. Preference order: built-in pandas/numpy → `groupby().transform()`
  → `apply(raw=True)` → `apply()`. Never use `iterrows()`, `itertuples()`, or Python
  for-loops over rows.
- String filtering must be done inside a `map_partitions` function, not directly on a
  Dask Series (Dask `.str` accessor is unreliable after repartition/astype).
- The final sequence before writing Parquet must be:
  1. `set_index("well_id")` — triggers distributed shuffle; call exactly once.
  2. `repartition(npartitions=max(10, min(ddf.npartitions, 50)))`.
  3. Sort within each partition by `production_date` using `map_partitions`.
- `meta=` for every `map_partitions` call must be derived by calling the target function
  on `ddf._meta.copy()` (an empty DataFrame with the correct schema). Never manually
  construct the meta column list — it will drift when the function changes.
- Row count after deduplication must be ≤ raw row count (TR-15).
- Zeros in production volumes (`OilProduced`, `GasProduced`, `WaterProduced`) must be
  preserved as `0.0`, not converted to null (TR-05).
- The stage uses the Dask distributed scheduler. Check for an existing Client via
  `distributed.get_client()`; catch `ValueError` to detect no running client and create
  a `LocalCluster` + `Client` from `config["dask"]`.

---

## Task 01: Implement TransformError and well_id builder

**Module:** `cogcc_pipeline/transform.py`
**Classes/Functions:**
- `class TransformError(RuntimeError)` — raised on unrecoverable transform failures
- `_build_well_id(df: pd.DataFrame) -> pd.Series`

**Description:**
`TransformError` is a thin subclass of `RuntimeError`.

`_build_well_id` takes a pandas DataFrame (one partition) and returns a `pd.Series` of
dtype `pd.StringDtype()` containing the composite well identifier. Construction rule:
- `"05-" + ApiCountyCode + "-" + ApiSequenceNumber + "-" + ApiSidetrack.fillna("00")`
- Pad `ApiSidetrack` to exactly 2 digits using `.str.zfill(2)` before substituting `"00"`
  for nulls.
- Use vectorised string concatenation only — no apply, no loop.

**Test cases:**

- `@pytest.mark.unit` — Given a DataFrame row with `ApiCountyCode="01"`,
  `ApiSequenceNumber="12345"`, `ApiSidetrack="00"`, assert `well_id == "05-01-12345-00"`.
- `@pytest.mark.unit` — Given a row with null `ApiSidetrack`, assert `well_id` uses
  `"00"` as the sidetrack component.
- `@pytest.mark.unit` — Given a DataFrame with 100 rows, assert no Python `for` loop is
  used (check that result matches vectorised computation).
- `@pytest.mark.unit` — Assert return dtype is `pd.StringDtype()`.

**Definition of done:** `TransformError` and `_build_well_id` implemented, all test
cases pass, ruff and mypy report no errors, `requirements.txt` updated with all
third-party packages imported in this task.

---

## Task 02: Implement production_date column builder

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_build_production_date(df: pd.DataFrame) -> pd.Series`

**Description:**
Takes a pandas DataFrame partition and returns a `pd.Series` of `datetime64[ns]`
representing the first day of the production month. Uses:
`pd.to_datetime({"year": df["ReportYear"], "month": df["ReportMonth"], "day": 1})`

Returns the resulting Series. If any row has an invalid year/month combination, those
rows produce `NaT`.

**Test cases:**

- `@pytest.mark.unit` — Given `ReportYear=2021`, `ReportMonth=6`, assert result is
  `Timestamp("2021-06-01")`.
- `@pytest.mark.unit` — Given `ReportYear=2020`, `ReportMonth=12`, assert result is
  `Timestamp("2020-12-01")`.
- `@pytest.mark.unit` — Given an invalid month (e.g. `ReportMonth=13`), assert the
  result is `NaT` (not an exception).
- `@pytest.mark.unit` — Assert return dtype is `datetime64[ns]`.

**Definition of done:** `_build_production_date` implemented, all test cases pass, ruff
and mypy report no errors, `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 03: Implement deduplication

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_deduplicate(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Removes duplicate rows from a partition. Deduplication key: `["ApiCountyCode",
"ApiSequenceNumber", "ReportYear", "ReportMonth"]`. When duplicates are found, keep the
row with the most recent `AcceptedDate` (sort descending by `AcceptedDate`, then keep
first). If `AcceptedDate` is null for all duplicates, keep the first occurrence. Uses
vectorised sort + `drop_duplicates` — no apply, no loop.

Idempotency requirement: calling `_deduplicate` twice on the same DataFrame must produce
the same result as calling it once (TR-15).

**Test cases:**

- `@pytest.mark.unit` — Given a DataFrame with two rows sharing the same
  `ApiCountyCode`, `ApiSequenceNumber`, `ReportYear`, `ReportMonth` but different
  `AcceptedDate`, assert only the row with the later `AcceptedDate` is retained.
- `@pytest.mark.unit` — Given a DataFrame with no duplicates, assert row count is
  unchanged.
- `@pytest.mark.unit` — Given a DataFrame with all-null `AcceptedDate` for duplicates,
  assert only one row per key is retained.
- `@pytest.mark.unit` — Assert idempotency: apply `_deduplicate` twice and assert the
  result equals applying it once (TR-15).
- `@pytest.mark.unit` — Assert row count after deduplication ≤ row count before (TR-15).

**Definition of done:** `_deduplicate` implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 04: Implement production volume cleaner

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_clean_production_volumes(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Cleans the three primary production volume columns: `OilProduced`, `GasProduced`,
`WaterProduced`. Rules applied using vectorised `numpy`/`pandas` operations only:

1. **Negative values** — Replace any negative value with `pd.NA`. Physical production
   volumes cannot be negative (TR-01). Use `numpy.where` or boolean masking.
2. **Zero preservation** — A value of exactly `0.0` is a valid measurement (TR-05). Do
   NOT replace zeros with null.
3. **Oil unit outlier check (TR-02)** — Single-well monthly `OilProduced` values above
   50,000 BBL are almost certainly a unit error. Replace with `pd.NA` and log a
   `WARNING` with the `well_id` and the erroneous value.
4. **Days consistency** — If `DaysProduced` is 0 and any of the three volumes are
   non-zero, log a `WARNING` (do not null the production — this is a data quality flag
   only).
5. **Return** the cleaned DataFrame.

**Test cases:**

- `@pytest.mark.unit` — Given a row with `OilProduced=-100.0`, assert it becomes
  `pd.NA` (TR-01).
- `@pytest.mark.unit` — Given a row with `OilProduced=0.0`, assert it remains `0.0`
  (not null) (TR-05).
- `@pytest.mark.unit` — Given a row with `OilProduced=75000.0`, assert it becomes
  `pd.NA` and a `WARNING` is logged (TR-02).
- `@pytest.mark.unit` — Given a row with `GasProduced=-1.0`, assert it becomes `pd.NA`.
- `@pytest.mark.unit` — Given a row with `WaterProduced=0.0` and `DaysProduced=0`,
  assert no exception and both values are preserved as-is (zero is valid).
- `@pytest.mark.unit` — Given a row with `DaysProduced=0` and `OilProduced=100.0`,
  assert a `WARNING` is logged and `OilProduced` remains `100.0`.

**Definition of done:** `_clean_production_volumes` implemented, all test cases pass,
ruff and mypy report no errors, `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 05: Implement WellStatus categorical caster

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_cast_well_status(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Casts the `WellStatus` column from `pd.StringDtype()` to
`pd.CategoricalDtype(categories=["AB","AC","DG","PA","PR","SI","SO","TA","WO"], ordered=False)`.
Before casting:
1. Replace any value not in the allowed category list with `pd.NA` using `numpy.where`
   or `isin` + boolean mask — no loop.
2. Then cast the column.

**Test cases:**

- `@pytest.mark.unit` — Given a DataFrame with `WellStatus` values `["AC", "SI", "PA"]`,
  assert the cast column dtype is `CategoricalDtype` with the declared categories.
- `@pytest.mark.unit` — Given a value `"XX"` (not in the allowed list), assert it
  becomes `pd.NA` after casting.
- `@pytest.mark.unit` — Given a null `WellStatus`, assert it remains null (not
  erroneously assigned a category).
- `@pytest.mark.unit` — Assert that unknown category values do not propagate silently —
  the number of non-null values in the output must equal the number of rows with a
  recognised status code in the input.

**Definition of done:** `_cast_well_status` implemented, all test cases pass, ruff and
mypy report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 06: Implement partition-level transform orchestrator (map_partitions callback)

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_transform_partition(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Applied via `map_partitions` to each Dask partition. Executes the full per-partition
transformation chain in order:
1. `df = _deduplicate(df)`
2. `df = _clean_production_volumes(df)`
3. `df["well_id"] = _build_well_id(df)` — insert as first column
4. `df["production_date"] = _build_production_date(df)` — insert after `well_id`
5. `df = _cast_well_status(df)`
6. Drop `ReportYear` and `ReportMonth` from the output (superseded by `production_date`).
7. Return the transformed DataFrame.

**Meta construction:** The `meta=` argument for `ddf.map_partitions(_transform_partition, meta=meta)`
must be derived by calling `_transform_partition(ddf._meta.copy())`. Do not manually
specify column names or dtypes for meta.

**Test cases:**

- `@pytest.mark.unit` — Given a minimal synthetic partition DataFrame with all required
  columns, assert the output contains `well_id` and `production_date` columns.
- `@pytest.mark.unit` — Assert the output does NOT contain `ReportYear` or `ReportMonth`.
- `@pytest.mark.unit` — Assert `well_id` dtype is `pd.StringDtype()`.
- `@pytest.mark.unit` — Assert `production_date` dtype is `datetime64[ns]`.
- `@pytest.mark.unit` — Assert `WellStatus` dtype is `CategoricalDtype`.
- `@pytest.mark.unit` — Meta consistency (TR-23): call `_transform_partition` on
  `ddf._meta.copy()` and assert column names, column order, and dtypes match the `meta=`
  argument passed to `map_partitions`.

**Definition of done:** `_transform_partition` implemented, all test cases pass, ruff and
mypy report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 07: Implement well completeness checker

**Module:** `cogcc_pipeline/transform.py`
**Function:** `check_well_completeness(ddf: dask.dataframe.DataFrame) -> pd.DataFrame`

**Description:**
Checks for unexpected gaps in each well's monthly production record (TR-04). Computes
for each well:
- `first_date`: minimum `production_date`
- `last_date`: maximum `production_date`
- `expected_months`: months between first and last date (inclusive)
- `actual_months`: count of distinct `production_date` values
- `gap_count`: `expected_months - actual_months`

Uses `ddf.groupby("well_id")["production_date"].agg(["min", "max", "count"])` followed
by `.compute()` to get a summary pandas DataFrame. Returns this summary. Logs a
`WARNING` for each well with `gap_count > 0`.

Note: this is a diagnostic/audit function — it does not modify the data. It calls
`.compute()` once on the aggregated summary (small result), not on the full dataset.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Given a synthetic well with 12 consecutive monthly records (Jan
  2021 – Dec 2021), assert `gap_count == 0` (TR-04).
- `@pytest.mark.unit` — Given a well missing the March 2021 record (11 out of 12
  months), assert `gap_count == 1` and a `WARNING` is logged (TR-04).
- `@pytest.mark.unit` — Given two wells, assert the summary DataFrame has two rows.

**Definition of done:** `check_well_completeness` implemented, all test cases pass, ruff
and mypy report no errors, `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 08: Implement data integrity spot-checker

**Module:** `cogcc_pipeline/transform.py`
**Function:** `check_data_integrity(interim_ddf: dask.dataframe.DataFrame, processed_ddf: dask.dataframe.DataFrame, sample_n: int = 20) -> None`

**Description:**
Spot-checks production volumes from interim to processed data for a random sample of
wells (TR-11). Steps:
1. Get a list of distinct `well_id` values from `processed_ddf` by computing
   `processed_ddf["well_id"].unique().compute()`.
2. Draw a random sample of `min(sample_n, len(well_ids))` well IDs.
3. For each sampled well, compare `OilProduced` sum from interim vs processed using
   `dask.compute()` (batch both at once — not two sequential `.compute()` calls).
4. Assert the difference is within a tolerance of 1e-3 BBL (floating-point rounding only).
5. Log a `WARNING` for any well with a discrepancy beyond tolerance.
6. Log `INFO` with the number of wells checked and the number with discrepancies.

**Non-negotiable:** Batch both interim and processed compute calls using `dask.compute()`,
never call `.compute()` sequentially (see non-negotiable compute constraint).

**Test cases:**

- `@pytest.mark.unit` — Given matching synthetic interim and processed Dask DataFrames,
  assert no warnings are logged (TR-11).
- `@pytest.mark.unit` — Given a processed DataFrame where one well's `OilProduced` sum
  differs from interim by more than 1e-3, assert a `WARNING` is logged.

**Definition of done:** `check_data_integrity` implemented, all test cases pass, ruff
and mypy report no errors, `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 09: Implement transform stage orchestrator

**Module:** `cogcc_pipeline/transform.py`
**Function:** `run_transform(config: dict) -> dask.dataframe.DataFrame`

**Description:**
Orchestrates the full transform stage:
1. Detect or initialise Dask distributed Client (same pattern as ingest: try
   `distributed.get_client()`, catch `ValueError`, create `LocalCluster` + `Client`).
2. Read interim Parquet: `ddf = dd.read_parquet(config["transform"]["interim_dir"] + "/production.parquet", engine="pyarrow")`.
3. Apply per-partition transform: `ddf = ddf.map_partitions(_transform_partition, meta=meta)`
   where `meta = _transform_partition(ddf._meta.copy())`.
4. Filter using `map_partitions` (not `.str` accessor directly): remove rows where
   `well_id` is null or empty.
5. Call `check_well_completeness(ddf)` (diagnostic only — does not block pipeline).
6. **Final sequence (non-negotiable order):**
   a. `ddf = ddf.set_index("well_id")` — distributed shuffle; exactly once.
   b. `ddf = ddf.repartition(npartitions=max(10, min(ddf.npartitions, 50)))`.
   c. Sort within each partition by `production_date` using `map_partitions` with a
      lambda that calls `df.sort_values("production_date")`.
7. Write processed Parquet: `ddf.to_parquet(config["transform"]["processed_dir"] + "/production.parquet", engine="pyarrow", write_index=True, overwrite=True)`.
8. Call `check_data_integrity` (audit step).
9. Return the Dask DataFrame (do NOT call `.compute()` on the full dataset).

**Non-negotiable:** `set_index` is called exactly once, at step 6a. Never call it before
cleaning or in the middle of the transform chain. The `meta=` for map_partitions must be
derived from calling the function on `ddf._meta.copy()`.

**Test cases:**

- `@pytest.mark.unit` — Mock all sub-functions; assert `run_transform` returns a
  `dask.dataframe.DataFrame` (TR-17).
- `@pytest.mark.unit` — Assert `.compute()` is never called on the full dataset inside
  `run_transform` (TR-17).
- `@pytest.mark.integration` — Run `run_transform(config)` against `data/interim/`; assert
  `data/processed/production.parquet/` is created and readable (TR-18).

**Data cleaning validation (TR-12):**

- `@pytest.mark.integration` — After `run_transform`, read a sample partition from
  `data/processed/production.parquet/`. Assert: no column uses `object` dtype (use
  `pd.StringDtype()` or `CategoricalDtype`); `production_date` is `datetime64[ns]`;
  `OilProduced` is `Float64`; `WellStatus` is `CategoricalDtype`.

**Row count reconciliation (TR-15):**

- `@pytest.mark.integration` — Assert processed row count ≤ interim row count. Run
  `run_transform` twice and assert the processed row count is identical both times
  (idempotency).

**Sort stability (TR-16):**

- `@pytest.mark.unit` — Given a synthetic Dask DataFrame with two partitions for the
  same well, after the sort step assert the last `production_date` in partition 0 is
  earlier than the first `production_date` in partition 1.

**Zero preservation (TR-05):**

- `@pytest.mark.unit` — Given a partition with `OilProduced=0.0`, assert `0.0` appears
  in the processed output (not `pd.NA`).

**Schema stability (TR-14):**

- `@pytest.mark.integration` — Read two different partition files from
  `data/processed/production.parquet/`; assert their column names and dtypes are
  identical.

**Partition correctness (TR-13):**

- `@pytest.mark.integration` — After `run_transform`, read each Parquet partition file
  individually and assert the index (well_id) within each partition contains only one
  unique value (since `set_index("well_id")` was used — rows for a given well are
  co-located after the shuffle).

**Definition of done:** `run_transform` implemented, all test cases pass (including TR-05,
TR-11, TR-12, TR-13, TR-14, TR-15, TR-16, TR-17, TR-18, TR-23), ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.
