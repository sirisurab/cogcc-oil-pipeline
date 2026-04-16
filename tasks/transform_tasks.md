# Transform Stage — Task Specifications

## Stage Overview

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/transform.py`
**Test file:** `tests/test_transform.py`
**Input directory:** `data/interim/` (Parquet, output of ingest)
**Output directory:** `data/processed/` (Parquet, ML-ready)
**Scheduler:** Dask distributed scheduler (CPU-bound stage — ADR-001)

The transform stage reads the interim Parquet produced by ingest, cleans and validates
data, derives a synthetic `production_date` column, re-indexes on the entity column,
sorts by `production_date` within partitions, casts categoricals, and writes
analysis-ready Parquet to `data/processed/`.

### Boundary contract received from ingest
- All columns carry data-dictionary dtypes (no re-casting needed).
- Column names match the data dictionary exactly.
- Nullable absent columns are filled with NA (no absent nullable column handling needed).
- Partitioned to `max(10, min(n, 50))` — no re-partitioning needed before processing.
- Sort: unsorted — transform must not assume any sort order on input.

### Boundary contract delivered by transform (→ features)
- Entity-indexed on `well_id` (see TRN-01 for derivation).
- Sorted by `production_date` within each partition.
- Categoricals cast to declared category sets.
- Invalid values replaced with the appropriate null sentinel (not dropped).
- Partitioned to `max(10, min(n, 50))`.

### Key design constraints (from ADRs)
- ADR-001: Use Dask distributed scheduler. Reuse the existing `Client` — do not
  initialize a new cluster inside transform functions.
- ADR-002: All operations must be vectorized. No row iteration. No per-entity Python
  loops. All grouped operations use Dask/pandas grouped vectorized transforms.
- ADR-003: Dtype changes are prohibited — the data dictionary is the only source of
  truth for types. Do not re-infer types from the data.
- ADR-004: `repartition()` must be the last operation before `to_parquet()`. No
  operations after repartition.
- ADR-005: Keep the task graph lazy until the final write. No `.compute()` calls
  inside transform functions.

---

## Task TRN-01: Derive entity key and production date

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_add_derived_columns(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level function (called via `map_partitions`) that adds two derived columns
to the input DataFrame. Must be pure and stateless.

### Derived columns:

**`well_id`** — The canonical well entity key used for indexing, grouping, and joining
throughout the remainder of the pipeline. Derived by zero-padding and concatenating
the three API number components:
- `ApiCountyCode`: zero-padded to 2 characters
- `ApiSequenceNumber`: zero-padded to 5 characters
- `ApiSidetrack`: zero-padded to 2 characters (use `"00"` when `ApiSidetrack` is NA)
- Concatenated as: `f"{county:>02}{seq:>05}{sidetrack:>02}"`
- The result is a 9-character string (e.g., `"010123400"` for county `01`, sequence
  `01234`, sidetrack `00`).
- All three component columns are `string` dtype — use pandas string operations,
  not Python string formatting in a row-wise apply.

**`production_date`** — A `datetime64[ns]` column representing the first day of the
production reporting period. Derived as:
`pd.to_datetime({"year": df["ReportYear"], "month": df["ReportMonth"], "day": 1})`

### Implementation constraint:
Both derivations must be fully vectorized using pandas string and datetime
constructors — no `apply`, no `iterrows`, no per-row lambda.

**Meta derivation:**
When calling `ddf.map_partitions(_add_derived_columns, meta=meta)`, derive `meta` by
calling `_add_derived_columns(ddf._meta.copy())`. Never construct meta manually
(ADR-003, TR-23).

**Dependencies:** `pandas`

**Test cases:**
- Given a DataFrame with `ApiCountyCode="01"`, `ApiSequenceNumber="00123"`,
  `ApiSidetrack="00"`, assert `well_id == "010012300"`.
- Given a DataFrame with `ApiCountyCode="5"` (not zero-padded in source),
  `ApiSequenceNumber="456"`, `ApiSidetrack=pd.NA`, assert `well_id == "050045600"`.
- Given `ReportYear=2021`, `ReportMonth=6`, assert `production_date == pd.Timestamp("2021-06-01")`.
- Assert `production_date` dtype is `datetime64[ns]`.
- Assert `well_id` dtype is `string` or `object` (str).
- TR-23: Call `_add_derived_columns` on an empty DataFrame with the input schema and
  assert the output column list and dtypes match what would be passed as `meta` in
  `map_partitions`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task TRN-02: Production volume validation and cleaning

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_clean_production_volumes(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level cleaning function (called via `map_partitions`) that enforces physical
and domain validity rules on production volume columns. Must be pure and stateless.

### Columns subject to cleaning:
`OilProduced`, `OilSales`, `GasProduced`, `GasSales`, `GasUsedOnLease`,
`FlaredVented`, `WaterProduced`

### Cleaning rules (applied in order):

1. **Negative value replacement (TR-01 physical bound):**
   For each of the listed columns, replace any negative value with `pd.NA`.
   A negative production volume is physically impossible — it is a data entry error
   or a unit conversion mistake, not a valid measurement.
   Zero values must NOT be replaced — zero is a valid measurement (TR-05).

2. **Unit range outlier flagging (TR-02 unit consistency):**
   For `OilProduced` only: if a value exceeds 50,000 BBL/month for a single well,
   flag the row by setting a new boolean column `oil_unit_flag = True` for that row.
   For all other rows set `oil_unit_flag = False`. Do not null out the value — only flag.
   The column `oil_unit_flag` must be added to the DataFrame by this function.

3. **Pressure column negative replacement (TR-01):**
   Replace negative values in `GasPressureTubing`, `GasPressureCasing`,
   `WaterPressureTubing`, `WaterPressureCasing` with `pd.NA`.

4. **DaysProduced range validation:**
   `DaysProduced` must be between 0 and 31 inclusive. Values outside this range
   → replace with `pd.NA`.

### Zero vs null preservation (TR-05):
Zeros in the input must remain as zeros (not converted to null). Only negative values
are replaced with `pd.NA`.

**Meta derivation:**
Derive `meta` by calling `_clean_production_volumes(ddf._meta.copy())` before the
`map_partitions` call (ADR-003, TR-23).

**Dependencies:** `pandas`

**Test cases (TR-01, TR-02, TR-05):**
- Given a row with `OilProduced=-100.0`, assert the cleaned value is `pd.NA`.
- Given a row with `OilProduced=0.0`, assert the cleaned value remains `0.0` (not NA).
- Given a row with `OilProduced=51000.0`, assert `oil_unit_flag=True` and value is
  unchanged (not nulled).
- Given a row with `OilProduced=100.0`, assert `oil_unit_flag=False`.
- Given a row with `GasProduced=-5.0`, assert the cleaned value is `pd.NA`.
- Given a row with `WaterProduced=0.0`, assert the cleaned value remains `0.0`.
- Given a row with `GasPressureTubing=-10.0`, assert the cleaned value is `pd.NA`.
- Given a row with `DaysProduced=35`, assert the cleaned value is `pd.NA`.
- Given a row with `DaysProduced=0`, assert the cleaned value remains `0`.
- TR-23: Call on an empty DataFrame and assert output schema matches `meta`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task TRN-03: Operator name normalization

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_normalize_operator_names(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level function (called via `map_partitions`) that standardizes `OpName`
values to improve grouping consistency. Must be pure and stateless.

### Normalization rules (applied in order to `OpName`):
1. Strip leading and trailing whitespace.
2. Collapse internal multiple spaces to a single space.
3. Convert to uppercase.
4. Remove common legal-form suffixes to normalize variants:
   Remove (case-insensitively, then re-upper) the following trailing tokens if present:
   `, LLC`, `, INC`, `, INC.`, `, LP`, `, LTD`, `, CORP`, `, CO.`, `, CO`
   after stripping they must also be stripped of any remaining trailing commas/spaces.
5. If `OpName` is `pd.NA` or empty after normalization → leave as `pd.NA`.

The result column replaces `OpName` in place (same column name, modified values).

### Implementation constraint:
Use pandas string accessor operations (`.str.strip()`, `.str.upper()`, `.str.replace()`,
`.str.contains()`, `.str.extract()`) — no per-row apply or lambda.

**Dependencies:** `pandas`

**Test cases:**
- Assert `"  Devon Energy Corp , LLC  "` normalizes to `"DEVON ENERGY CORP"`.
- Assert `"Continental Resources, Inc."` normalizes to `"CONTINENTAL RESOURCES"`.
- Assert `"ENERPLUS RESOURCES"` normalizes to `"ENERPLUS RESOURCES"` (no change).
- Assert `pd.NA` remains `pd.NA` after normalization.
- Assert `""` (empty string) becomes `pd.NA`.
- Assert `"PDC  Energy,  LP"` (double space) normalizes to `"PDC ENERGY"`.
- TR-23: Call on an empty DataFrame and assert output schema matches `meta`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task TRN-04: Deduplication

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_deduplicate(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level deduplication function (called via `map_partitions`). Removes exact
duplicate rows and handles the COGCC revision pattern where a later record supersedes
an earlier one for the same well-month combination.

### Deduplication rules (applied in order):

1. **Exact duplicate removal:** Drop rows where all column values are identical.
   Use `df.drop_duplicates()`.

2. **Well-month revision deduplication:** For the same `(well_id, ReportYear, ReportMonth)`
   combination where multiple rows exist and the `Revised` column is `True` on some:
   - Keep only the row with `Revised=True` if present (supersedes the original).
   - If multiple revised rows exist for the same key, keep the one with the latest
     `AcceptedDate`. If `AcceptedDate` is NA, keep the last row in sort order.
   - Use `sort_values` + `drop_duplicates(keep="last")` — do not iterate over groups.

### Row count constraint (TR-15):
After deduplication, `len(output_df) <= len(input_df)` must hold. The function must
never increase the row count.

**Meta derivation:**
Derive `meta` by calling `_deduplicate(ddf._meta.copy())` before `map_partitions`
(TR-23).

**Dependencies:** `pandas`

**Test cases (TR-15):**
- Given a DataFrame with 5 identical rows, assert deduplication returns 1 row.
- Given a DataFrame with 2 rows for the same `(well_id, ReportYear, ReportMonth)`,
  one with `Revised=False` and one with `Revised=True`, assert only the `Revised=True`
  row is kept.
- Given a DataFrame with 2 revised rows for the same key and different `AcceptedDate`,
  assert the row with the later `AcceptedDate` is kept.
- Assert output row count is always <= input row count (test with 100 random rows
  where 30% are duplicates).
- TR-15 idempotency: Apply `_deduplicate` twice on the same input and assert the
  output after the second run is identical to the output after the first.
- TR-23: Call on an empty DataFrame and assert output schema matches `meta`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task TRN-05: Well completeness gap detection

**Module:** `cogcc_pipeline/transform.py`
**Function:** `check_well_completeness(ddf: dd.DataFrame) -> pd.DataFrame`

**Description:**
Compute a summary DataFrame reporting wells with unexpected gaps in their monthly
production record. This is a diagnostic/QC function — it does NOT filter or modify
the production data. It returns a pandas DataFrame (i.e., it calls `.compute()` once
on a grouped aggregation) suitable for logging or writing to a QC report file.

### Logic:
For each unique `well_id`:
1. Find the minimum and maximum `production_date`.
2. Compute the expected number of months: `(max_year * 12 + max_month) - (min_year * 12 + min_month) + 1`.
3. Compute the actual number of records for that well.
4. If `actual_count < expected_count`, the well has gaps — include it in the output.

### Output schema (pandas DataFrame):
Columns: `well_id`, `min_date`, `max_date`, `expected_months`, `actual_months`,
`gap_count` (= `expected_months - actual_months`).
Return only wells where `gap_count > 0`, sorted descending by `gap_count`.

### Implementation constraint:
Use a Dask groupby aggregation — do not compute the full dataset to pandas before
aggregating. Call `.compute()` only on the final aggregated result.

**Dependencies:** `dask.dataframe`, `pandas`

**Test cases (TR-04):**
- Given a synthetic Dask DataFrame for one well with records for Jan–Dec 2021 (12 rows),
  assert `check_well_completeness` returns an empty DataFrame (no gaps).
- Given a well with records for Jan, Feb, Apr 2021 (3 rows — March missing), assert
  the well appears in the output with `expected_months=4`, `actual_months=3`,
  `gap_count=1`.
- Given two wells — one with no gaps and one with 2 gaps — assert only the gapped
  well appears in the output.
- Assert the output is a pandas DataFrame (not Dask).
- Assert the output columns are exactly: `well_id`, `min_date`, `max_date`,
  `expected_months`, `actual_months`, `gap_count`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task TRN-06: Transform pipeline assembler

**Module:** `cogcc_pipeline/transform.py`
**Function:** `run_transform(config_path: str = "config/config.yaml") -> None`

**Description:**
Top-level entry point for the transform stage. Orchestrates all cleaning, validation,
sorting, and partitioning steps and writes the final Parquet output.

### Processing steps (in order):

1. `load_config(config_path)` from `cogcc_pipeline.utils`.
2. Read `data/interim/` with `dd.read_parquet(interim_dir)` — no re-casting needed
   (the boundary contract guarantees canonical dtypes from ingest).
3. Apply `_add_derived_columns` via `map_partitions` (adds `well_id`,
   `production_date`).
4. Apply `_clean_production_volumes` via `map_partitions`.
5. Apply `_normalize_operator_names` via `map_partitions`.
6. Apply `_deduplicate` via `map_partitions`.
7. **Set index on `well_id`:** `ddf = ddf.set_index("well_id", sorted=False, drop=True)`.
   This triggers a distributed shuffle — it is intentional and expected per the
   boundary contract.
8. **Sort by `production_date` within partitions:**
   After `set_index`, apply a `map_partitions` call that sorts each partition's
   DataFrame by `production_date` ascending using `df.sort_values("production_date")`.
   This satisfies the sort guarantee required by the features stage (H1, transform manifest).
9. **Repartition:** `ddf.repartition(npartitions=get_partition_count(n))` where
   `n` is derived from the number of interim Parquet files (use `len(list(Path(interim_dir).glob("*.parquet")))`).
   Repartition must be the last operation before writing (ADR-004).
10. **Write Parquet:** `ddf.to_parquet(processed_dir, write_index=True, overwrite=True)`.
11. **Log well completeness:** Call `check_well_completeness(ddf_before_repartition)`
    on the sorted but not-yet-repartitioned Dask DataFrame; log the result as INFO if
    no gaps are found, or WARNING listing the top 10 gapped wells if gaps are present.
    This is a diagnostic step — it must not block or fail the pipeline.
12. Log total rows in output (from partition metadata).

### Scheduler:
Reuse an existing Dask `Client` — do not initialize a new cluster inside this function.

**Error handling:**
- If `interim_dir` has no Parquet files → log WARNING and return.
- Any exception in `_add_derived_columns` or `_clean_production_volumes` →
  propagate; log ERROR with the stage name.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`, `cogcc_pipeline.utils`

**Test cases:**
- Given synthetic interim Parquet in a temp dir (2 wells × 6 months each), assert
  `run_transform` writes Parquet to `processed_dir` without error.
- Assert the Parquet output is readable by `dd.read_parquet` (TR-18).
- Assert the output contains columns: `production_date`, `well_id`, `OilProduced`,
  `GasProduced`, `WaterProduced`, `oil_unit_flag`.
- Assert the output is sorted by `production_date` ascending within each partition
  (read each partition with `pd.read_parquet`, assert `df["production_date"].is_monotonic_increasing`).
- TR-16 sort stability: If a well spans multiple partitions, assert the last
  `production_date` in partition N is strictly less than the first in partition N+1
  for the same well.
- TR-17: Assert `run_transform` does not call `.compute()` before the final write.
- Given an empty `interim_dir`, assert `run_transform` returns without raising.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task TRN-07: Transform stage integration tests

**Module:** `tests/test_transform.py`
**Scope:** TR-01, TR-02, TR-04, TR-05, TR-11, TR-12, TR-13, TR-14, TR-15, TR-16, TR-17, TR-23

**Description:**
Write integration-level tests for the complete transform output contract using
synthetic interim Parquet data.

### TR-11 — Data integrity spot-check
- Create synthetic interim Parquet with 50 rows across 5 wells.
- Run `run_transform`.
- For 10 randomly selected `(well_id, production_date)` pairs, assert that the
  `OilProduced` value in the processed output matches the corresponding cleaned
  expected value (i.e., positive values are preserved, negatives are NA).

### TR-12 — Data cleaning validation
- Create input with: 2 null `OilProduced` rows, 3 negative `GasProduced` rows,
  1 duplicate row.
- Run `run_transform`.
- Assert null `OilProduced` remains null (not filled).
- Assert negative `GasProduced` rows become NA.
- Assert duplicate row is removed (row count decreases by 1).
- Assert `production_date` dtype is `datetime64[ns]`.
- Assert `OilProduced` dtype is `float64`.

### TR-13 — Partition correctness
- After `run_transform` with 3 known wells, read each Parquet partition file.
- For each partition file, assert all rows have the same `well_id` value in the
  index — no partition should contain rows from multiple wells.

### TR-14 — Schema stability across wells
- Read 2 different partition files after `run_transform`.
- Assert that the column names list of partition 1 equals the column names list
  of partition 2 (identical names and identical order).
- Assert that the dtypes dict of partition 1 equals the dtypes dict of partition 2.

### TR-15 — Row count reconciliation
- Count rows in interim Parquet input (before transform).
- Count rows in processed Parquet output (after transform).
- Assert output row count <= input row count.
- Run `run_transform` a second time on the same input (idempotency check).
- Assert output row count is identical to the first run.

### TR-16 — Sort stability
- Use a synthetic well with 12 months of production data that would span at least 2
  partitions after repartition.
- Assert that for all consecutive partition pairs containing rows for this well, the
  max `production_date` in partition N < min `production_date` in partition N+1.

### TR-23 — map_partitions meta consistency
- For `_add_derived_columns`, `_clean_production_volumes`, `_normalize_operator_names`,
  `_deduplicate`: call each function on an empty DataFrame matching the expected input
  schema. Assert the output column list and dtypes match what would be passed as `meta`
  to `map_partitions`.

**Definition of done:** All TR-01, TR-02, TR-04, TR-05, TR-11, TR-12, TR-13, TR-14,
TR-15, TR-16, TR-17, TR-23 test cases pass, ruff and mypy report no errors,
requirements.txt updated with all third-party packages imported in this task.
