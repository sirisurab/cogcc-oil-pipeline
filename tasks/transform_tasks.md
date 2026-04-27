# Transform Stage Tasks

**Module:** `cogcc_pipeline/transform.py`
**Test file:** `tests/test_transform.py`

---

## Governing documents

Input state contract: boundary-ingest-transform.md.
Output state contract: boundary-transform-features.md.
Stage requirements: stage-manifest-transform.md.
Dtype and schema decisions: ADR-003, `references/production-data-dictionary.csv`.
Scheduler and parallelization: ADR-001.
Parquet partitioning, repartition ordering: ADR-004.
Task graph laziness: ADR-005.
Scale and vectorization: ADR-002.
Test marking: ADR-008.
Deduplication rule: task-writer-cogcc.md `<deduplication>` section.

---

## Scope constraints (apply to every task in this file)

- bs4/BeautifulSoup is present for the acquire stage only — do not use it in transform.
- No schema inference from data. No dynamic column discovery (ADR-003).
- Input state is fully defined by boundary-ingest-transform.md — no column
  name reconciliation, no re-casting on read.
- Null sentinel rule (ADR-003): `np.nan` for float64 columns; `pd.NA` for nullable
  extension types (Int64, StringDtype, CategoricalDtype, BooleanDtype). Using `pd.NA`
  on a float64 column raises TypeError at compute time.
- Meta derivation for any `map_partitions` or `dd.from_delayed` call (ADR-003): call the
  actual function on a minimal real input and slice to zero rows — do not construct a
  named helper or empty-frame builder.
- Per-row iteration is prohibited (ADR-002). All column operations must use vectorized
  methods. String operations must use the `.str` accessor (ADR-002).
- Task graph must remain lazy until the Parquet write (ADR-005).
- `set_index` is the last structural operation before Parquet write (stage-manifest-
  transform.md H4). All operations that reference the entity column must complete before
  `set_index` is called.
- Repartition is the last operation before Parquet write (ADR-004).

---

## Input state (from boundary-ingest-transform.md)

- Canonical column names as defined in `references/production-data-dictionary.csv`.
- Data-dictionary dtypes enforced on all columns.
- Nullable absent columns already filled with NA.
- Partitions: `max(10, min(n, 50))`.
- Unsorted.

---

## Output state (from boundary-transform-features.md)

- Entity-indexed on `ApiSequenceNumber` (the entity column, derived from data dictionary
  and domain context — the five-digit well sequence number uniquely identifies a well
  within a county).
- Sorted by `production_date` within each partition.
- Categoricals cast to declared category sets.
- Invalid values replaced with the appropriate null sentinel.
- Partitions: `max(10, min(n, 50))`.

---

## Task 01: Load transform configuration

**Module:** `cogcc_pipeline/transform.py`
**Function:** `load_config(config_path: str) -> dict`

**Description:** Read `config.yaml` from `config_path` and return the full config dict.
The transform section must contain: interim input directory path, processed output
directory path, and the entity column name. Raise `KeyError` with a descriptive message
if any required transform key is missing.

**Dependencies:** pyyaml

**Not required:** config merging, environment variable fallback.

**Test cases (all marked `@pytest.mark.unit`):**
- Given a config with all required transform keys, assert the returned dict contains them.
- Given a config missing the transform section, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Derive production_date column

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_add_production_date(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function that constructs a `production_date` column of
dtype `datetime64[ns]` from `ReportYear` and `ReportMonth`. The constructed date is the
first day of the reported month. Returns a DataFrame with all input columns plus the
new `production_date` column. Every return path must return a DataFrame with the same
column set and dtypes — including the zero-row case (stage-manifest-ingest.md H2 applies
by extension; ADR-003 governs dtype uniformity).

Vectorization: all date construction must use vectorized pandas operations — no row-wise
iteration (ADR-002).

Meta derivation (ADR-003): meta for any `map_partitions` call wrapping this function must
be derived by calling this function on a minimal real input and slicing to zero rows.

**Dependencies:** pandas, numpy

**Test cases (all marked `@pytest.mark.unit`):**
- Given a partition with `ReportYear=2022` and `ReportMonth=3`, assert `production_date`
  equals `2022-03-01` as `datetime64[ns]`.
- Given a zero-row partition with correct schema, assert the function returns a zero-row
  DataFrame with `production_date` column present and dtype `datetime64[ns]`.
- Assert the return type is a pandas DataFrame (not a Dask DataFrame).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Cast categorical columns

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_cast_categoricals(partition: pd.DataFrame, data_dict: dict) -> pd.DataFrame`

**Description:** Partition-level function that casts all columns declared as `categorical`
in the data dictionary to `pd.CategoricalDtype` with the declared allowed values. Before
casting, any value in a categorical column that is not in the declared category set must
be replaced with the appropriate null sentinel (`np.nan` if the underlying dtype is
float-backed, `pd.NA` for extension-typed categoricals) — see ADR-003 and stage-manifest-
transform.md H2. Passing unrecognized values through to the cast raises a ValueError from
pandas. Returns a DataFrame with the same column set; only categorical column dtypes
change. Every return path — including the zero-row case — must satisfy the same schema
contract.

Vectorization: use vectorized `.isin()` and `.where()` or `.mask()` for replacement —
no row-wise iteration (ADR-002).

Meta derivation (ADR-003): meta for any `map_partitions` call wrapping this function must
be derived by calling this function on a minimal real input and slicing to zero rows.

**Data dictionary source for allowed values:** `WellStatus` categories are
`AB`, `AC`, `DG`, `PA`, `PR`, `SI`, `SO`, `TA`, `WO` — from
`references/production-data-dictionary.csv`. The data dictionary is the only source for
allowed category values (ADR-003).

**Dependencies:** pandas, numpy

**Test cases (all marked `@pytest.mark.unit`):**
- Given a partition where `WellStatus` contains all declared category values, assert the
  column dtype is `CategoricalDtype` with the correct categories after casting.
- Given a partition where `WellStatus` contains a value not in the declared set (e.g.
  `"XX"`), assert the invalid value is replaced with the null sentinel before the cast —
  not passed through, not raised as an error.
- Given a zero-row partition, assert the function returns a zero-row DataFrame with
  `WellStatus` as CategoricalDtype with the correct categories (TR-12).
- Assert that TR-01 (physical bound validation) does not apply to categoricals — this
  function handles only categorical replacement, not numeric bounds.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Validate numeric bounds

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_validate_numeric_bounds(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function that enforces physical bounds on production
volume and pressure columns. Any value violating a bound must be replaced with `np.nan`
— not raised as an error, not silently passed through (TR-01). Columns and bounds:

- All volume columns (`OilProduced`, `OilSales`, `OilAdjustment`, `GasProduced`,
  `GasSales`, `GasUsedOnLease`, `GasShrinkage`, `WaterProduced`, `FlaredVented`,
  `BomInvent`, `EomInvent`, `GasBtuSales`): must be `>= 0`. Negative values → `np.nan`.
- Pressure columns (`GasPressureTubing`, `GasPressureCasing`, `WaterPressureTubing`,
  `WaterPressureCasing`): must be `>= 0`. Negative values → `np.nan`.
- `OilGravity`: no bound enforced here (API gravity can be negative for some heavy oils).
- `DaysProduced`: must be `>= 0` and `<= 31`. Out-of-range values → `np.nan` (cast to
  float if currently Int64 before applying np.nan, or keep as Int64 and use pd.NA).

Unit consistency check (TR-02): `OilProduced` values above 50,000 BBL/month for a single
well are flagged as likely unit errors. Replace with `np.nan` and log a warning with the
row count affected. This threshold applies to `OilProduced` only.

Zero production is a valid measurement — do not replace zeros with nulls (TR-05).

Vectorization: all bound enforcement must use vectorized `.clip()`, `.where()`, or
`.mask()` — no row-wise iteration (ADR-002).

Returns a DataFrame with the same schema as input. Every return path — including the
zero-row case — must satisfy the same schema and dtype contract (ADR-003).

Null sentinel rule: all replacements use `np.nan` for float64 columns (ADR-003).

**Dependencies:** pandas, numpy, logging (stdlib)

**Test cases (all marked `@pytest.mark.unit`):**
- Given a row where `OilProduced = -5.0`, assert it is replaced with `np.nan` (TR-01).
- Given a row where `WaterProduced = 0.0`, assert it remains `0.0` — not replaced
  with `np.nan` (TR-05).
- Given a row where `OilProduced = 60000.0`, assert it is replaced with `np.nan` and a
  warning is logged (TR-02).
- Given a row where `GasPressureTubing = -1.0`, assert it is replaced with `np.nan`
  (TR-01).
- Given a zero-row partition, assert the function returns a zero-row DataFrame with
  unchanged schema and dtypes.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Deduplicate revised production records

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_deduplicate(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function that removes duplicate production records for
the same `(ApiSequenceNumber, production_date)` pair — the tie-breaking rule is defined
in `task-writer-cogcc.md`: sort by `AcceptedDate` descending before dedup and keep the
first row, retaining the most recently accepted (revised) report. A bare
`drop_duplicates()` with no subset must not be used (stage-manifest-transform.md H5).

This function operates on a pandas partition that already has `production_date` present
(from Task 02). Deduplication keys on `(ApiSequenceNumber, production_date)`.

Returns a DataFrame with the same schema as input and `<= input row count` rows (TR-15).
Every return path must satisfy the same schema and dtype contract.

Idempotency: running this function twice on the same partition must produce the same
result as running it once (TR-15).

Vectorization: use pandas `sort_values` + `drop_duplicates(subset=...)` — no row-wise
iteration (ADR-002).

**Dependencies:** pandas

**Test cases (all marked `@pytest.mark.unit`):**
- Given a partition with two rows for the same `(ApiSequenceNumber, production_date)` —
  one with an earlier `AcceptedDate` and one with a later — assert the function retains
  the row with the later `AcceptedDate` (the revised record).
- Given a partition with no duplicates, assert the function returns the same row count.
- Given a partition that is already deduplicated (running twice), assert the second run
  produces the same output as the first — idempotency (TR-15).
- Given a partition where two rows differ only in `OilProduced` (same entity and date),
  assert neither row is retained in full — only the more recently accepted one is kept.
- Given a zero-row partition, assert the function returns a zero-row DataFrame with the
  same schema.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 06: Apply all partition-level transforms

**Module:** `cogcc_pipeline/transform.py`
**Function:** `_transform_partition(partition: pd.DataFrame, data_dict: dict) -> pd.DataFrame`

**Description:** Partition-level function that applies all cleaning and enrichment steps
in sequence to a single pandas partition: add `production_date` (Task 02), validate
numeric bounds (Task 04), cast categoricals (Task 03), deduplicate (Task 05). Returns
the cleaned, enriched, deduplicated partition. Every return path must satisfy the output
schema required by boundary-transform-features.md, including the zero-row case.

This is the function passed to `dask.dataframe.map_partitions`. Meta derivation (ADR-003):
meta must be derived by calling `_transform_partition` on a minimal real input and slicing
to zero rows. Do not construct a named helper or empty-frame builder.

Vectorization: all operations must be vectorized (ADR-002).

**Dependencies:** pandas, numpy

**Test cases (all marked `@pytest.mark.unit`):**
- Given a synthetic partition with a duplicate row, a negative volume, an invalid
  categorical value, and valid `ReportYear`/`ReportMonth` columns, assert the output
  has: `production_date` present, no negative volumes, no invalid categorical values,
  and no duplicate rows.
- Given a zero-row partition with the correct input schema, assert the function returns a
  zero-row DataFrame with the correct output schema (TR-12, ADR-003).
- Assert that `WellStatus` dtype is CategoricalDtype with the declared categories.
- Assert that `production_date` dtype is `datetime64[ns]`.
- Assert the return type is a pandas DataFrame (TR-17 — map_partitions wraps this).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Check well record completeness

**Module:** `cogcc_pipeline/transform.py`
**Function:** `check_well_completeness(ddf: dd.DataFrame) -> pd.DataFrame`

**Description:** Compute a summary DataFrame (in pandas, after `.compute()`) that, for
each unique `ApiSequenceNumber`, reports: the first production date, the last production
date, the expected monthly record count (based on the date span), and the actual record
count. Returns this summary as a pandas DataFrame for logging and diagnostic use. This
function is called from the orchestrator for diagnostic purposes only — its output is
logged, not written to Parquet.

Implements TR-04 (well completeness check). The expected count is computed as the number
of calendar months between first and last production date (inclusive).

Vectorization: per-entity aggregation must use groupby — no iteration over entity groups
(ADR-002).

**Dependencies:** pandas, dask

**Test cases (all marked `@pytest.mark.unit`):**
- Given a Dask DataFrame with a single well having records for Jan–Dec 2021 (12 records),
  assert the summary row for that well shows expected=12 and actual=12.
- Given a well with a gap (e.g. Jan–Mar and May–Dec 2021 — 11 records, expected 12),
  assert the summary shows actual=11 and expected=12 (TR-04).
- Assert the return type is a pandas DataFrame.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Write transform Parquet output

**Module:** `cogcc_pipeline/transform.py`
**Function:** `write_parquet(ddf: dd.DataFrame, output_dir: str) -> None`

**Description:** Set the entity index (`ApiSequenceNumber`) on the Dask DataFrame using
`set_index`, then repartition to `max(10, min(n_partitions, 50))`, then write to
`output_dir` as partitioned Parquet. Ordering of operations (governed by stage-manifest-
transform.md H1, H3, H4, and ADR-004):

- `set_index` is the last structural operation before repartition and write.
- After `set_index`, sort by `production_date` within each partition — the distributed
  shuffle from `set_index` destroys prior row ordering (stage-manifest-transform.md H1).
- Repartition is the last operation before writing (ADR-004).
- No operations may be applied after repartition.

Partition count formula: `max(10, min(n_partitions, 50))` — governed by ADR-004.

The output directory must be created if it does not exist.

**Not required:** per-entity file partitioning (prohibited by ADR-004), compression codec
selection beyond pyarrow defaults.

**Dependencies:** dask, pyarrow, pathlib (stdlib)

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a Dask DataFrame, assert the written Parquet is readable and the index is set to
  `ApiSequenceNumber` (TR-16).
- `@pytest.mark.integration` (TR-18): Write to `tmp_path`; assert Parquet files are
  readable by both `dask.dataframe.read_parquet` and `pandas.read_parquet`.
- `@pytest.mark.integration` (TR-16): Assert sort stability — for a well spanning
  multiple partitions, the last date in one Parquet file is earlier than the first date
  in the next file for the same well.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 09: Transform stage orchestrator

**Module:** `cogcc_pipeline/transform.py`
**Function:** `transform(config: dict) -> None`

**Description:** Orchestrate the full transform stage. Read interim Parquet from the
configured input directory, apply all partition-level transforms via `map_partitions`,
run the completeness diagnostic, and write processed Parquet to the configured output
directory. The Dask distributed scheduler must be reused if already running — do not
initialize a new cluster inside this function (build-env-manifest.md). Scheduler choice
is governed by ADR-001.

Task graph must remain lazy until the Parquet write (ADR-005). All independent
computations must be batched into a single compute call — no sequential intermediate
`.compute()` calls (ADR-005). The completeness check (Task 07) is a diagnostic and may
trigger a separate `.compute()` after the main write.

Execution sequence:
1. Read interim Parquet from `config["transform"]["interim_dir"]`.
2. Load data dictionary (reuse Task 01 from ingest, or load independently).
3. Apply `_transform_partition` via `map_partitions` (Task 06).
4. Write processed Parquet (Task 08).
5. Run completeness diagnostic (Task 07) and log results.

**Not required:** per-partition error recovery, schema re-derivation on read.

**Dependencies:** dask, logging (stdlib)

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a config pointing to a directory with no Parquet files, assert the function logs
  a warning and returns without raising.
- `@pytest.mark.integration` (TR-25): Run `transform()` on the interim Parquet output
  from a prior ingest run (using `tmp_path`). Assert: (a) processed Parquet is readable;
  (b) output satisfies all upstream guarantees in boundary-transform-features.md — entity
  index, sort order, partition count, categorical cast, null sentinels.
- `@pytest.mark.integration` (TR-11): Spot-check production volumes from interim to
  processed for a sample of wells and months — assert values are identical (data
  integrity).
- `@pytest.mark.integration` (TR-12): Assert dtypes are correctly set in the processed
  data: datetime for `production_date`, float for volume columns, CategoricalDtype for
  `WellStatus`.
- `@pytest.mark.integration` (TR-13): Assert that no Parquet partition contains rows from
  more than one `ApiSequenceNumber` value after `set_index`.
- `@pytest.mark.integration` (TR-14): Assert schema (column names and dtypes) sampled
  from one partition matches schema from another partition.
- `@pytest.mark.integration` (TR-15): Assert processed row count is `<=` interim row
  count. Assert running transform twice produces the same row count (idempotency).
- `@pytest.mark.integration` (TR-17): Assert the return value of `transform()` is `None`;
  intermediate pipeline functions return `dask.dataframe.DataFrame`, not
  `pandas.DataFrame`.
- `@pytest.mark.integration` (TR-23): For each `map_partitions` call in transform, assert
  the `meta=` argument matches the actual function output in column names, column order,
  and dtypes by calling the function on a minimal synthetic input.
- `@pytest.mark.integration` (TR-27): Combined with ingest and features in the end-to-end
  pipeline integration test — output satisfies boundary-transform-features.md.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
