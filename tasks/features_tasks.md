# Features Stage Tasks

**Module:** `cogcc_pipeline/features.py`
**Test file:** `tests/test_features.py`

---

## Governing documents

Input state contract: boundary-transform-features.md.
Stage requirements and output state: stage-manifest-features.md.
Dtype, meta, and null-sentinel decisions: ADR-003.
Scheduler and parallelization: ADR-001.
Parquet partitioning and repartition ordering: ADR-004.
Task graph laziness and compute batching: ADR-005.
Scale and vectorization: ADR-002.
Test marking: ADR-008.

---

## Scope constraints (apply to every task in this file)

- bs4/BeautifulSoup is present for the acquire stage only — do not use it in features.
- No schema inference from data. No dynamic column discovery. No re-casting or
  re-validation of categorical columns (boundary-transform-features.md).
- No re-sorting before computing time-dependent features — temporal ordering is
  guaranteed by the transform stage (boundary-transform-features.md).
- No re-indexing — the entity index is established by transform
  (boundary-transform-features.md).
- Null sentinel rule (ADR-003): `np.nan` for float64 columns; `pd.NA` valid only for
  nullable extension types. Using `pd.NA` on a float64 column raises TypeError at compute
  time.
- Meta derivation for every `map_partitions` call (ADR-003): call the actual function on
  a minimal real input and slice to zero rows — do not construct a named helper or
  empty-frame builder.
- Per-row iteration is prohibited (ADR-002). Per-entity aggregations must use vectorized
  grouped transformations (ADR-002).
- Task graph must remain lazy until the Parquet write (ADR-005).
- Repartition is the last operation before Parquet write (ADR-004).

---

## Input state (from boundary-transform-features.md)

- Entity-indexed on `ApiSequenceNumber`.
- Sorted by `production_date` within each partition.
- Categoricals cast to declared category sets.
- Invalid values replaced with the appropriate null sentinel.
- Partitions: `max(10, min(n, 50))`.

---

## Output state (from stage-manifest-features.md)

All input columns plus the derived feature columns listed in Task 02 through Task 05.
ML-ready Parquet files in `data/processed/`. The complete output column list required
by TR-19: all input columns plus `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`,
`decline_rate_oil`, `oil_rolling_3m`, `oil_rolling_6m`, `gas_rolling_3m`,
`gas_rolling_6m`, `water_rolling_3m`, `water_rolling_6m`, `oil_lag_1`, `gas_lag_1`,
`water_lag_1`.

---

## Task 01: Load features configuration

**Module:** `cogcc_pipeline/features.py`
**Function:** `load_config(config_path: str) -> dict`

**Description:** Read `config.yaml` from `config_path` and return the full config dict.
The features section must contain: processed input directory path and ML-ready output
directory path. Raise `KeyError` with a descriptive message if any required features key
is missing.

**Dependencies:** pyyaml

**Not required:** config merging, environment variable fallback.

**Test cases (all marked `@pytest.mark.unit`):**
- Given a config with all required features keys, assert the returned dict contains them.
- Given a config missing the features section, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Compute cumulative production volumes

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_cumulative_features(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function that computes cumulative oil, gas, and water
production for each well (entity) within the partition. Adds three columns: `cum_oil`
(cumulative sum of `OilProduced`), `cum_gas` (cumulative sum of `GasProduced`),
`cum_water` (cumulative sum of `WaterProduced`). All three are float64 columns. Null
values in the source columns are treated as zero for cumulative purposes (fill with 0.0
before cumsum).

Temporal ordering within each entity group is guaranteed by the transform stage
(boundary-transform-features.md) — no re-sorting is required.

Vectorization: use grouped cumulative sum with `groupby().cumsum()` — no per-entity
iteration (ADR-002).

Returns a DataFrame with all input columns plus the three new cumulative columns. Every
return path — including the zero-row case — must carry the same schema (ADR-003).

Meta derivation (ADR-003): meta for the `map_partitions` call must be derived by calling
this function on a minimal real input and slicing to zero rows.

**Domain constraint (TR-03):** cumulative values must be monotonically non-decreasing per
entity within the partition — a well cannot un-produce. This is enforced by the sort
guarantee from transform; a test must verify it.

**Domain constraint (TR-08):** for a shut-in well (zero production months), cumulative
values must remain flat (equal to the previous month's value), not decrease or jump
forward incorrectly.

**Dependencies:** pandas, numpy

**Test cases (all marked `@pytest.mark.unit`):**
- Given a synthetic single-well partition with known monthly oil volumes, assert `cum_oil`
  equals the hand-computed cumulative sum for each row (TR-09a).
- Given a partition where a well has two consecutive zero-production months mid-sequence,
  assert `cum_oil` remains flat during those months — not zero, not decremented (TR-08a).
- Given a partition where a well has zero-production months at the start, assert
  `cum_oil` starts at zero and remains zero until production begins (TR-08b).
- Assert `cum_oil` is monotonically non-decreasing for every entity in the partition
  (TR-03).
- Given a zero-row partition, assert the function returns a zero-row DataFrame with
  `cum_oil`, `cum_gas`, and `cum_water` columns present and dtype float64.
- Assert the return type is a pandas DataFrame.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Compute GOR and water cut

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_ratio_features(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function that computes Gas-Oil Ratio (GOR) and water cut
for each row. Adds two float64 columns: `gor` and `water_cut`.

**GOR formula (TR-09d):** `gor = GasProduced / OilProduced`. Division uses vectorized
pandas operations — no row iteration. Zero-denominator handling (TR-06):
- `OilProduced == 0` and `GasProduced > 0` → `gor = np.nan`
- `OilProduced == 0` and `GasProduced == 0` → `gor = np.nan`
- `OilProduced > 0` and `GasProduced == 0` → `gor = 0.0`
- `OilProduced > 0` and `GasProduced > 0` → `gor = GasProduced / OilProduced`

**Water cut formula (TR-09e):** `water_cut = WaterProduced / (OilProduced + WaterProduced)`.
Zero-denominator handling: when `OilProduced + WaterProduced == 0`, `water_cut = np.nan`.
Valid boundary values (TR-10): `water_cut = 0.0` (no water) and `water_cut = 1.0`
(all water) are physically valid — neither is treated as invalid or replaced.

Vectorization: use `numpy.where` or pandas `.where()` / `.mask()` — no row iteration
(ADR-002).

Returns a DataFrame with all input columns plus `gor` and `water_cut`. Every return path
— including the zero-row case — must carry the same schema (ADR-003). Null sentinel for
both columns is `np.nan` (float64 columns, ADR-003).

Meta derivation (ADR-003): meta for `map_partitions` must be derived by calling this
function on a minimal real input and slicing to zero rows.

**Dependencies:** pandas, numpy

**Test cases (all marked `@pytest.mark.unit`):**
- Given `OilProduced=0`, `GasProduced=100`, assert `gor = np.nan` (TR-06a).
- Given `OilProduced=0`, `GasProduced=0`, assert `gor = np.nan` (TR-06b).
- Given `OilProduced=50`, `GasProduced=0`, assert `gor = 0.0` (TR-06c).
- Given `OilProduced=50`, `GasProduced=500`, assert `gor = 10.0` (TR-09d).
- Given `WaterProduced=0`, `OilProduced=100`, assert `water_cut = 0.0` — not null,
  not an error (TR-10a).
- Given `OilProduced=0`, `WaterProduced=200`, assert `water_cut = 1.0` — not null,
  not an error (TR-10b).
- Given `OilProduced=100`, `WaterProduced=100`, assert `water_cut = 0.5` (TR-09e).
- Assert that `water_cut` values outside `[0, 1]` are not produced by valid positive
  inputs — they would indicate a formula error (TR-01 water cut bound check).
- Given a zero-row partition, assert the function returns a zero-row DataFrame with
  `gor` and `water_cut` columns present and dtype float64.
- Assert the return type is a pandas DataFrame (TR-17).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Compute decline rates

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_decline_rates(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function that computes the period-over-period oil
production decline rate and clips it to the engineering bounds `[-1.0, 10.0]` (TR-07).
Adds one float64 column: `decline_rate_oil`.

**Formula:** `decline_rate_oil = (OilProduced_prev - OilProduced) / OilProduced_prev`
per entity group, where `OilProduced_prev` is the prior month's `OilProduced` within the
same entity. Temporal ordering within each entity is guaranteed (boundary-transform-
features.md).

Zero-denominator handling (TR-07d): when `OilProduced_prev == 0`, the raw rate is
undefined — use `np.nan` before clipping (do not produce an unclipped extreme value
before the clip is evaluated). Clipping is applied after the zero-denominator substitution.

Clip bounds (TR-07): `decline_rate_oil` is clipped to `[-1.0, 10.0]`. These are ML
feature stability bounds, not physical constants.

Vectorization: use grouped `shift(1)` for per-entity lag — no per-entity iteration
(ADR-002).

Returns a DataFrame with all input columns plus `decline_rate_oil`. Every return path —
including the zero-row case — must carry the same schema (ADR-003). Null sentinel for
`decline_rate_oil` is `np.nan` (ADR-003).

Meta derivation (ADR-003): meta for `map_partitions` must be derived by calling this
function on a minimal real input and slicing to zero rows.

**Dependencies:** pandas, numpy

**Test cases (all marked `@pytest.mark.unit`):**
- Given consecutive months with `OilProduced=[100, 80]`, assert `decline_rate_oil` for
  month 2 equals `0.2` (20% decline) and month 1 is `np.nan` (no prior) (TR-07c).
- Given a computed rate below `-1.0`, assert it is clipped to exactly `-1.0` (TR-07a).
- Given a computed rate above `10.0`, assert it is clipped to exactly `10.0` (TR-07b).
- Given two consecutive zero-production months, assert `decline_rate_oil` is `np.nan`
  (or clipped if defined) — not an unclipped extreme value before clipping (TR-07d).
- Given a zero-row partition, assert the function returns a zero-row DataFrame with
  `decline_rate_oil` present and dtype float64.
- Assert the return type is a pandas DataFrame.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Compute rolling averages and lag features

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_rolling_and_lag_features(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function that computes rolling averages and 1-month lag
features per entity group. Temporal ordering within each entity is guaranteed
(boundary-transform-features.md) — no re-sorting required.

**Rolling averages (TR-09a):** 3-month and 6-month rolling means for `OilProduced`,
`GasProduced`, and `WaterProduced`. Added columns: `oil_rolling_3m`, `oil_rolling_6m`,
`gas_rolling_3m`, `gas_rolling_6m`, `water_rolling_3m`, `water_rolling_6m`. All are
float64.

**Window behaviour (TR-09b):** when the rolling window is larger than the available
history (e.g. the first 1 or 2 months of a well's record), the result must be `np.nan`
for that row (use `min_periods=window` — do not use partial windows unless explicitly
specified). This ensures the first 2 rows of a 3-month window are `np.nan`, not
silently zero or a wrong partial value.

**Lag features (TR-09c):** 1-month lag of `OilProduced`, `GasProduced`, and
`WaterProduced` per entity group. Added columns: `oil_lag_1`, `gas_lag_1`, `water_lag_1`.
All are float64. Lag-1 for month N equals the raw production value for month N-1.

Vectorization: use grouped `rolling()` and grouped `shift(1)` — no per-entity iteration
(ADR-002).

Returns a DataFrame with all input columns plus all 9 new feature columns (6 rolling + 3
lag). Every return path — including the zero-row case — must carry the same schema
(ADR-003). All new columns use `np.nan` as the null sentinel (ADR-003).

Meta derivation (ADR-003): meta for `map_partitions` must be derived by calling this
function on a minimal real input and slicing to zero rows.

**Dependencies:** pandas, numpy

**Test cases (all marked `@pytest.mark.unit`):**
- Given a synthetic single-well partition with 6 months of known `OilProduced` values,
  assert `oil_rolling_3m` for month 3 equals the hand-computed mean of months 1–3, and
  for months 1 and 2 is `np.nan` (TR-09a, TR-09b).
- Assert `oil_rolling_6m` for months 1–5 is `np.nan`, and for month 6 equals the mean
  of months 1–6 (TR-09b).
- Assert `oil_lag_1` for month 2 equals the `OilProduced` value of month 1, for month 3
  equals month 2's value, for month 4 equals month 3's value (TR-09c — 3 consecutive
  months verified).
- Assert `oil_lag_1` for the first month of a well is `np.nan`.
- Given a zero-row partition, assert the function returns a zero-row DataFrame with all
  9 new columns present and dtype float64.
- Assert the return type is a pandas DataFrame.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 06: Apply all partition-level feature transforms

**Module:** `cogcc_pipeline/features.py`
**Function:** `_compute_features_partition(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function that applies all feature computation steps in
sequence: cumulative volumes (Task 02), GOR and water cut (Task 03), decline rates
(Task 04), rolling averages and lag features (Task 05). Returns a DataFrame with all
input columns plus all derived feature columns. Every return path — including the
zero-row case — must carry the same output schema (ADR-003).

This is the function passed to `dask.dataframe.map_partitions`. Meta derivation
(ADR-003): meta must be derived by calling `_compute_features_partition` on a minimal
real input and slicing to zero rows. Do not construct a named helper or empty-frame
builder.

**Dependencies:** pandas, numpy

**Test cases (all marked `@pytest.mark.unit`):**
- Given a synthetic multi-well partition with known production values, assert all derived
  columns (`cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`, `decline_rate_oil`,
  `oil_rolling_3m`, `oil_rolling_6m`, `gas_rolling_3m`, `gas_rolling_6m`,
  `water_rolling_3m`, `water_rolling_6m`, `oil_lag_1`, `gas_lag_1`, `water_lag_1`) are
  present in the output (TR-19).
- Given a zero-row partition with correct input schema, assert the function returns a
  zero-row DataFrame with all feature columns present and dtype float64 (ADR-003).
- Assert the return type is a pandas DataFrame (TR-17).
- Assert `cum_oil` is monotonically non-decreasing per entity (TR-03).
- `@pytest.mark.unit` (TR-23): Call `_compute_features_partition` on a minimal synthetic
  input and assert the resulting column list and dtypes match the `meta=` argument passed
  to `map_partitions` — column names, column order, and dtypes must all match exactly.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Write ML-ready Parquet output

**Module:** `cogcc_pipeline/features.py`
**Function:** `write_parquet(ddf: dd.DataFrame, output_dir: str) -> None`

**Description:** Repartition the Dask DataFrame to `max(10, min(n_partitions, 50))`
partitions and write to `output_dir` as partitioned Parquet. Repartition is the last
operation before writing (ADR-004) — no operations may be applied after repartition.
The output directory must be created if it does not exist.

Partition count formula: governed by ADR-004.

**Not required:** per-entity file partitioning (prohibited by ADR-004).

**Dependencies:** dask, pyarrow, pathlib (stdlib)

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a Dask DataFrame, assert the written Parquet is readable and contains all
  expected feature columns.
- `@pytest.mark.integration` (TR-18): Write to `tmp_path`; assert Parquet files are
  readable by both `dask.dataframe.read_parquet` and `pandas.read_parquet`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Features stage orchestrator

**Module:** `cogcc_pipeline/features.py`
**Function:** `features(config: dict) -> None`

**Description:** Orchestrate the full features stage. Read processed Parquet from the
configured input directory, apply all feature computations via `map_partitions`, and
write ML-ready Parquet to the configured output directory. The Dask distributed scheduler
must be reused if already running — do not initialize a new cluster inside this function
(build-env-manifest.md). Scheduler choice is governed by ADR-001.

Task graph must remain lazy until the Parquet write (ADR-005). All independent
computations must be batched into a single compute call — no sequential intermediate
`.compute()` calls (ADR-005).

Execution sequence:
1. Read processed Parquet from `config["features"]["processed_dir"]`.
2. Apply `_compute_features_partition` via `map_partitions`.
3. Write ML-ready Parquet to `config["features"]["output_dir"]` (Task 07).

**Not required:** schema re-derivation on read, categorical re-casting.

**Dependencies:** dask, logging (stdlib)

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a config pointing to a directory with no Parquet files, assert the function logs
  a warning and returns without raising.
- `@pytest.mark.integration` (TR-26): Run `features()` on the processed Parquet output
  from a prior transform run (using `tmp_path`). Assert: (a) features Parquet is readable;
  (b) all derived feature columns are present; (c) schema is consistent across a sample
  of partitions (TR-14).
- `@pytest.mark.integration` (TR-19): Assert the features Parquet output contains all
  expected derived columns: `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`,
  `decline_rate_oil`, `oil_rolling_3m`, `oil_rolling_6m`, `gas_rolling_3m`,
  `gas_rolling_6m`, `water_rolling_3m`, `water_rolling_6m`, `oil_lag_1`, `gas_lag_1`,
  `water_lag_1`.
- `@pytest.mark.integration` (TR-17): Assert intermediate pipeline functions return
  `dask.dataframe.DataFrame`, not `pandas.DataFrame`.
- `@pytest.mark.integration` (TR-23): For each `map_partitions` call in features, assert
  the `meta=` argument matches the actual function output in column names, column order,
  and dtypes.
- `@pytest.mark.integration` (TR-27): Combined with ingest and transform in the end-to-
  end pipeline integration test — output satisfies stage-manifest-features.md output
  state.
- `@pytest.mark.integration` (TR-01): Assert no `water_cut` value is outside `[0, 1]`
  in the features output.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
