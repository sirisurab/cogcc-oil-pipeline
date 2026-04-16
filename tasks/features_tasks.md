# Features Stage — Task Specifications

## Stage Overview

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/features.py`
**Test file:** `tests/test_features.py`
**Input directory:** `data/processed/` (Parquet, output of transform)
**Output directory:** `data/features/` (Parquet, ML-ready)
**Scheduler:** Dask distributed scheduler (CPU-bound stage — ADR-001)

The features stage reads the processed Parquet from transform, computes all derived
ML features (cumulative volumes, production ratios, decline rates, rolling averages,
lag features, encoded categoricals), and writes ML-ready Parquet to `data/features/`.

### Boundary contract received from transform
- Entity-indexed on `well_id`.
- Sorted by `production_date` within each partition.
- Categoricals cast to declared category sets.
- Invalid values replaced with the appropriate null sentinel.
- Partitioned to `max(10, min(n, 50))`.
- No re-sorting, re-indexing, re-casting, or re-validation needed before feature
  computation.

### Output schema (all input columns PLUS derived feature columns)
Every output Parquet file must contain all of the following columns (TR-19):
`well_id` (index), `production_date`, `OilProduced`, `GasProduced`, `WaterProduced`,
`cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`, `wor`, `wgr`,
`decline_rate_oil`, `decline_rate_gas`,
`oil_roll3`, `oil_roll6`, `oil_roll12`,
`gas_roll3`, `gas_roll6`, `gas_roll12`,
`water_roll3`, `water_roll6`, `water_roll12`,
`oil_lag1`, `gas_lag1`, `water_lag1`,
`wellstatus_encoded`, `days_produced_norm`,
plus all original input columns not named above.

### Key design constraints (from ADRs)
- ADR-001: Use Dask distributed scheduler. Reuse the existing `Client`.
- ADR-002: All feature operations must be vectorized. No row iteration. Use
  `transform` (not `apply`) for grouped window computations. Use `groupby(...).transform`
  or `map_partitions` with partition-level pandas grouped operations.
- ADR-003: meta= arguments derived by calling the function on an empty DataFrame
  (TR-23). Never construct meta manually.
- ADR-004: `repartition()` is the last operation before `to_parquet()`.
- ADR-005: Task graph stays lazy until the final write. No intermediate `.compute()`.
- H1 (features manifest): Temporal ordering within each entity group must be preserved
  at the point each time-dependent feature is computed. The transform stage guarantees
  this ordering — do not re-sort.

### Domain correctness constraints
All feature computations must observe the following domain rules:
- TR-01: Physical bound validation — production volumes, pressures, GOR must be non-negative.
- TR-03: Cumulative production must be monotonically non-decreasing per well.
- TR-06: GOR zero-denominator — oil_bbl=0 with gas>0 → NaN (not exception).
- TR-07: Decline rate clipped to [-1.0, 10.0].
- TR-08: Cumulative production flat during shut-in months (zero production).
- TR-10: Water cut boundary values 0.0 and 1.0 are both valid and must not be
  treated as outliers.

---

## Task FEA-01: Cumulative production features

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_cumulative_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level function (called via `map_partitions`) that computes per-well
cumulative production for oil, gas, and water. Must be pure and stateless.

### Cumulative columns computed:
- `cum_oil` — cumulative sum of `OilProduced` per `well_id`, sorted ascending by
  `production_date`.
- `cum_gas` — cumulative sum of `GasProduced` per `well_id`.
- `cum_water` — cumulative sum of `WaterProduced` per `well_id`.

### Implementation:
Use `df.groupby(level="well_id", group_keys=False)["OilProduced"].cumsum()` (and
similarly for gas and water). The index is `well_id` (set by transform). Since the
DataFrame is already sorted by `production_date` within each partition (transform
guarantee), the cumulative sum is computed in correct temporal order without
re-sorting.

Null values in `OilProduced`/`GasProduced`/`WaterProduced` must propagate through
cumsum — a null period should produce a null cumulative value for that row (not
skip it). Use `skipna=False` in `cumsum()`.

### Domain constraint (TR-03, TR-08):
Since the input is sorted and cumsum is monotone by definition, the monotonicity
guarantee is inherently satisfied if the input data is non-negative (which the
transform stage guarantees by replacing negative values with NA). Shut-in months
with `OilProduced=0` produce a flat cumulative segment (previous value repeated),
which is the correct domain behavior.

**Meta derivation:**
Derive meta by calling `_add_cumulative_features(ddf._meta.copy())` before the
`map_partitions` call (TR-23).

**Dependencies:** `pandas`

**Test cases (TR-03, TR-08):**
- Given a single well with `OilProduced=[100, 200, 150]`, assert
  `cum_oil=[100, 300, 450]`.
- Given a well with `OilProduced=[100, 0, 50]` (shut-in in month 2), assert
  `cum_oil=[100, 100, 150]` — cumulative is flat during shut-in (TR-08a).
- Given a well with `OilProduced=[0, 0, 100]` (not yet online), assert
  `cum_oil=[0, 0, 100]` (TR-08b).
- Given a well with `OilProduced=[100, 0, 0, 200]` (shut-in then resumed), assert
  `cum_oil=[100, 100, 100, 300]` (TR-08c).
- Assert `cum_oil` is monotonically non-decreasing for all non-null values (TR-03).
- Assert that `cum_oil` is `pd.NA` where `OilProduced` is `pd.NA` (null propagation).
- TR-23: Call on empty DataFrame with correct schema; assert output column list and
  dtypes match meta.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task FEA-02: Production ratio features (GOR, WOR, WGR, water cut)

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_ratio_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level function (called via `map_partitions`) that computes monthly
production ratios. Must be pure and stateless.

### Ratio columns computed:
- `gor` — Gas-Oil Ratio: `GasProduced / OilProduced` (gas MCF per oil BBL)
- `water_cut` — Water fraction: `WaterProduced / (OilProduced + WaterProduced)`
- `wor` — Water-Oil Ratio: `WaterProduced / OilProduced`
- `wgr` — Water-Gas Ratio: `WaterProduced / GasProduced`

### Zero-denominator rules (TR-06, TR-10):
All divisions must use pandas vectorized division with `np.where` or `.where()` guards:

**GOR (`gor`):**
- `OilProduced > 0` and `GasProduced >= 0` → `GasProduced / OilProduced`
- `OilProduced == 0` and `GasProduced > 0` → `np.nan` (physically valid pure gas well,
  result undefined as a ratio)
- `OilProduced == 0` and `GasProduced == 0` → `np.nan` (shut-in, no ratio defined)
- `OilProduced > 0` and `GasProduced == 0` → `0.0`
- Either operand is `pd.NA` → result is `pd.NA`

**Water cut (`water_cut`):**
- `(OilProduced + WaterProduced) > 0` → `WaterProduced / (OilProduced + WaterProduced)`
- Both zero → `np.nan`
- Values of exactly `0.0` and `1.0` are valid (TR-10) — do not flag or replace them.

**WOR (`wor`):**
- `OilProduced > 0` → `WaterProduced / OilProduced`
- `OilProduced == 0` → `np.nan`

**WGR (`wgr`):**
- `GasProduced > 0` → `WaterProduced / GasProduced`
- `GasProduced == 0` → `np.nan`

### Implementation constraint:
Use `pd.Series.where()` or `np.where()` for all zero-denominator guards —
never use Python division directly on a column without guarding.
No per-row apply. All operations vectorized.

**Meta derivation:**
Derive meta by calling `_add_ratio_features(ddf._meta.copy())` before `map_partitions`
(TR-23).

**Dependencies:** `pandas`, `numpy`

**Test cases (TR-06, TR-09, TR-10):**
- GOR: `OilProduced=100, GasProduced=500` → `gor=5.0` (TR-09d).
- GOR: `OilProduced=0, GasProduced=500` → `gor=NaN` (not exception, TR-06a).
- GOR: `OilProduced=0, GasProduced=0` → `gor=NaN` (TR-06b).
- GOR: `OilProduced=100, GasProduced=0` → `gor=0.0` (TR-06c).
- Water cut: `WaterProduced=0, OilProduced=100` → `water_cut=0.0` (TR-10a).
- Water cut: `WaterProduced=200, OilProduced=0` → `water_cut=1.0` (TR-10b, valid
  end-of-life well state — do not flag or null).
- Water cut: `WaterProduced=75, OilProduced=25` → `water_cut=0.75` (TR-09e).
- WOR: `OilProduced=0, WaterProduced=50` → `wor=NaN`.
- WOR: `OilProduced=100, WaterProduced=50` → `wor=0.5`.
- Assert no `ZeroDivisionError` or unhandled `RuntimeWarning` for any zero combination.
- TR-23: Call on empty DataFrame; assert output schema matches meta.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task FEA-03: Decline rate feature

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_decline_rates(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level function (called via `map_partitions`) that computes period-over-period
decline rates for oil and gas production per well. Must be pure and stateless.

### Decline rate columns computed:
- `decline_rate_oil` — month-over-month change in `OilProduced` per well:
  `(OilProduced_t - OilProduced_{t-1}) / OilProduced_{t-1}`
  where `t` is the current month and `t-1` is the prior month for the same well.
- `decline_rate_gas` — same formula applied to `GasProduced`.

### Formula derivation:
Use `df.groupby(level="well_id", group_keys=False)["OilProduced"].pct_change()`.
`pct_change()` computes `(current - previous) / previous`, which is the
period-over-period decline rate. The first record per well has no prior period →
result is `NaN`.

### Zero prior-period handling (TR-07d):
If `OilProduced_{t-1} == 0` (shut-in well), `pct_change()` will produce `inf`
or `nan`. Apply the following rule before clipping:
- If prior period is 0 and current period is also 0 → result is `0.0` (both shut-in).
- If prior period is 0 and current period is > 0 → result is `np.nan` (well coming
  back online — rate is undefined, not extreme). Do not let it produce `inf`.
Replace `inf` and `-inf` with `np.nan` before clipping.

### Clip bounds (TR-07):
After computing the raw decline rate (with inf replaced by nan), clip to `[-1.0, 10.0]`:
- Values below `-1.0` → clipped to exactly `-1.0` (TR-07a).
- Values above `10.0` → clipped to exactly `10.0` (TR-07b).
- Values within `[-1.0, 10.0]` → pass through unchanged (TR-07c).
- `NaN` values → remain `NaN` after clipping (do not clip NaN to a bound).

**Meta derivation:**
Derive meta by calling `_add_decline_rates(ddf._meta.copy())` before `map_partitions`
(TR-23).

**Dependencies:** `pandas`, `numpy`

**Test cases (TR-07, TR-09):**
- Given `OilProduced=[100, 80, 60]` for one well, assert
  `decline_rate_oil=[NaN, -0.2, -0.25]`.
- A decline rate of `-2.0` (below clip bound) → clipped to `-1.0` (TR-07a).
- A decline rate of `15.0` (above clip bound) → clipped to `10.0` (TR-07b).
- A decline rate of `0.5` (within bounds) → passes through as `0.5` (TR-07c).
- Prior period `OilProduced=0`, current `OilProduced=0` → `decline_rate_oil=0.0`
  (not inf, not NaN — shut-in remains 0 — TR-07d).
- Prior period `OilProduced=0`, current `OilProduced=100` → `decline_rate_oil=NaN`
  (well resuming, undefined rate — TR-07d).
- Assert no `inf` or `-inf` in output (all replaced before clip).
- Assert first record per well is always `NaN` (no prior period).
- TR-23: Call on empty DataFrame; assert output schema matches meta.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task FEA-04: Rolling average features

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_rolling_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level function (called via `map_partitions`) that computes rolling window
averages for oil, gas, and water production per well. Must be pure and stateless.

### Rolling columns computed (all use `min_periods=1` unless specified otherwise):
- `oil_roll3` — 3-month rolling mean of `OilProduced` per well
- `oil_roll6` — 6-month rolling mean of `OilProduced` per well
- `oil_roll12` — 12-month rolling mean of `OilProduced` per well
- `gas_roll3`, `gas_roll6`, `gas_roll12` — same for `GasProduced`
- `water_roll3`, `water_roll6`, `water_roll12` — same for `WaterProduced`

### Rolling window specification (TR-09b):
Use `df.groupby(level="well_id", group_keys=False)["OilProduced"].rolling(window=3, min_periods=1).mean()`.
- `min_periods=1`: When fewer than `window` prior periods exist (e.g., the first 2
  months of a well's life), the rolling function uses the available periods only —
  not `NaN` and not `0`.
- Exception: If the caller explicitly requires `NaN` for partial windows, the task
  spec takes precedence over this default — use `min_periods=window` in that case.
  **This pipeline uses `min_periods=1`** — partial windows are computed from available data.

### Implementation constraint:
Use `groupby(...).rolling(...)` — do not use `expanding()` or per-group Python loops.
No apply, no iterrows.

**Meta derivation:**
Derive meta by calling `_add_rolling_features(ddf._meta.copy())` before `map_partitions`
(TR-23).

**Dependencies:** `pandas`

**Test cases (TR-09a, TR-09b):**
- Given `OilProduced=[100, 200, 300, 400]` for one well:
  - `oil_roll3[0]` = 100.0 (only 1 period available, min_periods=1)
  - `oil_roll3[1]` = 150.0 (mean of 100, 200)
  - `oil_roll3[2]` = 200.0 (mean of 100, 200, 300)
  - `oil_roll3[3]` = 300.0 (mean of 200, 300, 400)
- Given `GasProduced=[500, 600, 700]`:
  - `gas_roll6[0]` = 500.0, `gas_roll6[1]` = 550.0, `gas_roll6[2]` = 600.0
    (fewer than 6 periods → uses min_periods=1)
- TR-09b: For a well with only 2 months of history, assert `oil_roll3` at month 2
  equals the mean of months 1 and 2 (not NaN, not 0).
- Assert all rolling columns are present in the output DataFrame.
- Assert no `oil_roll3` value is negative when all input is non-negative.
- TR-23: Call on empty DataFrame; assert output schema matches meta.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task FEA-05: Lag features

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_lag_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level function (called via `map_partitions`) that computes 1-period lag
features for oil, gas, and water production per well. Must be pure and stateless.

### Lag columns computed:
- `oil_lag1` — previous month's `OilProduced` for the same well
- `gas_lag1` — previous month's `GasProduced` for the same well
- `water_lag1` — previous month's `WaterProduced` for the same well

### Implementation:
Use `df.groupby(level="well_id", group_keys=False)["OilProduced"].shift(1)`.
The first record per well → `NaN` (no prior period to lag from).

### Correctness constraint (TR-09c):
The lag-1 value at month N must equal the raw production value at month N-1.
This must be verified for at least 3 consecutive months in the test cases.

**Meta derivation:**
Derive meta by calling `_add_lag_features(ddf._meta.copy())` before `map_partitions`
(TR-23).

**Dependencies:** `pandas`

**Test cases (TR-09c):**
- Given `OilProduced=[100, 200, 300]` for one well:
  - `oil_lag1[0]` = NaN (no prior)
  - `oil_lag1[1]` = 100.0 (previous month's value)
  - `oil_lag1[2]` = 200.0 (previous month's value)
- Given 2 wells interleaved in the same partition, assert lags do not bleed across
  well boundaries (well A's first record has NaN lag, not well B's last value).
- Assert `oil_lag1` dtype is `float64` (NaN forces float even if input is Int64).
- TR-23: Call on empty DataFrame; assert output schema matches meta.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task FEA-06: Categorical encoding and days normalization

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_encoded_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Partition-level function (called via `map_partitions`) that adds ML-friendly encoded
columns for categorical and count features. Must be pure and stateless.

### Encoded columns computed:

**`wellstatus_encoded`** — ordinal integer encoding of `WellStatus` for ML use.
Map the declared category values to integers in this order (consistent across all wells):
`AB=0, AC=1, DG=2, PA=3, PR=4, SI=5, SO=6, TA=7, WO=8`.
If `WellStatus` is `pd.NA` → `wellstatus_encoded = -1` (sentinel for unknown).
Result dtype: `Int64` (nullable integer, to accommodate the `-1` sentinel and NA cases).

**`days_produced_norm`** — `DaysProduced` normalized to [0, 1] by dividing by 31.0:
`DaysProduced / 31.0`. If `DaysProduced` is NA → `days_produced_norm = pd.NA`.
A value of `0.0` is valid (well did not produce in this month).
Result dtype: `float64`.

### Implementation constraint:
Use a pandas categorical `.cat.codes` mapping or `pd.Categorical(...).codes` for
`WellStatus` encoding — not a Python dict with per-row lookup.

**Meta derivation:**
Derive meta by calling `_add_encoded_features(ddf._meta.copy())` before `map_partitions`
(TR-23).

**Dependencies:** `pandas`

**Test cases:**
- Assert `WellStatus="AC"` → `wellstatus_encoded=1`.
- Assert `WellStatus="SI"` → `wellstatus_encoded=5`.
- Assert `WellStatus=pd.NA` → `wellstatus_encoded=-1`.
- Assert `DaysProduced=31` → `days_produced_norm=1.0`.
- Assert `DaysProduced=0` → `days_produced_norm=0.0`.
- Assert `DaysProduced=pd.NA` → `days_produced_norm=pd.NA`.
- Assert `days_produced_norm` dtype is `float64`.
- Assert `wellstatus_encoded` dtype is `Int64`.
- TR-23: Call on empty DataFrame; assert output schema matches meta.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task FEA-07: Features pipeline assembler

**Module:** `cogcc_pipeline/features.py`
**Function:** `run_features(config_path: str = "config/config.yaml") -> None`

**Description:**
Top-level entry point for the features stage. Orchestrates all feature computation
steps and writes ML-ready Parquet to `data/features/`.

### Processing steps (in order):

1. `load_config(config_path)` from `cogcc_pipeline.utils`.
2. Read `data/processed/` with `dd.read_parquet(processed_dir)` — sorted by
   `production_date` within each partition (transform guarantee).
3. Apply `_add_cumulative_features` via `map_partitions`.
4. Apply `_add_ratio_features` via `map_partitions`.
5. Apply `_add_decline_rates` via `map_partitions`.
6. Apply `_add_rolling_features` via `map_partitions`.
7. Apply `_add_lag_features` via `map_partitions`.
8. Apply `_add_encoded_features` via `map_partitions`.
9. **Repartition:** `ddf.repartition(npartitions=get_partition_count(n))` where
   `n` is derived from `len(list(Path(processed_dir).glob("*.parquet")))`.
   Repartition must be the last operation before writing (ADR-004).
10. **Write Parquet:** `ddf.to_parquet(features_dir, write_index=True, overwrite=True)`.
11. Log total feature columns in output schema.

### Scheduler:
Reuse existing Dask `Client`. Do not initialize a new cluster.

### meta= derivation for each map_partitions call:
For each of the 6 `map_partitions` calls, derive meta by applying the partition
function to `ddf._meta.copy()` at the time of chaining. Chain all 6
`map_partitions` calls lazily before repartition — do not compute between calls.

**Error handling:**
- If `processed_dir` has no Parquet files → log WARNING and return.
- Propagate any exception from feature functions; log ERROR with function name.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`, `cogcc_pipeline.utils`

**Test cases:**
- Given synthetic processed Parquet (1 well × 12 months), assert `run_features`
  writes Parquet to `features_dir` without error.
- Assert the Parquet output is readable by `dd.read_parquet` (TR-18).
- TR-19: Read the features Parquet and assert ALL of the following columns are present:
  `production_date`, `OilProduced`, `GasProduced`, `WaterProduced`,
  `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`, `wor`, `wgr`,
  `decline_rate_oil`, `decline_rate_gas`,
  `oil_roll3`, `oil_roll6`, `oil_roll12`,
  `gas_roll3`, `gas_roll6`, `gas_roll12`,
  `water_roll3`, `water_roll6`, `water_roll12`,
  `oil_lag1`, `gas_lag1`, `water_lag1`,
  `wellstatus_encoded`, `days_produced_norm`.
- TR-17: Assert `run_features` does not call `.compute()` before the final write.
- Given an empty `processed_dir`, assert `run_features` returns without raising.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task FEA-08: Pipeline entry point

**Module:** `cogcc_pipeline/pipeline.py`
**Function:** `main() -> None`

**Description:**
The registered CLI entry point (`cogcc-pipeline`) that orchestrates all four pipeline
stages in order. This is the function registered as `cogcc-pipeline` in `pyproject.toml`.

### CLI arguments (via `argparse`):
- `--stages` — space-separated list of stages to run; choices: `acquire`, `ingest`,
  `transform`, `features`, `all`; default: `all`. When `all` is specified, run all
  four in order.
- `--config` — path to `config.yaml`; default: `"config/config.yaml"`.
- `--log-level` — override the log level from config; choices: `DEBUG`, `INFO`,
  `WARNING`, `ERROR`; optional.

### Execution sequence:
1. Parse CLI arguments.
2. Load config from `--config`.
3. Call `setup_logging(log_file, level)` from `cogcc_pipeline.utils` — logging must
   be configured before any stage runs (ADR-006).
4. If stages include `acquire`: call `run_acquire(config_path)`. Time the call and
   log duration as INFO.
5. Initialize Dask distributed `Client` after acquire completes and before ingest
   begins (build-env-manifest.md). Use `dask.scheduler` settings from config:
   - If `dask.scheduler == "local"` → `Client(LocalCluster(n_workers=..., ...))`
   - If `dask.scheduler` is a URL → `Client(address=url)`
   Log the Dask dashboard URL after initialization.
6. If stages include `ingest`: call `run_ingest(config_path)`. Time and log.
7. If stages include `transform`: call `run_transform(config_path)`. Time and log.
8. If stages include `features`: call `run_features(config_path)`. Time and log.
9. On any stage failure, log ERROR with the stage name and exception, then
   `sys.exit(1)` — downstream stages must not run after an upstream failure.
10. Close the Dask `Client` on completion or failure.

**Dependencies:** `argparse`, `sys`, `logging`, `time`, `dask.distributed`,
`cogcc_pipeline.acquire`, `cogcc_pipeline.ingest`, `cogcc_pipeline.transform`,
`cogcc_pipeline.features`, `cogcc_pipeline.utils`

**Test cases:**
- Assert `--stages acquire ingest` runs only `run_acquire` and `run_ingest`, not
  `run_transform` or `run_features` — verify by patching all four stage functions.
- Assert `--stages all` runs all four stages in order (acquire → ingest → transform
  → features).
- Given `run_ingest` raising an exception, assert `run_transform` is never called
  and `sys.exit(1)` is invoked.
- Assert the Dask `Client` is closed in both success and failure scenarios
  (use `try/finally` in implementation).
- Assert `setup_logging` is called before any stage function is called — verify
  call order using `pytest-mock` call tracking.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task FEA-09: Features stage integration tests

**Module:** `tests/test_features.py`
**Scope:** TR-01, TR-03, TR-06, TR-07, TR-08, TR-09, TR-10, TR-14, TR-17, TR-18,
TR-19, TR-23 (from test-requirements.xml)

**Description:**
Write integration and unit tests covering all domain and technical correctness
requirements for the features stage. All tests use synthetic data — no real files
required.

### TR-01 — Physical bound validation
- Given a synthetic well with one row having `OilProduced=-10` (negative, should have
  been nulled by transform), assert that `_add_ratio_features` and `_add_cumulative_features`
  do not propagate or amplify the negative value in GOR or cumulative columns.
  (Negative values reaching features indicate a transform stage bug — the test
  documents the contract.)

### TR-03 — Decline curve monotonicity
- Given a synthetic 12-month well sequence with all positive `OilProduced`, assert
  that `cum_oil` is monotonically non-decreasing across all 12 rows.
- Assert `cum_oil[i] >= cum_oil[i-1]` for all `i` in `[1, 11]`.

### TR-06 — GOR zero-denominator
- `OilProduced=0, GasProduced=500` → `gor=NaN`, no exception (TR-06a).
- `OilProduced=0, GasProduced=0` → `gor=NaN`, no exception (TR-06b).
- `OilProduced=100, GasProduced=0` → `gor=0.0` (TR-06c).

### TR-07 — Decline rate clip bounds
- Synthetic sequence producing raw decline rate of `-2.0` → clipped to `-1.0` (TR-07a).
- Synthetic sequence producing raw decline rate of `15.0` → clipped to `10.0` (TR-07b).
- Raw decline rate of `0.3` → passes through as `0.3` (TR-07c).
- Prior month `OilProduced=0`, current `OilProduced=0` → `decline_rate_oil=0.0`,
  not `inf` before clipping (TR-07d).

### TR-08 — Cumulative production flat periods
- Sequence with zero-production mid-months → `cum_oil` flat during those months (TR-08a).
- Sequence starting with zeros → `cum_oil` starts at 0 and stays 0 until first
  positive production (TR-08b).
- Sequence with shut-in then resumption → `cum_oil` resumes correctly from flat
  value after resumption (TR-08c).

### TR-09 — Feature calculation correctness
- Rolling 3-month average: hand-compute expected values for a known 6-month sequence
  and assert exact match (TR-09a).
- Rolling window larger than history: first 2 months of a well → `oil_roll3` equals
  partial-window mean, not NaN or 0 (TR-09b, min_periods=1).
- Lag-1 features: for months 2, 3, and 4, assert `oil_lag1` equals `OilProduced`
  from months 1, 2, and 3 respectively (TR-09c).
- GOR formula: confirm the formula is `GasProduced / OilProduced`, not the inverse (TR-09d).
- Water cut formula: confirm denominator is `OilProduced + WaterProduced`, not just
  `OilProduced` (TR-09e).

### TR-10 — Water cut boundary values
- `WaterProduced=0, OilProduced=100` → `water_cut=0.0`, row retained (TR-10a).
- `WaterProduced=200, OilProduced=0` → `water_cut=1.0`, row retained (TR-10b).
- Only values outside `[0, 1]` should be considered invalid.

### TR-14 — Schema stability across wells
- Run `run_features` on 2 wells.
- Read 2 different partition files and assert identical column names, order, and dtypes.

### TR-18 — Parquet readability
- After `run_features`, read every file in `features_dir` with `pd.read_parquet`.
- Assert no exception for any file.

### TR-19 — Feature column presence
- Given a synthetic single-well 12-month input, run `run_features`.
- Read the output and assert ALL required columns are present (full list in FEA-07).

### TR-23 — map_partitions meta schema consistency
- For each of the 6 feature functions (`_add_cumulative_features`,
  `_add_ratio_features`, `_add_decline_rates`, `_add_rolling_features`,
  `_add_lag_features`, `_add_encoded_features`):
  - Call the function on an empty DataFrame with the correct input schema.
  - Assert the output column list and dtypes match the `meta` argument that would
    be passed to `map_partitions`.

**Definition of done:** All TR-01, TR-03, TR-06, TR-07, TR-08, TR-09, TR-10, TR-14,
TR-17, TR-18, TR-19, TR-23 test cases pass, ruff and mypy report no errors,
requirements.txt updated with all third-party packages imported in this task.
