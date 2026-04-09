# Features Component Tasks

**Pipeline package:** `cogcc_pipeline`
**Module file:** `cogcc_pipeline/features.py`
**Test file:** `tests/test_features.py`

## Overview

The features stage reads the processed Parquet dataset, engineers derived columns
required for ML and analytics workflows, and writes a feature-matrix Parquet dataset to
`data/features/`. Derived features include: cumulative production (Np, Gp, Wp), GOR
(gas-oil ratio), water cut, month-over-month decline rates, rolling averages, and lag
features. All computation uses vectorised pandas/numpy inside `map_partitions` callbacks.
Per-well operations (cumulative sums, rolling windows, lag features) use
`groupby().transform()` — never Python for-loops over groups. The stage uses the Dask
distributed scheduler (reuse existing Client or initialise its own).

---

## Feature Definitions

All derived features are computed within `map_partitions` functions that operate on
pandas DataFrames (one partition per call). Partitions are indexed by `well_id` after
`set_index` in the transform stage, so each partition is already sorted by
`production_date` within each well group (guaranteed by the transform stage's final sort
step).

### Cumulative Production
- `cum_oil`: `groupby("well_id")["OilProduced"].transform("cumsum")` — cumulative oil
  in BBL (Np). Must be monotonically non-decreasing per well (TR-03).
- `cum_gas`: cumulative gas in MCF (Gp). Same constraint.
- `cum_water`: cumulative water in BBL (Wp). Same constraint.

### GOR (Gas-Oil Ratio)
- `gor`: `GasProduced / OilProduced`. When `OilProduced == 0` and `GasProduced > 0`,
  result must be `NaN` (not an exception). When both are 0 (shut-in), result must be
  `0.0` or `NaN` — not an exception. When `OilProduced > 0` and `GasProduced == 0`,
  result must be `0.0`. Use `numpy.where` or division with `.replace(0, pd.NA)` before
  dividing, following the rules in TR-06 exactly.

### Water Cut
- `water_cut`: `WaterProduced / (OilProduced + WaterProduced)`.
  - `0.0` is valid (no water, e.g., new well) — preserve as-is (TR-10).
  - `1.0` is valid (100% water, late-life well) — preserve as-is (TR-10).
  - When denominator is 0 (both oil and water are 0), result is `NaN`.
  - Values outside `[0, 1]` after computation indicate data error — replace with `pd.NA`
    and log a `WARNING`.

### Decline Rate
- `decline_rate`: period-over-period oil production decline rate per well:
  `(OilProduced_t-1 - OilProduced_t) / OilProduced_t-1`
  Computed using `groupby("well_id")["OilProduced"].transform(lambda s: s.pct_change() * -1)`.
  - Clip to `[-1.0, 10.0]` after computation (engineering bounds for ML stability, TR-07).
  - When `OilProduced_t-1 == 0` (shut-in month), the raw pct_change is `inf` or `nan` —
    this must be handled before clipping (replace `inf` with `np.nan` using
    `np.where(np.isinf(raw), np.nan, raw)`).
  - After inf-handling, apply `.clip(-1.0, 10.0)`.

### Rolling Averages (from `config["features"]["rolling_windows"]`, default `[3, 6]`)
For each window `w` in rolling windows, for each volume column `[OilProduced, GasProduced, WaterProduced]`:
- `oil_rolling_{w}m`: `groupby("well_id")["OilProduced"].transform(lambda s: s.rolling(w, min_periods=1).mean())`
- `gas_rolling_{w}m`: same for `GasProduced`
- `water_rolling_{w}m`: same for `WaterProduced`
- When window is larger than available history (e.g., first 2 months of a well's life
  with a 3-month window), use `min_periods=1` so the result is a partial-window average
  — not `NaN` or zero (TR-09b).

### Lag Features (from `config["features"]["lag_periods"]`, default `[1, 3]`)
For each lag `l` in lag periods, for `OilProduced` only:
- `oil_lag_{l}m`: `groupby("well_id")["OilProduced"].transform(lambda s: s.shift(l))`
- Lag-1 for month N must equal the raw `OilProduced` for month N-1 (TR-09c).

---

## Design Decisions and Constraints

- All transformation logic inside `map_partitions` must use vectorised pandas/numpy
  operations. Use `groupby().transform()` for all per-well operations — never a Python
  for-loop over groupby groups.
- The `meta=` argument for every `map_partitions` call must be derived by calling the
  function on `ddf._meta.copy()`. Never manually construct meta column lists.
- Physical bound validation: GOR must be non-negative (TR-01). Water cut must be in
  `[0, 1]` (TR-01, TR-10). Production volumes must be non-negative (already enforced by
  transform; assert in tests).
- Cumulative production must be monotonically non-decreasing per well (TR-03).
- Decline rate is clipped to `[-1.0, 10.0]` — these are ML stability bounds, not
  physical constants (TR-07).
- Output directory: `data/features/production_features.parquet/`.
- Repartition before writing: `max(10, min(ddf.npartitions, 50))`.
- The stage uses the Dask distributed scheduler. Check for an existing Client via
  `distributed.get_client()`; catch `ValueError`; create `LocalCluster` + `Client` if
  none exists.
- The required output columns are: `well_id` (index), `production_date`, `OilProduced`,
  `GasProduced`, `WaterProduced`, `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`,
  `decline_rate`, and all rolling and lag columns (TR-19).

---

## Task 01: Implement FeaturesError

**Module:** `cogcc_pipeline/features.py`
**Class:** `class FeaturesError(RuntimeError)`

**Description:**
Thin subclass of `RuntimeError` with no additional attributes. Raised on unrecoverable
feature-engineering failures.

**Test cases:**

- `@pytest.mark.unit` — Assert `FeaturesError` is a subclass of `RuntimeError`.
- `@pytest.mark.unit` — Assert `FeaturesError("msg")` can be raised and caught as a
  `RuntimeError`.

**Definition of done:** `FeaturesError` implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 02: Implement cumulative production feature builder

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_cumulative_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Adds `cum_oil`, `cum_gas`, `cum_water` columns to a pandas partition DataFrame using
`groupby("well_id").transform("cumsum")` for each respective production column. Each
cumulative column has dtype `pd.Float64Dtype()`. Returns the augmented DataFrame.

The input DataFrame may have `well_id` as the index (after `set_index` in transform).
Reset the index before groupby if needed, then restore it.

**Test cases:**

- `@pytest.mark.unit` — Given a synthetic single-well DataFrame with
  `OilProduced=[10.0, 20.0, 30.0]`, assert `cum_oil=[10.0, 30.0, 60.0]` (TR-03).
- `@pytest.mark.unit` — Given a well with `OilProduced=[10.0, 0.0, 5.0]` (shut-in
  month), assert `cum_oil=[10.0, 10.0, 15.0]` — flat during zero month (TR-08).
- `@pytest.mark.unit` — Given two wells in the same partition, assert cumulative values
  are computed independently per well (not summed across wells).
- `@pytest.mark.unit` — Assert `cum_oil` is monotonically non-decreasing per well
  (TR-03): `all(cum_oil.diff().dropna() >= 0)`.
- `@pytest.mark.unit` — Assert output dtypes for `cum_oil`, `cum_gas`, `cum_water` are
  `pd.Float64Dtype()`.
- `@pytest.mark.unit` — Meta consistency (TR-23): call `_add_cumulative_features` on an
  empty DataFrame matching the input schema and assert column names and dtypes match the
  `meta=` argument for `map_partitions`.

**Cumulative flat periods (TR-08):**

- `@pytest.mark.unit` — Synthetic sequence: zero-production months mid-sequence; assert
  cumulative stays flat during zeros (TR-08a).
- `@pytest.mark.unit` — Synthetic sequence: zero-production months at the start (well
  not yet online); assert cumulative is 0 during those months (TR-08b).
- `@pytest.mark.unit` — Synthetic sequence: production resumes after shut-in; assert
  cumulative resumes correctly from the flat value (TR-08c).

**Definition of done:** `_add_cumulative_features` implemented, all test cases pass,
ruff and mypy report no errors, `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 03: Implement GOR and water cut feature builder

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_ratio_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Adds `gor` and `water_cut` columns to the partition DataFrame using vectorised
numpy/pandas operations. Follows the rules from TR-06 and TR-09/TR-10 exactly:

**GOR (`gor = GasProduced / OilProduced`):**
- `OilProduced > 0`: `gor = GasProduced / OilProduced`
- `OilProduced == 0` and `GasProduced > 0`: `gor = NaN`
- `OilProduced == 0` and `GasProduced == 0`: `gor = 0.0` or `NaN` (specify one; prefer `NaN` for mathematical consistency)
- `OilProduced > 0` and `GasProduced == 0`: `gor = 0.0`
- Implement using `numpy.where` or `pandas.where` — no `apply`, no `lambda` row-wise.

**Water Cut (`water_cut = WaterProduced / (OilProduced + WaterProduced)`):**
- When `OilProduced + WaterProduced == 0`: `water_cut = NaN` (not an exception).
- `0.0` and `1.0` are valid values — do not null them (TR-10).
- Values outside `[0, 1]` after computation: replace with `pd.NA` and log a `WARNING`.
- Implement using vectorised division and masking — no `apply`.

Returns the augmented DataFrame. Output dtypes: `gor` and `water_cut` both
`pd.Float64Dtype()`.

**Test cases:**

**GOR tests (TR-06):**
- `@pytest.mark.unit` — `oil_bbl=0`, `gas_mcf=100.0` → `gor` is `NaN` (TR-06a).
- `@pytest.mark.unit` — `oil_bbl=0`, `gas_mcf=0` → `gor` is `NaN` or `0.0`, not an
  exception (TR-06b).
- `@pytest.mark.unit` — `oil_bbl=100.0`, `gas_mcf=0` → `gor == 0.0` (TR-06c).
- `@pytest.mark.unit` — `oil_bbl=100.0`, `gas_mcf=500.0` → `gor == 5.0`.
- `@pytest.mark.unit` — A late-life well with very low `OilProduced` (e.g. 0.1 BBL) and
  high `GasProduced` produces a legitimately high GOR — assert it is retained as-is (not
  treated as a data error).

**Water cut tests (TR-10):**
- `@pytest.mark.unit` — `water_bbl=0`, `oil_bbl=100.0` → `water_cut == 0.0` (TR-10a).
- `@pytest.mark.unit` — `water_bbl=500.0`, `oil_bbl=0` → `water_cut == 1.0` (TR-10b).
- `@pytest.mark.unit` — `water_bbl=0`, `oil_bbl=0` → `water_cut is NaN`.
- `@pytest.mark.unit` — Only values outside `[0, 1]` are treated as invalid (TR-10c).

**Meta consistency (TR-23):**
- `@pytest.mark.unit` — Call `_add_ratio_features` on an empty DataFrame matching input
  schema; assert the column list and dtypes match the `meta=` argument for
  `map_partitions`.

**Physical bounds (TR-01):**
- `@pytest.mark.unit` — Assert `gor` is always non-negative where not `NaN`.
- `@pytest.mark.unit` — Assert `water_cut` is always in `[0, 1]` where not `NaN`.

**Definition of done:** `_add_ratio_features` implemented, all test cases pass (including
TR-01, TR-06, TR-09d, TR-09e, TR-10), ruff and mypy report no errors, `requirements.txt`
updated with all third-party packages imported in this task.

---

## Task 04: Implement decline rate feature builder

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_decline_rates(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Adds `decline_rate` column using vectorised per-well computation:

1. Compute raw pct_change per well using
   `groupby("well_id")["OilProduced"].transform(lambda s: s.pct_change() * -1)`.
2. Replace `inf` and `-inf` values with `np.nan` using
   `np.where(np.isinf(raw), np.nan, raw)` before clipping.
3. Clip to `[-1.0, 10.0]` using `.clip(-1.0, 10.0)`.
4. Return the augmented DataFrame. Output dtype: `pd.Float64Dtype()`.

**Test cases (TR-07):**

- `@pytest.mark.unit` — Given `OilProduced=[100.0, 50.0, 200.0]` for one well, assert
  `decline_rate[1] == 0.5` (50% decline), `decline_rate[2] == -3.0` (clipped to max
  growth: 200 from 50 = -300% raw, clipped to -1.0 at lower bound — verify clip
  direction carefully against the formula).
- `@pytest.mark.unit` — A decline rate below `-1.0` is clipped to exactly `-1.0`
  (TR-07a).
- `@pytest.mark.unit` — A decline rate above `10.0` is clipped to exactly `10.0`
  (TR-07b).
- `@pytest.mark.unit` — A value within `[-1.0, 10.0]` passes through unchanged (TR-07c).
- `@pytest.mark.unit` — Two consecutive zero-production months (shut-in) do not produce
  an unclipped extreme value before clipping: verify the `inf` → `NaN` handling fires
  before clip (TR-07d).
- `@pytest.mark.unit` — Two wells in the same partition produce independent decline
  rates (no cross-well contamination).
- `@pytest.mark.unit` — Meta consistency (TR-23): call `_add_decline_rates` on an empty
  DataFrame matching input schema; assert column list and dtypes match `meta=`.

**Definition of done:** `_add_decline_rates` implemented, all test cases pass (including
TR-07), ruff and mypy report no errors, `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 05: Implement rolling average feature builder

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_rolling_features(df: pd.DataFrame, windows: list[int]) -> pd.DataFrame`

**Description:**
For each window `w` in `windows`, adds three rolling average columns for
`OilProduced`, `GasProduced`, `WaterProduced` per well:
- `oil_rolling_{w}m`
- `gas_rolling_{w}m`
- `water_rolling_{w}m`

Uses `groupby("well_id")[col].transform(lambda s: s.rolling(w, min_periods=1).mean())`
for each column. When the available history is shorter than the window, `min_periods=1`
ensures a partial-window mean is returned (not `NaN`). All rolling average columns have
dtype `pd.Float64Dtype()`.

**Test cases (TR-09):**

- `@pytest.mark.unit` — Given a well with `OilProduced=[10.0, 20.0, 30.0, 40.0]` and
  `window=3`, assert `oil_rolling_3m=[10.0, 15.0, 20.0, 30.0]` (hand-computed 3-month
  rolling mean) (TR-09a).
- `@pytest.mark.unit` — Given a well with only 2 months of history and `window=3`,
  assert the result is a partial-window mean (not `NaN` or zero) for both months
  (TR-09b).
- `@pytest.mark.unit` — For `window=6`, verify a 6-month rolling average against a
  known sequence of 8 months (TR-09a).
- `@pytest.mark.unit` — Two wells in the same partition produce independent rolling
  means.
- `@pytest.mark.unit` — Meta consistency (TR-23): call `_add_rolling_features` on an
  empty DataFrame matching input schema; assert column list and dtypes match `meta=`.

**Definition of done:** `_add_rolling_features` implemented, all test cases pass
(including TR-09a, TR-09b), ruff and mypy report no errors, `requirements.txt` updated
with all third-party packages imported in this task.

---

## Task 06: Implement lag feature builder

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_lag_features(df: pd.DataFrame, lags: list[int]) -> pd.DataFrame`

**Description:**
For each lag `l` in `lags`, adds `oil_lag_{l}m` column:
`groupby("well_id")["OilProduced"].transform(lambda s: s.shift(l))`

The first `l` records per well will be `NaN` (no prior history). All lag columns have
dtype `pd.Float64Dtype()`. Lag-1 feature for month N must equal raw `OilProduced` for
month N-1 (TR-09c).

**Test cases (TR-09):**

- `@pytest.mark.unit` — Given `OilProduced=[10.0, 20.0, 30.0]` with `lag=1`, assert
  `oil_lag_1m=[NaN, 10.0, 20.0]` (TR-09c).
- `@pytest.mark.unit` — Given `lag=3`, assert first 3 values are `NaN` and 4th value
  equals `OilProduced[0]`.
- `@pytest.mark.unit` — Two wells in the same partition produce independent lag features.
- `@pytest.mark.unit` — Meta consistency (TR-23): call `_add_lag_features` on an empty
  DataFrame matching input schema; assert column list and dtypes match `meta=`.

**Definition of done:** `_add_lag_features` implemented, all test cases pass (including
TR-09c), ruff and mypy report no errors, `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 07: Implement partition-level features orchestrator (map_partitions callback)

**Module:** `cogcc_pipeline/features.py`
**Function:** `_engineer_features_partition(df: pd.DataFrame, windows: list[int], lags: list[int]) -> pd.DataFrame`

**Description:**
Applied via `map_partitions` to each Dask partition. Executes all feature engineering in
order:
1. `df = _add_cumulative_features(df)`
2. `df = _add_ratio_features(df)`
3. `df = _add_decline_rates(df)`
4. `df = _add_rolling_features(df, windows)`
5. `df = _add_lag_features(df, lags)`
6. Return the augmented DataFrame.

The `meta=` argument for `ddf.map_partitions(_engineer_features_partition, windows, lags, meta=meta)`
must be derived by calling `_engineer_features_partition(ddf._meta.copy(), windows, lags)`.

**Test cases:**

- `@pytest.mark.unit` — Given a synthetic single-well partition, assert all expected
  output columns are present (TR-19): `OilProduced`, `GasProduced`, `WaterProduced`,
  `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`, `decline_rate`,
  `oil_rolling_3m`, `oil_rolling_6m`, `gas_rolling_3m`, `gas_rolling_6m`,
  `water_rolling_3m`, `water_rolling_6m`, `oil_lag_1m`, `oil_lag_3m`.
- `@pytest.mark.unit` — Verify formula correctness for GOR, water cut, and 3-month
  rolling against hand-computed values (TR-09d, TR-09e).
- `@pytest.mark.unit` — Meta consistency (TR-23): call `_engineer_features_partition` on
  `ddf._meta.copy()` and assert column names, order, and dtypes match the `meta=`
  argument passed to `map_partitions`.

**Definition of done:** `_engineer_features_partition` implemented, all test cases pass
(including TR-09, TR-19, TR-23), ruff and mypy report no errors, `requirements.txt`
updated with all third-party packages imported in this task.

---

## Task 08: Implement features output validator

**Module:** `cogcc_pipeline/features.py`
**Function:** `validate_feature_output(ddf: dask.dataframe.DataFrame, config: dict) -> None`

**Description:**
Validates the feature-engineered Dask DataFrame before writing:
1. Assert all required output columns are present (TR-19). Raise `FeaturesError` listing
   missing columns if not.
2. Assert `GOR` is non-negative where not null (TR-01). Log `WARNING` for violations.
3. Assert `water_cut` is in `[0, 1]` where not null (TR-01). Log `WARNING` for violations.
4. Assert `decline_rate` is within `[-1.0, 10.0]` where not null (TR-07). Log `WARNING`
   for violations.
5. Assert schema is consistent across a sample of 3 partitions (TR-14): column names and
   dtypes are identical. Raise `FeaturesError` if schema drift is detected.

All assertions use `dask.compute()` with batched calls — never sequential `.compute()`.

**Test cases:**

- `@pytest.mark.unit` — Given a Dask DataFrame with all required columns, assert no
  exception is raised (TR-19).
- `@pytest.mark.unit` — Given a Dask DataFrame missing `gor`, assert `FeaturesError` is
  raised.
- `@pytest.mark.unit` — Given a DataFrame with `gor=-1.0`, assert a `WARNING` is
  logged.
- `@pytest.mark.unit` — Given a DataFrame with `water_cut=1.5`, assert a `WARNING` is
  logged.
- `@pytest.mark.unit` — Given a DataFrame with `decline_rate=15.0`, assert a `WARNING`
  is logged.

**Definition of done:** `validate_feature_output` implemented, all test cases pass (TR-01,
TR-07, TR-14, TR-19), ruff and mypy report no errors, `requirements.txt` updated with all
third-party packages imported in this task.

---

## Task 09: Implement features stage orchestrator

**Module:** `cogcc_pipeline/features.py`
**Function:** `run_features(config: dict) -> dask.dataframe.DataFrame`

**Description:**
Orchestrates the full features stage:
1. Detect or initialise Dask distributed Client (try `distributed.get_client()`, catch
   `ValueError`, create `LocalCluster` + `Client` from `config["dask"]`).
2. Read processed Parquet: `ddf = dd.read_parquet(config["features"]["processed_dir"] + "/production.parquet", engine="pyarrow")`.
3. Read `windows` from `config["features"]["rolling_windows"]` and `lags` from
   `config["features"]["lag_periods"]`.
4. Build `meta` by calling `_engineer_features_partition(ddf._meta.copy(), windows, lags)`.
5. Apply features: `ddf = ddf.map_partitions(_engineer_features_partition, windows, lags, meta=meta)`.
6. Call `validate_feature_output(ddf, config)`.
7. Repartition: `ddf = ddf.repartition(npartitions=max(10, min(ddf.npartitions, 50)))`.
8. Write: `ddf.to_parquet(config["features"]["output_dir"] + "/production_features.parquet", engine="pyarrow", write_index=True, overwrite=True)`.
9. Return the Dask DataFrame (do NOT call `.compute()` on the full dataset).

**Non-negotiable:** Do NOT call `.compute()` inside `run_features` except in
`validate_feature_output` (which batches its calls via `dask.compute()`). Return the
Dask DataFrame (TR-17).

**Test cases:**

- `@pytest.mark.unit` — Mock all sub-functions; assert `run_features` returns a
  `dask.dataframe.DataFrame` (TR-17).
- `@pytest.mark.unit` — Assert `.compute()` is never called on the full dataset inside
  `run_features` (TR-17).
- `@pytest.mark.integration` — Run `run_features(config)` against
  `data/processed/production.parquet/`; assert `data/features/production_features.parquet/`
  exists and is readable (TR-18).

**Feature column presence (TR-19):**

- `@pytest.mark.integration` — After `run_features`, read the output Parquet and assert
  all required columns are present: `production_date`, `OilProduced`, `GasProduced`,
  `WaterProduced`, `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`,
  `decline_rate`, `oil_rolling_3m`, `oil_rolling_6m`, `gas_rolling_3m`, `gas_rolling_6m`,
  `water_rolling_3m`, `water_rolling_6m`, `oil_lag_1m`, `oil_lag_3m`.

**Monotonicity (TR-03):**

- `@pytest.mark.integration` — After `run_features`, sample one well's data from the
  features Parquet and assert `cum_oil.diff().dropna() >= 0` for all rows.

**Schema stability (TR-14):**

- `@pytest.mark.integration` — Read two different partition files from
  `data/features/production_features.parquet/`; assert column names and dtypes are
  identical.

**Parquet readability (TR-18):**

- `@pytest.mark.integration` — After `run_features`, assert every `.parquet` file in
  `data/features/production_features.parquet/` is readable by `pd.read_parquet` without
  error.

**Feature calculation correctness (TR-09) — synthetic end-to-end test:**

- `@pytest.mark.unit` — Create a synthetic processed-schema Dask DataFrame for a single
  well with 8 months of known `OilProduced`, `GasProduced`, `WaterProduced` values.
  Call `run_features` (with mocked I/O); assert:
  - (a) 3-month and 6-month rolling oil, gas, and water averages match hand-computed
    values for months 3–8 (TR-09a).
  - (b) First 2 months with `window=3` return partial-window means, not zero or NaN
    (TR-09b).
  - (c) `oil_lag_1m` for month N equals `OilProduced` for month N-1 for months 2–8
    (TR-09c).
  - (d) `gor` equals `GasProduced / OilProduced` with correct unit handling (TR-09d).
  - (e) `water_cut` equals `WaterProduced / (OilProduced + WaterProduced)` (TR-09e).

**Definition of done:** `run_features` implemented, all test cases pass (including TR-01,
TR-03, TR-07, TR-09, TR-14, TR-17, TR-18, TR-19, TR-23), ruff and mypy report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 10: Implement pipeline orchestrator

**Module:** `cogcc_pipeline/pipeline.py`
**Functions:**
- `run_pipeline(stages: list[str] | None = None) -> None`
- `main() -> None`

**Description:**
`run_pipeline` chains all four stages in order: acquire → ingest → transform → features.
When `stages` is `None`, all four run. When a subset is provided, only those stages run
in the declared order (ignore stages not in the list but maintain relative order).

Steps:
1. Load config from `config.yaml` using `load_config()`.
2. Call `setup_logging(config)` to configure logging to stdout and `logs/pipeline.log`.
3. Run `run_acquire(config)` if `"acquire"` in stages. Log per-stage timing.
4. Initialise Dask distributed Client after acquire completes and before ingest begins:
   - If `config["dask"]["scheduler"] == "local"`, create `LocalCluster(n_workers=...,
     threads_per_worker=..., memory_limit=...)` and `Client(cluster)`.
   - If `config["dask"]["scheduler"]` is a URL (e.g., `"tcp://host:port"`), create
     `Client(scheduler)`.
   - Log the dashboard URL at `INFO` level.
5. Run `run_ingest(config)` if `"ingest"` in stages.
6. Run `run_transform(config)` if `"transform"` in stages.
7. Run `run_features(config)` if `"features"` in stages.
8. On any stage failure, log the error at `ERROR` level and raise `RuntimeError` —
   preventing downstream stages from running.
9. Close the Dask Client and cluster on completion or error (use `finally` block).

`main()` is the CLI entry point registered in `pyproject.toml`. It must parse optional
`--stages` argument (comma-separated string, e.g. `--stages acquire,ingest`). When no
argument is provided, run all stages.

**Dependencies:** `dask.distributed`, `logging`, `argparse`, `time`

**Test cases:**

- `@pytest.mark.unit` — Mock all stage `run_*` functions; assert they are called in
  order when `stages=None`.
- `@pytest.mark.unit` — Given `stages=["ingest", "transform"]`, assert `run_acquire`
  and `run_features` are NOT called.
- `@pytest.mark.unit` — Given a stage that raises `RuntimeError`, assert subsequent
  stages are not called and the error is re-raised.
- `@pytest.mark.unit` — Assert that the Dask Client is initialised after acquire and
  before ingest (mock the Client constructor and verify call order).
- `@pytest.mark.integration` — Run `main()` end-to-end with `--stages
  acquire,ingest,transform,features` against real data; assert
  `data/features/production_features.parquet/` exists and is non-empty.

**Definition of done:** `run_pipeline` and `main()` implemented, all test cases pass,
`cogcc-pipeline` CLI entry point works, `make pipeline` runs end-to-end, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported in
this task.
