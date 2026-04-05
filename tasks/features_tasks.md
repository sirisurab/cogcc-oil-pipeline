# Features Stage — Task Specifications

## Overview

The features stage reads the cleaned Parquet dataset produced by the transform stage,
engineers ML-ready features for each well, encodes categorical variables, normalises
numeric columns, and writes a final feature matrix Parquet file to
`data/processed/cogcc_production_features.parquet/`.

Feature engineering is performed per-well using Dask's `groupby` and `map_partitions`
mechanisms. The final output is a flat feature matrix with one row per
`(well_id, production_date)` pair, suitable for supervised and unsupervised ML workflows.

The pipeline package is named `cogcc_pipeline`. All source modules live under
`cogcc_pipeline/`. All test files live under `tests/`.

---

## Domain Context (from oil-and-gas-domain-expert)

### Cumulative Production (Np, Gp, Wp)
Cumulative production is the running total of all oil, gas, and water produced by a well
since its first production record. Because a well cannot "un-produce" fluid, cumulative
values must be monotonically non-decreasing over time (TR-03). Shut-in months (zero
production) result in a flat cumulative — the value from the previous month is carried
forward (TR-08).

### Gas-Oil Ratio (GOR)
GOR = `gas_mcf / oil_bbl`. When `oil_bbl = 0` and `gas_mcf > 0`, the result is
mathematically undefined but physically meaningful (pure gas well or pressure-depleted
well producing below bubble point). Return `NaN` in this case rather than raising an
exception or producing `Inf` (TR-06). When both are zero (shut-in), return `0.0` or
`NaN` but not an exception. When `oil_bbl > 0` and `gas_mcf = 0`, return `0.0`.

### Water Cut
Water cut = `water_bbl / (oil_bbl + water_bbl)`. When the denominator is zero (no
liquid produced), return `NaN`. Values of exactly `0.0` and exactly `1.0` are physically
valid (TR-10). Never flag water_cut of 0 or 1 as an outlier.

### Production Decline Rate
Decline rate captures the period-over-period fractional change in oil production:
`decline_rate = (oil_bbl_prev - oil_bbl_curr) / oil_bbl_prev`.
When `oil_bbl_prev = 0` (previous month was shut-in), the raw computation is undefined.
Handle this before clipping: if `oil_bbl_prev = 0` and `oil_bbl_curr > 0`, set decline
rate to `-1.0` (step-change from no production). If both are zero, set to `0.0` (TR-07).
After computing the raw value, clip to the range `[-1.0, 10.0]` (ML feature stability
bounds, not physical constants).

### Rolling Averages
Compute rolling averages over windows of 3, 6, and 12 months for `oil_bbl`, `gas_mcf`,
and `water_bbl` per well, sorted by `production_date`. Use a minimum period of 1 so that
early months produce a partial-window average rather than NaN. For ML purposes, having
some value is preferable to NaN — but document the minimum-periods choice in the
function docstring. Note: when the window is larger than the available history, the result
should follow the `min_periods=1` rule (TR-09b).

### Lag Features
Lag-1 features (previous month's value) for `oil_bbl`, `gas_mcf`, `water_bbl` per well.
For the first record of a well, lag-1 is `NaN` (no prior month exists).

### Well Age
Well age in months = number of months elapsed since the well's first production date
(where `production_date` equals the well's `min(production_date)`). The first month has
age 0.

### Days-Normalised Production
Normalise monthly production volumes to a 30-day equivalent:
`oil_bbl_30d = oil_bbl * 30.0 / days_produced`.
When `days_produced = 0` or null, return `NaN`.

### Categorical Encoding
Encode `OpName`, `Well`, and county (derived from `well_id` characters 2–4) as integer
label-encoded columns. Store the encoding mappings as JSON artefacts alongside the
feature matrix. Do not use one-hot encoding — the cardinality is too high for one-hot.

### Numeric Normalisation
Apply `sklearn.preprocessing.StandardScaler` to the following columns:
`oil_bbl`, `gas_mcf`, `water_bbl`, `cum_oil`, `cum_gas`, `cum_water`,
`gor`, `water_cut`, `decline_rate`, `well_age_months`,
`oil_bbl_rolling_3m`, `oil_bbl_rolling_6m`, `oil_bbl_rolling_12m`.
Fit the scaler on the training set only. At this stage, fit on the full dataset
(no train/test split — that is an ML workflow concern). Save the fitted scaler as a
`joblib` pickle to `data/processed/scaler.joblib`.

---

## Output Schema

The feature matrix Parquet must contain the following columns (TR-19):

| Column | Type | Description |
|---|---|---|
| `well_id` | string | 10-digit API well number |
| `production_date` | datetime64[ns] | First day of production month |
| `oil_bbl` | float | Monthly oil production BBL |
| `gas_mcf` | float | Monthly gas production MCF |
| `water_bbl` | float | Monthly water production BBL |
| `days_produced` | float | Days producing in month |
| `cum_oil` | float | Cumulative oil BBL |
| `cum_gas` | float | Cumulative gas MCF |
| `cum_water` | float | Cumulative water BBL |
| `gor` | float | Gas-oil ratio (gas_mcf / oil_bbl) |
| `water_cut` | float | Water cut (water_bbl / (oil_bbl + water_bbl)) |
| `decline_rate` | float | Period-over-period oil decline rate, clipped [-1.0, 10.0] |
| `well_age_months` | int | Months since first production |
| `oil_bbl_30d` | float | Oil production normalised to 30 days |
| `gas_mcf_30d` | float | Gas production normalised to 30 days |
| `water_bbl_30d` | float | Water production normalised to 30 days |
| `oil_bbl_rolling_3m` | float | 3-month rolling average oil BBL |
| `oil_bbl_rolling_6m` | float | 6-month rolling average oil BBL |
| `oil_bbl_rolling_12m` | float | 12-month rolling average oil BBL |
| `gas_mcf_rolling_3m` | float | 3-month rolling average gas MCF |
| `gas_mcf_rolling_6m` | float | 6-month rolling average gas MCF |
| `gas_mcf_rolling_12m` | float | 12-month rolling average gas MCF |
| `water_bbl_rolling_3m` | float | 3-month rolling average water BBL |
| `water_bbl_rolling_6m` | float | 6-month rolling average water BBL |
| `water_bbl_rolling_12m` | float | 12-month rolling average water BBL |
| `oil_bbl_lag1` | float | Previous month oil BBL |
| `gas_mcf_lag1` | float | Previous month gas MCF |
| `water_bbl_lag1` | float | Previous month water BBL |
| `op_name_encoded` | int | Label-encoded operator name |
| `county_encoded` | int | Label-encoded county code |

---

## Component Architecture

### Modules and files produced by this stage

| Artefact | Purpose |
|---|---|
| `cogcc_pipeline/features.py` | All feature engineering logic |
| `data/processed/cogcc_production_features.parquet/` | ML-ready feature matrix |
| `data/processed/scaler.joblib` | Fitted StandardScaler |
| `data/processed/label_encoders.json` | Categorical encoding mappings |
| `tests/test_features.py` | Pytest unit and integration tests |

---

## Design Decisions and Constraints

- **Dask throughout**: All functions accept and return `dask.dataframe.DataFrame`. No
  `.compute()` inside per-well feature computation functions (TR-17).
- **Per-well operations**: Use `dask.dataframe.groupby` + `apply` or `map_partitions`
  to apply per-well sorted operations (cumulative, rolling, lag). The input must be sorted
  by `["well_id", "production_date"]` before any per-well operation.
- **meta dtypes**: All `map_partitions` meta DataFrames must use `pd.StringDtype()` for
  string columns. Never use `"object"` dtype in `meta=`.
- **Repartition after read**: Immediately repartition to `min(npartitions, 50)` after
  reading the cleaned Parquet input.
- **Output file count**: Target 20–50 Parquet files. Repartition to 30 partitions before
  writing. Do NOT use `partition_on="well_id"`.
- **Parquet write**: `ddf.repartition(npartitions=30).to_parquet(..., write_index=False, engine="pyarrow", overwrite=True)`.
- **Zero distinction**: Zeros in production columns must remain zeros throughout feature
  engineering. Do not coerce zeros to NaN when computing cumulative or rolling values.
- **Type hints and docstrings**: All public functions and classes.

---

## Task 23: Implement per-well cumulative production feature

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_cumulative_production(df: pd.DataFrame) -> pd.DataFrame`

**Description:**

This function operates on a single-well pandas DataFrame (sorted ascending by
`production_date`). Compute cumulative sums:
- `cum_oil = df["oil_bbl"].fillna(0).cumsum()`
- `cum_gas = df["gas_mcf"].fillna(0).cumsum()`
- `cum_water = df["water_bbl"].fillna(0).cumsum()`

Note: `fillna(0)` is applied before `cumsum` so that null months (missing data) do not
propagate NaN into the cumulative. This is distinct from zero production months — zeros
are preserved as zeros before the cumsum.

Return the input DataFrame with three new columns: `cum_oil`, `cum_gas`, `cum_water`.

This function is designed to be called inside a `groupby(...).apply(...)` or
`map_partitions` per-well loop — it is not a Dask function.

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` (TR-03) — Given a synthetic 12-month well sequence with
  `oil_bbl = [100, 200, 150, 0, 0, 50, 75, 100, 80, 90, 110, 120]`, assert `cum_oil`
  is monotonically non-decreasing across all 12 rows.
- `@pytest.mark.unit` (TR-08a) — Given a sequence with consecutive zero-production
  months (e.g. months 4 and 5 have `oil_bbl = 0`), assert `cum_oil` is flat (equal to
  the prior month's value) during those months.
- `@pytest.mark.unit` (TR-08b) — Given zero-production months at the START of the record
  (well not yet online), assert `cum_oil` starts at 0 and remains 0 until first positive
  production.
- `@pytest.mark.unit` (TR-08c) — Given production that resumes after a shut-in period,
  assert `cum_oil` resumes correctly from the prior cumulative value (not reset to 0).
- `@pytest.mark.unit` — Given a well with null `oil_bbl` for one month (missing data,
  not zero), assert `cum_oil` treats it as 0 for the purpose of cumsum (NaN does not
  halt the cumulative sum).

**Definition of done:** `compute_cumulative_production` implemented; all tests pass;
`ruff` and `mypy` report no errors; `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 24: Implement GOR and water cut features

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_ratios(df: pd.DataFrame) -> pd.DataFrame`

**Description:**

Compute `gor` and `water_cut` columns for a single-well (or any) pandas DataFrame.

GOR computation:
- If `oil_bbl > 0`: `gor = gas_mcf / oil_bbl`
- If `oil_bbl == 0` and `gas_mcf > 0`: `gor = NaN`
- If `oil_bbl == 0` and `gas_mcf == 0`: `gor = NaN`
- If either input is null: `gor = NaN`

Water cut computation:
- `denominator = oil_bbl + water_bbl`
- If `denominator > 0`: `water_cut = water_bbl / denominator`
- If `denominator == 0`: `water_cut = NaN`
- If either input is null: `water_cut = NaN`

Do not use Python division directly — use `numpy.where` or `pandas.where` to handle
zero denominators without raising `ZeroDivisionError` or producing `Inf`.

Return the input DataFrame with `gor` and `water_cut` added.

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` (TR-06a) — `oil_bbl=0, gas_mcf=100` → assert `gor` is `NaN`,
  no exception raised.
- `@pytest.mark.unit` (TR-06b) — `oil_bbl=0, gas_mcf=0` → assert `gor` is `NaN` or
  `0`, no exception raised.
- `@pytest.mark.unit` (TR-06c) — `oil_bbl=100, gas_mcf=0` → assert `gor == 0.0`.
- `@pytest.mark.unit` (TR-06) — `oil_bbl=200, gas_mcf=400` → assert `gor == 2.0`.
- `@pytest.mark.unit` (TR-09d) — Assert the GOR formula uses `gas_mcf / oil_bbl` (not
  the unit-swapped variant): given `oil_bbl=1, gas_mcf=5`, assert `gor == 5.0`.
- `@pytest.mark.unit` (TR-09e) — `water_bbl=30, oil_bbl=70` → assert
  `water_cut == 0.3`.
- `@pytest.mark.unit` (TR-10a) — `water_bbl=0, oil_bbl=500` → assert
  `water_cut == 0.0` (valid, retained).
- `@pytest.mark.unit` (TR-10b) — `water_bbl=500, oil_bbl=0` → assert
  `water_cut == 1.0` (valid end-of-life state, retained).
- `@pytest.mark.unit` (TR-10c) — Only values outside [0, 1] are treated as invalid;
  given a row with `water_cut = 0.0`, assert it is not flagged or nulled.

**Definition of done:** `compute_ratios` implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 25: Implement production decline rate feature

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_decline_rate(df: pd.DataFrame) -> pd.DataFrame`

**Description:**

Compute `decline_rate` for a single-well pandas DataFrame (sorted ascending by
`production_date`). The formula is period-over-period:
`decline_rate = (oil_bbl_prev - oil_bbl_curr) / oil_bbl_prev`
where `oil_bbl_prev` is the previous month's `oil_bbl` (lag-1).

Before dividing, handle zero denominators:
- If `oil_bbl_prev == 0` and `oil_bbl_curr > 0`: set `decline_rate = -1.0`.
- If `oil_bbl_prev == 0` and `oil_bbl_curr == 0`: set `decline_rate = 0.0`.
- If `oil_bbl_prev` is null (first record): `decline_rate = NaN`.

After computing the raw value, clip to the range `[-1.0, 10.0]`.

Return the input DataFrame with `decline_rate` added.

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` (TR-07a) — Construct a synthetic sequence where a computed
  pre-clip decline rate is `-2.5`. Assert `decline_rate == -1.0` after clipping.
- `@pytest.mark.unit` (TR-07b) — Construct a sequence where the pre-clip rate is `15.0`.
  Assert `decline_rate == 10.0` after clipping.
- `@pytest.mark.unit` (TR-07c) — Construct a sequence where the rate is `0.5`. Assert
  `decline_rate == 0.5` (within bounds, unchanged).
- `@pytest.mark.unit` (TR-07d) — Construct a well with `oil_bbl = [0, 0]` for two
  consecutive months. Assert `decline_rate == 0.0` (shut-in does not produce an unclipped
  extreme value).
- `@pytest.mark.unit` — Construct a sequence where `oil_bbl_prev = 0` and
  `oil_bbl_curr = 100`. Assert `decline_rate == -1.0`.
- `@pytest.mark.unit` — For the first record of a well (no prior month), assert
  `decline_rate` is `NaN`.

**Definition of done:** `compute_decline_rate` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 26: Implement rolling average and lag features

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_rolling_and_lag(df: pd.DataFrame, windows: list[int]) -> pd.DataFrame`

**Description:**

Operate on a single-well pandas DataFrame sorted ascending by `production_date`.

Compute rolling averages for `oil_bbl`, `gas_mcf`, `water_bbl` for each window size in
`windows` (e.g. `[3, 6, 12]`). Use `df[col].rolling(window=w, min_periods=1).mean()`.
Name the resulting columns `{col}_rolling_{w}m` (e.g. `oil_bbl_rolling_3m`).

Compute lag-1 features for `oil_bbl`, `gas_mcf`, `water_bbl`:
`{col}_lag1 = df[col].shift(1)`.
The first record of each well will have `NaN` for lag features.

Return the input DataFrame with all rolling and lag columns added.

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` (TR-09a) — Given a 6-month sequence
  `oil_bbl = [100, 200, 150, 300, 250, 400]`, compute `oil_bbl_rolling_3m` and assert
  the value for month 3 (index 2) equals `(100 + 200 + 150) / 3 = 150.0`.
- `@pytest.mark.unit` (TR-09a) — Assert the value for month 6 (index 5) of
  `oil_bbl_rolling_3m` equals `(300 + 250 + 400) / 3 = 316.67` (±0.01).
- `@pytest.mark.unit` (TR-09b) — Given only 2 months of history, assert
  `oil_bbl_rolling_3m` for month 2 uses the partial window (2 values) and is NOT zero
  or NaN (due to `min_periods=1`). Specifically, assert it equals the mean of the first
  2 values.
- `@pytest.mark.unit` (TR-09c) — Given a 3-month sequence, assert `oil_bbl_lag1` for
  month 2 equals the `oil_bbl` value of month 1, and `oil_bbl_lag1` for month 3 equals
  `oil_bbl` of month 2. Assert month 1 lag-1 is `NaN`.
- `@pytest.mark.unit` — Given `windows=[3, 6, 12]`, assert the returned DataFrame
  contains exactly the columns `oil_bbl_rolling_3m`, `oil_bbl_rolling_6m`,
  `oil_bbl_rolling_12m` (and corresponding gas/water columns).

**Definition of done:** `compute_rolling_and_lag` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 27: Implement well age and days-normalised production features

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_well_age_and_normalised(df: pd.DataFrame) -> pd.DataFrame`

**Description:**

Operate on a single-well pandas DataFrame sorted ascending by `production_date`.

Compute `well_age_months`:
`well_age_months = (production_date.dt.year - first_date.year) * 12 + (production_date.dt.month - first_date.month)`
where `first_date = df["production_date"].min()`.
The first month has `well_age_months = 0`.

Compute days-normalised production:
- `oil_bbl_30d = oil_bbl * 30.0 / days_produced` (when `days_produced > 0`)
- `gas_mcf_30d = gas_mcf * 30.0 / days_produced` (when `days_produced > 0`)
- `water_bbl_30d = water_bbl * 30.0 / days_produced` (when `days_produced > 0`)
- When `days_produced` is null or 0, set the normalised value to `NaN`.

Return the input DataFrame with `well_age_months`, `oil_bbl_30d`, `gas_mcf_30d`,
`water_bbl_30d` added.

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` — Given a well with first `production_date = 2021-01-01`, assert
  `well_age_months = 0` for Jan 2021 and `well_age_months = 11` for Dec 2021.
- `@pytest.mark.unit` — Given `oil_bbl = 300, days_produced = 15`, assert
  `oil_bbl_30d == 600.0`.
- `@pytest.mark.unit` — Given `days_produced = 0`, assert `oil_bbl_30d` is `NaN`.
- `@pytest.mark.unit` — Given `days_produced = null`, assert `oil_bbl_30d` is `NaN`.

**Definition of done:** `compute_well_age_and_normalised` implemented; all tests pass;
`ruff` and `mypy` report no errors; `requirements.txt` updated with all third-party
packages imported in this task.

---

## Task 28: Implement per-well feature engineering dispatcher

**Module:** `cogcc_pipeline/features.py`
**Function:** `engineer_well_features(df: pd.DataFrame, windows: list[int]) -> pd.DataFrame`

**Description:**

This function is the per-well orchestrator. It is designed to be called inside
`ddf.groupby("well_id").apply(...)` or `ddf.map_partitions(...)` after the DataFrame has
been grouped by well. It receives a single-well pandas DataFrame (all rows for one
`well_id`) and applies the following steps in order:

1. Sort by `production_date` ascending.
2. Call `compute_cumulative_production(df)`.
3. Call `compute_ratios(df)`.
4. Call `compute_decline_rate(df)`.
5. Call `compute_rolling_and_lag(df, windows)`.
6. Call `compute_well_age_and_normalised(df)`.
7. Return the enriched DataFrame.

This function must produce a DataFrame with exactly the columns listed in the Output
Schema (see above). If any column is missing after step 6, raise `ValueError` with the
missing column name.

**Dependencies:** `pandas`

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` (TR-19) — Given a synthetic single-well 12-month DataFrame, call
  `engineer_well_features` and assert the result contains all 29 columns listed in the
  Output Schema.
- `@pytest.mark.unit` (TR-09) — Use the same synthetic input and assert:
  - `oil_bbl_rolling_3m` for month 3 matches the hand-computed value.
  - `oil_bbl_lag1` for month 2 equals `oil_bbl` for month 1.
  - `gor` formula uses gas / oil (not the unit-swapped variant).
  - `water_cut` formula uses `water_bbl / (oil_bbl + water_bbl)`.
- `@pytest.mark.unit` (TR-01) — Assert no column in the result has a physically
  impossible value: all `cum_oil`, `cum_gas`, `cum_water` ≥ 0; all `water_cut` values
  either NaN or in [0, 1]; all `decline_rate` values either NaN or in [-1.0, 10.0].

**Definition of done:** `engineer_well_features` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 29: Implement categorical label encoding

**Module:** `cogcc_pipeline/features.py`
**Function:** `encode_categoricals(ddf: dask.dataframe.DataFrame, encoders_output_path: Path, logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Encode `OpName` and county (extracted as `county_code = well_id.str[2:5]`) as integer
label-encoded columns `op_name_encoded` and `county_encoded`.

Steps:
1. Compute the unique values for `OpName` and `county_code` by calling `.compute()` on
   the respective Dask Series.
2. Build a mapping dict: `{value: integer_index}` for each column, sorted alphabetically
   for determinism. Unknown values at transform time should map to `-1`.
3. Apply the mapping using `map_partitions`. The meta for the encoded integer columns
   must be `pd.Int32Dtype()`.
4. Save the two mapping dicts as JSON to `encoders_output_path` (a single JSON file with
   keys `"op_name"` and `"county_code"`).
5. Return the Dask DataFrame with `op_name_encoded` and `county_encoded` added.

Drop the original `OpName` column and `county_code` intermediate column from the output.

**Dependencies:** `dask.dataframe`, `pandas`, `json`, `pathlib`, `logging`

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` — Given a Dask DataFrame with 3 distinct `OpName` values, assert
  `op_name_encoded` contains integer values in the range [0, 2] and the JSON mapping is
  written correctly.
- `@pytest.mark.unit` — Given a row with an `OpName` not seen during encoding, assert
  it is mapped to `-1` (not an exception).
- `@pytest.mark.unit` (TR-17) — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** `encode_categoricals` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 30: Implement numeric feature scaling

**Module:** `cogcc_pipeline/features.py`
**Function:** `scale_numeric_features(ddf: dask.dataframe.DataFrame, scaler_output_path: Path, logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Apply `sklearn.preprocessing.StandardScaler` to the following columns:
`oil_bbl`, `gas_mcf`, `water_bbl`, `cum_oil`, `cum_gas`, `cum_water`,
`gor`, `water_cut`, `decline_rate`, `well_age_months`,
`oil_bbl_rolling_3m`, `oil_bbl_rolling_6m`, `oil_bbl_rolling_12m`.

Steps:
1. Call `.compute()` to materialise the Dask DataFrame as a pandas DataFrame for fitting.
2. Fill NaN values with column mean before fitting (do not drop rows for scaler fitting).
3. Fit the `StandardScaler` on the pandas DataFrame.
4. Save the fitted scaler to `scaler_output_path` using `joblib.dump`.
5. Apply the scaler transform back to the Dask DataFrame using `map_partitions`. The
   scaled column values replace the original values in the same column names. Fill NaN
   with column mean before applying transform (use the mean values from the fitted
   scaler).
6. Return the Dask DataFrame with scaled values.

Do not scale `well_id`, `production_date`, `op_name_encoded`, `county_encoded`,
`oil_bbl_lag1`, `gas_mcf_lag1`, `water_bbl_lag1`, `oil_bbl_30d`, `gas_mcf_30d`,
`water_bbl_30d` — these are either identifiers, categorical, or normalised separately.

**Dependencies:** `dask.dataframe`, `pandas`, `sklearn`, `joblib`, `pathlib`, `logging`

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` — Given a small synthetic DataFrame, assert that after scaling
  `oil_bbl`, the mean of the scaled `oil_bbl` column is approximately 0.0 and std is
  approximately 1.0 (StandardScaler property).
- `@pytest.mark.unit` — Assert the scaler `.joblib` file is written to `scaler_output_path`
  and can be re-loaded with `joblib.load`.
- `@pytest.mark.unit` (TR-17) — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** `scale_numeric_features` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task 31: Implement Dask-parallel per-well feature engineering

**Module:** `cogcc_pipeline/features.py`
**Function:** `apply_well_features_parallel(ddf: dask.dataframe.DataFrame, windows: list[int], logger: logging.Logger) -> dask.dataframe.DataFrame`

**Description:**

Apply `engineer_well_features` to each well in parallel using Dask.

The preferred approach is:
1. Sort `ddf` by `["well_id", "production_date"]`.
2. Use `ddf.groupby("well_id").apply(engineer_well_features, windows=windows, meta=<meta_df>)`.

The meta DataFrame for `groupby.apply` must have the exact schema of the Output Schema
table (all 29 columns with correct dtypes). String columns use `pd.StringDtype()`. Integer
columns use `pd.Int32Dtype()` or `pd.Int64Dtype()`. Float columns use `pd.Float64Dtype()`.
Never use `"object"` in meta.

Do not call `.compute()` inside this function.

Return the resulting Dask DataFrame.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` (TR-17) — Given a synthetic multi-well Dask DataFrame, assert the
  return type is `dask.dataframe.DataFrame` (`.compute()` not called internally).
- `@pytest.mark.unit` (TR-14) — Given a synthetic 2-well Dask DataFrame, call
  `apply_well_features_parallel` and then `.compute()`. Assert both wells' data is
  present, and both have identical column names and dtypes.
- `@pytest.mark.unit` (TR-03) — After computing the result, group by `well_id` and
  assert `cum_oil` is monotonically non-decreasing for every well in the output.

**Definition of done:** `apply_well_features_parallel` implemented; all tests pass; `ruff`
and `mypy` report no errors; `requirements.txt` updated with all third-party packages
imported in this task.

---

## Task 32: Implement feature matrix Parquet writer and schema validator

**Module:** `cogcc_pipeline/features.py`
**Function:** `write_features_parquet(ddf: dask.dataframe.DataFrame, processed_dir: Path, filename: str, logger: logging.Logger) -> Path`

**Description:**

Repartition to 30 partitions. Write using:
`ddf.repartition(npartitions=30).to_parquet(str(processed_dir / filename), write_index=False, engine="pyarrow", overwrite=True)`.

Return `processed_dir / filename` as a Path.

**Function:** `validate_features_schema(features_parquet_path: Path, logger: logging.Logger) -> list[str]`

**Description:**

Read a sample partition from the features Parquet output. Assert all 29 expected columns
from the Output Schema are present. Return a list of validation error strings for any
missing column.

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` (TR-18) — After writing a synthetic features DataFrame, read with
  `pd.read_parquet` and assert non-empty and correct schema.
- `@pytest.mark.unit` (TR-19) — Call `validate_features_schema` on a Parquet with all 29
  expected columns. Assert the error list is empty.
- `@pytest.mark.unit` (TR-19) — Call `validate_features_schema` on a Parquet missing
  `decline_rate`. Assert the error list contains one entry naming that column.
- `@pytest.mark.unit` — Assert output Parquet directory contains at most 50 `.parquet`
  files.

**Definition of done:** Both functions implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 33: Implement top-level features entry point

**Module:** `cogcc_pipeline/features.py`
**Function:** `run_features(config: dict, logger: logging.Logger) -> Path`

**Description:**

Top-level entry point for the features stage. Executes the following steps in order:

1. Read `features` config section.
2. Read cleaned Parquet using `dask.dataframe.read_parquet(processed_dir / cleaned_file)`.
3. Immediately repartition to `min(ddf.npartitions, 50)`.
4. Call `apply_well_features_parallel(ddf, windows, logger)`.
5. Call `encode_categoricals(ddf, encoders_output_path, logger)`. This step calls
   `.compute()` to build encoding maps.
6. Call `scale_numeric_features(ddf, scaler_output_path, logger)`. This step calls
   `.compute()` to fit the scaler.
7. Call `write_features_parquet(ddf, processed_dir, features_file, logger)`.
8. Call `validate_features_schema(features_parquet_path, logger)`. Log any errors.
9. Return the features Parquet path.

**Test cases (`tests/test_features.py`):**

- `@pytest.mark.unit` (TR-17) — Mock steps 4–7 to intercept the Dask DataFrame before
  each `.compute()` call in steps 5 and 6. Assert that before step 5, the DataFrame is
  still a Dask DataFrame (not prematurely materialised).
- `@pytest.mark.unit` (TR-19) — Mock `validate_features_schema` to return one error.
  Assert `run_features` still returns the features path (no exception) and logs the error.
- `@pytest.mark.integration` — Run `run_features` against real `data/processed/` data.
  Assert the features Parquet exists, is readable, and contains all 29 expected columns.
  Assert `scaler.joblib` and `label_encoders.json` exist. Requires data at
  `data/processed/cogcc_production_cleaned.parquet/`.

**Additional integration test cases:**

- `@pytest.mark.integration` (TR-03) — Read the features Parquet. For each `well_id`,
  assert `cum_oil` is monotonically non-decreasing across all its rows.
- `@pytest.mark.integration` (TR-14) — Read two partition files from the features Parquet.
  Assert column names and dtypes are identical across both partitions (schema stability).
- `@pytest.mark.integration` (TR-01) — Read the features Parquet. Assert no row has
  `cum_oil < 0`, `cum_gas < 0`, `cum_water < 0`, or `water_cut` outside `[0, 1]` (where
  not null).

**Definition of done:** `run_features` implemented; all tests pass; `ruff` and `mypy`
report no errors; `requirements.txt` updated with all third-party packages imported in
this task.

---

## Task 34: Implement pipeline orchestrator

**Module:** `cogcc_pipeline/orchestrator.py`
**Function:** `orchestrate(config_path: str | Path) -> None`

**Description:**

The pipeline orchestrator ties all four stages together in sequence. It:

1. Calls `load_config(config_path)` from `cogcc_pipeline.utils`.
2. Calls `get_logger(...)` using logging config.
3. Calls `run_acquire(config, logger)` — acquire stage.
4. Calls `validate_raw_files(raw_dir, logger)` and logs any errors.
5. Calls `run_ingest(config, logger)` — ingest stage.
6. Calls `run_transform(config, logger)` — transform stage.
7. Calls `run_features(config, logger)` — features stage.
8. Logs a completion summary including: files acquired, rows ingested (from quality
   report), rows in feature matrix, and elapsed wall-clock time.
9. Returns None. Exits with code 1 if any stage raises an unhandled exception.

Each stage is wrapped in a `try/except` block. If a stage raises, log the exception with
traceback and re-raise so the orchestrator exits with a non-zero code.

**Function:** `main() -> None`

Entry point called by `cogcc_pipeline/__main__.py`. Reads `config_path` from
`sys.argv[1]` if provided, otherwise defaults to `"config/pipeline_config.yaml"`. Calls
`orchestrate(config_path)`.

**Test cases (`tests/test_features.py` orchestrator section, or a separate
`tests/test_orchestrator.py`):**

- `@pytest.mark.unit` — Mock all four `run_*` functions. Call `orchestrate`. Assert each
  stage is called exactly once in the correct order (acquire → ingest → transform →
  features).
- `@pytest.mark.unit` — Mock `run_ingest` to raise `RuntimeError`. Assert `orchestrate`
  propagates the exception (does not swallow it) and that `run_transform` and
  `run_features` are NOT called.
- `@pytest.mark.unit` — Mock all stage functions to succeed. Assert `orchestrate`
  returns `None` (does not raise).
- `@pytest.mark.integration` — Run the full pipeline end-to-end for years 2020–2021.
  Assert all output files exist: `data/processed/cogcc_production_cleaned.parquet/`,
  `data/processed/cogcc_production_features.parquet/`,
  `data/processed/data_quality_report.json`,
  `data/processed/scaler.joblib`,
  `data/processed/label_encoders.json`,
  `logs/pipeline_run.log`. Requires network and disk access.

**Definition of done:** `orchestrate` and `main` implemented; all tests pass; `ruff` and
`mypy` report no errors; `requirements.txt` updated with all third-party packages imported
in this task.
