# Features Stage — Task Specifications

## Overview

The features stage reads the cleaned Parquet files from `data/processed/`, computes all
domain-specific engineered features, encodes categorical variables, scales numerical features,
and writes the final ML-ready Parquet dataset to `data/processed/features/`.

This stage is the last in the pipeline before ML/analytics consumption. It must produce a
complete, self-contained dataset with no dependency on the earlier raw or interim stages.

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/features.py`
**Test file:** `tests/test_features.py`

---

## Output Schema

The final ML-ready Parquet files must contain at minimum the following columns (in addition to
any pass-through identifier columns):

| Column               | Type      | Description                                              |
|----------------------|-----------|----------------------------------------------------------|
| well_id              | string    | Composite well identifier (from transform stage)         |
| production_date      | datetime  | First day of the reporting month                         |
| report_year          | int32     | Reporting year                                           |
| report_month         | int32     | Reporting month (1–12)                                   |
| county_code          | string    | 2-digit county code                                      |
| operator_name        | string    | Operator name                                            |
| oil_bbl              | float64   | Monthly oil production (BBL)                             |
| gas_mcf              | float64   | Monthly gas production (MCF)                             |
| water_bbl            | float64   | Monthly water production (BBL)                           |
| days_produced        | float64   | Days of production in the month                          |
| is_valid             | bool      | Validity flag from transform stage                       |
| cum_oil              | float64   | Cumulative oil production per well (BBL)                 |
| cum_gas              | float64   | Cumulative gas production per well (MCF)                 |
| cum_water            | float64   | Cumulative water production per well (BBL)               |
| gor                  | float64   | Gas-oil ratio (gas_mcf / oil_bbl); NaN when oil=0, gas>0 |
| water_cut            | float64   | Water cut (water_bbl / (oil_bbl + water_bbl))            |
| decline_rate         | float64   | Period-over-period oil production decline rate (clipped) |
| oil_roll_3m          | float64   | 3-month rolling average oil production (BBL)             |
| gas_roll_3m          | float64   | 3-month rolling average gas production (MCF)             |
| water_roll_3m        | float64   | 3-month rolling average water production (BBL)           |
| oil_roll_6m          | float64   | 6-month rolling average oil production (BBL)             |
| gas_roll_6m          | float64   | 6-month rolling average gas production (MCF)             |
| water_roll_6m        | float64   | 6-month rolling average water production (BBL)           |
| oil_lag_1m           | float64   | Lag-1 month oil production (BBL)                         |
| gas_lag_1m           | float64   | Lag-1 month gas production (MCF)                         |
| water_lag_1m         | float64   | Lag-1 month water production (BBL)                       |
| oil_mom_pct          | float64   | Month-over-month % change in oil production              |
| gas_mom_pct          | float64   | Month-over-month % change in gas production              |
| county_code_enc      | int32     | Label-encoded county_code                                |
| operator_name_enc    | int32     | Label-encoded operator_name                              |
| oil_bbl_scaled       | float64   | StandardScaler-normalised oil_bbl                        |
| gas_mcf_scaled       | float64   | StandardScaler-normalised gas_mcf                        |
| water_bbl_scaled     | float64   | StandardScaler-normalised water_bbl                      |
| cum_oil_scaled       | float64   | StandardScaler-normalised cum_oil                        |
| cum_gas_scaled       | float64   | StandardScaler-normalised cum_gas                        |
| cum_water_scaled     | float64   | StandardScaler-normalised cum_water                      |

---

## Domain Constraints (informed by oil-and-gas domain expert)

- **GOR (gas-oil ratio) rules (TR-06):**
  - Formula: `gor = gas_mcf / oil_bbl`
  - `oil_bbl = 0`, `gas_mcf > 0`: return NaN (mathematically undefined; do NOT raise exception)
  - `oil_bbl = 0`, `gas_mcf = 0` (shut-in): return `0.0` or NaN per spec (use NaN consistently)
  - `oil_bbl > 0`, `gas_mcf = 0`: return `0.0` (no free gas produced)
  - Do NOT treat high GOR on low oil production as a data error — this is physically valid for
    wells below bubble point.

- **Water cut rules (TR-10):**
  - Formula: `water_cut = water_bbl / (oil_bbl + water_bbl)`
  - `water_bbl = 0` and `oil_bbl > 0`: return `0.0` (valid new well / dry reservoir)
  - `oil_bbl = 0` and `water_bbl > 0`: return `1.0` (valid late-life 100%-water well)
  - Both zero (shut-in): return NaN
  - Values outside `[0.0, 1.0]` are physically impossible — raise `ValueError` or assert in tests
  - Do NOT flag `water_cut=0.0` or `water_cut=1.0` as outliers

- **Decline rate rules (TR-07):**
  - Formula: `decline_rate = (oil_bbl_current - oil_bbl_prev) / oil_bbl_prev`
  - When `oil_bbl_prev = 0` (division by zero): return `0.0` for the raw value before clipping
  - Clip the result to the range `[-1.0, 10.0]` — these are ML feature stability bounds,
    not physical constants
  - The clipping is applied AFTER handling the zero-denominator case (not before)

- **Cumulative production rules (TR-03, TR-08):**
  - Cumulative (`cum_oil`, `cum_gas`, `cum_water`) must be computed per well, sorted by
    `production_date` ascending, using `cumsum()`.
  - Cumulative values must be monotonically non-decreasing (a well cannot un-produce).
  - When a well has zero production months (shut-in), the cumulative value must remain flat
    (equal to the prior month), not jump or decrease.
  - Ensure the data is sorted by `production_date` within each well before cumsum.

---

## Design Decisions & Constraints

- Use `dask.dataframe` for all operations.
- All per-well computations (cumulative production, rolling averages, lag features, decline rate,
  GOR, water cut) must be computed using `map_partitions` with a per-well `groupby` applied inside
  the Pandas inner function.
- After reading processed Parquet, immediately repartition to `min(npartitions, 50)`.
- Never call `.compute()` inside any feature function (TR-17). Only the orchestrator
  `run_features()` may call `.compute()` once for the row-count estimate.
- All `meta=` arguments to `map_partitions` must use `pd.StringDtype()` for string columns and
  never use `"object"` as a dtype value.
- Scaler fitting: fit `StandardScaler` on a sample or the full dataset using `.compute()` in the
  orchestrator. Then apply the fitted scaler inside `map_partitions` using `transform()` (not
  `fit_transform()`, to avoid data leakage across partitions).
- Label encoding: fit `LabelEncoder` for each categorical column (computed once in orchestrator),
  then apply using `map_partitions`.
- Output target: `npartitions = max(1, min(total_rows // 500_000, 50))`. Write to
  `data/processed/features/` with `to_parquet(..., write_index=False, overwrite=True)`.
- Do NOT use `partition_on=` with high-cardinality columns.

---

## Task 01: Implement cumulative production calculator (TR-03, TR-08)

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_cumulative_production(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
For each well, compute cumulative oil, gas, and water production (`cum_oil`, `cum_gas`,
`cum_water`) by summing monthly production values in chronological order. Cumulative values must
be monotonically non-decreasing — a zero-production month leaves the cumulative flat.

- Use `map_partitions` with an inner Pandas function that:
  - Groups by `well_id`.
  - Sorts each group by `production_date` ascending.
  - Applies `cumsum()` to `oil_bbl`, `gas_mcf`, `water_bbl` within each group.
  - Assigns results to new columns `cum_oil`, `cum_gas`, `cum_water`.
  - NaN values in production columns are treated as 0.0 for the purpose of cumsum (use
    `fillna(0)` before cumsum within the inner function, applied only for the cumsum calculation).
    The original production columns must NOT be modified.
  - Returns the full DataFrame with the three new columns added.
- `meta=` must include `cum_oil`, `cum_gas`, `cum_water` as `float64`, plus all pre-existing
  columns.
- Do not call `.compute()`.

**Error handling:** If `well_id` or `production_date` is missing, raise `KeyError`.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a single well with monthly oil productions `[100, 200, 300]` in
  order, assert `cum_oil = [100, 300, 600]` (TR-03 monotonicity).
- `@pytest.mark.unit` — Given a well with oil productions `[100, 0, 200]` (zero in month 2),
  assert `cum_oil = [100, 100, 300]` — the cumulative remains flat in the zero month (TR-08).
- `@pytest.mark.unit` — Given a well with oil productions `[0, 0, 100]` (not yet online for
  first two months), assert `cum_oil = [0, 0, 100]` (TR-08 zero-production at start).
- `@pytest.mark.unit` — Assert `cum_oil` is monotonically non-decreasing for all wells in a
  synthetic 3-well dataset (TR-03).
- `@pytest.mark.unit` — Given a well with a zero-production period followed by resumed production,
  assert cumulative resumes correctly from the flat value after the zero period (TR-08c).
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement GOR calculator (TR-06)

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_gor(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute the Gas-Oil Ratio (GOR) as `gas_mcf / oil_bbl`, with explicit handling for zero and
null denominators. Add the result as a new column `gor` (float64).

- Use `map_partitions`. Inner function:
  - Where `oil_bbl > 0`: `gor = gas_mcf / oil_bbl`
  - Where `oil_bbl == 0` and `gas_mcf > 0`: `gor = NaN`
  - Where `oil_bbl == 0` and `gas_mcf == 0`: `gor = NaN`
  - Where `oil_bbl == 0` and `gas_mcf` is NaN: `gor = NaN`
  - Where `oil_bbl > 0` and `gas_mcf == 0`: `gor = 0.0`
  - Do NOT raise `ZeroDivisionError` under any circumstance.
- `meta=` must include `gor` as `float64`.
- Do not call `.compute()`.

**Error handling:** No exceptions raised for zero/null denominators. Log a DEBUG message if more
than 10% of valid rows have null `gor`.

**Dependencies:** dask[dataframe], pandas, logging, numpy

**Test cases:**
- `@pytest.mark.unit` — `oil_bbl=0, gas_mcf=500`: assert `gor = NaN` (not exception) (TR-06a).
- `@pytest.mark.unit` — `oil_bbl=0, gas_mcf=0`: assert `gor = NaN` (not exception) (TR-06b).
- `@pytest.mark.unit` — `oil_bbl=100, gas_mcf=0`: assert `gor = 0.0` (TR-06c).
- `@pytest.mark.unit` — `oil_bbl=100, gas_mcf=500`: assert `gor = 5.0`.
- `@pytest.mark.unit` — Given a row with extremely high GOR (e.g., `oil_bbl=1, gas_mcf=100000`),
  assert the value is returned as-is (not treated as an error — physically valid below bubble
  point) (TR-06 domain note).
- `@pytest.mark.unit` — Assert `gor` column dtype is `float64`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement water cut calculator (TR-09, TR-10)

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_water_cut(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute water cut as `water_bbl / (oil_bbl + water_bbl)`, with explicit handling for boundary
and zero cases. Add as new column `water_cut` (float64).

- Use `map_partitions`. Inner function:
  - `oil_bbl + water_bbl > 0`: `water_cut = water_bbl / (oil_bbl + water_bbl)`
  - `oil_bbl = 0, water_bbl > 0`: `water_cut = 1.0` (100% water — valid end-of-life state)
  - `water_bbl = 0, oil_bbl > 0`: `water_cut = 0.0` (0% water — valid new well)
  - `oil_bbl = 0, water_bbl = 0` (shut-in): `water_cut = NaN`
  - Result must always be in `[0.0, 1.0]` for non-NaN values.
  - Formula uses total liquid `(oil_bbl + water_bbl)` as denominator — NOT just `oil_bbl`.
- `meta=` must include `water_cut` as `float64`.
- Do not call `.compute()`.

**Error handling:** No exceptions for zero denominators. If any computed non-NaN `water_cut` is
outside `[0.0, 1.0]`, log an ERROR (this indicates a calculation bug).

**Dependencies:** dask[dataframe], pandas, logging, numpy

**Test cases:**
- `@pytest.mark.unit` — `water_bbl=0, oil_bbl=500`: assert `water_cut = 0.0` (TR-10a).
- `@pytest.mark.unit` — `oil_bbl=0, water_bbl=300`: assert `water_cut = 1.0` (TR-10b).
- `@pytest.mark.unit` — `oil_bbl=0, water_bbl=0`: assert `water_cut = NaN`.
- `@pytest.mark.unit` — `oil_bbl=100, water_bbl=100`: assert `water_cut = 0.5` (TR-09e).
- `@pytest.mark.unit` — `oil_bbl=100, water_bbl=300`: assert `water_cut = 0.75`.
- `@pytest.mark.unit` — Assert all non-NaN values in `water_cut` are in `[0.0, 1.0]` for a
  synthetic 50-row DataFrame (TR-01 physical bounds).
- `@pytest.mark.unit` — Assert that `water_cut = 0.0` rows are NOT flagged or dropped (TR-10a
  boundary value is valid).
- `@pytest.mark.unit` — Assert that `water_cut = 1.0` rows are NOT flagged or dropped (TR-10b).
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement decline rate calculator (TR-07)

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_decline_rate(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute the period-over-period oil production decline rate per well. Add as new column
`decline_rate` (float64). The decline rate is clipped to `[-1.0, 10.0]` for ML feature stability.

- Use `map_partitions` with a per-well `groupby`. Inner function:
  - Sort each well group by `production_date` ascending.
  - Compute `oil_bbl_prev = oil_bbl.shift(1)` (lag-1).
  - Handle zero denominator BEFORE clipping:
    - When `oil_bbl_prev = 0` and `oil_bbl > 0`: set raw decline rate to `10.0` (max growth,
      to be handled by clip)
    - When `oil_bbl_prev = 0` and `oil_bbl = 0`: set raw decline rate to `0.0` (shut-in, no
      change)
    - When `oil_bbl_prev > 0`: `decline_rate = (oil_bbl - oil_bbl_prev) / oil_bbl_prev`
  - Apply clip: `decline_rate = decline_rate.clip(lower=-1.0, upper=10.0)`.
  - The first row of each well group (no previous month) must be NaN.
- `meta=` must include `decline_rate` as `float64`.
- Do not call `.compute()`.

**Error handling:** Zero denominator is handled explicitly before clip. No exceptions raised.

**Dependencies:** dask[dataframe], pandas, logging, numpy

**Test cases:**
- `@pytest.mark.unit` — Given `oil_bbl_prev=100, oil_bbl=80`: assert `decline_rate = -0.2` (TR-07c
  value within bounds passes through unchanged).
- `@pytest.mark.unit` — Given a computed value of `-2.0` (below -1.0): assert clipped result
  is exactly `-1.0` (TR-07a).
- `@pytest.mark.unit` — Given a computed value of `15.0` (above 10.0): assert clipped result
  is exactly `10.0` (TR-07b).
- `@pytest.mark.unit` — Given `oil_bbl_prev=0, oil_bbl=0` (two consecutive shut-in months):
  assert the raw pre-clip value is `0.0` (TR-07d — no unclipped extreme value for shut-in).
- `@pytest.mark.unit` — Assert the first row of a well's time series has `decline_rate = NaN`.
- `@pytest.mark.unit` — Assert all non-NaN values are in `[-1.0, 10.0]` for a synthetic
  20-row well sequence.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement rolling average and lag feature calculator (TR-09)

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_rolling_and_lag_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute 3-month and 6-month rolling averages, and 1-month lag features for oil, gas, and water
production, per well. All computations are per-well, sorted by `production_date` ascending.

Columns added:
- `oil_roll_3m`, `gas_roll_3m`, `water_roll_3m` — 3-month rolling mean (window=3, min_periods=1
  is NOT used — use min_periods=3 so partial windows return NaN)
- `oil_roll_6m`, `gas_roll_6m`, `water_roll_6m` — 6-month rolling mean (window=6, min_periods=6)
- `oil_lag_1m`, `gas_lag_1m`, `water_lag_1m` — shift(1) within well group

Rolling windows use `min_periods=window` so that months with fewer than `window` historical
observations return NaN rather than a partial-window value (TR-09b).

- Use `map_partitions` with per-well groupby inside the inner function.
- `meta=` must include all 9 new columns as `float64`.
- Do not call `.compute()`.

**Error handling:** If `well_id` or `production_date` is missing, raise `KeyError`.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a well with `oil_bbl = [10, 20, 30, 40]` in monthly order, assert
  `oil_roll_3m = [NaN, NaN, 20.0, 30.0]` (window=3, min_periods=3 means first 2 are NaN) (TR-09a).
- `@pytest.mark.unit` — Assert `oil_roll_3m` for the first 2 months of a well is NaN (TR-09b).
- `@pytest.mark.unit` — Given `oil_bbl = [10, 20, 30]`, assert `oil_lag_1m = [NaN, 10.0, 20.0]`
  (TR-09c — lag-1 for month N equals raw value for month N-1).
- `@pytest.mark.unit` — Verify lag-1 correctness for at least 3 consecutive months (TR-09c).
- `@pytest.mark.unit` — Assert `oil_roll_6m` is NaN for the first 5 months of a 7-month sequence.
- `@pytest.mark.unit` — Assert all 9 new columns are present in the output schema.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Implement month-over-month percentage change calculator

**Module:** `cogcc_pipeline/features.py`
**Function:** `compute_mom_pct_change(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute the month-over-month percentage change for oil and gas production per well.
Formula: `((current - prev) / prev) * 100` where `prev` is the prior month's value for
the same well. Add as columns `oil_mom_pct` and `gas_mom_pct` (float64).

- Implement using `map_partitions` with per-well groupby.
- When `prev = 0`: set `oil_mom_pct = NaN` (undefined percentage change from zero).
- First row of each well: NaN.
- Clip to `[-100.0, 1000.0]` for ML feature stability.
- `meta=` must include `oil_mom_pct` and `gas_mom_pct` as `float64`.
- Do not call `.compute()`.

**Error handling:** Zero denominator returns NaN (not exception). Clipping applied after NaN
handling.

**Dependencies:** dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given `oil_bbl = [100, 120]`, assert `oil_mom_pct = [NaN, 20.0]`.
- `@pytest.mark.unit` — Given `oil_bbl = [100, 50]`, assert `oil_mom_pct = [NaN, -50.0]`.
- `@pytest.mark.unit` — Given `oil_bbl_prev = 0`, assert `oil_mom_pct = NaN` (not exception).
- `@pytest.mark.unit` — Assert first row of each well has `oil_mom_pct = NaN`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Implement categorical encoder

**Module:** `cogcc_pipeline/features.py`
**Function:** `encode_categoricals(ddf: dask.dataframe.DataFrame, encoders: dict) -> dask.dataframe.DataFrame`

**Description:**
Apply pre-fitted label encoders to categorical columns, adding `_enc` suffix columns. The
encoders are fitted once in the orchestrator (on the full dataset using `.compute()`) and passed
in as a dict `{column_name: fitted_LabelEncoder}`.

Columns to encode:
- `county_code` → `county_code_enc` (int32)
- `operator_name` → `operator_name_enc` (int32)

- Use `map_partitions`. Inner function applies `encoder.transform(...)` within a try/except — if
  an unseen label is encountered, assign `-1` (not exception).
- `meta=` must include `county_code_enc` and `operator_name_enc` as `int32`.
- Do not call `.compute()`.

**Error handling:** Unseen labels mapped to `-1`. Log a WARNING if any unseen labels are found.

**Dependencies:** dask[dataframe], pandas, scikit-learn, logging

**Test cases:**
- `@pytest.mark.unit` — Given a fitted `LabelEncoder` with classes `["A", "B", "C"]` and input
  `["A", "B", "C"]`, assert `county_code_enc` values are `[0, 1, 2]`.
- `@pytest.mark.unit` — Given an unseen label `"D"`, assert the encoded value is `-1` (not
  exception).
- `@pytest.mark.unit` — Assert `county_code_enc` dtype is `int32`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Implement numerical scaler

**Module:** `cogcc_pipeline/features.py`
**Function:** `scale_numerics(ddf: dask.dataframe.DataFrame, scalers: dict) -> dask.dataframe.DataFrame`

**Description:**
Apply pre-fitted `StandardScaler` instances to the specified numerical columns, adding `_scaled`
suffix columns. Scalers are fitted once in the orchestrator using `.compute()` on the full dataset
and passed in as `{column_name: fitted_StandardScaler}`.

Columns to scale (add `_scaled` variant):
- `oil_bbl`, `gas_mcf`, `water_bbl`, `cum_oil`, `cum_gas`, `cum_water`

- Use `map_partitions`. Inner function applies `scaler.transform(X)` where X is the column
  reshaped to `(-1, 1)`.
- NaN values in the source column must remain NaN in the scaled column (do not fill before
  scaling; use `scaler.transform` on non-null values only and assign back).
- `meta=` must include all 6 `_scaled` columns as `float64`.
- Do not call `.compute()`.

**Error handling:** If a scaler for a required column is missing from `scalers`, raise
`KeyError(f"No scaler provided for column: {col}")`.

**Dependencies:** dask[dataframe], pandas, scikit-learn, logging, numpy

**Test cases:**
- `@pytest.mark.unit` — Given a fitted `StandardScaler` for `oil_bbl` (mean=100, std=50) and
  input `oil_bbl=150`, assert `oil_bbl_scaled ≈ 1.0`.
- `@pytest.mark.unit` — Given `oil_bbl = NaN`, assert `oil_bbl_scaled = NaN` (NaN propagates).
- `@pytest.mark.unit` — Assert `oil_bbl_scaled` dtype is `float64`.
- `@pytest.mark.unit` — Given a missing scaler for `gas_mcf`, assert `KeyError` is raised.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 09: Implement scaler and encoder fitter

**Module:** `cogcc_pipeline/features.py`
**Function:** `fit_scalers_and_encoders(ddf: dask.dataframe.DataFrame) -> tuple[dict, dict]`

**Description:**
Fit `StandardScaler` for each numerical column and `LabelEncoder` for each categorical column on
the full dataset. This function calls `.compute()` once on the relevant columns (permitted in the
orchestrator as the sole `.compute()` call for fitting).

- Numeric columns to scale: `oil_bbl`, `gas_mcf`, `water_bbl`, `cum_oil`, `cum_gas`, `cum_water`
- Categorical columns to encode: `county_code`, `operator_name`
- For scaling: call `ddf[numeric_cols].compute()`, drop NaN rows, fit a `StandardScaler` per
  column individually.
- For encoding: call `ddf[cat_cols].compute()`, fill null with `"UNKNOWN"`, fit a `LabelEncoder`
  per column.
- Return `(scalers_dict, encoders_dict)` where each dict maps column name to fitted estimator.
- Log the number of unique categories found per column and the mean/std per numeric column.

**Error handling:** If any required column is missing, raise `KeyError`. If a column has zero
non-null values for fitting, log a WARNING and use a default scaler (mean=0, std=1).

**Dependencies:** scikit-learn, dask[dataframe], pandas, logging

**Test cases:**
- `@pytest.mark.unit` — Given a synthetic Dask DataFrame with `oil_bbl = [100, 200, 300]`, assert
  the returned `StandardScaler` for `oil_bbl` has `mean_ ≈ 200.0`.
- `@pytest.mark.unit` — Given `county_code = ["01", "02", "01"]`, assert the returned
  `LabelEncoder` has `classes_` containing `"01"` and `"02"`.
- `@pytest.mark.unit` — Assert `scalers_dict` contains keys for all 6 numeric columns.
- `@pytest.mark.unit` — Assert `encoders_dict` contains keys `county_code` and `operator_name`.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 10: Implement features Parquet writer

**Module:** `cogcc_pipeline/features.py`
**Function:** `write_features_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path, total_rows_estimate: int) -> None`

**Description:**
Write the ML-ready feature DataFrame to `data/processed/features/` as consolidated Parquet files.

- `npartitions = max(1, min(total_rows_estimate // 500_000, 50))`.
- Repartition before writing: `ddf.repartition(npartitions=npartitions).to_parquet(str(output_dir), write_index=False, overwrite=True)`.
- Create `output_dir` if it does not exist.
- Log the target `npartitions` and path at INFO level.
- Do NOT use `partition_on=` with high-cardinality columns.

**Error handling:** Propagate `OSError` and `pyarrow` write errors after logging.

**Dependencies:** dask[dataframe], pyarrow, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` — Given `total_rows_estimate=3_000_000`, assert `npartitions = 6`.
- `@pytest.mark.unit` — Given `total_rows_estimate=200`, assert `npartitions = 1`.
- `@pytest.mark.integration` — Write a synthetic Dask DataFrame to a tmp directory; read back
  with `pandas.read_parquet` and assert no error (TR-18).
- `@pytest.mark.integration` — Assert the number of files written is <= 50.

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 11: Implement features orchestrator

**Module:** `cogcc_pipeline/features.py`
**Function:** `run_features(config_path: str = "config.yaml") -> dask.dataframe.DataFrame`

**Description:**
Top-level entry point for the features stage. Reads processed Parquet, applies all feature
engineering steps in sequence, fits scalers/encoders, applies them, writes the output, and returns
the final Dask DataFrame.

Execution order:
1. Load config via `load_config(config_path)`.
2. Read `data/processed/` with `dask.dataframe.read_parquet`.
3. Repartition to `min(npartitions, 50)`.
4. Filter to only `is_valid = True` rows (feature engineering is performed on valid data only;
   invalid rows are excluded from the ML-ready output).
5. Call `compute_cumulative_production`.
6. Call `compute_gor`.
7. Call `compute_water_cut`.
8. Call `compute_decline_rate`.
9. Call `compute_rolling_and_lag_features`.
10. Call `compute_mom_pct_change`.
11. Call `fit_scalers_and_encoders` (this calls `.compute()` internally — permitted once).
12. Call `scale_numerics(ddf, scalers)`.
13. Call `encode_categoricals(ddf, encoders)`.
14. Call `.shape[0].compute()` once to get `total_rows_estimate`.
15. Call `write_features_parquet`.
16. Return the Dask DataFrame.

- Log elapsed time and a summary (rows in, rows out, columns in output schema).

**Error handling:** Propagate errors from sub-functions. Log full traceback before re-raising.

**Dependencies:** dask[dataframe], pathlib, logging, datetime

**Test cases:**
- `@pytest.mark.unit` — Given mocked sub-functions, assert `run_features` calls them in the
  correct order.
- `@pytest.mark.integration` — Run `run_features` against real `data/processed/`; assert
  Parquet files exist in `data/processed/features/` and are readable (TR-18).
- `@pytest.mark.integration` — Read the output with `pandas.read_parquet`; assert all required
  output schema columns from the Output Schema table above are present (TR-19).

**Definition of done:** Function implemented, test cases pass, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task 12: Domain and technical correctness tests (TR-01, TR-03, TR-06–TR-10, TR-14, TR-17–TR-19)

**Module:** `tests/test_features.py`

**Description:**
Comprehensive test suite covering all features-stage test requirements. All tests use synthetic
DataFrames unless marked `integration`. Synthetic inputs must be single-well or small multi-well
sequences with known values so assertions are exact.

**Test cases (TR-01 — Physical bounds in features output):**
- `@pytest.mark.unit` — Assert all non-NaN `gor` values in the output are >= 0.0.
- `@pytest.mark.unit` — Assert all non-NaN `water_cut` values are in [0.0, 1.0].

**Test cases (TR-03 — Decline curve monotonicity):**
- `@pytest.mark.unit` — Given a synthetic 12-month well sequence, assert `cum_oil` is
  monotonically non-decreasing (each month >= previous month).
- `@pytest.mark.unit` — Assert `cum_gas` is monotonically non-decreasing.

**Test cases (TR-06 — GOR zero denominator):**
- `@pytest.mark.unit` — `oil_bbl=0, gas_mcf>0`: `gor = NaN`, no exception.
- `@pytest.mark.unit` — `oil_bbl=0, gas_mcf=0`: `gor = NaN`, no exception.
- `@pytest.mark.unit` — `oil_bbl>0, gas_mcf=0`: `gor = 0.0`.

**Test cases (TR-07 — Decline rate clip bounds):**
- `@pytest.mark.unit` — Decline rate below -1.0 clips to exactly -1.0.
- `@pytest.mark.unit` — Decline rate above 10.0 clips to exactly 10.0.
- `@pytest.mark.unit` — Decline rate within bounds passes through unchanged.
- `@pytest.mark.unit` — Two consecutive zero-production months: raw pre-clip value is 0.0,
  not an extreme.

**Test cases (TR-08 — Cumulative flat periods):**
- `@pytest.mark.unit` — Zero-production months mid-sequence: `cum_oil` is flat (equal to
  prior month value).
- `@pytest.mark.unit` — Zero-production months at the start: `cum_oil = [0, 0, ...]`.
- `@pytest.mark.unit` — After shut-in resumption, cumulative resumes from correct prior value.

**Test cases (TR-09 — Feature calculation correctness):**
- `@pytest.mark.unit` — Rolling 3-month avg matches hand-computed values for a known sequence.
- `@pytest.mark.unit` — Rolling 6-month avg is NaN for first 5 months of a well's life.
- `@pytest.mark.unit` — Lag-1 feature for month N equals raw value for month N-1 (verified
  for 3 consecutive months).
- `@pytest.mark.unit` — GOR formula is `gas_mcf / oil_bbl` (not `gas_bbl / oil_mcf`).
- `@pytest.mark.unit` — Water cut formula uses total liquid `(oil_bbl + water_bbl)` as
  denominator.

**Test cases (TR-10 — Water cut boundary values):**
- `@pytest.mark.unit` — `water_bbl=0, oil_bbl>0`: `water_cut=0.0`, row retained.
- `@pytest.mark.unit` — `oil_bbl=0, water_bbl>0`: `water_cut=1.0`, row retained.
- `@pytest.mark.unit` — Only values outside [0, 1] would be treated as invalid (assert no
  valid-range value is flagged).

**Test cases (TR-14 — Schema stability):**
- `@pytest.mark.integration` — Read two feature output partition files; assert column names
  and dtypes are identical across both.

**Test cases (TR-17 — Lazy Dask evaluation):**
- `@pytest.mark.unit` — Assert `compute_cumulative_production`, `compute_gor`,
  `compute_water_cut`, `compute_decline_rate`, `compute_rolling_and_lag_features`,
  `compute_mom_pct_change`, `encode_categoricals`, `scale_numerics` all return
  `dask.dataframe.DataFrame`.

**Test cases (TR-18 — Parquet readability):**
- `@pytest.mark.integration` — Read every file in `data/processed/features/` with
  `pandas.read_parquet`; assert no file raises an exception.

**Test cases (TR-19 — Feature column presence):**
- `@pytest.mark.unit` — Given a synthetic single-well input (12 months), assert the output
  DataFrame contains all required columns from the Output Schema table including: `well_id`,
  `production_date`, `oil_bbl`, `gas_mcf`, `water_bbl`, `cum_oil`, `cum_gas`, `cum_water`,
  `gor`, `water_cut`, `decline_rate`, `oil_roll_3m`, `gas_roll_3m`, `water_roll_3m`,
  `oil_roll_6m`, `gas_roll_6m`, `water_roll_6m`, `oil_lag_1m`, `gas_lag_1m`, `water_lag_1m`,
  `oil_mom_pct`, `gas_mom_pct`, `county_code_enc`, `operator_name_enc`, `oil_bbl_scaled`,
  `gas_mcf_scaled`, `water_bbl_scaled`, `cum_oil_scaled`, `cum_gas_scaled`, `cum_water_scaled`.

**Definition of done:** All tests implemented and passing, `ruff` and `mypy` report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Module-level entry point

**Module:** `cogcc_pipeline/features.py`

Add a `if __name__ == "__main__":` block that calls `run_features()` so the stage can be executed
via `python -m cogcc_pipeline.features`.
