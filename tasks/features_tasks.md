# Features Stage — Task Specifications

**Package:** `cogcc_pipeline`
**Module:** `cogcc_pipeline/features.py`
**Test file:** `tests/test_features.py`

## Overview

The features stage reads the cleaned Parquet dataset (`data/interim/cogcc_cleaned.parquet`), engineers all derived features needed for ML and analytics workflows, and writes the final feature dataset to `data/processed/cogcc_features.parquet`. It also writes label-encoder mappings to `data/processed/encoder_mappings.json`.

This stage uses the Dask distributed scheduler — reuse an existing distributed `Client` if present, otherwise create one from config.

## Feature Catalog

All features below are derived from the cleaned production data and added as new columns. The original columns from the cleaned dataset are preserved.

| Feature column | Description | Notes |
|----------------|-------------|-------|
| `production_date` | `datetime64[ns]`, first of month | Already in cleaned data |
| `year` | Integer year extracted from `production_date` | `pd.Int64Dtype()` |
| `month` | Integer month (1–12) | `pd.Int64Dtype()` |
| `quarter` | Integer quarter (1–4) | `pd.Int64Dtype()` |
| `days_in_month` | Calendar days in the production month | `pd.Int64Dtype()` |
| `months_since_2020` | Months elapsed since 2020-01-01 | `pd.Int64Dtype()` |
| `oil_roll3_mean` | 3-month rolling mean of `OilProduced` per well | `pd.Float64Dtype()` |
| `oil_roll3_std` | 3-month rolling std of `OilProduced` per well | `pd.Float64Dtype()` |
| `oil_roll6_mean` | 6-month rolling mean of `OilProduced` per well | `pd.Float64Dtype()` |
| `oil_roll6_std` | 6-month rolling std of `OilProduced` per well | `pd.Float64Dtype()` |
| `gas_roll3_mean` | 3-month rolling mean of `GasProduced` per well | `pd.Float64Dtype()` |
| `gas_roll3_std` | 3-month rolling std of `GasProduced` per well | `pd.Float64Dtype()` |
| `gas_roll6_mean` | 6-month rolling mean of `GasProduced` per well | `pd.Float64Dtype()` |
| `gas_roll6_std` | 6-month rolling std of `GasProduced` per well | `pd.Float64Dtype()` |
| `water_roll3_mean` | 3-month rolling mean of `WaterProduced` per well | `pd.Float64Dtype()` |
| `water_roll3_std` | 3-month rolling std of `WaterProduced` per well | `pd.Float64Dtype()` |
| `water_roll6_mean` | 6-month rolling mean of `WaterProduced` per well | `pd.Float64Dtype()` |
| `water_roll6_std` | 6-month rolling std of `WaterProduced` per well | `pd.Float64Dtype()` |
| `cum_oil` | Cumulative `OilProduced` per well (Np) | `pd.Float64Dtype()` |
| `cum_gas` | Cumulative `GasProduced` per well (Gp) | `pd.Float64Dtype()` |
| `cum_water` | Cumulative `WaterProduced` per well (Wp) | `pd.Float64Dtype()` |
| `oil_decline_rate` | Month-over-month % change in `OilProduced`, clipped to `[-1.0, 10.0]` | `pd.Float64Dtype()` |
| `gas_decline_rate` | Month-over-month % change in `GasProduced`, clipped to `[-1.0, 10.0]` | `pd.Float64Dtype()` |
| `gor` | Gas-Oil Ratio: `GasProduced / OilProduced`; `NaN` when `OilProduced=0` and `GasProduced>0`; `0.0` when both are 0; `0.0` when `OilProduced>0` and `GasProduced=0` | `pd.Float64Dtype()` |
| `water_cut` | Water cut: `WaterProduced / (OilProduced + WaterProduced)`; `0.0` when both are 0; `NaN` only when denominator is `pd.NA` | `pd.Float64Dtype()` |
| `wor` | Water-Oil Ratio: `WaterProduced / OilProduced`; `NaN` when `OilProduced=0` and `WaterProduced>0`; `0.0` when both are 0 | `pd.Float64Dtype()` |
| `well_age_months` | Months since the well's first production date in the dataset | `pd.Int64Dtype()` |
| `operator_encoded` | Label-encoded integer for `OpName` | `pd.Int64Dtype()` |
| `formation_encoded` | Label-encoded integer for `FormationCode` | `pd.Int64Dtype()` |

## Design Decisions & Constraints

- All per-well computations (rolling, cumulative, decline rate, well age) must use `groupby().transform()` — **never** a Python for loop over groups. This is mandatory for performance at production data volumes.
- All transformation logic inside `map_partitions` functions must use vectorized pandas/numpy operations.
- After reading cleaned Parquet, immediately repartition to `min(npartitions, 50)`.
- Rolling and cumulative features require data to be sorted by `production_date` within each well. The transform stage guarantees this ordering; do not re-sort within the features stage (trust the prior stage's `_sort_partition_by_date`).
- Rolling windows: `min_periods=1` to produce partial windows for first N-1 months (not `NaN` for the full window) — but document in test cases that the first month's 3-month rolling mean equals the value itself.
- Decline rate: computed as `(current - prior) / prior.abs()`, where `prior` is the lag-1 value of the production column. When `prior == 0` and `current > 0`, the result would be infinite — replace with `pd.NA` **before** clipping. When both are 0 (shut-in), result is `0.0`. Then clip to `[config["features"]["decline_rate_clip_min"], config["features"]["decline_rate_clip_max"]]`.
- GOR, water_cut, WOR: use `np.where` or `pd.DataFrame.assign` with vectorized conditional logic. Never raise `ZeroDivisionError`.
- Label encoding: fit a `sklearn.preprocessing.LabelEncoder` on the full (computed) set of `OpName` and `FormationCode` values. Save mappings (class-to-index) to `data/processed/encoder_mappings.json`. Label-encoded columns use `pd.Int64Dtype()`.
- Final output: `data/processed/cogcc_features.parquet`. Target 20–50 output files, never more than 200.
- Never call `.compute()` on independent results sequentially — batch with `dask.compute()`.
- Use `pathlib.Path` throughout. All functions have type hints and docstrings.

## `config.yaml` — features section

```
features:
  interim_dir: "data/interim"
  processed_dir: "data/processed"
  rolling_windows: [3, 6]
  decline_rate_clip_min: -1.0
  decline_rate_clip_max: 10.0
```

---

## Task F-01: Time features

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_time_features(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Add time-derived columns to a pandas partition using vectorized pandas datetime operations on `production_date`:
- `year`: `pdf["production_date"].dt.year.astype(pd.Int64Dtype())`
- `month`: `pdf["production_date"].dt.month.astype(pd.Int64Dtype())`
- `quarter`: `pdf["production_date"].dt.quarter.astype(pd.Int64Dtype())`
- `days_in_month`: `pdf["production_date"].dt.days_in_month.astype(pd.Int64Dtype())`
- `months_since_2020`: `((year - 2020) * 12 + (month - 1)).astype(pd.Int64Dtype())`

Called via `map_partitions`. Meta must include all five new columns with `pd.Int64Dtype()`.

**Test cases:**
- `@pytest.mark.unit` Given `production_date=2021-03-01`, assert `year=2021`, `month=3`, `quarter=1`, `days_in_month=31`, `months_since_2020=14`.
- `@pytest.mark.unit` Given `production_date=2020-01-01`, assert `months_since_2020=0`.
- `@pytest.mark.unit` Given `production_date=2022-12-01`, assert `quarter=4` and `days_in_month=31`.
- `@pytest.mark.unit` Assert all new columns have dtype `pd.Int64Dtype()`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-02: Rolling statistics per well

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_rolling_features(pdf: pd.DataFrame, windows: list[int]) -> pd.DataFrame`

**Description:**
For each window size in `windows` (default `[3, 6]`) and each production column (`OilProduced`, `GasProduced`, `WaterProduced`), add rolling mean and std columns per well using `groupby("well_id").transform(lambda x: x.rolling(window, min_periods=1).mean())`. Use `groupby().transform()` — never iterate over groups.

Column naming convention:
- `oil_roll{window}_mean`, `oil_roll{window}_std`
- `gas_roll{window}_mean`, `gas_roll{window}_std`
- `water_roll{window}_mean`, `water_roll{window}_std`

All new columns use `pd.Float64Dtype()`.

Note: when the index column `well_id` has been set as the DataFrame index (from `set_index` in transform), use `pdf.groupby(level=0)` or `pdf.reset_index()` as appropriate within the partition function.

**Parameters:**
- `pdf`: pandas partition
- `windows`: list of window sizes from config

**Test cases:**
- `@pytest.mark.unit` **TR-09a** Given a known sequence of `OilProduced=[100, 200, 300]` for a single well (sorted by date), assert `oil_roll3_mean` values are `[100.0, 150.0, 200.0]` (partial window with `min_periods=1`).
- `@pytest.mark.unit` **TR-09b** When the well has only 2 months of history, assert `oil_roll6_mean` for month 2 equals the mean of the 2 available values (not 0, not NaN for min_periods=1).
- `@pytest.mark.unit` Assert `oil_roll3_std` for month 1 (only one data point) is `0.0` or `NaN` per the `rolling().std()` definition with `min_periods=1`.
- `@pytest.mark.unit` Given two wells in the same partition, assert rolling features are computed independently per well (group isolation).
- `@pytest.mark.unit` Assert all rolling feature columns have dtype `pd.Float64Dtype()`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-03: Cumulative production per well

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_cumulative_features(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Add cumulative production columns per well using `groupby().transform("cumsum")`:
- `cum_oil`: cumulative sum of `OilProduced` per well
- `cum_gas`: cumulative sum of `GasProduced` per well
- `cum_water`: cumulative sum of `WaterProduced` per well

All new columns use `pd.Float64Dtype()`. Use `groupby("well_id").transform("cumsum")` (or `groupby(level=0)` if index). Treat `pd.NA` in production columns as `0.0` when computing cumulative sums — replace NA with 0 before cumsum and restore NA flag from the original cleaned data. This prevents `NA` from propagating through the cumulative sum.

Called via `map_partitions`.

**Test cases:**
- `@pytest.mark.unit` **TR-03** Given `OilProduced=[100, 200, 150]` for a single well, assert `cum_oil=[100, 300, 450]` (monotonically non-decreasing).
- `@pytest.mark.unit` **TR-08a** Given `OilProduced=[100, 0, 0, 200]` for a single well (two shut-in months), assert `cum_oil=[100, 100, 100, 300]` — flat during shut-in, then resumes correctly.
- `@pytest.mark.unit` **TR-08b** Given `OilProduced=[0, 0, 100]` (well not yet online), assert `cum_oil=[0, 0, 100]`.
- `@pytest.mark.unit` **TR-03** Assert `cum_oil` values are monotonically non-decreasing for any valid input.
- `@pytest.mark.unit` Assert `cum_oil` dtype is `pd.Float64Dtype()`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-04: Decline rate calculation

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_decline_rates(pdf: pd.DataFrame, clip_min: float, clip_max: float) -> pd.DataFrame`

**Description:**
Compute month-over-month production decline rates for `OilProduced` and `GasProduced`, then clip to `[clip_min, clip_max]`:

1. For each production column, compute lag-1 (prior month value) per well using `groupby().transform(lambda x: x.shift(1))`.
2. Compute raw decline rate: `(current - prior) / prior.abs()`.
3. Handle edge cases **before** clipping:
   - `prior == 0` and `current > 0` → set to `pd.NA` (undefined, not infinite)
   - `prior == 0` and `current == 0` → set to `0.0` (shut-in)
   - `prior == pd.NA` or `current == pd.NA` → `pd.NA`
4. Clip the remaining non-NA values to `[clip_min, clip_max]`.
5. Add as `oil_decline_rate` and `gas_decline_rate`, both `pd.Float64Dtype()`.

Use `np.select` or `np.where` for the edge case logic — never row-by-row.

**Parameters:**
- `pdf`: pandas partition
- `clip_min`: from `config["features"]["decline_rate_clip_min"]`
- `clip_max`: from `config["features"]["decline_rate_clip_max"]`

**Test cases:**
- `@pytest.mark.unit` **TR-07a** Given a computed decline rate of `-2.0` (below clip_min=-1.0), assert the result is exactly `-1.0`.
- `@pytest.mark.unit` **TR-07b** Given a computed decline rate of `15.0` (above clip_max=10.0), assert the result is exactly `10.0`.
- `@pytest.mark.unit` **TR-07c** Given a decline rate of `0.5` (within bounds), assert the result is `0.5` unchanged.
- `@pytest.mark.unit` **TR-07d** Given `OilProduced=[0, 0]` for two consecutive months, assert `oil_decline_rate=0.0` (shut-in, not an extreme unclipped value).
- `@pytest.mark.unit` Given `OilProduced=[100, 50]`, assert `oil_decline_rate=-0.5` (50% decline).
- `@pytest.mark.unit` Given `OilProduced=[0, 100]` (`prior=0`, `current>0`), assert `oil_decline_rate=pd.NA`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-05: Ratio features (GOR, water cut, WOR)

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_ratio_features(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Add three ratio features using vectorized `np.where` or `pd.Series.where` logic:

**GOR** (`gor = GasProduced / OilProduced`):
- `OilProduced > 0`: `GasProduced / OilProduced`
- `OilProduced == 0` and `GasProduced == 0`: `0.0`
- `OilProduced == 0` and `GasProduced > 0`: `pd.NA` (mathematically undefined)
- Either value is `pd.NA`: `pd.NA`

**Water Cut** (`water_cut = WaterProduced / (OilProduced + WaterProduced)`):
- Denominator > 0: `WaterProduced / (OilProduced + WaterProduced)`
- Denominator == 0 (both zero): `0.0`
- Either value is `pd.NA`: `pd.NA`
- Result must be in `[0.0, 1.0]` for valid data; do not clamp valid values.

**WOR** (`wor = WaterProduced / OilProduced`):
- `OilProduced > 0`: `WaterProduced / OilProduced`
- `OilProduced == 0` and `WaterProduced == 0`: `0.0`
- `OilProduced == 0` and `WaterProduced > 0`: `pd.NA`
- Either value is `pd.NA`: `pd.NA`

All three new columns use `pd.Float64Dtype()`. Never raise `ZeroDivisionError`.

**Test cases:**
- `@pytest.mark.unit` **TR-06a** `OilProduced=0`, `GasProduced=50` → `gor=pd.NA` (no exception).
- `@pytest.mark.unit` **TR-06b** `OilProduced=0`, `GasProduced=0` → `gor=0.0` (no exception).
- `@pytest.mark.unit` **TR-06c** `OilProduced=100`, `GasProduced=0` → `gor=0.0`.
- `@pytest.mark.unit` **TR-06** `OilProduced=100`, `GasProduced=500` → `gor=5.0`.
- `@pytest.mark.unit` **TR-09d** Verify GOR formula is `GasProduced / OilProduced` (not swapped): given `GasProduced=1000`, `OilProduced=200`, assert `gor=5.0`.
- `@pytest.mark.unit` **TR-09e** Water cut formula: given `WaterProduced=300`, `OilProduced=700`, assert `water_cut=0.3` (denominator is total liquid, not just oil).
- `@pytest.mark.unit` **TR-10a** `WaterProduced=0`, `OilProduced=500` → `water_cut=0.0` (valid, not flagged).
- `@pytest.mark.unit` **TR-10b** `OilProduced=0`, `WaterProduced=200` → `water_cut=1.0` (valid end-of-life well, not flagged).
- `@pytest.mark.unit` **TR-01** GOR must be non-negative: given any valid non-NA inputs, assert `gor >= 0`.
- `@pytest.mark.unit` Assert no `ZeroDivisionError` or `RuntimeWarning` is raised for any combination of zeros and `pd.NA`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-06: Well age feature

**Module:** `cogcc_pipeline/features.py`
**Function:** `_add_well_age(pdf: pd.DataFrame) -> pd.DataFrame`

**Description:**
Add `well_age_months` (`pd.Int64Dtype()`): months since the well's first production date within the partition. Computed using:
- Per well, find `first_date = groupby("well_id")["production_date"].transform("min")`
- `well_age_months = ((production_date.dt.year - first_date.dt.year) * 12 + (production_date.dt.month - first_date.dt.month)).astype(pd.Int64Dtype())`

Use vectorized operations only. Result must be ≥ 0 for all valid rows.

**Test cases:**
- `@pytest.mark.unit` Given a well's first record is 2020-01, assert `well_age_months=0` for that month.
- `@pytest.mark.unit` Given records for 2020-01, 2020-06, 2021-01, assert `well_age_months=0`, `5`, `12` respectively.
- `@pytest.mark.unit` Assert `well_age_months` is non-negative for all rows.
- `@pytest.mark.unit` Assert `well_age_months` dtype is `pd.Int64Dtype()`.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-07: Label encoder for operator and formation

**Module:** `cogcc_pipeline/features.py`
**Function:** `fit_label_encoders(ddf: dd.DataFrame) -> dict[str, dict[str, int]]`
**Function:** `_apply_label_encoding(pdf: pd.DataFrame, mappings: dict[str, dict[str, int]]) -> pd.DataFrame`

**Description:**

`fit_label_encoders(ddf)`:
- Compute unique values of `OpName` and `FormationCode` from the full Dask DataFrame.
- Use `dask.compute(ddf["OpName"].unique(), ddf["FormationCode"].unique())` (batched, not sequential).
- Build a dict-of-dicts mapping: `{"OpName": {"CHEVRON USA": 0, ...}, "FormationCode": {"NIOBRARA": 0, ...}}`.
- Sorted alphabetically before assigning integer codes (ensures reproducible encoding).
- `pd.NA` / `None` maps to `-1` as a sentinel for unknown/missing values.
- Return the mappings dict.

`_apply_label_encoding(pdf, mappings)`:
- Add `operator_encoded` and `formation_encoded` columns using `pd.Series.map(mappings["OpName"])` and `pd.Series.map(mappings["FormationCode"])`.
- Both columns use `pd.Int64Dtype()`.
- Any value not present in the mapping (including `pd.NA`) maps to `-1`.
- Called via `map_partitions`.

**Test cases:**
- `@pytest.mark.unit` Given a DataFrame with `OpName=["A", "B", "A"]`, assert `fit_label_encoders` returns `{"OpName": {"A": 0, "B": 1}, ...}` (alphabetically sorted).
- `@pytest.mark.unit` Given `OpName=pd.NA`, assert `operator_encoded=-1` after `_apply_label_encoding`.
- `@pytest.mark.unit` Given a value not in the training set (unseen operator), assert encoding is `-1`.
- `@pytest.mark.unit` Assert `operator_encoded` dtype is `pd.Int64Dtype()`.

**Definition of done:** Both functions implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-08: Encoder mappings writer

**Module:** `cogcc_pipeline/features.py`
**Function:** `save_encoder_mappings(mappings: dict[str, dict[str, int]], output_path: Path) -> None`

**Description:**
Write the encoder mappings dict to a JSON file at `output_path`. The JSON must be human-readable (indented). Create parent directories if they do not exist.

**Parameters:**
- `mappings`: dict from `fit_label_encoders`
- `output_path`: path to write JSON

**Test cases:**
- `@pytest.mark.unit` Assert the file is created and JSON is valid.
- `@pytest.mark.unit` Assert the file can be re-read with `json.loads` and the round-tripped value equals the original.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-09: Features stage runner

**Module:** `cogcc_pipeline/features.py`
**Function:** `run_features(config: dict) -> None`

**Description:**
Top-level features stage entry point. Executes all feature engineering steps in the following order:

1. Get/create Dask distributed Client via `get_or_create_client(config)`.
2. Read `data/interim/cogcc_cleaned.parquet` using `dd.read_parquet`.
3. Repartition to `min(ddf.npartitions, 50)` immediately after read.
4. Apply `_add_time_features` via `map_partitions` (with updated meta).
5. Apply `_add_rolling_features` via `map_partitions` (passing `windows` from config).
6. Apply `_add_cumulative_features` via `map_partitions`.
7. Apply `_add_decline_rates` via `map_partitions` (passing clip bounds from config).
8. Apply `_add_ratio_features` via `map_partitions`.
9. Apply `_add_well_age` via `map_partitions`.
10. Compute label encoder mappings via `fit_label_encoders(ddf)`.
11. Apply `_apply_label_encoding` via `map_partitions` (passing mappings).
12. Create `data/processed/` directory if it does not exist.
13. Save encoder mappings via `save_encoder_mappings`.
14. Compute output partitions: `npartitions_out = max(1, ddf.npartitions // 10)`.
15. Write `ddf.repartition(npartitions=npartitions_out).to_parquet(processed_dir / "cogcc_features.parquet", write_index=True)`.
16. Log completion summary.
17. Close Client if created by this stage.

**Parameters:**
- `config`: full config dict

**Returns:** `None`

**Error handling:**
- Input Parquet not found → raise `FileNotFoundError`.
- Computation errors → log and re-raise as `RuntimeError`.

**Dependencies:** `dask`, `dask.distributed`, `pandas`, `numpy`, `scikit-learn`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` Mock all `map_partitions` steps; assert `to_parquet` is called with the correct output path.
- `@pytest.mark.unit` **TR-17** Assert no intermediate `.compute()` call is made inside `run_features` except in `fit_label_encoders` and the final `.to_parquet()`.
- `@pytest.mark.unit` If cleaned Parquet not found, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` Run `run_features` on real data; assert `data/processed/cogcc_features.parquet` exists and is non-empty.
- `@pytest.mark.integration` Assert `data/processed/encoder_mappings.json` exists and is valid JSON after a full run.

**Definition of done:** Function implemented, tests pass, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-10: Domain correctness and feature schema tests (TR-01, TR-03, TR-06 through TR-10, TR-14, TR-17, TR-18, TR-19)

**Test file:** `tests/test_features.py`

**Description:**
Implement all domain and technical test cases from `test-requirements.xml` that apply to the features stage.

**Test cases:**

- `@pytest.mark.unit` **TR-01** Physical bound: GOR must be non-negative for all non-NA inputs. Given any synthetic row with `OilProduced>0`, assert `gor >= 0`.
- `@pytest.mark.unit` **TR-03** Decline curve monotonicity: given a synthetic 6-month well sequence with all positive production, assert `cum_oil` values are monotonically non-decreasing.
- `@pytest.mark.unit` **TR-06a/b/c** GOR zero-denominator: test all three zero-denominator cases (detailed in TR-06 above). Assert no exception and correct sentinel values.
- `@pytest.mark.unit` **TR-07a/b/c/d** Decline rate clip bounds: test all four clip cases (detailed in TR-07 above). Use exact expected values.
- `@pytest.mark.unit` **TR-08a/b/c** Cumulative flat periods: test shut-in mid-sequence, start-of-sequence, and resumption (detailed in TR-08 above).
- `@pytest.mark.unit` **TR-09a** Rolling 3-month mean: given `OilProduced=[100,200,300]`, verify `oil_roll3_mean` matches hand-computed values `[100, 150, 200]`.
- `@pytest.mark.unit` **TR-09b** Partial rolling window: given 2 months of history, `oil_roll6_mean` equals the mean of available values.
- `@pytest.mark.unit` **TR-09c** Lag feature: given `OilProduced` at months N-1 and N, verify `oil_roll3_mean` at N includes month N-1 value.
- `@pytest.mark.unit` **TR-09d** GOR formula direction: `GasProduced=1000`, `OilProduced=200` → `gor=5.0` (not `0.2`).
- `@pytest.mark.unit` **TR-09e** Water cut denominator: `WaterProduced=300`, `OilProduced=700` → `water_cut=0.3`.
- `@pytest.mark.unit` **TR-10a** `water_cut=0.0` is valid and preserved: `WaterProduced=0`, `OilProduced=500` → `water_cut=0.0`.
- `@pytest.mark.unit` **TR-10b** `water_cut=1.0` is valid and preserved: `OilProduced=0`, `WaterProduced=200` → `water_cut=1.0`.
- `@pytest.mark.unit` **TR-14** Schema stability: create two synthetic single-well partitions; apply all feature functions to each; assert both output DataFrames have identical column names and dtypes.
- `@pytest.mark.unit` **TR-17** Lazy evaluation: assert `run_features`'s internal Dask graph returns `dd.DataFrame` before final `.to_parquet()`.
- `@pytest.mark.integration` **TR-18** After `run_features`, open each Parquet file in `data/processed/cogcc_features.parquet` using `pd.read_parquet`; assert no exception.
- `@pytest.mark.integration` **TR-19** Feature column presence: read `data/processed/cogcc_features.parquet` and assert all of the following columns are present: `well_id` (as index or column), `production_date`, `OilProduced`, `GasProduced`, `WaterProduced`, `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`, `oil_decline_rate`, `gas_decline_rate`, `oil_roll3_mean`, `oil_roll3_std`, `oil_roll6_mean`, `oil_roll6_std`, `gas_roll3_mean`, `gas_roll3_std`, `gas_roll6_mean`, `gas_roll6_std`, `water_roll3_mean`, `water_roll3_std`, `water_roll6_mean`, `water_roll6_std`, `year`, `month`, `quarter`, `days_in_month`, `months_since_2020`, `well_age_months`, `operator_encoded`, `formation_encoded`.

**Definition of done:** All test cases implemented, integration tests marked with `@pytest.mark.integration`, unit tests passing, `ruff` and `mypy` report no errors, `requirements.txt` updated with all third-party packages imported in this task.
